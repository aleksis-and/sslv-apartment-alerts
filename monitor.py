import json
import re
from pathlib import Path
import requests
import feedparser

FEEDS = [
    "https://www.ss.com/lv/real-estate/flats/riga-region/adazu-nov/sell/rss/",
    "https://www.ss.lv/lv/real-estate/flats/riga-region/sigulda/sell/rss/",
]

MIN_PRICE = 150000
MAX_PRICE = 200000
MIN_ROOMS = 4

BOT_TOKEN = Path("bot_token.txt").read_text().strip()
CHAT_ID = Path("chat_id.txt").read_text().strip()

SEEN_FILE = Path("seen_ids.json")


def load_seen():
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text()))
        except Exception:
            return set()
    return set()


def save_seen(seen):
    SEEN_FILE.write_text(json.dumps(sorted(seen)))


def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    response = requests.post(
        url,
        json={
            "chat_id": CHAT_ID,
            "text": text,
            "disable_web_page_preview": False,
        },
        timeout=30,
    )
    response.raise_for_status()


def parse_price(text):
    # Example: 178,000 € or 178000 €
    m = re.search(r"(\d[\d\s.,]*)\s*€", text)
    if not m:
        return None

    raw = m.group(1)
    raw = raw.replace(" ", "").replace(",", "").replace(".", "")
    try:
        return int(raw)
    except ValueError:
        return None


def parse_rooms(text):
    text = text.lower()

    patterns = [
        r"(\d+)\s*[- ]?ist",
        r"istabas[: ]+(\d+)",
        r"komn\.?[: ]+(\d+)",
        r"rooms?[: ]+(\d+)",
    ]

    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass

    return None


def listing_id(entry):
    return entry.get("id") or entry.get("link") or entry.get("title")


def extract_text(entry):
    parts = [
        entry.get("title", ""),
        entry.get("summary", ""),
        entry.get("description", ""),
    ]
    return " | ".join(parts)


def qualifies(entry):
    text = extract_text(entry)
    price = parse_price(text)
    rooms = parse_rooms(text)

    if price is None or rooms is None:
        return False, price, rooms

    ok = (
        rooms >= MIN_ROOMS and
        MIN_PRICE <= price <= MAX_PRICE
    )
    return ok, price, rooms


def main():
    seen = load_seen()
    new_seen = set(seen)

    for feed_url in FEEDS:
        feed = feedparser.parse(feed_url)

        for entry in feed.entries:
            item_id = listing_id(entry)
            if not item_id or item_id in seen:
                continue

            ok, price, rooms = qualifies(entry)

            if ok:
                title = entry.get("title", "New listing")
                link = entry.get("link", "")
                message = (
                    "New SS.lv apartment match!\n\n"
                    f"{title}\n"
                    f"Rooms: {rooms}\n"
                    f"Price: €{price}\n"
                    f"{link}"
                )
                send_telegram_message(message)

            new_seen.add(item_id)

    save_seen(new_seen)


if __name__ == "__main__":
    send_telegram_message("Test message from GitHub Actions")
    main()
