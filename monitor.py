import json
import re
from pathlib import Path

import feedparser
import requests

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
    SEEN_FILE.write_text(json.dumps(sorted(seen), ensure_ascii=False, indent=2))


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
    # Examples: 178,000 € / 178000 € / 178 000 €
    match = re.search(r"(\d[\d\s.,]*)\s*€", text)
    if not match:
        return None

    raw = match.group(1)
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
        match = re.search(pattern, text)
        if match:
            try:
                return int(match.group(1))
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

    print(f"TITLE: {entry.get('title', '')}")
    print(f"PARSED PRICE: {price}, PARSED ROOMS: {rooms}")

    if price is None or rooms is None:
        return False, price, rooms

    ok = rooms >= MIN_ROOMS and MIN_PRICE <= price <= MAX_PRICE
    return ok, price, rooms


def main():
    seen = load_seen()
    new_seen = set(seen)
    matches = []

    for feed_url in FEEDS:
        print(f"Checking feed: {feed_url}")
        feed = feedparser.parse(feed_url)

        for entry in feed.entries:
            item_id = listing_id(entry)
            if not item_id or item_id in seen:
                continue

            ok, price, rooms = qualifies(entry)

            if ok:
                matches.append(
                    {
                        "title": entry.get("title", "New listing"),
                        "rooms": rooms,
                        "price": price,
                        "link": entry.get("link", ""),
                    }
                )

            new_seen.add(item_id)

    if matches:
        message = "🏠 New SS.lv apartment matches\n\n"

        for i, match in enumerate(matches, start=1):
            message += (
                f"{i}. {match['title']}\n"
                f"• Rooms: {match['rooms']}\n"
                f"• Price: €{match['price']}\n"
                f"• Link: {match['link']}\n\n"
            )

        send_telegram_message(message.strip())
    else:
        print("No new matching listings found.")

    save_seen(new_seen)


if __name__ == "__main__":
    main()
