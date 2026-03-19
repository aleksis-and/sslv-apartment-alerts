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


def listing_id(entry):
    return entry.get("id") or entry.get("link") or entry.get("title")


def normalize_int(value):
    if value is None:
        return None
    value = value.replace(" ", "").replace(",", "").replace(".", "")
    try:
        return int(value)
    except ValueError:
        return None


def extract_field(text, label):
    # Works on flattened HTML text like "Istabas: 4 Platība: 93 m² Cena: 178 000 €"
    patterns = {
        "rooms": [
            r"Istabas:\s*(\d+)",
        ],
        "area": [
            r"Platība:\s*(\d+(?:[.,]\d+)?)\s*m[²2]",
        ],
        "price": [
            r"Cena:\s*([\d\s.,]+)\s*€",
        ],
        "floor": [
            r"Stāvs:\s*([^\s]+)",
        ],
        "street": [
            r"Iela:\s*(.+?)\s+(?:Istabas:|Platība:|Stāvs:|Sērija:|Mājas tips:|Ērtības:|Cena:)",
        ],
    }

    for pattern in patterns.get(label, []):
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

    return None


def fetch_listing_details(url):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "lv,en;q=0.9",
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    html = response.text

    title_match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    title = re.sub(r"\s+", " ", title_match.group(1)).strip() if title_match else url

    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&nbsp;|&#160;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    rooms_raw = extract_field(text, "rooms")
    area_raw = extract_field(text, "area")
    price_raw = extract_field(text, "price")
    floor_raw = extract_field(text, "floor")
    street_raw = extract_field(text, "street")

    rooms = int(rooms_raw) if rooms_raw and rooms_raw.isdigit() else None
    area = float(area_raw.replace(",", ".")) if area_raw else None
    price = normalize_int(price_raw)

    details = {
        "title": title,
        "rooms": rooms,
        "area": area,
        "price": price,
        "floor": floor_raw,
        "street": street_raw,
        "url": url,
    }

    print(f"DETAILS: {details}")
    return details


def qualifies(details):
    price = details.get("price")
    rooms = details.get("rooms")

    if price is None or rooms is None:
        return False

    return rooms >= MIN_ROOMS and MIN_PRICE <= price <= MAX_PRICE


def format_price(price):
    if price is None:
        return "N/A"
    return f"€{price:,}".replace(",", " ")


def format_area(area):
    if area is None:
        return "N/A"
    if float(area).is_integer():
        return f"{int(area)} m²"
    return f"{area:.1f} m²"


def main():
    seen = load_seen()
    new_seen = set(seen)
    matches = []

    for feed_url in FEEDS:
        print(f"Checking feed: {feed_url}")
        feed = feedparser.parse(feed_url)

        for entry in feed.entries:
            item_id = listing_id(entry)
            link = entry.get("link", "")

            if not item_id or item_id in seen:
                continue

            try:
                details = fetch_listing_details(link)
                if qualifies(details):
                    matches.append(details)
            except Exception as e:
                print(f"Failed to parse listing {link}: {e}")

            new_seen.add(item_id)

    if matches:
        message = "🏠 New SS.lv apartment matches\n\n"

        for i, match in enumerate(matches, start=1):
            message += (
                f"{i}. {match['title']}\n"
                f"• Rooms: {match['rooms']}\n"
                f"• Price: {format_price(match['price'])}\n"
                f"• Area: {format_area(match['area'])}\n"
                f"• Floor: {match['floor'] or 'N/A'}\n"
                f"• Address: {match['street'] or 'N/A'}\n"
                f"• Link: {match['url']}\n\n"
            )

        send_telegram_message(message.strip())
    else:
        print("No new matching listings found.")

    save_seen(new_seen)


if __name__ == "__main__":
    main()
