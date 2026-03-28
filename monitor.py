import json
import re
import os
from supabase import create_client

import feedparser
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

DISTRICT_FEEDS = {
    "riga": "https://www.ss.lv/lv/real-estate/flats/riga/all/sell/rss/",
    "jugla": "https://www.ss.lv/lv/real-estate/flats/riga/jugla/sell/rss/",
    "purvciems": "https://www.ss.lv/lv/real-estate/flats/riga/purvciems/sell/rss/",
    "imanta": "https://www.ss.lv/lv/real-estate/flats/riga/imanta/sell/rss/",
    "kengarags": "https://www.ss.lv/lv/real-estate/flats/riga/kengarags/sell/rss/",
    "mezciems": "https://www.ss.lv/lv/real-estate/flats/riga/mezciems/sell/rss/",
    "adazu-nov": "https://www.ss.lv/lv/real-estate/flats/riga-region/adazu-nov/sell/rss/",
    "sigulda": "https://www.ss.lv/lv/real-estate/flats/riga-region/sigulda/sell/rss/",
}

def load_seen():
    try:
        with open("seen_ids.json") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_seen(seen):
    with open("seen_ids.json", "w") as f:
        json.dump(sorted(seen), f, ensure_ascii=False, indent=2)

def send_telegram_message(bot_token, chat_id, text):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=30)

def normalize_int(value):
    if value is None:
        return None
    value = value.replace(" ", "").replace(",", "").replace(".", "")
    try:
        return int(value)
    except ValueError:
        return None

def extract_field(text, label):
    patterns = {
        "rooms": [r"Istabas:\s*(\d+)"],
        "area": [r"Platība:\s*(\d+(?:[.,]\d+)?)\s*m[²2]"],
        "price": [r"Cena:\s*([\d\s.,]+)\s*€"],
        "floor": [r"Stāvs:\s*([^\s]+)"],
        "street": [r"Iela:\s*(.+?)\s+(?:Istabas:|Platība:|Stāvs:|Sērija:|Mājas tips:|Ērtības:|Cena:)"],
    }
    for pattern in patterns.get(label, []):
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None

def fetch_listing_details(url):
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "lv,en;q=0.9"}
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

    return {
        "title": title,
        "rooms": int(rooms_raw) if rooms_raw and rooms_raw.isdigit() else None,
        "area": float(area_raw.replace(",", ".")) if area_raw else None,
        "price": normalize_int(price_raw),
        "floor": floor_raw,
        "street": street_raw,
        "url": url,
    }

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
    bot_token = os.environ["BOT_TOKEN"]

    # Fetch all active users from Supabase
    result = supabase.table("users").select("*").eq("active", True).execute()
    users = result.data
    print(f"Found {len(users)} active users")

    seen = load_seen()
    new_seen = set(seen)

    # Collect all unique districts needed
    all_districts = set()
    for user in users:
        for d in user.get("districts", []):
            all_districts.add(d)

    # Fetch listings per district
    district_listings = {}
    for district in all_districts:
        feed_url = DISTRICT_FEEDS.get(district)
        if not feed_url:
            continue
        print(f"Checking feed: {feed_url}")
        feed = feedparser.parse(feed_url)
        listings = []
        for entry in feed.entries:
            item_id = entry.get("id") or entry.get("link") or entry.get("title")
            link = entry.get("link", "")
            if not item_id or item_id in seen:
                continue
            try:
                details = fetch_listing_details(link)
                details["item_id"] = item_id
                listings.append(details)
            except Exception as e:
                print(f"Failed to parse {link}: {e}")
            new_seen.add(item_id)
        district_listings[district] = listings

    # Match listings to users and send alerts
    for user in users:
        chat_id = user["chat_id"]
        min_price = user["min_price"]
        max_price = user["max_price"]
        min_rooms = user["min_rooms"]
        user_districts = user.get("districts", [])

        matches = []
        for district in user_districts:
            for listing in district_listings.get(district, []):
                price = listing.get("price")
                rooms = listing.get("rooms")
                if price is None or rooms is None:
                    continue
                if rooms >= min_rooms and min_price <= price <= max_price:
                    matches.append(listing)

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
            send_telegram_message(bot_token, chat_id, message.strip())
            print(f"Sent {len(matches)} matches to {chat_id}")
        else:
            print(f"No matches for {chat_id}")

    save_seen(new_seen)

if __name__ == "__main__":
    main()
