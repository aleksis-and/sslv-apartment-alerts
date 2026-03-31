import json
import re
import os
import time
import logging
from supabase import create_client
from apscheduler.schedulers.blocking import BlockingScheduler
import feedparser
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

DISTRICT_FEEDS = {
    "riga": "https://www.ss.lv/lv/real-estate/flats/riga/all/sell/rss/",
    "centrs": "https://www.ss.lv/lv/real-estate/flats/riga/centre/sell/rss/",
    "agenskalns": "https://www.ss.lv/lv/real-estate/flats/riga/agenskalns/sell/rss/",
    "purvciems": "https://www.ss.lv/lv/real-estate/flats/riga/purvciems/sell/rss/",
    "teika": "https://www.ss.lv/lv/real-estate/flats/riga/teika/sell/rss/",
    "plavnieki": "https://www.ss.lv/lv/real-estate/flats/riga/plavnieki/sell/rss/",
    "imanta": "https://www.ss.lv/lv/real-estate/flats/riga/imanta/sell/rss/",
    "jugla": "https://www.ss.lv/lv/real-estate/flats/riga/jugla/sell/rss/",
    "mezciems": "https://www.ss.lv/lv/real-estate/flats/riga/mezciems/sell/rss/",
    "kengarags": "https://www.ss.lv/lv/real-estate/flats/riga/kengarags/sell/rss/",
    "zolitude": "https://www.ss.lv/lv/real-estate/flats/riga/zolitude/sell/rss/",
    "bolderaja": "https://www.ss.lv/lv/real-estate/flats/riga/bolderaja/sell/rss/",
    "ilguciems": "https://www.ss.lv/lv/real-estate/flats/riga/ilguciems/sell/rss/",
    "pardaugava": "https://www.ss.lv/lv/real-estate/flats/riga/pardaugava/sell/rss/",
    "adazu-nov": "https://www.ss.lv/lv/real-estate/flats/riga-region/adazu-nov/sell/rss/",
    "sigulda": "https://www.ss.lv/lv/real-estate/flats/riga-region/sigulda/sell/rss/",
    "salaspils": "https://www.ss.lv/lv/real-estate/flats/riga-region/salaspils/sell/rss/",
    "marupe": "https://www.ss.lv/lv/real-estate/flats/riga-region/marupe/sell/rss/",
    "olaine": "https://www.ss.lv/lv/real-estate/flats/riga-region/olaine/sell/rss/",
    "stopini": "https://www.ss.lv/lv/real-estate/flats/riga-region/stopini/sell/rss/",
    "jurmala": "https://www.ss.lv/lv/real-estate/flats/jurmala/all/sell/rss/",
    "jelgava": "https://www.ss.lv/lv/real-estate/flats/jelgava-and-district/jelgava/sell/rss/",
    "liepaja": "https://www.ss.lv/lv/real-estate/flats/liepaja-and-district/liepaja/sell/rss/",
    "daugavpils": "https://www.ss.lv/lv/real-estate/flats/daugavpils-and-district/daugavpils/sell/rss/",
    "ventspils": "https://www.ss.lv/lv/real-estate/flats/ventspils-and-district/ventspils/sell/rss/",
}

def load_seen():
    result = supabase.table("seen_listings").select("id").execute()
    return set(row["id"] for row in result.data)

def save_seen(new_ids):
    if not new_ids:
        return
    rows = [{"id": id} for id in new_ids]
    supabase.table("seen_listings").upsert(rows).execute()

def send_telegram_message(chat_id, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}, timeout=30)

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
        return "Nav"
    return f"€{price:,}".replace(",", " ")

def format_area(area):
    if area is None:
        return "Nav"
    if float(area).is_integer():
        return f"{int(area)} m²"
    return f"{area:.1f} m²"

def run():
    logging.info("Starting monitor run...")

    result = supabase.table("users").select("*").eq("active", True).execute()
    users = result.data
    logging.info(f"Found {len(users)} active users")

    if not users:
        return

    seen = load_seen()
    new_seen = set()

    all_districts = set()
    for user in users:
        for d in user.get("districts", []):
            all_districts.add(d)

    district_listings = {}
    for district in all_districts:
        feed_url = DISTRICT_FEEDS.get(district)
        if not feed_url:
            continue
        logging.info(f"Checking feed: {feed_url}")
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
                new_seen.add(item_id)
            except Exception as e:
                logging.error(f"Failed to parse {link}: {e}")
                new_seen.add(item_id)
        district_listings[district] = listings

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
            message = "🏠 *Jauni SS.lv dzīvokļi pēc taviem kritērijiem*\n\n"
            for i, match in enumerate(matches, start=1):
                message += (
                    f"{i}. {match['title']}\n"
                    f"• Istabas: {match['rooms']}\n"
                    f"• Cena: {format_price(match['price'])}\n"
                    f"• Platība: {format_area(match['area'])}\n"
                    f"• Stāvs: {match['floor'] or 'Nav'}\n"
                    f"• Adrese: {match['street'] or 'Nav'}\n"
                    f"• Saite: {match['url']}\n\n"
                )
            send_telegram_message(chat_id, message.strip())
            logging.info(f"Sent {len(matches)} matches to {chat_id}")
        else:
            logging.info(f"No matches for {chat_id}")

    save_seen(new_seen)
    logging.info("Run complete.")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(run, 'interval', minutes=30, next_run_time=__import__('datetime').datetime.now())
    logging.info("Scheduler started — running every 30 minutes.")
    scheduler.start()
