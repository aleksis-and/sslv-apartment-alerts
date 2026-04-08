import re
import os
import logging
import threading
from flask import Flask, request, jsonify
from supabase import create_client
from apscheduler.schedulers.blocking import BlockingScheduler
import feedparser
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

flask_app = Flask(__name__)

APARTMENT_BUY_FEEDS = {
    "riga": "https://www.ss.lv/lv/real-estate/flats/riga/all/sell/rss/",
    "centrs": "https://www.ss.lv/lv/real-estate/flats/riga/centre/sell/rss/",
    "agenskalns": "https://www.ss.lv/lv/real-estate/flats/riga/agenskalns/sell/rss/",
    "aplokciems": "https://www.ss.lv/lv/real-estate/flats/riga/aplokciems/sell/rss/",
    "bergi": "https://www.ss.lv/lv/real-estate/flats/riga/bergi/sell/rss/",
    "bierini": "https://www.ss.lv/lv/real-estate/flats/riga/bierini/sell/rss/",
    "bolderaja": "https://www.ss.lv/lv/real-estate/flats/riga/bolderaja/sell/rss/",
    "breksi": "https://www.ss.lv/lv/real-estate/flats/riga/breksi/sell/rss/",
    "ciekurkalns": "https://www.ss.lv/lv/real-estate/flats/riga/ciekurkalns/sell/rss/",
    "darzciems": "https://www.ss.lv/lv/real-estate/flats/riga/darzciems/sell/rss/",
    "daugavgriva": "https://www.ss.lv/lv/real-estate/flats/riga/daugavgriva/sell/rss/",
    "dreilini": "https://www.ss.lv/lv/real-estate/flats/riga/dreilini/sell/rss/",
    "dzeguzkalns": "https://www.ss.lv/lv/real-estate/flats/riga/dzeguzkalns/sell/rss/",
    "grizinkalns": "https://www.ss.lv/lv/real-estate/flats/riga/grizinkalns/sell/rss/",
    "ilguciems": "https://www.ss.lv/lv/real-estate/flats/riga/ilguciems/sell/rss/",
    "imanta": "https://www.ss.lv/lv/real-estate/flats/riga/imanta/sell/rss/",
    "jaunciems": "https://www.ss.lv/lv/real-estate/flats/riga/jaunciems/sell/rss/",
    "jugla": "https://www.ss.lv/lv/real-estate/flats/riga/jugla/sell/rss/",
    "kengarags": "https://www.ss.lv/lv/real-estate/flats/riga/kengarags/sell/rss/",
    "kipsala": "https://www.ss.lv/lv/real-estate/flats/riga/kipsala/sell/rss/",
    "kliversala": "https://www.ss.lv/lv/real-estate/flats/riga/kliversala/sell/rss/",
    "krasta-r-ns": "https://www.ss.lv/lv/real-estate/flats/riga/krasta-r-ns/sell/rss/",
    "latgales-priekspilseta": "https://www.ss.lv/lv/real-estate/flats/riga/latgales-priekspilseta/sell/rss/",
    "mangali": "https://www.ss.lv/lv/real-estate/flats/riga/mangali/sell/rss/",
    "mezaparks": "https://www.ss.lv/lv/real-estate/flats/riga/mezaparks/sell/rss/",
    "mezciems": "https://www.ss.lv/lv/real-estate/flats/riga/mezciems/sell/rss/",
    "plavnieki": "https://www.ss.lv/lv/real-estate/flats/riga/plavnieki/sell/rss/",
    "purvciems": "https://www.ss.lv/lv/real-estate/flats/riga/purvciems/sell/rss/",
    "sarkandaugava": "https://www.ss.lv/lv/real-estate/flats/riga/sarkandaugava/sell/rss/",
    "sampeteris": "https://www.ss.lv/lv/real-estate/flats/riga/sampeteris-pleskodāle/sell/rss/",
    "teika": "https://www.ss.lv/lv/real-estate/flats/riga/teika/sell/rss/",
    "tornakalns": "https://www.ss.lv/lv/real-estate/flats/riga/tornakalns/sell/rss/",
    "vecaki": "https://www.ss.lv/lv/real-estate/flats/riga/vecaki/sell/rss/",
    "vecmilgravis": "https://www.ss.lv/lv/real-estate/flats/riga/vecmilgravis/sell/rss/",
    "vecriga": "https://www.ss.lv/lv/real-estate/flats/riga/vecriga/sell/rss/",
    "ziepniekkalns": "https://www.ss.lv/lv/real-estate/flats/riga/ziepniekkalns/sell/rss/",
    "zolitude": "https://www.ss.lv/lv/real-estate/flats/riga/zolitude/sell/rss/",
    "pardaugava": "https://www.ss.lv/lv/real-estate/flats/riga/pardaugava/sell/rss/",
    "zasulauks": "https://www.ss.lv/lv/real-estate/flats/riga/zasulauks/sell/rss/",
    "vef": "https://www.ss.lv/lv/real-estate/flats/riga/vef/sell/rss/",
    "jurmala": "https://www.ss.lv/lv/real-estate/flats/jurmala/all/sell/rss/",
    "riga-region": "https://www.ss.lv/lv/real-estate/flats/riga-region/all/sell/rss/",
    "adazu-nov": "https://www.ss.lv/lv/real-estate/flats/riga-region/adazu-nov/sell/rss/",
    "sigulda": "https://www.ss.lv/lv/real-estate/flats/riga-region/sigulda/sell/rss/",
    "salaspils": "https://www.ss.lv/lv/real-estate/flats/riga-region/salaspils/sell/rss/",
    "marupe": "https://www.ss.lv/lv/real-estate/flats/riga-region/marupe/sell/rss/",
    "olaine": "https://www.ss.lv/lv/real-estate/flats/riga-region/olaine/sell/rss/",
    "stopini": "https://www.ss.lv/lv/real-estate/flats/riga-region/stopini/sell/rss/",
    "aizkraukle-and-reg": "https://www.ss.lv/lv/real-estate/flats/aizkraukle-and-reg/sell/rss/",
    "aluksne-and-reg": "https://www.ss.lv/lv/real-estate/flats/aluksne-and-reg/sell/rss/",
    "balvi-and-reg": "https://www.ss.lv/lv/real-estate/flats/balvi-and-reg/sell/rss/",
    "bauska-and-reg": "https://www.ss.lv/lv/real-estate/flats/bauska-and-reg/sell/rss/",
    "cesis-and-reg": "https://www.ss.lv/lv/real-estate/flats/cesis-and-reg/sell/rss/",
    "daugavpils-and-reg": "https://www.ss.lv/lv/real-estate/flats/daugavpils-and-reg/sell/rss/",
    "dobele-and-reg": "https://www.ss.lv/lv/real-estate/flats/dobele-and-reg/sell/rss/",
    "gulbene-and-reg": "https://www.ss.lv/lv/real-estate/flats/gulbene-and-reg/sell/rss/",
    "jekabpils-and-reg": "https://www.ss.lv/lv/real-estate/flats/jekabpils-and-reg/sell/rss/",
    "jelgava-and-reg": "https://www.ss.lv/lv/real-estate/flats/jelgava-and-reg/sell/rss/",
    "kraslava-and-reg": "https://www.ss.lv/lv/real-estate/flats/kraslava-and-reg/sell/rss/",
    "kuldiga-and-reg": "https://www.ss.lv/lv/real-estate/flats/kuldiga-and-reg/sell/rss/",
    "liepaja-and-reg": "https://www.ss.lv/lv/real-estate/flats/liepaja-and-reg/sell/rss/",
    "limbadzi-and-reg": "https://www.ss.lv/lv/real-estate/flats/limbadzi-and-reg/sell/rss/",
    "ludza-and-reg": "https://www.ss.lv/lv/real-estate/flats/ludza-and-reg/sell/rss/",
    "madona-and-reg": "https://www.ss.lv/lv/real-estate/flats/madona-and-reg/sell/rss/",
    "ogre-and-reg": "https://www.ss.lv/lv/real-estate/flats/ogre-and-reg/sell/rss/",
    "preili-and-reg": "https://www.ss.lv/lv/real-estate/flats/preili-and-reg/sell/rss/",
    "rezekne-and-reg": "https://www.ss.lv/lv/real-estate/flats/rezekne-and-reg/sell/rss/",
    "saldus-and-reg": "https://www.ss.lv/lv/real-estate/flats/saldus-and-reg/sell/rss/",
    "talsi-and-reg": "https://www.ss.lv/lv/real-estate/flats/talsi-and-reg/sell/rss/",
    "tukums-and-reg": "https://www.ss.lv/lv/real-estate/flats/tukums-and-reg/sell/rss/",
    "valka-and-reg": "https://www.ss.lv/lv/real-estate/flats/valka-and-reg/sell/rss/",
    "valmiera-and-reg": "https://www.ss.lv/lv/real-estate/flats/valmiera-and-reg/sell/rss/",
    "ventspils-and-reg": "https://www.ss.lv/lv/real-estate/flats/ventspils-and-reg/sell/rss/",
}

APARTMENT_RENT_FEEDS = {k: v.replace("/sell/", "/hand_over/") for k, v in APARTMENT_BUY_FEEDS.items()}

HOUSE_BUY_FEEDS = {
    "riga": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/sell/rss/",
    "centrs": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/centre/sell/rss/",
    "agenskalns": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/agenskalns/sell/rss/",
    "bolderaja": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/bolderaja/sell/rss/",
    "daugavgriva": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/daugavgriva/sell/rss/",
    "ilguciems": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/ilguciems/sell/rss/",
    "imanta": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/imanta/sell/rss/",
    "jugla": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/jugla/sell/rss/",
    "kengarags": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/kengarags/sell/rss/",
    "mezaparks": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/mezaparks/sell/rss/",
    "mezciems": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/mezciems/sell/rss/",
    "purvciems": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/purvciems/sell/rss/",
    "sarkandaugava": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/sarkandaugava/sell/rss/",
    "teika": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/teika/sell/rss/",
    "tornakalns": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/tornakalns/sell/rss/",
    "vecaki": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/vecaki/sell/rss/",
    "vecmilgravis": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/vecmilgravis/sell/rss/",
    "ziepniekkalns": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/ziepniekkalns/sell/rss/",
    "zolitude": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga/zolitude/sell/rss/",
    "jurmala": "https://www.ss.lv/lv/real-estate/homes-summer-residences/jurmala/all/sell/rss/",
    "riga-region": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga-region/sell/rss/",
    "adazu-nov": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga-region/adazu-nov/sell/rss/",
    "sigulda": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga-region/sigulda/sell/rss/",
    "salaspils": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga-region/salaspils/sell/rss/",
    "marupe": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga-region/marupe/sell/rss/",
    "olaine": "https://www.ss.lv/lv/real-estate/homes-summer-residences/riga-region/olaine/sell/rss/",
    "aizkraukle-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/aizkraukle-and-district/sell/rss/",
    "aluksne-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/aluksne-and-district/sell/rss/",
    "balvi-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/balvi-and-district/sell/rss/",
    "bauska-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/bauska-and-district/sell/rss/",
    "cesis-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/cesis-and-district/sell/rss/",
    "daugavpils-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/daugavpils-and-district/sell/rss/",
    "dobele-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/dobele-and-district/sell/rss/",
    "gulbene-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/gulbene-and-district/sell/rss/",
    "jekabpils-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/jekabpils-and-district/sell/rss/",
    "jelgava-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/jelgava-and-district/sell/rss/",
    "kraslava-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/kraslava-and-district/sell/rss/",
    "kuldiga-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/kuldiga-and-district/sell/rss/",
    "liepaja-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/liepaja-and-district/sell/rss/",
    "limbadzi-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/limbazi-and-district/sell/rss/",
    "ludza-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/ludza-and-district/sell/rss/",
    "madona-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/madona-and-district/sell/rss/",
    "ogre-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/ogre-and-district/sell/rss/",
    "preili-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/preili-and-district/sell/rss/",
    "rezekne-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/rezekne-and-district/sell/rss/",
    "saldus-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/saldus-and-district/sell/rss/",
    "talsi-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/talsi-and-district/sell/rss/",
    "tukums-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/tukums-and-district/sell/rss/",
    "valka-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/valka-and-district/sell/rss/",
    "valmiera-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/valmiera-and-district/sell/rss/",
    "ventspils-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/ventspils-and-district/sell/rss/",
}

HOUSE_RENT_FEEDS = {k: v.replace("/sell/", "/hand_over/") for k, v in HOUSE_BUY_FEEDS.items()}

DISTRICT_NAMES = {
    "riga": "Rīga (visi rajoni)",
    "centrs": "Centrs",
    "agenskalns": "Āgenskalns",
    "aplokciems": "Aplokciems",
    "bergi": "Berģi",
    "bierini": "Bieriņi",
    "bolderaja": "Bolderāja",
    "breksi": "Brekši",
    "ciekurkalns": "Čiekurkalns",
    "darzciems": "Dārzciems",
    "daugavgriva": "Daugavgrīva",
    "dreilini": "Dreiliņi",
    "dzeguzkalns": "Dzegužkalns",
    "grizinkalns": "Grīziņkalns",
    "ilguciems": "Iļģuciems",
    "imanta": "Imanta",
    "jaunciems": "Jaunciems",
    "jugla": "Jugla",
    "kengarags": "Ķengarags",
    "kipsala": "Ķīpsala",
    "kliversala": "Klīversala",
    "krasta-r-ns": "Krasta rajons",
    "latgales-priekspilseta": "Latgales priekšpilsēta",
    "mangali": "Mangaļi",
    "mezaparks": "Mežaparks",
    "mezciems": "Mežciems",
    "plavnieki": "Pļavnieki",
    "purvciems": "Purvciems",
    "sarkandaugava": "Sarkandaugava",
    "sampeteris": "Šampēteris",
    "teika": "Teika",
    "tornakalns": "Torņakalns",
    "vecaki": "Vecāķi",
    "vecmilgravis": "Vecmīlgrāvis",
    "vecriga": "Vecrīga",
    "ziepniekkalns": "Ziepniekkalns",
    "zolitude": "Zolitūde",
    "pardaugava": "Pārdaugava",
    "zasulauks": "Zasulauks",
    "vef": "VEF",
    "jurmala": "Jūrmala",
    "riga-region": "Rīgas rajons",
    "adazu-nov": "Ādaži",
    "sigulda": "Sigulda",
    "salaspils": "Salaspils",
    "marupe": "Mārupes pag.",
    "olaine": "Olaine",
    "stopini": "Stopiņi",
    "aizkraukle-and-reg": "Aizkraukle un rajons",
    "aluksne-and-reg": "Alūksne un rajons",
    "balvi-and-reg": "Balvi un rajons",
    "bauska-and-reg": "Bauska un rajons",
    "cesis-and-reg": "Cēsis un rajons",
    "daugavpils-and-reg": "Daugavpils un rajons",
    "dobele-and-reg": "Dobele un rajons",
    "gulbene-and-reg": "Gulbene un rajons",
    "jekabpils-and-reg": "Jēkabpils un rajons",
    "jelgava-and-reg": "Jelgava un rajons",
    "kraslava-and-reg": "Krāslava un rajons",
    "kuldiga-and-reg": "Kuldīga un rajons",
    "liepaja-and-reg": "Liepāja un rajons",
    "limbadzi-and-reg": "Limbaži un rajons",
    "ludza-and-reg": "Ludza un rajons",
    "madona-and-reg": "Madona un rajons",
    "ogre-and-reg": "Ogre un rajons",
    "preili-and-reg": "Preiļi un rajons",
    "rezekne-and-reg": "Rēzekne un rajons",
    "saldus-and-reg": "Saldus un rajons",
    "talsi-and-reg": "Talsi un rajons",
    "tukums-and-reg": "Tukums un rajons",
    "valka-and-reg": "Valka un rajons",
    "valmiera-and-reg": "Valmiera un rajons",
    "ventspils-and-reg": "Ventspils un rajons",
}

def load_seen_for_user(chat_id):
    result = supabase.table("seen_listings").select("id").eq("chat_id", chat_id).execute()
    return set(row["id"] for row in result.data)

def save_seen_for_user(chat_id, new_ids):
    if not new_ids:
        return
    rows = [{"id": id, "chat_id": chat_id} for id in new_ids]
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
        "price": [
            r"Cena:\s*([\d\s.,]+)\s*€",
            r"cena\s+([\d\s]+)\s*€",
            r"(\d[\d\s]*)\s*€",
        ],
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

    # Fallback: extract rooms from title
    if not rooms_raw:
        word_to_num = {
            "vienistabu": "1", "divistabu": "2", "trīsistabu": "3",
            "četristabu": "4", "piecīstabu": "5", "sešistabu": "6",
            "1-istabu": "1", "2-istabu": "2", "3-istabu": "3",
            "4-istabu": "4", "5-istabu": "5", "6-istabu": "6",
        }
        for word, num in word_to_num.items():
            if word in title.lower():
                rooms_raw = num
                break
        if not rooms_raw:
            title_rooms = re.search(r'(\d+)\s*-?\s*istabu', title, re.IGNORECASE)
            if title_rooms:
                rooms_raw = title_rooms.group(1)

    # Fallback: extract price from title
    if not price_raw:
        title_price = re.search(r'(\d[\d\s]*)\s*€', title)
        if title_price:
            price_raw = title_price.group(1)

    return {
        "title": title,
        "rooms": int(rooms_raw) if rooms_raw and str(rooms_raw).strip().isdigit() else None,
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

def fetch_feeds(districts, feeds_dict):
    listings = {}
    for district in districts:
        feed_url = feeds_dict.get(district)
        if not feed_url:
            logging.info(f"No feed for district: {district} — skipping")
            continue
        logging.info(f"Checking feed: {feed_url}")
        feed = feedparser.parse(feed_url)
        district_listings = []
        for entry in feed.entries:
            item_id = entry.get("id") or entry.get("link") or entry.get("title")
            link = entry.get("link", "")
            if not item_id:
                continue
            try:
                details = fetch_listing_details(link)
                details["item_id"] = item_id
                district_listings.append(details)
            except Exception as e:
                logging.error(f"Failed to parse {link}: {e}")
        listings[district] = district_listings
    return listings

def get_feeds(category, intent):
    if category == 'house':
        return HOUSE_BUY_FEEDS if intent == 'buy' else HOUSE_RENT_FEEDS
    return APARTMENT_BUY_FEEDS if intent == 'buy' else APARTMENT_RENT_FEEDS

def process_user(user):
    chat_id = user["chat_id"]
    min_price = user.get("min_price", 0)
    max_price = user.get("max_price", 9999999)
    min_area = user.get("min_area", 0)
    max_area = user.get("max_area", 9999)
    user_rooms = user.get("rooms") or []
    user_districts = user.get("districts", [])
    category = user.get("category", "apartment")
    intent = user.get("intent", "buy")

    feeds = get_feeds(category, intent)
    listings_source = fetch_feeds(set(user_districts), feeds)
    seen = load_seen_for_user(chat_id)
    new_seen = set()
    matches = []

    for district in user_districts:
        for listing in listings_source.get(district, []):
            item_id = listing.get("item_id")
            if item_id in seen:
                continue
            price = listing.get("price")
            rooms = listing.get("rooms")
            area = listing.get("area")

            if price is None or rooms is None:
                new_seen.add(item_id)
                continue
            if user_rooms and rooms not in user_rooms:
                new_seen.add(item_id)
                continue
            if not (min_price <= price <= max_price):
                new_seen.add(item_id)
                continue
            if area is not None and not (min_area <= area <= max_area):
                new_seen.add(item_id)
                continue
            matches.append(listing)
            new_seen.add(item_id)

    if matches:
        category_lv = "dzīvokļi" if category == "apartment" else "mājas"
        intent_lv = "pārdošanā" if intent == "buy" else "īrei"
        district_names = ", ".join([DISTRICT_NAMES.get(d, d) for d in user_districts])
        message = f"🏠 *Jauni SS.lv {category_lv} {intent_lv}*\n"
        message += f"📍 {district_names}\n\n"
        for i, match in enumerate(matches, start=1):
            rooms_str = str(match['rooms']) if match['rooms'] is not None else "Nav"
            message += (
                f"{i}. {match['title']}\n"
                f"• Istabas: {rooms_str}\n"
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

    save_seen_for_user(chat_id, new_seen)

def run():
    logging.info("Starting monitor run...")
    result = supabase.table("users").select("*").eq("active", True).execute()
    users = result.data
    logging.info(f"Found {len(users)} active users")
    if not users:
        return
    for user in users:
        process_user(user)
    logging.info("Run complete.")

@flask_app.route("/run-for-user", methods=["POST"])
def run_for_user():
    data = request.get_json()
    chat_id = data.get("chat_id")
    if not chat_id:
        return jsonify({"error": "chat_id required"}), 400
    result = supabase.table("users").select("*").eq("chat_id", chat_id).eq("active", True).execute()
    if not result.data:
        return jsonify({"error": "user not found"}), 404
    user = result.data[0]
    threading.Thread(target=process_user, args=(user,)).start()
    logging.info(f"Triggered run for user {chat_id}")
    return jsonify({"success": True})

@flask_app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(run, 'interval', minutes=30, next_run_time=__import__('datetime').datetime.now())
    logging.info("Scheduler started — running every 30 minutes.")

    flask_thread = threading.Thread(
        target=lambda: flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    )
    flask_thread.daemon = True
    flask_thread.start()

    scheduler.start()
