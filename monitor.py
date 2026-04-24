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
RESEND_API_KEY = os.environ["RESEND_API_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

flask_app = Flask(__name__)

CITY24_DISTRICT_MAP = {
    "centrs":                   {"city": 245396, "district": 270700},
    "agenskalns":               {"city": 245396, "district": 270733},
    "aplokciems":               {"city": 245396, "district": 270729},
    "bergi":                    {"city": 245396, "district": 270722},
    "bierini":                  {"city": 245396, "district": 270721},
    "bolderaja":                {"city": 245396, "district": 270716},
    "breksi":                   {"city": 245396, "district": 270749},
    "ciekurkalns":              {"city": 245396, "district": 270699},
    "darzciems":                {"city": 245396, "district": 270723},
    "daugavgriva":              {"city": 245396, "district": 270728},
    "dreilini":                 {"city": 245396, "district": 270724},
    "dzeguzkalns":              {"city": 245396, "district": None},
    "grizinkalns":              {"city": 245396, "district": 270737},
    "ilguciems":                {"city": 245396, "district": 270738},
    "imanta":                   {"city": 245396, "district": 270736},
    "jaunciems":                {"city": 245396, "district": 270730},
    "jugla":                    {"city": 245396, "district": 270726},
    "kengarags":                {"city": 245396, "district": 270748},
    "kipsala":                  {"city": 245396, "district": None},
    "kliversala":               {"city": 245396, "district": 270745},
    "krasta-r-ns":              {"city": 245396, "district": 270739},
    "latgales-priekspilseta":   {"city": 245396, "district": 270742},
    "mangali":                  {"city": 245396, "district": 270744},
    "mezaparks":                {"city": 245396, "district": 270740},
    "mezciems":                 {"city": 245396, "district": 270703},
    "plavnieki":                {"city": 245396, "district": 270709},
    "purvciems":                {"city": 245396, "district": 270704},
    "sarkandaugava":            {"city": 245396, "district": 270707},
    "sampeteris":               {"city": 245396, "district": 270705},
    "teika":                    {"city": 245396, "district": 270708},
    "tornakalns":               {"city": 245396, "district": 270712},
    "vecaki":                   {"city": 245396, "district": 270711},
    "vecmilgravis":             {"city": 245396, "district": 270719},
    "vecriga":                  {"city": 245396, "district": 270718},
    "ziepniekkalns":            {"city": 245396, "district": 270714},
    "zolitude":                 {"city": 245396, "district": 270715},
    "pardaugava":               {"city": 245396, "district": 270701},
    "zasulauks":                {"city": 245396, "district": 270713},
    "vef":                      {"city": 245396, "district": None},
    "riga":                     {"city": 245396, "district": None},
    "jurmala":                  {"city": 245372, "district": None},
    "sigulda":                  {"city": 245404, "district": None},
    "salaspils":                {"city": 245400, "district": None},
    "marupe":                   {"city": 245387, "district": None},
    "olaine":                   {"city": 245389, "district": None},
    "adazu-nov":                {"city": 245423, "district": None},
    "stopini":                  {"city": 245689, "district": None},
    "riga-region":              {"city": 245330, "district": None},
    "aizkraukle-and-reg":       {"city": 245346, "district": None},
    "aluksne-and-reg":          {"city": 245350, "district": None},
    "balvi-and-reg":            None,
    "bauska-and-reg":           {"city": 245356, "district": None},
    "cesis-and-reg":            {"city": 245359, "district": None},
    "daugavpils-and-reg":       {"city": 245361, "district": None},
    "dobele-and-reg":           {"city": 245362, "district": None},
    "gulbene-and-reg":          {"city": 245365, "district": None},
    "jekabpils-and-reg":        {"city": 245371, "district": None},
    "jelgava-and-reg":          {"city": 245370, "district": None},
    "kraslava-and-reg":         {"city": 245375, "district": None},
    "kuldiga-and-reg":          {"city": 245376, "district": None},
    "liepaja-and-reg":          {"city": 245379, "district": None},
    "limbadzi-and-reg":         None,
    "ludza-and-reg":            {"city": 245382, "district": None},
    "madona-and-reg":           {"city": 245385, "district": None},
    "ogre-and-reg":             {"city": 245388, "district": None},
    "preili-and-reg":           None,
    "rezekne-and-reg":          {"city": 245395, "district": None},
    "saldus-and-reg":           None,
    "talsi-and-reg":            {"city": 245411, "district": None},
    "tukums-and-reg":           {"city": 245412, "district": None},
    "valka-and-reg":            None,
    "valmiera-and-reg":         {"city": 245415, "district": None},
    "ventspils-and-reg":        {"city": 245418, "district": None},
}

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
    "aizkraukle-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/aizkraukle-and-reg/sell/rss/",
    "aluksne-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/aluksne-and-reg/sell/rss/",
    "balvi-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/balvi-and-reg/sell/rss/",
    "bauska-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/bauska-and-reg/sell/rss/",
    "cesis-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/cesis-and-reg/sell/rss/",
    "daugavpils-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/daugavpils-and-reg/sell/rss/",
    "dobele-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/dobele-and-reg/sell/rss/",
    "gulbene-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/gulbene-and-reg/sell/rss/",
    "jekabpils-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/jekabpils-and-reg/sell/rss/",
    "jelgava-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/jelgava-and-reg/sell/rss/",
    "kraslava-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/kraslava-and-reg/sell/rss/",
    "kuldiga-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/kuldiga-and-reg/sell/rss/",
    "liepaja-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/liepaja-and-reg/sell/rss/",
    "limbadzi-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/limbadzi-and-reg/sell/rss/",
    "ludza-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/ludza-and-reg/sell/rss/",
    "madona-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/madona-and-reg/sell/rss/",
    "ogre-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/ogre-and-reg/sell/rss/",
    "preili-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/preili-and-reg/sell/rss/",
    "rezekne-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/rezekne-and-reg/sell/rss/",
    "saldus-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/saldus-and-reg/sell/rss/",
    "talsi-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/talsi-and-reg/sell/rss/",
    "tukums-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/tukums-and-reg/sell/rss/",
    "valka-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/valka-and-reg/sell/rss/",
    "valmiera-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/valmiera-and-reg/sell/rss/",
    "ventspils-and-reg": "https://www.ss.lv/lv/real-estate/homes-summer-residences/ventspils-and-reg/sell/rss/",
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

def slugify(text):
    if not text:
        return ""
    replacements = {
        'ā': 'a', 'č': 'c', 'ē': 'e', 'ģ': 'g', 'ī': 'i',
        'ķ': 'k', 'ļ': 'l', 'ņ': 'n', 'š': 's', 'ū': 'u', 'ž': 'z',
        'Ā': 'a', 'Č': 'c', 'Ē': 'e', 'Ģ': 'g', 'Ī': 'i',
        'Ķ': 'k', 'Ļ': 'l', 'Ņ': 'n', 'Š': 's', 'Ū': 'u', 'Ž': 'z',
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = text.strip('-')
    return text

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
    requests.post(url, json={
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }, timeout=30)

def send_email_message(to_email, subject, html_content):
    try:
        response = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "from": "Paziņojumi <no-reply@pazinojumi.lv>",
                "to": [to_email],
                "subject": subject,
                "html": html_content
            },
            timeout=30
        )
        if response.status_code == 200:
            logging.info(f"Email sent to {to_email}")
        else:
            logging.error(f"Email failed: {response.status_code} {response.text}")
    except Exception as e:
        logging.error(f"Email error: {e}")

def send_push_notification(push_token, title, body):
    if not push_token:
        return
    try:
        response = requests.post(
            "https://exp.host/--/api/v2/push/send",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json"
            },
            json={
                "to": push_token,
                "title": title,
                "body": body,
                "sound": "default",
                "data": {}
            },
            timeout=30
        )
        logging.info(f"Push sent: {response.status_code} {response.text}")
    except Exception as e:
        logging.error(f"Push error: {e}")

def save_listings_to_db(user, matches, district_names):
    user_id = user.get("id")
    auth_user_id = user.get("auth_user_id")
    if not user_id:
        return
    rows = []
    for match in matches:
        rows.append({
            "user_id": user_id,
            "auth_user_id": auth_user_id,
            "title": match.get("title", ""),
            "price": match.get("price"),
            "rooms": match.get("rooms"),
            "area": match.get("area"),
            "district": district_names,
            "url": match.get("url", ""),
            "image_url": None,
            "source": match.get("source", "SS.lv"),
            "seen": False,
            "saved": False,
        })
    if rows:
        try:
            supabase.table("listings").insert(rows).execute()
            logging.info(f"Saved {len(rows)} listings to DB for user {user_id}")
        except Exception as e:
            logging.error(f"Failed to save listings to DB: {e}")

def build_email_html(matches, category, intent, district_names):
    category_lv = "dzīvokļi" if category == "apartment" else "mājas"
    intent_lv = "pārdošanā" if intent == "buy" else "īrei"
    icon = "🏢" if category == "apartment" else "🏡"

    items_html = ""
    for i, match in enumerate(matches, start=1):
        rooms_str = str(match['rooms']) if match['rooms'] is not None else "Nav"
        source = match.get('source', 'SS.lv')
        source_badge = "City24" if source == "City24.lv" else "SS.lv"
        address = match.get('street') or 'Nav'
        city_name = match.get('city_name', '') if source == "City24.lv" else ""
        heading = f"{address}, {city_name} [{source_badge}]" if city_name else f"{address} [{source_badge}]"
        price = match.get('price')
        area = match.get('area')

        items_html += f"""
        <div style="background:#fff;border:1px solid #f0ece4;border-radius:12px;padding:16px;margin-bottom:12px;">
            <p style="font-size:15px;font-weight:600;color:#1a1a1a;margin-bottom:10px;">{i}. {icon} {heading}</p>
            <table style="width:100%;border-collapse:collapse;">
                <tr><td style="color:#888;font-size:13px;padding:3px 0;width:100px;">Cena</td><td style="font-size:13px;font-weight:600;color:#1a1a1a;">{format_price(price)}</td></tr>
                <tr><td style="color:#888;font-size:13px;padding:3px 0;">Cena/m²</td><td style="font-size:13px;color:#1a1a1a;">{format_price_per_sqm(price, area)}</td></tr>
                <tr><td style="color:#888;font-size:13px;padding:3px 0;">Istabas</td><td style="font-size:13px;color:#1a1a1a;">{rooms_str}</td></tr>
                <tr><td style="color:#888;font-size:13px;padding:3px 0;">Platība</td><td style="font-size:13px;color:#1a1a1a;">{format_area(area)}</td></tr>
                <tr><td style="color:#888;font-size:13px;padding:3px 0;">Stāvs</td><td style="font-size:13px;color:#1a1a1a;">{match['floor'] or 'Nav'}</td></tr>
            </table>
            <a href="{match['url']}" style="display:inline-block;margin-top:10px;padding:8px 16px;background:#f5a623;color:#fff;border-radius:8px;text-decoration:none;font-size:13px;font-weight:600;">Skatīt sludinājumu →</a>
        </div>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/></head>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#fffdf9;margin:0;padding:20px;">
        <div style="max-width:560px;margin:0 auto;">
            <div style="text-align:center;margin-bottom:24px;">
                <h1 style="font-size:22px;font-weight:700;color:#1a1a1a;margin:0;">Jauni {category_lv} {intent_lv}</h1>
                <p style="color:#888;font-size:14px;margin-top:6px;">📍 {district_names}</p>
            </div>
            {items_html}
            <div style="text-align:center;margin-top:24px;padding-top:16px;border-top:1px solid #f0ece4;">
                <p style="color:#bbb;font-size:11px;">Atteikties no paziņojumiem — <a href="https://pazinojumi.lv" style="color:#f5a623;">pazinojumi.lv</a></p>
            </div>
        </div>
    </body>
    </html>
    """

def normalize_int(value):
    if value is None:
        return None
    value = str(value).replace(" ", "").replace(",", "").replace(".", "")
    try:
        return int(value)
    except ValueError:
        return None

def extract_field(text, label):
    patterns = {
        "rooms": [r"Istabas:\s*(\d+)"],
        "area": [r"(?<![a-zA-ZāčēģīķļņšūžĀČĒĢĪĶĻŅŠŪŽ])Platība:\s*(\d+(?:[.,]\d+)?)\s*m[²2]"],
        "price": [
            r"Cena:\s*([\d\s.,]+)\s*€",
            r"cena\s+([\d\s]+)\s*€",
            r"(\d[\d\s]*)\s*€",
        ],
        "floor": [r"Stāvs:\s*([^\s]+)", r"Stāvu skaits:\s*(\d+)"],
        "street": [r"Iela:\s*(.+?)\s+(?:Istabas:|Platība:|Stāvs:|Stāvu skaits:|Sērija:|Mājas tips:|Ērtības:|Cena:|Zemes platība:)"],
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
    if street_raw:
        street_raw = re.sub(r'\s*\[?\s*Karte\s*\]?\s*$', '', street_raw).strip()
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
        "city_name": "",
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

def format_price_per_sqm(price, area):
    if price is None or area is None or area == 0:
        return "Nav"
    ppm = round(price / area)
    return f"€{ppm:,}/m²".replace(",", " ")

def fetch_feeds(districts, feeds_dict):
    listings = {}
    for district in districts:
        feed_url = feeds_dict.get(district)
        if not feed_url:
            logging.info(f"No feed for district: {district} — skipping")
            continue
        logging.info(f"Checking SS.lv feed: {feed_url}")
        feed = feedparser.parse(feed_url)
        district_listings = []
        for entry in feed.entries:
            item_id = entry.get("id") or entry.get("link") or entry.get("title")
            link = entry.get("link", "")
            if not item_id:
                continue
            try:
                details = fetch_listing_details(link)
                details["item_id"] = "ss_" + item_id
                details["source"] = "SS.lv"
                district_listings.append(details)
            except Exception as e:
                logging.error(f"Failed to parse {link}: {e}")
        listings[district] = district_listings
    return listings

def fetch_city24_listings(districts, category, intent):
    unit_type = "Apartment" if category == "apartment" else "House"
    ts_type = "sale" if intent == "buy" else "rent"
    listing_type = "apartments" if category == "apartment" else "houses"
    listing_intent = "sale" if intent == "buy" else "rent"
    all_listings = {}

    for district in districts:
        mapping = CITY24_DISTRICT_MAP.get(district)
        if not mapping:
            continue

        city_id = mapping["city"]
        district_id = mapping.get("district")

        params = {
            "address[cc]": 2,
            "address[city][]": city_id,
            "tsType": ts_type,
            "unitType": unit_type,
            "adReach": 1,
            "itemsPerPage": 50,
            "page": 1,
        }
        if district_id:
            params["address[district][]"] = district_id

        try:
            url = "https://api.city24.lv/lv_LV/search/realties"
            headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            district_listings = []
            for item in data:
                item_id = "city24_" + str(item.get("id", ""))
                friendly_id = item.get("friendly_id", "")
                price_raw = item.get("price")
                rooms = item.get("room_count")
                area = item.get("property_size")
                addr = item.get("address", {})
                street_name_raw = addr.get("street_name", "")
                house_number = addr.get("house_number", "") if addr.get("export_house_number") else ""
                apartment_number = addr.get("apartment_number", "") if addr.get("export_apartment_number") else ""
                city_name_raw = addr.get("city_name", "")
                county_name_raw = addr.get("county_name") or city_name_raw
                attrs = item.get("attributes", {})
                floor = attrs.get("FLOOR")
                total_floors = attrs.get("TOTAL_FLOORS")
                floor_str = f"{floor}/{total_floors}" if floor and total_floors else (str(floor) if floor else None)

                full_address = street_name_raw
                if house_number:
                    full_address += f" {house_number}"
                if apartment_number:
                    full_address += f"-{apartment_number}"

                county_slug = slugify(county_name_raw)
                city_slug = slugify(city_name_raw)
                street_slug = slugify(street_name_raw)
                parts = [county_slug]
                if city_slug and city_slug != county_slug:
                    parts.append(city_slug)
                if street_slug:
                    parts.append(street_slug)
                address_slug = re.sub(r'-+', '-', "-".join(filter(None, parts))).strip('-')
                listing_url = f"https://www.city24.lv/real-estate/{listing_type}-for-{listing_intent}/{address_slug}/{friendly_id}?i=0"

                district_listings.append({
                    "item_id": item_id,
                    "title": f"City24.lv — {full_address}, {city_name_raw}",
                    "city_name": city_name_raw,
                    "rooms": rooms,
                    "area": float(area) if area else None,
                    "price": int(float(price_raw)) if price_raw else None,
                    "floor": floor_str,
                    "street": full_address,
                    "url": listing_url,
                    "source": "City24.lv",
                })
            all_listings[district] = district_listings
            logging.info(f"City24 fetched {len(district_listings)} listings for {district}")
        except Exception as e:
            logging.error(f"City24 fetch failed for {district}: {e}")

    return all_listings

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
    channel = user.get("channel", "telegram")
    email = user.get("email", "")

    feeds = get_feeds(category, intent)
    ss_listings = fetch_feeds(set(user_districts), feeds)
    city24_listings = fetch_city24_listings(user_districts, category, intent)

    seen = load_seen_for_user(chat_id)
    new_seen = set()
    matches = []

    for district in user_districts:
        combined = list(ss_listings.get(district, [])) + list(city24_listings.get(district, []))
        for listing in combined:
            item_id = listing.get("item_id")
            if item_id in seen:
                continue
            price = listing.get("price")
            rooms = listing.get("rooms")
            area = listing.get("area")
            if price is None:
                new_seen.add(item_id)
                continue
            if category == 'apartment' and rooms is None:
                new_seen.add(item_id)
                continue
            if user_rooms and rooms is not None and rooms not in user_rooms:
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
        icon = "🏢" if category == "apartment" else "🏡"
        district_names = ", ".join([DISTRICT_NAMES.get(d, d) for d in user_districts])

        if channel == "email" and email:
            subject = f"Jauni {category_lv} {intent_lv} — {district_names}"
            html = build_email_html(matches, category, intent, district_names)
            send_email_message(email, subject, html)

        elif channel == "push":
            push_token = user.get("push_token")
            save_listings_to_db(user, matches, district_names)
            send_push_notification(
                push_token,
                f"{len(matches)} jauni sludinājumi",
                f"{matches[0].get('street') or category_lv} — {format_price(matches[0].get('price'))}"
            )

        else:
            message = f"🏠 *Jauni {category_lv} {intent_lv}*\n"
            message += f"📍 {district_names}\n\n"
            for i, match in enumerate(matches, start=1):
                rooms_str = str(match['rooms']) if match['rooms'] is not None else "Nav"
                source = match.get('source', 'SS.lv')
                source_badge = "City24" if source == "City24.lv" else "SS.lv"
                address = match.get('street') or 'Nav'
                city_name = match.get('city_name', '') if source == "City24.lv" else ""
                heading = f"{address}, {city_name} [{source_badge}]" if city_name else f"{address} [{source_badge}]"
                price = match.get('price')
                area = match.get('area')
                message += (
                    f"{i}. {icon} *{heading}*\n"
                    f"• Cena: {format_price(price)}\n"
                    f"• Cena/m²: {format_price_per_sqm(price, area)}\n"
                    f"• Istabas: {rooms_str}\n"
                    f"• Platība: {format_area(area)}\n"
                    f"• Stāvs: {match['floor'] or 'Nav'}\n"
                    f"• [Skatīt sludinājumu]({match['url']})\n\n"
                )
            send_telegram_message(chat_id, message.strip())

        logging.info(f"Sent {len(matches)} matches to {chat_id} via {channel}")
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
