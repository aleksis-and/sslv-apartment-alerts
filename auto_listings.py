import html
import re
from html.parser import HTMLParser


class SaleRows(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.row = None
        self.cell = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "tr":
            self.row = {"cells": []} if re.fullmatch(r"tr_\d+", attrs.get("id", "")) else None
            self.cell = None
        elif self.row is not None:
            if tag == "td":
                self.cell = [] if "pp6" in attrs.get("class", "").split() else None
            elif tag == "a" and attrs.get("class") == "am":
                self.row["path"] = attrs.get("href", "")
            elif tag == "img" and "foto_list" in attrs.get("class", "").split():
                self.row["image"] = attrs.get("src")

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag == "td" and self.cell is not None:
            self.row["cells"].append(" ".join("".join(self.cell).split()))
            self.cell = None
        elif tag == "tr" and self.row is not None:
            self.rows.append(self.row)
            self.row = None


def normalized_model(value):
    return re.sub(r"[^a-z0-9]", "", value.lower())


def parse_sales(body, make):
    parser = SaleRows()
    parser.feed(body)
    for row in parser.rows:
        path = row.get("path", "")
        if not re.fullmatch(rf"/msg/lv/transport/cars/{re.escape(make)}/[a-z0-9-]+/[a-z]+\.html", path):
            continue
        cells = row["cells"]
        if len(cells) != 5:
            continue
        model, year, displacement, mileage, price = cells
        if not re.fullmatch(r"\d{4}", year) or not re.fullmatch(r"[\d ,]+\s*€", price):
            continue
        liters = re.match(r"\d+(?:[.,]\d+)?", displacement)
        km = re.fullmatch(r"([\d.,]+)\s*tūkst\.", mileage)
        url = "https://www.ss.lv" + path
        make_label = make.upper() if make in ("bmw", "gaz", "vaz") else make.replace("-", " ").title()
        title = f"{make_label} {model} · {year}"
        yield {
            "item_id": "ss_" + url, "url": url, "title": title, "street": title,
            "price": int(re.sub(r"\D", "", price)), "source": "SS.lv",
            "rooms": None, "area": None, "image_url": row.get("image"),
            "auto_data": {
                "model": model, "model_slug": path.split("/")[-2], "year": int(year),
                "liters": float(liters[0].replace(",", ".")) if liters else None,
                "mileage": round(float(km[1].replace(",", ".")) * 1000) if km else None,
            },
        }


def read_specs(body):
    def field(id):
        match = re.search(rf'<td\b[^>]*\bid="{id}"[^>]*>(.*?)</td>', body, re.S)
        return html.unescape(re.sub(r"<[^>]+>", "", match[1])).strip().lower() if match else ""
    engine, gearbox = field("tdo_15"), field("tdo_35")
    fuels = (("hibrīd", "hybrid"), ("gāze", "lpg"), ("dīzel", "diesel"),
             ("benzīn", "petrol"), ("elektr", "electric"))
    return {
        "engine": next((value for label, value in fuels if label in engine), None),
        "transmission": "automatic" if "automāt" in gearbox else "manual" if "manuāl" in gearbox else None,
    }


def matches_auto(listing, filters, specs=None):
    data = listing["auto_data"]
    raw_model = filters.get("model") or ""
    if not isinstance(raw_model, str):
        return False
    model = normalized_model(raw_model)
    if model and model not in {normalized_model(data["model"]), normalized_model(data["model_slug"])}:
        return False
    for bound, key, minimum in (
        ("min_year", "year", True), ("max_year", "year", False),
        ("min_liters", "liters", True), ("max_liters", "liters", False),
        ("max_mileage", "mileage", False),
    ):
        target = filters.get(bound)
        if target is not None and (type(target) not in (int, float) or data[key] is None
                                   or not (data[key] >= target if minimum else data[key] <= target)):
            return False
    if specs is not None:
        return all(not filters.get(key) or specs.get(key) == filters[key] for key in ("engine", "transmission"))
    return True
