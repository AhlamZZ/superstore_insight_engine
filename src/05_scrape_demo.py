import os, json, sys, traceback
import requests
from pyquery import PyQuery as pq
from urllib.parse import urljoin, quote_plus

OUT_JSON = os.path.join("artifacts","reports","scraped_excerpt.json")

# === Target URLs (override with env vars if needed) ===
TIME_URL       = os.getenv("SCRAPE_TIME_URL",    "https://shop.timecenter.jo/collections/men-watches")
CITY_URL       = os.getenv("SCRAPE_CITY_URL",    "https://citycenter.jo/pc-and-laptops/pc-and-laptops-laptops")
READERS_URL    = os.getenv("SCRAPE_READERS_URL", "https://www.readers.jo/")
READERS_QUERY  = os.getenv("SCRAPE_READERS_QUERY", "book")  # used for fallback search

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.google.com/"
}

def log(m): print(m, flush=True)

def _clean(s: str) -> str:
    if not isinstance(s, str): return s
    return s.replace("\\xa0", " ").replace("\xa0", " ").replace("Â", "").strip()

def fetch(url, params=None):
    r = requests.get(url, headers=UA_HEADERS, timeout=12, params=params)
    r.raise_for_status()
    return r

# ---------- City Center ----------
def parse_citycenter_laptops():
    r = fetch(CITY_URL)
    doc = pq(r.text)
    items = []
    cards = doc("div.product, div.product-item, div.product-thumb, li.product, div.card, div.product-layout")
    for card in cards.items():
        if len(items) >= 8: break
        title = (card("h4 a").text()
                 or card("h3 a").text()
                 or card(".product-title a").text()
                 or card(".title a").text()
                 or card("a").attr("title"))
        price = (card(".price .price-new").text()
                 or card(".price").text()
                 or card(".product-price").text()
                 or card(".new-price").text()
                 or card(".amount").text())
        href  = card("h4 a").attr("href") or card("h3 a").attr("href") or card("a").attr("href") or ""
        if title:
            items.append({"title": _clean(title), "price": _clean(price), "href": urljoin(CITY_URL, href)})
    if not items:
        raise ValueError("No product cards parsed on City Center page.")
    return {"source": CITY_URL, "items_count": len(items), "items_preview": items[:8]}

# ---------- Time Center (Shopify JSON first, HTML fallback) ----------
def parse_timecenter_shopify():
    try:
        base = TIME_URL.split("?")[0].rstrip("/")
        prod_json_url = base + "/products.json" if "/collections/" in base else "https://shop.timecenter.jo/products.json"
        r = fetch(prod_json_url, params={"limit": 12})
        data = r.json()
        products = data.get("products", data if isinstance(data, list) else [])
        items = []
        for p in products[:8]:
            title = p.get("title")
            handle = p.get("handle")
            variants = p.get("variants") or []
            price = (variants[0].get("price") if variants else None)
            href = f"https://shop.timecenter.jo/products/{handle}" if handle else TIME_URL
            if title:
                items.append({"title": _clean(title), "price": _clean(str(price) if price is not None else ""), "href": href})
        if items:
            return {"source": prod_json_url, "items_count": len(items), "items_preview": items}
    except Exception:
        pass  # HTML fallback below

    r = fetch(TIME_URL)
    doc = pq(r.text)
    items = []
    cards = doc("article.product, article.product-card, div.product-item, div.grid__item, div.card")
    for card in cards.items():
        if len(items) >= 8: break
        title = (card("h3 a").attr("title") or card("h3 a").text()
                 or card(".card__heading a").text()
                 or card(".product-item__title a").text()
                 or card(".product-title a").text())
        price = (card(".price-item--regular").text()
                 or card(".price__current").text()
                 or card(".price .price-item").text()
                 or card(".money").text()
                 or card(".price").text())
        href  = card("a").attr("href") or ""
        if title:
            items.append({"title": _clean(title), "price": _clean(price), "href": urljoin(TIME_URL, href)})
    if not items:
        raise ValueError("No product items from Shopify JSON nor HTML on Time Center.")
    return {"source": TIME_URL, "items_count": len(items), "items_preview": items[:8]}

# ---------- Readers.jo (OpenCart selectors + JSON-LD + search fallback) ----------
def _readers_extract_from_doc(doc, base):
    items = []

    # 1) JSON-LD Product / ItemList
    for script in doc('script[type="application/ld+json"]').items():
        try:
            data = json.loads(script.text())
        except Exception:
            continue

        def push_product(obj):
            if not isinstance(obj, dict): return
            title = obj.get("name")
            offers = obj.get("offers") or {}
            price = None
            if isinstance(offers, dict):
                price = offers.get("price") or (offers.get("priceSpecification") or {}).get("price")
            url = obj.get("url") or ""
            if title:
                items.append({"title": _clean(title), "price": _clean(str(price) if price is not None else ""), "href": urljoin(base, url)})

        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict):
                    if entry.get("@type") == "Product":
                        push_product(entry)
                    elif entry.get("@type") == "ItemList":
                        for li in entry.get("itemListElement", []):
                            if isinstance(li, dict):
                                push_product(li.get("item") or li)
        elif isinstance(data, dict):
            if data.get("@type") == "Product":
                push_product(data)
            elif data.get("@type") == "ItemList":
                for li in data.get("itemListElement", []):
                    if isinstance(li, dict):
                        push_product(li.get("item") or li)

    # 2) HTML product cards (OpenCart patterns)
    if len(items) < 8:
        cards = doc("div.product-thumb, div.product-layout, li.product, div.product-grid-item, div.product")
        for card in cards.items():
            if len(items) >= 8: break
            title = (card(".caption h4 a").text()
                     or card("h4 a").text()
                     or card(".name a").text()
                     or card("h3 a").text()
                     or card("a").attr("title"))
            price = (card(".price .price-new").text()
                     or card(".price .price-old").text()
                     or card(".price").text()
                     or card(".product-price").text())
            href  = (card(".caption h4 a").attr("href")
                     or card("h4 a").attr("href")
                     or card("a").attr("href")
                     or "")
            if title:
                items.append({"title": _clean(title), "price": _clean(price), "href": urljoin(base, href)})

    return items

def parse_readers():
    # Try the provided URL first
    r = fetch(READERS_URL)
    doc = pq(r.text)
    items = _readers_extract_from_doc(doc, READERS_URL)

    # If nothing found, try site search (OpenCart search route)
    if not items:
        search_url = f"https://www.readers.jo/index.php?route=product/search&search={quote_plus(READERS_QUERY)}"
        r = fetch(search_url)
        doc = pq(r.text)
        items = _readers_extract_from_doc(doc, search_url)
        if items:
            return {"source": search_url, "items_count": len(items), "items_preview": items[:8], "note": f"Search fallback for '{READERS_QUERY}'"}
        else:
            raise ValueError("No product cards or JSON-LD products found on Readers (homepage and search).")

    return {"source": READERS_URL, "items_count": len(items), "items_preview": items[:8]}

def main():
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    out = {}

    steps = [
        ("citycenter", parse_citycenter_laptops),
        ("timecenter", parse_timecenter_shopify),
        ("readers", parse_readers),
    ]

    for name, fn in steps:
        try:
            log(f"running step: {name}")
            out[name] = fn()
            log(f"OK: {name}")
        except Exception as e:
            log(f"ERR: {name}: {type(e).__name__}: {e}")
            out[name + "_error"] = f"{type(e).__name__}: {e}"
            out[name + "_trace"] = traceback.format_exc()

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    log(f"saved -> {os.path.abspath(OUT_JSON)}")

if __name__ == "__main__":
    main()
