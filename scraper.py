"""
scraper.py — PropiedadesChile v2
Usa APIs internas y selectores más robustos.
Mercado Libre tiene API pública — es el más confiable.
"""
import json, time, random, logging, re
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("scraper")

PAGINAS = 3
DELAY = (3, 7)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

session = requests.Session()
session.headers.update(HEADERS)

def get_html(url):
    time.sleep(random.uniform(*DELAY))
    try:
        r = session.get(url, timeout=25)
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning(f"  Error GET: {e}")
        return None

def get_json(url, extra_headers=None):
    time.sleep(random.uniform(*DELAY))
    try:
        h = {**HEADERS, "Accept": "application/json"}
        if extra_headers:
            h.update(extra_headers)
        r = requests.get(url, headers=h, timeout=25)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"  Error JSON: {e}")
        return None

def num(texto):
    if not texto: return 0
    s = re.sub(r"[^\d]", "", str(texto))
    return int(s) if s else 0

def limpiar_precio(texto):
    if not texto: return 0
    s = re.sub(r"[^\d]", "", str(texto))
    return int(s) if s else 0

# ─── MERCADO LIBRE (API pública oficial) ─────────────────────────
def scrape_mercadolibre(tipo, propiedad):
    results = []
    log.info(f"[Mercado Libre] {tipo}/{propiedad}")

    cat_map = {
        "departamento": "MLC1459",
        "casa": "MLC1452",
        "oficina": "MLC1467",
        "local": "MLC1470",
        "terreno": "MLC1462",
    }
    op_map = {"arriendo": "rent", "venta": "sale"}

    cat_id = cat_map.get(propiedad, "MLC1459")
    op = op_map.get(tipo, "rent")

    for pag in range(1, PAGINAS + 1):
        offset = (pag - 1) * 48
        url = f"https://api.mercadolibre.com/sites/MLC/search?category={cat_id}&OPERATION={op}&offset={offset}&limit=48"
        data = get_json(url)

        if not data or "results" not in data:
            log.warning(f"  Sin resultados en pág {pag}")
            continue

        for item in data["results"]:
            try:
                precio = int(item.get("price", 0) or 0)
                moneda = item.get("currency_id", "CLP")
                foto = item.get("thumbnail", "").replace("-I.jpg", "-O.jpg")

                attrs = {"dormitorios": 0, "banos": 0, "superficie": 0}
                for a in item.get("attributes", []):
                    iid = a.get("id", "")
                    val = a.get("value_name", "") or ""
                    if "BEDROOM" in iid:
                        attrs["dormitorios"] = num(val)
                    elif "BATHROOM" in iid:
                        attrs["banos"] = num(val)
                    elif "TOTAL_AREA" in iid or "COVERED_AREA" in iid:
                        attrs["superficie"] = num(val)

                loc = item.get("location", {})
                ciudad = ""
                if isinstance(loc, dict):
                    ciudad = (loc.get("city") or {}).get("name", "") or \
                             (loc.get("state") or {}).get("name", "")

                prop = {
                    "titulo": item.get("title", ""),
                    "tipo": tipo,
                    "propiedad": propiedad,
                    "comuna": ciudad,
                    "precio": precio,
                    "uf": precio if moneda == "CLF" else None,
                    "portal": "Mercado Libre",
                    "url": item.get("permalink", ""),
                    "foto": foto,
                    "fecha": datetime.now().isoformat(),
                    "fecha_scraping": datetime.now().isoformat(),
                    **attrs,
                }
                if prop["titulo"] and prop["precio"] > 0:
                    results.append(prop)
            except Exception as e:
                log.debug(f"  item error: {e}")

        log.info(f"  pág {pag}: {len(results)} acumulados")

    return results


# ─── PORTAL INMOBILIARIO (HTML scraping) ─────────────────────────
def scrape_portal_inmobiliario(tipo, propiedad):
    results = []
    log.info(f"[Portal Inmobiliario] {tipo}/{propiedad}")

    for pag in range(1, PAGINAS + 1):
        url = f"https://www.portalinmobiliario.com/{tipo}/{propiedad}/_Desde_{(pag-1)*20+1}_NoIndex_True"
        r = get_html(url)
        if not r:
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        # Buscar datos en el JSON embebido en el HTML (más confiable que el HTML)
        scripts = soup.find_all("script", type="application/json")
        for script in scripts:
            try:
                data = json.loads(script.string or "")
                items = []
                if isinstance(data, dict):
                    items = (data.get("initialState", {})
                             .get("results", {})
                             .get("results", []))
                for item in items:
                    try:
                        precio = int(item.get("price", {}).get("amount", 0) or 0)
                        prop = {
                            "titulo": item.get("title", ""),
                            "tipo": tipo, "propiedad": propiedad,
                            "comuna": item.get("location", {}).get("city", {}).get("name", ""),
                            "precio": precio,
                            "uf": None,
                            "dormitorios": 0, "banos": 0, "superficie": 0,
                            "portal": "Portal Inmobiliario",
                            "url": item.get("permalink", ""),
                            "foto": item.get("thumbnail", ""),
                            "fecha": datetime.now().isoformat(),
                            "fecha_scraping": datetime.now().isoformat(),
                        }
                        for a in item.get("attributes", []):
                            iid = a.get("id", "")
                            val = a.get("value_name", "")
                            if "BEDROOM" in iid: prop["dormitorios"] = num(val)
                            elif "BATHROOM" in iid: prop["banos"] = num(val)
                            elif "AREA" in iid: prop["superficie"] = num(val)
                        if prop["titulo"] and prop["precio"] > 0:
                            results.append(prop)
                    except: pass
            except: pass

        # Fallback: selectores HTML directos
        if not results:
            cards = soup.select("li.ui-search-layout__item, .ui-search-result__wrapper")
            for card in cards:
                try:
                    titulo = card.select_one("[class*='title']") or card.select_one("h2")
                    precio_el = card.select_one("[class*='price-tag-fraction']") or card.select_one("[class*='price']")
                    link = card.select_one("a[href]")
                    img = card.select_one("img[data-src], img[src]")
                    ubicacion = card.select_one("[class*='location']")
                    texto = card.get_text(" ", strip=True)
                    dorm = re.search(r"(\d+)\s*dorm", texto, re.I)
                    bano = re.search(r"(\d+)\s*baño", texto, re.I)
                    sup  = re.search(r"(\d+)\s*m[²2]", texto)
                    prop = {
                        "titulo": titulo.get_text(strip=True) if titulo else "",
                        "tipo": tipo, "propiedad": propiedad,
                        "comuna": ubicacion.get_text(strip=True) if ubicacion else "",
                        "precio": limpiar_precio(precio_el.get_text() if precio_el else "0"),
                        "uf": None,
                        "dormitorios": int(dorm.group(1)) if dorm else 0,
                        "banos": int(bano.group(1)) if bano else 0,
                        "superficie": int(sup.group(1)) if sup else 0,
                        "portal": "Portal Inmobiliario",
                        "url": link["href"] if link else "",
                        "foto": (img.get("data-src") or img.get("src","")) if img else "",
                        "fecha": datetime.now().isoformat(),
                        "fecha_scraping": datetime.now().isoformat(),
                    }
                    if prop["titulo"] and prop["precio"] > 0:
                        results.append(prop)
                except: pass

        log.info(f"  pág {pag}: {len(results)} acumulados")

    return results


# ─── YAPO ─────────────────────────────────────────────────────────
def scrape_yapo(tipo, propiedad):
    results = []
    log.info(f"[Yapo] {tipo}/{propiedad}")

    cat_map = {
        "departamento": "departamentos_y_flats",
        "casa": "casas",
        "oficina": "oficinas_y_locales",
        "local": "oficinas_y_locales",
        "terreno": "terrenos_y_parcelas",
    }
    cat = cat_map.get(propiedad, "departamentos_y_flats")
    op_param = "arriendo" if tipo == "arriendo" else "venta"

    for pag in range(1, PAGINAS + 1):
        url = f"https://www.yapo.cl/region_metropolitana/{cat}?real_estate_operation={op_param}&page={pag}"
        r = get_html(url)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")

        cards = (soup.select("article.listing-card") or
                 soup.select("[class*='listing-card']") or
                 soup.select(".ad-listing-item") or
                 soup.select("li[data-ad-id]"))

        for card in cards:
            try:
                titulo = card.select_one("h2, h3, [class*='title']")
                precio_el = card.select_one("[class*='price']")
                link = card.select_one("a[href]")
                img = card.select_one("img")
                ubicacion = card.select_one("[class*='location'], [class*='commune']")

                href = link["href"] if link else ""
                if href and not href.startswith("http"):
                    href = "https://www.yapo.cl" + href

                foto = ""
                if img:
                    foto = img.get("data-lazy") or img.get("data-src") or img.get("src", "")

                texto = card.get_text(" ", strip=True)
                dorm = re.search(r"(\d+)\s*dorm", texto, re.I)
                bano = re.search(r"(\d+)\s*baño", texto, re.I)
                sup  = re.search(r"(\d+)\s*m[²2]", texto)

                prop = {
                    "titulo": titulo.get_text(strip=True) if titulo else "",
                    "tipo": tipo, "propiedad": propiedad,
                    "comuna": ubicacion.get_text(strip=True) if ubicacion else "Región Metropolitana",
                    "precio": limpiar_precio(precio_el.get_text() if precio_el else "0"),
                    "uf": None,
                    "dormitorios": int(dorm.group(1)) if dorm else 0,
                    "banos": int(bano.group(1)) if bano else 0,
                    "superficie": int(sup.group(1)) if sup else 0,
                    "portal": "Yapo",
                    "url": href,
                    "foto": foto,
                    "fecha": datetime.now().isoformat(),
                    "fecha_scraping": datetime.now().isoformat(),
                }
                if prop["titulo"] and prop["precio"] > 0:
                    results.append(prop)
            except: pass

        log.info(f"  pág {pag}: {len(results)} acumulados")

    return results


# ─── TOCTOC ───────────────────────────────────────────────────────
def scrape_toctoc(tipo, propiedad):
    results = []
    log.info(f"[TocToc] {tipo}/{propiedad}")

    prop_map = {
        "departamento": "departamento",
        "casa": "casa",
        "oficina": "oficina",
        "local": "local-comercial",
        "terreno": "terreno",
    }
    prop_url = prop_map.get(propiedad, "departamento")

    for pag in range(1, PAGINAS + 1):
        url = f"https://www.toctoc.com/{tipo}/{prop_url}?page={pag}"
        r = get_html(url)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")

        # Buscar JSON embebido
        for script in soup.find_all("script"):
            txt = script.string or ""
            if "listings" in txt or "propiedades" in txt:
                try:
                    match = re.search(r'"listings"\s*:\s*(\[.*?\])', txt, re.DOTALL)
                    if match:
                        items = json.loads(match.group(1))
                        for item in items:
                            try:
                                prop = {
                                    "titulo": item.get("title", item.get("titulo", "")),
                                    "tipo": tipo, "propiedad": propiedad,
                                    "comuna": item.get("commune", item.get("comuna", "")),
                                    "precio": int(item.get("price", item.get("precio", 0)) or 0),
                                    "uf": item.get("uf_price"),
                                    "dormitorios": int(item.get("bedrooms", item.get("dormitorios", 0)) or 0),
                                    "banos": int(item.get("bathrooms", item.get("banos", 0)) or 0),
                                    "superficie": int(item.get("total_area", item.get("superficie", 0)) or 0),
                                    "portal": "TocToc",
                                    "url": item.get("url", item.get("link", "")),
                                    "foto": item.get("main_image", item.get("photo", "")),
                                    "fecha": datetime.now().isoformat(),
                                    "fecha_scraping": datetime.now().isoformat(),
                                }
                                if prop["titulo"] and prop["precio"] > 0:
                                    results.append(prop)
                            except: pass
                except: pass

        # Fallback HTML
        cards = (soup.select("[class*='PropertyCard']") or
                 soup.select("[class*='property-card']") or
                 soup.select("article"))

        for card in cards:
            try:
                titulo = card.select_one("h2, h3, [class*='Title'], [class*='title']")
                precio_el = card.select_one("[class*='Price'], [class*='price']")
                link = card.select_one("a[href]")
                img = card.select_one("img")
                ubicacion = card.select_one("[class*='Location'], [class*='location'], [class*='commune']")

                href = link["href"] if link else ""
                if href and not href.startswith("http"):
                    href = "https://www.toctoc.com" + href

                texto = card.get_text(" ", strip=True)
                dorm = re.search(r"(\d+)\s*dorm", texto, re.I)
                bano = re.search(r"(\d+)\s*baño", texto, re.I)
                sup  = re.search(r"(\d+)\s*m[²2]", texto)

                prop = {
                    "titulo": titulo.get_text(strip=True) if titulo else "",
                    "tipo": tipo, "propiedad": propiedad,
                    "comuna": ubicacion.get_text(strip=True) if ubicacion else "",
                    "precio": limpiar_precio(precio_el.get_text() if precio_el else "0"),
                    "uf": None,
                    "dormitorios": int(dorm.group(1)) if dorm else 0,
                    "banos": int(bano.group(1)) if bano else 0,
                    "superficie": int(sup.group(1)) if sup else 0,
                    "portal": "TocToc",
                    "url": href,
                    "foto": (img.get("src") or img.get("data-src","")) if img else "",
                    "fecha": datetime.now().isoformat(),
                    "fecha_scraping": datetime.now().isoformat(),
                }
                if prop["titulo"] and prop["precio"] > 0:
                    results.append(prop)
            except: pass

        log.info(f"  pág {pag}: {len(results)} acumulados")

    return results


# ─── MOTOR PRINCIPAL ──────────────────────────────────────────────
SCRAPERS = [
    ("Mercado Libre",        scrape_mercadolibre),
    ("Portal Inmobiliario",  scrape_portal_inmobiliario),
    ("Yapo",                 scrape_yapo),
    ("TocToc",               scrape_toctoc),
]

COMBOS = [
    ("arriendo", "departamento"),
    ("arriendo", "casa"),
    ("venta",    "departamento"),
    ("venta",    "casa"),
    ("arriendo", "oficina"),
]

def correr():
    log.info("=" * 55)
    log.info(f"SCRAPING INICIADO — {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log.info("=" * 55)

    todas = []
    for nombre, fn in SCRAPERS:
        for tipo, prop in COMBOS:
            try:
                r = fn(tipo, prop)
                todas.extend(r)
                log.info(f"✓ {nombre} {tipo}/{prop}: {len(r)}")
            except Exception as e:
                log.error(f"✗ {nombre} {tipo}/{prop}: {e}")

    # Deduplicar
    vistas, unicas = set(), []
    for p in todas:
        key = p.get("url","") or p.get("titulo","")
        if key and key not in vistas:
            vistas.add(key)
            unicas.append(p)

    for i, p in enumerate(unicas, 1):
        p["id"] = i

    log.info(f"\nTOTAL FINAL: {len(unicas)} propiedades únicas")

    out = Path("propiedades.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(unicas, f, ensure_ascii=False, separators=(",",":"))

    log.info(f"✓ Guardado: {out.resolve()}")
    log.info("=" * 55)
    return unicas

if __name__ == "__main__":
    correr()
