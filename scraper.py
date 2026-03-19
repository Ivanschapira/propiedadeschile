"""
scraper.py — PropiedadesChile
Raspa Portal Inmobiliario, Yapo, TocToc y Mercado Libre.
Genera propiedades.json para el sitio web.
"""
import json, time, random, logging, re
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("scraper")

PAGINAS_POR_PORTAL = 3   # Aumentar para más resultados (más lento)
DELAY = (2, 5)           # Segundos entre requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "es-CL,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}

def get(url):
    time.sleep(random.uniform(*DELAY))
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning(f"  Error: {e}")
        return None

def num(texto):
    if not texto: return 0
    s = re.sub(r"[^\d]", "", str(texto))
    return int(s) if s else 0

# ─── PORTAL INMOBILIARIO ──────────────────────────────────────────
def scrape_pi(tipo, propiedad):
    results = []
    base = "https://www.portalinmobiliario.com"
    log.info(f"[Portal Inmobiliario] {tipo} / {propiedad}")
    for pag in range(1, PAGINAS_POR_PORTAL + 1):
        url = f"{base}/{tipo}/{propiedad}/_Desde_{(pag-1)*20+1}_NoIndex_True"
        r = get(url)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".ui-search-result__wrapper, .andes-card")
        for card in cards:
            try:
                titulo = card.select_one(".ui-search-item__title, h2")
                precio_el = card.select_one(".price-tag-fraction")
                ubicacion = card.select_one(".ui-search-item__location, .ui-search-item__group--location")
                link = card.select_one("a[href]")
                img = card.select_one("img[data-src], img[src]")
                dorm_el = [a for a in card.select(".ui-search-card-attributes__attribute") if "dorm" in a.get_text().lower()]
                bano_el = [a for a in card.select(".ui-search-card-attributes__attribute") if "baño" in a.get_text().lower()]
                sup_el  = [a for a in card.select(".ui-search-card-attributes__attribute") if "m²" in a.get_text()]
                moneda = card.select_one(".price-tag-symbol")
                es_uf = moneda and "UF" in moneda.get_text()
                precio = num(precio_el.get_text() if precio_el else "0")
                prop = {
                    "titulo": titulo.get_text(strip=True) if titulo else "",
                    "tipo": tipo, "propiedad": propiedad,
                    "comuna": ubicacion.get_text(strip=True) if ubicacion else "",
                    "precio": precio,
                    "uf": round(precio) if es_uf else None,
                    "dormitorios": num(dorm_el[0].get_text()) if dorm_el else 0,
                    "banos": num(bano_el[0].get_text()) if bano_el else 0,
                    "superficie": num(sup_el[0].get_text()) if sup_el else 0,
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

# ─── MERCADO LIBRE ────────────────────────────────────────────────
def scrape_ml(tipo, propiedad):
    results = []
    log.info(f"[Mercado Libre] {tipo} / {propiedad}")
    for pag in range(1, PAGINAS_POR_PORTAL + 1):
        offset = (pag - 1) * 48 + 1
        url = f"https://listado.mercadolibre.cl/{propiedad}-{tipo}/_Desde_{offset}_NoIndex_True"
        r = get(url)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".ui-search-result__wrapper")
        for card in cards:
            try:
                titulo = card.select_one(".ui-search-item__title")
                precio_el = card.select_one(".price-tag-fraction")
                ubicacion = card.select_one(".ui-search-item__location")
                link = card.select_one("a.ui-search-link")
                img = card.select_one("img[data-src], img[src]")
                attrs_text = " ".join(a.get_text() for a in card.select(".ui-search-card-attributes__attribute"))
                dorm = re.search(r"(\d+)\s*dorm", attrs_text, re.I)
                bano = re.search(r"(\d+)\s*baño", attrs_text, re.I)
                sup  = re.search(r"(\d+)\s*m²", attrs_text)
                prop = {
                    "titulo": titulo.get_text(strip=True) if titulo else "",
                    "tipo": tipo, "propiedad": propiedad,
                    "comuna": ubicacion.get_text(strip=True) if ubicacion else "",
                    "precio": num(precio_el.get_text() if precio_el else "0"),
                    "uf": None,
                    "dormitorios": int(dorm.group(1)) if dorm else 0,
                    "banos": int(bano.group(1)) if bano else 0,
                    "superficie": int(sup.group(1)) if sup else 0,
                    "portal": "Mercado Libre",
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
    cat_map = {"departamento": "departamentos", "casa": "casas", "oficina": "oficinas", "local": "locales-comerciales"}
    cat = cat_map.get(propiedad, "propiedades")
    log.info(f"[Yapo] {tipo} / {propiedad}")
    for pag in range(1, PAGINAS_POR_PORTAL + 1):
        url = f"https://www.yapo.cl/chile/{cat}?ad_type=offer&real_estate_operation={tipo}&page={pag}"
        r = get(url)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("article.listing-card, .ad-listing-item, li[data-ad-id]")
        for card in cards:
            try:
                titulo = card.select_one("h2, h3, .listing-card__title, [class*=title]")
                precio_el = card.select_one(".listing-card__price, [class*=price]")
                ubicacion = card.select_one(".listing-card__location, [class*=location]")
                link = card.select_one("a[href]")
                img = card.select_one("img[src], img[data-src]")
                href = (link["href"] if link else "")
                if href and not href.startswith("http"):
                    href = "https://www.yapo.cl" + href
                titulo_txt = titulo.get_text(strip=True) if titulo else ""
                dorm = re.search(r"(\d+)\s*dorm", titulo_txt, re.I)
                sup  = re.search(r"(\d+)\s*m²", titulo_txt)
                prop = {
                    "titulo": titulo_txt,
                    "tipo": tipo, "propiedad": propiedad,
                    "comuna": ubicacion.get_text(strip=True) if ubicacion else "",
                    "precio": num(precio_el.get_text() if precio_el else "0"),
                    "uf": None,
                    "dormitorios": int(dorm.group(1)) if dorm else 0,
                    "banos": 0,
                    "superficie": int(sup.group(1)) if sup else 0,
                    "portal": "Yapo",
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

# ─── TOCTOC ───────────────────────────────────────────────────────
def scrape_toctoc(tipo, propiedad):
    results = []
    log.info(f"[TocToc] {tipo} / {propiedad}")
    for pag in range(1, PAGINAS_POR_PORTAL + 1):
        url = f"https://www.toctoc.com/{tipo}/{propiedad}?page={pag}"
        r = get(url)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")
        # TocToc usa diferentes selectores según versión
        cards = soup.select("[class*=PropertyCard],[class*=property-card],[class*=listing-card],.result-item")
        for card in cards:
            try:
                titulo = card.select_one("h2,h3,[class*=title],[class*=Title]")
                precio_el = card.select_one("[class*=price],[class*=Price]")
                ubicacion = card.select_one("[class*=location],[class*=Location],[class*=address]")
                link = card.select_one("a[href]")
                img = card.select_one("img[src],img[data-src]")
                href = (link["href"] if link else "")
                if href and not href.startswith("http"):
                    href = "https://www.toctoc.com" + href
                card_txt = card.get_text()
                dorm = re.search(r"(\d+)\s*dorm", card_txt, re.I)
                bano = re.search(r"(\d+)\s*baño", card_txt, re.I)
                sup  = re.search(r"(\d+)\s*m²", card_txt)
                prop = {
                    "titulo": titulo.get_text(strip=True) if titulo else "",
                    "tipo": tipo, "propiedad": propiedad,
                    "comuna": ubicacion.get_text(strip=True) if ubicacion else "",
                    "precio": num(precio_el.get_text() if precio_el else "0"),
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
    ("Portal Inmobiliario", scrape_pi),
    ("Mercado Libre",       scrape_ml),
    ("Yapo",                scrape_yapo),
    ("TocToc",              scrape_toctoc),
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
                log.info(f"  ✓ {nombre} {tipo}/{prop}: {len(r)} propiedades")
            except Exception as e:
                log.error(f"  ✗ {nombre} {tipo}/{prop}: {e}")

    # Deduplicar por URL
    vistas, unicas = set(), []
    for p in todas:
        key = p.get("url","") or p.get("titulo","")
        if key and key not in vistas:
            vistas.add(key)
            unicas.append(p)

    # Asignar IDs
    for i, p in enumerate(unicas, 1):
        p["id"] = i

    log.info(f"\nTOTAL: {len(unicas)} propiedades únicas de {len(todas)} encontradas")

    # Guardar JSON
    out = Path("propiedades.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(unicas, f, ensure_ascii=False, separators=(",",":"))
    log.info(f"Guardado: {out.resolve()}")
    log.info("=" * 55)
    return unicas

if __name__ == "__main__":
    correr()
