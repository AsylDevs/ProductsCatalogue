# pip install requests beautifulsoup4 python-slugify
import sys, re, requests, html
from bs4 import BeautifulSoup

CATEGORIES = [
    "01 BAR_BEVERAGES",
    "02 BAR_SNACKS",
    "03 BAR_PACKAGED_FOOD",
    "04 BAR_HOUSEHOLD",
    "05 BAR_PERSONAL_CARE",
    "06 BAR_CLEANING",
    "07 BAR_BABY_PRODUCTS",
    "08 BAR_PET_SUPPLIES",
    "09 BAR_DAIRY",
    "10 BAR_BAKERY BAR_STAPLES",
    "11 BAR_ALCOHOL_TOBACCO",
    "12 BAR_OTHER",
]

# --- OpenFoodFacts (JSON) ---
def fetch_openfoodfacts(barcode: str) -> dict | None:
    url = f"https://world.openfoodfacts.org/api/v0/product/{barcode}.json"
    try:
        r = requests.get(url, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json()
        if data.get("status") != 1:
            return None
        p = data.get("product", {}) or {}
        return {
            "name": p.get("product_name") or p.get("generic_name"),
            "brand": p.get("brands"),
            "categories": p.get("categories"),
            "image": p.get("image_url"),
            "source": "openfoodfacts",
        }
    except Exception:
        return None

# --- barcode-list.ru helpers ---
def _parse_barcode_list_product_page(soup, barcode: str):
    import re, html as _html
    h1 = soup.find("h1")
    title = soup.title.string if soup.title else ""
    og = soup.find("meta", attrs={"property": "og:title"})
    name = (h1.get_text(strip=True) if h1 else "") or (og.get("content", "").strip() if og else "") or (title or "")
    name = _html.unescape(name).strip()
    if name:
        name = re.sub(rf"^\s*Штрихкод\s*(?:{re.escape(barcode)}|\d+)\s*[-—:]\s*", "", name, flags=re.I).strip()
    if name and name.lower() in {"штрихкод", "barcode"}:
        name = None

    brand = None
    for row in soup.select("table tr"):
        th = row.find("th"); td = row.find("td")
        if th and td:
            key = th.get_text(" ", strip=True).lower()
            val = td.get_text(" ", strip=True)
        else:
            cells = [c.get_text(" ", strip=True) for c in row.find_all(["td","th"])]
            if len(cells) < 2:
                continue
            key, val = cells[0].lower(), cells[1]
        if not brand and any(k in key for k in ["производитель", "бренд", "торговая марка"]):
            brand = val.strip() or brand

    return {"name": name or None, "brand": brand or None, "categories": None, "image": None}

def _parse_barcode_list_search_page(soup, barcode: str):
    """
    Parse https://barcode-list.ru/barcode/RU/Поиск.htm?barcode=<barcode>
    Find the row where a <td> equals the barcode and return the next cell as name.
    """
    import re
    def digits(s: str) -> str:
        return re.sub(r"\D+", "", s or "")

    wanted = digits(barcode)
    name = None

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            for i, td in enumerate(tds):
                cell = td.get_text(" ", strip=True)
                if digits(cell) == wanted:
                    if i + 1 < len(tds):
                        name = tds[i + 1].get_text(" ", strip=True)
                    elif i + 2 < len(tds):
                        name = tds[i + 2].get_text(" ", strip=True)
                    break
            if name:
                break
        if name:
            break

    if name and digits(name) == name:
        name = None

    return {"name": name or None, "brand": None, "categories": None, "image": None}

def fetch_barcode_list_ru(barcode: str) -> dict | None:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "ru,en;q=0.8"
    }
    # 1) Direct product page
    try:
        url_direct = f"https://barcode-list.ru/barcode/{barcode}/"
        r = requests.get(url_direct, headers=headers, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            data = _parse_barcode_list_product_page(soup, barcode)
            if data.get("name"):
                return data
    except Exception:
        pass

    # 2) Search page
    try:
        url_search = "https://barcode-list.ru/barcode/RU/%D0%9F%D0%BE%D0%B8%D1%81%D0%BA.htm"
        rs = requests.get(url_search, headers=headers, params={"barcode": barcode}, timeout=10)
        if rs.status_code != 200:
            return None
        if not rs.encoding or rs.encoding.lower() in {"iso-8859-1", "latin-1"}:
            rs.encoding = "cp1251"
        soup_s = BeautifulSoup(rs.text, "html.parser")
        return _parse_barcode_list_search_page(soup_s, barcode)
    except Exception:
        return None

# --- formatting & glue ---
def build_table(barcode: str, name: str | None, desc: str | None, cat: str | None, brand: str | None, img: str | None) -> str:
    return (
        "| Поле           | Значение |\n"
        "|----------------|----------|\n"
        f"| barcode        | {barcode} |\n"
        f"| name           | {name if name else 'null'} |\n"
        f"| productDesc    | {desc if desc else 'null'} |\n"
        f"| category       | {cat if cat else 'null'} |\n"
        f"| brand          | {brand if brand else 'null'} |\n"
        f"| productImg     | {img if img else 'null'} |\n"
    )

def make_desc(name: str | None, brand: str | None) -> str | None:
    if name and name.isdigit():
        name = None
    if not name and not brand:
        return None
    if name and brand:
        return f"{name} — {brand}"
    return name or brand

def process_barcode(barcode: str) -> str:
    #data = fetch_openfoodfacts(barcode) or fetch_barcode_list_ru(barcode) or {}
    data = fetch_barcode_list_ru(barcode) or {}

    name = (data or {}).get("name")
    brand = (data or {}).get("brand")
    image = (data or {}).get("image")

    # category left empty
    category = None
    desc = make_desc(name, brand)
    img = image if image else None

    return build_table(barcode, name, desc, category, brand, img)

def main():
    args = sys.argv[1:]
    if not args:
        args = ["5011007015534"]
    tables = [process_barcode(b.strip()) for b in args if b.strip()]
    print("\n".join(tables))

if __name__ == "__main__":
    main()
