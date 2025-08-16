# pip install requests beautifulsoup4 python-slugify
import sys, requests

# Categories list (kept for future, but category is always null for now)
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

# --- OpenFoodFacts (JSON, ODbL license) ---
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
        }
    except Exception:
        return None

# --- Formatting & glue ---
def build_table(barcode: str, name: str | None, desc: str | None,
                cat: str | None, brand: str | None, img: str | None) -> str:
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
    # Only OpenFoodFacts
    data = fetch_openfoodfacts(barcode) or {}

    name = (data or {}).get("name")
    brand = (data or {}).get("brand")
    image = (data or {}).get("image")

    category = None  # left null for now
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
