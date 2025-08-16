# pip install requests beautifulsoup4 openai
import sys, requests, json, os

from bs4 import BeautifulSoup  # kept only because you already had it; not used now
from openai import OpenAI

# Fixed category set (GPT must choose exactly one code from this list)
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

# -------- OpenFoodFacts (JSON, ODbL) --------
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
        # Only pass what we’ll use
        return {
            "name": p.get("product_name") or p.get("generic_name"),
            "brand": p.get("brands"),
            "categories": p.get("categories"),
            "quantity": p.get("quantity"),
            "labels": p.get("labels"),
            "image": p.get("image_url"),
        }
    except Exception:
        return None

# -------- ChatGPT enrichment (ONLY if OFF found product) --------
def gpt_enrich(barcode: str, off: dict) -> dict | None:
    """
    Uses ChatGPT to:
      - translate/normalize name to Russian (keep useful size/volume),
      - improve brand/producer,
      - pick one category code from CATEGORIES,
      - produce a short neutral productDesc (<= 100 chars).
    Returns dict with keys: name, brand, category, productDesc.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        # No API key -> skip enrichment
        return None

    client = OpenAI(api_key=api_key)

    # Build a compact, deterministic prompt
    categories_str = "\n".join(CATEGORIES)
    off_name = (off.get("name") or "").strip()
    off_brand = (off.get("brand") or "").strip()
    off_cats = (off.get("categories") or "").strip()
    off_qty = (off.get("quantity") or "").strip()
    off_labels = (off.get("labels") or "").strip()

    system_msg = (
        "Ты — ассистент по нормализации товарных карточек для розничной базы данных. "
        "Всегда отвечай ТОЛЬКО в JSON. Язык вывода: русский."
    )
    user_msg = f"""
Дано (из OpenFoodFacts). Если данных не хватает — делай лучшую безопасную нормализацию без выдумок.

barcode: {barcode}
off_name: {off_name}
off_brand: {off_brand}
off_categories_raw: {off_cats}
off_quantity: {off_qty}
off_labels: {off_labels}

Требования:
1) name: короткое русское наименование для мобильного приложения, сохраняй полезный объём/вес, например:
   - Шампиньоны "Mikado" – 400 г
   - Виски "Jameson" 0,5 л
   Правила:
   - Переведи бренд/тип продукта при необходимости (Irish Whiskey -> Виски).
   - Используй кавычки для бренда в имени, если бренд есть: Виски "Jameson" 0,5 л.
   - Разделяй тире/пробелы аккуратно: '–' допустимо, но без лишней болтовни.
   - Допускай сокращения л/мл/г/кг. Десятичные дроби с запятой: 0,5 л; 425 г.
   - Не добавляй маркетинговые слова (премиум, лучший и т.п.).

2) brand: кратко, без мусора (только торговая марка/производитель, если известен из OFF).

3) category: выбери строго ОДИН код из списка ниже (верни строку целиком — код + имя):
{categories_str}

Подсказки по категоризации (необязательно исчерпывающие):
- Виски, водка, пиво, вино -> 11 BAR_ALCOHOL_TOBACCO
- Вода, соки, чай, кофе -> 01 BAR_BEVERAGES
- Молоко, йогурт, сыр -> 09 BAR_DAIRY
- Хлеб, мука, рис, макароны -> 10 BAR_BAKERY BAR_STAPLES
- Чипсы, шоколад, печенье -> 02 BAR_SNACKS
- Консервы, соусы, каши -> 03 BAR_PACKAGED_FOOD
- Детское питание/подгузники -> 07 BAR_BABY_PRODUCTS
- Корм для животных -> 08 BAR_PET_SUPPLIES
- Бытовая химия -> 06 BAR_CLEANING
- Личная гигиена -> 05 BAR_PERSONAL_CARE
- Товары для дома -> 04 BAR_HOUSEHOLD
- Иное -> 12 BAR_OTHER

4) productDesc: краткое нейтральное описание на русском (<= 100 символов), без рекламы.

Верни JSON строго в формате:
{{
  "name": "…",
  "brand": "…",
  "category": "…",
  "productDesc": "…"
}}
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            temperature=0.2,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
        )
        content = resp.choices[0].message.content
        data = json.loads(content)
        # Defensive cleanup
        out = {
            "name": (data.get("name") or "").strip() or None,
            "brand": (data.get("brand") or "").strip() or None,
            "category": (data.get("category") or "").strip() or None,
            "productDesc": (data.get("productDesc") or "").strip() or None,
        }
        return out
    except Exception:
        return None

# -------- Formatting & glue --------
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

def process_barcode(barcode: str) -> str:
    # 1) OFF lookup
    off = fetch_openfoodfacts(barcode)

    # If not found in DBs -> return nulls
    if not off or not (off.get("name") or off.get("brand")):
        return build_table(barcode, None, None, None, None, None)

    # 2) Enrich via GPT (only because OFF returned something)
    enriched = gpt_enrich(barcode, off) or {}

    # Decide final fields with fallback to OFF
    name = enriched.get("name") or off.get("name")
    brand = enriched.get("brand") or off.get("brand")
    category = enriched.get("category") or None
    desc = enriched.get("productDesc") or None
    img = off.get("image") or None

    return build_table(barcode, name, desc, category, brand, img)

def main():
    args = sys.argv[1:]
    if not args:
        args = ["4870006610321"]
    tables = [process_barcode(b.strip()) for b in args if b.strip()]
    print("\n".join(tables))

if __name__ == "__main__":
    main()
