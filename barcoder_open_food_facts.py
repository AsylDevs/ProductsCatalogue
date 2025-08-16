# fill_barcodes.py
import csv, os, json, requests
from openai import OpenAI

INPUT_FILE = "barcodes.csv"
OUTPUT_FILE = "barcodes_filled.csv"

CATEGORIES = [
    "01 BAR_BEVERAGES", "02 BAR_SNACKS", "03 BAR_PACKAGED_FOOD",
    "04 BAR_HOUSEHOLD", "05 BAR_PERSONAL_CARE", "06 BAR_CLEANING",
    "07 BAR_BABY_PRODUCTS", "08 BAR_PET_SUPPLIES", "09 BAR_DAIRY",
    "10 BAR_BAKERY BAR_STAPLES", "11 BAR_ALCOHOL_TOBACCO", "12 BAR_OTHER"
]

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
            "quantity": p.get("quantity"),
            "labels": p.get("labels"),
            "image": p.get("image_url"),
        }
    except Exception:
        return None

def gpt_enrich(barcode: str, off: dict) -> dict | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    client = OpenAI(api_key=api_key)

    categories_str = "\n".join(CATEGORIES)
    off_name = (off.get("name") or "").strip()
    off_brand = (off.get("brand") or "").strip()
    off_cats = (off.get("categories") or "").strip()
    off_qty = (off.get("quantity") or "").strip()

    system_msg = (
        "Ты — ассистент по нормализации товарных карточек для розничной базы данных. "
        "Всегда отвечай ТОЛЬКО в JSON. Язык вывода: русский."
    )
    user_msg = f"""
barcode: {barcode}
off_name: {off_name}
off_brand: {off_brand}
off_categories_raw: {off_cats}
off_quantity: {off_qty}

Сделай:
1) name: короткое русское наименование (сохрани объём/вес).
2) brand: торговая марка/производитель.
3) category: один код из списка:
{categories_str}
4) productDesc: краткое нейтральное описание (<= 100 символов).
JSON формат:
{{
  "name": "...",
  "brand": "...",
  "category": "...",
  "productDesc": "..."
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
        data = json.loads(resp.choices[0].message.content)
        return {
            "name": (data.get("name") or "").strip(),
            "brand": (data.get("brand") or "").strip(),
            "category": (data.get("category") or "").strip(),
            "productDesc": (data.get("productDesc") or "").strip(),
        }
    except Exception as e:
        print("GPT error:", e)
        return None
def main():
    print(f"[START] reading {INPUT_FILE}")

    rows = []
    total = found = enriched_cnt = 0

    with open(INPUT_FILE, newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        fieldnames_in = reader.fieldnames or []
        print(f"[HEADER-IN] {fieldnames_in}")

        # ensure the output has our enriched columns
        required_cols = ["name", "productDesc", "category", "brand", "productImg"]
        fieldnames_out = list(fieldnames_in)
        for c in required_cols:
            if c not in fieldnames_out:
                fieldnames_out.append(c)

        for i, row in enumerate(reader, start=1):
            total += 1
            barcode = (row.get("barcode") or "").strip()
            if not barcode:
                print(f"[{i:04}] empty barcode → skip")
                rows.append(row)
                continue

            print(f"[{i:04}] {barcode} → querying OFF")
            off = fetch_openfoodfacts(barcode)

            if not off or not (off.get("name") or off.get("brand")):
                print(f"       NOT FOUND in OpenFoodFacts")
                rows.append(row)
                continue

            found += 1
            enriched = gpt_enrich(barcode, off) or {}
            if enriched:
                enriched_cnt += 1

            # merge (GPT > OFF > existing)
            row["name"] = (enriched.get("name") or off.get("name") or row.get("name") or "").strip()
            row["brand"] = (enriched.get("brand") or off.get("brand") or row.get("brand") or "").strip()
            row["category"] = (enriched.get("category") or row.get("category") or "").strip()
            row["productDesc"] = (enriched.get("productDesc") or row.get("productDesc") or "").strip()
            row["productImg"] = (off.get("image") or row.get("productImg") or "").strip()

            print(f"       SAVED name={row['name']!r}, brand={row['brand']!r}")
            rows.append(row)

    # write to OUTPUT_FILE (with extended header)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames_out, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            # make sure all required cols exist
            for c in required_cols:
                r.setdefault(c, "")
            writer.writerow(r)

    # final report
    print("\n========== REPORT ==========")
    print(f"Total barcodes processed : {total}")
    print(f"Found in OpenFoodFacts   : {found}")
    print(f"Enriched with GPT        : {enriched_cnt}")
    print(f"Saved to                 : {OUTPUT_FILE}")
    print("============================")


if __name__ == "__main__":
    main()