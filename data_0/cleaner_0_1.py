"""
CSV Cleaner to assign product categories.

Steps:
1. Reads data_0_1.csv into a DataFrame.
2. Walks through rows:
   - If the row is a category header (e.g. "Молочная продукция"), 
     set current category = mapped enum from CATEGORY_MAP and mark
     that row’s category as empty.
   - Otherwise, assign the last seen category enum to the product.
3. After all rows are processed, fills a new 'category' column.
4. Drops pure category header rows (they have no barcode, productDesc, or brand).
   → Only product rows remain, each with its category filled.
5. Writes cleaned data to data_0_2.csv (UTF-8 with BOM).
"""


import pandas as pd

INPUT = "data_0_1.csv"
OUTPUT = "data_0_2.csv"

# Mapping Russian category names → enum
CATEGORY_MAP = {
    "Молочная продукция": "BAR_DAIRY",
    "Мясная консервация": "BAR_PACKAGED_FOOD",
    "Сигареты": "BAR_ALCOHOL_TOBACCO",
    "Химия": "BAR_CLEANING",
    "Вода": "BAR_BEVERAGES",
    "Натуральные соки,компоты": "BAR_BEVERAGES",
    "Овощная консервация": "BAR_PACKAGED_FOOD",
    "Кофе,Чай": "BAR_BEVERAGES",
    "Мука,макарны,крупы": "BAR_STAPLES",
    "Хлеб,хлебобулочная продукция": "BAR_BAKERY",
    "Кондитерка": "BAR_SNACKS",
    "Специи,растит масла": "BAR_STAPLES",
    "Напитки сладкие,холод.чай,энергетики": "BAR_BEVERAGES",
    "Детское питание": "BAR_BABY_PRODUCTS",
    "Канц товары,1000 мелочей,Игрушки": "BAR_OTHER",
    "Мороженое,заморозка": "BAR_SNACKS",
    "Яйца": "BAR_DAIRY",  # grouped under dairy
    "Колбасные изделия": "BAR_PACKAGED_FOOD",
    "Алкогольная продукция": "BAR_ALCOHOL_TOBACCO",
    "Снеки": "BAR_SNACKS",
    "Продукты быстрого приготовления": "BAR_PACKAGED_FOOD",
    "Диет питание,Продукты правильного питания": "BAR_OTHER",
    "Продукты готовые к употреблению": "BAR_PACKAGED_FOOD",
    "Кассовая зона": "BAR_OTHER",
    "АКЦИЯ": "BAR_OTHER",
    "Мед,Варенье,Джемы": "BAR_STAPLES",
    "Рыбная консервация": "BAR_PACKAGED_FOOD",
    "Бумажная продукция": "BAR_HOUSEHOLD",
    "Корм для животных": "BAR_PET_SUPPLIES",
    "Удаленные товары": "BAR_OTHER",
    "Посуда": "BAR_OTHER",
    "Мясо ,мясные продукты": "BAR_PACKAGED_FOOD",
}

def main():
    df = pd.read_csv(INPUT, dtype=str).fillna("")

    current_cat = None
    categories = []

    for _, row in df.iterrows():
        name = str(row["name"]).strip()

        # Detect category rows (only category text, others empty)
        if name in CATEGORY_MAP.keys():
            current_cat = CATEGORY_MAP[name]
            categories.append("")  # leave category rows themselves empty
        else:
            categories.append(current_cat)

    df["category"] = categories

    # Drop pure category rows (optional: if you want only products left)
    df = df[df["barcode"].ne("") | df["productDesc"].ne("") | df["brand"].ne("")]

    df.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
    print(f"[OK] Wrote {OUTPUT}")

if __name__ == "__main__":
    main()
