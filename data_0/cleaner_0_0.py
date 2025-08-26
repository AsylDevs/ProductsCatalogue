"""
CSV cleaner for product list.

What it does:
- Reads input CSV as strings (keeps empty cells), UTF‑8.
- Renames columns:
    'Номенклатура' -> 'name'
    'Штрихкоды'    -> 'barcode'
    'Ед.'          -> 'quantityUnitType'
    'Основной тип цен' -> 'salesPrice'
- Adds empty columns if missing: 'productDesc', 'category', 'brand', 'productImg'.
- Normalizes unit values in 'quantityUnitType':
    'кг' -> 'kg'
    'упак', 'упак.', 'уп', 'шт' -> 'pcs'
    blanks stay blank; unknown values kept as‑is.
- Removes the row where name == 'Профитроли' and barcode contains '2500606430013'.
- Saves to output CSV with UTF‑8‑SIG BOM.

Usage:
    INPUT  = "data_0_0.csv"
    OUTPUT = "data_0_1.csv"
    python script.py
"""


import re
import pandas as pd

# --- settings ---
INPUT  = "data_0_0.csv"   # source file
OUTPUT = "data_0_1.csv"   # destination file

UNIT_MAP = {
    "кг": "kg",
    "килограмм": "kg",
    "килограммы": "kg",
    "kg": "kg",
    "шт": "pcs",
    "штука": "pcs",
    "штуки": "pcs",
    "упак": "pcs",
    "упак.": "pcs",
    "уп": "pcs",
}

def normalize_unit(val: str) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    key = s.lower()
    return UNIT_MAP.get(key, s)  # leave unknown values unchanged

def main():
    # read CSV as strings
    df = pd.read_csv(INPUT, dtype=str, keep_default_na=False)

    # rename columns
    df = df.rename(columns={
        "Номенклатура": "name",
        "Штрихкоды": "barcode",
        "Ед.": "quantityUnitType",
        "Основной тип цен": "salesPrice",
    })

    # add new empty columns if missing
    for col in ["productDesc", "category", "brand", "productImg"]:
        if col not in df.columns:
            df[col] = ""

    # normalize unit types if column exists
    if "quantityUnitType" in df.columns:
        df["quantityUnitType"] = df["quantityUnitType"].apply(normalize_unit)

    # remove the specific row: name == "Профитроли" AND barcode contains 2500606430013
    if {"name", "barcode"}.issubset(df.columns):
        mask_remove = (
            df["name"].astype(str).str.strip().eq("Профитроли")
            & df["barcode"].astype(str).str.contains(r"\b2500606430013\b", na=False)
        )
        df = df[~mask_remove].copy()

    # save
    df.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved → {OUTPUT}")

if __name__ == "__main__":
    main()
