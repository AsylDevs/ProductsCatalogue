"""
Minimal cleaner:
- Trims text
- salesPrice -> digits only + '.00'
- primaryBarcode + extraBarcodes (digits only)
- Deduplicate by primaryBarcode (keep first)
- issues.csv with suspicious rows
Writes: cleaned.csv, issues.csv
"""

import re
import pandas as pd

INPUT  = "data_0_3.csv"
OUTPUT = "cleaned.csv"
ISSUES = "issues.csv"

VALID_BARCODE_LENGTHS = {8, 12, 13, 14}
SPLIT = re.compile(r"[,\s;]+")

def trim(s): return re.sub(r"\s+", " ", str(s)).strip()

def clean_price(v: str) -> str:
    digits = re.sub(r"\D", "", str(v or ""))
    return f"{digits}.00" if digits else ""

def extract_barcodes(raw: str):
    # split on commas/spaces/semicolons, keep digits only, dedupe (order-preserving)
    parts = [re.sub(r"\D", "", p) for p in SPLIT.split(trim(raw)) if p]
    seen, uniq = set(), []
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            uniq.append(p)
    primary = uniq[0] if uniq else ""
    extras  = uniq[1:] if len(uniq) > 1 else []
    return primary, extras, uniq

# --- load
df = pd.read_csv(INPUT, dtype=str, keep_default_na=False)

# Canonicalize expected columns (lightweight)
rename_map = {}
for col in df.columns:
    low = col.lower()
    if low == "name": rename_map[col] = "name"
    if low in ("barcode", "barcodes"): rename_map[col] = "barcode"
    if low in ("salesprice", "price"): rename_map[col] = "salesPrice"
    if low in ("uom", "unit"): rename_map[col] = "uom"
    if low in ("category", "cat"): rename_map[col] = "category"
if rename_map:
    df.rename(columns=rename_map, inplace=True)
for need in ["name","barcode","uom","salesPrice","category"]:
    if need not in df.columns: df[need] = ""

# Trim all cells
for c in df.columns:
    df[c] = df[c].map(trim)

# Prices
df["salesPrice"] = df["salesPrice"].map(clean_price)

# Barcodes
prim, extras_col, invalid_len, dup_of = [], [], [], []
seen_primary = {}

for i, raw in enumerate(df["barcode"]):
    primary, extras, all_codes = extract_barcodes(raw)
    prim.append(primary)
    extras_col.append(" ".join(extras))
    invalid_len.append(" ".join([b for b in all_codes if len(b) not in VALID_BARCODE_LENGTHS]))
    if primary and primary in seen_primary:
        dup_of.append(str(seen_primary[primary]))
    else:
        dup_of.append("")
        if primary:
            seen_primary[primary] = i

df["primaryBarcode"] = prim
df["extraBarcodes"] = extras_col
df["barcodeInvalidLengths"] = invalid_len
df["duplicateOf"] = dup_of

# Keep-first dedupe view (do NOT drop rows in issues.csv analysis)
clean = df[df["duplicateOf"] == ""].copy()

# Issues to review
def is_zero_or_blank(price: str) -> bool:
    return (price == "") or (price == "0.00")

issues = df[
    (df["primaryBarcode"] == "") |
    (df["barcodeInvalidLengths"] != "") |
    (df["duplicateOf"] != "") |
    (df["salesPrice"].map(is_zero_or_blank))
].copy()

# Order columns for convenience
front = ["name","primaryBarcode","extraBarcodes","uom","salesPrice","category","duplicateOf","barcodeInvalidLengths"]
rest = [c for c in clean.columns if c not in front]
clean[front + rest].to_csv(OUTPUT, index=False)
issues[front + rest].to_csv(ISSUES, index=False)

print(f"Done. Wrote {OUTPUT} (rows: {len(clean)}) and {ISSUES} (rows: {len(issues)})")
