import re
import pandas as pd

# --- settings ---
INPUT  = "barcodes.csv"
OUTPUT = "barcodes_clean.csv"

# 1) Read as text
df = pd.read_csv(INPUT, dtype=str, keep_default_na=False)

# 2) Keep only needed columns, rename
df = df[['file', 'value']].rename(columns={'value': 'barcode'})
df['barcode'] = df['barcode'].astype(str).str.strip()

# 3) Drop rows with empty barcode OR barcode that looks like a link
#    (http://, https://, ftp://, www., or something like foo.tld/…)
url_regex = re.compile(
    r'^\s*(?:https?://|ftp://|www\.)|[A-Za-z0-9-]+\.[A-Za-z]{2,}(/|$)',
    re.IGNORECASE
)

mask_has_barcode = df['barcode'].ne("")
mask_not_url = ~df['barcode'].str.contains(url_regex, na=False)
filtered = df[mask_has_barcode & mask_not_url].copy()

# 4) Add required empty columns + productImg
for col in ['name', 'productDesc', 'category', 'brand']:
    filtered[col] = ""

filtered['productImg'] = filtered['barcode'] + ".png"

# 5) Save
filtered.to_csv(OUTPUT, index=False)

print(f"Input rows:    {len(df)}")
print(f"Kept rows:     {len(filtered)}")
print(f"Dropped rows:  {len(df) - len(filtered)}")
print(f"Saved to:      {OUTPUT}")
