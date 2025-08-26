"""
This script normalizes the 'salesPrice' column in a product CSV.

Operations:
1. Removes commas used as thousand separators, e.g. "1,215" → "1215".
2. Keeps only digits in salesPrice (drops stray letters/symbols).
3. Appends ".00" to all non-empty prices → "1215" → "1215.00".
4. Leaves all other columns unchanged.

Input:  data_0_2.csv
Output: data_0_3.csv
"""

import re
import pandas as pd

INPUT = "data_0_2.csv"
OUTPUT = "data_0_3.csv"

def clean_price(value: str) -> str:
    if not value:
        return value
    # Keep only digits
    digits = re.sub(r"\D", "", str(value))
    if digits:
        return f"{digits}.00"
    return ""

if __name__ == "__main__":
    df = pd.read_csv(INPUT, dtype=str, keep_default_na=False)
    if "salesPrice" not in df.columns:
        raise ValueError("CSV must contain 'salesPrice' column.")

    df["salesPrice"] = df["salesPrice"].apply(clean_price)
    df.to_csv(OUTPUT, index=False)
