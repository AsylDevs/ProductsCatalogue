# CodeSnippets/barcodes_cleaner_1.py
# Removes QR-code rows AND rows with empty/missing barcodes
# Outputs are always written into ../csvs/
# Ensures final columns: file, symbology, barcode, status

import argparse
from pathlib import Path
import pandas as pd

def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    csvs_dir = project_root / "csvs"
    csvs_dir.mkdir(exist_ok=True)

    default_input = project_root / "barcodes.csv"
    default_cleaned = csvs_dir / "barcodes_clean_1.csv"
    default_removed = csvs_dir / "barcodes_removed.csv"

    ap = argparse.ArgumentParser(
        description="Remove QR-code rows and rows with empty/missing barcodes."
    )
    ap.add_argument("--input", default=str(default_input),
                    help=f"Input CSV path (default: {default_input})")
    ap.add_argument("--cleaned-out", default=str(default_cleaned),
                    help="Output CSV for cleaned data (default: csvs/barcodes_clean_1.csv)")
    ap.add_argument("--removed-out", default=str(default_removed),
                    help="Output CSV for removed rows (default: csvs/barcodes_removed.csv)")
    args = ap.parse_args()

    in_path = Path(args.input).resolve()
    if not in_path.exists():
        raise FileNotFoundError(f"Input not found: {in_path}")

    cleaned_out = Path(args.cleaned_out).resolve()
    removed_out = Path(args.removed_out).resolve()

    df = pd.read_csv(in_path, dtype=str, keep_default_na=False)

    required_cols = {"file", "symbology", "value", "status"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required column(s): {', '.join(sorted(missing))}")

    # Normalize columns
    df["symbology"] = df["symbology"].fillna("").str.strip()
    df["value"] = df["value"].fillna("").str.strip()

    # Masks
    qr_mask = df["symbology"].str.lower().isin({"qrcode", "qr code", "qr-code"})
    empty_mask = df["value"].eq("")

    removed = df[qr_mask | empty_mask].copy()
    cleaned = df[~(qr_mask | empty_mask)].copy()

    # 🔑 Rename "value" → "barcode"
    cleaned = cleaned.rename(columns={"value": "barcode"})
    removed = removed.rename(columns={"value": "barcode"})

    cleaned.to_csv(cleaned_out, index=False)
    removed.to_csv(removed_out, index=False)

    print(f"[OK] Input: {in_path}")
    print(f"[OK] Total rows: {len(df)}")
    print(f"[OK] Removed rows: {len(removed)} → {removed_out}")
    print(f"[OK] Cleaned rows: {len(cleaned)} → {cleaned_out}")

if __name__ == "__main__":
    main()
