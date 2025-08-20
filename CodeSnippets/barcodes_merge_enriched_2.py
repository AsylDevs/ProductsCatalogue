# CodeSnippets/barcodes_merge_enriched.py
# Usage (from project root):
#   python CodeSnippets/barcodes_merge_enriched.py
# Optional:
#   python CodeSnippets/barcodes_merge_enriched.py --clean ./csvs/barcodes_clean_1.csv --filled ./Other/barcodes_filled.csv --out ./csvs/barcodes_enriched_2.csv

import argparse
from pathlib import Path
import pandas as pd

ENRICH_COLS = ["name", "productDesc", "category", "brand", "productImg"]

def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    csvs_dir = project_root / "csvs"
    csvs_dir.mkdir(exist_ok=True)

    default_clean  = csvs_dir / "barcodes_clean_1.csv"          # has: file,symbology,barcode,status
    default_filled = project_root / "Other" / "barcodes_filled.csv"  # has: file,symbology,barcode,status + enrich
    default_out    = csvs_dir / "barcodes_enriched_2.csv"
    default_audit  = csvs_dir / "barcodes_updated_from_filled.csv"

    ap = argparse.ArgumentParser(description="Merge enriched rows into barcodes_clean_1 and append enriched extras.")
    ap.add_argument("--clean",  default=str(default_clean),  help="Path to csvs/barcodes_clean_1.csv")
    ap.add_argument("--filled", default=str(default_filled), help="Path to Other/barcodes_filled.csv")
    ap.add_argument("--out",    default=str(default_out),    help="Output merged CSV (default: csvs/barcodes_enriched_2.csv)")
    ap.add_argument("--audit",  default=str(default_audit),  help="Audit CSV (default: csvs/barcodes_updated_from_filled.csv)")
    args = ap.parse_args()

    clean = pd.read_csv(args.clean, dtype=str, keep_default_na=False)
    filled = pd.read_csv(args.filled, dtype=str, keep_default_na=False)

    # --- Validate schemas ---
    # Clean must now have 'barcode' (not 'value')
    required_clean = {"file", "symbology", "barcode", "status"}
    missing_clean = required_clean - set(clean.columns)
    if missing_clean:
        # If user passed an old file with 'value', accept it by renaming
        if "value" in clean.columns and "barcode" not in clean.columns:
            clean = clean.rename(columns={"value": "barcode"})
        else:
            raise ValueError(f"{args.clean} missing column(s): {', '.join(sorted(missing_clean))}")

    required_filled = {"file", "symbology", "barcode", "status"} | set(ENRICH_COLS)
    missing_filled = required_filled - set(filled.columns)
    if missing_filled:
        raise ValueError(f"{args.filled} missing column(s): {', '.join(sorted(missing_filled))}")

    # --- Normalize ---
    clean["barcode"] = clean["barcode"].fillna("").astype(str).str.strip()
    filled["barcode"] = filled["barcode"].fillna("").astype(str).str.strip()
    for c in ENRICH_COLS:
        filled[c] = filled[c].fillna("").astype(str).str.strip()

    # Ignore QR code rows from the filled file
    sym_f = filled["symbology"].fillna("").str.strip().str.lower()
    filled = filled[~sym_f.isin({"qrcode", "qr code", "qr-code"})].copy()

    # Keep only actually enriched rows (any enrichment columns non-empty)
    enriched_rows = filled[filled[ENRICH_COLS].apply(lambda r: any(r.astype(str).str.strip() != ""), axis=1)].copy()

    # If nothing to enrich, just copy clean to out and exit
    if enriched_rows.empty:
        clean.to_csv(args.out, index=False)
        pd.DataFrame(columns=list(clean.columns) + ENRICH_COLS + ["action"]).to_csv(args.audit, index=False)
        print("[OK] No enriched rows found. Wrote clean data only.")
        return

    # Deduplicate by barcode (last occurrence wins for each enrichment field)
    enriched_rows = enriched_rows.iloc[::-1]  # reverse so last wins
    keep_cols_for_append = ["file", "symbology", "barcode", "status"] + ENRICH_COLS
    enriched_by_barcode_full = (
        enriched_rows[keep_cols_for_append]
        .groupby("barcode", as_index=False)
        .agg(lambda s: next((x for x in s if str(x).strip() != ""), ""))
    )

    # --- UPDATE existing rows (merge on barcode) ---
    merged = clean.merge(
        enriched_by_barcode_full[["barcode"] + ENRICH_COLS],
        how="left",
        on="barcode",
        suffixes=("", "_new")
    )

    for c in ENRICH_COLS:
        if c not in merged.columns:
            merged[c] = ""
        if f"{c}_new" in merged.columns:
            merged[c] = merged[f"{c}_new"].where(merged[f"{c}_new"].astype(str).str.strip() != "", merged[c])

    # Drop helper suffix columns if present
    for c in [f"{x}_new" for x in ENRICH_COLS if f"{x}_new" in merged.columns]:
        merged = merged.drop(columns=[c])

    # --- APPEND extras (barcodes in filled but not in clean) ---
    existing_keys = set(clean["barcode"])
    extras = enriched_by_barcode_full[~enriched_by_barcode_full["barcode"].isin(existing_keys)].copy()

    if not extras.empty:
        extras_rows = pd.DataFrame({
            "file": extras["file"],
            "symbology": extras["symbology"],
            "barcode": extras["barcode"],
            "status": extras["status"],
        })
        for c in ENRICH_COLS:
            extras_rows[c] = extras[c]
        # Ensure all columns present
        for col in merged.columns:
            if col not in extras_rows.columns:
                extras_rows[col] = ""
        extras_rows = extras_rows[merged.columns]
        merged = pd.concat([merged, extras_rows], ignore_index=True)

    # --- Audit (updated/appended rows) ---
    updated_mask = merged["barcode"].isin(enriched_by_barcode_full["barcode"])
    audit = merged[updated_mask].copy()
    audit["action"] = audit["barcode"].apply(lambda v: "appended" if v not in existing_keys else "updated")

    # --- Save ---
    merged.to_csv(args.out, index=False)
    audit.to_csv(args.audit, index=False)

    print(f"[OK] Base rows kept: {len(clean)}")
    print(f"[OK] Enriched barcodes found: {len(enriched_by_barcode_full)}")
    print(f"[OK] Appended new barcodes: {len(extras)}")
    print(f"[OK] Final rows written: {len(merged)} → {args.out}")
    print(f"[OK] Audit written: {args.audit}")

if __name__ == "__main__":
    main()
