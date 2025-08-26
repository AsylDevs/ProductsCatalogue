# run_parallel.py
import os
import sys
import shutil
import subprocess
from pathlib import Path
import pandas as pd

# ========== SIMPLE CONFIG ==========
BASE_DIR        = Path(".")            # current folder where you run the script
SCRIPT_FILENAME = "modifier_1_0.py"    # your existing script (unchanged)
INPUT_NAME      = "data_1_0.csv"
OUTPUT_NAME     = "data_1_1.csv"
REPORT_NAME     = "data_1_0_changes.csv"
NUM_WORKERS     = 3

# Optional: override env vars your script reads (no need to change the script)
SCRIPT_ENV_OVERRIDES = {
    # "BATCH_SIZE": "15",
    # "PAUSE": "0.4",
    # "RETRIES": "5",
    # "READ_TIMEOUT_SEC": "45",
}
# ===================================


def main():
    base_dir = BASE_DIR
    base_dir.mkdir(exist_ok=True)

    script_path = base_dir / SCRIPT_FILENAME
    input_path  = base_dir / INPUT_NAME

    if not script_path.exists():
        print(f"[error] Could not find script: {script_path}")
        sys.exit(1)
    if not input_path.exists():
        print(f"[error] Could not find input CSV: {input_path}")
        sys.exit(1)

    print(f"[info] Using script: {script_path}")
    print(f"[info] Using input : {input_path}")

    # Load full dataset
    df = pd.read_csv(input_path, dtype=str).fillna("")
    print(f"[info] Loaded {len(df)} rows from input")

    # Add helper column if missing
    if "global_row" not in df.columns:
        df.insert(0, "global_row", range(len(df)))
        print(f"[info] Added global_row column")

    # Shard by modulo
    shards = []
    for w in range(NUM_WORKERS):
        shard_df = df[df["global_row"].astype(int) % NUM_WORKERS == w].copy()
        shards.append(shard_df)
        print(f"[info] Prepared shard {w} with {len(shard_df)} rows")

    # Create shard dirs & inputs
    workdirs = []
    for w, shard_df in enumerate(shards):
        shard_dir = base_dir / f"shard_{w}"
        shard_dir.mkdir(exist_ok=True)
        workdirs.append(shard_dir)

        shutil.copy2(script_path, shard_dir / SCRIPT_FILENAME)

        shard_input = shard_dir / INPUT_NAME
        shard_df.to_csv(shard_input, index=False)
        print(f"[info] Wrote input for shard {w} → {shard_input}")

        (shard_dir / "logs").mkdir(exist_ok=True)

    # Launch workers
    procs = []
    for w, shard_dir in enumerate(workdirs):
        env = os.environ.copy()
        env.update(SCRIPT_ENV_OVERRIDES)
        env["EVA_SHARD_ID"] = str(w)
        env["PYTHONUNBUFFERED"] = "1"  # live logs

        log_path = shard_dir / "logs" / f"worker_{w}.log"
        print(f"[info] Starting worker {w} in {shard_dir} → log {log_path}")
        logf = open(log_path, "w", encoding="utf-8")
        p = subprocess.Popen(
            [sys.executable, "-u", SCRIPT_FILENAME],
            cwd=str(shard_dir),
            env=env,
            stdout=logf,
            stderr=subprocess.STDOUT,
        )
        procs.append((w, p, logf))

    # Wait for completion
    failed = []
    for w, p, logf in procs:
        print(f"[info] Waiting for worker {w} (pid={p.pid}) ...")
        rc = p.wait()
        logf.close()
        print(f"[info] Worker {w} finished with code {rc}")
        if rc != 0:
            failed.append(w)

    if failed:
        print("\n[error] Some workers failed:")
        for w in failed:
            print(f" - worker {w} (see {base_dir / f'shard_{w}' / 'logs' / f'worker_{w}.log'})")
        sys.exit(2)

    print("[info] All workers finished successfully, merging outputs...")

    # Merge outputs
    merged_out_parts = []
    merged_rep_parts = []

    for w, shard_dir in enumerate(workdirs):
        shard_out = shard_dir / OUTPUT_NAME
        shard_rep = shard_dir / REPORT_NAME

        if not shard_out.exists():
            print(f"[error] Missing output for worker {w}: {shard_out}")
            sys.exit(3)

        print(f"[info] Reading shard {w} output {shard_out}")
        out_df = pd.read_csv(shard_out, dtype=str).fillna("")
        merged_out_parts.append(out_df)

        if shard_rep.exists():
            print(f"[info] Reading shard {w} report {shard_rep}")
            rep_df = pd.read_csv(shard_rep, dtype=str).fillna("")
            rep_df.insert(0, "shard_id", str(w))
            merged_rep_parts.append(rep_df)

    # Final merge
    merged_out = pd.concat(merged_out_parts, ignore_index=True)
    merged_out["global_row"] = merged_out["global_row"].astype(int)
    merged_out = merged_out.sort_values("global_row").reset_index(drop=True)
    merged_out = merged_out.drop(columns=["global_row"], errors="ignore")

    merged_out_path = base_dir / (Path(OUTPUT_NAME).stem + ".merged.csv")
    merged_out.to_csv(merged_out_path, index=False)
    print(f"[ok] Merged data   → {merged_out_path}")

    if merged_rep_parts:
        merged_rep = pd.concat(merged_rep_parts, ignore_index=True)
        merged_rep_path = base_dir / (Path(REPORT_NAME).stem + ".merged.csv")
        merged_rep.to_csv(merged_rep_path, index=False)
        print(f"[ok] Merged report → {merged_rep_path}")

    # Row sanity check
    orig_n = len(df)
    merged_n = len(merged_out)
    if merged_n != orig_n:
        print(f"[warn] Row count changed: original {orig_n} → merged {merged_n}")
    else:
        print(f"[ok] Row count verified: {merged_n} rows")

    print("\nShard folders kept for inspection:")
    for shard_dir in workdirs:
        print(f" - {shard_dir}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[info] Interrupted by user. Check shard logs for progress.")
        sys.exit(130)
