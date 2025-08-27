# run_parallel.py
import os
import sys
import shutil
import subprocess
import time
from pathlib import Path
import pandas as pd

# ========== SIMPLE CONFIG ==========
BASE_DIR        = Path(".")            # run from data_1 folder
SCRIPT_FILENAME = "modifier_1_0.py"    # your existing script
INPUT_NAME      = "data_1_0.csv"
OUTPUT_NAME     = "data_1_1.csv"
REPORT_NAME     = "data_1_0_changes.csv"
NUM_WORKERS     = 5                    # ← five shards

# Optional: override env vars your script reads (no script edits needed)
SCRIPT_ENV_OVERRIDES = {
    # Safer defaults when parallel:
    "BATCH_SIZE": "15",          # smaller batches per worker
    "PAUSE": "0.6",              # slightly longer backoff inside your script
    "RETRIES": "8",              # per-batch retries inside your script
    "READ_TIMEOUT_SEC": "90",    # longer HTTP read timeout
}
# Backoff for restarting whole workers if the process exits non-zero
RESTART_BACKOFF_BASE_SEC = 20       # initial wait
RESTART_BACKOFF_FACTOR   = 1.6      # exponential growth
RESTART_BACKOFF_CAP_SEC  = 300      # max 5 minutes between restarts
# ===================================


def copy_env_if_present(src_dir: Path, dst_dir: Path):
    """Copy a .env into the shard dir if we find one nearby."""
    for cand in [src_dir / ".env", Path(".") / ".env", Path("..") / ".env"]:
        if cand.exists():
            shutil.copy2(cand, dst_dir / ".env")
            return cand
    return None


def launch_worker(shard_id: int, shard_dir: Path, env: dict):
    """Start one worker process with unbuffered output into its own log."""
    log_path = shard_dir / "logs" / f"worker_{shard_id}.log"
    logf = open(log_path, "a", encoding="utf-8")
    print(f"[info] Starting worker {shard_id} in {shard_dir} → log {log_path}")
    p = subprocess.Popen(
        [sys.executable, "-u", SCRIPT_FILENAME],
        cwd=str(shard_dir),
        env=env,
        stdout=logf,
        stderr=subprocess.STDOUT,
    )
    return p, logf, log_path


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

        # Optional: copy .env so load_dotenv() finds your keys
        found_env = copy_env_if_present(base_dir, shard_dir)
        if found_env:
            print(f"[info] Copied .env from {found_env} → {shard_dir / '.env'}")

        shard_input = shard_dir / INPUT_NAME
        shard_df.to_csv(shard_input, index=False)
        print(f"[info] Wrote input for shard {w} → {shard_input}")

        (shard_dir / "logs").mkdir(exist_ok=True)

    # Launch & supervise workers with auto-restart
    # We keep retrying a shard process until it finishes with rc==0
    active = {}
    for w, shard_dir in enumerate(workdirs):
        env = os.environ.copy()
        env.update(SCRIPT_ENV_OVERRIDES)
        env["EVA_SHARD_ID"] = str(w)
        env["PYTHONUNBUFFERED"] = "1"

        p, logf, log_path = launch_worker(w, shard_dir, env)
        active[w] = {
            "env": env,
            "dir": shard_dir,
            "proc": p,
            "logf": logf,
            "log_path": log_path,
            "attempt": 1,
            "backoff": RESTART_BACKOFF_BASE_SEC,
        }

    # Wait loop: restart failed workers until they succeed
    failed_any = False
    while active:
        # Pull a snapshot to iterate
        for w in list(active.keys()):
            rec = active[w]
            p = rec["proc"]
            rc = p.poll()
            if rc is None:
                continue  # still running

            # closed; flush/close log handle
            rec["logf"].flush()
            rec["logf"].close()
            if rc == 0:
                print(f"[info] Worker {w} finished OK")
                del active[w]
            else:
                failed_any = True
                attempt = rec["attempt"]
                backoff = rec["backoff"]
                print(f"[warn] Worker {w} exited with code {rc} (attempt {attempt}). "
                      f"Restarting in {int(backoff)}s. See log: {rec['log_path']}")

                try:
                    time.sleep(backoff)
                except KeyboardInterrupt:
                    print("\n[info] Interrupted during backoff; exiting.")
                    sys.exit(130)

                # Exponential backoff with cap
                rec["attempt"] += 1
                rec["backoff"] = min(int(backoff * RESTART_BACKOFF_FACTOR), RESTART_BACKOFF_CAP_SEC)

                # Reopen log in append mode and relaunch
                env = rec["env"]
                p, logf, log_path = launch_worker(w, rec["dir"], env)
                rec["proc"] = p
                rec["logf"] = logf
                rec["log_path"] = log_path

        # Avoid busy loop
        time.sleep(1)

    if failed_any:
        print("[info] Some shards needed restarts but all finished successfully.")

    print("[info] All workers finished, merging outputs...")

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
