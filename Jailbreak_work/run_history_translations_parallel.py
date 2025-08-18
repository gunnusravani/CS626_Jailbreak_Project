#!/usr/bin/env python3
# run_history_translations_parallel.py
"""
Parallel runner for history_tranlations.py
- Loads history_tranlations.py directly (no sys.path hassle)
- Spawns processes; each creates its own googletrans.Translator
- Calls translate_columns(file, translator, column_pairs) per file
"""

import os
import argparse
import traceback
import importlib.util
from concurrent.futures import ProcessPoolExecutor, as_completed

# ---- Load local history_tranlations.py by filename (same folder) ----
import history_translations 


def list_csvs(root_dir: str):
    files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            if not fn.lower().endswith(".csv"):
                continue
            if "checkpoint" in fn.lower():
                continue
            files.append(os.path.join(dirpath, fn))
    return files


def process_one_file(file_path: str) -> str:
    """
    Worker: create a Translator and call translate_columns() for one file.
    Uses the same column_pairs as in history_tranlations.process_directory().
    """
    try:
        from googletrans import Translator
        translator = Translator()

        column_pairs = [
            ("initial_response", "gtrans_initial_response"),
            ("final_response", "gtrans_final_response"),
        ]

        history_translations.translate_columns(file_path, translator, column_pairs)
        return f"OK   {file_path}"
    except Exception as e:
        tb = traceback.format_exc(limit=2)
        return f"ERR  {file_path}\n{e}\n{tb}"


def main():
    ap = argparse.ArgumentParser(
        description="Parallel runner for history_tranlations.py over multiple CSV files."
    )
    ap.add_argument("--root", default="../jailbreak_responses_final/history/gemma3_12b",
                    help="Root directory to scan for CSVs")
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 4,
                    help="Number of parallel processes")
    ap.add_argument("--limit", type=int, default=0,
                    help="(Optional) process only first N files for testing")
    args = ap.parse_args()

    files = list_csvs(args.root)
    if args.limit and args.limit > 0:
        files = files[:args.limit]

    if not files:
        print(f"No CSV files found under: {args.root}")
        return

    print(f"Discovered {len(files)} CSVs. Running with {args.workers} workers.")

    done = 0
    errs = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        fut2file = {ex.submit(process_one_file, f): f for f in files}
        for fut in as_completed(fut2file):
            msg = fut.result()
            print(msg, flush=True)
            done += 1
            if msg.startswith("ERR"):
                errs += 1

    print(f"\nFinished. Files processed: {done}, errors: {errs}")


if __name__ == "__main__":
    main()
