#!/usr/bin/env python3
# run_translations_parallel.py
"""
Runs translate_response_column() from translations.py in parallel across many CSV files.
Each process creates its own googletrans.Translator() instance (safer than sharing).
"""

import os
import argparse
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
# import sys
# sys.path.append("/workspace/CS626_Jailbreak_Project/Jailbreak_work/translation.py")

# Import your existing module (the one you pasted)
# Ensure this file is in the same folder as translations.py or adjust sys.path
import translations  # <- your original file

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
    Worker: create a Translator and call your existing translate_response_column()
    for a single file. Returns a short status string.
    """
    try:
        from googletrans import Translator
        tr = Translator()  # one per process call
        translations.translate_response_column(file_path, tr)
        return f"OK   {file_path}"
    except Exception as e:
        tb = traceback.format_exc(limit=2)
        return f"ERR  {file_path}\n{e}\n{tb}"

def main():
    ap = argparse.ArgumentParser(description="Parallel runner for translations.py over multiple CSV files.")
    ap.add_argument("--root", default="../complete/provocation", help="Root directory to scan")
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 4, help="Number of parallel processes")
    ap.add_argument("--limit", type=int, default=0, help="(Optional) process only first N files for testing")
    args = ap.parse_args()

    files = list_csvs(args.root)
    if args.limit and args.limit > 0:
        files = files[:args.limit]

    if not files:
        print(f"No CSVs found under: {args.root}")
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
