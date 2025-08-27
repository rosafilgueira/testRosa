#!/usr/bin/env python3
import argparse
import gzip
from pathlib import Path
from typing import Tuple, IO, Iterable

def open_maybe_gzip(path: Path, mode: str = "rt", encoding: str = "utf-8", errors: str = "ignore") -> IO:
    """
    Open a file normally, or via gzip if it ends with .gz.
    Text mode by default.
    """
    if str(path).endswith(".gz"):
        return gzip.open(path, mode=mode, encoding=encoding, errors=errors)  # type: ignore[arg-type]
    return open(path, mode=mode, encoding=encoding, errors=errors)  # type: ignore[call-arg]

def iter_lines(path: Path) -> Iterable[str]:
    with open_maybe_gzip(path, "rt") as f:
        for line in f:
            yield line

def count_lines(path: Path, assume_header: bool = True) -> Tuple[int, int]:
    """
    Returns (total_lines, data_rows_excl_header).
    If assume_header=True, subtract 1 if file has at least 1 line.
    """
    total = 0
    for _ in iter_lines(path):
        total += 1
    data_rows = max(total - 1, 0) if assume_header and total > 0 else total
    return total, data_rows

def main():
    ap = argparse.ArgumentParser(description="Count total lines and data rows (excluding header) for CSV (or .gz) files.")
    ap.add_argument("files", nargs="+", help="Paths to CSV files (supports .gz).")
    ap.add_argument("--no-header", action="store_true", help="Treat files as having no header (do not subtract 1).")
    ap.add_argument("--csv", action="store_true", help="Output results as CSV to stdout.")
    args = ap.parse_args()

    rows = []
    for f in args.files:
        p = Path(f)
        if not p.exists():
            rows.append((p.name, False, None, None))
            continue
        total, data = count_lines(p, assume_header=not args.no_header)
        rows.append((p.name, True, total, data))

    if args.csv:
        print("file,exists,total_lines,data_rows_excl_header")
        for name, exists, total, data in rows:
            if not exists:
                print(f"{name},False,,")
            else:
                print(f"{name},True,{total},{data}")
    else:
        # Pretty table
        w1 = max(len("file"), max(len(r[0]) for r in rows))
        header = f"{'file'.ljust(w1)}  {'exists':<6}  {'total_lines':>12}  {'data_rows_excl_header':>23}"
        print(header)
        print("-" * len(header))
        for name, exists, total, data in rows:
            if not exists:
                print(f"{name.ljust(w1)}  {'False':<6}  {'-':>12}  {'-':>23}")
            else:
                print(f"{name.ljust(w1)}  {'True':<6}  {total:>12}  {data:>23}")

if __name__ == "__main__":
    main()

