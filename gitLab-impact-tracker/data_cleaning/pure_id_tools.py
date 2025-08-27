#!/usr/bin/env python3
import argparse
import sys
import unicodedata
import pandas as pd
from pathlib import Path
import re

# ---------- encoding-robust CSV reader ----------
def _try_read_csv(path, encoding, engine):
    return pd.read_csv(
        path, dtype=str, encoding=encoding, engine=engine,
        on_bad_lines="skip", keep_default_na=False
    )

def read_csv_any_encoding(path: Path) -> pd.DataFrame:
    detected = None
    try:
        import charset_normalizer as cn
        best = cn.from_bytes(path.read_bytes()).best()
        if best: detected = best.encoding
    except Exception:
        try:
            import chardet
            det = chardet.detect(path.read_bytes())
            detected = det.get("encoding")
        except Exception:
            pass

    candidates = ([detected] if detected else []) + [
        "utf-8-sig", "utf-8", "cp1252", "latin1", "iso-8859-1", "mac_roman"
    ]
    for engine in ("c", "python"):
        for enc in candidates:
            try:
                return _try_read_csv(path, enc, engine)
            except Exception:
                continue

    from io import StringIO
    text = path.read_bytes().decode("utf-8", errors="replace")
    return pd.read_csv(StringIO(text), dtype=str, on_bad_lines="skip", keep_default_na=False)

# ---------- PURE utilities ----------
def norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", s).strip().casefold()

def pick_column(cols, *candidates_regex):
    for pat in candidates_regex:
        rx = re.compile(pat, re.IGNORECASE)
        for c in cols:
            if rx.search(c): return c
    return None

def detect_columns(df: pd.DataFrame):
    cols = list(df.columns)
    pure_col = pick_column(
        cols,
        r"\bpure\b.*\bid\b", r"\bid\b.*\bpure\b",
        r"^pure[_\s-]?id$", r"^person[_\s-]?id$", r"^pure person id$"
    )
    # Note the (?:\(s\))? to handle 'First name(s)'
    first_col = pick_column(
        cols,
        r"^first\s*name(?:\(s\))?$",
        r"\bgiven\b.*\bname\b", r"\bforename\b", r"\bpreferred\s*name\b"
    )
    last_col  = pick_column(
        cols,
        r"^last\s*name(?:\(s\))?$",
        r"\bfamily\b.*\bname\b", r"\bsurname\b"
    )
    name_col  = pick_column(
        cols,
        r"^name$", r"\bperson\b.*\bname\b", r"\bdisplay\s*name\b", r"\bfull\s*name\b"
    )
    return pure_col, first_col, last_col, name_col

def build_fullname_series(df, first_col, last_col, name_col):
    if first_col and last_col:
        full = (df[first_col].astype(str).fillna("") + " " +
                df[last_col].astype(str).fillna("")).str.strip()
    elif name_col:
        full = df[name_col].astype(str)
    else:
        full = pd.Series([""] * len(df), index=df.index)
    return full

def count_distinct_pure_ids(df, pure_col):
    if not pure_col: return 0
    ids = df[pure_col].astype(str).map(str.strip).replace({"": pd.NA}).dropna()
    return ids.nunique()

def find_pure_ids_by_name(df, pure_col, first_col, last_col, name_col, first, last, contains=True):
    if not pure_col: return set()
    full = build_fullname_series(df, first_col, last_col, name_col)
    target_first, target_last = norm(first), norm(last)
    cand = full.map(norm)
    if contains:
        mask = cand.map(lambda s: (target_first in s) and (target_last in s))
    else:
        a = f"{target_first} {target_last}".strip()
        b = f"{target_last} {target_first}".strip()
        mask = cand.isin({a, b})
    ids = (df.loc[mask, pure_col].astype(str).map(str.strip)
             .replace({"": pd.NA}).dropna().unique().tolist())
    return set(ids)

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="PURE ID counter and finder (encoding-robust).")
    ap.add_argument("csv", help="Path to Persons_and_Affiliations.csv")
    ap.add_argument("--count", action="store_true", help="Count distinct PURE IDs")
    ap.add_argument("--find", nargs=2, metavar=("FIRST_NAME","LAST_NAME"),
                    help="Find PURE IDs for a given first name and surname")
    ap.add_argument("--exact", action="store_true", help="Exact name match (default: substring)")
    # Optional explicit column overrides:
    ap.add_argument("--pure-col", help="Column name for PURE ID (e.g., 'Pure ID')")
    ap.add_argument("--first-col", help="Column name for first name (e.g., 'First name(s)')")
    ap.add_argument("--last-col",  help="Column name for last name (e.g., 'Last name')")
    ap.add_argument("--name-col",  help="Single full-name column (if present)")
    args = ap.parse_args()

    path = Path(args.csv)
    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    df = read_csv_any_encoding(path)
    pure_col, first_col, last_col, name_col = detect_columns(df)

    # Apply user overrides if provided
    if args.pure_col:  pure_col = args.pure_col
    if args.first_col: first_col = args.first_col
    if args.last_col:  last_col  = args.last_col
    if args.name_col:  name_col  = args.name_col

    # Info to stderr
    print(f"[info] Rows: {len(df)}", file=sys.stderr)
    print(f"[info] Columns: {list(df.columns)}", file=sys.stderr)
    print(f"[info] PURE ID column: {pure_col}", file=sys.stderr)
    if first_col and last_col:
        print(f"[info] First: {first_col} | Last: {last_col}", file=sys.stderr)
    elif name_col:
        print(f"[info] Name: {name_col}", file=sys.stderr)
    else:
        print("[warn] No name columns detected; --find may return nothing.", file=sys.stderr)

    did = False
    if args.count:
        print(f"distinct_pure_ids: {count_distinct_pure_ids(df, pure_col)}")
        did = True

    if args.find:
        first, last = args.find
        ids = find_pure_ids_by_name(df, pure_col, first_col, last_col, name_col,
                                    first, last, contains=not args.exact)
        print("pure_ids_for_name:", ", ".join(sorted(ids)) if ids else "(none)")
        did = True

    if not did:
        print(f"distinct_pure_ids: {count_distinct_pure_ids(df, pure_col)}")

if __name__ == "__main__":
    main()

