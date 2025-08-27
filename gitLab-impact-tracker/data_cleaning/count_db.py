#!/usr/bin/env python3
import argparse, sqlite3, sys, os

Q_R_BY_OID = """
SELECT COUNT(DISTINCT a.r_id)
FROM affiliates a
JOIN department d ON d.d_id = a.d_id
WHERE d.o_id = ?;
"""
Q_D_BY_OID = """
SELECT COUNT(DISTINCT d.d_id)
FROM affiliates a
JOIN department d ON d.d_id = a.d_id
WHERE d.o_id = ?;
"""
Q_OID_BY_NAME = "SELECT o_id FROM organization WHERE name = ? COLLATE NOCASE;"

def main():
    ap = argparse.ArgumentParser(description="Count researchers/departments (via affiliates) for an organization.")
    ap.add_argument("db", help="Path to SQLite DB")
    ap.add_argument("--org-id", type=int, help="Organization ID (preferred, e.g., 1)")
    ap.add_argument("--org-name", default="University of Edinburgh",
                    help="Organization name (used if --org-id not given)")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        print(f"DB not found: {args.db}", file=sys.stderr); sys.exit(1)

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    o_id = args.org_id
    if o_id is None:
        cur.execute(Q_OID_BY_NAME, (args.org_name,))
        row = cur.fetchone()
        if not row:
            print(f"Organization not found by name: {args.org_name}", file=sys.stderr); sys.exit(2)
        o_id = row[0]

    cur.execute(Q_R_BY_OID, (o_id,))
    researchers = cur.fetchone()[0] or 0

    cur.execute(Q_D_BY_OID, (o_id,))
    departments = cur.fetchone()[0] or 0

    print(f"organization_id: {o_id}")
    print(f"researchers_with_affiliation: {researchers}")
    print(f"departments_with_affiliation: {departments}")

if __name__ == "__main__":
    main()

