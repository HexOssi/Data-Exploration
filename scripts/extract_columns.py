
import sqlite3, csv, sys, os

db = r"E:\OSSI\EDA_on_CAC\datasrc\cac-combined.db"
table = "organizations_old"
out = r"E:\OSSI\EDA_on_CAC\out\organizations_columns_samples.csv"

conn = sqlite3.connect(db)
if conn is None:
    print("Failed to connect to database")
    sys.exit(1)
cur = conn.cursor()

# verify table exists
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
if not cur.fetchone():
    print("Table not found:", table); sys.exit(1)

# get columns
cur.execute(f"PRAGMA table_info('{table}')")
cols = cur.fetchall()  # cid, name, type, notnull, dflt_value, pk

with open(out, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["column","type","notnull","dflt_value","pk","sample"])
    for cid, name, ctype, notnull, dflt, pk in cols:
        # sample one non-null value
        try:
            cur.execute(f"SELECT {name} FROM {table} WHERE {name} IS NOT NULL LIMIT 1")
            row = cur.fetchone()
            sample = row[0] if row else ""
        except Exception as e:
            sample = f"<error: {e}>"
        w.writerow([name, ctype, notnull, dflt, pk, sample])

conn.close()
print("Wrote", out)
