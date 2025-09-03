import time
import pandas as pd
import sqlite3

conn = sqlite3.connect('cac-combined.db')

print("Started analysis..")
query= "SELECT * FROM organizations_old"

start = time.time()
# df = pd.read_sql_query(query, conn)
print("Pandas load time:", time.time() - start)

start = time.time()
cursor = conn.execute(query)
rows = cursor.fetchall()
print("Direct query time:", time.time() - start)

conn.close()

