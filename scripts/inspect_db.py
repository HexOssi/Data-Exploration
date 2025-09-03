import sqlite3

database_file = r'datasrc\cac-data-contd-32.db'

try:
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()

    # Get list of tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tables in the database:")
    for table in tables:
        table_name = table[0]
        print(f"- {table_name}")
        # Get schema for each table
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema = cursor.fetchall()
        print(f"  Schema for table '{table_name}':")
        for col in schema:
            print(f"    Column: {col[1]}, Type: {col[2]}, Not Null: {col[3]}, Primary Key: {col[5]}")

except sqlite3.Error as e:
    print(f"Database error: {e}")
finally:
    if conn:
        conn.close()