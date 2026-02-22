import sqlite3
import pickle

conn = sqlite3.connect("products.db")
cursor = conn.cursor()

cursor.execute("SELECT category_id, category_name, embedding FROM categories LIMIT 5")

rows = cursor.fetchall()

for row in rows:

    embedding = pickle.loads(row[2])

    print("ID:", row[0])
    print("Name:", row[1])
    print("Embedding length:", len(embedding))
    print("-"*50)

conn.close()