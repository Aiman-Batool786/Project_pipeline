import pandas as pd
import sqlite3
import pickle
import numpy as np


DB_NAME = "products.db"


conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS categories (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT,
    embedding BLOB
)
""")

# Load category names from CSV
df = pd.read_csv("categories_with_embeddings.csv")
print(f"Loaded {len(df)} categories from CSV")

# Load embeddings from pickle (full precision, not truncated)
with open("category_embeddings.pkl", "rb") as f:
    embeddings = pickle.load(f)

print(f"Loaded {len(embeddings)} embeddings from pickle")

if len(df) != len(embeddings):
    print(f"WARNING: CSV rows ({len(df)}) != pickle embeddings ({len(embeddings)})")

success = 0
failed = 0

for i, row in df.iterrows():
    try:
        embedding_blob = pickle.dumps(embeddings[i])

        cursor.execute("""
            INSERT OR REPLACE INTO categories
            (category_id, category_name, embedding)
            VALUES (?, ?, ?)
        """, (
            int(row["category_id"]),
            row["category_name"],
            embedding_blob
        ))

        success += 1

    except Exception as e:
        print(f"  Skipping row {i}: {e}")
        failed += 1

conn.commit()
conn.close()

print(f"Done! {success} categories stored, {failed} failed.")
