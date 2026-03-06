import pandas as pd
import sqlite3
import pickle
import numpy as np

DB_NAME = "products.db"

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

# ✅ RENAME OLD categories TABLE IF IT EXISTS
try:
    cursor.execute("ALTER TABLE categories RENAME TO categories_old")
    print("✅ Old categories table renamed to categories_old")
except sqlite3.OperationalError:
    print("ℹ No old categories table found")

cursor.execute("""
CREATE TABLE IF NOT EXISTS categories (
    category_id TEXT PRIMARY KEY,
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
            row["code"],  # ✅ CHANGED: use 'code' instead of 'category_id'
            row["category_text"],  # ✅ CHANGED: use 'category_text' instead of 'category_name'
            embedding_blob
        ))
        success += 1
    except Exception as e:
        print(f"  Skipping row {i}: {e}")
        failed += 1

conn.commit()
conn.close()

print(f"Done! {success} categories stored, {failed} failed.")