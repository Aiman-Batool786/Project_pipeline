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


df = pd.read_csv("categories_with_embeddings.csv")

print(f"Loading {len(df)} categories...")

success = 0
failed = 0

for _, row in df.iterrows():

    try:
        # FIX: Use numpy to parse the embedding instead of ast.literal_eval
        # Handles truncated numpy format like [0.123 -0.456 ... 0.789]
        embedding_array = np.fromstring(
            row["embedding"].strip("[]"),
            sep=" "
        )

        # If space-separated failed, try comma-separated
        if len(embedding_array) < 10:
            embedding_array = np.fromstring(
                row["embedding"].strip("[]"),
                sep=","
            )

        embedding_blob = pickle.dumps(embedding_array)

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
        print(f"  Skipping row {row['category_id']}: {e}")
        failed += 1


conn.commit()
conn.close()

print(f"Done! {success} categories stored, {failed} failed.")
