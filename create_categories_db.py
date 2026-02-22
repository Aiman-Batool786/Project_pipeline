import pandas as pd
import sqlite3
import pickle
import ast


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


for _, row in df.iterrows():

    embedding = pickle.dumps(ast.literal_eval(row["embedding"]))

    cursor.execute("""

        INSERT OR REPLACE INTO categories
        (category_id, category_name, embedding)

        VALUES (?, ?, ?)

    """, (

        int(row["category_id"]),
        row["category_name"],
        embedding

    ))


conn.commit()

conn.close()


print("✅ Categories stored in SQLite successfully")