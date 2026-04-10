"""
restricted_category_embeddings.py
"""

import os
import sys
import pickle
import sqlite3

import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

# ── Setup ─────────────────────────────────────────────────────────────────────

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

DB      = "products.db"
MODEL   = "text-embedding-3-small"
CSV_ARG = sys.argv[1] if len(sys.argv) > 1 else "restricted_categories.csv"


# ── Step 0: Ensure the table exists ───────────────────────────────────────────

def ensure_table(db_path: str):
    """Create restricted_categories table if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS restricted_categories (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            category   TEXT UNIQUE NOT NULL,
            embedding  BLOB,
            added_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    print(f"[setup] Table 'restricted_categories' ready in {db_path}")


# ── Step 1: Load CSV ───────────────────────────────────────────────────────────

def load_categories(csv_path: str) -> list:
    if not os.path.exists(csv_path):
        print(f"[error] CSV not found: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip().str.lower().str.strip('"')

    # Accept column named 'categories' or 'category'
    col = None
    for candidate in ('categories', 'category'):
        if candidate in df.columns:
            col = candidate
            break

    if col is None:
        print(f"[error] CSV must have a 'categories' column. Found: {list(df.columns)}")
        sys.exit(1)

    cats = df[col].dropna().astype(str).str.strip().tolist()
    cats = [c for c in cats if c]
    print(f"[load] {len(cats)} categories loaded from '{csv_path}'")
    return cats


# ── Step 2: Generate embeddings ───────────────────────────────────────────────

def generate_embeddings(categories: list) -> list:
    """Batch-embed all categories. Returns list of float vectors."""
    print(f"[embed] Generating embeddings for {len(categories)} categories...")
    response = client.embeddings.create(model=MODEL, input=categories)
    vectors  = [item.embedding for item in response.data]
    print(f"[embed] Done. Vector dim = {len(vectors[0])}")
    return vectors


# ── Step 3: Store in DB ────────────────────────────────────────────────────────

def store_embeddings(db_path: str, categories: list, vectors: list):
    """
    Insert categories + embeddings into restricted_categories table.
    Embeddings stored as BLOB (pickle) so they can be loaded back as
    numpy arrays for cosine similarity.
    """
    conn    = sqlite3.connect(db_path)
    cursor  = conn.cursor()
    inserted = 0
    skipped  = 0

    for cat, vec in zip(categories, vectors):
        blob = pickle.dumps(vec)          # serialize float list → bytes
        try:
            cursor.execute(
                "INSERT OR IGNORE INTO restricted_categories (category, embedding) VALUES (?, ?)",
                (cat, blob)
            )
            if cursor.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"[warn] Could not insert '{cat[:50]}': {e}")
            skipped += 1

    conn.commit()
    conn.close()
    print(f"[store] Inserted: {inserted} | Skipped (duplicates): {skipped}")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{'='*55}")
    print(f"  Restricted Category Embeddings Loader")
    print(f"  CSV : {CSV_ARG}")
    print(f"  DB  : {DB}")
    print(f"  Model: {MODEL}")
    print(f"{'='*55}\n")

    ensure_table(DB)
    categories = load_categories(CSV_ARG)
    vectors    = generate_embeddings(categories)
    store_embeddings(DB, categories, vectors)

    print(f"\nDone! {len(categories)} categories processed.")
    print(f"Verify: python -c \"import sqlite3; conn=sqlite3.connect('{DB}'); "
          f"print(conn.execute('SELECT COUNT(*) FROM restricted_categories').fetchone())\"")
