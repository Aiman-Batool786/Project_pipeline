"""
keyword_embeddings.py
═════════════════════
Load restricted keywords from CSV, generate OpenAI embeddings,
and store them in the restricted_keywords table in products.db.

Usage:
    python keyword_embeddings.py
    python keyword_embeddings.py path/to/restricted_keywords_list.csv

CSV format — must have a column named:
    desc_and_spec_restricted_keywords

The script is idempotent: keywords already embedded are skipped.
"""

import os
import sys
import pickle
import sqlite3

import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────

DB_PATH  = os.getenv("DB_PATH", "products.db")
MODEL    = "text-embedding-3-small"
BATCH    = 100   # OpenAI max inputs per request
CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "restricted_keywords_list.csv"
CSV_COL  = "desc_and_spec_restricted_keywords"

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _ensure_table(conn):
    """Create table and embedding column if not already present."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS restricted_keywords (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword   TEXT UNIQUE NOT NULL COLLATE NOCASE,
            embedding BLOB,
            added_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    try:
        conn.execute("ALTER TABLE restricted_keywords ADD COLUMN embedding BLOB")
    except Exception:
        pass  # column already exists
    conn.commit()


def _load_csv(csv_path):
    if not os.path.exists(csv_path):
        print(f"[error] CSV not found: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip().str.strip('"')

    col = None
    for candidate in (CSV_COL, "keyword", "keywords"):
        if candidate in df.columns:
            col = candidate
            break
    if col is None:
        col = df.columns[0]
        print(f"[warn] Expected column '{CSV_COL}' not found; using '{col}'")

    keywords = df[col].dropna().astype(str).str.strip().str.strip('"').tolist()
    keywords = [k for k in keywords if k]
    print(f"[load] {len(keywords)} keywords loaded from '{csv_path}'")
    return keywords


def _keywords_needing_embeddings(conn, keywords):
    """Return only keywords missing from DB or with NULL embedding."""
    placeholders = ",".join("?" * len(keywords))
    rows = conn.execute(
        f"SELECT keyword FROM restricted_keywords "
        f"WHERE keyword IN ({placeholders}) AND embedding IS NOT NULL",
        keywords,
    ).fetchall()
    already_done = {r[0].lower() for r in rows}
    return [k for k in keywords if k.lower() not in already_done]


def _embed_batch(texts):
    all_vectors = []
    for i in range(0, len(texts), BATCH):
        batch = texts[i: i + BATCH]
        resp  = client.embeddings.create(model=MODEL, input=batch)
        all_vectors.extend(item.embedding for item in resp.data)
        print(f"   embedded {min(i + BATCH, len(texts))} / {len(texts)}")
    return all_vectors


def _upsert(conn, keywords, vectors):
    inserted = updated = 0
    for kw, vec in zip(keywords, vectors):
        blob = pickle.dumps(vec)
        conn.execute(
            "INSERT OR IGNORE INTO restricted_keywords (keyword, embedding) VALUES (?, ?)",
            (kw, blob),
        )
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            inserted += 1
        else:
            conn.execute(
                "UPDATE restricted_keywords SET embedding = ? "
                "WHERE keyword = ? AND embedding IS NULL",
                (blob, kw),
            )
            if conn.execute("SELECT changes()").fetchone()[0] > 0:
                updated += 1
    conn.commit()
    return inserted, updated


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*55}")
    print(f"  Restricted Keyword Embeddings Loader")
    print(f"  CSV  : {CSV_PATH}")
    print(f"  DB   : {DB_PATH}")
    print(f"  Model: {MODEL}")
    print(f"{'='*55}\n")

    conn = sqlite3.connect(DB_PATH)
    _ensure_table(conn)

    keywords = _load_csv(CSV_PATH)
    if not keywords:
        print("[warn] No keywords found — nothing to do.")
        conn.close()
        return

    to_embed = _keywords_needing_embeddings(conn, keywords)
    if not to_embed:
        print(f"[info] All {len(keywords)} keywords already have embeddings — nothing to do.")
        conn.close()
        return

    print(f"[embed] {len(to_embed)} need embeddings "
          f"({len(keywords) - len(to_embed)} already done, skipping)")
    vectors = _embed_batch(to_embed)

    inserted, updated = _upsert(conn, to_embed, vectors)
    conn.close()

    print(f"\n[done] inserted={inserted}  updated={updated}")
    n = sqlite3.connect(DB_PATH).execute(
        "SELECT COUNT(*) FROM restricted_keywords WHERE embedding IS NOT NULL"
    ).fetchone()[0]
    print(f"[done] Total keywords with embeddings in DB: {n}")


if __name__ == "__main__":
    main()
