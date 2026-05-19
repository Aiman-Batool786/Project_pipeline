"""
migrate_exported_at.py
----------------------
One-time (idempotent) migration script.
Adds the exported_at column to the existing scraped_products table
in Project 1's products.db without losing any data.

Run from the project root:
    python3 migrate_exported_at.py

Safe to run multiple times — it is a no-op if the column already exists.

Background
----------
The export_to_template batch export module (data/export_to_template.py)
uses exported_at to support incremental exports (--only-new flag).
Without this column the table schema is incomplete and incremental
exports will fail.

The column defaults to NULL for all existing rows, meaning they will be
included in the next export run.  After a successful export run,
export_to_template.py sets exported_at = <utc-timestamp> for every
product it writes to an .xlsm file.
"""

import os
import sqlite3
import sys

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "products.db")

# Table that lives in Project 1 (equivalent to product_fetched in Project 2)
TARGET_TABLE = "scraped_products"
NEW_COLUMN   = "exported_at"


def migrate(db_path: str = DB_PATH) -> None:
    """Add exported_at DATETIME column to scraped_products if absent."""

    if not os.path.exists(db_path):
        print(f"❌  Database not found: {db_path}")
        print("    Make sure you run this from the project root,")
        print("    or that products.db has been created by create_all_tables().")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    # ── Verify target table exists ────────────────────────────────────────
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (TARGET_TABLE,),
    )
    if not cur.fetchone():
        print(f"❌  Table '{TARGET_TABLE}' not found in {db_path}.")
        print("    Run the application once to initialise the schema first.")
        conn.close()
        sys.exit(1)

    # ── Check existing columns ────────────────────────────────────────────
    cur.execute(f"PRAGMA table_info({TARGET_TABLE})")
    existing_cols = {row[1] for row in cur.fetchall()}
    print(f"Existing columns in '{TARGET_TABLE}': {sorted(existing_cols)}")

    if NEW_COLUMN in existing_cols:
        print(f"✅  '{NEW_COLUMN}' column already exists — nothing to do.")
        conn.close()
        return

    # ── Apply migration ───────────────────────────────────────────────────
    print(f"\nAdding '{NEW_COLUMN}' column …")
    cur.execute(
        f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {NEW_COLUMN} DATETIME"
    )
    conn.commit()

    # ── Verify & report ───────────────────────────────────────────────────
    cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
    total = cur.fetchone()[0]

    cur.execute(
        f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE {NEW_COLUMN} IS NULL"
    )
    nulls = cur.fetchone()[0]

    conn.close()

    print("✅  Migration complete.")
    print(f"   Column '{NEW_COLUMN}' added to '{TARGET_TABLE}'.")
    print(f"   {total} existing product(s) → all set to NULL")
    print(f"   (they will be included in the next export run).")
    print(f"   NULL count: {nulls} / {total}")


if __name__ == "__main__":
    migrate()
