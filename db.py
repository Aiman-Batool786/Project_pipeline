import sqlite3

DB_NAME = "products.db"

def create_connection():
    conn = sqlite3.connect(DB_NAME)
    return conn

# PRODUCTS TABLE
def create_table():
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        title TEXT,
        description TEXT,
        improved_title TEXT,
        improved_description TEXT,
        bullet_points TEXT,
        category_id INTEGER,
        category_name TEXT,
        confidence REAL,
        enhanced_category TEXT   -- <-- Added this column
    )
    """)
    conn.commit()
    conn.close()

# CATEGORIES TABLE
def create_categories_table():
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        category_id INTEGER PRIMARY KEY,
        category_name TEXT,
        embedding BLOB
    )
    """)
    conn.commit()
    conn.close()

def insert_product(data):
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
        INSERT INTO products (
            url,
            title,
            description,
            improved_title,
            improved_description,
            bullet_points,
            category_id,
            category_name,
            confidence,
            enhanced_category      -- <-- Added here
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data)
        conn.commit()
        print("✅ Saved into SQLite")
    except sqlite3.IntegrityError:
        print("⚠ Duplicate URL skipped")
    conn.close()
