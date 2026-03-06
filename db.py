import sqlite3

DB_NAME = "products.db"


def create_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    return conn


# ─────────────────────────────────────────
# CREATE ALL TABLES
# ─────────────────────────────────────────

def create_all_tables():
    conn = create_connection()
    cursor = conn.cursor()

    # ✅ RENAME OLD categories TABLE IF IT EXISTS
    try:
        cursor.execute("ALTER TABLE categories RENAME TO categories_old")
        print("✅ Old categories table renamed to categories_old")
    except sqlite3.OperationalError:
        print("ℹ No old categories table found")

    # 1. Categories table (from category_with_embeddings)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        category_id   INTEGER PRIMARY KEY,
        category_name TEXT,
        embedding     BLOB
    )
    """)

    # 2. Original content table (raw scraped data)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS original_content (
        product_id  INTEGER PRIMARY KEY AUTOINCREMENT,
        url         TEXT UNIQUE,
        title       TEXT,
        description TEXT,
        image_url   TEXT
    )
    """)

    # 3. Enhanced content table (LLM/OpenAI refined)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS enhanced_content (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id   INTEGER UNIQUE,
        title        TEXT,
        description  TEXT,
        bullet_points TEXT,
        image_url    TEXT,
        FOREIGN KEY (product_id) REFERENCES original_content(product_id)
    )
    """)

    # 4. Category assignments table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS category_assignments (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id          INTEGER UNIQUE,
        original_category_id   INTEGER,
        original_category_name TEXT,
        enhanced_category_id   INTEGER,
        enhanced_category_name TEXT,
        confidence          REAL,
        FOREIGN KEY (product_id) REFERENCES original_content(product_id)
    )
    """)

    # 5. Products table — keeps all info in one place (as before)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        url                  TEXT UNIQUE,
        title                TEXT,
        description          TEXT,
        improved_title       TEXT,
        improved_description TEXT,
        bullet_points        TEXT,
        category_id          INTEGER,
        category_name        TEXT,
        confidence           REAL,
        enhanced_category    TEXT
    )
    """)

    conn.commit()
    conn.close()
    print("✅ All tables created")


# ─────────────────────────────────────────
# INSERT FUNCTIONS
# ─────────────────────────────────────────

def insert_original_content(url, title, description, image_url):
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO original_content (url, title, description, image_url)
            VALUES (?, ?, ?, ?)
        """, (url, title, description, image_url))
        conn.commit()
        product_id = cursor.lastrowid
        print(f"✅ Original content saved (product_id={product_id})")
        return product_id
    except sqlite3.IntegrityError:
        cursor.execute("SELECT product_id FROM original_content WHERE url = ?", (url,))
        row = cursor.fetchone()
        print(f"⚠ Duplicate URL, using existing product_id={row[0]}")
        return row[0]
    finally:
        conn.close()


def insert_enhanced_content(product_id, title, description, bullet_points, image_url):
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO enhanced_content (product_id, title, description, bullet_points, image_url)
            VALUES (?, ?, ?, ?, ?)
        """, (product_id, title, description, bullet_points, image_url))
        conn.commit()
        print(f"✅ Enhanced content saved (product_id={product_id})")
    except sqlite3.IntegrityError:
        print(f"⚠ Enhanced content already exists for product_id={product_id}")
    finally:
        conn.close()


def insert_category_assignment(product_id, orig_cat_id, orig_cat_name, enh_cat_id, enh_cat_name, confidence):
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO category_assignments
            (product_id, original_category_id, original_category_name,
             enhanced_category_id, enhanced_category_name, confidence)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (product_id, orig_cat_id, orig_cat_name, enh_cat_id, enh_cat_name, confidence))
        conn.commit()
        print(f"✅ Category assignment saved (product_id={product_id})")
    except sqlite3.IntegrityError:
        print(f"⚠ Category already assigned for product_id={product_id}")
    finally:
        conn.close()


def insert_product(data):
    """Keep original products table working as before"""
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
        INSERT INTO products (
            url, title, description,
            improved_title, improved_description,
            bullet_points,
            category_id, category_name, confidence,
            enhanced_category
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data)
        conn.commit()
        print("✅ Saved into products table")
    except sqlite3.IntegrityError:
        print("⚠ Duplicate URL skipped in products table")
    finally:
        conn.close()


# Keep old function names working (backward compatibility)
def create_table():
    create_all_tables()

def create_categories_table():
    pass  # already handled in create_all_tables