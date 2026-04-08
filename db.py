"""
db.py - Complete schema with seller_info + compliance + restricted_keywords tables.
"""

import sqlite3
import json
import csv
import os

DB_NAME = "products.db"


def create_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)


def create_all_tables():
    conn = create_connection()
    cursor = conn.cursor()

    # Categories table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        category_id   INTEGER PRIMARY KEY,
        category_name TEXT,
        embedding     BLOB
    )""")

    # Scraped products table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS scraped_products (
        product_id         INTEGER PRIMARY KEY AUTOINCREMENT,
        url                TEXT UNIQUE,
        title              TEXT,
        description        TEXT,
        brand              TEXT,
        image_1            TEXT,
        image_2            TEXT,
        image_3            TEXT,
        image_4            TEXT,
        image_5            TEXT,
        image_6            TEXT,
        color              TEXT,
        dimensions         TEXT,
        weight             TEXT,
        material           TEXT,
        age_from           TEXT,
        age_to             TEXT,
        certifications     TEXT,
        country_of_origin  TEXT,
        bullet_points      TEXT,
        price              TEXT,
        shipping           TEXT,
        warranty           TEXT,
        product_type       TEXT,
        store_name         TEXT,
        raw_json           TEXT,
        scraped_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Seller info table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS seller_info (
        id                    INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id            INTEGER UNIQUE,
        store_name            TEXT,
        store_id              TEXT,
        store_url             TEXT,
        seller_id             TEXT,
        seller_positive_rate  TEXT,
        seller_rating         TEXT,
        seller_communication  TEXT,
        seller_shipping_speed TEXT,
        seller_country        TEXT,
        store_open_date       TEXT,
        seller_level          TEXT,
        seller_total_reviews  TEXT,
        seller_positive_num   TEXT,
        is_top_rated          TEXT,
        scraped_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )""")

    # Compliance info table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS compliance_info (
        id                       INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id               INTEGER NOT NULL,
        compliance_product_id    TEXT,
        manufacturer_name        TEXT,
        manufacturer_address     TEXT,
        manufacturer_email       TEXT,
        manufacturer_phone       TEXT,
        eu_responsible_name      TEXT,
        eu_responsible_address   TEXT,
        eu_responsible_email     TEXT,
        eu_responsible_phone     TEXT,
        extracted_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(product_id, compliance_product_id),
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )""")

    # Restricted keywords table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restricted_keywords (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword     TEXT UNIQUE,
        category    TEXT DEFAULT 'description_spec',
        created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Enhanced content table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS enhanced_content (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id        INTEGER UNIQUE,
        title             TEXT,
        description       TEXT,
        bullet_points     TEXT,
        html_description  TEXT,
        brand             TEXT,
        color             TEXT,
        dimensions        TEXT,
        weight            TEXT,
        material          TEXT,
        certifications    TEXT,
        country_of_origin TEXT,
        warranty          TEXT,
        product_type      TEXT,
        enhanced_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )""")

    # Category assignments table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS category_assignments (
        id                     INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id             INTEGER UNIQUE,
        original_category_id   TEXT,
        original_category_name TEXT,
        enhanced_category_id   TEXT,
        enhanced_category_name TEXT,
        confidence             REAL,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )""")

    # Mapped products table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mapped_products (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id         INTEGER UNIQUE,
        gtin               TEXT,
        seller_reference   TEXT,
        titre              TEXT,
        description        TEXT,
        url_image_1        TEXT,
        marque             TEXT,
        couleur_principale TEXT,
        dimensions         TEXT,
        poids              TEXT,
        matiere            TEXT,
        age_from           TEXT,
        age_to             TEXT,
        certifications     TEXT,
        pays_origine       TEXT,
        fabricant_nom      TEXT,
        garantie           TEXT,
        notes              TEXT,
        additional_fields  TEXT,
        mapped_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )""")

    # Template outputs table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS template_outputs (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id  INTEGER,
        category_id TEXT,
        output_type TEXT,
        file_path   TEXT,
        file_name   TEXT,
        status      TEXT,
        notes       TEXT,
        created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )""")

    # Processing logs table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS processing_logs (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id  INTEGER,
        url         TEXT,
        step        TEXT,
        status      TEXT,
        message     TEXT,
        log_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )""")

    # Original specifications table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS original_specifications (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id        INTEGER UNIQUE,
        brand             TEXT,
        color             TEXT,
        dimensions        TEXT,
        weight            TEXT,
        material          TEXT,
        certifications    TEXT,
        country_of_origin TEXT,
        warranty          TEXT,
        product_type      TEXT,
        age_from          TEXT,
        age_to            TEXT,
        gender            TEXT,
        source            TEXT DEFAULT 'scraper',
        extracted_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )""")

    # Enhanced specifications table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS enhanced_specifications (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id        INTEGER UNIQUE,
        brand             TEXT,
        color             TEXT,
        dimensions        TEXT,
        weight            TEXT,
        material          TEXT,
        certifications    TEXT,
        country_of_origin TEXT,
        warranty          TEXT,
        product_type      TEXT,
        age_from          TEXT,
        age_to            TEXT,
        gender            TEXT,
        source            TEXT DEFAULT 'openai',
        enhanced_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )""")

    # Specification audit log table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS specification_audit_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id      INTEGER,
        spec_field      TEXT,
        original_value  TEXT,
        enhanced_value  TEXT,
        template_value  TEXT,
        source_used     TEXT,
        notes           TEXT,
        recorded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )""")

    conn.commit()
    conn.close()
    print("✅ All tables created (including seller_info, compliance_info, and restricted_keywords)")


def load_restricted_keywords_from_csv(csv_file_path):
    """
    Load restricted keywords from CSV file into database
    
    Args:
        csv_file_path: Path to CSV file with column 'desc_and_spec_restricted_keywords'
    
    Returns:
        Number of keywords loaded
    """
    if not os.path.exists(csv_file_path):
        print(f"❌ CSV file not found: {csv_file_path}")
        return 0
    
    conn = create_connection()
    cursor = conn.cursor()
    
    count = 0
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                keyword = row.get('desc_and_spec_restricted_keywords', '').strip()
                if keyword:
                    try:
                        cursor.execute(
                            "INSERT OR IGNORE INTO restricted_keywords (keyword, category) VALUES (?, ?)",
                            (keyword, 'description_spec')
                        )
                        if cursor.rowcount > 0:
                            count += 1
                    except Exception as e:
                        print(f"⚠️ Error inserting keyword '{keyword}': {e}")
        
        conn.commit()
        print(f"✅ Loaded {count} restricted keywords from CSV")
        
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
    finally:
        conn.close()
    
    return count


def get_restricted_keywords():
    """Get all restricted keywords from database"""
    conn = create_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT keyword FROM restricted_keywords")
        keywords = [row[0] for row in cursor.fetchall()]
        return keywords
    except Exception as e:
        print(f"❌ Error fetching restricted keywords: {e}")
        return []
    finally:
        conn.close()


# Seller info functions
SELLER_FIELDS = [
    'store_name', 'store_id', 'store_url', 'seller_id',
    'seller_positive_rate', 'seller_rating', 'seller_communication',
    'seller_shipping_speed', 'seller_country', 'store_open_date',
    'seller_level', 'seller_total_reviews', 'seller_positive_num', 'is_top_rated'
]


def insert_seller_info(product_id: int, seller_data: dict) -> bool:
    """Insert or update seller information"""
    if not seller_data:
        return False
    
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO seller_info (
                product_id, store_name, store_id, store_url, seller_id,
                seller_positive_rate, seller_rating, seller_communication,
                seller_shipping_speed, seller_country, store_open_date,
                seller_level, seller_total_reviews, seller_positive_num, is_top_rated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            product_id,
            seller_data.get('store_name', ''),
            seller_data.get('store_id', ''),
            seller_data.get('store_url', ''),
            seller_data.get('seller_id', ''),
            seller_data.get('seller_positive_rate', ''),
            seller_data.get('seller_rating', ''),
            seller_data.get('seller_communication', ''),
            seller_data.get('seller_shipping_speed', ''),
            seller_data.get('seller_country', ''),
            seller_data.get('store_open_date', ''),
            seller_data.get('seller_level', ''),
            seller_data.get('seller_total_reviews', ''),
            seller_data.get('seller_positive_num', ''),
            seller_data.get('is_top_rated', ''),
        ))
        conn.commit()
        print(f"✅ Seller info saved (product_id={product_id})")
        return True
    except Exception as e:
        print(f"❌ Seller info error: {e}")
        return False
    finally:
        conn.close()


def get_seller_info(product_id: int) -> dict:
    """Get seller information for a product"""
    conn = create_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM seller_info WHERE product_id = ?", (product_id,))
        row = cursor.fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}
    finally:
        conn.close()


def insert_compliance_info(product_id: int, compliance_data: dict) -> bool:
    """Store compliance info"""
    if not compliance_data:
        return False
    
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO compliance_info (
                product_id,
                compliance_product_id,
                manufacturer_name,
                manufacturer_address,
                manufacturer_email,
                manufacturer_phone,
                eu_responsible_name,
                eu_responsible_address,
                eu_responsible_email,
                eu_responsible_phone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            product_id,
            compliance_data.get('compliance_product_id', ''),
            compliance_data.get('manufacturer_name', ''),
            compliance_data.get('manufacturer_address', ''),
            compliance_data.get('manufacturer_email', ''),
            compliance_data.get('manufacturer_phone', ''),
            compliance_data.get('eu_responsible_name', ''),
            compliance_data.get('eu_responsible_address', ''),
            compliance_data.get('eu_responsible_email', ''),
            compliance_data.get('eu_responsible_phone', ''),
        ))
        conn.commit()
        print(f"✅ Compliance info saved (product_id={product_id})")
        return True
    except Exception as e:
        print(f"❌ Compliance info error: {e}")
        return False
    finally:
        conn.close()


def get_compliance_info(product_id: int) -> list:
    """Get compliance information for a product"""
    conn = create_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT * FROM compliance_info WHERE product_id = ?", (product_id,)
        )
        return [dict(r) for r in cursor.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


def insert_scraped_product(url, attributes):
    """Insert scraped product data"""
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO scraped_products (
                url, title, description, brand,
                image_1, image_2, image_3, image_4, image_5, image_6,
                color, dimensions, weight, material,
                age_from, age_to, certifications, country_of_origin,
                bullet_points, price, shipping, warranty, product_type,
                store_name, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            url,
            attributes.get("title", "")[:500],
            attributes.get("description", "")[:5000],
            attributes.get("brand", "")[:100],
            attributes.get("image_1", ""),
            attributes.get("image_2", ""),
            attributes.get("image_3", ""),
            attributes.get("image_4", ""),
            attributes.get("image_5", ""),
            attributes.get("image_6", ""),
            attributes.get("color", "")[:100],
            attributes.get("dimensions", "")[:100],
            attributes.get("weight", "")[:100],
            attributes.get("material", "")[:200],
            attributes.get("age_from", "")[:20],
            attributes.get("age_to", "")[:20],
            attributes.get("certifications", "")[:200],
            attributes.get("country_of_origin", "")[:100],
            json.dumps(attributes.get("bullet_points", [])),
            attributes.get("price", "")[:50],
            attributes.get("shipping", "")[:100],
            attributes.get("warranty", "")[:200],
            attributes.get("product_type", "")[:100],
            attributes.get("store_name", "")[:200],
            json.dumps(attributes)
        ))
        conn.commit()
        product_id = cursor.lastrowid
        print(f"✅ Scraped product saved (product_id={product_id})")
        return product_id
    except Exception as e:
        print(f"❌ Error inserting product: {e}")
        return None
    finally:
        conn.close()


def insert_category_assignment(product_id, orig_cat_id, orig_cat_name,
                                enh_cat_id, enh_cat_name, confidence):
    """Insert category assignment"""
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO category_assignments
            (product_id, original_category_id, original_category_name,
             enhanced_category_id, enhanced_category_name, confidence)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (product_id, orig_cat_id, orig_cat_name,
              enh_cat_id, enh_cat_name, confidence))
        conn.commit()
    except Exception as e:
        print(f"⚠️ Category assignment error: {e}")
    finally:
        conn.close()


def log_processing(product_id, url, step, status, message=""):
    """Log processing step"""
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO processing_logs (product_id, url, step, status, message)
            VALUES (?, ?, ?, ?, ?)
        """, (product_id, url, step, status, message[:500]))
        conn.commit()
    except Exception as e:
        print(f"⚠️ Log error: {e}")
    finally:
        conn.close()


def create_table():
    create_all_tables()


def create_categories_table():
    pass
