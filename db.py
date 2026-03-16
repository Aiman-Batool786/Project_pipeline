"""
db.py - HYBRID APPROACH + SELLER INFO
Complete schema with audit trail and seller information table.
Seller info is stored as original — never LLM-enhanced.
"""

import sqlite3
import json

DB_NAME = "products.db"


def create_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    return conn


# ─────────────────────────────────────────
# CREATE ALL TABLES
# ─────────────────────────────────────────

def create_all_tables():
    conn   = create_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("ALTER TABLE categories RENAME TO categories_old")
    except sqlite3.OperationalError:
        pass

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        category_id   INTEGER PRIMARY KEY,
        category_name TEXT,
        embedding     BLOB
    )""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS original_content (
        product_id  INTEGER PRIMARY KEY AUTOINCREMENT,
        url         TEXT UNIQUE,
        title       TEXT,
        description TEXT,
        image_url   TEXT
    )""")

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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS category_assignments (
        id                     INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id             INTEGER UNIQUE,
        original_category_id   INTEGER,
        original_category_name TEXT,
        enhanced_category_id   INTEGER,
        enhanced_category_name TEXT,
        confidence             REAL,
        FOREIGN KEY (product_id) REFERENCES original_content(product_id)
    )""")

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
    )""")

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

    # ── SELLER INFO TABLE (new) ────────────────────────────────────────────
    # Stores original seller data — never LLM-enhanced
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

    conn.commit()
    conn.close()
    print("✅ All tables created (including seller_info table)")


# ─────────────────────────────────────────
# LEGACY INSERT FUNCTIONS
# ─────────────────────────────────────────

def insert_original_content(url, title, description, image_url):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO original_content (url, title, description, image_url)
            VALUES (?, ?, ?, ?)
        """, (url, title, description, image_url))
        conn.commit()
        product_id = cursor.lastrowid
        return product_id
    except sqlite3.IntegrityError:
        cursor.execute("SELECT product_id FROM original_content WHERE url = ?", (url,))
        return cursor.fetchone()[0]
    finally:
        conn.close()


def insert_category_assignment(product_id, orig_cat_id, orig_cat_name,
                                enh_cat_id, enh_cat_name, confidence):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO category_assignments
            (product_id, original_category_id, original_category_name,
             enhanced_category_id, enhanced_category_name, confidence)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (product_id, orig_cat_id, orig_cat_name,
              enh_cat_id, enh_cat_name, confidence))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()


def insert_product(data):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
        INSERT INTO products (
            url, title, description, improved_title, improved_description,
            bullet_points, category_id, category_name, confidence, enhanced_category
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data)
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()


# ─────────────────────────────────────────
# TASK 2 INSERT FUNCTIONS
# ─────────────────────────────────────────

def insert_scraped_product(url, attributes):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO scraped_products (
                url, title, description, brand,
                image_1, image_2, image_3, image_4, image_5, image_6,
                color, dimensions, weight, material,
                age_from, age_to, certifications, country_of_origin,
                bullet_points, price, shipping, warranty, product_type,
                store_name, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            url,
            attributes.get("title", ""),
            attributes.get("description", ""),
            attributes.get("brand", ""),
            attributes.get("image_1", ""),
            attributes.get("image_2", ""),
            attributes.get("image_3", ""),
            attributes.get("image_4", ""),
            attributes.get("image_5", ""),
            attributes.get("image_6", ""),
            attributes.get("color", ""),
            attributes.get("dimensions", ""),
            attributes.get("weight", ""),
            attributes.get("material", ""),
            attributes.get("age_from", ""),
            attributes.get("age_to", ""),
            attributes.get("certifications", ""),
            attributes.get("country_of_origin", ""),
            json.dumps(attributes.get("bullet_points", [])),
            attributes.get("price", ""),
            attributes.get("shipping", ""),
            attributes.get("warranty", ""),
            attributes.get("product_type", ""),
            attributes.get("store_name", ""),
            json.dumps(attributes)
        ))
        conn.commit()
        product_id = cursor.lastrowid
        print(f"✅ Scraped product saved (product_id={product_id})")
        return product_id
    except sqlite3.IntegrityError:
        cursor.execute("SELECT product_id FROM scraped_products WHERE url = ?", (url,))
        product_id = cursor.fetchone()[0]
        print(f"⚠  Product already exists (product_id={product_id})")
        return product_id
    except Exception as e:
        print(f"❌ Error: {e}")
        return None
    finally:
        conn.close()


def insert_mapped_product(product_id, category_id, mapped_data):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO mapped_products (
                product_id, titre, description, marque,
                url_image_1, couleur_principale, dimensions, poids, matiere,
                age_from, age_to, certifications, pays_origine,
                fabricant_nom, garantie, notes, additional_fields
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            product_id,
            mapped_data.get("title", ""),
            mapped_data.get("description", ""),
            mapped_data.get("brand", ""),
            mapped_data.get("sellerPictureUrls_1", ""),
            mapped_data.get("3264", ""),
            mapped_data.get("24069", ""),
            mapped_data.get("5403", ""),
            mapped_data.get("24061", ""),
            mapped_data.get("11335", ""),
            mapped_data.get("24947", ""),
            mapped_data.get("38412", ""),
            mapped_data.get("37045", ""),
            mapped_data.get("47456", ""),
            mapped_data.get("37937", ""),
            mapped_data.get("6587", ""),
            json.dumps({k: v for k, v in mapped_data.items()})
        ))
        conn.commit()
        print(f"✅ Mapped product saved (product_id={product_id})")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        conn.close()


def insert_template_output(product_id, category_id, output_type,
                            file_path, file_name, status="success"):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO template_outputs
            (product_id, category_id, output_type, file_path, file_name, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (product_id, category_id, output_type, file_path, file_name, status))
        conn.commit()
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        conn.close()


def log_processing(product_id, url, step, status, message=""):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO processing_logs (product_id, url, step, status, message)
            VALUES (?, ?, ?, ?, ?)
        """, (product_id, url, step, status, message))
        conn.commit()
    except Exception as e:
        print(f"❌ Error logging: {e}")
    finally:
        conn.close()


def get_product_by_id(product_id):
    conn   = create_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sp.*, ca.category_name, ca.confidence
        FROM scraped_products sp
        LEFT JOIN category_assignments ca ON sp.product_id = ca.product_id
        WHERE sp.product_id = ?
    """, (product_id,))
    result = cursor.fetchone()
    conn.close()
    return result


# ─────────────────────────────────────────
# SELLER INFO FUNCTIONS (new)
# ─────────────────────────────────────────

SELLER_FIELDS = [
    'store_name', 'store_id', 'store_url', 'seller_id',
    'seller_positive_rate', 'seller_rating', 'seller_communication',
    'seller_shipping_speed', 'seller_country', 'store_open_date',
    'seller_level', 'seller_total_reviews', 'seller_positive_num', 'is_top_rated'
]


def insert_seller_info(product_id: int, seller_data: dict) -> bool:
    """
    Store original seller info. Never modified by LLM.
    seller_data: dict with seller/store fields from scraper.
    """
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO seller_info (
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
    except sqlite3.IntegrityError:
        print(f"⚠  Seller info already exists for product_id={product_id}")
        return False
    except Exception as e:
        print(f"❌ Error saving seller info: {e}")
        return False
    finally:
        conn.close()


def get_seller_info(product_id: int) -> dict:
    """Retrieve seller info for a product."""
    conn   = create_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT * FROM seller_info WHERE product_id = ?", (product_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════
# HYBRID AUDIT TRAIL FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

SPEC_FIELDS = [
    'brand', 'color', 'dimensions', 'weight', 'material',
    'certifications', 'country_of_origin', 'warranty',
    'product_type', 'age_from', 'age_to', 'gender'
]


def insert_enhanced_content(product_id, enhanced_data):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO enhanced_content (
                product_id, title, description, bullet_points, html_description,
                brand, color, dimensions, weight, material, certifications,
                country_of_origin, warranty, product_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            product_id,
            enhanced_data.get('title', ''),
            enhanced_data.get('description', ''),
            json.dumps(enhanced_data.get('bullet_points', [])),
            enhanced_data.get('html_description', ''),
            enhanced_data.get('brand', ''),
            enhanced_data.get('color', ''),
            enhanced_data.get('dimensions', ''),
            enhanced_data.get('weight', ''),
            enhanced_data.get('material', ''),
            enhanced_data.get('certifications', ''),
            enhanced_data.get('country_of_origin', ''),
            enhanced_data.get('warranty', ''),
            enhanced_data.get('product_type', '')
        ))
        conn.commit()
        print(f"✅ Enhanced content saved (product_id={product_id})")
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        conn.close()


def insert_original_specifications(product_id, original_specs):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO original_specifications (
                product_id, brand, color, dimensions, weight, material,
                certifications, country_of_origin, warranty, product_type,
                age_from, age_to, gender
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            product_id,
            original_specs.get("brand", ""),
            original_specs.get("color", ""),
            original_specs.get("dimensions", ""),
            original_specs.get("weight", ""),
            original_specs.get("material", ""),
            original_specs.get("certifications", ""),
            original_specs.get("country_of_origin", ""),
            original_specs.get("warranty", ""),
            original_specs.get("product_type", ""),
            original_specs.get("age_from", ""),
            original_specs.get("age_to", ""),
            original_specs.get("gender", "")
        ))
        conn.commit()
        print(f"✅ Original specifications saved (product_id={product_id})")
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as e:
        print(f"⚠  Could not save original specs: {e}")
        return False
    finally:
        conn.close()


def insert_enhanced_specifications(product_id, enhanced_specs):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO enhanced_specifications (
                product_id, brand, color, dimensions, weight, material,
                certifications, country_of_origin, warranty, product_type,
                age_from, age_to, gender
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            product_id,
            enhanced_specs.get("brand", ""),
            enhanced_specs.get("color", ""),
            enhanced_specs.get("dimensions", ""),
            enhanced_specs.get("weight", ""),
            enhanced_specs.get("material", ""),
            enhanced_specs.get("certifications", ""),
            enhanced_specs.get("country_of_origin", ""),
            enhanced_specs.get("warranty", ""),
            enhanced_specs.get("product_type", ""),
            enhanced_specs.get("age_from", ""),
            enhanced_specs.get("age_to", ""),
            enhanced_specs.get("gender", "")
        ))
        conn.commit()
        print(f"✅ Enhanced specifications saved (product_id={product_id})")
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as e:
        print(f"⚠  Could not save enhanced specs: {e}")
        return False
    finally:
        conn.close()


def log_specification_audit(product_id, spec_field, original_value,
                             enhanced_value, template_value, source_used, notes=""):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO specification_audit_log (
                product_id, spec_field, original_value,
                enhanced_value, template_value, source_used, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (product_id, spec_field, original_value or "",
              enhanced_value or "", template_value or "", source_used, notes))
        conn.commit()
    except Exception as e:
        print(f"⚠  Error logging spec audit: {e}")
    finally:
        conn.close()


def log_all_spec_audits(product_id, scraped_data, specs_enhanced, enriched_data_for_template):
    audit_fields = [
        'brand', 'color', 'dimensions', 'weight', 'material',
        'certifications', 'country_of_origin', 'warranty', 'product_type'
    ]
    for field in audit_fields:
        original_val = scraped_data.get(field, "")
        enhanced_val = specs_enhanced.get(field, "")
        template_val = enriched_data_for_template.get(field, "")
        source       = "enhanced" if template_val else "empty"
        log_specification_audit(product_id, field, original_val,
                                enhanced_val, template_val, source)
    print(f"✅ Specification audit log written for product_id={product_id}")


# Backward compatibility
def create_table():
    create_all_tables()

def create_categories_table():
    pass
