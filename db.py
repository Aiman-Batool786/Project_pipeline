import sqlite3
import json
from datetime import datetime

DB_NAME = "products.db"


def create_connection():
    """Create database connection"""
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    return conn


def create_all_tables():
    """Create ALL tables for Task 1 and Task 2"""
    
    conn = create_connection()
    cursor = conn.cursor()
    
    print("\n📊 Creating database tables...\n")
    
    # ============================================================
    # TASK 1 TABLES (Original functionality)
    # ============================================================
    
    # 1. Categories table (shared with Task 1)
    print("  [TASK 1] Creating categories table...")
    try:
        cursor.execute("ALTER TABLE categories RENAME TO categories_old")
        print("  [TASK 1] ✅ Renamed old categories table")
    except:
        pass
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        category_id TEXT PRIMARY KEY,
        category_name TEXT,
        embedding BLOB
    )
    """)
    
    # 2. Original content table (Task 1)
    print("  [TASK 1] Creating original_content table...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS original_content (
        product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        title TEXT,
        description TEXT,
        image_url TEXT
    )
    """)
    
    # 3. Enhanced content table (Task 1)
    print("  [TASK 1] Creating enhanced_content table...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS enhanced_content (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER UNIQUE,
        title TEXT,
        description TEXT,
        bullet_points TEXT,
        image_url TEXT,
        FOREIGN KEY (product_id) REFERENCES original_content(product_id)
    )
    """)
    
    # 4. Category assignments table (Task 1)
    print("  [TASK 1] Creating category_assignments table...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS category_assignments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER UNIQUE,
        original_category_id TEXT,
        original_category_name TEXT,
        enhanced_category_id TEXT,
        enhanced_category_name TEXT,
        confidence REAL,
        FOREIGN KEY (product_id) REFERENCES original_content(product_id)
    )
    """)
    
    # 5. Legacy products table (Task 1)
    print("  [TASK 1] Creating products table...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        title TEXT,
        description TEXT,
        improved_title TEXT,
        improved_description TEXT,
        bullet_points TEXT,
        category_id TEXT,
        category_name TEXT,
        confidence REAL,
        enhanced_category TEXT
    )
    """)
    
    # ============================================================
    # TASK 2 TABLES (New functionality)
    # ============================================================
    
    # 6. Scraped products table (Task 2)
    print("  [TASK 2] Creating scraped_products table...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS scraped_products (
        product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        title TEXT,
        description TEXT,
        brand TEXT,
        image_1 TEXT,
        image_2 TEXT,
        image_3 TEXT,
        image_4 TEXT,
        image_5 TEXT,
        image_6 TEXT,
        color TEXT,
        dimensions TEXT,
        weight TEXT,
        material TEXT,
        age_from TEXT,
        age_to TEXT,
        certifications TEXT,
        country_of_origin TEXT,
        bullet_points TEXT,
        price TEXT,
        shipping TEXT,
        warranty TEXT,
        product_type TEXT,
        store_name TEXT,
        raw_json TEXT,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 7. Mapped products table (Task 2)
    print("  [TASK 2] Creating mapped_products table...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mapped_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER UNIQUE,
        gtin TEXT,
        seller_reference TEXT,
        titre TEXT,
        description TEXT,
        url_image_1 TEXT,
        marque TEXT,
        description_marketing TEXT,
        couleur_principale TEXT,
        dimensions TEXT,
        poids TEXT,
        matiere TEXT,
        age_from TEXT,
        age_to TEXT,
        certifications TEXT,
        pays_origine TEXT,
        accessoires TEXT,
        technologie TEXT,
        fonction_video TEXT,
        type_transmission TEXT,
        type_ecran TEXT,
        type_alimentation TEXT,
        modeles TEXT,
        objet_connecte TEXT,
        avertissements_securite TEXT,
        fabricant_nom TEXT,
        fabricant_adresse TEXT,
        fabricant_email TEXT,
        garantie TEXT,
        informations_complementaires TEXT,
        compatibilite TEXT,
        autonomie TEXT,
        gamme TEXT,
        label TEXT,
        notes TEXT,
        additional_fields TEXT,
        mapped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )
    """)
    
    # 8. Template outputs table (Task 2)
    print("  [TASK 2] Creating template_outputs table...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS template_outputs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        category_id TEXT,
        output_type TEXT,
        file_path TEXT,
        file_name TEXT,
        status TEXT,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )
    """)
    
    # 9. Processing logs table (Task 2)
    print("  [TASK 2] Creating processing_logs table...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS processing_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        url TEXT,
        step TEXT,
        status TEXT,
        message TEXT,
        log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )
    """)
    
    conn.commit()
    conn.close()
    
    print("\n✅ All tables created successfully (Task 1 + Task 2)\n")


# ============================================================
# TASK 1 INSERT FUNCTIONS (Original)
# ============================================================

def insert_original_content(url, title, description, image_url):
    """Task 1: Save original scraped content"""
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
    """Task 1: Save enhanced content"""
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
    """Task 1: Save category assignments"""
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
    """Task 1: Save to legacy products table"""
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


# ============================================================
# TASK 2 INSERT FUNCTIONS (New)
# ============================================================

def insert_scraped_product(url, attributes):
    """Task 2: Store raw scraped product data"""
    conn = create_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO scraped_products (
                url, title, description, brand, image_1, image_2, image_3,
                image_4, image_5, image_6, color, dimensions, weight, material,
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
        conn.close()
        return product_id
        
    except sqlite3.IntegrityError:
        cursor.execute("SELECT product_id FROM scraped_products WHERE url = ?", (url,))
        product_id = cursor.fetchone()[0]
        print(f"⚠ Product already exists (product_id={product_id})")
        conn.close()
        return product_id
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.close()
        return None


def insert_mapped_product(product_id, category_id, mapped_data):
    """Task 2: Store mapped product data"""
    conn = create_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO mapped_products (
                product_id, category_id, titre, description, marque,
                url_image_1, couleur_principale, dimensions, poids, matiere,
                age_from, age_to, certifications, pays_origine,
                fabricant_nom, garantie, notes, additional_fields
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            product_id,
            category_id,
            mapped_data.get("Titre*", ""),
            mapped_data.get("Description*", ""),
            mapped_data.get("Marque", ""),
            mapped_data.get("URL image 1*", ""),
            mapped_data.get("Couleur principale", ""),
            mapped_data.get("Dimensions", ""),
            mapped_data.get("Poids", ""),
            mapped_data.get("Matières", ""),
            mapped_data.get("Age (A partir de)", ""),
            mapped_data.get("Age (Jusqu'à)", ""),
            mapped_data.get("Certifications et normes", ""),
            mapped_data.get("Pays d'origine", ""),
            mapped_data.get("Fabricant - Nom et raison sociale", ""),
            mapped_data.get("Garantie (²)", ""),
            mapped_data.get("Notes", ""),
            json.dumps({k: v for k, v in mapped_data.items()})
        ))
        
        conn.commit()
        print(f"✅ Mapped product saved (product_id={product_id})")
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.close()
        return False


def insert_template_output(product_id, category_id, output_type, file_path, file_name, status="success"):
    """Task 2: Store template output info"""
    conn = create_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO template_outputs (product_id, category_id, output_type, file_path, file_name, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (product_id, category_id, output_type, file_path, file_name, status))
        
        conn.commit()
        print(f"✅ Template output recorded: {file_name}")
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.close()
        return False


def log_processing(product_id, url, step, status, message=""):
    """Task 2: Log processing steps"""
    conn = create_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO processing_logs (product_id, url, step, status, message)
            VALUES (?, ?, ?, ?, ?)
        """, (product_id, url, step, status, message))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error logging: {e}")
        conn.close()


# ============================================================
# QUERY FUNCTIONS
# ============================================================

def get_product_by_id(product_id):
    """Get product information"""
    conn = create_connection()
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


# ============================================================
# BACKWARD COMPATIBILITY
# ============================================================

def create_table():
    """Backward compatibility"""
    create_all_tables()

def create_categories_table():
    """Backward compatibility"""
    pass
