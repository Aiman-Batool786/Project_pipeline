"""
db.py — Complete schema with all tables.

Tables:
  categories              — Octopia category tree with embeddings
  scraped_products        — Raw scraped product data
  seller_info             — Seller / store information (1:1 with product)
  compliance_info         — EU DSA compliance modal data (1:many)
  enhanced_content        — OpenAI-enhanced titles, descriptions, bullet points
  category_assignments    — Category assigned to each product
  mapped_products         — Template-column-mapped product data
  template_outputs        — Generated Excel file paths
  processing_logs         — Step-by-step pipeline log
  original_specifications — Specs as scraped (before enhancement)
  enhanced_specifications — Specs after OpenAI enhancement
  specification_audit_log — Diff: original vs enhanced vs template
  restricted_keywords     — Keywords forbidden in descriptions / specs
  restricted_categories   — Product categories that are forbidden/restricted
  processed_ids           — Global deduplication table for merchant scraper
  translation             — Per-language translations of title, description,
                            and specification for each product.
                            Supported: Romanian, German, Portuguese, Finnish, French.
                            (v3.5: Spanish replaced with Finnish.)
                            One row per (url_id, language) pair.
  product_details         — Star rating and delivery date per product ID,
                            scraped by star_rating_scraper.py.
  varient                 — Product variant data (colors + country-mapped sizes)
                            One row per individual variant option.
"""

import sqlite3
import json
import csv
import os
import re
from typing import Optional

DB_NAME = "products.db"


def create_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)


def create_all_tables():
    conn   = create_connection()
    cursor = conn.cursor()

    # ── CATEGORIES ─────────────────────────────────────────────────────────
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

    # ── SCRAPED PRODUCTS ────────────────────────────────────────────────────
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
        scraped_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        exported_at        DATETIME
    )""")

    _add_columns_if_missing(cursor, "scraped_products", [
        ("exported_at", "DATETIME"),
    ])

    # ── SELLER INFO ─────────────────────────────────────────────────────────
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

    # ── COMPLIANCE INFO ─────────────────────────────────────────────────────
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

    # ── ENHANCED CONTENT ────────────────────────────────────────────────────
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

    _add_columns_if_missing(cursor, "enhanced_content", [
        ("brand",             "TEXT"),
        ("color",             "TEXT"),
        ("dimensions",        "TEXT"),
        ("weight",            "TEXT"),
        ("material",          "TEXT"),
        ("certifications",    "TEXT"),
        ("country_of_origin", "TEXT"),
        ("warranty",          "TEXT"),
        ("product_type",      "TEXT"),
    ])

    # ── CATEGORY ASSIGNMENTS ────────────────────────────────────────────────
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

    # ── MAPPED PRODUCTS ─────────────────────────────────────────────────────
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

    # ── TEMPLATE OUTPUTS ────────────────────────────────────────────────────
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

    # ── PROCESSING LOGS ─────────────────────────────────────────────────────
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

    # ── ORIGINAL SPECIFICATIONS ─────────────────────────────────────────────
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

    # ── ENHANCED SPECIFICATIONS ─────────────────────────────────────────────
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

    # ── SPECIFICATION AUDIT LOG ─────────────────────────────────────────────
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

    # ── RESTRICTED KEYWORDS ─────────────────────────────────────────────────
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restricted_keywords (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword    TEXT UNIQUE NOT NULL COLLATE NOCASE,
        embedding  BLOB,
        added_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    _add_columns_if_missing(cursor, "restricted_keywords", [("embedding", "BLOB")])

    # ── RESTRICTED CATEGORIES ───────────────────────────────────────────────
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restricted_categories (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        category   TEXT UNIQUE NOT NULL,
        embedding  BLOB,
        added_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # ── TRANSLATION ─────────────────────────────────────────────────────────
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS translation (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        url_id        INTEGER NOT NULL,
        language      TEXT    NOT NULL,
        title         TEXT,
        description   TEXT,
        specification TEXT,
        translated_at TEXT DEFAULT (datetime('now')),
        UNIQUE(url_id, language),
        FOREIGN KEY (url_id) REFERENCES scraped_products(product_id)
    )""")

    # ── PROCESSED IDS (DEDUPLICATION) ──────────────────────────────────────
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS processed_ids (
        id           TEXT PRIMARY KEY,
        job_id       TEXT,
        processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")

    # ── PRODUCT DETAILS (STAR RATING + DELIVERY DATE) ───────────────────────
    # Populated by star_rating_scraper.py / POST /product-details endpoint.
    # Keyed by AliExpress numeric product ID string.
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS product_details (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id   TEXT    NOT NULL UNIQUE,
        star_rating  TEXT,
        delivery     TEXT,
        price        TEXT,
        quantity     TEXT,
        ship_country TEXT,
        scraped_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        url          TEXT
    )""")

    # ── VARIENT ─────────────────────────────────────────────────────────────
    # Stores all product variants: colors and country-mapped sizes.
    # One row per individual variant option.
    #
    # Color row:
    #   variant_type = 'color'
    #   name         = color name (e.g. "White", "Black")
    #   image_url    = full CDN image URL (not downloaded)
    #   sku_col_id   = raw data-sku-col value (e.g. "14-29")
    #   country      = NULL
    #   is_selected  = 1 if this swatch was selected at scrape time
    #
    # Size row:
    #   variant_type = 'size'
    #   name         = size label (e.g. "S(US 36)", "M(EU 38)")
    #   country      = country code (e.g. "US", "EU", "FR")
    #                  NULL when type is plain (no dropdown)
    #   image_url    = NULL
    #   sku_col_id   = NULL
    #   is_selected  = 0
    #
    # Duplicate guard: UNIQUE(product_id, variant_type, name, country)
    # ensures re-scraping the same product does not create duplicate rows.
    # country is coalesced to '' in the unique index because SQLite treats
    # NULL != NULL, which would allow infinite duplicate NULL-country rows.
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS varient (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id   TEXT    NOT NULL,
        variant_type TEXT    NOT NULL CHECK(variant_type IN ('color', 'size')),
        name         TEXT    NOT NULL,
        country      TEXT,
        image_url    TEXT,
        sku_col_id   TEXT,
        is_selected  INTEGER NOT NULL DEFAULT 0 CHECK(is_selected IN (0, 1)),
        created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Unique index: (product_id, variant_type, name, coalesce(country,''))
    # Prevents duplicates on re-scrape while correctly handling NULL country.
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uix_varient_dedup
    ON varient (product_id, variant_type, name, COALESCE(country, ''))
    """)

    conn.commit()
    conn.close()
    print("All tables created (including varient)")


# ---------------------------------------------------------------------------
# Internal migration helper
# ---------------------------------------------------------------------------

def _add_columns_if_missing(cursor, table: str, columns: list) -> None:
    for col_name, col_type in columns:
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass


# =============================================================================
# VARIENT — save / read helpers
# =============================================================================

def save_variants(data: dict) -> dict:
    """
    Persist all variant data from the scraper JSON into the varient table.

    Args:
        data: The full variant dict returned by scrape_product_variants().
              Must contain at minimum:
                {
                  "product_id": "1005010435033239",
                  "variants": {
                    "color": [ {"name": ..., "image_url": ...,
                                "sku_col_id": ..., "selected": bool} ],
                    "size": {
                      "type": "country_mapped" | "plain",
                      "systems": [ {"country": "US", "options": ["S(US 36)", ...]} ],
                      "plain_options": ["S", "M", "L"]
                    }
                  }
                }

    Returns:
        {"inserted": N, "skipped": N, "errors": N}
        inserted — new rows added
        skipped  — duplicates silently ignored (ON CONFLICT DO NOTHING)
        errors   — rows that raised an unexpected exception
    """
    product_id = str(data.get("product_id", "")).strip()
    if not product_id:
        print("[db.save_variants] No product_id in data — skipping")
        return {"inserted": 0, "skipped": 0, "errors": 0}

    variants   = data.get("variants", {})
    color_list = variants.get("color", [])
    size_data  = variants.get("size", {})

    rows = []

    # ── Build color rows ────────────────────────────────────────────────────
    for c in color_list:
        name = (c.get("name") or "").strip()
        if not name:
            continue
        rows.append({
            "product_id":   product_id,
            "variant_type": "color",
            "name":         name,
            "country":      None,
            "image_url":    (c.get("image_url") or "").strip() or None,
            "sku_col_id":   (c.get("sku_col_id") or "").strip() or None,
            "is_selected":  1 if c.get("selected") else 0,
        })

    # ── Build size rows ─────────────────────────────────────────────────────
    size_type = size_data.get("type", "plain")

    if size_type == "country_mapped":
        # Each system is one country; each option is one size label
        for system in size_data.get("systems", []):
            country = (system.get("country") or "").strip() or None
            for opt in system.get("options", []):
                opt = (opt or "").strip()
                if not opt:
                    continue
                rows.append({
                    "product_id":   product_id,
                    "variant_type": "size",
                    "name":         opt,
                    "country":      country,
                    "image_url":    None,
                    "sku_col_id":   None,
                    "is_selected":  0,
                })
    else:
        # Plain sizes — no country
        for opt in size_data.get("plain_options", []):
            opt = (opt or "").strip()
            if not opt:
                continue
            rows.append({
                "product_id":   product_id,
                "variant_type": "size",
                "name":         opt,
                "country":      None,
                "image_url":    None,
                "sku_col_id":   None,
                "is_selected":  0,
            })

    if not rows:
        print(f"[db.save_variants] No rows to insert for product_id={product_id}")
        return {"inserted": 0, "skipped": 0, "errors": 0}

    # ── Insert with duplicate guard ─────────────────────────────────────────
    inserted = skipped = errors = 0
    conn = create_connection()

    try:
        cursor = conn.cursor()
        for row in rows:
            try:
                cursor.execute("""
                    INSERT INTO varient
                        (product_id, variant_type, name, country,
                         image_url, sku_col_id, is_selected)
                    VALUES
                        (:product_id, :variant_type, :name, :country,
                         :image_url, :sku_col_id, :is_selected)
                    ON CONFLICT(product_id, variant_type, name, COALESCE(country, ''))
                    DO NOTHING
                """, row)

                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1

            except Exception as exc:
                errors += 1
                print(f"[db.save_variants] Row error — {row}: {exc}")

        conn.commit()

    except Exception as exc:
        print(f"[db.save_variants] Connection error: {exc}")
        errors += len(rows)
    finally:
        conn.close()

    print(
        f"[db.save_variants] product_id={product_id} | "
        f"inserted={inserted} skipped={skipped} errors={errors}"
    )
    return {"inserted": inserted, "skipped": skipped, "errors": errors}


def get_variants(product_id: str) -> dict:
    """
    Re-assemble the full variant structure from the varient table.

    Args:
        product_id: AliExpress product ID string.

    Returns:
        {
          "product_id": "...",
          "variants": {
            "color": [ {"name": ..., "image_url": ...,
                        "sku_col_id": ..., "selected": bool} ],
            "size": {
              "type": "country_mapped" | "plain",
              "systems": [ {"country": "US", "options": [...]} ],
              "plain_options": [...]
            }
          }
        }
        Returns None if no rows exist for this product_id.
    """
    conn = create_connection()
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT variant_type, name, country, image_url, sku_col_id, is_selected
            FROM varient
            WHERE product_id = ?
            ORDER BY variant_type, id
        """, (str(product_id),)).fetchall()
    except Exception as exc:
        print(f"[db.get_variants] Error: {exc}")
        return None
    finally:
        conn.close()

    if not rows:
        return None

    colors        = []
    sizes_plain   = []
    sizes_mapped  = {}   # country -> [label, ...]

    for row in rows:
        if row["variant_type"] == "color":
            colors.append({
                "name":       row["name"],
                "image_url":  row["image_url"] or "",
                "sku_col_id": row["sku_col_id"] or "",
                "selected":   bool(row["is_selected"]),
            })
        elif row["variant_type"] == "size":
            country = row["country"]
            if country:
                sizes_mapped.setdefault(country, []).append(row["name"])
            else:
                sizes_plain.append(row["name"])

    if sizes_mapped:
        size_block = {
            "type":          "country_mapped",
            "systems":       [{"country": c, "options": o} for c, o in sizes_mapped.items()],
            "plain_options": [],
        }
    else:
        size_block = {
            "type":          "plain",
            "systems":       [],
            "plain_options": sizes_plain,
        }

    return {
        "product_id": str(product_id),
        "variants":   {"color": colors, "size": size_block},
    }


def delete_variants(product_id: str) -> int:
    """
    Delete all variant rows for a product.
    Returns the number of rows deleted.
    """
    conn = create_connection()
    try:
        conn.execute("DELETE FROM varient WHERE product_id = ?", (str(product_id),))
        conn.commit()
        n = conn.execute("SELECT changes()").fetchone()[0]
        print(f"[db.delete_variants] Deleted {n} rows for product_id={product_id}")
        return n
    except Exception as exc:
        print(f"[db.delete_variants] Error: {exc}")
        return 0
    finally:
        conn.close()


def get_variant_summary(product_id: str) -> dict:
    """
    Return a quick summary: how many color and size rows exist per country.
    Useful for a dashboard or API status check.

    Returns e.g.:
        {
          "product_id": "...",
          "color_count": 2,
          "size_count": 84,
          "countries": ["EU", "US", "FR", "DE", "IT", "ES", "UK",
                        "MX", "BR", "AU", "SG", "JP", "KR"]
        }
    """
    conn = create_connection()
    try:
        color_count = conn.execute(
            "SELECT COUNT(*) FROM varient WHERE product_id = ? AND variant_type = 'color'",
            (str(product_id),)
        ).fetchone()[0]

        size_count = conn.execute(
            "SELECT COUNT(*) FROM varient WHERE product_id = ? AND variant_type = 'size'",
            (str(product_id),)
        ).fetchone()[0]

        country_rows = conn.execute(
            "SELECT DISTINCT country FROM varient "
            "WHERE product_id = ? AND variant_type = 'size' AND country IS NOT NULL "
            "ORDER BY country",
            (str(product_id),)
        ).fetchall()

        countries = [r[0] for r in country_rows]

        return {
            "product_id":  str(product_id),
            "color_count": color_count,
            "size_count":  size_count,
            "countries":   countries,
        }
    except Exception as exc:
        print(f"[db.get_variant_summary] Error: {exc}")
        return {}
    finally:
        conn.close()


# =============================================================================
# PRODUCT DETAILS (STAR RATING + DELIVERY DATE)
# =============================================================================

def save_star_rating_delivery(product_id: str, data: dict) -> bool:
    """
    Upsert star rating, delivery date, price, quantity, and ship_country
    for a given AliExpress product ID string.

    Args:
        product_id: AliExpress numeric ID string (e.g. "1005010435033239").
        data: Dict returned by star_rating_scraper.scrape_product_details().
              Expected keys: rating, delivery, price, quantity, ship_country,
              url, scraped_at (all optional).

    Returns:
        True on success, False on error.
    """
    if not product_id:
        print("[db.save_star_rating_delivery] No product_id — skipping")
        return False

    conn = create_connection()
    try:
        conn.execute("""
            INSERT INTO product_details
                (product_id, star_rating, delivery, price, quantity, ship_country, url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(product_id) DO UPDATE SET
                star_rating  = excluded.star_rating,
                delivery     = excluded.delivery,
                price        = excluded.price,
                quantity     = excluded.quantity,
                ship_country = excluded.ship_country,
                url          = excluded.url,
                scraped_at   = CURRENT_TIMESTAMP
        """, (
            str(product_id),
            data.get("rating"),
            data.get("delivery"),
            data.get("price"),
            data.get("quantity"),
            data.get("ship_country"),
            data.get("url"),
        ))
        conn.commit()
        print(f"[db] product_details saved (product_id={product_id})")
        return True
    except Exception as exc:
        print(f"[db] save_star_rating_delivery error: {exc}")
        return False
    finally:
        conn.close()


def get_product_details(product_id: str) -> Optional[dict]:
    """
    Return stored star-rating/delivery data for a product ID, or None.
    """
    conn = create_connection()
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM product_details WHERE product_id = ?",
            (str(product_id),),
        ).fetchone()
        return dict(row) if row else None
    except Exception as exc:
        print(f"[db] get_product_details error: {exc}")
        return None
    finally:
        conn.close()


def get_all_product_details(limit: int = 100) -> list:
    """Return all rows from product_details ordered by most recent."""
    conn = create_connection()
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM product_details ORDER BY scraped_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        print(f"[db] get_all_product_details error: {exc}")
        return []
    finally:
        conn.close()


# =============================================================================
# TRANSLATION HELPERS
# =============================================================================

def insert_translation(
    url_id: int,
    language: str,
    title: str = "",
    description: str = "",
    specification: str = "",
) -> bool:
    conn = create_connection()
    try:
        conn.execute("""
            INSERT INTO translation (url_id, language, title, description, specification)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(url_id, language) DO UPDATE SET
                title         = excluded.title,
                description   = excluded.description,
                specification = excluded.specification,
                translated_at = datetime('now')
        """, (url_id, language, title or "", description or "", specification or ""))
        conn.commit()
        print(f"[db] Translation saved (url_id={url_id}, language={language})")
        return True
    except Exception as exc:
        print(f"[db] insert_translation error: {exc}")
        return False
    finally:
        conn.close()


def get_translations(url_id: int) -> list:
    conn = create_connection()
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT url_id, language, title, description, specification, translated_at "
            "FROM translation WHERE url_id = ? ORDER BY language",
            (url_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        print(f"[db] get_translations error: {exc}")
        return []
    finally:
        conn.close()


def get_translation(url_id: int, language: str) -> Optional[dict]:
    conn = create_connection()
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT url_id, language, title, description, specification, translated_at "
            "FROM translation WHERE url_id = ? AND language = ?",
            (url_id, language),
        ).fetchone()
        return dict(row) if row else None
    except Exception as exc:
        print(f"[db] get_translation error: {exc}")
        return None
    finally:
        conn.close()


def delete_translations(url_id: int, language: str = None) -> int:
    conn = create_connection()
    try:
        if language:
            conn.execute(
                "DELETE FROM translation WHERE url_id = ? AND language = ?",
                (url_id, language),
            )
        else:
            conn.execute("DELETE FROM translation WHERE url_id = ?", (url_id,))
        conn.commit()
        return conn.execute("SELECT changes()").fetchone()[0]
    except Exception as exc:
        print(f"[db] delete_translations error: {exc}")
        return 0
    finally:
        conn.close()


# =============================================================================
# RESTRICTED KEYWORDS
# =============================================================================

def load_restricted_keywords_from_csv(csv_path: str) -> int:
    if not os.path.exists(csv_path):
        print(f"[db] CSV not found: {csv_path}")
        return 0

    conn    = create_connection()
    cursor  = conn.cursor()
    count   = 0
    skipped = 0

    try:
        with open(csv_path, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                keyword = (
                    row.get('desc_and_spec_restricted_keywords') or
                    row.get('"desc_and_spec_restricted_keywords"') or
                    list(row.values())[0]
                )
                if not keyword:
                    continue
                keyword = keyword.strip().strip('"')
                if not keyword:
                    continue
                try:
                    cursor.execute(
                        "INSERT OR IGNORE INTO restricted_keywords (keyword) VALUES (?)",
                        (keyword,)
                    )
                    count   += cursor.rowcount
                    skipped += 1 - cursor.rowcount
                except Exception:
                    skipped += 1

        conn.commit()
        print(f"[db] Keywords loaded: {count} inserted, {skipped} skipped")
        return count

    except Exception as exc:
        print(f"[db] Error loading restricted keywords: {exc}")
        return 0
    finally:
        conn.close()


def get_restricted_keywords() -> list:
    conn = create_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT keyword FROM restricted_keywords")
        return [row[0].lower().strip() for row in cursor.fetchall() if row[0]]
    except Exception as exc:
        print(f"[db] Error reading restricted keywords: {exc}")
        return []
    finally:
        conn.close()


def filter_restricted_keywords(text: str, keywords: list = None) -> tuple:
    if not text:
        return text, []
    if keywords is None:
        keywords = get_restricted_keywords()
    found   = []
    cleaned = text
    for kw in keywords:
        pattern = re.compile(re.escape(kw), re.IGNORECASE)
        if pattern.search(cleaned):
            found.append(kw)
            cleaned = pattern.sub('[REMOVED]', cleaned)
    return cleaned, found


# =============================================================================
# DEDUPLICATION HELPERS
# =============================================================================

def get_processed_ids(job_id: str = None) -> list:
    conn = create_connection()
    try:
        cursor = conn.cursor()
        if job_id:
            cursor.execute(
                "SELECT id FROM processed_ids WHERE job_id = ? ORDER BY processed_at",
                (job_id,),
            )
        else:
            cursor.execute("SELECT id FROM processed_ids ORDER BY processed_at")
        return [row[0] for row in cursor.fetchall()]
    except Exception as exc:
        print(f"[db] Error reading processed_ids: {exc}")
        return []
    finally:
        conn.close()


def get_processed_id_count(job_id: str = None) -> int:
    conn = create_connection()
    try:
        cursor = conn.cursor()
        if job_id:
            cursor.execute(
                "SELECT COUNT(*) FROM processed_ids WHERE job_id = ?", (job_id,)
            )
        else:
            cursor.execute("SELECT COUNT(*) FROM processed_ids")
        row = cursor.fetchone()
        return row[0] if row else 0
    except Exception as exc:
        print(f"[db] Error counting processed_ids: {exc}")
        return 0
    finally:
        conn.close()


# =============================================================================
# SELLER INFO
# =============================================================================

SELLER_FIELDS = [
    'store_name', 'store_id', 'store_url', 'seller_id',
    'seller_positive_rate', 'seller_rating', 'seller_communication',
    'seller_shipping_speed', 'seller_country', 'store_open_date',
    'seller_level', 'seller_total_reviews', 'seller_positive_num', 'is_top_rated'
]


def insert_seller_info(product_id: int, seller_data: dict) -> bool:
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
        print(f"Seller info saved (product_id={product_id})")
        return True
    except sqlite3.IntegrityError:
        cursor.execute("""
            UPDATE seller_info SET
                store_name=?, store_id=?, store_url=?, seller_id=?,
                seller_positive_rate=?, seller_rating=?, seller_communication=?,
                seller_shipping_speed=?, seller_country=?, store_open_date=?,
                seller_level=?, seller_total_reviews=?, seller_positive_num=?,
                is_top_rated=?
            WHERE product_id=?
        """, (
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
            product_id,
        ))
        conn.commit()
        print(f"Seller info updated (product_id={product_id})")
        return True
    except Exception as exc:
        print(f"Seller info error: {exc}")
        return False
    finally:
        conn.close()


def get_seller_info(product_id: int) -> dict:
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


# =============================================================================
# COMPLIANCE INFO
# =============================================================================

def insert_compliance_info(product_id: int, compliance_data: dict) -> bool:
    if not compliance_data:
        return False
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO compliance_info (
                product_id, compliance_product_id,
                manufacturer_name, manufacturer_address,
                manufacturer_email, manufacturer_phone,
                eu_responsible_name, eu_responsible_address,
                eu_responsible_email, eu_responsible_phone
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
        if cursor.rowcount > 0:
            print(f"Compliance info saved (product_id={product_id})")
        return True
    except Exception as exc:
        print(f"Compliance info error: {exc}")
        return False
    finally:
        conn.close()


def get_compliance_info(product_id: int) -> list:
    conn = create_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM compliance_info WHERE product_id = ?", (product_id,))
        return [dict(r) for r in cursor.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


# =============================================================================
# SCRAPED PRODUCTS
# =============================================================================

def insert_scraped_product(url: str, attributes: dict):
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
            json.dumps(attributes),
        ))
        conn.commit()
        product_id = cursor.lastrowid
        print(f"Scraped product saved (product_id={product_id})")
        return product_id
    except sqlite3.IntegrityError:
        cursor.execute("SELECT product_id FROM scraped_products WHERE url = ?", (url,))
        row = cursor.fetchone()
        product_id = row[0] if row else None
        print(f"Product already exists (product_id={product_id})")
        return product_id
    except Exception as exc:
        print(f"Error inserting scraped product: {exc}")
        return None
    finally:
        conn.close()


# =============================================================================
# CATEGORY ASSIGNMENT
# =============================================================================

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


# =============================================================================
# MAPPED PRODUCTS
# =============================================================================

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
            json.dumps({k: v for k, v in mapped_data.items()}),
        ))
        conn.commit()
        print(f"Mapped product saved (product_id={product_id})")
        return True
    except Exception as exc:
        print(f"Mapped product error: {exc}")
        return False
    finally:
        conn.close()


# =============================================================================
# TEMPLATE OUTPUT
# =============================================================================

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
    except Exception as exc:
        print(f"Template output error: {exc}")
        return False
    finally:
        conn.close()


# =============================================================================
# PROCESSING LOGS
# =============================================================================

def log_processing(product_id, url, step, status, message=""):
    conn   = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO processing_logs (product_id, url, step, status, message)
            VALUES (?, ?, ?, ?, ?)
        """, (product_id, url, step, status, message))
        conn.commit()
    except Exception as exc:
        print(f"Log error: {exc}")
    finally:
        conn.close()


# =============================================================================
# SPECIFICATIONS
# =============================================================================

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
            enhanced_data.get('product_type', ''),
        ))
        conn.commit()
        print(f"Enhanced content saved (product_id={product_id})")
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as exc:
        print(f"Enhanced content error: {exc}")
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
            original_specs.get("gender", ""),
        ))
        conn.commit()
        print(f"Original specs saved (product_id={product_id})")
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as exc:
        print(f"Original specs error: {exc}")
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
            enhanced_specs.get("gender", ""),
        ))
        conn.commit()
        print(f"Enhanced specs saved (product_id={product_id})")
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as exc:
        print(f"Enhanced specs error: {exc}")
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
    except Exception as exc:
        print(f"Audit log error: {exc}")
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
    print(f"Audit log written (product_id={product_id})")


def get_all_manufacturer_info(limit: int = 10) -> list:
    conn = create_connection()
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM compliance_info ORDER BY extracted_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        print(f"[db] get_all_manufacturer_info error: {exc}")
        return []
    finally:
        conn.close()


# =============================================================================
# BACKWARD COMPAT
# =============================================================================

def create_table():
    create_all_tables()


def create_categories_table():
    pass


# =============================================================================
# CLI
# =============================================================================

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'load-keywords':
        csv_file = sys.argv[2] if len(sys.argv) > 2 else 'restricted_keywords.csv'
        create_all_tables()
        n = load_restricted_keywords_from_csv(csv_file)
        print(f"Loaded {n} keywords from {csv_file}")
    elif len(sys.argv) > 1 and sys.argv[1] == 'test-variants':
        # Quick smoke test: insert sample data and read it back
        create_all_tables()
        sample = {
            "product_id": "1005010435033239",
            "variants": {
                "color": [
                    {"name": "White", "image_url": "https://example.com/white.jpg",
                     "sku_col_id": "14-29", "selected": True},
                    {"name": "Black", "image_url": "https://example.com/black.jpg",
                     "sku_col_id": "14-193", "selected": False},
                ],
                "size": {
                    "type": "country_mapped",
                    "systems": [
                        {"country": "EU", "options": ["S(EU 36)", "M(EU 38)", "L(EU 40/42)"]},
                        {"country": "US", "options": ["S(US 4)", "M(US 6)", "L(US 08/10)"]},
                        {"country": "UK", "options": ["S(UK 8)", "M(UK 10)", "L(UK 12/14)"]},
                    ],
                    "plain_options": [],
                },
            },
        }
        result = save_variants(sample)
        print(f"Save result: {result}")
        summary = get_variant_summary("1005010435033239")
        print(f"Summary: {summary}")
        recovered = get_variants("1005010435033239")
        print(json.dumps(recovered, indent=2, ensure_ascii=False))
    else:
        create_all_tables()
        print("Tables created. Commands: load-keywords <csv> | test-variants")
