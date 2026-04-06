"""
FastAPI Server - HYBRID APPROACH v2.6
Pipeline: Scrape → Store → Enhance → Categorize → Map → Excel
Compliance info extracted and stored separately.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import sqlite3
import logging
import json
from typing import List, Dict, Any
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Octopia Template Pipeline",
    version="2.6.0"
)

DB_NAME = "products.db"
SPEC_FIELDS = [
    'brand', 'color', 'dimensions', 'weight', 'material',
    'certifications', 'country_of_origin', 'warranty', 'product_type'
]
SELLER_FIELDS = [
    'store_name', 'store_id', 'store_url', 'seller_id',
    'seller_positive_rate', 'seller_rating', 'seller_communication',
    'seller_shipping_speed', 'seller_country', 'store_open_date',
    'seller_level', 'seller_total_reviews', 'seller_positive_num', 'is_top_rated'
]


@app.on_event("startup")
def startup_event():
    try:
        from db import create_all_tables
        create_all_tables()
        logger.info("✅ API Ready")
    except Exception as e:
        logger.error(f"Startup error: {e}")


class ProductURLRequest(BaseModel):
    url: str
    extract_compliance: bool = True

    class Config:
        json_schema_extra = {
            "example": {
                "url": "https://www.aliexpress.com/item/1005010388288135.html",
                "extract_compliance": True
            }
        }


class BulkProductRequest(BaseModel):
    urls: List[str]
    extract_compliance: bool = False


@app.get("/", tags=["Info"])
def root():
    return {
        "status":  "running",
        "service": "Octopia Template Pipeline",
        "version": "2.6.0",
        "features": [
            "Camoufox Firefox browser (API interception)",
            "Seller info stored in seller_info table",
            "EU compliance info extracted and stored",
            "Content enhancement via OpenAI",
            "Octopia categorization",
            "Template mapping + Excel generation",
        ]
    }


@app.get("/health", tags=["Info"])
def health_check():
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.close()
        return {"status": "healthy"}
    except Exception:
        return {"status": "error"}


# ─────────────────────────────────────────
# MAIN PROCESSING FUNCTION
# ─────────────────────────────────────────

def process_product_complete(url: str, extract_compliance: bool = True) -> Dict[str, Any]:
    product_id = None

    try:
        logger.info(f"\n🚀 Processing: {url}")

        # ── Imports ──────────────────────────────────────────────────────
        from scraper import get_product_info, resolve_category
        from category_utils import assign_category
        from data_mapper import map_scraped_data_to_template, validate_mapped_data
        from template_filler import fill_template_for_product
        from openai_client import improve_product_content
        from db import (
            create_all_tables,
            insert_scraped_product,
            insert_seller_info,
            insert_compliance_info,
            insert_category_assignment,
            insert_mapped_product,
            insert_template_output,
            insert_enhanced_content,
            insert_original_specifications,
            insert_enhanced_specifications,
            log_all_spec_audits,
            log_processing,
        )

        create_all_tables()

        TEMPLATE_PATH        = None
        FILLED_TEMPLATES_DIR = "./filled_templates"
        os.makedirs(FILLED_TEMPLATES_DIR, exist_ok=True)

        for candidate in [
            "pdt_template_fr-FR_20260305_090255.xlsm",
            "./pdt_template_fr-FR_20260305_090255.xlsm",
            os.path.join(os.path.dirname(__file__), "pdt_template_fr-FR_20260305_090255.xlsm"),
        ]:
            if os.path.exists(candidate):
                TEMPLATE_PATH = candidate
                logger.info(f"📄 Template found: {candidate}")
                break

        if not TEMPLATE_PATH:
            logger.warning("⚠️  Template file not found — Excel will be skipped")

        # ── STEP 1: SCRAPE ───────────────────────────────────────────────
        logger.info("📥 Scraping with Camoufox...")
        scraped_data = get_product_info(url, extract_compliance=extract_compliance)

        if not scraped_data:
            return {"success": False, "url": url, "error": "Scraping failed",
                    "timestamp": datetime.now().isoformat()}

        title       = scraped_data.get("title", "")
        description = scraped_data.get("description", "")

        if not title:
            return {"success": False, "url": url, "error": "No title extracted",
                    "timestamp": datetime.now().isoformat()}

        scraped_specs_count = len([k for k in SPEC_FIELDS if scraped_data.get(k)])
        logger.info(f"✅ Extracted {len(scraped_data)} attributes "
                    f"({scraped_specs_count} spec fields)")

        # ── STEP 2: STORE SCRAPED DATA ───────────────────────────────────
        logger.info("💾 Storing scraped data...")
        product_id = insert_scraped_product(url, scraped_data)

        if not product_id:
            return {"success": False, "url": url, "error": "Failed to store scraped data",
                    "timestamp": datetime.now().isoformat()}

        log_processing(product_id, url, "scraping", "success")

        # ── STEP 2B: STORE SELLER INFO ───────────────────────────────────
        logger.info("🏪 Storing seller info...")
        seller_data = {k: scraped_data.get(k, '') for k in SELLER_FIELDS}
        insert_seller_info(product_id, seller_data)
        log_processing(product_id, url, "seller_info", "success")

        # ── STEP 2C: STORE COMPLIANCE INFO ───────────────────────────────
        compliance_data = scraped_data.get('compliance', {})
        if compliance_data:
            logger.info(f"🔒 Storing compliance info: {list(compliance_data.keys())}")
            insert_compliance_info(product_id, compliance_data)
            log_processing(product_id, url, "compliance_info", "success")
        else:
            logger.info("ℹ️ No compliance info (non-EU page or not found)")

        # ── STEP 2D: STORE ORIGINAL SPECS ────────────────────────────────
        logger.info("📋 Saving original specifications...")
        insert_original_specifications(product_id, scraped_data)

        # ── STEP 3: ENHANCE CONTENT ──────────────────────────────────────
        logger.info("🤖 Enhancing product content with OpenAI...")
        product_data_for_llm = {
            k: v for k, v in scraped_data.items()
            if k not in SELLER_FIELDS and k != 'compliance'
        }

        try:
            enhanced = improve_product_content(
                title=title,
                description=description,
                specifications=product_data_for_llm,
                category=None
            )
            if not enhanced:
                raise ValueError("OpenAI returned None")
        except Exception as e:
            logger.warning(f"Enhancement skipped: {e}")
            enhanced = {
                "title": title,
                "description": description,
                "bullet_points": scraped_data.get("bullet_points", []),
                "html_description": "",
                "specifications_enhanced": {}
            }

        specs_enhanced = enhanced.get("specifications_enhanced", {})
        insert_enhanced_specifications(product_id, specs_enhanced)

        enriched_data_for_template = scraped_data.copy()
        enriched_data_for_template['title']            = enhanced.get('title', title)
        enriched_data_for_template['description']      = enhanced.get('description', description)
        enriched_data_for_template['bullet_points']    = enhanced.get('bullet_points', [])
        enriched_data_for_template['html_description'] = enhanced.get('html_description', '')

        for field in SPEC_FIELDS:
            enh_val = specs_enhanced.get(field, '')
            enriched_data_for_template[field] = enh_val if (enh_val and enh_val.strip()) else ""

        for field in SELLER_FIELDS:
            enriched_data_for_template[field] = scraped_data.get(field, '')

        insert_enhanced_content(product_id, enriched_data_for_template)
        log_all_spec_audits(product_id, scraped_data, specs_enhanced,
                            enriched_data_for_template)

        # ── STEP 4: CATEGORIZE ───────────────────────────────────────────
        logger.info("🏷️  Categorizing...")

        # First try category from scraper (from mtop API)
        scraper_category = resolve_category(scraped_data)
        logger.info(f"   Scraper category: {scraper_category}")

        try:
            category = assign_category(
                enhanced.get("title", title),
                enhanced.get("description", description)
            )
            # If scraper found a category with high confidence, prefer it
            if scraper_category['confidence'] >= 0.9 and scraper_category['category_id'] != '0':
                category = scraper_category
                logger.info(f"   Using scraper category: {category}")
        except Exception as e:
            logger.warning(f"Categorization fallback: {e}")
            category = scraper_category if scraper_category['category_id'] != '0' else {
                "category_id": "0", "category_name": "Unknown",
                "category_leaf": "Unknown", "confidence": 0.0
            }

        insert_category_assignment(
            product_id,
            category.get("category_id", "0"), category.get("category_name", "Unknown"),
            category.get("category_id", "0"), category.get("category_name", "Unknown"),
            category.get("confidence", 0.0)
        )
        log_processing(product_id, url, "categorization", "success")

        # ── STEP 5: MAP ──────────────────────────────────────────────────
        logger.info("🗺️  Mapping to template columns...")
        mapped_data = {}
        is_valid    = False

        try:
            mapped_data = map_scraped_data_to_template(enriched_data_for_template)
            is_valid, missing = validate_mapped_data(mapped_data)
            insert_mapped_product(product_id, category.get("category_id", "0"), mapped_data)
            log_processing(product_id, url, "mapping", "success" if is_valid else "warning")
            logger.info(f"✅ {len(mapped_data)} fields mapped")
        except Exception as e:
            logger.warning(f"Mapping error: {e}")
            log_processing(product_id, url, "mapping", "error", str(e))

        # ── STEP 6: GENERATE TEMPLATE ─────────────────────────────────────
        logger.info("📋 Generating Excel template...")
        template_file = None

        if TEMPLATE_PATH:
            try:
                template_file = fill_template_for_product(
                    TEMPLATE_PATH, mapped_data, product_id, FILLED_TEMPLATES_DIR,
                    category_id=category.get("category_id", "0"),
                    category_name=category.get("category_leaf", "Unknown")
                )
                if template_file and os.path.exists(template_file):
                    insert_template_output(
                        product_id, category.get("category_id", "0"),
                        "xlsm", template_file, os.path.basename(template_file)
                    )
                    log_processing(product_id, url, "template_fill", "success")
                    logger.info(f"✅ Template: {os.path.basename(template_file)}")
                else:
                    logger.warning("⚠️  Template filler returned no file path")
            except Exception as e:
                logger.error(f"❌ Template generation failed: {e}", exc_info=True)
                log_processing(product_id, url, "template_fill", "error", str(e))
        else:
            logger.warning("⚠️  Skipping template — .xlsm not found")

        logger.info("✅ Processing complete\n")

        return {
            "success":    True,
            "product_id": product_id,
            "url":        url,

            "original": {
                "title":       title,
                "description": (description[:200] + "..." if len(description) > 200
                                else description),
                **{f: scraped_data.get(f, "") for f in SPEC_FIELDS},
                "images": sum(1 for i in range(1, 21) if scraped_data.get(f"image_{i}"))
            },

            "seller": {f: scraped_data.get(f, "") for f in SELLER_FIELDS},

            "compliance": compliance_data,

            "category": {
                "id":         category.get("category_id", ""),
                "name":       category.get("category_name", ""),
                "leaf":       category.get("category_leaf", ""),
                "path":       category.get("category_path", ""),
                "confidence": round(category.get("confidence", 0.0), 2)
            },

            "enhanced": {
                "title":                   enhanced.get("title", ""),
                "description":             (enhanced.get("description", "")[:200] + "..."
                                            if enhanced.get("description") else ""),
                "bullet_points":           enhanced.get("bullet_points", [])[:3],
                "has_html_description":    bool(enhanced.get("html_description", "")),
                "specifications_enhanced": specs_enhanced
            },

            "template": {
                "file":           os.path.basename(template_file) if template_file else None,
                "columns_mapped": len(mapped_data),
                "fields_valid":   is_valid,
            },

            "extracted": {
                "specifications": sum(1 for k in SPEC_FIELDS
                                      if enriched_data_for_template.get(k)),
                "images":         sum(1 for i in range(1, 21) if scraped_data.get(f"image_{i}")),
                "seller_fields":  len([k for k in SELLER_FIELDS if scraped_data.get(k)]),
                "compliance_fields": len(compliance_data),
            },

            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return {"success": False, "url": url, "product_id": product_id,
                "error": str(e), "timestamp": datetime.now().isoformat()}


# ─────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────

@app.post("/generate-product", tags=["Product Processing"])
def generate_product(req: ProductURLRequest):
    if not req.url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")
    return process_product_complete(req.url, extract_compliance=req.extract_compliance)


@app.post("/generate-products", tags=["Product Processing"])
def generate_products(req: BulkProductRequest):
    if not req.urls:
        raise HTTPException(status_code=400, detail="URLs list cannot be empty")
    if len(req.urls) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 URLs per request")

    results    = []
    successful = failed = 0
    for url in req.urls:
        result = process_product_complete(url, extract_compliance=req.extract_compliance)
        results.append(result)
        if result.get("success"):
            successful += 1
        else:
            failed += 1

    return {"total": len(req.urls), "successful": successful,
            "failed": failed, "results": results,
            "timestamp": datetime.now().isoformat()}


# ─────────────────────────────────────────
# DATABASE ENDPOINTS
# ─────────────────────────────────────────

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/scraped-products", tags=["Database"])
def view_scraped_products(limit: int = 100):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM scraped_products ORDER BY scraped_at DESC LIMIT {min(limit,1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/seller-info", tags=["Database"])
def view_seller_info(limit: int = 100):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM seller_info ORDER BY scraped_at DESC LIMIT {min(limit,1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/seller-info/{product_id}", tags=["Database"])
def get_seller_info_by_product(product_id: int):
    try:
        conn = get_db_connection()
        row  = conn.execute(
            "SELECT * FROM seller_info WHERE product_id = ?", (product_id,)
        ).fetchone()
        conn.close()
        return dict(row) if row else {"message": f"No seller info for product {product_id}"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/compliance-info", tags=["Database"])
def view_compliance_info(limit: int = 100):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM compliance_info ORDER BY extracted_at DESC LIMIT {min(limit,1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/compliance-info/{product_id}", tags=["Database"])
def get_compliance_by_product(product_id: int):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            "SELECT * FROM compliance_info WHERE product_id = ?", (product_id,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": f"No compliance for product {product_id}"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/mapped-products", tags=["Database"])
def view_mapped_products(limit: int = 100):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM mapped_products ORDER BY mapped_at DESC LIMIT {min(limit,1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/template-outputs", tags=["Database"])
def view_template_outputs(limit: int = 100):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM template_outputs ORDER BY created_at DESC LIMIT {min(limit,1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/processing-logs", tags=["Database"])
def view_processing_logs(limit: int = 500):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM processing_logs ORDER BY log_time DESC LIMIT {min(limit,1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/enhanced-products", tags=["Database"])
def view_enhanced_products(limit: int = 100):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM enhanced_content ORDER BY id DESC LIMIT {min(limit,1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/original-specifications", tags=["Database"])
def view_original_specifications(limit: int = 100):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM original_specifications ORDER BY extracted_at DESC LIMIT {min(limit,1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/enhanced-specifications", tags=["Database"])
def view_enhanced_specifications(limit: int = 100):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM enhanced_specifications ORDER BY enhanced_at DESC LIMIT {min(limit,1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/specification-audit", tags=["Database"])
def view_specification_audit(limit: int = 200):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM specification_audit_log ORDER BY recorded_at DESC LIMIT {min(limit,1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/stats", tags=["Database"])
def get_stats():
    try:
        conn   = get_db_connection()
        tables = [
            "scraped_products", "mapped_products", "template_outputs",
            "processing_logs", "category_assignments", "enhanced_content",
            "original_specifications", "enhanced_specifications",
            "specification_audit_log", "seller_info", "compliance_info"
        ]
        stats = {}
        for table in tables:
            try:
                stats[table] = conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
            except Exception:
                stats[table] = 0
        conn.close()
        return stats
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8686))
    logger.info("🚀 Octopia Template Pipeline v2.6 — Camoufox + Compliance edition")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
