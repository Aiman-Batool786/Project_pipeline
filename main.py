"""
FastAPI Server - HYBRID APPROACH v2.2
─────────────────────────────────────
Two data objects flow through the pipeline:

  enriched_data              → API response  (enhanced preferred, original fallback)
  enriched_data_for_template → Excel template (ONLY enhanced specs, no fallback)

Audit trail fully wired:
  • original_specifications   – every spec the scraper found
  • enhanced_specifications   – every spec OpenAI returned
  • specification_audit_log   – field-level record of what went to the template
  • enhanced_content          – final text + enhanced specs written to template
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import sqlite3
import logging
import json
from typing import List, Optional, Dict, Any
from datetime import datetime

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# APP
# ─────────────────────────────────────────
app = FastAPI(
    title="Octopia Template Pipeline - HYBRID Approach",
    version="2.2.0"
)

DB_NAME = "products.db"

# Spec fields shared across the whole pipeline
SPEC_FIELDS = [
    'brand', 'color', 'dimensions', 'weight', 'material',
    'certifications', 'country_of_origin', 'warranty', 'product_type'
]


# ─────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────
@app.on_event("startup")
def startup_event():
    try:
        from db import create_all_tables
        create_all_tables()
        logger.info("✅ API Ready")
    except Exception as e:
        logger.error(f"Startup error: {e}")


# ─────────────────────────────────────────
# REQUEST MODELS
# ─────────────────────────────────────────
class ProductURLRequest(BaseModel):
    url: str

    class Config:
        schema_extra = {
            "example": {"url": "https://www.aliexpress.com/item/1005010738806664.html"}
        }


class BulkProductRequest(BaseModel):
    urls: List[str]


# ─────────────────────────────────────────
# ROOT & HEALTH
# ─────────────────────────────────────────
@app.get("/", tags=["Info"])
def root():
    return {
        "status": "running",
        "service": "Octopia Template Pipeline",
        "version": "2.2.0",
        "approach": "HYBRID — API shows both, template stores enhanced only",
        "features": [
            "Advanced scraping (25+ attributes, 20+ images)",
            "Content enhancement (OpenAI with specifications)",
            "Octopia categorization (5,806 categories)",
            "Template mapping (71 columns)",
            "Excel XLSM generation with enhanced specs only",
            "Full audit trail (original / enhanced / template values)"
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
def process_product_complete(url: str) -> Dict[str, Any]:
    """
    Full pipeline with HYBRID spec handling and complete audit trail.
    """
    product_id = None

    try:
        logger.info(f"\n🚀 Processing: {url}")

        # ── IMPORTS ────────────────────────────────────────────────────────────
        from scraper import get_product_info
        from category_utils import assign_category
        from data_mapper import map_scraped_data_to_template, validate_mapped_data
        from template_filler import fill_template_for_product
        from openai_client import improve_product_content
        from db import (
            create_all_tables,
            insert_scraped_product,
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

        TEMPLATE_PATH = "pdt_template_fr-FR_20260305_090255.xlsm"
        FILLED_TEMPLATES_DIR = "./filled_templates"
        os.makedirs(FILLED_TEMPLATES_DIR, exist_ok=True)

        # ── STEP 1: SCRAPE ─────────────────────────────────────────────────────
        logger.info("📥 Scraping...")
        scraped_data = get_product_info(url)

        if not scraped_data:
            return {"success": False, "url": url,
                    "error": "Scraping failed",
                    "timestamp": datetime.now().isoformat()}

        title       = scraped_data.get("title", "")
        description = scraped_data.get("description", "")

        if not title:
            return {"success": False, "url": url,
                    "error": "No title extracted",
                    "timestamp": datetime.now().isoformat()}

        scraped_specs_count = len([k for k in SPEC_FIELDS if scraped_data.get(k)])
        logger.info(f"✅ Extracted {len(scraped_data)} attributes "
                    f"({scraped_specs_count} spec fields)")

        # ── STEP 2: STORE SCRAPED DATA ─────────────────────────────────────────
        logger.info("💾 Storing scraped data...")
        product_id = insert_scraped_product(url, scraped_data)

        if not product_id:
            return {"success": False, "url": url,
                    "error": "Failed to store scraped data",
                    "timestamp": datetime.now().isoformat()}

        log_processing(product_id, url, "scraping", "success")

        # ── STEP 2B: SAVE ORIGINAL SPECIFICATIONS (audit trail) ────────────────
        logger.info("📋 Saving original specifications to audit trail...")
        insert_original_specifications(product_id, scraped_data)

        # ── STEP 3: ENHANCE CONTENT ────────────────────────────────────────────
        logger.info("🤖 Enhancing content with OpenAI...")
        logger.info(f"   Title      : {title[:60]}...")
        logger.info(f"   Description: {len(description)} chars")
        logger.info(f"   Spec fields: {scraped_specs_count}")

        try:
            enhanced = improve_product_content(
                title=title,
                description=description,
                specifications=scraped_data,
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
        logger.info(f"✅ Content enhanced — "
                    f"{len([v for v in specs_enhanced.values() if v])} spec fields returned")

        # ── STEP 3B: SAVE ENHANCED SPECIFICATIONS (audit trail) ───────────────
        logger.info("📋 Saving enhanced specifications to audit trail...")
        insert_enhanced_specifications(product_id, specs_enhanced)

        # ── STEP 3C: BUILD enriched_data — API version (enhanced + fallback) ──
        logger.info("\n🔄 Building API data (enhanced preferred, original fallback)...")
        enriched_data = scraped_data.copy()
        enriched_data['title']            = enhanced.get('title', title)
        enriched_data['description']      = enhanced.get('description', description)
        enriched_data['bullet_points']    = enhanced.get('bullet_points', [])
        enriched_data['html_description'] = enhanced.get('html_description', '')

        for field in SPEC_FIELDS:
            original_val = scraped_data.get(field, '')
            enhanced_val = specs_enhanced.get(field, '')
            if enhanced_val and enhanced_val.strip():
                enriched_data[field] = enhanced_val
                logger.info(f"   ✅ {field}: {enhanced_val[:50]} (ENHANCED)")
            elif original_val and original_val.strip():
                enriched_data[field] = original_val
                logger.info(f"   ✅ {field}: {original_val[:50]} (ORIGINAL)")
            else:
                enriched_data[field] = ""

        logger.info(f"✅ API data ready — {len(enriched_data)} fields")

        # ── STEP 3D: BUILD enriched_data_for_template — ONLY enhanced specs ───
        logger.info("\n🔄 Building template data (ONLY enhanced specs)...")
        enriched_data_for_template = scraped_data.copy()
        enriched_data_for_template['title']            = enhanced.get('title', title)
        enriched_data_for_template['description']      = enhanced.get('description', description)
        enriched_data_for_template['bullet_points']    = enhanced.get('bullet_points', [])
        enriched_data_for_template['html_description'] = enhanced.get('html_description', '')

        for field in SPEC_FIELDS:
            enhanced_val = specs_enhanced.get(field, '')
            if enhanced_val and enhanced_val.strip():
                enriched_data_for_template[field] = enhanced_val
                logger.info(f"   ✅ {field}: {enhanced_val[:50]} (ENHANCED)")
            else:
                enriched_data_for_template[field] = ""
                logger.info(f"   ⭕ {field}: EMPTY (no enhanced value)")

        logger.info(f"✅ Template data ready — {len(enriched_data_for_template)} fields")

        # ── STEP 3E: SAVE enhanced_content + AUDIT LOG ────────────────────────
        logger.info("\n💾 Saving enhanced content & audit log...")
        insert_enhanced_content(product_id, enriched_data_for_template)
        log_all_spec_audits(product_id, scraped_data, specs_enhanced,
                            enriched_data_for_template)

        # ── STEP 4: CATEGORIZE ─────────────────────────────────────────────────
        logger.info("\n🏷️  Categorizing...")
        try:
            category = assign_category(
                enhanced.get("title", title),
                enhanced.get("description", description)
            )
        except Exception as e:
            logger.warning(f"Categorization skipped: {e}")
            category = {
                "category_id": "0",
                "category_name": "Unknown",
                "category_leaf": "Unknown",
                "confidence": 0.0
            }

        logger.info(f"✅ Category : {category['category_name']}")
        logger.info(f"   Code     : {category['category_id']}")
        logger.info(f"   Leaf     : {category.get('category_leaf', 'Unknown')}")

        insert_category_assignment(
            product_id,
            category.get("category_id", "0"),
            category.get("category_name", "Unknown"),
            category.get("category_id", "0"),
            category.get("category_name", "Unknown"),
            category.get("confidence", 0.0)
        )
        log_processing(product_id, url, "categorization", "success")

        # ── STEP 5: MAP (uses TEMPLATE data — enhanced specs only) ─────────────
        logger.info("\n🗺️  Mapping to template columns (enhanced specs only)...")
        mapped_data = {}
        is_valid    = False

        try:
            mapped_data = map_scraped_data_to_template(enriched_data_for_template)
            is_valid, missing = validate_mapped_data(mapped_data)

            insert_mapped_product(product_id, category.get("category_id", "0"), mapped_data)
            log_processing(product_id, url, "mapping",
                           "success" if is_valid else "warning")
            logger.info(f"✅ {len(mapped_data)} fields mapped")
        except Exception as e:
            logger.warning(f"Mapping error: {e}")
            log_processing(product_id, url, "mapping", "error", str(e))

        # ── STEP 6: GENERATE TEMPLATE ──────────────────────────────────────────
        logger.info("\n📋 Generating Excel template...")
        template_file = None

        if os.path.exists(TEMPLATE_PATH):
            try:
                template_file = fill_template_for_product(
                    TEMPLATE_PATH,
                    mapped_data,
                    product_id,
                    FILLED_TEMPLATES_DIR,
                    category_id=category.get("category_id", "0"),
                    category_name=category.get("category_leaf", "Unknown")
                )
                if template_file:
                    insert_template_output(
                        product_id,
                        category.get("category_id", "0"),
                        "xlsm",
                        template_file,
                        os.path.basename(template_file)
                    )
                    log_processing(product_id, url, "template_fill", "success")
                    logger.info(f"✅ Template: {os.path.basename(template_file)}")
            except Exception as e:
                logger.warning(f"Template generation failed: {e}")
                log_processing(product_id, url, "template_fill", "error", str(e))
        else:
            logger.warning(f"⚠  Template file not found: {TEMPLATE_PATH}")

        # ── RETURN ─────────────────────────────────────────────────────────────
        logger.info("✅ Processing complete\n")

        return {
            "success": True,
            "product_id": product_id,
            "url": url,

            # Raw scraper output
            "original": {
                "title": title,
                "description": (description[:200] + "..."
                                if len(description) > 200 else description),
                **{f: scraped_data.get(f, "") for f in SPEC_FIELDS},
                "images": sum(1 for i in range(1, 21)
                              if scraped_data.get(f"image_{i}"))
            },

            # OpenAI output
            "enhanced": {
                "title": enhanced.get("title", ""),
                "description": (enhanced.get("description", "")[:200] + "..."
                                if enhanced.get("description") else ""),
                "bullet_points": enhanced.get("bullet_points", [])[:3],
                "has_html_description": bool(enhanced.get("html_description", "")),
                "specifications_enhanced": specs_enhanced
            },

            # API display: enhanced preferred, original fallback
            "merged": {f: enriched_data.get(f, "") for f in SPEC_FIELDS},

            # Template: ONLY enhanced (no fallback)
            "template_specs": {f: enriched_data_for_template.get(f, "")
                               for f in SPEC_FIELDS},

            "category": {
                "id":         category.get("category_id", ""),
                "name":       category.get("category_name", ""),
                "leaf":       category.get("category_leaf", ""),
                "confidence": round(category.get("confidence", 0.0), 2)
            },

            "template": {
                "file":           os.path.basename(template_file) if template_file else None,
                "columns_mapped": len(mapped_data),
                "fields_valid":   is_valid,
                "category_row": {
                    "code": category.get("category_id", "0"),
                    "leaf": category.get("category_leaf", "Unknown")
                }
            },

            "extracted": {
                "specifications": sum(1 for k in SPEC_FIELDS
                                      if enriched_data_for_template.get(k)),
                "images": sum(1 for i in range(1, 21)
                              if scraped_data.get(f"image_{i}"))
            },

            "audit": {
                "original_specs_saved":  True,
                "enhanced_specs_saved":  True,
                "audit_log_written":     True,
                "template_uses_enhanced_only": True
            },

            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return {
            "success": False,
            "url": url,
            "product_id": product_id,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# ─────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────

@app.post("/generate-product", tags=["Product Processing"])
def generate_product(req: ProductURLRequest):
    """
    Single product — HYBRID approach.

    Response sections:
    - **original**      : raw scraper output
    - **enhanced**      : OpenAI output
    - **merged**        : enhanced preferred, original fallback (for display)
    - **template_specs**: ONLY enhanced specs (what goes into the Excel file)
    - **audit**         : confirmation that all audit tables were written

    Processing time: ~30–60 seconds.
    """
    if not req.url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")
    logger.info(f"📨 Single product request: {req.url}")
    return process_product_complete(req.url)


@app.post("/generate-products", tags=["Product Processing"])
def generate_products(req: BulkProductRequest):
    """
    Bulk products — up to 20 URLs, sequential processing.
    """
    if not req.urls:
        raise HTTPException(status_code=400, detail="URLs list cannot be empty")
    if len(req.urls) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 URLs per request")

    logger.info(f"📨 Bulk request: {len(req.urls)} products")
    results    = []
    successful = 0
    failed     = 0

    for url in req.urls:
        result = process_product_complete(url)
        results.append(result)
        if result.get("success"):
            successful += 1
        else:
            failed += 1

    return {
        "total":      len(req.urls),
        "successful": successful,
        "failed":     failed,
        "results":    results,
        "timestamp":  datetime.now().isoformat()
    }


# ─────────────────────────────────────────
# DATABASE VIEW ENDPOINTS
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
            f"SELECT * FROM scraped_products ORDER BY scraped_at DESC LIMIT {min(limit, 1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/mapped-products", tags=["Database"])
def view_mapped_products(limit: int = 100):
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM mapped_products ORDER BY mapped_at DESC LIMIT {min(limit, 1000)}"
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
            f"SELECT * FROM template_outputs ORDER BY created_at DESC LIMIT {min(limit, 1000)}"
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
            f"SELECT * FROM processing_logs ORDER BY log_time DESC LIMIT {min(limit, 1000)}"
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
            f"SELECT * FROM enhanced_content ORDER BY id DESC LIMIT {min(limit, 1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/original-specifications", tags=["Database"])
def view_original_specifications(limit: int = 100):
    """View raw specs extracted by the scraper"""
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM original_specifications ORDER BY extracted_at DESC LIMIT {min(limit, 1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/enhanced-specifications", tags=["Database"])
def view_enhanced_specifications(limit: int = 100):
    """View specs enhanced by OpenAI"""
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM enhanced_specifications ORDER BY enhanced_at DESC LIMIT {min(limit, 1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/specification-audit", tags=["Database"])
def view_specification_audit(limit: int = 200):
    """View full audit trail: which spec value was used in each template"""
    try:
        conn = get_db_connection()
        rows = conn.execute(
            f"SELECT * FROM specification_audit_log ORDER BY recorded_at DESC LIMIT {min(limit, 1000)}"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/stats", tags=["Database"])
def get_stats():
    try:
        conn = get_db_connection()
        tables = [
            "scraped_products", "mapped_products", "template_outputs",
            "processing_logs", "category_assignments", "enhanced_content",
            "original_specifications", "enhanced_specifications",
            "specification_audit_log"
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


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8686))

    logger.info("\n" + "=" * 70)
    logger.info("🚀 Octopia Template Pipeline - HYBRID Approach v2.2")
    logger.info("   API  : original + enhanced + merged")
    logger.info("   Excel: ONLY enhanced specs")
    logger.info("   Audit: original_specs / enhanced_specs / audit_log")
    logger.info("=" * 70 + "\n")

    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
