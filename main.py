"""
FastAPI Server - HYBRID APPROACH v2.7
Pipeline: Scrape → Store → Enhance → Categorize → Map → Excel
Compliance info extracted and stored separately.
Added: Search results scraping endpoint
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware  # Add this
from pydantic import BaseModel
import os
import sqlite3
import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

# scrape_search_results lives inside scraper.py (merged v6.2)
from scraper import scrape_search_results, MAX_SEARCH_PAGES
from product_filter import filter_product, filter_products, reload_filter_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Octopia Template Pipeline",
    version="2.7.0"
)

# Add CORS middleware to allow testing from browsers
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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


# =============================================================================
# PYDANTIC MODELS (Existing + New)
# =============================================================================

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


class SearchScrapeRequest(BaseModel):
    """Request model for search scraping endpoint"""
    search_url: str
    max_pages: Optional[int] = None
    delay_between_requests: float = 1.0

    class Config:
        json_schema_extra = {
            "example": {
                "search_url": "https://www.aliexpress.com/w/wholesale-enter-keywords.html?SearchText=enter-keywords&catId=0&g=y&shipFromCountry=enter_ship_from_country&trafficChannel=main",
                "max_pages": 2,
                "delay_between_requests": 1.0
            }
        }


class SearchProductInfo(BaseModel):
    """Product info from search results"""
    product_id: str
    product_url: str
    title: str


# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# STARTUP EVENT
# =============================================================================

@app.on_event("startup")
def startup_event():
    try:
        from db import create_all_tables
        create_all_tables()
        logger.info("✅ API Ready")
        logger.info("📋 Available endpoints:")
        logger.info("   POST /scrape-products   - Search results scraping (max 2 pages)")
        logger.info("   POST /generate-product  - Single product scraping + filtering")
        logger.info("   POST /generate-products - Bulk product scraping + filtering")
        logger.info("   POST /reload-filters    - Hot-reload filter CSVs")
    except Exception as e:
        logger.error(f"Startup error: {e}")


# =============================================================================
# INFO ENDPOINTS
# =============================================================================

@app.get("/", tags=["Info"])
def root():
    return {
        "status": "running",
        "service": "Octopia Template Pipeline",
        "version": "2.7.0",
        "endpoints": {
            "GET /": "This info",
            "GET /health": "Health check",
            "POST /scrape-products": "Scrape search results with pagination (max 2 pages)",
            "POST /generate-product": "Scrape single product + keyword/category filtering",
            "POST /generate-products": "Scrape multiple products + keyword/category filtering",
            "POST /reload-filters": "Hot-reload restricted_keywords.csv / restricted_categories.csv",
            "GET /scraped-products": "View scraped products",
            "GET /seller-info": "View seller information",
            "GET /compliance-info": "View compliance information",
            "GET /stats": "Database statistics"
        },
        "features": [
            "Camoufox Firefox browser (API interception)",
            "Seller info stored in seller_info table",
            "EU compliance info extracted and stored",
            "Content enhancement via OpenAI",
            "Octopia categorization",
            "Template mapping + Excel generation",
            "Search results scraping with pagination (2 pages default)",
            "Restricted keyword filtering on product titles",
            "Restricted category filtering — blocked products excluded from output",
            "Hot-reload filter CSVs without server restart",
        ]
    }


@app.get("/health", tags=["Info"])
def health_check():
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}


# =============================================================================
# MAIN PROCESSING FUNCTION (Existing - keep as is)
# =============================================================================

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

        scraper_category = resolve_category(scraped_data)
        logger.info(f"   Scraper category: {scraper_category}")

        try:
            category = assign_category(
                enhanced.get("title", title),
                enhanced.get("description", description)
            )
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
                "title": title,
                "description": (description[:200] + "..." if len(description) > 200 else description),
                **{f: scraped_data.get(f, "") for f in SPEC_FIELDS},
                "images": sum(1 for i in range(1, 21) if scraped_data.get(f"image_{i}"))
            },
            "seller": {f: scraped_data.get(f, "") for f in SELLER_FIELDS},
            "compliance": compliance_data,
            "category": {
                "id": category.get("category_id", ""),
                "name": category.get("category_name", ""),
                "leaf": category.get("category_leaf", ""),
                "path": category.get("category_path", ""),
                "confidence": round(category.get("confidence", 0.0), 2)
            },
            "enhanced": {
                "title": enhanced.get("title", ""),
                "description": (enhanced.get("description", "")[:200] + "..." if enhanced.get("description") else ""),
                "bullet_points": enhanced.get("bullet_points", [])[:3],
                "has_html_description": bool(enhanced.get("html_description", "")),
                "specifications_enhanced": specs_enhanced
            },
            "template": {
                "file": os.path.basename(template_file) if template_file else None,
                "columns_mapped": len(mapped_data),
                "fields_valid": is_valid,
            },
            "extracted": {
                "specifications": sum(1 for k in SPEC_FIELDS if enriched_data_for_template.get(k)),
                "images": sum(1 for i in range(1, 21) if scraped_data.get(f"image_{i}")),
                "seller_fields": len([k for k in SELLER_FIELDS if scraped_data.get(k)]),
                "compliance_fields": len(compliance_data),
            },
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return {"success": False, "url": url, "product_id": product_id,
                "error": str(e), "timestamp": datetime.now().isoformat()}


# =============================================================================
# PRODUCT PROCESSING ENDPOINTS
# =============================================================================

@app.post("/generate-product", tags=["Product Processing"])
def generate_product(req: ProductURLRequest):
    """
    Scrape and process a single product URL.

    Filtering applied (both active by default):
      - Keyword filter : title must not contain any restricted keyword
      - Category filter: category must not appear in restricted_categories.csv

    If the product is filtered out, returns success=False with a
    'filtered' flag and the rejection reason. The category field is
    omitted from the response when the category filter triggered.
    """
    if not req.url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")

    result = process_product_complete(req.url, extract_compliance=req.extract_compliance)

    if not result.get("success"):
        return result

    # Build the category dict in the shape filter_product expects
    raw_category = result.get("category")  # already a dict with id/name/leaf/path/confidence

    title = result.get("original", {}).get("title", "")
    allowed, reason = filter_product(title, raw_category)

    if not allowed:
        logger.info(f"[filter] Product {result.get('product_id')} rejected: {reason}")
        return {
            "success":          False,
            "filtered":         True,
            "rejection_reason": reason,
            "product_id":       result.get("product_id"),
            "url":              req.url,
            "timestamp":        datetime.now().isoformat(),
        }

    return result


@app.post("/generate-products", tags=["Product Processing"])
def generate_products(req: BulkProductRequest):
    """
    Scrape and process multiple product URLs (max 20).

    Filtering applied to every product:
      - Keyword filter : title must not contain restricted keywords
      - Category filter: category must not be in restricted_categories.csv

    Filtered products are NOT included in 'results' and are counted
    separately under the 'filtered' key so the caller can see how
    many were dropped without exposing the restricted category data.
    """
    if not req.urls:
        raise HTTPException(status_code=400, detail="URLs list cannot be empty")
    if len(req.urls) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 URLs per request")

    results        = []
    successful     = 0
    failed         = 0
    filtered_count = 0

    for url in req.urls:
        result = process_product_complete(url, extract_compliance=req.extract_compliance)

        if not result.get("success"):
            failed += 1
            results.append(result)
            continue

        title        = result.get("original", {}).get("title", "")
        raw_category = result.get("category")
        allowed, reason = filter_product(title, raw_category)

        if not allowed:
            filtered_count += 1
            logger.info(f"[filter] Filtered out ({reason}): {url}")
            # Excluded from output — do not append to results
            continue

        successful += 1
        results.append(result)

    return {
        "total":      len(req.urls),
        "successful": successful,
        "failed":     failed,
        "filtered":   filtered_count,
        "results":    results,
        "timestamp":  datetime.now().isoformat(),
    }


@app.post("/reload-filters", tags=["Product Processing"])
def reload_filters():
    """
    Hot-reload restricted_keywords.csv and restricted_categories.csv
    without restarting the server. Call this after updating either CSV.
    """
    reload_filter_data()
    return {"status": "ok", "message": "Filter data reloaded from CSV files"}


# =============================================================================
# SEARCH SCRAPING ENDPOINT (MAKE SURE THIS IS HERE)
# =============================================================================

@app.post("/scrape-products", response_model=List[SearchProductInfo], tags=["Product Processing"])
def scrape_search_products(request: SearchScrapeRequest):
    """
    Scrape product URLs, IDs, and titles from AliExpress search results.

    Pagination: scrapes up to max_pages (default: 2, hard cap: 2 unless overridden).
    Each page yields ~60 products; duplicates are removed automatically.

    Returns a flat list of {product_id, product_url, title}.
    """
    if not request.search_url:
        raise HTTPException(status_code=400, detail="search_url is required")

    if 'aliexpress.com' not in request.search_url.lower():
        raise HTTPException(status_code=400, detail="Invalid AliExpress URL")

    # Enforce 2-page default; never exceed MAX_SEARCH_PAGES unless caller is explicit
    max_pages = request.max_pages if request.max_pages is not None else 2

    try:
        logger.info(f"🔍 Starting search scrape: {request.search_url}")
        logger.info(f"   Max pages: {max_pages}")

        raw = scrape_search_results(
            search_url=request.search_url,
            max_pages=max_pages,
            delay=request.delay_between_requests,
        )

        # scrape_search_results returns List[Dict] directly (search_scraper.py)
        # Guard against a legacy Dict wrapper just in case
        if isinstance(raw, dict):
            products = raw.get("products", [])
        else:
            products = raw or []

        if not products:
            logger.info("No products found")
            return []

        logger.info(f"✅ Search scrape complete: {len(products)} products found")

        return [
            SearchProductInfo(
                product_id=str(p["product_id"]),
                product_url=p["product_url"],
                title=p.get("title", ""),
            )
            for p in products
            if p.get("product_id")   # skip any malformed entries
        ]

    except Exception as e:
        logger.error(f"❌ Search scrape failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Scraping failed: {str(e)}")


# =============================================================================
# DATABASE VIEW ENDPOINTS
# =============================================================================

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
        row = conn.execute(
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
        conn = get_db_connection()
        tables = [
            "scraped_products", "mapped_products", "template_outputs",
            "processing_logs", "category_assignments", "enhanced_content",
            "original_specifications", "enhanced_specifications",
            "specification_audit_log", "seller_info", "compliance_info"
        ]
        stats = {}
        for table in tables:
            try:
                stats[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except Exception:
                stats[table] = 0
        conn.close()
        return stats
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8686))
    logger.info("🚀 Octopia Template Pipeline v2.7 — Camoufox + Compliance + Search Scraper")
    logger.info(f"📡 Server running on http://0.0.0.0:{port}")
    logger.info("📋 Available endpoints:")
    logger.info(f"   POST http://localhost:{port}/scrape-products")
    logger.info(f"   POST http://localhost:{port}/generate-product")
    logger.info(f"   POST http://localhost:{port}/generate-products")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
