"""
FastAPI Server - HYBRID APPROACH v2.8
Pipeline: Scrape → Store → Enhance → Categorize → Map → Excel

Key changes vs v2.7:
  - /scrape-products now defaults to 5 pages (was 2)
  - Keyword filter: uses filter_restricted_keywords(title) — string match only
    Output: "Title has restricted keyword"
  - Category filter: applied on category.leaf via embedding
    Output: "Category blocked due to restriction"
  - No description field used in filtering
  - Both filter messages are printed by product_filter.py (not by main.py)
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import sqlite3
import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

from scraper import scrape_search_results, MAX_SEARCH_PAGES
from product_filter import (
    filter_product,
    filter_products,
    filter_restricted_keywords,
    reload_filter_data,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Octopia Template Pipeline",
    version="2.8.0"
)

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
# PYDANTIC MODELS
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
    """
    Request model for the /scrape-products endpoint.

    Scrapes AliExpress search pages and returns:
      - product_id
      - product_url
      - title

    Pagination defaults to 5 pages. Keyword + category filters are NOT
    applied at this stage (they require a full product scrape).
    """
    search_url: str
    max_pages: Optional[int] = None          # defaults to 5 inside the endpoint
    delay_between_requests: float = 1.0

    class Config:
        json_schema_extra = {
            "example": {
                "search_url": (
                    "https://www.aliexpress.com/w/wholesale-bags.html"
                    "?SearchText=bags&page=1&catId=0&g=y&shipFromCountry=AE"
                ),
                "max_pages": 5,
                "delay_between_requests": 1.0
            }
        }


class SearchProductInfo(BaseModel):
    """Product info extracted from search results."""
    product_id:  str
    product_url: str
    title:       str


# =============================================================================
# DATABASE HELPERS
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
        logger.info("   POST /scrape-products   - Search scraping (5 pages default)")
        logger.info("   POST /generate-product  - Single product scraping + filtering")
        logger.info("   POST /generate-products - Bulk product scraping + filtering")
        logger.info("   POST /reload-filters    - Hot-reload filter data from DB")
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
        "version": "2.8.0",
        "endpoints": {
            "GET /":                  "This info",
            "GET /health":            "Health check",
            "POST /scrape-products":  "Scrape search results — returns product_id, product_url, title (5 pages default)",
            "POST /generate-product": "Scrape + keyword/category filter (single product)",
            "POST /generate-products":"Scrape + filter (bulk, max 20)",
            "POST /reload-filters":   "Hot-reload keyword + category filter data",
        },
        "filter_behaviour": {
            "keyword_filter": {
                "applies_to":    "title only",
                "method":        "string match (case-insensitive, word-boundary)",
                "blocked_msg":   "Title has restricted keyword",
            },
            "category_filter": {
                "applies_to":    "category.leaf only",
                "method":        "embedding cosine similarity",
                "thresholds":    {"BLOCK": "> 0.85", "REVIEW": "0.60–0.85", "ALLOW": "< 0.60"},
                "blocked_msg":   "Category blocked due to restriction",
            }
        }
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
# MAIN PROCESSING FUNCTION
# =============================================================================

def process_product_complete(url: str, extract_compliance: bool = True) -> Dict[str, Any]:
    product_id = None

    try:
        logger.info(f"\n🚀 Processing: {url}")

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
        logger.info(f"✅ Extracted {len(scraped_data)} attributes ({scraped_specs_count} spec fields)")

        # ── STEP 2: STORE ────────────────────────────────────────────────
        logger.info("💾 Storing scraped data...")
        product_id = insert_scraped_product(url, scraped_data)

        if not product_id:
            return {"success": False, "url": url, "error": "Failed to store scraped data",
                    "timestamp": datetime.now().isoformat()}

        log_processing(product_id, url, "scraping", "success")

        seller_data = {k: scraped_data.get(k, '') for k in SELLER_FIELDS}
        insert_seller_info(product_id, seller_data)
        log_processing(product_id, url, "seller_info", "success")

        compliance_data = scraped_data.get('compliance', {})
        if compliance_data:
            logger.info(f"🔒 Storing compliance info: {list(compliance_data.keys())}")
            insert_compliance_info(product_id, compliance_data)
            log_processing(product_id, url, "compliance_info", "success")

        insert_original_specifications(product_id, scraped_data)

        # ── STEP 3: ENHANCE ──────────────────────────────────────────────
        logger.info("🤖 Enhancing content with OpenAI...")
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
                "title":                   title,
                "description":             description,
                "bullet_points":           scraped_data.get("bullet_points", []),
                "html_description":        "",
                "specifications_enhanced": {}
            }

        specs_enhanced = enhanced.get("specifications_enhanced", {})
        insert_enhanced_specifications(product_id, specs_enhanced)

        enriched_data = scraped_data.copy()
        enriched_data['title']            = enhanced.get('title', title)
        enriched_data['description']      = enhanced.get('description', description)
        enriched_data['bullet_points']    = enhanced.get('bullet_points', [])
        enriched_data['html_description'] = enhanced.get('html_description', '')

        for field in SPEC_FIELDS:
            enh_val = specs_enhanced.get(field, '')
            enriched_data[field] = enh_val if (enh_val and enh_val.strip()) else ""

        for field in SELLER_FIELDS:
            enriched_data[field] = scraped_data.get(field, '')

        insert_enhanced_content(product_id, enriched_data)
        log_all_spec_audits(product_id, scraped_data, specs_enhanced, enriched_data)

        # ── STEP 4: CATEGORIZE ───────────────────────────────────────────
        logger.info("🏷️  Categorizing...")
        scraper_category = resolve_category(scraped_data)

        try:
            category = assign_category(
                enhanced.get("title", title),
                enhanced.get("description", description)
            )
            if scraper_category['confidence'] >= 0.9 and scraper_category['category_id'] != '0':
                category = scraper_category
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
            mapped_data = map_scraped_data_to_template(enriched_data)
            is_valid, _ = validate_mapped_data(mapped_data)
            insert_mapped_product(product_id, category.get("category_id", "0"), mapped_data)
            log_processing(product_id, url, "mapping", "success" if is_valid else "warning")
        except Exception as e:
            logger.warning(f"Mapping error: {e}")
            log_processing(product_id, url, "mapping", "error", str(e))

        # ── STEP 6: EXCEL ────────────────────────────────────────────────
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
            except Exception as e:
                logger.error(f"❌ Template error: {e}", exc_info=True)
                log_processing(product_id, url, "template_fill", "error", str(e))

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
            "seller":     {f: scraped_data.get(f, "") for f in SELLER_FIELDS},
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
                "specifications":  sum(1 for k in SPEC_FIELDS if enriched_data.get(k)),
                "images":          sum(1 for i in range(1, 21) if scraped_data.get(f"image_{i}")),
                "seller_fields":   len([k for k in SELLER_FIELDS if scraped_data.get(k)]),
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

    Filters applied (both active by default):
      1. Keyword filter  — filter_restricted_keywords(title)
         Blocked message: "Title has restricted keyword"
      2. Category filter — embedding similarity on category.leaf
         Blocked message: "Category blocked due to restriction"

    If filtered → returns success=False + filtered=True + rejection_reason.
    """
    if not req.url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")

    result = process_product_complete(req.url, extract_compliance=req.extract_compliance)

    if not result.get("success"):
        return result

    title        = result.get("original", {}).get("title", "")
    raw_category = result.get("category")

    # ── Keyword filter ───────────────────────────────────────────────────────
    kw_blocked = filter_restricted_keywords(title)
    if kw_blocked:
        return {
            "success":          False,
            "filtered":         True,
            "rejection_reason": "Title has restricted keyword",
            "product_id":       result.get("product_id"),
            "url":              req.url,
            "timestamp":        datetime.now().isoformat(),
        }

    # ── Category filter (on leaf) ─────────────────────────────────────────────
    allowed, reason = filter_product(title, raw_category,
                                     apply_keyword_filter=False,
                                     apply_category_filter=True)
    if not allowed:
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

    Both filters applied to every product.
    Filtered products are excluded from 'results' and counted under 'filtered'.
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

        # Keyword filter
        kw_blocked = filter_restricted_keywords(title)
        if kw_blocked:
            filtered_count += 1
            logger.info(f"[filter] Keyword-blocked: {url}")
            continue

        # Category filter
        allowed, reason = filter_product(title, raw_category,
                                         apply_keyword_filter=False,
                                         apply_category_filter=True)
        if not allowed:
            filtered_count += 1
            logger.info(f"[filter] Category-blocked ({reason}): {url}")
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
    """Hot-reload keyword + category filter data from DB."""
    reload_filter_data()
    return {"status": "ok", "message": "Filter data reloaded from DB"}


# =============================================================================
# SEARCH SCRAPING ENDPOINT
# =============================================================================

@app.post("/scrape-products", response_model=List[SearchProductInfo], tags=["Product Processing"])
def scrape_search_products(request: SearchScrapeRequest):
    """
    Scrape product IDs, URLs, and titles from AliExpress search results.

    Pagination:
      • Default: 5 pages
      • Each page yields ~60 products
      • Duplicates removed automatically

    Fields returned per product:
      - product_id   (AliExpress item ID)
      - product_url  (canonical https://www.aliexpress.com/item/<id>.html)
      - title        (display title, AliExpress category suffixes stripped)

    Note: keyword/category filters are NOT applied here — this endpoint
    only returns raw search results. Use /generate-product for filtered
    full product processing.

    Reference URL format:
      https://www.aliexpress.com/w/wholesale-bags.html
        ?SearchText=bags&page=1&catId=0&g=y&shipFromCountry=AE
    """
    if not request.search_url:
        raise HTTPException(status_code=400, detail="search_url is required")

    if 'aliexpress.com' not in request.search_url.lower():
        raise HTTPException(status_code=400, detail="Invalid AliExpress URL")

    # Default 5 pages; never exceed hard cap
    max_pages = request.max_pages if request.max_pages is not None else 5
    max_pages = min(max_pages, MAX_SEARCH_PAGES)

    try:
        logger.info(f"🔍 Starting search scrape: {request.search_url}")
        logger.info(f"   Max pages: {max_pages}")

        raw = scrape_search_results(
            search_url=request.search_url,
            max_pages=max_pages,
            delay=request.delay_between_requests,
        )

        # scrape_search_results always returns a dict wrapper
        if isinstance(raw, dict):
            products = raw.get("products", [])
        else:
            products = raw or []

        if not products:
            logger.info("No products found")
            return []

        logger.info(
            f"✅ Search scrape complete: {len(products)} products from "
            f"{raw.get('pages_scraped', '?')} pages"
        )

        return [
            SearchProductInfo(
                product_id=str(p["product_id"]),
                product_url=p["product_url"],
                title=p.get("title", ""),
            )
            for p in products
            if p.get("product_id")
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
            f"SELECT * FROM scraped_products ORDER BY scraped_at DESC LIMIT {min(limit, 1000)}"
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
            f"SELECT * FROM seller_info ORDER BY scraped_at DESC LIMIT {min(limit, 1000)}"
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
            f"SELECT * FROM compliance_info ORDER BY extracted_at DESC LIMIT {min(limit, 1000)}"
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


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8686))
    logger.info("🚀 Octopia Template Pipeline v2.8")
    logger.info(f"📡 Server: http://0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
