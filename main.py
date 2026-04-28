"""
FastAPI Server - HYBRID APPROACH v2.9
Pipeline: Scrape → Store → Enhance → Categorize → Map → Excel

Key changes vs v2.8:
  ─────────────────────────────────────────────────────────────────────────
  POST /scrape-products
    • Returns ALL products (restricted + accepted) — no silent skips
    • Applies keyword filter on ORIGINAL title
    • Restricted shape:  { product_id, product_url, title (ORIGINAL),
                           status:"rejected",
                           message:"Title not fetched due to restricted keyword" }
    • Accepted shape:    { product_id, product_url, title (ORIGINAL),
                           status:"accepted" }
    • title is ALWAYS the original scraped title — never enhanced

  POST /generate-product / POST /generate-products
    • Category confidence < 0.75 → set category = "Uncategorized"
    • Accepted response includes both original_title and enhanced_title
    • Rejected response: { status:"rejected",
                           reason:"Title has restricted keyword" }

  Merchant Bulk (v2):
    • Batch-safe: results written to disk per batch — no data loss on crash
    • Browser reuse: one browser per worker chunk (20 merchants per session)
    • BATCH_SIZE=50, CONCURRENCY=8 for faster 2000+ merchant processing
    • /merchant-job-status: reads from disk, survives server restart
    • /merchant-download: streams from disk file, never held in memory
    • /merchant-jobs: lists all jobs from disk
  ─────────────────────────────────────────────────────────────────────────
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import sqlite3
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from scraper import scrape_search_results, MAX_SEARCH_PAGES
from product_filter import (
    filter_product,
    filter_products,
    filter_restricted_keywords,
    validate_category_confidence,
    reload_filter_data,
)

# ── Merchant bulk processing ──────────────────────────────────────────────────
import uuid
from fastapi import UploadFile, File
from fastapi.responses import Response as FastAPIResponse
from merchant_scraper import (
    parse_merchant_csv,
    start_bulk_job,
    get_job_status,
    get_output_path,
    list_all_jobs,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Octopia Template Pipeline",
    version="2.9.0"
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
    Request body for POST /scrape-products.
    Scrapes up to max_pages (default 5) of AliExpress search results.
    ALL products are returned — both accepted and keyword-restricted ones.
    """
    search_url: str
    max_pages: Optional[int] = None
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


class MerchantIDsRequest(BaseModel):
    """JSON-body alternative to CSV upload — send IDs directly."""
    merchant_ids: List[str]

    class Config:
        json_schema_extra = {
            "example": {"merchant_ids": ["1103833861", "912519001", "567839201"]}
        }


# =============================================================================
# DB HELPERS
# =============================================================================

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# STARTUP
# =============================================================================

@app.on_event("startup")
def startup_event():
    try:
        from db import create_all_tables
        create_all_tables()
        logger.info("✅ API Ready — v2.9.0")
    except Exception as e:
        logger.error(f"Startup error: {e}")


# =============================================================================
# INFO
# =============================================================================

@app.get("/", tags=["Info"])
def root():
    return {
        "status":  "running",
        "service": "Octopia Template Pipeline",
        "version": "2.9.0",
        "endpoints": {
            "POST /scrape-products":      "Search scraping — 5 pages, ALL products",
            "POST /generate-product":     "Single product full pipeline + filtering",
            "POST /generate-products":    "Bulk product pipeline + filtering (max 20)",
            "GET  /product-info/{id}":    "Product by AliExpress ID (EUR currency)",
            "POST /upload-csv":           "Upload MerchantID CSV → start bulk job",
            "POST /submit-merchant-ids":  "Submit merchant IDs as JSON → start bulk job",
            "GET  /merchant-job-status/{job_id}": "Poll job progress",
            "GET  /merchant-download/{job_id}":   "Download result CSV",
            "GET  /merchant-jobs":        "List all merchant jobs",
            "POST /reload-filters":       "Hot-reload filter data from DB",
        },
        "merchant_bulk": {
            "batch_size":           50,
            "concurrency":          8,
            "merchants_per_browser": 20,
            "storage":              "disk (./merchant_jobs/) — crash-safe",
        },
        "scraping_rules": {
            "max_pages":     5,
            "pagination":    "Direct URL navigation — never stops early",
            "deduplication": True,
        },
        "filter_rules": {
            "keyword_filter": {
                "field":  "ORIGINAL title only",
                "method": "partial case-insensitive match",
            },
            "category_confidence": {
                "accept_threshold": "≥ 0.75",
                "reject_below":     "< 0.75 → set Uncategorized",
            },
            "category_embedding": {
                "block":  "> 0.85",
                "review": "0.60–0.85",
                "allow":  "< 0.60",
            },
        },
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
# MAIN PROCESSING FUNCTION  (single product full pipeline)
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
                break

        # ── STEP 1: SCRAPE ───────────────────────────────────────────────
        scraped_data = get_product_info(url, extract_compliance=extract_compliance)

        if not scraped_data:
            return {"success": False, "url": url, "error": "Scraping failed",
                    "timestamp": datetime.now().isoformat()}

        original_title = scraped_data.get("title", "")
        description    = scraped_data.get("description", "")

        if not original_title:
            return {"success": False, "url": url, "error": "No title extracted",
                    "timestamp": datetime.now().isoformat()}

        # ── STEP 2: STORE ────────────────────────────────────────────────
        product_id = insert_scraped_product(url, scraped_data)
        if not product_id:
            return {"success": False, "url": url, "error": "Failed to store scraped data",
                    "timestamp": datetime.now().isoformat()}

        log_processing(product_id, url, "scraping", "success")
        insert_seller_info(product_id, {k: scraped_data.get(k, '') for k in SELLER_FIELDS})
        log_processing(product_id, url, "seller_info", "success")

        compliance_data = scraped_data.get('compliance', {})
        if compliance_data:
            insert_compliance_info(product_id, compliance_data)
            log_processing(product_id, url, "compliance_info", "success")

        insert_original_specifications(product_id, scraped_data)

        # ── STEP 3: ENHANCE ──────────────────────────────────────────────
        product_data_for_llm = {
            k: v for k, v in scraped_data.items()
            if k not in SELLER_FIELDS and k != 'compliance'
        }

        try:
            enhanced = improve_product_content(
                title=original_title,
                description=description,
                specifications=product_data_for_llm,
                category=None
            )
            if not enhanced:
                raise ValueError("OpenAI returned None")
        except Exception as e:
            logger.warning(f"Enhancement skipped: {e}")
            enhanced = {
                "title":                   original_title,
                "description":             description,
                "bullet_points":           scraped_data.get("bullet_points", []),
                "html_description":        "",
                "specifications_enhanced": {}
            }

        enhanced_title = enhanced.get("title", original_title)
        specs_enhanced = enhanced.get("specifications_enhanced", {})
        insert_enhanced_specifications(product_id, specs_enhanced)

        enriched_data                     = scraped_data.copy()
        enriched_data['title']            = enhanced_title
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
        scraper_category = resolve_category(scraped_data)

        try:
            category = assign_category(enhanced_title, enhanced.get("description", description))
            if scraper_category['confidence'] >= 0.9 and scraper_category['category_id'] != '0':
                category = scraper_category
        except Exception as e:
            logger.warning(f"Categorization fallback: {e}")
            category = scraper_category if scraper_category['category_id'] != '0' else {
                "category_id": "0", "category_name": "Unknown",
                "category_leaf": "Unknown", "confidence": 0.0
            }

        # Apply confidence threshold: < 0.75 → Uncategorized
        confidence = float(category.get("confidence", 0.0))
        conf_accepted, conf_reason = validate_category_confidence(confidence)
        if not conf_accepted:
            logger.info(
                f"[categorize] Confidence {confidence:.2f} < 0.75 → "
                f"Uncategorized. {conf_reason}"
            )
            category = {
                "category_id":   "0",
                "category_name": "Uncategorized",
                "category_leaf": "Uncategorized",
                "category_path": "",
                "confidence":    confidence,
            }

        insert_category_assignment(
            product_id,
            category.get("category_id", "0"), category.get("category_name", "Uncategorized"),
            category.get("category_id", "0"), category.get("category_name", "Uncategorized"),
            confidence
        )
        log_processing(product_id, url, "categorization", "success")

        # ── STEP 5: MAP ──────────────────────────────────────────────────
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
        template_file = None
        if TEMPLATE_PATH:
            try:
                template_file = fill_template_for_product(
                    TEMPLATE_PATH, mapped_data, product_id, FILLED_TEMPLATES_DIR,
                    category_id=category.get("category_id", "0"),
                    category_name=category.get("category_leaf", "Uncategorized")
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
            "success":        True,
            "product_id":     product_id,
            "url":            url,
            "original_title": original_title,
            "enhanced_title": enhanced_title,
            "original": {
                "title":       original_title,
                "description": (description[:200] + "..." if len(description) > 200
                                else description),
                **{f: scraped_data.get(f, "") for f in SPEC_FIELDS},
                "images": sum(1 for i in range(1, 21) if scraped_data.get(f"image_{i}"))
            },
            "seller":     {f: scraped_data.get(f, "") for f in SELLER_FIELDS},
            "compliance": compliance_data,
            # Detail page extracted fields
            "shipment_country": scraped_data.get("shipment_country"),
            "delivery_start":   scraped_data.get("delivery_start"),
            "delivery_end":     scraped_data.get("delivery_end"),
            "delivery_days":    scraped_data.get("delivery_days"),
            "remaining_stock":  scraped_data.get("remaining_stock"),
            "rating":           scraped_data.get("rating", ""),
            "category": {
                "id":         category.get("category_id", ""),
                "name":       category.get("category_name", ""),
                "leaf":       category.get("category_leaf", ""),
                "path":       category.get("category_path", ""),
                "confidence": round(confidence, 2)
            },
            "enhanced": {
                "title":                   enhanced_title,
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
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return {"success": False, "url": url, "product_id": product_id,
                "error": str(e), "timestamp": datetime.now().isoformat()}


# =============================================================================
# SEARCH SCRAPING ENDPOINT
# =============================================================================

@app.post("/scrape-products", tags=["Product Processing"])
def scrape_search_products(request: SearchScrapeRequest):
    """
    Scrape product IDs, URLs, and ORIGINAL titles from AliExpress search results.

    Output shape (ALL products — accepted and rejected):
      Restricted: { product_id, product_url, title, rating, sold_count,
                    status:"rejected", message:"..." }
      Accepted:   { product_id, product_url, title, rating, sold_count,
                    status:"accepted" }

    title is ALWAYS the original raw scraped title — never LLM-enhanced.
    rating < 4.0 → rejected with reason in message field.
    """
    if not request.search_url:
        raise HTTPException(status_code=400, detail="search_url is required")
    if 'aliexpress.com' not in request.search_url.lower():
        raise HTTPException(status_code=400, detail="Invalid AliExpress URL")

    max_pages = request.max_pages if request.max_pages is not None else 5
    max_pages = min(max_pages, MAX_SEARCH_PAGES)

    try:
        logger.info(f"🔍 Search scrape started: {request.search_url}")
        logger.info(f"   Max pages: {max_pages}")

        raw = scrape_search_results(
            search_url=request.search_url,
            max_pages=max_pages,
            delay=request.delay_between_requests,
        )

        products = raw.get("products", []) if isinstance(raw, dict) else (raw or [])

        logger.info(
            f"✅ Scraped {len(products)} products from "
            f"{raw.get('pages_scraped', '?') if isinstance(raw, dict) else '?'} pages"
        )

        result         = []
        accepted_count = 0
        rejected_count = 0

        for p in products:
            if not p.get("product_id"):
                continue

            original_title = p.get("title", "")
            is_restricted  = filter_restricted_keywords(original_title)

            product_rating     = p.get("rating", "")
            rating_float       = 0.0
            rating_filter_fail = False
            if product_rating:
                try:
                    rating_float = float(str(product_rating).strip())
                    if rating_float < 4.0:
                        rating_filter_fail = True
                except (ValueError, TypeError):
                    pass  # unparseable rating → don't reject yet

            if is_restricted:
                result.append({
                    "product_id":  str(p["product_id"]),
                    "product_url": p["product_url"],
                    "title":       original_title,
                    "rating":      product_rating,
                    "sold_count":  p.get("sold_count", ""),
                    "status":      "rejected",
                    "message":     "Title not fetched due to restricted keyword",
                })
                rejected_count += 1
            elif rating_filter_fail:
                result.append({
                    "product_id":  str(p["product_id"]),
                    "product_url": p["product_url"],
                    "title":       original_title,
                    "rating":      product_rating,
                    "sold_count":  p.get("sold_count", ""),
                    "status":      "rejected",
                    "message":     f"Rating {rating_float:.1f} is below minimum 4.0",
                })
                rejected_count += 1
            else:
                result.append({
                    "product_id":  str(p["product_id"]),
                    "product_url": p["product_url"],
                    "title":       original_title,
                    "rating":      product_rating,
                    "sold_count":  p.get("sold_count", ""),
                    "status":      "accepted",
                })
                accepted_count += 1

        logger.info(
            f"   accepted={accepted_count} | rejected={rejected_count} | total={len(result)}"
        )
        return result

    except Exception as e:
        logger.error(f"❌ Search scrape failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Scraping failed: {str(e)}")


# =============================================================================
# GENERATE-PRODUCT ENDPOINT
# =============================================================================

@app.post("/generate-product", tags=["Product Processing"])
def generate_product(req: ProductURLRequest):
    """
    Full pipeline for a single product URL.

    Filter order (on ORIGINAL title):
      1. Keyword filter (partial match)
      2. Category confidence < 0.75 → Uncategorized
      3. Category embedding check on leaf

    Accepted response includes both original_title and enhanced_title
    plus all 4 detail fields (rating, shipment_country, delivery, remaining_stock).
    """
    if not req.url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")

    result = process_product_complete(req.url, extract_compliance=req.extract_compliance)

    if not result.get("success"):
        return result

    original_title = result.get("original_title", "")
    raw_category   = result.get("category")

    if filter_restricted_keywords(original_title):
        return {
            "status":         "rejected",
            "reason":         "Title has restricted keyword",
            "product_id":     result.get("product_id"),
            "original_title": original_title,
            "url":            req.url,
            "timestamp":      datetime.now().isoformat(),
        }

    from product_filter import is_category_restricted
    cat_blocked, cat_reason = is_category_restricted(raw_category)
    if cat_blocked:
        return {
            "status":         "rejected",
            "reason":         cat_reason,
            "product_id":     result.get("product_id"),
            "original_title": original_title,
            "url":            req.url,
            "timestamp":      datetime.now().isoformat(),
        }

    return {
        "status":           "accepted",
        "product_id":       result.get("product_id"),
        "url":              req.url,
        "original_title":   original_title,
        "enhanced_title":   result.get("enhanced_title", ""),
        "category":         result.get("category", {}).get("name", ""),
        "confidence":       result.get("category", {}).get("confidence", 0.0),
        "rating":           result.get("rating", ""),
        "shipment_country": result.get("shipment_country"),
        "delivery_start":   result.get("delivery_start"),
        "delivery_end":     result.get("delivery_end"),
        "delivery_days":    result.get("delivery_days"),
        "remaining_stock":  result.get("remaining_stock"),
        "enhanced":         result.get("enhanced", {}),
        "seller":           result.get("seller", {}),
        "compliance":       result.get("compliance", {}),
        "template":         result.get("template", {}),
        "timestamp":        result.get("timestamp"),
    }


# =============================================================================
# GENERATE-PRODUCTS ENDPOINT
# =============================================================================

@app.post("/generate-products", tags=["Product Processing"])
def generate_products(req: BulkProductRequest):
    """Full pipeline for multiple product URLs (max 20)."""
    if not req.urls:
        raise HTTPException(status_code=400, detail="URLs list cannot be empty")
    if len(req.urls) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 URLs per request")

    from product_filter import is_category_restricted

    results    = []
    successful = 0
    rejected   = 0
    failed     = 0

    for url in req.urls:
        result = process_product_complete(url, extract_compliance=req.extract_compliance)

        if not result.get("success"):
            failed += 1
            results.append({
                "status":    "error",
                "url":       url,
                "reason":    result.get("error", "Unknown error"),
                "timestamp": datetime.now().isoformat(),
            })
            continue

        original_title = result.get("original_title", "")
        raw_category   = result.get("category")

        if filter_restricted_keywords(original_title):
            rejected += 1
            results.append({
                "status":         "rejected",
                "reason":         "Title has restricted keyword",
                "original_title": original_title,
                "url":            url,
                "timestamp":      datetime.now().isoformat(),
            })
            continue

        cat_blocked, cat_reason = is_category_restricted(raw_category)
        if cat_blocked:
            rejected += 1
            results.append({
                "status":         "rejected",
                "reason":         cat_reason,
                "original_title": original_title,
                "url":            url,
                "timestamp":      datetime.now().isoformat(),
            })
            continue

        successful += 1
        results.append({
            "status":         "accepted",
            "product_id":     result.get("product_id"),
            "url":            url,
            "original_title": original_title,
            "enhanced_title": result.get("enhanced_title", ""),
            "category":       result.get("category", {}).get("name", ""),
            "confidence":     result.get("category", {}).get("confidence", 0.0),
            "enhanced":       result.get("enhanced", {}),
            "seller":         result.get("seller", {}),
            "compliance":     result.get("compliance", {}),
            "template":       result.get("template", {}),
            "timestamp":      result.get("timestamp"),
        })

    return {
        "total":      len(req.urls),
        "successful": successful,
        "rejected":   rejected,
        "failed":     failed,
        "results":    results,
        "timestamp":  datetime.now().isoformat(),
    }


# =============================================================================
# PRODUCT INFO BY ID ENDPOINT
# =============================================================================

@app.get("/product-info/{product_id}", tags=["Product Processing"])
def get_product_info_by_id(product_id: str, extract_compliance: bool = False):
    """
    Full pipeline for a single product by AliExpress product ID.
    Uses EUR currency URL automatically.
    """
    if not product_id or not product_id.isdigit():
        raise HTTPException(status_code=400, detail="product_id must be numeric")

    eur_url = (
        f"https://www.aliexpress.com/item/{product_id}.html"
        f"?language=en&currency=EUR&gatewayAdapt=pol2glo"
    )

    result = process_product_complete(eur_url, extract_compliance=extract_compliance)

    if not result.get("success"):
        return result

    original_title = result.get("original_title", "")

    if filter_restricted_keywords(original_title):
        return {
            "status":         "rejected",
            "reason":         "Title has restricted keyword",
            "product_id":     result.get("product_id"),
            "original_title": original_title,
            "url":            eur_url,
            "timestamp":      datetime.now().isoformat(),
        }

    from product_filter import is_category_restricted
    cat_blocked, cat_reason = is_category_restricted(result.get("category"))
    if cat_blocked:
        return {
            "status":         "rejected",
            "reason":         cat_reason,
            "product_id":     result.get("product_id"),
            "original_title": original_title,
            "url":            eur_url,
            "timestamp":      datetime.now().isoformat(),
        }

    return {
        "status":           "accepted",
        "product_id":       result.get("product_id"),
        "aliexpress_id":    product_id,
        "url":              eur_url,
        "original_title":   original_title,
        "enhanced_title":   result.get("enhanced_title", ""),
        "category":         result.get("category", {}).get("name", ""),
        "confidence":       result.get("category", {}).get("confidence", 0.0),
        "rating":           result.get("rating", ""),
        "shipment_country": result.get("shipment_country"),
        "delivery_start":   result.get("delivery_start"),
        "delivery_end":     result.get("delivery_end"),
        "delivery_days":    result.get("delivery_days"),
        "remaining_stock":  result.get("remaining_stock"),
        "enhanced":         result.get("enhanced", {}),
        "seller":           result.get("seller", {}),
        "compliance":       result.get("compliance", {}),
        "template":         result.get("template", {}),
        "timestamp":        result.get("timestamp"),
    }


# =============================================================================
# MERCHANT BULK PROCESSING ENDPOINTS
# =============================================================================

@app.post("/upload-csv", tags=["Merchant Bulk"])
async def upload_merchant_csv(file: UploadFile = File(...)):
    """
    Upload a CSV file containing a MerchantID column.

    REQUIRES:  pip install python-multipart

    CSV format:
      MerchantID
      1103833861
      912519001

    Processing:
      • Splits into batches of 50
      • 8 concurrent workers, each processing 20 merchants per browser session
      • Results written to disk after each batch — no data loss on crash
      • Returns job_id immediately — poll /merchant-job-status/{job_id}
    """
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted")

    try:
        content      = await file.read()
        merchant_ids = parse_merchant_csv(content)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CSV parse error: {e}")

    if not merchant_ids:
        raise HTTPException(status_code=422, detail="No valid MerchantID rows found in CSV")

    job_id = str(uuid.uuid4())
    start_bulk_job(job_id, merchant_ids)
    logger.info(f"[merchant] Job {job_id} queued via CSV — {len(merchant_ids)} merchants")

    return {
        "job_id":         job_id,
        "merchant_count": len(merchant_ids),
        "status":         "queued",
        "poll_url":       f"/merchant-job-status/{job_id}",
        "download_url":   f"/merchant-download/{job_id}",
        "message": (
            f"Processing {len(merchant_ids)} merchants in background "
            f"(batches of 50, 8 workers, 20 merchants/browser). "
            f"Poll /merchant-job-status/{job_id} to track progress."
        ),
    }


@app.post("/submit-merchant-ids", tags=["Merchant Bulk"])
def submit_merchant_ids(req: MerchantIDsRequest):
    """
    JSON alternative to /upload-csv — no file upload needed.

    Body:
      { "merchant_ids": ["1103833861", "912519001"] }

    Same processing as /upload-csv (batches of 50, 8 workers).
    """
    import re as _re
    clean_ids = [mid.strip() for mid in req.merchant_ids
                 if mid and _re.match(r"^\d+$", mid.strip())]

    if not clean_ids:
        raise HTTPException(status_code=422, detail="No valid numeric merchant IDs provided")

    job_id = str(uuid.uuid4())
    start_bulk_job(job_id, clean_ids)
    logger.info(f"[merchant] Job {job_id} queued via JSON — {len(clean_ids)} merchants")

    return {
        "job_id":         job_id,
        "merchant_count": len(clean_ids),
        "status":         "queued",
        "poll_url":       f"/merchant-job-status/{job_id}",
        "download_url":   f"/merchant-download/{job_id}",
        "message": (
            f"Processing {len(clean_ids)} merchants in background. "
            f"Poll /merchant-job-status/{job_id} to track progress."
        ),
    }


@app.get("/merchant-job-status/{job_id}", tags=["Merchant Bulk"])
def merchant_job_status(job_id: str):
    """
    Poll the progress of a merchant bulk job.

    Reads from disk — always accurate, survives server restart.

    status: "queued" | "running" | "done"
    """
    job = get_job_status(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return {
        "job_id":              job_id,
        "status":              job.get("status"),
        "total_merchants":     job.get("total_merchants", 0),
        "merchants_done":      job.get("merchants_done", 0),
        "merchants_remaining": job.get("merchants_remaining", 0),
        "batches_total":       job.get("batches_total", 0),
        "batches_done":        job.get("batches_done", 0),
        "batches_failed":      job.get("batches_failed", 0),
        "progress_pct":        job.get("progress_pct", 0.0),
        "config":              job.get("config", {}),
        "started_at":          job.get("started_at"),
        "finished_at":         job.get("finished_at"),
        # Show last 10 batch statuses (not full results — they're on disk)
        "recent_batches":      (job.get("batches") or [])[-10:],
        "download_ready":      job.get("download_ready", False),
        "download_url":        job.get("download_url"),
    }


@app.get("/merchant-download/{job_id}", tags=["Merchant Bulk"])
def merchant_download(job_id: str):
    """
    Download the processed result as a CSV file.
    Only available once job status = "done".
    Streams from disk — never held in memory.

    CSV columns: MerchantID, TotalItems, Error
    """
    job = get_job_status(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    if job.get("status") != "done":
        batches_done  = job.get("batches_done", 0)
        batches_total = job.get("batches_total", 0)
        raise HTTPException(
            status_code=202,
            detail=(
                f"Job not complete yet — status: {job['status']} "
                f"({batches_done}/{batches_total} batches done). "
                f"Retry when status = 'done'."
            ),
        )

    out_path = get_output_path(job_id)
    if not out_path:
        raise HTTPException(status_code=404, detail="Output file not found on disk")

    csv_bytes = out_path.read_bytes()
    filename  = f"merchants_{job_id[:8]}.csv"

    return FastAPIResponse(
        content=csv_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/merchant-jobs", tags=["Merchant Bulk"])
def list_merchant_jobs():
    """
    List all merchant bulk jobs.
    Reads from disk — survives server restart.
    Shows per-job progress, batch breakdown, and download link when done.
    """
    jobs = list_all_jobs()
    return {"jobs": jobs, "count": len(jobs)}
# =============================================================================
# merchant single id
# =============================================================================

@app.post("/merchant-debug", tags=["Merchant Debug"])
def merchant_debug(req: dict):
    """
    DEBUG ENDPOINT (single merchant test)

    Input:
    {
      "merchant_id": "1104990029"
    }

    Purpose:
    - Test ONE merchant scraping
    - Returns raw total_items + full debug info
    - Does NOT run batch system
    """

    merchant_id = req.get("merchant_id")

    if not merchant_id or not str(merchant_id).isdigit():
        raise HTTPException(status_code=400, detail="merchant_id must be numeric")

    try:
        from camoufox.sync_api import Camoufox
        from merchant_scraper import (
            STORE_URL_TEMPLATE,
            _extract_item_count_from_html
        )
        import random
        import time

        url = STORE_URL_TEMPLATE.format(merchant_id=merchant_id)
        ua = random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
        ])

        with Camoufox(headless=True, os="windows") as browser:
            ctx = browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                user_agent=ua,
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )

            page = ctx.new_page()

            start = time.time()

            page.goto(url, timeout=45000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)

            html = page.content()
            lower = html.lower()

            # Detect blocking
            blocked = any(k in lower for k in [
                "captcha", "robot", "verify you are human",
                "access denied", "blocked"
            ])

            count = _extract_item_count_from_html(html)

            end = time.time()

            return {
                "success": True,
                "merchant_id": merchant_id,
                "url": url,
                "total_items": count,
                "status": "BLOCKED" if blocked else "OK",
                "debug": {
                    "page_loaded": True,
                    "html_size": len(html),
                    "blocked_detected": blocked,
                    "selector_used": "auto-extract",
                    "load_time_sec": round(end - start, 2),
                    "final_url": page.url
                }
            }

    except Exception as e:
        return {
            "success": False,
            "merchant_id": merchant_id,
            "error": str(e),
            "status": "FAILED",
            "debug": {
                "page_loaded": False
            }
        }



# =============================================================================
# RELOAD FILTERS
# =============================================================================

@app.post("/reload-filters", tags=["Product Processing"])
def reload_filters():
    """Hot-reload keyword + category filter data from DB."""
    reload_filter_data()
    return {"status": "ok", "message": "Filter data reloaded from DB"}


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
        return ([dict(r) for r in rows] if rows
                else {"message": f"No compliance for product {product_id}"})
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
    logger.info("🚀 Octopia Template Pipeline v2.9")
    logger.info(f"📡 Server: http://0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
