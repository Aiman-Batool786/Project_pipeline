"""
FastAPI Server - HYBRID APPROACH v2.9
Pipeline: Scrape → Store → Enhance → Categorize → Map → Excel
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

import uuid
import random
import time
import re as _re
from fastapi import UploadFile, File, BackgroundTasks
from fastapi.responses import Response as FastAPIResponse
from merchant_scraper import (
    parse_merchant_csv,
    start_bulk_job,
    get_job_status,
    get_output_path,
    list_all_jobs,
    _make_context,
    _JS_EXTRACT_COUNT,
    _wait_for_item_count,
    STORE_URL_TEMPLATE,
    USER_AGENTS,
    PAGE_TIMEOUT,
    REAL_BLOCK_SIGNALS,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Octopia Template Pipeline", version="2.9.0")

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
    merchant_ids: List[str]

    class Config:
        json_schema_extra = {
            "example": {"merchant_ids": ["1103833861", "912519001", "567839201"]}
        }


class MerchantDebugRequest(BaseModel):
    merchant_id: str

    class Config:
        json_schema_extra = {"example": {"merchant_id": "1104990029"}}


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
            create_all_tables, insert_scraped_product, insert_seller_info,
            insert_compliance_info, insert_category_assignment, insert_mapped_product,
            insert_template_output, insert_enhanced_content, insert_original_specifications,
            insert_enhanced_specifications, log_all_spec_audits, log_processing,
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

        scraped_data = get_product_info(url, extract_compliance=extract_compliance)
        if not scraped_data:
            return {"success": False, "url": url, "error": "Scraping failed",
                    "timestamp": datetime.now().isoformat()}

        original_title = scraped_data.get("title", "")
        description    = scraped_data.get("description", "")
        if not original_title:
            return {"success": False, "url": url, "error": "No title extracted",
                    "timestamp": datetime.now().isoformat()}

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

        product_data_for_llm = {
            k: v for k, v in scraped_data.items()
            if k not in SELLER_FIELDS and k != 'compliance'
        }

        try:
            enhanced = improve_product_content(
                title=original_title, description=description,
                specifications=product_data_for_llm, category=None
            )
            if not enhanced:
                raise ValueError("OpenAI returned None")
        except Exception as e:
            logger.warning(f"Enhancement skipped: {e}")
            enhanced = {
                "title": original_title, "description": description,
                "bullet_points": scraped_data.get("bullet_points", []),
                "html_description": "", "specifications_enhanced": {}
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

        confidence = float(category.get("confidence", 0.0))
        conf_accepted, conf_reason = validate_category_confidence(confidence)
        if not conf_accepted:
            category = {
                "category_id": "0", "category_name": "Uncategorized",
                "category_leaf": "Uncategorized", "category_path": "",
                "confidence": confidence,
            }

        insert_category_assignment(
            product_id,
            category.get("category_id", "0"), category.get("category_name", "Uncategorized"),
            category.get("category_id", "0"), category.get("category_name", "Uncategorized"),
            confidence
        )
        log_processing(product_id, url, "categorization", "success")

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
            "success": True, "product_id": product_id, "url": url,
            "original_title": original_title, "enhanced_title": enhanced_title,
            "original": {
                "title": original_title,
                "description": (description[:200] + "..." if len(description) > 200 else description),
                **{f: scraped_data.get(f, "") for f in SPEC_FIELDS},
                "images": sum(1 for i in range(1, 21) if scraped_data.get(f"image_{i}"))
            },
            "seller": {f: scraped_data.get(f, "") for f in SELLER_FIELDS},
            "compliance": compliance_data,
            "shipment_country": scraped_data.get("shipment_country"),
            "delivery_start":   scraped_data.get("delivery_start"),
            "delivery_end":     scraped_data.get("delivery_end"),
            "delivery_days":    scraped_data.get("delivery_days"),
            "remaining_stock":  scraped_data.get("remaining_stock"),
            "rating":           scraped_data.get("rating", ""),
            "category": {
                "id": category.get("category_id", ""), "name": category.get("category_name", ""),
                "leaf": category.get("category_leaf", ""), "path": category.get("category_path", ""),
                "confidence": round(confidence, 2)
            },
            "enhanced": {
                "title": enhanced_title,
                "description": (enhanced.get("description", "")[:200] + "..."
                                if enhanced.get("description") else ""),
                "bullet_points": enhanced.get("bullet_points", [])[:3],
                "has_html_description": bool(enhanced.get("html_description", "")),
                "specifications_enhanced": specs_enhanced
            },
            "template": {
                "file": os.path.basename(template_file) if template_file else None,
                "columns_mapped": len(mapped_data), "fields_valid": is_valid,
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
    if not request.search_url:
        raise HTTPException(status_code=400, detail="search_url is required")
    if 'aliexpress.com' not in request.search_url.lower():
        raise HTTPException(status_code=400, detail="Invalid AliExpress URL")

    max_pages = request.max_pages if request.max_pages is not None else 5
    max_pages = min(max_pages, MAX_SEARCH_PAGES)

    try:
        raw      = scrape_search_results(search_url=request.search_url,
                                         max_pages=max_pages,
                                         delay=request.delay_between_requests)
        products = raw.get("products", []) if isinstance(raw, dict) else (raw or [])

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
                    pass

            if is_restricted:
                result.append({
                    "product_id": str(p["product_id"]), "product_url": p["product_url"],
                    "title": original_title, "rating": product_rating,
                    "sold_count": p.get("sold_count", ""), "status": "rejected",
                    "message": "Title not fetched due to restricted keyword",
                })
                rejected_count += 1
            elif rating_filter_fail:
                result.append({
                    "product_id": str(p["product_id"]), "product_url": p["product_url"],
                    "title": original_title, "rating": product_rating,
                    "sold_count": p.get("sold_count", ""), "status": "rejected",
                    "message": f"Rating {rating_float:.1f} is below minimum 4.0",
                })
                rejected_count += 1
            else:
                result.append({
                    "product_id": str(p["product_id"]), "product_url": p["product_url"],
                    "title": original_title, "rating": product_rating,
                    "sold_count": p.get("sold_count", ""), "status": "accepted",
                })
                accepted_count += 1

        return result

    except Exception as e:
        logger.error(f"❌ Search scrape failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Scraping failed: {str(e)}")


# =============================================================================
# GENERATE-PRODUCT ENDPOINT
# =============================================================================

@app.post("/generate-product", tags=["Product Processing"])
def generate_product(req: ProductURLRequest):
    if not req.url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")

    result = process_product_complete(req.url, extract_compliance=req.extract_compliance)
    if not result.get("success"):
        return result

    original_title = result.get("original_title", "")
    raw_category   = result.get("category")

    if filter_restricted_keywords(original_title):
        return {
            "status": "rejected", "reason": "Title has restricted keyword",
            "product_id": result.get("product_id"), "original_title": original_title,
            "url": req.url, "timestamp": datetime.now().isoformat(),
        }

    from product_filter import is_category_restricted
    cat_blocked, cat_reason = is_category_restricted(raw_category)
    if cat_blocked:
        return {
            "status": "rejected", "reason": cat_reason,
            "product_id": result.get("product_id"), "original_title": original_title,
            "url": req.url, "timestamp": datetime.now().isoformat(),
        }

    return {
        "status": "accepted", "product_id": result.get("product_id"),
        "url": req.url, "original_title": original_title,
        "enhanced_title": result.get("enhanced_title", ""),
        "category": result.get("category", {}).get("name", ""),
        "confidence": result.get("category", {}).get("confidence", 0.0),
        "rating": result.get("rating", ""),
        "shipment_country": result.get("shipment_country"),
        "delivery_start":   result.get("delivery_start"),
        "delivery_end":     result.get("delivery_end"),
        "delivery_days":    result.get("delivery_days"),
        "remaining_stock":  result.get("remaining_stock"),
        "enhanced":   result.get("enhanced", {}),
        "seller":     result.get("seller", {}),
        "compliance": result.get("compliance", {}),
        "template":   result.get("template", {}),
        "timestamp":  result.get("timestamp"),
    }


# =============================================================================
# GENERATE-PRODUCTS ENDPOINT
# =============================================================================

@app.post("/generate-products", tags=["Product Processing"])
def generate_products(req: BulkProductRequest):
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
            results.append({"status": "error", "url": url,
                             "reason": result.get("error", "Unknown error"),
                             "timestamp": datetime.now().isoformat()})
            continue

        original_title = result.get("original_title", "")
        raw_category   = result.get("category")

        if filter_restricted_keywords(original_title):
            rejected += 1
            results.append({"status": "rejected", "reason": "Title has restricted keyword",
                             "original_title": original_title, "url": url,
                             "timestamp": datetime.now().isoformat()})
            continue

        cat_blocked, cat_reason = is_category_restricted(raw_category)
        if cat_blocked:
            rejected += 1
            results.append({"status": "rejected", "reason": cat_reason,
                             "original_title": original_title, "url": url,
                             "timestamp": datetime.now().isoformat()})
            continue

        successful += 1
        results.append({
            "status": "accepted", "product_id": result.get("product_id"),
            "url": url, "original_title": original_title,
            "enhanced_title": result.get("enhanced_title", ""),
            "category": result.get("category", {}).get("name", ""),
            "confidence": result.get("category", {}).get("confidence", 0.0),
            "enhanced": result.get("enhanced", {}), "seller": result.get("seller", {}),
            "compliance": result.get("compliance", {}), "template": result.get("template", {}),
            "timestamp": result.get("timestamp"),
        })

    return {
        "total": len(req.urls), "successful": successful,
        "rejected": rejected, "failed": failed,
        "results": results, "timestamp": datetime.now().isoformat(),
    }


# =============================================================================
# PRODUCT INFO BY ID ENDPOINT
# =============================================================================

@app.get("/product-info/{product_id}", tags=["Product Processing"])
def get_product_info_by_id(product_id: str, extract_compliance: bool = False):
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
            "status": "rejected", "reason": "Title has restricted keyword",
            "product_id": result.get("product_id"), "original_title": original_title,
            "url": eur_url, "timestamp": datetime.now().isoformat(),
        }

    from product_filter import is_category_restricted
    cat_blocked, cat_reason = is_category_restricted(result.get("category"))
    if cat_blocked:
        return {
            "status": "rejected", "reason": cat_reason,
            "product_id": result.get("product_id"), "original_title": original_title,
            "url": eur_url, "timestamp": datetime.now().isoformat(),
        }

    return {
        "status": "accepted", "product_id": result.get("product_id"),
        "aliexpress_id": product_id, "url": eur_url,
        "original_title": original_title, "enhanced_title": result.get("enhanced_title", ""),
        "category": result.get("category", {}).get("name", ""),
        "confidence": result.get("category", {}).get("confidence", 0.0),
        "rating": result.get("rating", ""),
        "shipment_country": result.get("shipment_country"),
        "delivery_start":   result.get("delivery_start"),
        "delivery_end":     result.get("delivery_end"),
        "delivery_days":    result.get("delivery_days"),
        "remaining_stock":  result.get("remaining_stock"),
        "enhanced":   result.get("enhanced", {}),
        "seller":     result.get("seller", {}),
        "compliance": result.get("compliance", {}),
        "template":   result.get("template", {}),
        "timestamp":  result.get("timestamp"),
    }


# =============================================================================
# MERCHANT BULK ENDPOINTS
# =============================================================================

@app.post("/upload-csv", tags=["Merchant Bulk"])
async def upload_merchant_csv(file: UploadFile = File(...)):
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
    return {
        "job_id": job_id, "merchant_count": len(merchant_ids), "status": "queued",
        "poll_url": f"/merchant-job-status/{job_id}",
        "download_url": f"/merchant-download/{job_id}",
        "message": f"Processing {len(merchant_ids)} merchants in background.",
    }


@app.post("/submit-merchant-ids", tags=["Merchant Bulk"])
def submit_merchant_ids(req: MerchantIDsRequest):
    import re as _re2
    clean_ids = [mid.strip() for mid in req.merchant_ids
                 if mid and _re2.match(r"^\d+$", mid.strip())]
    if not clean_ids:
        raise HTTPException(status_code=422, detail="No valid numeric merchant IDs provided")

    job_id = str(uuid.uuid4())
    start_bulk_job(job_id, clean_ids)
    return {
        "job_id": job_id, "merchant_count": len(clean_ids), "status": "queued",
        "poll_url": f"/merchant-job-status/{job_id}",
        "download_url": f"/merchant-download/{job_id}",
        "message": f"Processing {len(clean_ids)} merchants in background.",
    }


@app.get("/merchant-job-status/{job_id}", tags=["Merchant Bulk"])
def merchant_job_status(job_id: str):
    job = get_job_status(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return {
        "job_id":   job_id,
        "status":   job.get("status", "unknown"),
        "total":    job.get("total_merchants", job.get("total", 0)),
        "done":     job.get("merchants_done", 0),
        "progress_pct":   job.get("progress_pct", 0.0),
        "batches_total":  job.get("batches_total", 0),
        "batches_done":   job.get("batches_done", 0),
        "batches_failed": job.get("batches_failed", 0),
        "download_ready": job.get("download_ready", False),
        "download_url":   job.get("download_url"),
    }


@app.get("/merchant-download/{job_id}", tags=["Merchant Bulk"])
def merchant_download(job_id: str):
    job = get_job_status(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    if job.get("status") != "done":
        raise HTTPException(status_code=202,
                            detail=f"Job not complete yet — status: {job['status']}")

    out_path = get_output_path(job_id)
    if not out_path:
        raise HTTPException(status_code=404, detail="Output file not found on disk")

    return FastAPIResponse(
        content=out_path.read_bytes(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="merchants_{job_id[:8]}.csv"'},
    )


@app.get("/merchant-jobs", tags=["Merchant Bulk"])
def list_merchant_jobs():
    jobs = list_all_jobs()
    return {"jobs": jobs, "count": len(jobs)}


# =============================================================================
# MERCHANT DEBUG  v3.4
# KEY FIX: uses _wait_for_item_count() which POLLS the DOM every 100 ms
# instead of a single page.evaluate() that fires before React mounts.
# =============================================================================

@app.post("/merchant-debug", tags=["Merchant Bulk"])
def merchant_debug(req: MerchantDebugRequest):
    """
    DEBUG — scrape ONE merchant with full diagnostic info.

    v3.4 FIX — Root cause of "Selector Missing":
      networkidle fires when no network requests for 500 ms, but React can
      still be mid-render with zero network activity. A single page.evaluate()
      at that moment returns null because the span isn't in the DOM yet.

    Fix: _wait_for_item_count() polls the DOM every 100 ms for up to 20 s
    using wait_for_function(). The moment React mounts the item count span,
    we capture it — regardless of how long rendering takes.

    Input:  { "merchant_id": "246548581" }
    """
    from camoufox.sync_api import Camoufox

    merchant_id = str(req.merchant_id).strip()
    if not merchant_id.isdigit():
        raise HTTPException(status_code=400, detail="merchant_id must be numeric")

    url = STORE_URL_TEMPLATE.format(merchant_id=merchant_id)
    ua  = random.choice(USER_AGENTS)
    t0  = time.time()

    debug = {
        "url":                    url,
        "page_loaded":            False,
        "final_url":              None,
        "html_size_bytes":        0,
        "blocked":                False,
        "reload_path_detected":   None,
        "networkidle":            None,
        "selector_hit":           None,
        "poll_result":            None,   # what DOM polling found
        "js_count":               None,   # fallback evaluate result
        "redirected_to":          None,
        "locale_cookies_injected": True,
        "load_time_sec":          None,
        "nav_error":              None,
    }

    try:
        with Camoufox(headless=True, os="windows") as browser:
            ctx  = _make_context(browser, ua)
            page = ctx.new_page()

            # ── STEP 1: Navigate ──────────────────────────────────────────────
            try:
                page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                debug["page_loaded"] = True
            except Exception as nav_err:
                es = str(nav_err)
                debug["nav_error"] = es[:200]
                if "NS_BINDING_ABORTED" in es or "ERR_ABORTED" in es:
                    debug["page_loaded"] = True
                    debug["nav_error"]   = "Redirect mid-load — continuing"
                elif any(x in es for x in ["ERR_NAME_NOT_RESOLVED", "NS_ERROR"]):
                    page.close(); ctx.close()
                    return {"success": False, "merchant_id": merchant_id,
                            "total_items": None, "error": "Page Not Found", "debug": debug}
                else:
                    page.close(); ctx.close()
                    return {"success": False, "merchant_id": merchant_id,
                            "total_items": None, "error": f"Nav: {es[:150]}", "debug": debug}

            # ── STEP 2: Follow ae:reload_path meta-redirect ───────────────────
            try:
                reload_url = page.evaluate("""() => {
                    const m = document.querySelector('meta[property="ae:reload_path"]');
                    return m ? m.getAttribute('content') : null;
                }""")
                if reload_url and reload_url.strip() != page.url.strip():
                    debug["reload_path_detected"] = reload_url
                    try:
                        page.goto(reload_url, timeout=PAGE_TIMEOUT,
                                  wait_until="domcontentloaded")
                    except Exception as redir_err:
                        rs = str(redir_err)
                        if "NS_BINDING_ABORTED" not in rs and "ERR_ABORTED" not in rs:
                            debug["nav_error"] = f"reload_path: {rs[:100]}"
            except Exception as meta_err:
                debug["nav_error"] = f"meta eval: {str(meta_err)[:80]}"

            # ── STEP 3: networkidle warm-up (not the extraction signal) ───────
            try:
                page.wait_for_load_state("networkidle", timeout=15_000)
                debug["networkidle"] = "reached"
            except Exception:
                debug["networkidle"] = "timeout — used fallback"

            # ── STEP 4: Scroll to trigger lazy-loaded sections ────────────────
            for _ in range(3):
                page.mouse.wheel(0, 700)
                page.wait_for_timeout(400)
            page.wait_for_timeout(1_000)

            # ── STEP 5: POLL DOM until item count appears ─────────────────────
            # This is the core fix. Polls every 100 ms for up to 20 s.
            # Catches the element the moment React renders it.
            polled_count = _wait_for_item_count(page, poll_timeout_ms=20_000)
            debug["poll_result"] = polled_count

            # Fallback: single evaluate if polling timed out
            js_count = polled_count
            if js_count is None:
                try:
                    js_count = page.evaluate(_JS_EXTRACT_COUNT)
                    debug["js_count"] = js_count
                except Exception:
                    pass

            debug["final_url"]     = page.url
            debug["load_time_sec"] = round(time.time() - t0, 2)

            html = page.content()
            debug["html_size_bytes"] = len(html)
            page.close()
            ctx.close()

        lower = html.lower()

        # ── Redirect detection ────────────────────────────────────────────────
        m_redir = _re.search(r'/store/(\d+)/', debug["final_url"] or "")
        if m_redir and m_redir.group(1) != merchant_id:
            debug["redirected_to"] = m_redir.group(1)
            debug["note"] = (
                f"Store {merchant_id} → {debug['redirected_to']} (ID migration)"
            )

        # ── Block detection ───────────────────────────────────────────────────
        debug["blocked"] = any(sig in lower for sig in REAL_BLOCK_SIGNALS)
        if debug["blocked"]:
            return {"success": False, "merchant_id": merchant_id,
                    "total_items": None, "error": "Blocked/CAPTCHA", "debug": debug}

        if js_count is not None:
            return {"success": True, "merchant_id": merchant_id,
                    "total_items": js_count, "error": None, "debug": debug}

        debug["html_head_500"] = html[:500].replace("\n", " ")
        return {
            "success": False, "merchant_id": merchant_id, "total_items": None,
            "error": "Selector Missing after polling — check debug.html_head_500",
            "debug": debug,
        }

    except Exception as exc:
        debug["load_time_sec"] = round(time.time() - t0, 2)
        return {"success": False, "merchant_id": merchant_id,
                "total_items": None, "error": str(exc)[:300], "debug": debug}


# =============================================================================
# RELOAD FILTERS
# =============================================================================

@app.post("/reload-filters", tags=["Product Processing"])
def reload_filters():
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
