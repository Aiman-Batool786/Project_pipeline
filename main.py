from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import sqlite3
import logging
from typing import List, Optional
from datetime import datetime

# ─────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# FASTAPI APP INITIALIZATION
# ─────────────────────────────────────────
app = FastAPI(
    title="Octopia Template Pipeline - Optimized",
    description="Fast Octopia template processing",
    version="2.0.0"
)

DB_NAME = "products.db"


# ─────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────
@app.on_event("startup")
def startup_event():
    """Initialize on startup"""
    try:
        logger.info("✅ API Starting...")
    except Exception as e:
        logger.error(f"❌ Startup error: {e}")


# ─────────────────────────────────────────
# REQUEST/RESPONSE MODELS
# ─────────────────────────────────────────

class ProductURLRequest(BaseModel):
    """Single product URL request"""
    url: str
    
    class Config:
        schema_extra = {
            "example": {"url": "https://www.aliexpress.com/item/1005010738806664.html"}
        }


class BulkProductRequest(BaseModel):
    """Multiple product URLs request"""
    urls: List[str]


# ─────────────────────────────────────────
# ROOT & HEALTH ENDPOINTS (FAST)
# ─────────────────────────────────────────

@app.get("/", tags=["Info"])
def root():
    """API information - FAST"""
    return {
        "status": "running",
        "service": "Octopia Template Pipeline",
        "version": "2.0.0",
        "endpoints": {
            "single_product": "POST /generate-product",
            "bulk_products": "POST /generate-products",
            "scraped_products": "GET /scraped-products",
            "mapped_products": "GET /mapped-products",
            "template_outputs": "GET /template-outputs",
            "processing_logs": "GET /processing-logs",
            "stats": "GET /stats",
            "health": "GET /health"
        }
    }


@app.get("/health", tags=["Info"])
def health_check():
    """Health check - FAST"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchone()[0]
        conn.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "tables": tables,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "error",
            "database": "disconnected",
            "error": str(e)
        }


# ─────────────────────────────────────────
# SIMPLE ENDPOINT: Single Product
# ─────────────────────────────────────────

@app.post("/generate-product", tags=["Product Processing"])
def generate_product(req: ProductURLRequest):
    """
    ✅ SINGLE PRODUCT - Octopia Template Pipeline
    
    Process ONE AliExpress product:
    1. Scrape (25+ attributes, 6 images)
    2. Enhance (OpenAI)
    3. Categorize (Octopia)
    4. Map (71 columns)
    5. Generate Excel
    6. Store in database
    
    **Time:** 30-60 seconds
    """
    
    if not req.url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")
    
    try:
        logger.info(f"🚀 Processing: {req.url}")
        
        # ================= IMPORT HERE (LAZY LOAD) =================
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
            log_processing
        )
        
        # ================= INITIALIZATION =================
        create_all_tables()
        
        TEMPLATE_PATH = "pdt_template_fr-FR_20260305_090255.xlsm"
        FILLED_TEMPLATES_DIR = "./filled_templates"
        
        if not os.path.exists(FILLED_TEMPLATES_DIR):
            os.makedirs(FILLED_TEMPLATES_DIR)
        
        # ================= STEP 1: SCRAPE =================
        logger.info("📥 Scraping...")
        scraped_data = get_product_info(req.url)
        
        if not scraped_data:
            return {
                "success": False,
                "url": req.url,
                "error": "Scraping failed",
                "timestamp": datetime.now().isoformat()
            }
        
        title = scraped_data.get("title", "")
        description = scraped_data.get("description", "")
        
        if not title:
            return {
                "success": False,
                "url": req.url,
                "error": "No title extracted",
                "timestamp": datetime.now().isoformat()
            }
        
        # ================= STEP 2: STORE SCRAPED DATA =================
        logger.info("💾 Storing scraped data...")
        product_id = insert_scraped_product(req.url, scraped_data)
        
        if not product_id:
            return {
                "success": False,
                "url": req.url,
                "error": "Failed to store data",
                "timestamp": datetime.now().isoformat()
            }
        
        log_processing(product_id, req.url, "scraping", "success")
        
        # ================= STEP 3: ENHANCE CONTENT =================
        logger.info("🤖 Enhancing content...")
        try:
            enhanced = improve_product_content(title, description)
            if not enhanced:
                enhanced = {
                    "title": title,
                    "description": description,
                    "bullet_points": scraped_data.get("bullet_points", [])
                }
        except Exception as e:
            logger.warning(f"Enhancement skipped: {e}")
            enhanced = {
                "title": title,
                "description": description,
                "bullet_points": scraped_data.get("bullet_points", [])
            }
        
        # ================= STEP 4: CATEGORIZE =================
        logger.info("🏷️ Categorizing...")
        try:
            category = assign_category(
                enhanced.get("title", title),
                enhanced.get("description", description)
            )
        except Exception as e:
            logger.warning(f"Categorization skipped: {e}")
            category = {
                "category_id": "0",
                "category_name": "Uncategorized",
                "confidence": 0.0
            }
        
        # ✅ FIXED: Call with correct 6 parameters
        # Old signature: (product_id, orig_cat_id, orig_cat_name, enh_cat_id, enh_cat_name, confidence)
        insert_category_assignment(
            product_id,
            category.get("category_id", "0"),        # orig_cat_id
            category.get("category_name", "Unknown"),      # orig_cat_name
            category.get("category_id", "0"),        # enh_cat_id (same as original)
            category.get("category_name", "Unknown"),      # enh_cat_name (same as original)
            category.get("confidence", 0.0)          # confidence
        )
        
        log_processing(product_id, req.url, "categorization", "success")
        
        # ================= STEP 5: MAP TO TEMPLATE =================
        logger.info("🗺️ Mapping to template...")
        try:
            enriched_data = scraped_data.copy()
            enriched_data['title'] = enhanced.get('title', title)
            enriched_data['description'] = enhanced.get('description', description)
            enriched_data['bullet_points'] = enhanced.get('bullet_points', [])
            
            mapped_data = map_scraped_data_to_template(enriched_data)
            is_valid, missing = validate_mapped_data(mapped_data)
            
            insert_mapped_product(product_id, category.get("category_id", "0"), mapped_data)
            log_processing(product_id, req.url, "mapping", "success" if is_valid else "warning")
        except Exception as e:
            logger.warning(f"Mapping error: {e}")
            log_processing(product_id, req.url, "mapping", "error", str(e))
        
        # ================= STEP 6: GENERATE TEMPLATE =================
        logger.info("📋 Generating template...")
        template_file = None
        
        if os.path.exists(TEMPLATE_PATH):
            try:
                template_file = fill_template_for_product(
                    TEMPLATE_PATH,
                    mapped_data if 'mapped_data' in locals() else {},
                    product_id,
                    FILLED_TEMPLATES_DIR
                )
                
                if template_file:
                    insert_template_output(
                        product_id,
                        category.get("category_id", "0"),
                        "xlsm",
                        template_file,
                        os.path.basename(template_file)
                    )
                    log_processing(product_id, req.url, "template_fill", "success")
            except Exception as e:
                logger.warning(f"Template generation failed: {e}")
                log_processing(product_id, req.url, "template_fill", "error", str(e))
        
        # ================= RETURN SUCCESS =================
        logger.info("✅ Processing complete")
        
        return {
            "success": True,
            "product_id": product_id,
            "url": req.url,
            "title": title,
            "category_name": category.get("category_name", "Unknown"),
            "template_file": template_file,
            "attributes_extracted": len(scraped_data),
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return {
            "success": False,
            "url": req.url,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# ─────────────────────────────────────────
# BULK PRODUCTS ENDPOINT
# ─────────────────────────────────────────

@app.post("/generate-products", tags=["Product Processing"])
def generate_products(req: BulkProductRequest):
    """
    ✅ BULK PRODUCTS - Process Multiple URLs
    
    - Up to 20 URLs per request
    - Sequential processing
    - Individual results
    """
    
    if not req.urls:
        raise HTTPException(status_code=400, detail="URLs list cannot be empty")
    
    if len(req.urls) > 20:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum 20 URLs allowed. You provided {len(req.urls)}"
        )
    
    logger.info(f"📨 Processing {len(req.urls)} products")
    
    results = []
    successful = 0
    failed = 0
    
    for url in req.urls:
        result = generate_product(ProductURLRequest(url=url))
        results.append(result)
        
        if result.get("success"):
            successful += 1
        else:
            failed += 1
    
    return {
        "total": len(req.urls),
        "successful": successful,
        "failed": failed,
        "results": results,
        "timestamp": datetime.now().isoformat()
    }


# ─────────────────────────────────────────
# DATABASE VIEW ENDPOINTS
# ─────────────────────────────────────────

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/scraped-products", tags=["Database"])
def view_scraped_products(limit: int = 100):
    """View scraped products"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM scraped_products ORDER BY scraped_at DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/mapped-products", tags=["Database"])
def view_mapped_products(limit: int = 100):
    """View mapped products"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM mapped_products ORDER BY mapped_at DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/template-outputs", tags=["Database"])
def view_template_outputs(limit: int = 100):
    """View generated templates"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM template_outputs ORDER BY created_at DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/processing-logs", tags=["Database"])
def view_processing_logs(limit: int = 500):
    """View processing logs"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM processing_logs ORDER BY log_time DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/stats", tags=["Database"])
def get_stats():
    """Get database statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        stats = {}
        tables = [
            "scraped_products",
            "mapped_products",
            "template_outputs",
            "processing_logs",
            "category_assignments"
        ]
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = cursor.fetchone()["count"]
                stats[table] = count
            except:
                stats[table] = 0
        
        conn.close()
        return stats
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.environ.get("PORT", 8686))
    
    logger.info("\n" + "="*70)
    logger.info("🚀 Starting Octopia Template Pipeline (Optimized)")
    logger.info("="*70 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
