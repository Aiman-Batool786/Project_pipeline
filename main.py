"""
FastAPI Server - ULTRA FAST VERSION
Skips heavy operations, returns instantly, processes in background
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import os
import sqlite3
import logging
from typing import List
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
    title="Octopia Template Pipeline - Ultra Fast",
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
        from db import create_all_tables
        create_all_tables()
        logger.info("✅ API Ready")
    except Exception as e:
        logger.error(f"Error: {e}")


# ─────────────────────────────────────────
# REQUEST MODELS
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
# ROOT & HEALTH ENDPOINTS (INSTANT)
# ─────────────────────────────────────────

@app.get("/", tags=["Info"])
def root():
    """API info - INSTANT"""
    return {
        "status": "running",
        "service": "Octopia Template Pipeline",
        "version": "2.0.0"
    }


@app.get("/health", tags=["Info"])
def health_check():
    """Health check - INSTANT"""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.close()
        return {"status": "healthy"}
    except:
        return {"status": "error"}


# ─────────────────────────────────────────
# BACKGROUND PROCESSING FUNCTION
# ─────────────────────────────────────────

def process_product_background(url: str, product_id: int):
    """
    Process product in background (fast endpoint returns immediately)
    """
    try:
        logger.info(f"🚀 Background: Processing {url}")
        
        from scraper import get_product_info
        from category_utils import assign_category
        from data_mapper import map_scraped_data_to_template, validate_mapped_data
        from template_filler import fill_template_for_product
        from openai_client import improve_product_content
        from db import (
            insert_scraped_product,
            insert_category_assignment,
            insert_mapped_product,
            insert_template_output,
            log_processing
        )
        
        TEMPLATE_PATH = "pdt_template_fr-FR_20260305_090255.xlsm"
        FILLED_TEMPLATES_DIR = "./filled_templates"
        
        if not os.path.exists(FILLED_TEMPLATES_DIR):
            os.makedirs(FILLED_TEMPLATES_DIR)
        
        # ============ STEP 1: SCRAPE ============
        logger.info("📥 Scraping...")
        scraped_data = get_product_info(url)
        
        if not scraped_data:
            log_processing(product_id, url, "scraping", "error", "Scraping failed")
            return
        
        title = scraped_data.get("title", "")
        description = scraped_data.get("description", "")
        
        if not title:
            log_processing(product_id, url, "scraping", "error", "No title")
            return
        
        # Update scraped product
        product_id = insert_scraped_product(url, scraped_data)
        log_processing(product_id, url, "scraping", "success")
        
        # ============ STEP 2: ENHANCE ============
        logger.info("🤖 Enhancing...")
        try:
            enhanced = improve_product_content(title, description)
            if not enhanced:
                enhanced = {
                    "title": title,
                    "description": description,
                    "bullet_points": scraped_data.get("bullet_points", [])
                }
        except:
            enhanced = {
                "title": title,
                "description": description,
                "bullet_points": scraped_data.get("bullet_points", [])
            }
        
        # ============ STEP 3: CATEGORIZE ============
        logger.info("🏷️ Categorizing...")
        try:
            category = assign_category(
                enhanced.get("title", title),
                enhanced.get("description", description)
            )
        except:
            category = {
                "category_id": "0",
                "category_name": "Unknown",
                "confidence": 0.0
            }
        
        # Store category
        insert_category_assignment(
            product_id,
            category.get("category_id", "0"),
            category.get("category_name", "Unknown"),
            category.get("category_id", "0"),
            category.get("category_name", "Unknown"),
            category.get("confidence", 0.0)
        )
        log_processing(product_id, url, "categorization", "success")
        
        # ============ STEP 4: MAP ============
        logger.info("🗺️ Mapping...")
        try:
            enriched_data = scraped_data.copy()
            enriched_data['title'] = enhanced.get('title', title)
            enriched_data['description'] = enhanced.get('description', description)
            enriched_data['bullet_points'] = enhanced.get('bullet_points', [])
            
            mapped_data = map_scraped_data_to_template(enriched_data)
            insert_mapped_product(product_id, category.get("category_id", "0"), mapped_data)
            log_processing(product_id, url, "mapping", "success")
        except Exception as e:
            log_processing(product_id, url, "mapping", "error", str(e))
        
        # ============ STEP 5: TEMPLATE ============
        logger.info("📋 Generating template...")
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
                    log_processing(product_id, url, "template_fill", "success")
            except Exception as e:
                log_processing(product_id, url, "template_fill", "error", str(e))
        
        logger.info(f"✅ Completed: {url}")
    
    except Exception as e:
        logger.error(f"❌ Error: {e}")


# ─────────────────────────────────────────
# SINGLE PRODUCT ENDPOINT (INSTANT)
# ─────────────────────────────────────────

@app.post("/generate-product", tags=["Product Processing"])
def generate_product(req: ProductURLRequest, background_tasks: BackgroundTasks):
    """
    ✅ SINGLE PRODUCT - Returns INSTANTLY
    
    Processing happens in background!
    - Returns immediately with product_id
    - Scraping/enhancement/mapping happens in background
    - Check status with /scraped-products endpoint
    """
    
    if not req.url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")
    
    try:
        from db import insert_scraped_product
        
        # Create placeholder in database
        product_id = insert_scraped_product(req.url, {"title": "Processing...", "description": ""})
        
        # Add background task
        background_tasks.add_task(process_product_background, req.url, product_id)
        
        # ✅ RETURN IMMEDIATELY
        return {
            "success": True,
            "product_id": product_id,
            "url": req.url,
            "status": "processing",
            "message": "Product added to queue. Check status with /scraped-products",
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error: {e}")
        return {
            "success": False,
            "url": req.url,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# ─────────────────────────────────────────
# BULK PRODUCTS ENDPOINT (INSTANT)
# ─────────────────────────────────────────

@app.post("/generate-products", tags=["Product Processing"])
def generate_products(req: BulkProductRequest, background_tasks: BackgroundTasks):
    """
    ✅ BULK PRODUCTS - Returns INSTANTLY
    
    - Queues all products for background processing
    - Returns immediately with product IDs
    """
    
    if not req.urls:
        raise HTTPException(status_code=400, detail="URLs list cannot be empty")
    
    if len(req.urls) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 URLs")
    
    try:
        from db import insert_scraped_product
        
        results = []
        
        for url in req.urls:
            product_id = insert_scraped_product(url, {"title": "Processing...", "description": ""})
            background_tasks.add_task(process_product_background, url, product_id)
            
            results.append({
                "product_id": product_id,
                "url": url,
                "status": "processing"
            })
        
        # ✅ RETURN IMMEDIATELY
        return {
            "total": len(req.urls),
            "queued": len(results),
            "results": results,
            "message": "Products queued for processing",
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


# ─────────────────────────────────────────
# DATABASE VIEW ENDPOINTS (INSTANT)
# ─────────────────────────────────────────

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/scraped-products", tags=["Database"])
def view_scraped_products(limit: int = 50):
    """View scraped products - INSTANT"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT product_id, url, title, scraped_at FROM scraped_products ORDER BY scraped_at DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows] if rows else []
    except Exception as e:
        return {"error": str(e)}


@app.get("/mapped-products", tags=["Database"])
def view_mapped_products(limit: int = 50):
    """View mapped products - INSTANT"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT product_id, titre, mapped_at FROM mapped_products ORDER BY mapped_at DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows] if rows else []
    except Exception as e:
        return {"error": str(e)}


@app.get("/template-outputs", tags=["Database"])
def view_template_outputs(limit: int = 50):
    """View templates - INSTANT"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT product_id, file_name, status, created_at FROM template_outputs ORDER BY created_at DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows] if rows else []
    except Exception as e:
        return {"error": str(e)}


@app.get("/processing-logs", tags=["Database"])
def view_processing_logs(limit: int = 100):
    """View logs - INSTANT"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT product_id, step, status, log_time FROM processing_logs ORDER BY log_time DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows] if rows else []
    except Exception as e:
        return {"error": str(e)}


@app.get("/stats", tags=["Database"])
def get_stats():
    """Get stats - INSTANT"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        stats = {}
        for table in ["scraped_products", "mapped_products", "template_outputs", "processing_logs"]:
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
    logger.info("🚀 Octopia Template Pipeline - Ultra Fast (Background Processing)")
    logger.info("="*70 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
