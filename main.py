from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
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
    log_processing,
    get_product_by_id
)

import json
import os
import sqlite3
import logging
from typing import List, Optional, Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    title="Octopia Template Pipeline - Complete Task 2",
    description="Advanced scraping + content enhancement + Octopia template mapping + database",
    version="2.0.0"
)

# Thread pool for concurrent processing
executor = ThreadPoolExecutor(max_workers=3)

# Task 2 configuration
TEMPLATE_PATH = "pdt_template_fr-FR_20260305_090255.xlsm"
FILLED_TEMPLATES_DIR = "./filled_templates"
EXPORTS_DIR = "./exports"
DB_NAME = "products.db"


# ─────────────────────────────────────────
# STARTUP/SHUTDOWN EVENTS
# ─────────────────────────────────────────
@app.on_event("startup")
def startup_event():
    """Initialize on startup"""
    try:
        create_all_tables()
        logger.info("✅ Database initialized (Task 2 tables created)")
        
        # Create directories
        for directory in [FILLED_TEMPLATES_DIR, EXPORTS_DIR]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                logger.info(f"✅ Created directory: {directory}")
        
        # Check template
        if os.path.exists(TEMPLATE_PATH):
            logger.info(f"✅ Template found: {TEMPLATE_PATH}")
        else:
            logger.warning(f"⚠️ Template not found: {TEMPLATE_PATH}")
    
    except Exception as e:
        logger.error(f"❌ Startup error: {e}")
        raise


@app.on_event("shutdown")
def shutdown_event():
    """Cleanup on shutdown"""
    try:
        executor.shutdown(wait=True)
        logger.info("✅ ThreadPoolExecutor shut down")
    except Exception as e:
        logger.error(f"⚠️ Shutdown error: {e}")


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
    
    class Config:
        schema_extra = {
            "example": {
                "urls": [
                    "https://www.aliexpress.com/item/1005010738806664.html",
                    "https://www.aliexpress.com/item/1005007757337814.html"
                ]
            }
        }


class ProductResponse(BaseModel):
    """Single product response"""
    success: bool
    product_id: Optional[int] = None
    url: str
    title: str
    category_name: str
    template_file: Optional[str] = None
    error: Optional[str] = None
    timestamp: str


# ─────────────────────────────────────────
# ROOT & HEALTH ENDPOINTS
# ─────────────────────────────────────────

@app.get("/", tags=["Info"])
def root():
    """API information"""
    return {
        "status": "running",
        "service": "Octopia Template Pipeline - Task 2",
        "version": "2.0.0",
        "features": [
            "Advanced product scraping (25+ attributes, 6 images)",
            "Content enhancement with OpenAI",
            "Octopia category detection (5,806 categories)",
            "Template mapping (71 Octopia columns)",
            "Excel XLSM generation",
            "Bulk processing (concurrent)",
            "Database storage and views"
        ],
        "endpoints": {
            "single_product": "POST /generate-product",
            "bulk_products": "POST /generate-products",
            "scraped_products": "GET /scraped-products",
            "mapped_products": "GET /mapped-products",
            "template_outputs": "GET /template-outputs",
            "processing_logs": "GET /processing-logs",
            "stats": "GET /stats",
            "health": "GET /health"
        },
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health", tags=["Info"])
def health_check():
    """Health check endpoint"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # Get table counts
        tables = ["scraped_products", "mapped_products", "template_outputs", "processing_logs"]
        counts = {}
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                counts[table] = count
            except:
                counts[table] = 0
        
        conn.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "scraper": "Advanced (25+ attributes)",
            "template": "Octopia (71 columns)",
            "enhancement": "OpenAI enabled",
            "products_processed": counts.get("scraped_products", 0),
            "products_mapped": counts.get("mapped_products", 0),
            "templates_generated": counts.get("template_outputs", 0),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")


# ═════════════════════════════════════════
# MAIN PROCESSING FUNCTION
# ═════════════════════════════════════════

def process_product_complete(url: str) -> ProductResponse:
    """
    Complete Octopia pipeline for single product:
    1. Advanced scraping (25+ attributes)
    2. Content enhancement (OpenAI)
    3. Octopia categorization
    4. Template mapping (71 columns)
    5. Excel generation
    6. Database storage
    """
    
    product_id = None
    
    try:
        logger.info(f"\n{'='*70}")
        logger.info(f"🚀 PROCESSING: {url}")
        logger.info(f"{'='*70}\n")
        
        # =====================================================
        # STEP 1: ADVANCED SCRAPING
        # =====================================================
        logger.info("📥 STEP 1: Advanced Scraping (25+ attributes, 6 images)")
        
        scraped_data = get_product_info(url)
        
        if not scraped_data:
            return ProductResponse(
                success=False,
                url=url,
                title="",
                category_name="",
                error="Scraping failed - page blocked or invalid URL",
                timestamp=datetime.now().isoformat()
            )
        
        logger.info(f"✅ Extracted {len(scraped_data)} attributes")
        
        title = scraped_data.get("title", "")
        description = scraped_data.get("description", "")
        
        if not title:
            return ProductResponse(
                success=False,
                url=url,
                title="",
                category_name="",
                error="Could not extract product title",
                timestamp=datetime.now().isoformat()
            )
        
        # =====================================================
        # STEP 2: STORE SCRAPED DATA
        # =====================================================
        logger.info("💾 STEP 2: Storing Scraped Data")
        
        product_id = insert_scraped_product(url, scraped_data)
        
        if not product_id:
            return ProductResponse(
                success=False,
                url=url,
                title=title,
                category_name="",
                error="Failed to store scraped data",
                timestamp=datetime.now().isoformat()
            )
        
        log_processing(product_id, url, "scraping", "success")
        logger.info(f"✅ Scraped data stored (product_id={product_id})")
        
        # =====================================================
        # STEP 3: CONTENT ENHANCEMENT WITH OPENAI
        # =====================================================
        logger.info("🤖 STEP 3: Enhancing Content with OpenAI")
        
        enhanced = improve_product_content(title, description)
        
        if not enhanced:
            logger.warning("⚠️ OpenAI enhancement failed, using original content")
            enhanced = {
                "title": title,
                "description": description,
                "bullet_points": scraped_data.get("bullet_points", [])
            }
        else:
            logger.info("✅ Content enhanced successfully")
        
        # =====================================================
        # STEP 4: OCTOPIA CATEGORY DETECTION
        # =====================================================
        logger.info("🏷️ STEP 4: Detecting Octopia Category")
        
        category = assign_category(
            enhanced.get("title", title),
            enhanced.get("description", description)
        )
        
        logger.info(f"✅ Category: {category['category_name']} (confidence: {category['confidence']:.2f})")
        
        insert_category_assignment(
            product_id,
            category["category_id"],
            category["category_name"],
            category["confidence"]
        )
        
        log_processing(product_id, url, "categorization", "success")
        
        # =====================================================
        # STEP 5: MAP TO OCTOPIA TEMPLATE COLUMNS
        # =====================================================
        logger.info("🗺️ STEP 5: Mapping to Octopia Template (71 columns)")
        
        # Use enhanced content for mapping
        enriched_data = scraped_data.copy()
        enriched_data['title'] = enhanced.get('title', title)
        enriched_data['description'] = enhanced.get('description', description)
        enriched_data['bullet_points'] = enhanced.get('bullet_points', scraped_data.get('bullet_points', []))
        
        mapped_data = map_scraped_data_to_template(enriched_data)
        is_valid, missing = validate_mapped_data(mapped_data)
        
        insert_mapped_product(product_id, category["category_id"], mapped_data)
        
        if is_valid:
            logger.info("✅ All required fields valid")
            log_processing(product_id, url, "mapping", "success")
        else:
            logger.warning(f"⚠️ Missing fields: {missing}")
            log_processing(product_id, url, "mapping", "warning", f"Missing: {missing}")
        
        # =====================================================
        # STEP 6: GENERATE OCTOPIA TEMPLATE FILE
        # =====================================================
        logger.info("📋 STEP 6: Generating Octopia Template (XLSM)")
        
        template_file = None
        
        if not os.path.exists(TEMPLATE_PATH):
            logger.warning(f"⚠️ Template not found: {TEMPLATE_PATH}")
            log_processing(product_id, url, "template_fill", "warning", "Template file not found")
        else:
            template_file = fill_template_for_product(
                TEMPLATE_PATH,
                mapped_data,
                product_id,
                FILLED_TEMPLATES_DIR
            )
            
            if template_file:
                logger.info(f"✅ Template generated: {os.path.basename(template_file)}")
                
                insert_template_output(
                    product_id,
                    category["category_id"],
                    "xlsm",
                    template_file,
                    os.path.basename(template_file)
                )
                
                log_processing(product_id, url, "template_fill", "success", template_file)
            else:
                logger.error("❌ Failed to generate template")
                log_processing(product_id, url, "template_fill", "error", "Template generation failed")
        
        # =====================================================
        # RETURN SUCCESS RESPONSE
        # =====================================================
        
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ PROCESSING COMPLETE (product_id={product_id})")
        logger.info(f"{'='*70}\n")
        
        return ProductResponse(
            success=True,
            product_id=product_id,
            url=url,
            title=title,
            category_name=category["category_name"],
            template_file=template_file,
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        logger.error(f"❌ ERROR: {e}", exc_info=True)
        
        if product_id:
            log_processing(product_id, url, "processing", "error", str(e))
        
        return ProductResponse(
            success=False,
            product_id=product_id,
            url=url,
            title="",
            category_name="",
            error=str(e),
            timestamp=datetime.now().isoformat()
        )


# ═════════════════════════════════════════
# SINGLE PRODUCT ENDPOINT
# ═════════════════════════════════════════

@app.post("/generate-product", response_model=ProductResponse, tags=["Product Processing"])
def generate_product(req: ProductURLRequest):
    """
    ✅ SINGLE PRODUCT - Complete Octopia Template Pipeline
    
    Process ONE AliExpress product with full pipeline:
    1. 📥 Advanced scraping (25+ attributes, 6 images)
    2. 💾 Store scraped data
    3. 🤖 OpenAI content enhancement
    4. 🏷️ Octopia category detection (5,806 categories)
    5. 🗺️ Template mapping (71 columns)
    6. 📋 Excel XLSM generation
    7. 💾 Database storage
    
    Returns:
    - Product ID
    - Octopia category
    - Generated template file path
    
    **Note:** Processing takes 30-60 seconds
    """
    
    if not req.url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")
    
    logger.info(f"📨 Single product request: {req.url}")
    return process_product_complete(req.url)


# ═════════════════════════════════════════
# BULK PRODUCTS ENDPOINT
# ═════════════════════════════════════════

@app.post("/generate-products", tags=["Product Processing"])
def generate_products(req: BulkProductRequest):
    """
    ✅ BULK PRODUCTS - Process Multiple URLs
    
    Process MULTIPLE AliExpress products:
    - Up to 20 URLs per request
    - Concurrent processing (3 at a time)
    - Full pipeline for each product
    - Individual results
    
    Returns:
    - Total processed
    - Success/Failed count
    - Results for each product
    """
    
    if not req.urls:
        raise HTTPException(status_code=400, detail="URLs list cannot be empty")
    
    if len(req.urls) > 20:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum 20 URLs allowed. You provided {len(req.urls)}"
        )
    
    logger.info(f"📨 Bulk request: {len(req.urls)} products")
    
    results = []
    successful = 0
    failed = 0
    
    # Process URLs concurrently
    with ThreadPoolExecutor(max_workers=3) as thread_executor:
        future_to_url = {
            thread_executor.submit(process_product_complete, url): url
            for url in req.urls
        }
        
        for future in as_completed(future_to_url):
            try:
                result = future.result(timeout=300)
                results.append(result.dict())
                
                if result.success:
                    successful += 1
                else:
                    failed += 1
            
            except Exception as e:
                url = future_to_url[future]
                logger.error(f"❌ Error processing {url}: {e}")
                results.append({
                    "success": False,
                    "url": url,
                    "title": "",
                    "category_name": "",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                failed += 1
    
    logger.info(f"\n{'='*70}")
    logger.info(f"✅ BULK PROCESSING COMPLETE: {successful} success, {failed} failed")
    logger.info(f"{'='*70}\n")
    
    return {
        "total": len(req.urls),
        "successful": successful,
        "failed": failed,
        "results": results,
        "timestamp": datetime.now().isoformat()
    }


# ═════════════════════════════════════════
# DATABASE VIEW ENDPOINTS
# ═════════════════════════════════════════

def get_db_connection():
    """Get database connection with row factory"""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/scraped-products", tags=["Database Views"])
def view_scraped_products(limit: int = 100):
    """
    View scraped products
    
    Shows all raw extracted data (25+ attributes)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM scraped_products ORDER BY scraped_at DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        logger.info(f"📊 Retrieved {len(rows)} scraped products")
        return [dict(row) for row in rows]
    
    except Exception as e:
        logger.error(f"❌ Error fetching scraped products: {e}")
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/mapped-products", tags=["Database Views"])
def view_mapped_products(limit: int = 100):
    """
    View mapped products
    
    Shows data mapped to 71 Octopia template columns
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM mapped_products ORDER BY mapped_at DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        logger.info(f"📊 Retrieved {len(rows)} mapped products")
        return [dict(row) for row in rows]
    
    except Exception as e:
        logger.error(f"❌ Error fetching mapped products: {e}")
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/template-outputs", tags=["Database Views"])
def view_template_outputs(limit: int = 100):
    """
    View generated template files
    
    Shows all Excel XLSM files generated
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM template_outputs ORDER BY created_at DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        logger.info(f"📊 Retrieved {len(rows)} template outputs")
        return [dict(row) for row in rows]
    
    except Exception as e:
        logger.error(f"❌ Error fetching template outputs: {e}")
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/processing-logs", tags=["Database Views"])
def view_processing_logs(limit: int = 500):
    """
    View processing logs
    
    Shows complete audit trail of all processing steps
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM processing_logs ORDER BY log_time DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        logger.info(f"📊 Retrieved {len(rows)} processing logs")
        return [dict(row) for row in rows]
    
    except Exception as e:
        logger.error(f"❌ Error fetching processing logs: {e}")
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/stats", tags=["Database Views"])
def get_database_stats():
    """
    Get database statistics
    
    Shows counts from all tables
    """
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
        
        return {
            "scraping": {
                "scraped_products": stats.get("scraped_products", 0)
            },
            "processing": {
                "mapped_products": stats.get("mapped_products", 0),
                "template_outputs": stats.get("template_outputs", 0),
                "category_assignments": stats.get("category_assignments", 0)
            },
            "audit": {
                "processing_logs": stats.get("processing_logs", 0)
            },
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"❌ Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail="Database error")


# ─────────────────────────────────────────
# ERROR HANDLER
# ─────────────────────────────────────────

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Custom error handler"""
    return {
        "success": False,
        "error": exc.detail,
        "status_code": exc.status_code,
        "timestamp": datetime.now().isoformat()
    }


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.environ.get("PORT", 8686))
    
    logger.info("\n" + "="*70)
    logger.info("🚀 Starting Octopia Template Pipeline (Complete Task 2)")
    logger.info("="*70)
    logger.info(f"Port: {port}")
    logger.info(f"Template: {TEMPLATE_PATH}")
    logger.info(f"Database: {DB_NAME}")
    logger.info("="*70 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
