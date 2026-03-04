from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, HttpUrl
from scraper import get_product_info
from utils import clean_text
from openai_client import improve_product_content
from category_utils import assign_category
from db import (
    create_all_tables,
    insert_original_content,
    insert_enhanced_content,
    insert_category_assignment,
    insert_product
)

import json
import uvicorn
import os
import sqlite3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
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
    title="AliExpress Product AI Enhancer",
    description="Scrape, enhance, and categorize AliExpress products using AI",
    version="2.0.0"
)

# Thread pool for concurrent URL processing
executor = ThreadPoolExecutor(max_workers=3)


# ─────────────────────────────────────────
# STARTUP/SHUTDOWN EVENTS
# ─────────────────────────────────────────
@app.on_event("startup")
def startup_event():
    """Initialize database on startup"""
    try:
        create_all_tables()
        logger.info("✅ Database initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize database: {e}")
        raise


@app.on_event("shutdown")
def shutdown_event():
    """Clean up resources on shutdown"""
    try:
        executor.shutdown(wait=True)
        logger.info("✅ ThreadPoolExecutor shut down gracefully")
    except Exception as e:
        logger.error(f"⚠️ Error during shutdown: {e}")


# ─────────────────────────────────────────
# REQUEST/RESPONSE MODELS
# ─────────────────────────────────────────
class ProductRequest(BaseModel):
    """Single product URL request"""
    url: str
    
    class Config:
        schema_extra = {
            "example": {"url": "https://www.aliexpress.com/item/1005007757337814.html"}
        }


class MultiProductRequest(BaseModel):
    """Multiple product URLs request"""
    urls: List[str]
    
    class Config:
        schema_extra = {
            "example": {
                "urls": [
                    "https://www.aliexpress.com/item/1005007757337814.html",
                    "https://www.aliexpress.com/item/1005007757337815.html"
                ]
            }
        }


class ProductResponse(BaseModel):
    """Response for single product"""
    status: str
    product_id: Optional[int] = None
    original: Optional[Dict] = None
    enhanced: Optional[Dict] = None
    error: Optional[str] = None
    timestamp: str


class MultiProductResponse(BaseModel):
    """Response for multiple products"""
    total: int
    success: int
    failed: int
    results: List[ProductResponse]
    timestamp: str


# ─────────────────────────────────────────
# ROOT ENDPOINT
# ─────────────────────────────────────────
@app.get("/", tags=["Health"])
def root():
    """Health check endpoint"""
    return {
        "status": "running",
        "message": "FastAPI Product AI Enhancer v2.0",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Detailed health check"""
    try:
        conn = sqlite3.connect("products.db")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM products")
        product_count = cursor.fetchone()[0]
        conn.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "products_count": product_count,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")


# ─────────────────────────────────────────
# PROCESS SINGLE URL (Core Logic)
# ─────────────────────────────────────────
def process_single_url(url: str) -> ProductResponse:
    """
    Process a single product URL through the complete pipeline
    
    Steps:
    1. Scrape product info
    2. Save original content
    3. Enhance with OpenAI
    4. Assign categories
    5. Save to database
    
    Returns:
        ProductResponse with results or error
    """
    try:
        logger.info(f"🔍 Processing URL: {url}")
        
        # Step 1: Scrape product information
        logger.info(f"📥 Starting scrape...")
        data = get_product_info(url)
        
        if not data:
            error_msg = "Scraping failed - likely blocked by AliExpress or invalid URL"
            logger.warning(f"⚠️ {error_msg}")
            return ProductResponse(
                status="failed",
                error=error_msg,
                timestamp=datetime.now().isoformat()
            )
        
        # Clean scraped content
        original_title = clean_text(data.get("title", ""))
        original_description = clean_text(data.get("description", ""))
        image_url = data.get("image_url", "")
        
        if not original_title:
            error_msg = "Could not extract product title"
            logger.warning(f"⚠️ {error_msg}")
            return ProductResponse(
                status="failed",
                error=error_msg,
                timestamp=datetime.now().isoformat()
            )
        
        # Step 2: Save original content and get product_id
        logger.info(f"💾 Saving original content...")
        product_id = insert_original_content(
            url, 
            original_title, 
            original_description, 
            image_url
        )
        logger.info(f"✅ Original content saved (product_id={product_id})")
        
        # Step 3: Enhance with OpenAI
        logger.info(f"🤖 Enhancing content with OpenAI...")
        improved = improve_product_content(original_title, original_description)
        
        if not improved:
            error_msg = "OpenAI enhancement failed"
            logger.error(f"❌ {error_msg}")
            return ProductResponse(
                status="failed",
                product_id=product_id,
                error=error_msg,
                timestamp=datetime.now().isoformat()
            )
        
        logger.info(f"✅ Content enhanced successfully")
        
        # Step 4: Save enhanced content
        logger.info(f"💾 Saving enhanced content...")
        insert_enhanced_content(
            product_id,
            improved.get("title", ""),
            improved.get("description", ""),
            json.dumps(improved.get("bullet_points", [])),
            image_url
        )
        logger.info(f"✅ Enhanced content saved")
        
        # Step 5: Assign categories (both original and enhanced)
        logger.info(f"🏷️ Assigning categories...")
        original_category = assign_category(original_title, original_description)
        enhanced_category = assign_category(
            improved.get("title", ""), 
            improved.get("description", "")
        )
        
        insert_category_assignment(
            product_id,
            original_category.get("category_id", 0),
            original_category.get("category_name", "Unknown"),
            enhanced_category.get("category_id", 0),
            enhanced_category.get("category_name", "Unknown"),
            enhanced_category.get("confidence", 0.0)
        )
        logger.info(f"✅ Categories assigned")
        
        # Step 6: Save to legacy products table (backward compatibility)
        logger.info(f"💾 Saving to products table...")
        insert_product((
            url,
            original_title,
            original_description,
            improved.get("title", ""),
            improved.get("description", ""),
            json.dumps(improved.get("bullet_points", [])),
            enhanced_category.get("category_id", 0),
            enhanced_category.get("category_name", "Unknown"),
            enhanced_category.get("confidence", 0.0),
            enhanced_category.get("category_name", "Unknown")
        ))
        logger.info(f"✅ Product saved to legacy table")
        
        # Return successful response
        return ProductResponse(
            status="success",
            product_id=product_id,
            original={
                "title": original_title,
                "description": original_description,
                "category": original_category.get("category_name", "Unknown")
            },
            enhanced={
                "title": improved.get("title", ""),
                "description": improved.get("description", ""),
                "bullet_points": improved.get("bullet_points", []),
                "category": enhanced_category.get("category_name", "Unknown")
            },
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        logger.error(f"❌ Exception processing URL: {type(e).__name__}: {e}", exc_info=True)
        return ProductResponse(
            status="failed",
            error=f"{type(e).__name__}: {str(e)}",
            timestamp=datetime.now().isoformat()
        )


# ─────────────────────────────────────────
# SINGLE URL ENDPOINT
# ─────────────────────────────────────────
@app.post("/Single-URL", response_model=ProductResponse, tags=["Single URL"])
def generate_product(req: ProductRequest):
    """
    Process a single AliExpress product URL
    
    - Scrapes product information
    - Enhances with AI
    - Assigns category
    - Saves to database
    
    **Note:** Processing may take 30-60 seconds due to anti-bot delays
    """
    if not req.url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")
    
    logger.info(f"📨 Single URL request: {req.url}")
    return process_single_url(req.url)


# ─────────────────────────────────────────
# MULTI URL ENDPOINT (with threading)
# ─────────────────────────────────────────
@app.post("/MULTI-URL", response_model=MultiProductResponse, tags=["Multiple URLs"])
def generate_products(req: MultiProductRequest):
    """
    Process multiple AliExpress product URLs concurrently
    
    - Accepts up to 20 URLs per request
    - Processes up to 3 URLs concurrently (configurable)
    - Returns individual results for each URL
    
    **Note:** Total processing time scales with URL count and network conditions
    """
    if not req.urls:
        raise HTTPException(status_code=400, detail="URLs list cannot be empty")
    
    if len(req.urls) > 20:
        raise HTTPException(
            status_code=400, 
            detail=f"Maximum 20 URLs allowed per request. You provided {len(req.urls)}"
        )
    
    logger.info(f"📨 Multi-URL request: {len(req.urls)} URLs")
    
    results = []
    success_count = 0
    failed_count = 0
    
    # Process URLs concurrently
    with ThreadPoolExecutor(max_workers=3) as thread_executor:
        future_to_url = {
            thread_executor.submit(process_single_url, url): url
            for url in req.urls
        }
        
        for future in as_completed(future_to_url):
            try:
                result = future.result(timeout=300)  # 5 minute timeout per URL
                results.append(result)
                
                if result.status == "success":
                    success_count += 1
                else:
                    failed_count += 1
            
            except Exception as e:
                url = future_to_url[future]
                logger.error(f"❌ Error processing {url}: {e}")
                results.append(ProductResponse(
                    status="failed",
                    error=f"Processing error: {str(e)}",
                    timestamp=datetime.now().isoformat()
                ))
                failed_count += 1
    
    logger.info(f"✅ Multi-URL processing complete: {success_count} success, {failed_count} failed")
    
    return MultiProductResponse(
        total=len(req.urls),
        success=success_count,
        failed=failed_count,
        results=results,
        timestamp=datetime.now().isoformat()
    )


# ─────────────────────────────────────────
# VIEW ENDPOINTS (Database)
# ─────────────────────────────────────────

def get_db_connection():
    """Get database connection with row factory"""
    conn = sqlite3.connect("products.db")
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/products", tags=["Database Views"])
def view_products(limit: int = 100):
    """
    Get all products from the products table
    
    - limit: Maximum number of products to return (default: 100)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM products LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        logger.info(f"📊 Retrieved {len(rows)} products")
        return [dict(row) for row in rows]
    
    except Exception as e:
        logger.error(f"❌ Error fetching products: {e}")
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/Original-Content-Table", tags=["Database Views"])
def view_original_content(limit: int = 100):
    """
    Get all original content from the database
    
    - limit: Maximum number of records to return (default: 100)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM original_content LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        logger.info(f"📊 Retrieved {len(rows)} original content records")
        return [dict(row) for row in rows]
    
    except Exception as e:
        logger.error(f"❌ Error fetching original content: {e}")
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/Enhanced-Content-Table", tags=["Database Views"])
def view_enhanced_content(limit: int = 100):
    """
    Get all enhanced content from the database
    
    - limit: Maximum number of records to return (default: 100)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM enhanced_content LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        logger.info(f"📊 Retrieved {len(rows)} enhanced content records")
        return [dict(row) for row in rows]
    
    except Exception as e:
        logger.error(f"❌ Error fetching enhanced content: {e}")
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/Categories-Table", tags=["Database Views"])
def view_category_assignments(limit: int = 100):
    """
    Get all category assignments from the database
    
    - limit: Maximum number of records to return (default: 100)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM category_assignments LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        logger.info(f"📊 Retrieved {len(rows)} category assignment records")
        return [dict(row) for row in rows]
    
    except Exception as e:
        logger.error(f"❌ Error fetching categories: {e}")
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/stats", tags=["Database Views"])
def get_database_stats():
    """
    Get database statistics
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as count FROM products")
        products_count = cursor.fetchone()["count"]
        
        cursor.execute("SELECT COUNT(*) as count FROM original_content")
        original_count = cursor.fetchone()["count"]
        
        cursor.execute("SELECT COUNT(*) as count FROM enhanced_content")
        enhanced_count = cursor.fetchone()["count"]
        
        cursor.execute("SELECT COUNT(*) as count FROM category_assignments")
        categories_count = cursor.fetchone()["count"]
        
        conn.close()
        
        return {
            "products": products_count,
            "original_content": original_count,
            "enhanced_content": enhanced_count,
            "category_assignments": categories_count,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"❌ Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail="Database error")


# ─────────────────────────────────────────
# ERROR HANDLERS
# ─────────────────────────────────────────
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Custom HTTP exception handler"""
    return {
        "status": "error",
        "detail": exc.detail,
        "status_code": exc.status_code,
        "timestamp": datetime.now().isoformat()
    }


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8686))
    
    logger.info(f"🚀 Starting FastAPI server on port {port}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
