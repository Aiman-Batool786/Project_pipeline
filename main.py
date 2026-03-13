"""
FastAPI Server - COMPLETE INFO IN RESPONSE
Returns all product details immediately (processing happens, returns full data)

UPDATED: Uses category_leaf for template Row 1
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import sqlite3
import logging
from typing import List, Optional, Dict, Any
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
    title="Octopia Template Pipeline - Complete Response",
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
# ROOT & HEALTH ENDPOINTS
# ─────────────────────────────────────────

@app.get("/", tags=["Info"])
def root():
    """API info"""
    return {
        "status": "running",
        "service": "Octopia Template Pipeline",
        "version": "2.0.0",
        "features": [
            "Advanced scraping (25+ attributes, 6+ images)",
            "Content enhancement (OpenAI with HTML)",
            "Octopia categorization (5,806 categories with leaf extraction)",
            "Template mapping (71 columns)",
            "Excel XLSM generation with category Row 1"
        ]
    }


@app.get("/health", tags=["Info"])
def health_check():
    """Health check"""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.close()
        return {"status": "healthy"}
    except:
        return {"status": "error"}


# ─────────────────────────────────────────
# MAIN PROCESSING FUNCTION
# ─────────────────────────────────────────

def process_product_complete(url: str) -> Dict[str, Any]:
    """
    Process product and return COMPLETE INFO
    """
    
    product_id = None
    
    try:
        logger.info(f"\n🚀 Processing: {url}")
        
        # ================= IMPORTS =================
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
        scraped_data = get_product_info(url)
        
        if not scraped_data:
            return {
                "success": False,
                "url": url,
                "error": "Scraping failed",
                "timestamp": datetime.now().isoformat()
            }
        
        title = scraped_data.get("title", "")
        description = scraped_data.get("description", "")
        
        if not title:
            return {
                "success": False,
                "url": url,
                "error": "No title extracted",
                "timestamp": datetime.now().isoformat()
            }
        
        logger.info(f"✅ Extracted {len(scraped_data)} attributes")
        
        # ================= STEP 2: STORE SCRAPED DATA =================
        logger.info("💾 Storing scraped data...")
        product_id = insert_scraped_product(url, scraped_data)
        
        if not product_id:
            return {
                "success": False,
                "url": url,
                "error": "Failed to store data",
                "timestamp": datetime.now().isoformat()
            }
        
        log_processing(product_id, url, "scraping", "success")
        
        # ================= STEP 3: ENHANCE CONTENT =================
        logger.info("🤖 Enhancing content...")
        try:
            enhanced = improve_product_content(title, description)
            if not enhanced:
                enhanced = {
                    "title": title,
                    "description": description,
                    "bullet_points": scraped_data.get("bullet_points", []),
                    "html_description": ""
                }
        except Exception as e:
            logger.warning(f"Enhancement skipped: {e}")
            enhanced = {
                "title": title,
                "description": description,
                "bullet_points": scraped_data.get("bullet_points", []),
                "html_description": ""
            }
        
        logger.info("✅ Content enhanced")
        
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
                "category_name": "Unknown",
                "category_leaf": "Unknown",  # ✅ ADD default
                "confidence": 0.0
            }
        
        logger.info(f"✅ Category: {category['category_name']}")
        logger.info(f"   Code: {category['category_id']}")
        logger.info(f"   Leaf: {category.get('category_leaf', 'Unknown')}")  # ✅ LOG IT
        
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
        
        # ================= STEP 5: MAP =================
        logger.info("🗺️ Mapping to template...")
        mapped_data = {}
        is_valid = False
        
        try:
            enriched_data = scraped_data.copy()
            enriched_data['title'] = enhanced.get('title', title)
            enriched_data['description'] = enhanced.get('description', description)
            enriched_data['bullet_points'] = enhanced.get('bullet_points', [])
            enriched_data['html_description'] = enhanced.get('html_description', '')  # ✅ ADD THIS
            
            mapped_data = map_scraped_data_to_template(enriched_data)
            is_valid, missing = validate_mapped_data(mapped_data)
            
            insert_mapped_product(product_id, category.get("category_id", "0"), mapped_data)
            log_processing(product_id, url, "mapping", "success" if is_valid else "warning")
            logger.info("✅ Data mapped")
        except Exception as e:
            logger.warning(f"Mapping error: {e}")
            log_processing(product_id, url, "mapping", "error", str(e))
        
        # ================= STEP 6: GENERATE TEMPLATE =================
        logger.info("📋 Generating template...")
        template_file = None
        
        if os.path.exists(TEMPLATE_PATH):
            try:
                # ✅ PASS category_id and category_leaf to template_filler
                template_file = fill_template_for_product(
                    TEMPLATE_PATH,
                    mapped_data,
                    product_id,
                    FILLED_TEMPLATES_DIR,
                    category_id=category.get("category_id", "0"),
                    category_name=category.get("category_leaf", "Unknown")  # ✅ USE category_leaf!
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
        
        # ================= RETURN COMPLETE INFO =================
        logger.info("✅ Processing complete\n")
        
        return {
            "success": True,
            "product_id": product_id,
            "url": url,
            "original": {
                "title": title,
                "description": description[:200] + "..." if len(description) > 200 else description,
                "brand": scraped_data.get("brand", ""),
                "images": sum(1 for i in range(1, 20) if scraped_data.get(f"image_{i}"))  # ✅ Check up to 20 images
            },
            "enhanced": {
                "title": enhanced.get("title", ""),
                "description": enhanced.get("description", "")[:200] + "..." if enhanced.get("description") else "",
                "bullet_points": enhanced.get("bullet_points", [])[:3],  # First 3 bullet points
                "has_html_description": bool(enhanced.get("html_description", ""))  # ✅ Show if HTML was generated
            },
            "category": {
                "id": category.get("category_id", ""),
                "name": category.get("category_name", ""),
                "leaf": category.get("category_leaf", ""),  # ✅ INCLUDE LEAF
                "confidence": round(category.get("confidence", 0.0), 2)
            },
            "template": {
                "file": os.path.basename(template_file) if template_file else None,
                "columns_mapped": len(mapped_data),
                "fields_valid": is_valid,
                "category_row": {  # ✅ SHOW WHAT WAS WRITTEN TO ROW 1
                    "code": category.get("category_id", "0"),
                    "leaf": category.get("category_leaf", "Unknown")
                }
            },
            "extracted": {
                "attributes": len(scraped_data),
                "images": sum(1 for i in range(1, 20) if scraped_data.get(f"image_{i}"))  # ✅ Count all images
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
# SINGLE PRODUCT ENDPOINT
# ─────────────────────────────────────────

@app.post("/generate-product", tags=["Product Processing"])
def generate_product(req: ProductURLRequest):
    """
    ✅ SINGLE PRODUCT - Complete Response
    
    Returns ALL product information:
    - Original scraped data (25+ attributes, 6+ images)
    - Enhanced content (LLM-generated title, description, HTML)
    - Category assignment (code + leaf category)
    - Template file path with Row 1 category info
    - All extracted attributes
    
    **Time:** 30-60 seconds (processing + response)
    """
    
    if not req.url:
        raise HTTPException(status_code=400, detail="URL cannot be empty")
    
    logger.info(f"📨 Single product request: {req.url}")
    return process_product_complete(req.url)


# ─────────────────────────────────────────
# BULK PRODUCTS ENDPOINT
# ─────────────────────────────────────────

@app.post("/generate-products", tags=["Product Processing"])
def generate_products(req: BulkProductRequest):
    """
    ✅ BULK PRODUCTS - Complete Response for Each
    
    - Up to 20 URLs per request
    - Returns complete info for each product
    - Sequential processing
    """
    
    if not req.urls:
        raise HTTPException(status_code=400, detail="URLs list cannot be empty")
    
    if len(req.urls) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 URLs")
    
    logger.info(f"📨 Bulk request: {len(req.urls)} products")
    
    results = []
    successful = 0
    failed = 0
    
    for url in req.urls:
        result = process_product_complete(url)
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


@app.get("/enhanced-products", tags=["Database"])
def view_enhanced_products(limit: int = 100):
    """View enhanced products (OpenAI enhanced content)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM enhanced_content ORDER BY id DESC LIMIT {min(limit, 1000)}")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows] if rows else {"message": "No records"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/complete-products", tags=["Database"])
def view_complete_products(limit: int = 100):
    """
    View COMPLETE product information
    Combines: scraped + enhanced + category + mapped data
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Join all tables to get complete product info
        query = """
        SELECT 
            sp.product_id,
            sp.url,
            sp.title as scraped_title,
            sp.description as scraped_description,
            sp.brand,
            sp.color,
            sp.dimensions,
            sp.weight,
            sp.material,
            sp.certifications,
            sp.country_of_origin,
            sp.price,
            sp.shipping,
            sp.warranty,
            sp.image_1, sp.image_2, sp.image_3, sp.image_4, sp.image_5, sp.image_6,
            sp.bullet_points as scraped_bullets,
            sp.scraped_at,
            
            ec.title as enhanced_title,
            ec.description as enhanced_description,
            ec.bullet_points as enhanced_bullets,
            
            ca.original_category_id,
            ca.original_category_name,
            ca.enhanced_category_id,
            ca.enhanced_category_name,
            ca.confidence,
            
            mp.titre as template_title,
            mp.description as template_description,
            mp.marque,
            mp.couleur_principale,
            mp.dimensions as template_dimensions,
            mp.poids,
            mp.matiere,
            mp.certifications as template_certifications,
            mp.pays_origine,
            mp.garantie,
            mp.mapped_at,
            
            to_.file_name as template_file,
            to_.created_at as template_created
            
        FROM scraped_products sp
        LEFT JOIN enhanced_content ec ON sp.product_id = ec.product_id
        LEFT JOIN category_assignments ca ON sp.product_id = ca.product_id
        LEFT JOIN mapped_products mp ON sp.product_id = mp.product_id
        LEFT JOIN template_outputs to_ ON sp.product_id = to_.product_id
        
        ORDER BY sp.scraped_at DESC
        LIMIT ?
        """
        
        cursor.execute(query, (min(limit, 1000),))
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
    logger.info("🚀 Octopia Template Pipeline - Complete Response")
    logger.info("="*70 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
