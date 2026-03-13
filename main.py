"""
FastAPI Server - HYBRID APPROACH
API shows both original and enhanced specs
Template stores ONLY enhanced specs

Two data objects:
1. enriched_data → for API response (merged specs)
2. enriched_data_for_template → for template (enhanced only)
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
    title="Octopia Template Pipeline - HYBRID Approach",
    version="2.1.0"
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
        "version": "2.1.0",
        "approach": "HYBRID - API shows both, template stores enhanced only",
        "features": [
            "Advanced scraping (25+ attributes, 20+ images)",
            "Content enhancement (OpenAI with specifications)",
            "Octopia categorization (5,806 categories)",
            "Template mapping (71 columns)",
            "Excel XLSM generation with enhanced specs only"
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
    Implements HYBRID approach:
    - API response shows: original, enhanced, merged
    - Template stores: ONLY enhanced specs
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
        
        # Log original specs
        spec_fields = ['brand', 'color', 'dimensions', 'weight', 'material', 
                      'certifications', 'country_of_origin', 'warranty', 'product_type']
        scraped_specs_count = len([k for k in spec_fields if scraped_data.get(k)])
        logger.info(f"   Specifications: {scraped_specs_count} fields")
        
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
        logger.info("🤖 Enhancing content with OpenAI...")
        logger.info("   Sending to OpenAI:")
        logger.info(f"   - Title: {title[:60]}...")
        logger.info(f"   - Description: {len(description)} chars")
        logger.info(f"   - Specifications: {scraped_specs_count} fields")
        
        try:
            # ✅ Pass specifications to OpenAI
            enhanced = improve_product_content(
                title=title,
                description=description,
                specifications=scraped_data,  # ✅ Pass scraped data with specs
                category=None
            )
            if not enhanced:
                enhanced = {
                    "title": title,
                    "description": description,
                    "bullet_points": scraped_data.get("bullet_points", []),
                    "html_description": "",
                    "specifications_enhanced": {}
                }
        except Exception as e:
            logger.warning(f"Enhancement skipped: {e}")
            enhanced = {
                "title": title,
                "description": description,
                "bullet_points": scraped_data.get("bullet_points", []),
                "html_description": "",
                "specifications_enhanced": {}
            }
        
        logger.info("✅ Content enhanced")
        
        # ═══════════════════════════════════════════════════════════════════════════════════
        # ✅ STEP 3B: CREATE DATA FOR API RESPONSE (merged specs)
        # ═══════════════════════════════════════════════════════════════════════════════════
        logger.info("\n🔄 Creating API data with merged specifications...")
        
        # Start with ORIGINAL scraped data
        enriched_data = scraped_data.copy()
        
        # Update TEXT content with ENHANCED versions
        enriched_data['title'] = enhanced.get('title', title)
        enriched_data['description'] = enhanced.get('description', description)
        enriched_data['bullet_points'] = enhanced.get('bullet_points', [])
        enriched_data['html_description'] = enhanced.get('html_description', '')
        
        logger.info("   ✅ Text content updated (title, description, bullets, HTML)")
        
        # ✅ FOR API SPECS: Use enhanced preferred, fallback to original (for display)
        specs_enhanced = enhanced.get('specifications_enhanced', {})
        
        specs_merged = {}
        for spec_field in spec_fields:
            original_value = scraped_data.get(spec_field, '')
            enhanced_value = specs_enhanced.get(spec_field, '')
            
            if enhanced_value and enhanced_value.strip() != "":
                enriched_data[spec_field] = enhanced_value
                specs_merged[spec_field] = enhanced_value
                logger.info(f"   ✅ {spec_field}: {enhanced_value[:50]} (ENHANCED)")
            elif original_value and original_value.strip() != "":
                enriched_data[spec_field] = original_value
                specs_merged[spec_field] = original_value
                logger.info(f"   ✅ {spec_field}: {original_value[:50]} (ORIGINAL)")
            else:
                enriched_data[spec_field] = ""
        
        logger.info(f"✅ API data ready with {len(enriched_data)} fields (merged specs)")
        
        # ═══════════════════════════════════════════════════════════════════════════════════
        # ✅ STEP 3C: CREATE DATA FOR TEMPLATE (ONLY enhanced specs, NO original fallback)
        # ═══════════════════════════════════════════════════════════════════════════════════
        logger.info("\n🔄 Creating template data with ONLY enhanced specifications...")
        
        # Start with ORIGINAL scraped data
        enriched_data_for_template = scraped_data.copy()
        
        # Update TEXT content with ENHANCED versions
        enriched_data_for_template['title'] = enhanced.get('title', title)
        enriched_data_for_template['description'] = enhanced.get('description', description)
        enriched_data_for_template['bullet_points'] = enhanced.get('bullet_points', [])
        enriched_data_for_template['html_description'] = enhanced.get('html_description', '')
        
        logger.info("   ✅ Text content updated (title, description, bullets, HTML)")
        
        # ✅ FOR TEMPLATE SPECS: Use ONLY enhanced (NOT fallback to original!)
        specs_template = {}
        for spec_field in spec_fields:
            enhanced_value = specs_enhanced.get(spec_field, '')
            
            if enhanced_value and enhanced_value.strip() != "":
                enriched_data_for_template[spec_field] = enhanced_value
                specs_template[spec_field] = enhanced_value
                logger.info(f"   ✅ {spec_field}: {enhanced_value[:50]} (ENHANCED)")
            else:
                # ✅ IMPORTANT: Set to EMPTY (NOT original!)
                enriched_data_for_template[spec_field] = ""
                logger.info(f"   ⭕ {spec_field}: EMPTY (no enhanced version)")
        
        logger.info(f"\n✅ Template data ready with {len(enriched_data_for_template)} fields")
        logger.info("   Using ONLY enhanced specs (no original fallback)")
        
        # ================= STEP 4: CATEGORIZE =================
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
        
        logger.info(f"✅ Category: {category['category_name']}")
        logger.info(f"   Code: {category['category_id']}")
        logger.info(f"   Leaf: {category.get('category_leaf', 'Unknown')}")
        
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
        
        # ═══════════════════════════════════════════════════════════════════════════════════
        # ✅ STEP 5: MAP - USE TEMPLATE DATA (ONLY ENHANCED SPECS)
        # ═══════════════════════════════════════════════════════════════════════════════════
        logger.info("\n🗺️  Mapping to template (using enhanced specs ONLY)...")
        mapped_data = {}
        is_valid = False
        
        try:
            # ✅ CRITICAL: Pass enriched_data_for_template (with ONLY enhanced specs)
            # NOT enriched_data (which has merged specs for API)
            mapped_data = map_scraped_data_to_template(enriched_data_for_template)
            is_valid, missing = validate_mapped_data(mapped_data)
            
            insert_mapped_product(product_id, category.get("category_id", "0"), mapped_data)
            log_processing(product_id, url, "mapping", "success" if is_valid else "warning")
            logger.info(f"✅ Data mapped to {len(mapped_data)} fields")
            logger.info("   Using template-only data (enhanced specs, no original fallback)")
        except Exception as e:
            logger.warning(f"Mapping error: {e}")
            log_processing(product_id, url, "mapping", "error", str(e))
        
        # ================= STEP 6: STORE ENHANCED CONTENT =================
        logger.info("\n💾 Storing enhanced content...")
        try:
            cursor = sqlite3.connect(DB_NAME).cursor()
            cursor.execute("""
                INSERT INTO enhanced_content
                (product_id, title, description, bullet_points, html_description,
                 brand, color, dimensions, weight, material, certifications,
                 country_of_origin, warranty, product_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                product_id,
                enriched_data_for_template.get('title', ''),
                enriched_data_for_template.get('description', ''),
                json.dumps(enriched_data_for_template.get('bullet_points', [])),
                enriched_data_for_template.get('html_description', ''),
                enriched_data_for_template.get('brand', ''),
                enriched_data_for_template.get('color', ''),
                enriched_data_for_template.get('dimensions', ''),
                enriched_data_for_template.get('weight', ''),
                enriched_data_for_template.get('material', ''),
                enriched_data_for_template.get('certifications', ''),
                enriched_data_for_template.get('country_of_origin', ''),
                enriched_data_for_template.get('warranty', ''),
                enriched_data_for_template.get('product_type', '')
            ))
            sqlite3.connect(DB_NAME).commit()
            logger.info("✅ Enhanced content stored (template-only version)")
        except Exception as e:
            logger.warning(f"Could not store enhanced content: {e}")
        
        # ================= STEP 7: GENERATE TEMPLATE =================
        logger.info("\n📋 Generating template...")
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
        
        # ================= RETURN COMPLETE INFO =================
        logger.info("✅ Processing complete\n")
        
        return {
            "success": True,
            "product_id": product_id,
            "url": url,
            
            # Original data (from scraper)
            "original": {
                "title": title,
                "description": description[:200] + "..." if len(description) > 200 else description,
                "brand": scraped_data.get("brand", ""),
                "color": scraped_data.get("color", ""),
                "dimensions": scraped_data.get("dimensions", ""),
                "weight": scraped_data.get("weight", ""),
                "material": scraped_data.get("material", ""),
                "images": sum(1 for i in range(1, 21) if scraped_data.get(f"image_{i}"))
            },
            
            # Enhanced data (from OpenAI)
            "enhanced": {
                "title": enhanced.get("title", ""),
                "description": enhanced.get("description", "")[:200] + "..." if enhanced.get("description") else "",
                "bullet_points": enhanced.get("bullet_points", [])[:3],
                "has_html_description": bool(enhanced.get("html_description", "")),
                "specifications_enhanced": enhanced.get("specifications_enhanced", {})
            },
            
            # Merged data (for API display: enhanced preferred, original fallback)
            "merged": {
                "brand": enriched_data.get("brand", ""),
                "color": enriched_data.get("color", ""),
                "dimensions": enriched_data.get("dimensions", ""),
                "weight": enriched_data.get("weight", ""),
                "material": enriched_data.get("material", ""),
                "certifications": enriched_data.get("certifications", ""),
                "country_of_origin": enriched_data.get("country_of_origin", ""),
                "warranty": enriched_data.get("warranty", ""),
                "product_type": enriched_data.get("product_type", "")
            },
            
            # Template specs (ONLY enhanced, no fallback)
            "template_specs": {
                "brand": enriched_data_for_template.get("brand", ""),
                "color": enriched_data_for_template.get("color", ""),
                "dimensions": enriched_data_for_template.get("dimensions", ""),
                "weight": enriched_data_for_template.get("weight", ""),
                "material": enriched_data_for_template.get("material", ""),
                "certifications": enriched_data_for_template.get("certifications", ""),
                "country_of_origin": enriched_data_for_template.get("country_of_origin", ""),
                "warranty": enriched_data_for_template.get("warranty", ""),
                "product_type": enriched_data_for_template.get("product_type", "")
            },
            
            # Category info
            "category": {
                "id": category.get("category_id", ""),
                "name": category.get("category_name", ""),
                "leaf": category.get("category_leaf", ""),
                "confidence": round(category.get("confidence", 0.0), 2)
            },
            
            # Template info
            "template": {
                "file": os.path.basename(template_file) if template_file else None,
                "columns_mapped": len(mapped_data),
                "fields_valid": is_valid,
                "category_row": {
                    "code": category.get("category_id", "0"),
                    "leaf": category.get("category_leaf", "Unknown")
                }
            },
            
            # Extracted data summary
            "extracted": {
                "specifications": sum(1 for k in spec_fields if enriched_data_for_template.get(k)),
                "images": sum(1 for i in range(1, 21) if scraped_data.get(f"image_{i}"))
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
    ✅ SINGLE PRODUCT - HYBRID Approach
    
    API Response shows:
    - Original: specs from scraper
    - Enhanced: specs from OpenAI
    - Merged: enhanced preferred, original fallback (for display)
    - Template Specs: ONLY enhanced (what goes to template)
    
    Template stores: ONLY enhanced specs
    
    **Time:** 30-60 seconds
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
    ✅ BULK PRODUCTS - HYBRID Approach
    
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
            "category_assignments",
            "enhanced_content"
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
    logger.info("🚀 Octopia Template Pipeline - HYBRID Approach")
    logger.info("   API shows: original + enhanced + merged")
    logger.info("   Template stores: ONLY enhanced specs")
    logger.info("="*70 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
