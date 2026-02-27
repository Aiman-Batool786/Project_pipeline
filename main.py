from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import json
import sqlite3
import os

# Import your scraper and utils
from scraper import get_product_info
from utils import clean_text
from openai_client import improve_product_content
from category_utils import assign_category
from db import create_table, insert_product, create_categories_table

# ==============================
# Initialize app
# ==============================
app = FastAPI(title="AliExpress Product AI Enhancer")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS", "HEAD"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=600,
)

# ==============================
# Startup: Create DB tables
# ==============================
@app.on_event("startup")
def startup():
    create_table()
    create_categories_table()
    print("Database ready")

# ==============================
# Root endpoint
# ==============================
@app.get("/")
def root():
    return {"status": "running", "message": "FastAPI Product AI Enhancer is running"}

# ==============================
# Request model
# ==============================
class ProductRequest(BaseModel):
    url: str

# ==============================
# Main API
# ==============================
@app.post("/generate-product")
def generate_product(req: ProductRequest):
    try:
        print("Processing URL:", req.url)

        # Step 1: Scrape
        data = get_product_info(req.url)
        if not data:
            return {"success": False, "error": "Scraping failed"}

        # Step 2: Clean text
        original_title = clean_text(data.get("title", ""))
        original_description = clean_text(data.get("description", ""))

        # Step 3: Improve with OpenAI
        improved = improve_product_content(original_title, original_description)
        if not improved:
            return {"success": False, "error": "OpenAI improvement failed"}

        # Step 4: Assign category
        category = assign_category(improved["title"], improved["description"])

        # Step 5: Save in DB
        insert_product((
            req.url,
            original_title,
            original_description,
            improved["title"],
            improved["description"],
            json.dumps(improved["bullet_points"]),
            category["category_id"],
            category["category_name"],
            category["confidence"]
        ))

        # Step 6: Return response
        return {
            "success": True,
            "url": req.url,
            "original": {"title": original_title, "description": original_description},
            "enhanced": {
                "title": improved["title"],
                "description": improved["description"],
                "bullet_points": improved["bullet_points"]
            },
            "category": category
        }

    except Exception as e:
        print("ERROR:", e)
        return {"error": str(e)}

# ==============================
# View saved products
# ==============================
@app.get("/products")
def view_products():
    try:
        conn = sqlite3.connect("products.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM products")
        rows = cursor.fetchall()
        conn.close()
        return {"success": True, "count": len(rows), "products": [dict(row) for row in rows]}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==============================
# Run server
# ==============================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8686))
    os.environ["PYTHONUNBUFFERED"] = "1"
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port, reload=True)
