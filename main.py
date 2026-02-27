from fastapi import FastAPI
from pydantic import BaseModel
from scraper import get_product_info
from utils import clean_text
from openai_client import improve_product_content
from category_utils import assign_category
from db import create_table, insert_product, create_categories_table

import json
import uvicorn
import os

app = FastAPI(title="AliExpress Product AI Enhancer")


#  Run at startup
@app.on_event("startup")
def startup():
    create_table()
    create_categories_table()
    print("Database ready")


#  Root path
@app.get("/")
def root():
    return {"message": "FastAPI Product AI Enhancer is running!"}


#  Request model
class ProductRequest(BaseModel):
    url: str


#  MAIN API
@app.post("/generate-product")
def generate_product(req: ProductRequest):
    try:
        data = get_product_info(req.url)
        if not data:
            return {"error": "Scraping failed"}

        original_title = clean_text(data["title"])
        original_description = clean_text(data["description"])

        improved = improve_product_content(
            original_title,
            original_description
        )
        if not improved:
            return {"error": "OpenAI failed"}

        # Category from embeddings (existing logic)
        category_from_embeddings = assign_category(
            improved["title"],
            improved["description"]
        )

        # Enhanced category (from OpenAI or default)
        enhanced_category = improved.get("enhanced_category", "Uncategorized")

        # Insert into DB including enhanced category
        insert_product(
            (
                req.url,
                original_title,
                original_description,
                improved["title"],
                improved["description"],
                json.dumps(improved["bullet_points"]),
                category_from_embeddings["category_id"],
                category_from_embeddings["category_name"],
                category_from_embeddings["confidence"],
                enhanced_category    # <-- Added
            )
        )

        # Return both categories in response
        return {
            "saved": True,
            "url": req.url,
            "original": {
                "title": original_title,
                "description": original_description
            },
            "enhanced": {
                "title": improved["title"],
                "description": improved["description"],
                "bullet_points": improved["bullet_points"]
            },
            "category": {
                "category_id": category_from_embeddings["category_id"],
                "category_name": category_from_embeddings["category_name"],
                "confidence": category_from_embeddings["confidence"],
                "enhanced_category": enhanced_category
            }
        }

    except Exception as e:
        return {"error": str(e)}
#  View saved products
@app.get("/products")
def view_products():
    import sqlite3
    conn = sqlite3.connect("products.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM products")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


if __name__ == "__main__":
    # Get port from environment variable, default to 8686
    port = int(os.environ.get("PORT", 8686))
    uvicorn.run(app, host="0.0.0.0", port=port)
