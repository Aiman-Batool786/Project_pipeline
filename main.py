from fastapi import FastAPI
from pydantic import BaseModel
from scraper import get_product_info
from utils import clean_text
from openai_client import improve_product_content
from category_utils import assign_category
from db import create_table, insert_product, create_categories_table

from openai import OpenAI
from dotenv import load_dotenv
import json
import uvicorn
import os

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

app = FastAPI(title="AliExpress Product AI Enhancer")


# Run at startup
@app.on_event("startup")
def startup():
    create_table()
    create_categories_table()
    print("Database ready")


# Root path
@app.get("/")
def root():
    return {"message": "FastAPI Product AI Enhancer is running!"}


# Request model
class ProductRequest(BaseModel):
    url: str


# CHANGE 3: Enhanced category via OpenAI
def get_enhanced_category(title: str, description: str, raw_category: str) -> str:
    try:
        prompt = f"""Given this product:
Title: {title}
Description: {description[:300]}
Current Category: {raw_category}

Return a short, clean, human-readable category name (max 5 words).
Return ONLY the category name, nothing else."""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=20
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print("Enhanced category error:", e)
        return raw_category


# MAIN API
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

        category = assign_category(
            improved["title"],
            improved["description"]
        )

        # Get enhanced category
        enhanced_category = get_enhanced_category(
            improved["title"],
            improved["description"],
            category["category_name"]
        )
        print("Enhanced category:", enhanced_category)

        insert_product(
            (
                req.url,
                original_title,
                original_description,
                improved["title"],
                improved["description"],
                json.dumps(improved["bullet_points"]),
                category["category_id"],
                category["category_name"],
                category["confidence"],
                enhanced_category
            )
        )

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
                "category_id": category["category_id"],
                "category_name": category["category_name"],
                "confidence": category["confidence"],
                "enhanced_category": enhanced_category
            }
        }

    except Exception as e:
        print("ERROR:", e)
        return {"error": str(e)}


# View saved products
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
    port = int(os.environ.get("PORT", 8686))
    uvicorn.run(app, host="0.0.0.0", port=port)
