from fastapi import FastAPI
from pydantic import BaseModel
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

app = FastAPI(title="AliExpress Product AI Enhancer")

# ─────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────
@app.on_event("startup")
def startup():
    create_all_tables()
    print("Database ready")


# ─────────────────────────────────────────
# ROOT
# ─────────────────────────────────────────
@app.get("/")
def root():
    return {"message": "FastAPI Product AI Enhancer is running!"}


# ─────────────────────────────────────────
# REQUEST MODELS
# ─────────────────────────────────────────
class ProductRequest(BaseModel):
    url: str


class MultiProductRequest(BaseModel):
    urls: List[str]  # accepts multiple URLs


# ─────────────────────────────────────────
# PROCESS ONE URL (used by both endpoints)
# ─────────────────────────────────────────
def process_single_url(url: str) -> dict:
    try:
        # Step 1: Scrape
        data = get_product_info(url)
        if not data:
            return {"url": url, "status": "failed", "reason": "Scraping failed or blocked"}

        original_title       = clean_text(data["title"])
        original_description = clean_text(data["description"])
        image_url            = data.get("image_url", "")

        # Step 2: Save original content → get product_id
        product_id = insert_original_content(
            url, original_title, original_description, image_url
        )

        # Step 3: OpenAI enhancement
        improved = improve_product_content(original_title, original_description)
        if not improved:
            return {"url": url, "status": "failed", "reason": "OpenAI enhancement failed"}

        # Step 4: Save enhanced content
        insert_enhanced_content(
            product_id,
            improved["title"],
            improved["description"],
            json.dumps(improved["bullet_points"]),
            image_url
        )

        # Step 5: Assign categories (original + enhanced)
        original_category = assign_category(original_title, original_description)
        enhanced_category = assign_category(improved["title"], improved["description"])

        insert_category_assignment(
            product_id,
            original_category["category_id"],
            original_category["category_name"],
            enhanced_category["category_id"],
            enhanced_category["category_name"],
            enhanced_category["confidence"]
        )

        # Step 6: Also save to old products table (backward compatibility)
        insert_product((
            url,
            original_title,
            original_description,
            improved["title"],
            improved["description"],
            json.dumps(improved["bullet_points"]),
            enhanced_category["category_id"],
            enhanced_category["category_name"],
            enhanced_category["confidence"],
            enhanced_category["category_name"]   # enhanced_category column
        ))

        return {
            "url": url,
            "status": "success",
            "product_id": product_id,
            "original": {
                "title": original_title,
                "description": original_description,
                "category": original_category["category_name"]
            },
            "enhanced": {
                "title": improved["title"],
                "description": improved["description"],
                "bullet_points": improved["bullet_points"],
                "category": enhanced_category["category_name"]
            }
        }

    except Exception as e:
        return {"url": url, "status": "failed", "reason": str(e)}


# ─────────────────────────────────────────
# SINGLE URL ENDPOINT (original, kept working)
# ─────────────────────────────────────────
@app.post("/Single-URL")
def generate_product(req: ProductRequest):
    return process_single_url(req.url)


# ─────────────────────────────────────────
# MULTI URL ENDPOINT (new — uses threads)
# ─────────────────────────────────────────
@app.post("/MULTI-URL")
def generate_products(req: MultiProductRequest):
    if not req.urls:
        return {"error": "No URLs provided"}

    if len(req.urls) > 20:
        return {"error": "Max 20 URLs allowed per request"}

    results = []
    success_count = 0
    failed_count = 0

    # Use ThreadPoolExecutor to process multiple URLs concurrently
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_url = {
            executor.submit(process_single_url, url): url
            for url in req.urls
        }
        for future in as_completed(future_to_url):
            result = future.result()
            results.append(result)
            if result["status"] == "success":
                success_count += 1
            else:
                failed_count += 1

    return {
        "total": len(req.urls),
        "success": success_count,
        "failed": failed_count,
        "results": results
    }


# ─────────────────────────────────────────
# VIEW ENDPOINTS
# ─────────────────────────────────────────
@app.get("/products")
def view_products():
    conn = sqlite3.connect("products.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM products")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/Original-Content-Table")
def view_original_content():
    conn = sqlite3.connect("products.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM original_content")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/Enhanced-Content-Table")
def view_enhanced_content():
    conn = sqlite3.connect("products.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM enhanced_content")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/Categories-Table")
def view_category_assignments():
    conn = sqlite3.connect("products.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM category_assignments")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8686))
    uvicorn.run(app, host="0.0.0.0", port=port)
