import sqlite3
import numpy as np
import pickle
import os
import re
from dotenv import load_dotenv
from openai import OpenAI
from sklearn.metrics.pairwise import cosine_similarity

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

DB_NAME = "products.db"
CONFIDENCE_THRESHOLD = 0.40


def extract_leaf_category(category_text):
    """
    Extract the LEAF (last part) from full category path
    
    Example:
    Input:  "ADULT - EROTIC/ARTICLES WITH SEXUAL CONNOTATIONS/DISHWASHER"
    Output: "DISHWASHER"
    """
    if not category_text:
        return "Uncategorized"
    
    # Split by "/" and get the last part
    parts = str(category_text).strip().split("/")
    if parts:
        leaf = parts[-1].strip()
        if leaf:
            return leaf
    
    return category_text


def load_categories():
    """Load categories from database with embeddings"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT category_id, category_name, embedding
            FROM categories
        """)

        rows = cursor.fetchall()
        conn.close()

        category_ids = []
        category_names = []
        embeddings = []

        for row in rows:
            category_ids.append(str(row[0]))
            category_names.append(row[1])

            try:
                embeddings.append(pickle.loads(row[2]))
            except:
                print("[category] WARNING: Failed to load embedding for", row[1])

        if len(embeddings) == 0:
            print("[category] ERROR: No embeddings loaded!")
            return [], [], np.array([])

        print(f"[category] ✅ Loaded {len(category_ids)} categories")
        return category_ids, category_names, np.array(embeddings)

    except Exception as e:
        print("[category] ERROR loading categories:", e)
        return [], [], np.array([])


# Lazy loading - categories loaded on first use
_category_ids = []
_category_names = []
_category_embeddings = np.array([])
_categories_loaded = False


def _ensure_categories_loaded():
    """Load categories from DB on first call; no-op on subsequent calls."""
    global _category_ids, _category_names, _category_embeddings, _categories_loaded
    if _categories_loaded:
        return
    print("[category] Lazy-loading categories from DB...")
    _category_ids, _category_names, _category_embeddings = load_categories()
    _categories_loaded = True
    if len(_category_ids) == 0:
        print("[category] ⚠️  No categories loaded — run create_categories_db.py first")


def get_embedding(text):
    try:
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=text[:8000]  # Truncate to avoid token limits
        )
        return np.array(response.data[0].embedding)

    except Exception as e:
        print("[category] ERROR getting embedding:", e)
        return None


def assign_category(title, description):
    """
    Assign category and extract LEAF category from full path
    """
    # Ensure DB categories are loaded
    _ensure_categories_loaded()

    title = (title or "").strip()
    description = (description or "").strip()
    product_text = (title + " " + description).strip()

    # Handle empty product text
    if not product_text:
        print("[category] WARNING: Empty title and description")
        return {
            "category_id": "0",
            "category_name": "Uncategorized",
            "category_leaf": "Uncategorized",
            "confidence": 0.0
        }

    # Handle blocked pages
    blocked_titles = [
        "aliexpress",
        "aliexpress.com",
        "just a moment",
        "attention required",
        "access denied",
        "captcha"
    ]

    if title.lower() in blocked_titles:
        print(f"[category] WARNING: Blocked page detected: '{title}'")
        return {
            "category_id": "0",
            "category_name": "Uncategorized",
            "category_leaf": "Uncategorized",
            "confidence": 0.0
        }

    # Handle no categories in database
    if len(_category_embeddings) == 0:
        print("[category] ❌ ERROR: No category embeddings loaded")
        return {
            "category_id": "0",
            "category_name": "Uncategorized",
            "category_leaf": "Uncategorized",
            "confidence": 0.0
        }

    # Generate embedding
    product_embedding = get_embedding(product_text)

    if product_embedding is None:
        print("[category] ERROR: Could not generate embedding")
        return {
            "category_id": "0",
            "category_name": "Uncategorized",
            "category_leaf": "Uncategorized",
            "confidence": 0.0
        }

    # Compute cosine similarity
    try:
        sims = cosine_similarity(
            [product_embedding],
            _category_embeddings
        )[0]

        idx = sims.argmax()
        best_score = float(sims[idx])
        best_category_id = _category_ids[idx]
        best_category_name = _category_names[idx]

        print(f"[category] Best match: {best_category_name} ({best_score:.3f})")

        # Check confidence threshold
        if best_score < CONFIDENCE_THRESHOLD:
            print(f"[category] Low confidence ({best_score:.3f} < {CONFIDENCE_THRESHOLD}) → Uncategorized")
            return {
                "category_id": "0",
                "category_name": "Uncategorized",
                "category_leaf": "Uncategorized",
                "confidence": best_score
            }

        # Extract leaf category from full path
        leaf_category = extract_leaf_category(best_category_name)

        print(f"[category] ✅ Assigned: {best_category_name}")
        print(f"[category]    Code: {best_category_id}")
        print(f"[category]    Leaf: {leaf_category}")

        return {
            "category_id": best_category_id,
            "category_name": best_category_name,
            "category_leaf": leaf_category,
            "confidence": best_score
        }
        
    except Exception as e:
        print(f"[category] ERROR computing similarity: {e}")
        return {
            "category_id": "0",
            "category_name": "Uncategorized",
            "category_leaf": "Uncategorized",
            "confidence": 0.0
        }


def filter_restricted_keywords(text, restricted_keywords):
    """
    Filter out restricted keywords from text
    
    Args:
        text: String to filter
        restricted_keywords: List of keywords to remove
    
    Returns:
        Filtered text
    """
    if not text or not restricted_keywords:
        return text
    
    filtered_text = text
    for keyword in restricted_keywords:
        # Case-insensitive replacement
        pattern = re.compile(re.escape(keyword), re.IGNORECASE)
        filtered_text = pattern.sub('', filtered_text)
    
    # Clean up extra spaces
    filtered_text = re.sub(r'\s+', ' ', filtered_text).strip()
    
    return filtered_text
