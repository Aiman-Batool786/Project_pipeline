import sqlite3
import numpy as np
import pickle
import os
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
                print("[category] WARNING: Failed to load embedding")

        if len(embeddings) == 0:
            print("[category] ERROR: No embeddings loaded!")
            return [], [], np.array([])

        print(f"[category] ✅ Loaded {len(category_ids)} categories")
        return category_ids, category_names, np.array(embeddings)

    except Exception as e:
        print("[category] ERROR loading categories:", e)
        return [], [], np.array([])


# Load once at startup
category_ids, category_names, category_embeddings = load_categories()


def get_embedding(text):
    try:
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=text
        )
        return np.array(response.data[0].embedding)

    except Exception as e:
        print("[category] ERROR getting embedding:", e)
        return None


def assign_category(title, description):
    """
    Assign category and extract LEAF category from full path
    """
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
    if len(category_embeddings) == 0:
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
    sims = cosine_similarity(
        [product_embedding],
        category_embeddings
    )[0]

    idx = sims.argmax()
    best_score = float(sims[idx])
    best_category_id = category_ids[idx]
    best_category_name = category_names[idx]

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

    # ✅ EXTRACT LEAF CATEGORY FROM FULL PATH
    leaf_category = extract_leaf_category(best_category_name)

    print(f"[category] ✅ Assigned: {best_category_name}")
    print(f"[category]    Code: {best_category_id}")
    print(f"[category]    Leaf: {leaf_category}")

    return {
        "category_id": best_category_id,
        "category_name": best_category_name,
        "category_leaf": leaf_category,  # ✅ NEW!
        "confidence": best_score
    }
