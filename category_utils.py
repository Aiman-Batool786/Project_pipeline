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

# similarity threshold
CONFIDENCE_THRESHOLD = 0.60


# ------------------------------------------------
# Load categories from database
# ------------------------------------------------
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
            category_ids.append(row[0])
            category_names.append(row[1])

            try:
                embeddings.append(pickle.loads(row[2]))
            except:
                print("[category] WARNING: Failed to load embedding")

        if len(embeddings) == 0:
            return [], [], np.array([])

        return category_ids, category_names, np.array(embeddings)

    except Exception as e:

        print("[category] ERROR loading categories:", e)

        return [], [], np.array([])


# load once when file starts
category_ids, category_names, category_embeddings = load_categories()


# ------------------------------------------------
# Get embedding from OpenAI
# ------------------------------------------------
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


# ------------------------------------------------
# Assign category using semantic similarity
# ------------------------------------------------
def assign_category(title, description):

    title = (title or "").strip()
    description = (description or "").strip()

    product_text = (title + " " + description).strip()

    # ------------------------------------------------
    # FIX 1: Empty product text
    # ------------------------------------------------
    if not product_text:

        print("[category] WARNING: Empty title and description")

        return {
            "category_id": 0,
            "category_name": "Uncategorized",
            "confidence": 0.0
        }

    # ------------------------------------------------
    # FIX 2: Blocked / bot detection pages
    # ------------------------------------------------
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
            "category_id": 0,
            "category_name": "Uncategorized",
            "confidence": 0.0
        }

    # ------------------------------------------------
    # FIX 3: No categories in database
    # ------------------------------------------------
    if len(category_embeddings) == 0:

        print("[category] WARNING: No category embeddings loaded")

        return {
            "category_id": 0,
            "category_name": "Uncategorized",
            "confidence": 0.0
        }

    # ------------------------------------------------
    # Generate embedding
    # ------------------------------------------------
    product_embedding = get_embedding(product_text)

    if product_embedding is None:

        return {
            "category_id": 0,
            "category_name": "Uncategorized",
            "confidence": 0.0
        }

    # ------------------------------------------------
    # Compute cosine similarity
    # ------------------------------------------------
    sims = cosine_similarity(
        [product_embedding],
        category_embeddings
    )[0]

    idx = sims.argmax()

    best_score = float(sims[idx])

    best_category_id = category_ids[idx]
    best_category_name = category_names[idx]

    print(f"[category] Best match: {best_category_name} ({best_score:.3f})")

    # ------------------------------------------------
    # FIX 4: Confidence threshold
    # ------------------------------------------------
    if best_score < CONFIDENCE_THRESHOLD:

        print("[category] Low confidence → Uncategorized")

        return {
            "category_id": 0,
            "category_name": "Uncategorized",
            "confidence": best_score
        }

    return {
        "category_id": best_category_id,
        "category_name": best_category_name,
        "confidence": best_score
    }
