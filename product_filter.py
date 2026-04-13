"""
product_filter.py
═════════════════
Embedding-based product filter using products.db.

Two independent filters applied in sequence:

  1. Keyword filter  — embed the product title with OpenAI text-embedding-3-small,
                       then cosine-similarity against every embedding stored in
                       restricted_keywords table.  If any keyword scores above
                       KEYWORD_THRESHOLD → product is rejected.

  2. Category filter — embed the assigned category string, then cosine-similarity
                       against every embedding in restricted_categories table.
                       If any category scores above CATEGORY_THRESHOLD → rejected.

Both embedding caches are loaded once from DB at first use and held in memory.
Call reload_filter_data() to refresh after the DB is updated.

Dependencies (already in requirements.txt):
    openai, numpy, python-dotenv
"""

import logging
import os
import pickle
import sqlite3
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import numpy as np
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DB_PATH             = os.getenv("DB_PATH", "products.db")
EMBED_MODEL         = "text-embedding-3-small"
KEYWORD_THRESHOLD   = float(os.getenv("KEYWORD_THRESHOLD",  "0.70"))
CATEGORY_THRESHOLD  = float(os.getenv("CATEGORY_THRESHOLD", "0.75"))

_openai_client: Optional[OpenAI] = None


def _get_client() -> OpenAI:
    global _openai_client
    if _openai_client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError("OPENAI_API_KEY not set")
        _openai_client = OpenAI(api_key=api_key)
    return _openai_client


# ─────────────────────────────────────────────────────────────────────────────
# EMBEDDING HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _embed(text: str) -> np.ndarray:
    """Embed a single string and return a unit-normalised float32 vector."""
    resp = _get_client().embeddings.create(model=EMBED_MODEL, input=[text])
    vec  = np.array(resp.data[0].embedding, dtype=np.float32)
    norm = np.linalg.norm(vec)
    return vec / norm if norm > 0 else vec


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity of two unit-normalised vectors (= dot product)."""
    return float(np.dot(a, b))


# ─────────────────────────────────────────────────────────────────────────────
# DB CACHE  — loaded once, held in module-level dict, cleared by reload_filter_data()
# ─────────────────────────────────────────────────────────────────────────────

# { keyword_text: np.ndarray }
_keyword_embeddings:  Optional[Dict[str, np.ndarray]] = None
# { category_text: np.ndarray }
_category_embeddings: Optional[Dict[str, np.ndarray]] = None


def _load_keyword_embeddings() -> Dict[str, np.ndarray]:
    """Load all keyword embeddings from restricted_keywords table."""
    global _keyword_embeddings
    if _keyword_embeddings is not None:
        return _keyword_embeddings

    result: Dict[str, np.ndarray] = {}
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT keyword, embedding FROM restricted_keywords WHERE embedding IS NOT NULL"
        ).fetchall()
        conn.close()
    except Exception as exc:
        logger.error(f"[filter] Failed to load keyword embeddings: {exc}")
        _keyword_embeddings = {}
        return _keyword_embeddings

    for keyword, blob in rows:
        try:
            vec  = pickle.loads(blob)
            arr  = np.array(vec, dtype=np.float32)
            norm = np.linalg.norm(arr)
            result[keyword.lower()] = arr / norm if norm > 0 else arr
        except Exception as exc:
            logger.warning(f"[filter] Bad embedding for keyword '{keyword}': {exc}")

    logger.info(f"[filter] Loaded {len(result)} keyword embeddings from DB")
    _keyword_embeddings = result
    return _keyword_embeddings


def _load_category_embeddings() -> Dict[str, np.ndarray]:
    """Load all category embeddings from restricted_categories table."""
    global _category_embeddings
    if _category_embeddings is not None:
        return _category_embeddings

    result: Dict[str, np.ndarray] = {}
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT category, embedding FROM restricted_categories WHERE embedding IS NOT NULL"
        ).fetchall()
        conn.close()
    except Exception as exc:
        logger.error(f"[filter] Failed to load category embeddings: {exc}")
        _category_embeddings = {}
        return _category_embeddings

    for category, blob in rows:
        try:
            vec  = pickle.loads(blob)
            arr  = np.array(vec, dtype=np.float32)
            norm = np.linalg.norm(arr)
            result[category] = arr / norm if norm > 0 else arr
        except Exception as exc:
            logger.warning(f"[filter] Bad embedding for category '{category}': {exc}")

    logger.info(f"[filter] Loaded {len(result)} category embeddings from DB")
    _category_embeddings = result
    return _category_embeddings


def reload_filter_data() -> None:
    """
    Clear in-memory caches so embeddings are re-read from DB on the next
    filter call.  Call this after running keyword_embeddings.py or
    restricted_category_embeddings.py to pick up new entries.
    """
    global _keyword_embeddings, _category_embeddings
    _keyword_embeddings  = None
    _category_embeddings = None
    logger.info("[filter] Embedding caches cleared — will reload from DB on next call")


# ─────────────────────────────────────────────────────────────────────────────
# FILTER LOGIC
# ─────────────────────────────────────────────────────────────────────────────

def is_title_restricted(title: str) -> Tuple[bool, Optional[str]]:
    """
    Embed the product title and compare against all keyword embeddings.

    Returns (True, matched_keyword) if any keyword exceeds KEYWORD_THRESHOLD,
    otherwise (False, None).

    Falls back to empty-safe: if DB has no embeddings, product is allowed.
    """
    if not title or not title.strip():
        return False, None

    kw_embeddings = _load_keyword_embeddings()
    if not kw_embeddings:
        logger.warning("[filter] No keyword embeddings in DB — keyword filter skipped")
        return False, None

    try:
        title_vec = _embed(title)
    except Exception as exc:
        logger.error(f"[filter] Failed to embed title: {exc}")
        return False, None

    best_score   = 0.0
    best_keyword = None

    for keyword, kw_vec in kw_embeddings.items():
        score = _cosine(title_vec, kw_vec)
        if score > best_score:
            best_score   = score
            best_keyword = keyword

    if best_score >= KEYWORD_THRESHOLD:
        logger.info(
            f"[filter] Title RESTRICTED — keyword='{best_keyword}' score={best_score:.3f} "
            f"threshold={KEYWORD_THRESHOLD}"
        )
        return True, best_keyword

    return False, None


def is_category_restricted(category: Optional[Dict]) -> Tuple[bool, Optional[str]]:
    """
    Embed the category name and compare against all restricted category embeddings.

    category dict shape (from process_product_complete):
        { "id": "10030H", "name": "CLOTHING - LINGERIE/...", "leaf": "EVENTAIL", ... }

    Returns (True, reason) if the category similarity exceeds CATEGORY_THRESHOLD,
    otherwise (False, None).
    """
    if not category:
        return False, None

    cat_name = str(category.get("name", "")).strip()
    cat_leaf = str(category.get("leaf", "")).strip()

    # Use full name when available, fall back to leaf
    text_to_embed = cat_name or cat_leaf
    if not text_to_embed:
        return False, None

    cat_embeddings = _load_category_embeddings()
    if not cat_embeddings:
        logger.warning("[filter] No category embeddings in DB — category filter skipped")
        return False, None

    try:
        cat_vec = _embed(text_to_embed)
    except Exception as exc:
        logger.error(f"[filter] Failed to embed category: {exc}")
        return False, None

    best_score    = 0.0
    best_category = None

    for restricted_cat, rc_vec in cat_embeddings.items():
        score = _cosine(cat_vec, rc_vec)
        if score > best_score:
            best_score    = score
            best_category = restricted_cat

    if best_score >= CATEGORY_THRESHOLD:
        logger.info(
            f"[filter] Category RESTRICTED — matched='{best_category}' "
            f"score={best_score:.3f} threshold={CATEGORY_THRESHOLD}"
        )
        return True, f"category matches restricted entry (score={best_score:.2f})"

    return False, None


def filter_product(
    title: str,
    category: Optional[Dict],
    *,
    apply_keyword_filter:  bool = True,
    apply_category_filter: bool = True,
) -> Tuple[bool, Optional[str]]:
    """
    Combined filter gate for a single product.

    Returns:
        (allowed, rejection_reason)

    If allowed is True  → product passes; rejection_reason is None.
    If allowed is False → rejection_reason describes which filter triggered.

    When a product is rejected by the category filter the caller must NOT
    include the category field in the final API response.
    """
    if apply_keyword_filter:
        restricted, matched_kw = is_title_restricted(title)
        if restricted:
            return False, f"title matches restricted keyword: '{matched_kw}'"

    if apply_category_filter:
        restricted, reason = is_category_restricted(category)
        if restricted:
            return False, reason

    return True, None


def filter_products(
    products: List[Dict],
    *,
    title_key:             str  = "title",
    category_key:          str  = "category",
    apply_keyword_filter:  bool = True,
    apply_category_filter: bool = True,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Filter a list of product dicts.

    Returns:
        (allowed_products, rejected_products)

    rejected_products include an extra '_rejection_reason' key for logging.
    """
    allowed  = []
    rejected = []

    for product in products:
        title    = product.get(title_key, "")
        category = product.get(category_key)

        ok, reason = filter_product(
            title,
            category,
            apply_keyword_filter=apply_keyword_filter,
            apply_category_filter=apply_category_filter,
        )

        if ok:
            allowed.append(product)
        else:
            rejected.append({**product, "_rejection_reason": reason})

    logger.info(
        f"[filter] {len(allowed)} allowed / {len(rejected)} rejected "
        f"out of {len(products)} products"
    )
    return allowed, rejected
