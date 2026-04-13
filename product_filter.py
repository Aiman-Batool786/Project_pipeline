"""
product_filter.py  (v2.1)
═════════════════════════
Two independent filters applied in sequence:

  1. KEYWORD FILTER  — direct string match on product title.
     Function: filter_restricted_keywords(title)
     ❌ Block condition: title contains any restricted keyword (case-insensitive)
     📢 Output message (when blocked): "Title has restricted keyword"

  2. CATEGORY FILTER — embedding-based similarity on category.leaf.
     Embed the leaf text with OpenAI text-embedding-3-small, then compare
     against every embedding stored in the restricted_categories table.

     Score thresholds:
       > 0.85  → BLOCK   ("Category blocked due to restriction")
       0.6–0.85 → REVIEW  (allowed but flagged for manual review)
       < 0.6   → ALLOW

     📢 Output message (when blocked): "Category blocked due to restriction"

⚠️  IMPORTANT:
  • Keyword filter operates ONLY on the title field — never on description.
  • Category filter operates ONLY on category.leaf — never on full name path.
  • These two messages are the ONLY console outputs for blocked products.
  • No description field is read or used anywhere.

Dependencies: openai, numpy, python-dotenv, sqlite3
"""

import logging
import os
import pickle
import re
import sqlite3
from typing import Dict, List, Optional, Tuple

import numpy as np
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DB_PATH = os.getenv("DB_PATH", "products.db")
EMBED_MODEL = "text-embedding-3-small"

# Category similarity thresholds
BLOCK_THRESHOLD  = float(os.getenv("CATEGORY_BLOCK_THRESHOLD",  "0.85"))
REVIEW_THRESHOLD = float(os.getenv("CATEGORY_REVIEW_THRESHOLD", "0.60"))

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
# EMBEDDING HELPERS
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
# DB CACHES — loaded once at first use
# ─────────────────────────────────────────────────────────────────────────────

# Plain keyword strings loaded from restricted_keywords table
_keyword_list: Optional[List[str]] = None

# { category_text: np.ndarray } loaded from restricted_categories table
_category_embeddings: Optional[Dict[str, np.ndarray]] = None


def _load_keyword_list() -> List[str]:
    """
    Load restricted keywords as plain strings from DB.
    Falls back to empty list — product is allowed when DB has no keywords.
    """
    global _keyword_list
    if _keyword_list is not None:
        return _keyword_list

    result: List[str] = []
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT keyword FROM restricted_keywords").fetchall()
        conn.close()
        result = [row[0].strip().lower() for row in rows if row[0]]
    except Exception as exc:
        logger.error(f"[filter] Failed to load keywords: {exc}")

    logger.info(f"[filter] Loaded {len(result)} restricted keywords from DB")
    _keyword_list = result
    return _keyword_list


def _load_category_embeddings() -> Dict[str, np.ndarray]:
    """Load restricted category embeddings from DB."""
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
    Clear in-memory caches so data is re-read from DB on the next filter call.
    Call this after updating the restricted_keywords or restricted_categories tables.
    """
    global _keyword_list, _category_embeddings
    _keyword_list         = None
    _category_embeddings  = None
    logger.info("[filter] Caches cleared — will reload from DB on next call")


# ─────────────────────────────────────────────────────────────────────────────
# 1. KEYWORD FILTER  (operates on title only)
# ─────────────────────────────────────────────────────────────────────────────

def filter_restricted_keywords(title: str) -> bool:
    """
    Check whether the product title contains any restricted keyword.

    Matching is case-insensitive whole-word (uses word-boundary regex so that
    e.g. "weapons" matches keyword "weapon" only if it is a separate word token).

    Returns:
        True  → title IS restricted  → print "Title has restricted keyword"
        False → title is clean       → allow
    """
    if not title or not title.strip():
        return False

    keywords = _load_keyword_list()
    if not keywords:
        logger.warning("[filter] No restricted keywords in DB — keyword filter skipped")
        return False

    title_lower = title.lower()

    for kw in keywords:
        if not kw:
            continue
        # Use word-boundary match so "adult" doesn't trigger on "adultery" etc.
        pattern = r'\b' + re.escape(kw) + r'\b'
        if re.search(pattern, title_lower):
            print("Title has restricted keyword")
            logger.info(f"[filter] Title BLOCKED — matched keyword: '{kw}'")
            return True

    return False


# backward-compatible alias used by existing code
def is_title_restricted(title: str) -> Tuple[bool, Optional[str]]:
    """
    Embed-free title check — wrapper around filter_restricted_keywords.
    Returns (blocked: bool, matched_keyword_or_None).
    """
    if not title or not title.strip():
        return False, None

    keywords = _load_keyword_list()
    if not keywords:
        return False, None

    title_lower = title.lower()
    for kw in keywords:
        if not kw:
            continue
        pattern = r'\b' + re.escape(kw) + r'\b'
        if re.search(pattern, title_lower):
            print("Title has restricted keyword")
            logger.info(f"[filter] Title BLOCKED — matched keyword: '{kw}'")
            return True, kw

    return False, None


# ─────────────────────────────────────────────────────────────────────────────
# 2. CATEGORY FILTER  (operates on category.leaf only)
# ─────────────────────────────────────────────────────────────────────────────

def is_category_restricted(category: Optional[Dict]) -> Tuple[bool, Optional[str]]:
    """
    Embed the category LEAF and compare against all restricted category embeddings.

    Decision rules based on cosine similarity score:
      > 0.85  → BLOCK   → print "Category blocked due to restriction"
      0.6–0.85 → REVIEW  → allowed but flagged
      < 0.6   → ALLOW

    category dict expected shape:
        {
          "id":         "0D0604",
          "name":       "GARDEN - POOL/OUTDOOR FURNITURE - ...",
          "leaf":       "SUN LOUNGER",          ← THIS is what we embed
          "path":       "",
          "confidence": 0.6
        }

    Returns (blocked: bool, reason_or_None).
    """
    if not category:
        return False, None

    # Use ONLY the leaf field
    leaf = str(category.get("leaf", "")).strip()
    if not leaf or leaf in ("Unknown", "Uncategorized", ""):
        return False, None

    cat_embeddings = _load_category_embeddings()
    if not cat_embeddings:
        logger.warning("[filter] No category embeddings in DB — category filter skipped")
        return False, None

    try:
        leaf_vec = _embed(leaf)
    except Exception as exc:
        logger.error(f"[filter] Failed to embed category leaf '{leaf}': {exc}")
        return False, None

    best_score    = 0.0
    best_category = None

    for restricted_cat, rc_vec in cat_embeddings.items():
        score = _cosine(leaf_vec, rc_vec)
        if score > best_score:
            best_score    = score
            best_category = restricted_cat

    if best_score >= BLOCK_THRESHOLD:
        print("Category blocked due to restriction")
        logger.info(
            f"[filter] Category BLOCKED — leaf='{leaf}' matched '{best_category}' "
            f"score={best_score:.3f} (>={BLOCK_THRESHOLD})"
        )
        return True, f"Category blocked due to restriction (score={best_score:.2f})"

    if best_score >= REVIEW_THRESHOLD:
        logger.info(
            f"[filter] Category REVIEW — leaf='{leaf}' matched '{best_category}' "
            f"score={best_score:.3f} (>={REVIEW_THRESHOLD}, <{BLOCK_THRESHOLD})"
        )
        # Allowed but flagged — caller can inspect the reason
        return False, f"REVIEW: leaf '{leaf}' score={best_score:.2f} (manual review recommended)"

    # score < REVIEW_THRESHOLD → ALLOW
    return False, None


# ─────────────────────────────────────────────────────────────────────────────
# COMBINED FILTER GATE
# ─────────────────────────────────────────────────────────────────────────────

def filter_product(
    title: str,
    category: Optional[Dict],
    *,
    apply_keyword_filter:  bool = True,
    apply_category_filter: bool = True,
) -> Tuple[bool, Optional[str]]:
    """
    Combined filter gate for a single product.

    Pipeline (short-circuits on first rejection):
      1. Keyword filter: filter_restricted_keywords(title)
      2. Category filter: is_category_restricted(category)

    Returns:
        (allowed: bool, rejection_reason: str | None)

    If allowed is True  → product passes; rejection_reason is None.
    If allowed is False → rejection_reason describes which filter triggered.
    """
    if apply_keyword_filter:
        blocked, matched_kw = is_title_restricted(title)
        if blocked:
            return False, f"title matches restricted keyword: '{matched_kw}'"

    if apply_category_filter:
        blocked, reason = is_category_restricted(category)
        if blocked:
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

    rejected_products have an extra '_rejection_reason' key for logging.
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
