"""
product_filter.py  (v2.3)
═════════════════════════
Two independent filters applied in sequence:

  1. KEYWORD FILTER — partial substring match on ORIGINAL product title only.
     • Case-insensitive
     • Partial match: keyword "gun" matches "airgun", "gunpowder", etc.
     • Checks ONLY the raw scraped title — never the enhanced/LLM title
     • Returns True → restricted ("Title has restricted keyword")

  2. CATEGORY CONFIDENCE VALIDATION — threshold on the confidence score.
     Rules:
       < 0.50  → Strong reject  → set category = "Uncategorized"
       0.50–0.75 → Borderline   → reject → set category = "Uncategorized"
       ≥ 0.75  → Accept

  3. RESTRICTED CATEGORY EMBEDDING FILTER — cosine similarity on category.leaf.
     Embedding-based check against restricted_categories table in products.db:
       > 0.85  → BLOCK  → "Category blocked due to restriction"
       0.60–0.85 → REVIEW (allowed, logged only)
       < 0.60  → ALLOW

Dependencies: openai, numpy, python-dotenv, sqlite3
"""

import logging
import os
import pickle
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

DB_PATH     = os.getenv("DB_PATH", "products.db")
EMBED_MODEL = "text-embedding-3-small"

# Category confidence acceptance threshold
CONFIDENCE_ACCEPT_THRESHOLD = float(os.getenv("CONFIDENCE_ACCEPT_THRESHOLD", "0.75"))

# Restricted-category embedding thresholds
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
    resp = _get_client().embeddings.create(model=EMBED_MODEL, input=[text])
    vec  = np.array(resp.data[0].embedding, dtype=np.float32)
    norm = np.linalg.norm(vec)
    return vec / norm if norm > 0 else vec


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.dot(a, b))


# ─────────────────────────────────────────────────────────────────────────────
# DB CACHES — loaded once at first use, cleared by reload_filter_data()
# ─────────────────────────────────────────────────────────────────────────────

_keyword_list:        Optional[List[str]]             = None
_category_embeddings: Optional[Dict[str, np.ndarray]] = None


def _load_keyword_list() -> List[str]:
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
        for category, blob in rows:
            try:
                vec  = pickle.loads(blob)
                arr  = np.array(vec, dtype=np.float32)
                norm = np.linalg.norm(arr)
                result[category] = arr / norm if norm > 0 else arr
            except Exception as exc:
                logger.warning(f"[filter] Bad embedding for '{category}': {exc}")
    except Exception as exc:
        logger.error(f"[filter] Failed to load category embeddings: {exc}")

    logger.info(f"[filter] Loaded {len(result)} category embeddings from DB")
    _category_embeddings = result
    return _category_embeddings


def reload_filter_data() -> None:
    """
    Clear in-memory caches so data is re-read from DB on next call.
    Call after updating restricted_keywords or restricted_categories tables.
    """
    global _keyword_list, _category_embeddings
    _keyword_list        = None
    _category_embeddings = None
    logger.info("[filter] Caches cleared — will reload from DB on next call")


# ─────────────────────────────────────────────────────────────────────────────
# 1. KEYWORD FILTER  (ORIGINAL title only — partial match)
# ─────────────────────────────────────────────────────────────────────────────

def filter_restricted_keywords(title: str) -> bool:
    """
    Check whether the ORIGINAL product title contains any restricted keyword.

    Rules:
      • Case-insensitive
      • Partial match allowed — keyword "gun" matches "airgun", "gunpowder"
      • Only checks original scraped title, NEVER the enhanced/LLM title

    Returns:
        True  → title IS restricted  → caller should output "Title has restricted keyword"
        False → title is clean
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
        # Plain substring match (partial match as required)
        if kw in title_lower:
            logger.info(f"[filter] Title BLOCKED — keyword '{kw}' found in title")
            return True

    return False


def is_title_restricted(title: str) -> Tuple[bool, Optional[str]]:
    """
    Backward-compatible wrapper around filter_restricted_keywords.
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
        if kw in title_lower:
            logger.info(f"[filter] Title BLOCKED — keyword '{kw}' found in title")
            return True, kw

    return False, None


# ─────────────────────────────────────────────────────────────────────────────
# 2. CATEGORY CONFIDENCE VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def validate_category_confidence(confidence: float) -> Tuple[bool, str]:
    """
    Validate category confidence score.

    Rules:
      < 0.50   → Strong reject  → set category = "Uncategorized"
      0.50–0.75 → Borderline   → reject  → set category = "Uncategorized"
      ≥ 0.75   → Accept

    Returns:
        (accepted: bool, reason: str)
    """
    if confidence < 0.50:
        return False, f"Confidence {confidence:.2f} < 0.50 — strong reject"
    elif confidence < CONFIDENCE_ACCEPT_THRESHOLD:
        return False, (
            f"Confidence {confidence:.2f} in borderline range "
            f"[0.50, {CONFIDENCE_ACCEPT_THRESHOLD}) — rejected"
        )
    else:
        return True, f"Confidence {confidence:.2f} accepted (≥ {CONFIDENCE_ACCEPT_THRESHOLD})"


def apply_category_confidence(category: Optional[Dict]) -> Dict:
    """
    Apply confidence validation to a category dict.

    If confidence < 0.75 → replace entire category with Uncategorized dict.
    If confidence ≥ 0.75 → return unchanged.

    Returns the (possibly replaced) category dict.
    """
    if not category:
        return {
            "id": "0", "name": "Uncategorized",
            "leaf": "Uncategorized", "path": "", "confidence": 0.0,
        }

    confidence = float(category.get("confidence", 0.0))
    accepted, reason = validate_category_confidence(confidence)

    if not accepted:
        logger.info(f"[filter] Category confidence rejected: {reason} — setting Uncategorized")
        return {
            "id": "0", "name": "Uncategorized",
            "leaf": "Uncategorized", "path": "",
            "confidence": confidence,
        }

    logger.info(f"[filter] Category confidence: {reason}")
    return category


# ─────────────────────────────────────────────────────────────────────────────
# 3. RESTRICTED CATEGORY EMBEDDING FILTER  (operates on category.leaf)
# ─────────────────────────────────────────────────────────────────────────────

def is_category_restricted(category: Optional[Dict]) -> Tuple[bool, Optional[str]]:
    """
    Embed the category LEAF and compare against restricted category embeddings.

    Decision:
      cosine > 0.85  → BLOCK  → "Category blocked due to restriction"
      0.60 – 0.85    → REVIEW  (allowed, flagged in logs only)
      < 0.60         → ALLOW

    Returns (blocked: bool, reason_or_None).
    """
    if not category:
        return False, None

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
        logger.info(
            f"[filter] Category BLOCKED — leaf='{leaf}' matched '{best_category}' "
            f"score={best_score:.3f} (>= {BLOCK_THRESHOLD})"
        )
        return True, "Category blocked due to restriction"

    if best_score >= REVIEW_THRESHOLD:
        logger.info(
            f"[filter] Category REVIEW — leaf='{leaf}' matched '{best_category}' "
            f"score={best_score:.3f} (>= {REVIEW_THRESHOLD}, < {BLOCK_THRESHOLD})"
        )
        return False, f"REVIEW: leaf '{leaf}' score={best_score:.2f}"

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

    Steps (short-circuits on first rejection):
      1. Keyword filter on original title (partial match)
      2. Category embedding filter on category.leaf

    Returns (allowed: bool, rejection_reason: str | None).
    """
    if apply_keyword_filter:
        blocked, matched_kw = is_title_restricted(title)
        if blocked:
            return False, f"Title has restricted keyword: '{matched_kw}'"

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

    Returns (allowed_products, rejected_products).
    rejected_products include an extra '_rejection_reason' key.
    """
    allowed  = []
    rejected = []

    for product in products:
        ok, reason = filter_product(
            product.get(title_key, ""),
            product.get(category_key),
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
