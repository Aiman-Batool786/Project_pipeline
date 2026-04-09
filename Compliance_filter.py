"""
compliance_filter.py
════════════════════
Semantic Compliance Filtering — OpenAI Embeddings for BOTH keywords and titles.

WHY ONE MODEL MATTERS:
  Your original code used OpenAI for keywords and SentenceTransformers for
  titles. These models produce vectors in different geometric spaces.
  Cosine similarity between them is meaningless — like comparing distances
  in miles vs kilometres and calling the numbers equal.

  Fix: OpenAI text-embedding-3-small for BOTH. Same space = valid scores.

PIPELINE:
  1. Load + preprocess restricted keywords from CSV
  2. Embed keywords with OpenAI → store in SQLite (run once)
  3. For each product title → embed with same OpenAI model
  4. Cosine similarity → classify Restricted / Safe

DEPENDENCIES:
  pip install openai scikit-learn numpy pandas python-dotenv
"""

import os
import json
import pickle
import sqlite3
import logging
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  — edit these if needed
# ─────────────────────────────────────────────────────────────────────────────

OPENAI_MODEL      = "text-embedding-3-small"   # cheap, fast, 1536-dim
OPENAI_BATCH_SIZE = 100                         # OpenAI max per request
DEFAULT_THRESHOLD = 0.65                        # tune: lower = stricter
DB_PATH           = "compliance.db"
KEYWORDS_CSV      = "restricted_keywords_list.csv"
CSV_COLUMN        = "desc_and_spec_restricted_keywords"


# ─────────────────────────────────────────────────────────────────────────────
# OPENAI EMBEDDING CLIENT
# ─────────────────────────────────────────────────────────────────────────────

class OpenAIEmbedder:
    """
    Thin wrapper around OpenAI Embeddings API.
    Single instance — reuse across the whole pipeline.
    """

    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "OPENAI_API_KEY not set. Add it to your .env file:\n"
                "  OPENAI_API_KEY=sk-..."
            )
        self._client   = OpenAI(api_key=api_key)
        self.model     = OPENAI_MODEL
        logger.info(f"[embedder] Using OpenAI model: {self.model}")

    def encode(self, texts: List[str]) -> np.ndarray:
        """
        Encode a list of texts into a unit-normalised embedding matrix.
        Shape: (len(texts), 1536)  — float32.

        Batches automatically at OPENAI_BATCH_SIZE to respect API limits.
        Adds a short retry on rate-limit errors.
        """
        if not texts:
            return np.zeros((0, 1536), dtype=np.float32)

        all_vecs = []
        for i in range(0, len(texts), OPENAI_BATCH_SIZE):
            batch = texts[i : i + OPENAI_BATCH_SIZE]
            for attempt in range(3):
                try:
                    resp = self._client.embeddings.create(
                        input=batch,
                        model=self.model,
                    )
                    vecs = np.array(
                        [item.embedding for item in resp.data],
                        dtype=np.float32,
                    )
                    # Unit-normalise so cosine similarity == dot product (fast)
                    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
                    vecs  = vecs / np.maximum(norms, 1e-9)
                    all_vecs.append(vecs)
                    break
                except Exception as e:
                    if attempt < 2:
                        wait = 2 ** attempt
                        logger.warning(f"[embedder] API error ({e}), retrying in {wait}s...")
                        time.sleep(wait)
                    else:
                        raise

        return np.vstack(all_vecs)


# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD PREPROCESSING
# ─────────────────────────────────────────────────────────────────────────────

def load_and_preprocess_keywords(csv_path: str = KEYWORDS_CSV) -> List[str]:
    """
    Load restricted keywords from CSV.
    Cleans: strip whitespace → lowercase → deduplicate → drop empty.
    """
    df = pd.read_csv(csv_path)

    col = CSV_COLUMN if CSV_COLUMN in df.columns else df.columns[0]
    if col != CSV_COLUMN:
        logger.warning(f"[keywords] Expected column '{CSV_COLUMN}', using '{col}'")

    raw = df[col].dropna().tolist()

    seen, clean = set(), []
    for kw in raw:
        k = str(kw).strip().lower()
        if k and k not in seen:
            seen.add(k)
            clean.append(k)

    logger.info(f"[keywords] {len(raw)} raw → {len(clean)} clean unique keywords")
    return clean


# ─────────────────────────────────────────────────────────────────────────────
# SQLITE — keyword embedding storage
# ─────────────────────────────────────────────────────────────────────────────

def init_db(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS keyword_embeddings (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword    TEXT    NOT NULL UNIQUE,
            embedding  BLOB    NOT NULL,
            model_name TEXT    NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS filter_config (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def store_keyword_embeddings(
    keywords:   List[str],
    embeddings: np.ndarray,
    model_name: str,
    db_path:    str = DB_PATH,
):
    """Store keyword embeddings in SQLite. Safe to re-run (INSERT OR REPLACE)."""
    assert len(keywords) == len(embeddings)
    conn = sqlite3.connect(db_path)
    rows = [(kw, pickle.dumps(embeddings[i]), model_name) for i, kw in enumerate(keywords)]
    conn.executemany(
        "INSERT OR REPLACE INTO keyword_embeddings (keyword, embedding, model_name) VALUES (?,?,?)",
        rows,
    )
    conn.execute("INSERT OR REPLACE INTO filter_config VALUES ('model', ?)",  (model_name,))
    conn.execute("INSERT OR REPLACE INTO filter_config VALUES ('count', ?)",  (str(len(keywords)),))
    conn.commit()
    conn.close()
    logger.info(f"[db] Stored {len(keywords)} keyword embeddings")


def load_keyword_embeddings(db_path: str = DB_PATH) -> Tuple[List[str], np.ndarray]:
    """Load all keyword embeddings from SQLite."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT keyword, embedding FROM keyword_embeddings ORDER BY id"
    ).fetchall()
    conn.close()

    if not rows:
        raise ValueError(
            "No keyword embeddings in DB. Run build_keyword_index() first."
        )

    keywords   = [r[0] for r in rows]
    embeddings = np.vstack([pickle.loads(r[1]) for r in rows]).astype(np.float32)
    logger.info(f"[db] Loaded {len(keywords)} keywords, shape={embeddings.shape}")
    return keywords, embeddings


# ─────────────────────────────────────────────────────────────────────────────
# INDEX BUILD — run once (or when keywords CSV changes)
# ─────────────────────────────────────────────────────────────────────────────

def build_keyword_index(
    csv_path:  str             = KEYWORDS_CSV,
    db_path:   str             = DB_PATH,
    embedder:  OpenAIEmbedder  = None,
) -> Tuple[List[str], np.ndarray]:
    """
    Build and persist the keyword embedding index.
    Returns (keywords, matrix) for immediate use.
    """
    if embedder is None:
        embedder = OpenAIEmbedder()

    keywords = load_and_preprocess_keywords(csv_path)

    print(f"[index] Embedding {len(keywords)} keywords with {embedder.model}...")
    embeddings = embedder.encode(keywords)
    print(f"[index] ✅ Embeddings shape: {embeddings.shape}")

    init_db(db_path)
    store_keyword_embeddings(keywords, embeddings, embedder.model, db_path)
    return keywords, embeddings


# ─────────────────────────────────────────────────────────────────────────────
# COMPLIANCE FILTER
# ─────────────────────────────────────────────────────────────────────────────

class ComplianceFilter:
    """
    Classifies product titles as 'Restricted' or 'Safe' using OpenAI
    embeddings + cosine similarity.

    Quick-start:
        cf = ComplianceFilter()
        cf.build_index("restricted_keywords_list.csv")  # once
        results = cf.classify_products(products)        # List[dict with 'title']
    """

    def __init__(
        self,
        db_path:   str   = DB_PATH,
        threshold: float = DEFAULT_THRESHOLD,
    ):
        self.db_path      = db_path
        self.threshold    = threshold
        self._embedder    = OpenAIEmbedder()
        self._keywords:   Optional[List[str]]  = None
        self._kw_matrix:  Optional[np.ndarray] = None

    # ── Setup ──────────────────────────────────────────────────────────────

    def build_index(self, csv_path: str = KEYWORDS_CSV) -> None:
        """Build keyword embedding index from CSV. Call once per keyword update."""
        self._keywords, self._kw_matrix = build_keyword_index(
            csv_path=csv_path,
            db_path=self.db_path,
            embedder=self._embedder,
        )

    def load_index(self) -> None:
        """Load an existing keyword index from DB."""
        self._keywords, self._kw_matrix = load_keyword_embeddings(self.db_path)

    def _ensure_index(self):
        if self._kw_matrix is None:
            self.load_index()

    # ── Single title ───────────────────────────────────────────────────────

    def classify_title(self, title: str) -> Dict:
        """
        Classify a single product title.

        Returns:
            status:          'Restricted' | 'Safe'
            max_similarity:  float
            matched_keyword: str | None
            top_matches:     [{keyword, similarity}]  ← top 3
        """
        self._ensure_index()
        vec  = self._embedder.encode([title])                      # (1, 1536)
        sims = cosine_similarity(vec, self._kw_matrix)[0]         # (n_keywords,)

        top3_idx  = np.argsort(sims)[::-1][:3]
        max_sim   = float(sims[top3_idx[0]])
        status    = "Restricted" if max_sim >= self.threshold else "Safe"
        matched   = self._keywords[top3_idx[0]] if status == "Restricted" else None
        top3      = [
            {"keyword": self._keywords[i], "similarity": round(float(sims[i]), 4)}
            for i in top3_idx
        ]
        return {
            "title":           title,
            "status":          status,
            "max_similarity":  round(max_sim, 4),
            "matched_keyword": matched,
            "top_matches":     top3,
        }

    # ── Batch titles ───────────────────────────────────────────────────────

    def classify_products(
        self,
        products: List[Dict],
    ) -> List[Dict]:
        """
        Classify a list of products. Each dict must have a 'title' key.

        Efficient: embeds ALL titles in one API call batch, then does a
        single matrix multiply for all similarities at once.

        Adds these keys to each product dict:
            compliance_status  — 'Restricted' | 'Safe'
            max_similarity     — float
            matched_keyword    — str | None
            top_matches        — list of top 3 {keyword, similarity}
        """
        self._ensure_index()

        titles = [p.get("title", "") for p in products]
        if not any(titles):
            logger.warning("[filter] No titles found in products list")
            return products

        print(f"[filter] Classifying {len(titles)} product titles via OpenAI...")
        t0 = time.time()

        # One batch call for all titles
        title_vecs = self._embedder.encode(titles)          # (n, 1536)

        # Single matrix multiply: (n, 1536) × (1536, n_kw) → (n, n_kw)
        sim_matrix = cosine_similarity(title_vecs, self._kw_matrix)

        results = []
        restricted_count = 0

        for i, product in enumerate(products):
            sims     = sim_matrix[i]
            top_idx  = np.argsort(sims)[::-1][:3]
            max_sim  = float(sims[top_idx[0]])
            status   = "Restricted" if max_sim >= self.threshold else "Safe"
            matched  = self._keywords[top_idx[0]] if status == "Restricted" else None
            top3     = [
                {"keyword": self._keywords[j], "similarity": round(float(sims[j]), 4)}
                for j in top_idx
            ]
            if status == "Restricted":
                restricted_count += 1

            results.append({
                **product,
                "compliance_status":  status,
                "max_similarity":     round(max_sim, 4),
                "matched_keyword":    matched,
                "top_matches":        top3,
            })

        elapsed = time.time() - t0
        safe_count = len(results) - restricted_count
        print(f"[filter] ✅ Done in {elapsed:.2f}s")
        print(f"[filter]    🚫 Restricted : {restricted_count}")
        print(f"[filter]    ✅ Safe       : {safe_count}")
        print(f"[filter]    Threshold    : {self.threshold}")
        return results

    # ── Report ─────────────────────────────────────────────────────────────

    def summary_dataframe(self, classified: List[Dict]) -> pd.DataFrame:
        rows = [{
            "Product ID":      p.get("product_id", ""),
            "Title":           p.get("title", ""),
            "Status":          p.get("compliance_status", ""),
            "Max Similarity":  p.get("max_similarity", 0),
            "Matched Keyword": p.get("matched_keyword", ""),
            "Product URL":     p.get("product_url", ""),
        } for p in classified]
        return pd.DataFrame(rows)
