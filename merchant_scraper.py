"""
merchant_scraper.py — Batch-Safe Bulk Processor (v2)
──────────────────────────────────────────────────────
Key improvements over v1:

  BROWSER REUSE PER WORKER:
    Instead of open→scrape→close per merchant (slow), each worker thread:
      • Opens browser ONCE
      • Processes a chunk of 20 merchants sequentially
      • Closes browser after all 20 are done
    Eliminates repeated browser startup cost for large volumes (2000+ merchants).

  SMALLER BATCHES (50 instead of 100):
    • Faster progress updates on disk
    • Better crash recovery (smaller unit of lost work)
    • More balanced task distribution

  MORE WORKERS (8 instead of 5):
    • 8 threads × 20 merchants per browser = 160 merchants processed per batch
    • Each batch of 50 is split into sub-chunks of ~7 merchants per worker

  FILE LAYOUT (unchanged):
    ./merchant_jobs/{job_id}/
        metadata.json        ← job config + per-batch statuses
        batch_0000.csv       ← results saved after batch 0 done
        batch_0001.csv       ← results saved after batch 1 done
        ...
        output.csv           ← merged final CSV (written when all batches done)

  MEMORY:
    _jobs dict holds ONLY status counts — never results.
    All results live on disk. Safe for 2267+ merchants with no OOM risk.
"""

import re
import csv
import json
import time
import math
import random
import logging
import threading
import io
from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from camoufox.sync_api import Camoufox

logger = logging.getLogger("merchant_scraper")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

STORE_URL_TEMPLATE = (
    "https://www.aliexpress.com/store/{merchant_id}/pages/all-items.html"
    "?shop_sortType=bestmatch_sort"
)

BATCH_SIZE          = 50    # merchants per batch file saved to disk
CONCURRENCY         = 8     # parallel worker threads per batch
MERCHANTS_PER_CHUNK = 20    # merchants each worker processes with ONE browser session
MAX_RETRIES         = 3
PAGE_TIMEOUT        = 45_000
DELAY_MIN           = 1.5
DELAY_MAX           = 3.5
JOBS_DIR            = Path("./merchant_jobs")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# In-memory registry: ONLY status counts, never results
_jobs: Dict[str, Dict] = {}
_jobs_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# DISK HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _job_dir(job_id: str) -> Path:
    d = JOBS_DIR / job_id
    d.mkdir(parents=True, exist_ok=True)
    return d

def _metadata_path(job_id: str) -> Path:
    return _job_dir(job_id) / "metadata.json"

def _batch_path(job_id: str, batch_idx: int) -> Path:
    return _job_dir(job_id) / f"batch_{batch_idx:04d}.csv"

def _output_path(job_id: str) -> Path:
    return _job_dir(job_id) / "output.csv"

def _save_metadata(job_id: str, meta: dict) -> None:
    with open(_metadata_path(job_id), "w") as f:
        json.dump(meta, f, indent=2)

def _load_metadata(job_id: str) -> Optional[dict]:
    path = _metadata_path(job_id)
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


# ─────────────────────────────────────────────────────────────────────────────
# CSV PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_merchant_csv(file_bytes: bytes) -> List[str]:
    """Read CSV bytes, strip Excel BOM, return list of merchant IDs."""
    text        = file_bytes.decode("utf-8-sig", errors="replace")
    reader      = csv.DictReader(io.StringIO(text))
    raw_headers = reader.fieldnames or []
    headers     = [h.strip().lstrip("\ufeff").lower() for h in raw_headers]
    header_map  = {h.strip().lstrip("\ufeff").lower(): h for h in raw_headers}

    id_col_norm = None
    for candidate in ["merchantid", "merchant_id", "merchant id", "id", "store_id", "storeid"]:
        if candidate in headers:
            id_col_norm = candidate
            break

    if id_col_norm is None:
        raise ValueError(f"CSV must have a 'MerchantID' column. Found: {raw_headers}")

    id_col = header_map[id_col_norm]
    ids = []
    for row in reader:
        raw = str(row.get(id_col, "") or "").strip()
        if raw and re.match(r"^\d+$", raw):
            ids.append(raw)

    logger.info(f"[merchant_scraper] Parsed {len(ids)} merchant IDs")
    return ids


# ─────────────────────────────────────────────────────────────────────────────
# ITEM COUNT EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def _extract_item_count_from_html(html: str) -> Optional[int]:
    # Primary: confirmed spm anchor selector
    m = re.search(
        r'data-spm-anchor-id="[^"]*store_pc_allItems[^"]*"[^>]*>\s*(\d[\d,]*)\s*items?',
        html, re.IGNORECASE
    )
    if m:
        return int(m.group(1).replace(",", ""))

    # Secondary: store context
    m2 = re.search(
        r'(?:store|allItems|groupList)[^>]*>[^<]*?(\d[\d,]+)\s*items?',
        html, re.IGNORECASE
    )
    if m2:
        return int(m2.group(1).replace(",", ""))

    # Broad: largest "N items" in page
    all_matches = re.findall(r'\b(\d[\d,]*)\s+items?\b', html, re.IGNORECASE)
    if all_matches:
        nums = [int(x.replace(",", "")) for x in all_matches]
        c = max(nums)
        if c > 0:
            return c

    # JSON fallback
    m3 = re.search(r'"(?:totalProducts|itemCount|totalItems)"\s*:\s*(\d+)', html)
    if m3:
        return int(m3.group(1))

    return None


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE PAGE FETCH (reuses existing browser context)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_merchant_with_context(page, merchant_id: str) -> Dict:
    """
    Scrape one merchant using an already-open browser page.
    Retries up to MAX_RETRIES on transient failures.
    Does NOT open or close the browser — caller manages that.
    """
    url = STORE_URL_TEMPLATE.format(merchant_id=merchant_id)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            try:
                page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            except Exception as nav_err:
                err_str = str(nav_err)
                if any(x in err_str for x in ["ERR_NAME_NOT_RESOLVED", "NS_ERROR"]):
                    return {"merchant_id": merchant_id, "total_items": None,
                            "error": "Page Not Found"}
                raise

            page.wait_for_timeout(random.randint(2000, 3500))
            for _ in range(2):
                page.mouse.wheel(0, 600)
                page.wait_for_timeout(300)

            html  = page.content()
            lower = html.lower()

            if any(k in lower for k in [
                "captcha", "robot", "verify you are human",
                "access denied", "blocked"
            ]):
                if attempt < MAX_RETRIES:
                    time.sleep(random.uniform(5, 12))
                    continue
                return {"merchant_id": merchant_id, "total_items": None, "error": "Blocked"}

            if any(k in lower for k in [
                "page not found", "store not found", "doesn't exist"
            ]):
                return {"merchant_id": merchant_id, "total_items": None,
                        "error": "Invalid Merchant"}

            count = _extract_item_count_from_html(html)
            if count is None:
                if attempt < MAX_RETRIES:
                    time.sleep(random.uniform(3, 7))
                    continue
                return {"merchant_id": merchant_id, "total_items": None,
                        "error": "Selector Missing"}

            logger.info(f"[merchant] {merchant_id} ✓ {count} items")
            return {"merchant_id": merchant_id, "total_items": count, "error": ""}

        except Exception as exc:
            err_str = str(exc)
            label   = "Timeout" if "timeout" in err_str.lower() else f"Error: {err_str[:80]}"
            logger.warning(f"[merchant] {merchant_id} attempt {attempt} — {label}")
            if attempt < MAX_RETRIES:
                time.sleep(random.uniform(4, 10))
                continue
            return {"merchant_id": merchant_id, "total_items": None, "error": label}

    return {"merchant_id": merchant_id, "total_items": None, "error": "Max retries exceeded"}


# ─────────────────────────────────────────────────────────────────────────────
# WORKER: open browser ONCE, process a chunk of merchants, close browser
# ─────────────────────────────────────────────────────────────────────────────

def _worker_process_chunk(merchant_ids: List[str]) -> List[Dict]:
    """
    Worker function run inside a thread.

    Opens ONE browser session, processes all merchant_ids sequentially,
    then closes the browser. This avoids the per-merchant open/close overhead
    that was the main bottleneck with 2000+ merchants.

    Returns list of result dicts (one per merchant_id).
    """
    results = []
    ua = random.choice(USER_AGENTS)

    try:
        with Camoufox(headless=True, os="windows") as browser:
            ctx = browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                user_agent=ua,
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            page = ctx.new_page()

            for merchant_id in merchant_ids:
                try:
                    result = _fetch_merchant_with_context(page, merchant_id)
                    results.append(result)
                except Exception as exc:
                    logger.error(f"[merchant] chunk error for {merchant_id}: {exc}")
                    results.append({
                        "merchant_id": merchant_id,
                        "total_items": None,
                        "error": f"ChunkError: {str(exc)[:80]}",
                    })

                # Polite delay between merchants in the same session
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            try:
                page.close()
                ctx.close()
            except Exception:
                pass

    except Exception as browser_exc:
        # If the browser itself fails to open, mark all remaining merchants as error
        logger.error(f"[merchant] Browser session failed: {browser_exc}")
        already_done = {r["merchant_id"] for r in results}
        for mid in merchant_ids:
            if mid not in already_done:
                results.append({
                    "merchant_id": mid,
                    "total_items": None,
                    "error": f"BrowserError: {str(browser_exc)[:80]}",
                })

    return results


# ─────────────────────────────────────────────────────────────────────────────
# BATCH WRITER
# ─────────────────────────────────────────────────────────────────────────────

def _write_batch_csv(job_id: str, batch_idx: int, rows: List[Dict]) -> None:
    """Write one batch result to its own CSV file. Safe on crash — already on disk."""
    path = _batch_path(job_id, batch_idx)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["MerchantID", "TotalItems", "Error"])
        for row in rows:
            w.writerow([
                row.get("merchant_id", ""),
                "" if row.get("total_items") is None else row["total_items"],
                row.get("error", ""),
            ])
    logger.info(f"[job:{job_id}] Batch {batch_idx:04d} → {path.name} ({len(rows)} rows)")


def _merge_batch_csvs(job_id: str, batches_total: int) -> Path:
    """Merge all batch CSVs → single output.csv."""
    out_path = _output_path(job_id)
    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["MerchantID", "TotalItems", "Error"])
        for idx in range(batches_total):
            bf = _batch_path(job_id, idx)
            if not bf.exists():
                continue
            with open(bf, newline="", encoding="utf-8") as in_f:
                reader = csv.reader(in_f)
                next(reader, None)  # skip header
                for row in reader:
                    writer.writerow(row)
    logger.info(f"[job:{job_id}] Merged {batches_total} batches → output.csv")
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# BATCH RUNNER — uses browser-reuse workers
# ─────────────────────────────────────────────────────────────────────────────

def _run_batch(job_id: str, batch_idx: int, merchant_ids: List[str]) -> None:
    """
    Process one batch of merchants.

    Splits the batch into chunks of MERCHANTS_PER_CHUNK, runs each chunk
    in its own thread (with one browser per thread), then writes the
    combined batch result to disk immediately.

    Example with BATCH_SIZE=50, CONCURRENCY=8, MERCHANTS_PER_CHUNK=20:
      50 merchants → ceil(50/20) = 3 chunks
      3 chunks run concurrently across up to 8 threads
      Each chunk opens 1 browser, processes its 20 merchants, closes browser
      Total: 3 browser sessions instead of 50
    """
    logger.info(
        f"[job:{job_id}] Batch {batch_idx:04d} start — "
        f"{len(merchant_ids)} merchants, chunk_size={MERCHANTS_PER_CHUNK}, "
        f"workers={CONCURRENCY}"
    )

    # Split batch into chunks
    chunks = [
        merchant_ids[i:i + MERCHANTS_PER_CHUNK]
        for i in range(0, len(merchant_ids), MERCHANTS_PER_CHUNK)
    ]

    all_rows: List[Dict] = []

    # Run chunks concurrently — each in its own browser session
    with ThreadPoolExecutor(max_workers=min(CONCURRENCY, len(chunks))) as pool:
        futures = {pool.submit(_worker_process_chunk, chunk): idx
                   for idx, chunk in enumerate(chunks)}
        for future in as_completed(futures):
            chunk_idx = futures[future]
            try:
                rows = future.result(timeout=300)
                all_rows.extend(rows)
                logger.info(
                    f"[job:{job_id}] Batch {batch_idx:04d} chunk {chunk_idx} done "
                    f"({len(rows)} merchants)"
                )
            except Exception as exc:
                # Recover: mark the whole chunk as failed
                chunk = chunks[chunk_idx]
                logger.error(
                    f"[job:{job_id}] Batch {batch_idx:04d} chunk {chunk_idx} "
                    f"FAILED: {exc}"
                )
                for mid in chunk:
                    all_rows.append({
                        "merchant_id": mid,
                        "total_items": None,
                        "error": f"ChunkFailed: {str(exc)[:80]}",
                    })

    # Write combined batch result to disk immediately
    _write_batch_csv(job_id, batch_idx, all_rows)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN JOB RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def _run_bulk_job(job_id: str, merchant_ids: List[str]) -> None:
    batches       = [merchant_ids[i:i + BATCH_SIZE]
                     for i in range(0, len(merchant_ids), BATCH_SIZE)]
    batches_total = len(batches)
    total         = len(merchant_ids)

    meta = {
        "job_id":        job_id,
        "status":        "running",
        "total":         total,
        "batches_total": batches_total,
        "batches_done":  0,
        "batches_failed": 0,
        "started_at":    datetime.utcnow().isoformat(),
        "finished_at":   None,
        "config": {
            "batch_size":            BATCH_SIZE,
            "concurrency":           CONCURRENCY,
            "merchants_per_chunk":   MERCHANTS_PER_CHUNK,
        },
        "batches": [
            {"idx": i, "size": len(b), "status": "queued"}
            for i, b in enumerate(batches)
        ],
    }
    _save_metadata(job_id, meta)

    with _jobs_lock:
        _jobs[job_id].update({
            "status":         "running",
            "total":          total,
            "batches_total":  batches_total,
            "batches_done":   0,
            "batches_failed": 0,
        })

    logger.info(
        f"[job:{job_id}] Start — {total} merchants | "
        f"{batches_total} batches × {BATCH_SIZE} | "
        f"{CONCURRENCY} workers × {MERCHANTS_PER_CHUNK} merchants/browser"
    )

    for idx, batch in enumerate(batches):
        meta["batches"][idx]["status"] = "running"
        _save_metadata(job_id, meta)

        try:
            _run_batch(job_id, idx, batch)
            meta["batches"][idx]["status"] = "done"
            meta["batches_done"] += 1
        except Exception as exc:
            logger.error(f"[job:{job_id}] Batch {idx:04d} FAILED: {exc}")
            meta["batches"][idx]["status"] = "failed"
            meta["batches"][idx]["error"]  = str(exc)[:200]
            meta["batches_failed"] += 1

        _save_metadata(job_id, meta)

        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["batches_done"]   = meta["batches_done"]
                _jobs[job_id]["batches_failed"] = meta["batches_failed"]

        processed      = meta["batches_done"] + meta["batches_failed"]
        merchants_done = min(processed * BATCH_SIZE, total)
        pct            = round(merchants_done / total * 100, 1)
        logger.info(
            f"[job:{job_id}] {processed}/{batches_total} batches done "
            f"(~{merchants_done}/{total} merchants, {pct}%)"
        )

    # Merge all batch CSVs → output.csv
    try:
        _merge_batch_csvs(job_id, batches_total)
    except Exception as e:
        logger.error(f"[job:{job_id}] Merge failed: {e}")

    meta["status"]      = "done"
    meta["finished_at"] = datetime.utcnow().isoformat()
    _save_metadata(job_id, meta)

    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "done"

    logger.info(
        f"[job:{job_id}] ✓ Complete — "
        f"{meta['batches_done']} ok | {meta['batches_failed']} failed"
    )


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

def start_bulk_job(job_id: str, merchant_ids: List[str]) -> None:
    """Launch bulk processing in a background daemon thread."""
    with _jobs_lock:
        _jobs[job_id] = {
            "status":         "queued",
            "total":          len(merchant_ids),
            "batches_total":  0,
            "batches_done":   0,
            "batches_failed": 0,
        }
    t = threading.Thread(
        target=_run_bulk_job,
        args=(job_id, merchant_ids),
        daemon=True,
        name=f"merchant-{job_id[:8]}",
    )
    t.start()


def get_job_status(job_id: str) -> Optional[Dict]:
    """Return status from disk (authoritative) merged with live memory counts."""
    with _jobs_lock:
        mem = dict(_jobs.get(job_id, {}))

    disk = _load_metadata(job_id)

    if not mem and not disk:
        return None

    if disk:
        total           = disk.get("total", 0)
        batches_total   = disk.get("batches_total", 0)

        # Count merchants processed from per-batch sizes
        merchants_done = 0
        for b in disk.get("batches", []):
            if b.get("status") in ("done", "failed"):
                merchants_done += b.get("size", BATCH_SIZE)
        merchants_done = min(merchants_done, total)

        return {
            "status":              disk.get("status", mem.get("status", "unknown")),
            "total_merchants":     total,
            "merchants_done":      merchants_done,
            "merchants_remaining": max(0, total - merchants_done),
            "batches_total":       batches_total,
            "batches_done":        disk.get("batches_done", 0),
            "batches_failed":      disk.get("batches_failed", 0),
            "progress_pct":        round(merchants_done / total * 100, 1) if total else 0.0,
            "started_at":          disk.get("started_at"),
            "finished_at":         disk.get("finished_at"),
            "config":              disk.get("config", {}),
            "batches":             disk.get("batches", []),
            "download_ready":      disk.get("status") == "done",
            "download_url":        (f"/merchant-download/{job_id}"
                                    if disk.get("status") == "done" else None),
        }

    return mem


def is_job_done(job_id: str) -> bool:
    meta = _load_metadata(job_id)
    return meta is not None and meta.get("status") == "done"


def get_output_path(job_id: str) -> Optional[Path]:
    path = _output_path(job_id)
    return path if path.exists() else None


def list_all_jobs() -> List[Dict]:
    """List all jobs from disk — survives server restart."""
    result = []
    if not JOBS_DIR.exists():
        return result

    for job_dir in sorted(JOBS_DIR.iterdir(), reverse=True):
        if not job_dir.is_dir():
            continue
        meta = _load_metadata(job_dir.name)
        if not meta:
            continue

        total = meta.get("total", 0)

        merchants_done = 0
        for b in meta.get("batches", []):
            if b.get("status") in ("done", "failed"):
                merchants_done += b.get("size", BATCH_SIZE)
        merchants_done = min(merchants_done, total)

        result.append({
            "job_id":              job_dir.name,
            "status":              meta.get("status"),
            "total_merchants":     total,
            "merchants_done":      merchants_done,
            "merchants_remaining": max(0, total - merchants_done),
            "batches_total":       meta.get("batches_total", 0),
            "batches_done":        meta.get("batches_done", 0),
            "batches_failed":      meta.get("batches_failed", 0),
            "progress_pct":        round(merchants_done / total * 100, 1) if total else 0.0,
            "config":              meta.get("config", {}),
            "started_at":          meta.get("started_at"),
            "finished_at":         meta.get("finished_at"),
            "download_url":        (f"/merchant-download/{job_dir.name}"
                                    if meta.get("status") == "done" else None),
        })
    return result
