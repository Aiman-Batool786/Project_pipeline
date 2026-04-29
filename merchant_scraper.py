"""
merchant_scraper.py — Batch-Safe Bulk Processor
─────────────────────────────────────────────────
Design:
  Splits merchants into batches of 100 → processes each batch with 5 threads
  → writes batch CSV to disk immediately → never loses data on crash.

File layout:
  ./merchant_jobs/{job_id}/
      metadata.json       ← job config + per-batch statuses
      batch_0000.csv      ← results saved after batch 0 done
      batch_0001.csv      ← results saved after batch 1 done
      ...
      output.csv          ← merged final CSV (written when all batches done)
"""

import re
import csv
import json
import time
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

BATCH_SIZE   = 100
CONCURRENCY  = 5
MAX_RETRIES  = 3
PAGE_TIMEOUT = 45_000
DELAY_MIN    = 1.5
DELAY_MAX    = 3.5
JOBS_DIR     = Path("./merchant_jobs")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# Lightweight in-memory registry — only status counts, NOT results
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
    """
    Extract item count from store page HTML.

    Exact confirmed HTML:
      <span data-spm-anchor-id="a2g0o.store_pc_allItems_or_groupList.0.i41.xxx"
            style="font-size: 15px; ...">227 items</span>
    """
    # Primary A: exact spm anchor — store_pc_allItems_or_groupList (confirmed selector)
    m = re.search(
        r'data-spm-anchor-id="[^"]*store_pc_allItems_or_groupList[^"]*"[^>]*>'
        r'\s*(\d[\d,]*)\s*items?',
        html, re.IGNORECASE
    )
    if m:
        return int(m.group(1).replace(",", ""))

    # Primary B: broader store_pc_allItems (catches variant anchor IDs)
    m = re.search(
        r'data-spm-anchor-id="[^"]*store_pc_allItems[^"]*"[^>]*>\s*(\d[\d,]*)\s*items?',
        html, re.IGNORECASE
    )
    if m:
        return int(m.group(1).replace(",", ""))

    # Secondary: span with inline font-size style near "N items"
    # Matches: <span style="font-size: 15px; font-weight: 400; color: rgb(25, 25, 25);">227 items</span>
    m2 = re.search(
        r'<span[^>]+font-size:\s*1[0-9]px[^>]*>\s*(\d[\d,]+)\s*items?\s*</span>',
        html, re.IGNORECASE
    )
    if m2:
        return int(m2.group(1).replace(",", ""))

    # Tertiary: store/allItems/groupList context
    m3 = re.search(
        r'(?:store|allItems|groupList|itemCount)[^>]{0,200}?(\d[\d,]+)\s*items?',
        html, re.IGNORECASE
    )
    if m3:
        val = int(m3.group(1).replace(",", ""))
        if val > 0:
            return val

    # Broad fallback: largest "N items" anywhere on page (N > 0)
    all_matches = re.findall(r'\b(\d[\d,]+)\s+items?\b', html, re.IGNORECASE)
    if all_matches:
        nums = [int(x.replace(",", "")) for x in all_matches if int(x.replace(",", "")) > 0]
        if nums:
            return max(nums)

    # JSON fallback
    m4 = re.search(r'"(?:totalProducts|itemCount|totalItems|storeItemCount)"\s*:\s*(\d+)', html)
    if m4:
        return int(m4.group(1))

    return None


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE MERCHANT SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_merchant(merchant_id: str) -> Dict:
    url = STORE_URL_TEMPLATE.format(merchant_id=merchant_id)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ua = random.choice(USER_AGENTS)
            with Camoufox(headless=True, os="windows") as browser:
                ctx  = browser.new_context(
                    viewport={"width": 1440, "height": 900},
                    locale="en-US",
                    user_agent=ua,
                    extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                )
                page = ctx.new_page()
                try:
                    page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                except Exception as nav_err:
                    err_str = str(nav_err)
                    if "NS_BINDING_ABORTED" in err_str or "ERR_ABORTED" in err_str:
                        # Redirect mid-load — content may still be usable, continue
                        logger.warning(f"[merchant] {merchant_id} nav aborted (redirect) — continuing")
                    elif any(x in err_str for x in ["ERR_NAME_NOT_RESOLVED", "NS_ERROR_UNKNOWN"]):
                        return {"merchant_id": merchant_id, "total_items": None, "error": "Page Not Found"}
                    elif "NS_ERROR" in err_str:
                        return {"merchant_id": merchant_id, "total_items": None, "error": "Page Not Found"}
                    else:
                        raise

                # Wait for JS to render item count span — try explicit selector first
                try:
                    page.wait_for_selector(
                        'span[data-spm-anchor-id*="store_pc_allItems"]',
                        timeout=8000
                    )
                except Exception:
                    # Selector didn't appear — extra JS render time
                    page.wait_for_timeout(random.randint(3000, 5000))

                page.wait_for_timeout(random.randint(1500, 2500))
                for _ in range(2):
                    page.mouse.wheel(0, 600)
                    page.wait_for_timeout(400)

                html = page.content()
                page.close()
                ctx.close()

            lower = html.lower()
            if any(k in lower for k in ["captcha", "robot", "verify you are human", "access denied", "blocked"]):
                if attempt < MAX_RETRIES:
                    time.sleep(random.uniform(5, 12))
                    continue
                return {"merchant_id": merchant_id, "total_items": None, "error": "Blocked"}

            if any(k in lower for k in ["page not found", "store not found", "doesn't exist"]):
                return {"merchant_id": merchant_id, "total_items": None, "error": "Invalid Merchant"}

            count = _extract_item_count_from_html(html)
            if count is None:
                if attempt < MAX_RETRIES:
                    time.sleep(random.uniform(3, 7))
                    continue
                return {"merchant_id": merchant_id, "total_items": None, "error": "Selector Missing"}

            logger.info(f"[merchant] {merchant_id} ✓ {count} items")
            return {"merchant_id": merchant_id, "total_items": count, "error": ""}

        except Exception as exc:
            err_str = str(exc)
            label = "Timeout" if "timeout" in err_str.lower() else f"Error: {err_str[:80]}"
            logger.error(f"[merchant] {merchant_id} attempt {attempt} — {label}")
            if attempt < MAX_RETRIES:
                time.sleep(random.uniform(4, 10))
                continue
            return {"merchant_id": merchant_id, "total_items": None, "error": label}

    return {"merchant_id": merchant_id, "total_items": None, "error": "Max retries exceeded"}


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
# BATCH RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def _run_batch(job_id: str, batch_idx: int, merchant_ids: List[str]) -> None:
    """Process one batch concurrently, write results to disk immediately."""
    logger.info(f"[job:{job_id}] Batch {batch_idx:04d} start — {len(merchant_ids)} merchants")
    rows = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = {pool.submit(_scrape_merchant, mid): mid for mid in merchant_ids}
        for future in as_completed(futures):
            try:
                row = future.result(timeout=240)
            except Exception as e:
                mid = futures[future]
                row = {"merchant_id": mid, "total_items": None, "error": str(e)[:120]}
            rows.append(row)
    _write_batch_csv(job_id, batch_idx, rows)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN JOB RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def _run_bulk_job(job_id: str, merchant_ids: List[str]) -> None:
    batches       = [merchant_ids[i:i + BATCH_SIZE] for i in range(0, len(merchant_ids), BATCH_SIZE)]
    batches_total = len(batches)
    total         = len(merchant_ids)

    meta = {
        "job_id": job_id, "status": "running", "total": total,
        "batches_total": batches_total, "batches_done": 0, "batches_failed": 0,
        "started_at": datetime.utcnow().isoformat(), "finished_at": None,
        "batches": [{"idx": i, "size": len(b), "status": "queued"} for i, b in enumerate(batches)],
    }
    _save_metadata(job_id, meta)

    with _jobs_lock:
        _jobs[job_id].update({"status": "running", "total": total,
                               "batches_total": batches_total, "batches_done": 0, "batches_failed": 0})

    logger.info(f"[job:{job_id}] Start — {total} merchants | {batches_total} batches of {BATCH_SIZE}")

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
                _jobs[job_id]["batches_done"]    = meta["batches_done"]
                _jobs[job_id]["batches_failed"]  = meta["batches_failed"]

        processed = meta["batches_done"] + meta["batches_failed"]
        pct       = round(processed / batches_total * 100, 1)
        merchants_done = min(processed * BATCH_SIZE, total)
        logger.info(f"[job:{job_id}] {processed}/{batches_total} batches done "
                    f"({merchants_done}/{total} merchants, {pct}%)")

        if idx < batches_total - 1:
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # Merge all batch CSVs into one final file
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

    logger.info(f"[job:{job_id}] ✓ Complete — "
                f"{meta['batches_done']} ok | {meta['batches_failed']} failed")


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

def start_bulk_job(job_id: str, merchant_ids: List[str]) -> None:
    """Launch bulk processing in a background daemon thread."""
    with _jobs_lock:
        _jobs[job_id] = {"status": "queued", "total": len(merchant_ids),
                         "batches_total": 0, "batches_done": 0, "batches_failed": 0}
    t = threading.Thread(target=_run_bulk_job, args=(job_id, merchant_ids),
                         daemon=True, name=f"merchant-{job_id[:8]}")
    t.start()


def get_job_status(job_id: str) -> Optional[Dict]:
    """Return status from memory (fast) with batch detail from disk."""
    with _jobs_lock:
        mem = dict(_jobs.get(job_id, {}))

    disk = _load_metadata(job_id)

    if not mem and not disk:
        return None

    if disk:
        total             = disk.get("batches_total", 0)
        processed         = disk.get("batches_done", 0) + disk.get("batches_failed", 0)
        total_merchants   = disk.get("total", 0)

        # Count merchants done from per-batch sizes
        merchants_done = 0
        for b in disk.get("batches", []):
            if b.get("status") in ("done", "failed"):
                merchants_done += b.get("size", BATCH_SIZE)
        merchants_done = min(merchants_done, total_merchants)

        return {
            "status":              disk.get("status", mem.get("status", "unknown")),
            "total_merchants":     total_merchants,
            "merchants_done":      merchants_done,
            "merchants_remaining": max(0, total_merchants - merchants_done),
            "batches_total":       disk.get("batches_total", 0),
            "batches_done":        disk.get("batches_done", 0),
            "batches_failed":      disk.get("batches_failed", 0),
            "progress_pct":        round(merchants_done / total_merchants * 100, 1) if total_merchants else 0.0,
            "started_at":          disk.get("started_at"),
            "finished_at":         disk.get("finished_at"),
            "batches":             disk.get("batches", []),
            "download_ready":      disk.get("status") == "done",
            "download_url":        f"/merchant-download/{job_id}" if disk.get("status") == "done" else None,
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
        batches_total   = meta.get("batches_total", 0)
        batches_done    = meta.get("batches_done", 0)
        batches_failed  = meta.get("batches_failed", 0)
        batches_processed = batches_done + batches_failed
        total           = meta.get("total", 0)

        # Count merchants processed by reading per-batch sizes from metadata
        merchants_done = 0
        for b in meta.get("batches", []):
            if b.get("status") in ("done", "failed"):
                merchants_done += b.get("size", BATCH_SIZE)
        # Cap at total in case of rounding
        merchants_done = min(merchants_done, total)

        result.append({
            "job_id":             job_dir.name,
            "status":             meta.get("status"),
            "total_merchants":    total,
            "merchants_done":     merchants_done,
            "merchants_remaining": max(0, total - merchants_done),
            "batches_total":      batches_total,
            "batches_done":       batches_done,
            "batches_failed":     batches_failed,
            "progress_pct":       round(merchants_done / total * 100, 1) if total else 0.0,
            "started_at":         meta.get("started_at"),
            "finished_at":        meta.get("finished_at"),
            "download_url":       f"/merchant-download/{job_dir.name}" if meta.get("status") == "done" else None,
        })
    return result
