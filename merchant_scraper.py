"""
merchant_scraper.py
────────────────────
Bulk Merchant ID processor for AliExpress store pages.

Workflow:
  1. Read MerchantID column from uploaded CSV
  2. Build store URL for each ID
  3. Visit each store page with Camoufox and extract total item count
  4. Write results to output CSV (MerchantID, TotalItems, Error)
  5. Support batch concurrency (5–10 at once), 3 retries, random delays

Store URL template:
  https://www.aliexpress.com/store/{merchantId}/pages/all-items.html?shop_sortType=bestmatch_sort

Item count selector:
  <span data-spm-anchor-id="a2g0o.store_pc_allItems_or_groupList...">32 items</span>
"""

import re
import csv
import time
import random
import logging
import threading
import io
from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from camoufox.sync_api import Camoufox

logger = logging.getLogger("merchant_scraper")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

STORE_URL_TEMPLATE = (
    "https://www.aliexpress.com/store/{merchant_id}/pages/all-items.html"
    "?shop_sortType=bestmatch_sort"
)

MAX_RETRIES       = 3
CONCURRENCY       = 5          # parallel browser threads
PAGE_TIMEOUT      = 45_000     # ms
DELAY_MIN         = 1.5        # seconds between batches
DELAY_MAX         = 3.5

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# ─────────────────────────────────────────────────────────────────────────────
# CSV PARSING
# ─────────────────────────────────────────────────────────────────────────────

def parse_merchant_csv(file_bytes: bytes) -> List[str]:
    """
    Read CSV bytes and return list of MerchantID strings.
    Accepts column names: MerchantID, merchantid, merchant_id, ID, id (case-insensitive).
    Skips blank rows and non-numeric IDs.
    """
    text    = file_bytes.decode("utf-8", errors="replace")
    reader  = csv.DictReader(io.StringIO(text))
    headers = [h.strip().lower() for h in (reader.fieldnames or [])]

    # Find which column holds merchant IDs
    id_col = None
    for candidate in ["merchantid", "merchant_id", "merchant id", "id", "store_id", "storeid"]:
        if candidate in headers:
            id_col = candidate
            break

    if id_col is None:
        raise ValueError(
            f"CSV must have a 'MerchantID' column. Found: {reader.fieldnames}"
        )

    ids = []
    for row in reader:
        raw = str(row.get(id_col, "") or "").strip()
        if raw and re.match(r"^\d+$", raw):
            ids.append(raw)

    logger.info(f"[merchant_scraper] Parsed {len(ids)} valid merchant IDs from CSV")
    return ids


# ─────────────────────────────────────────────────────────────────────────────
# ITEM COUNT EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def _extract_item_count_from_html(html: str) -> Optional[int]:
    """
    Extract item count from saved HTML using regex.

    Targets:
      <span data-spm-anchor-id="a2g0o.store_pc_allItems_or_groupList...">32 items</span>

    Fallbacks:
      - Any "N items" pattern near store context
      - "totalProducts" or "itemCount" in inline JSON
    """
    # Primary: spm anchor ID for store all-items count
    m = re.search(
        r'data-spm-anchor-id="[^"]*store_pc_allItems[^"]*"[^>]*>\s*(\d[\d,]*)\s*items?',
        html, re.IGNORECASE
    )
    if m:
        return int(m.group(1).replace(",", ""))

    # Secondary: any span/div containing "N items" near store page context
    # Only match if it's near a store-related class
    m2 = re.search(
        r'(?:store|allItems|groupList)[^>]*>[^<]*?(\d[\d,]+)\s*items?',
        html, re.IGNORECASE
    )
    if m2:
        return int(m2.group(1).replace(",", ""))

    # Broader: any "N items" text (use only if no other match)
    all_matches = re.findall(r'\b(\d[\d,]*)\s+items?\b', html, re.IGNORECASE)
    if all_matches:
        # Return the largest sensible value (avoid "1 items" noise)
        nums = [int(x.replace(",", "")) for x in all_matches]
        candidate = max(nums)
        if candidate > 0:
            return candidate

    # JSON fallback: "totalProducts" or "itemCount"
    m3 = re.search(r'"(?:totalProducts|itemCount|totalItems)"\s*:\s*(\d+)', html)
    if m3:
        return int(m3.group(1))

    return None


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE MERCHANT SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_merchant(merchant_id: str) -> Dict:
    """
    Visit the store page for one merchant ID and return:
      { merchant_id, total_items, error }

    Retries up to MAX_RETRIES times with exponential back-off.
    """
    url = STORE_URL_TEMPLATE.format(merchant_id=merchant_id)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"[merchant] {merchant_id} — attempt {attempt}/{MAX_RETRIES} → {url}")

            ua = random.choice(USER_AGENTS)

            with Camoufox(headless=True, os="windows") as browser:
                context = browser.new_context(
                    viewport={"width": 1440, "height": 900},
                    locale="en-US",
                    user_agent=ua,
                    extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                )
                page = context.new_page()

                try:
                    page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                except Exception as nav_err:
                    err_str = str(nav_err)
                    if "ERR_NAME_NOT_RESOLVED" in err_str or "NS_ERROR" in err_str:
                        return {
                            "merchant_id":  merchant_id,
                            "total_items":  None,
                            "error":        "Page Not Found",
                        }
                    raise

                # Short wait for JS to render item count
                page.wait_for_timeout(random.randint(3000, 5000))

                # Scroll to trigger lazy loading of the item count span
                for _ in range(2):
                    page.mouse.wheel(0, 600)
                    page.wait_for_timeout(500)

                html = page.content()
                page.close()
                context.close()

            # Detect blocked / CAPTCHA pages
            lower_html = html.lower()
            if any(k in lower_html for k in ["captcha", "robot", "verify you are human",
                                               "access denied", "blocked"]):
                logger.warning(f"[merchant] {merchant_id} — blocked/CAPTCHA on attempt {attempt}")
                if attempt < MAX_RETRIES:
                    time.sleep(random.uniform(5, 12))
                    continue
                return {"merchant_id": merchant_id, "total_items": None, "error": "Blocked"}

            # Detect 404 / invalid store
            if any(k in lower_html for k in ["page not found", "store not found",
                                              "404", "doesn't exist"]):
                return {
                    "merchant_id":  merchant_id,
                    "total_items":  None,
                    "error":        "Invalid Merchant",
                }

            count = _extract_item_count_from_html(html)

            if count is None:
                logger.warning(f"[merchant] {merchant_id} — selector missing (attempt {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(random.uniform(3, 7))
                    continue
                return {
                    "merchant_id":  merchant_id,
                    "total_items":  None,
                    "error":        "Selector Missing",
                }

            logger.info(f"[merchant] {merchant_id} — ✓ {count} items")
            return {"merchant_id": merchant_id, "total_items": count, "error": ""}

        except Exception as exc:
            err_msg = str(exc)
            logger.error(f"[merchant] {merchant_id} attempt {attempt} error: {err_msg}")

            # Classify common errors
            if "timeout" in err_msg.lower():
                error_label = "Timeout"
            elif "empty" in err_msg.lower():
                error_label = "Empty Response"
            else:
                error_label = f"Error: {err_msg[:80]}"

            if attempt < MAX_RETRIES:
                time.sleep(random.uniform(4, 10))
                continue

            return {"merchant_id": merchant_id, "total_items": None, "error": error_label}

    return {"merchant_id": merchant_id, "total_items": None, "error": "Max retries exceeded"}


# ─────────────────────────────────────────────────────────────────────────────
# BULK PROCESSOR
# ─────────────────────────────────────────────────────────────────────────────

# Global job registry  {job_id: {"status", "total", "done", "results", "error"}}
_jobs: Dict[str, Dict] = {}
_jobs_lock = threading.Lock()


def get_job_status(job_id: str) -> Optional[Dict]:
    with _jobs_lock:
        return _jobs.get(job_id)


def _run_bulk_job(job_id: str, merchant_ids: List[str]) -> None:
    """
    Background thread: processes all merchant IDs in batches with concurrency.
    Updates _jobs[job_id] in-place for progress polling.
    """
    results: List[Dict] = []
    total   = len(merchant_ids)

    with _jobs_lock:
        _jobs[job_id].update({"status": "running", "total": total, "done": 0, "results": []})

    logger.info(f"[job:{job_id}] Starting — {total} merchants | concurrency={CONCURRENCY}")

    # Process in batches to keep memory stable
    batch_size = CONCURRENCY
    for batch_start in range(0, total, batch_size):
        batch = merchant_ids[batch_start:batch_start + batch_size]

        with ThreadPoolExecutor(max_workers=batch_size) as pool:
            futures = {pool.submit(_scrape_merchant, mid): mid for mid in batch}
            for future in as_completed(futures):
                try:
                    row = future.result(timeout=240)
                except Exception as e:
                    mid = futures[future]
                    row = {"merchant_id": mid, "total_items": None, "error": str(e)[:120]}

                results.append(row)
                with _jobs_lock:
                    _jobs[job_id]["done"]    = len(results)
                    _jobs[job_id]["results"] = results

                logger.info(
                    f"[job:{job_id}] {len(results)}/{total} — "
                    f"{row['merchant_id']}: {row['total_items']} items | {row['error']}"
                )

        # Polite delay between batches (not after the last one)
        if batch_start + batch_size < total:
            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            logger.info(f"[job:{job_id}] Batch done. Sleeping {delay:.1f}s…")
            time.sleep(delay)

    with _jobs_lock:
        _jobs[job_id]["status"]  = "done"
        _jobs[job_id]["results"] = results

    logger.info(f"[job:{job_id}] ✓ Complete — {len(results)} merchants processed")


def start_bulk_job(job_id: str, merchant_ids: List[str]) -> None:
    """Launch bulk processing in a background daemon thread."""
    with _jobs_lock:
        _jobs[job_id] = {
            "status":  "queued",
            "total":   len(merchant_ids),
            "done":    0,
            "results": [],
        }
    t = threading.Thread(
        target=_run_bulk_job,
        args=(job_id, merchant_ids),
        daemon=True,
        name=f"merchant-job-{job_id}",
    )
    t.start()


# ─────────────────────────────────────────────────────────────────────────────
# CSV OUTPUT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_output_csv(results: List[Dict]) -> bytes:
    """
    Build the output CSV bytes from job results.

    Columns: MerchantID, TotalItems, Error
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["MerchantID", "TotalItems", "Error"])
    for row in results:
        writer.writerow([
            row.get("merchant_id", ""),
            row.get("total_items", "") if row.get("total_items") is not None else "",
            row.get("error", ""),
        ])
    return buf.getvalue().encode("utf-8")  
