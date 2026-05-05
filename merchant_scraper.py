"""
merchant_scraper.py — Session API Edition v5.0
────────────────────────────────────────────────

HONEST ARCHITECTURE (confirmed from research):
──────────────────────────────────────────────
AliExpress MTOP/internal APIs require:
  1. Valid session cookies (from a real browser visit to aliexpress.com)
  2. Signed token in the request
  3. Browser-matching headers

Pure requests() with no session = returns empty/blocked silently.

SOLUTION — Hybrid Session Model:
  Step 1: Open a REAL browser ONCE on startup.
          Visit aliexpress.com homepage.
          Extract all session cookies into a requests.Session.
          Close the browser.

  Step 2: Use that requests.Session for ALL merchant API calls.
          No browser per merchant. Pure HTTP. ~1-3s per merchant.

  Step 3: Redirect detection via API response — if the API returns
          data for a different storeNum than requested → "ID Migrated".
          No browser redirect to chase.

  Step 4: Refresh session every SESSION_REFRESH_INTERVAL merchants
          (or on 403/empty responses) by re-running Step 1.

REDIRECT POLICY (supervisor requirement):
  If merchant ID is migrated → total_items=null, error="ID Migrated".
  We do NOT scrape the new ID.
"""

import re
import csv
import json
import time
import random
import logging
import threading
import io
import requests as req_lib
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime

from camoufox.sync_api import Camoufox

logger = logging.getLogger("merchant_scraper")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# AliExpress internal store count endpoints (tried in order)
# These are the XHR endpoints the browser calls — we call them directly
# after establishing a real browser session
STORE_API_ENDPOINTS = [
    # Primary: async count endpoint used by all-items page
    "https://www.aliexpress.com/store/async/merchandise/count.do"
    "?storeNum={merchant_id}&storeType=1&productOrigin=",

    # Secondary: aecommerce CDN-backed version
    "https://aecommerce.aliexpress.com/store/async/merchandise/count"
    "?storeNum={merchant_id}&storeType=1",

    # Tertiary: store search with count in response
    "https://www.aliexpress.com/store/async/merchandise.do"
    "?storeNum={merchant_id}&storeType=1&page=1&pageSize=1&sort=bestmatch_sort",
]

# Warmup page — what the browser visits to establish session
ALIEXPRESS_HOME = "https://www.aliexpress.com/"

# How many merchants to process before refreshing the browser session
SESSION_REFRESH_INTERVAL = 50

BATCH_SIZE          = 20
MAX_RETRIES         = 3
API_TIMEOUT         = 12       # seconds per requests call
SESSION_TIMEOUT     = 45_000   # ms for browser session warmup
DELAY_MIN           = 3.0      # shorter — no browser overhead per merchant
DELAY_MAX           = 8.0
THROTTLE_DELAY_MIN  = 20
THROTTLE_DELAY_MAX  = 45
JOBS_DIR            = Path("./merchant_jobs")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]

# JSON count field names to search in API responses (in priority order)
COUNT_JSON_FIELDS = [
    "totalResults", "totalProducts", "itemCount", "totalItems",
    "storeItemCount", "allProductCount", "productCount",
    "total", "count", "totalNum", "totalRecord", "totalCount",
]

_jobs: Dict[str, Dict] = {}
_jobs_lock = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL SESSION STATE
# One requests.Session shared across all merchant calls in a job.
# Refreshed periodically via browser warmup.
# ─────────────────────────────────────────────────────────────────────────────

_session:         Optional[req_lib.Session] = None
_session_ua:      str = USER_AGENTS[0]
_session_lock:    threading.Lock = threading.Lock()
_session_calls:   int = 0   # how many API calls made on current session


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
# SESSION MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def _build_browser_session() -> req_lib.Session:
    """
    Launch a real browser, visit AliExpress homepage to establish session,
    extract all cookies, and return a configured requests.Session.

    This is the key step that makes direct API calls work — AliExpress
    requires a real session (cookies + browser fingerprint) that was
    established by an actual browser visit.
    """
    ua = random.choice(USER_AGENTS)
    logger.info("[session] Building new browser session...")

    cookies_dict = {}

    try:
        with Camoufox(headless=True, os="windows") as browser:
            ctx = browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                timezone_id="Europe/Stockholm",
                user_agent=ua,
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                              "image/avif,image/webp,*/*;q=0.8",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "none",
                },
            )
            page = ctx.new_page()

            # Visit homepage to establish real session
            try:
                page.goto(ALIEXPRESS_HOME, timeout=SESSION_TIMEOUT,
                          wait_until="domcontentloaded")
                # Let the page settle and fire its init XHRs
                page.wait_for_timeout(random.randint(3_000, 5_000))
                # Small human-like interaction
                page.mouse.move(random.randint(300, 900), random.randint(200, 600))
                page.wait_for_timeout(random.randint(1_000, 2_000))
            except Exception as e:
                logger.warning(f"[session] Homepage visit error (non-fatal): {e}")

            # Extract all cookies from the browser context
            browser_cookies = ctx.cookies()
            for ck in browser_cookies:
                cookies_dict[ck["name"]] = ck["value"]

            page.close()
            ctx.close()

    except Exception as e:
        logger.error(f"[session] Browser session build failed: {e}")
        # Fall back to manual locale cookies if browser fails
        cookies_dict = {
            "aep_usuc_f": "site=glo&c_tp=EUR&x_alimid=-&b_locale=en_US&ae_u_p_s=2",
            "ali_apache_currency": "EUR",
            "ali_apache_lang": "en_US",
            "intl_locale": "en_US",
            "xman_us_f": "x_l=1&acs_rt=",
        }

    # Build requests.Session with the extracted cookies
    session = req_lib.Session()
    session.headers.update({
        "User-Agent": ua,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.aliexpress.com/",
        "Origin": "https://www.aliexpress.com",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest",
    })
    for name, value in cookies_dict.items():
        session.cookies.set(name, value, domain=".aliexpress.com")

    cookie_count = len(cookies_dict)
    logger.info(f"[session] Ready — {cookie_count} cookies, UA: {ua[:40]}...")
    return session, ua


def _get_session() -> req_lib.Session:
    """
    Return the current global session, building it if needed.
    Thread-safe.
    """
    global _session, _session_ua, _session_calls
    with _session_lock:
        if _session is None or _session_calls >= SESSION_REFRESH_INTERVAL:
            _session, _session_ua = _build_browser_session()
            _session_calls = 0
        return _session


def _refresh_session() -> None:
    """Force a new browser session (call on 403 / repeated failures)."""
    global _session, _session_ua, _session_calls
    with _session_lock:
        _session, _session_ua = _build_browser_session()
        _session_calls = 0


def _increment_session_calls() -> None:
    global _session_calls
    with _session_lock:
        _session_calls += 1


# ─────────────────────────────────────────────────────────────────────────────
# JSON COUNT EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def _extract_count_from_json(data: Any, depth: int = 0) -> Optional[int]:
    """Recursively find a count value in a parsed JSON object."""
    if depth > 8:
        return None
    if isinstance(data, dict):
        # Check known field names first (ordered by likelihood)
        for field in COUNT_JSON_FIELDS:
            if field in data:
                val = data[field]
                if isinstance(val, (int, float)) and 0 < val < 10_000_000:
                    return int(val)
                if isinstance(val, str) and val.isdigit() and 0 < int(val) < 10_000_000:
                    return int(val)
        # Recurse into nested dicts
        for v in data.values():
            if isinstance(v, (dict, list)):
                result = _extract_count_from_json(v, depth + 1)
                if result is not None:
                    return result
    elif isinstance(data, list):
        for item in data:
            result = _extract_count_from_json(item, depth + 1)
            if result is not None:
                return result
    return None


def _parse_api_response(body: str) -> Optional[int]:
    """
    Parse an API response body and extract item count.
    Handles: pure JSON, JSONP, raw regex fallback.
    """
    if not body or len(body) < 5:
        return None

    # 1. Pure JSON
    try:
        data = json.loads(body)
        count = _extract_count_from_json(data)
        if count is not None:
            return count
    except (json.JSONDecodeError, ValueError):
        pass

    # 2. JSONP: callback({...})
    jsonp = re.search(r'\w+\s*\(\s*(\{.+\})\s*\)\s*;?\s*$', body, re.DOTALL)
    if jsonp:
        try:
            data = json.loads(jsonp.group(1))
            count = _extract_count_from_json(data)
            if count is not None:
                return count
        except (json.JSONDecodeError, ValueError):
            pass

    # 3. Raw regex on the body string
    for field in COUNT_JSON_FIELDS:
        m = re.search(rf'"{field}"\s*:\s*"?(\d+)"?', body, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            if 0 < val < 10_000_000:
                return val

    return None


# ─────────────────────────────────────────────────────────────────────────────
# REDIRECT DETECTION IN API RESPONSE
# ─────────────────────────────────────────────────────────────────────────────

def _detect_migrated_id_in_response(body: str, merchant_id: str) -> Optional[str]:
    """
    Check if the API response contains a storeNum different from what we
    requested — this means the store was migrated to a new ID.
    Returns the new ID if migrated, None if same store.
    """
    # Look for storeNum, storeId, or sellerId fields in the response
    for field in ["storeNum", "storeId", "sellerId", "storeNo"]:
        m = re.search(rf'"{field}"\s*:\s*"?(\d+)"?', body, re.IGNORECASE)
        if m:
            found_id = m.group(1)
            if found_id != merchant_id:
                return found_id
    return None


# ─────────────────────────────────────────────────────────────────────────────
# CORE: DIRECT API CALL PER MERCHANT
# ─────────────────────────────────────────────────────────────────────────────

def _api_get_item_count(merchant_id: str) -> Dict:
    """
    Try each API endpoint using the shared session.
    If the store has migrated to a new ID, follow it automatically and
    still return the item count — recording the new ID in migrated_to.

    Output CSV columns:
      MerchantID  = original ID you requested
      TotalItems  = count from current store (even if migrated)
      MigratedTo  = new ID if store was migrated, empty if not
      Error       = "" on success
    """
    session    = _get_session()
    migrated_to: Optional[str] = None   # track if a redirect happened

    for endpoint_template in STORE_API_ENDPOINTS:
        # Use the migrated ID for the actual request if one was discovered
        active_id = migrated_to if migrated_to else merchant_id
        url = endpoint_template.format(merchant_id=active_id)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = session.get(
                    url,
                    timeout=API_TIMEOUT,
                    allow_redirects=True,   # let requests follow HTTP redirects
                )

                # ── Handle auth/rate-limit failures ───────────────────────────
                if resp.status_code == 403:
                    logger.warning(
                        f"[api] {merchant_id} 403 on attempt {attempt} — "
                        f"session may be expired"
                    )
                    if attempt == MAX_RETRIES:
                        _refresh_session()
                        session = _get_session()
                    time.sleep(random.uniform(2, 5))
                    continue

                if resp.status_code == 429:
                    logger.warning(f"[api] {merchant_id} 429 rate limited")
                    time.sleep(random.uniform(10, 20))
                    continue

                if resp.status_code != 200:
                    logger.debug(
                        f"[api] {merchant_id} HTTP {resp.status_code} "
                        f"from {url[:60]}"
                    )
                    break  # Try next endpoint

                body = resp.text

                # ── Detect migrated ID inside response body ───────────────────
                # If the API returned data for a different storeNum, note it
                # but still extract the count — don't stop.
                detected_id = _detect_migrated_id_in_response(body, merchant_id)
                if detected_id and not migrated_to:
                    migrated_to = detected_id
                    logger.info(
                        f"[api] {merchant_id} → store migrated to {migrated_to} "
                        f"— fetching count from new ID"
                    )
                    # Re-request using the new ID for the remaining endpoints
                    active_id = migrated_to
                    url = endpoint_template.format(merchant_id=active_id)
                    resp = session.get(url, timeout=API_TIMEOUT, allow_redirects=True)
                    if resp.status_code != 200:
                        break
                    body = resp.text

                # ── Extract count ──────────────────────────────────────────────
                count = _parse_api_response(body)
                if count is not None:
                    _increment_session_calls()
                    logger.info(
                        f"[api] {merchant_id} ✓ {count} items"
                        + (f" (migrated → {migrated_to})" if migrated_to else "")
                    )
                    return {
                        "merchant_id": merchant_id,
                        "total_items": count,
                        "error":       "",
                        "migrated_to": migrated_to,
                        "source":      "direct_api",
                    }

                logger.debug(
                    f"[api] {merchant_id} 200 but no count in: {body[:200]}"
                )
                break  # Try next endpoint

            except req_lib.Timeout:
                logger.debug(f"[api] {merchant_id} timeout on {url[:60]}")
                if attempt < MAX_RETRIES:
                    time.sleep(random.uniform(2, 4))
                    continue
                break

            except req_lib.RequestException as e:
                logger.debug(f"[api] {merchant_id} request error: {e}")
                break

    # All endpoints exhausted
    return {
        "merchant_id": merchant_id,
        "total_items": None,
        "error":       "API Failed — all endpoints returned no count",
        "migrated_to": migrated_to,  # include even on failure so CSV shows it
        "source":      None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE MERCHANT — MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_merchant(merchant_id: str) -> Dict:
    """
    Direct API call only. No browser per merchant.

    Result format:
      total_items  : int or None
      error        : "" (success) | "ID Migrated" | "API Failed..." | ...
      migrated_to  : str (new ID) if error=="ID Migrated", else None
      source       : "direct_api" | None
    """
    result = _api_get_item_count(merchant_id)
    result.setdefault("migrated_to", None)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# BATCH WRITER
# ─────────────────────────────────────────────────────────────────────────────

def _write_batch_csv(job_id: str, batch_idx: int, rows: List[Dict]) -> None:
    path = _batch_path(job_id, batch_idx)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["MerchantID", "TotalItems", "MigratedTo", "Error", "Source"])
        for row in rows:
            w.writerow([
                row.get("merchant_id", ""),
                "" if row.get("total_items") is None else row["total_items"],
                row.get("migrated_to") or "",
                row.get("error", ""),
                row.get("source") or "",
            ])
    logger.info(f"[job:{job_id}] Batch {batch_idx:04d} → {path.name} ({len(rows)} rows)")


def _merge_batch_csvs(job_id: str, batches_total: int) -> Path:
    out_path = _output_path(job_id)
    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["MerchantID", "TotalItems", "MigratedTo", "Error", "Source"])
        for idx in range(batches_total):
            bf = _batch_path(job_id, idx)
            if not bf.exists():
                continue
            with open(bf, newline="", encoding="utf-8") as in_f:
                reader = csv.reader(in_f)
                next(reader, None)
                for row in reader:
                    while len(row) < 5:
                        row.append("")
                    writer.writerow(row)
    logger.info(f"[job:{job_id}] Merged {batches_total} batches → output.csv")
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# BATCH RUNNER — SEQUENTIAL
# ─────────────────────────────────────────────────────────────────────────────

def _run_batch(job_id: str, batch_idx: int, merchant_ids: List[str]) -> None:
    logger.info(f"[job:{job_id}] Batch {batch_idx:04d} — {len(merchant_ids)} merchants")
    rows = []

    for i, mid in enumerate(merchant_ids):
        try:
            row = _scrape_merchant(mid)
        except Exception as e:
            row = {
                "merchant_id": mid, "total_items": None,
                "error": str(e)[:120], "migrated_to": None, "source": None,
            }
        rows.append(row)

        if row.get("total_items") is not None:
            status = f"✓ {row['total_items']} [direct_api]"
        elif row.get("error") == "ID Migrated":
            status = f"⚠ ID Migrated → {row.get('migrated_to', '?')}"
        else:
            status = f"✗ {row.get('error', '?')[:60]}"

        logger.info(f"[job:{job_id}] [{i+1}/{len(merchant_ids)}] {mid} → {status}")

        if i < len(merchant_ids) - 1:
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    _write_batch_csv(job_id, batch_idx, rows)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN JOB RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def _run_bulk_job(job_id: str, merchant_ids: List[str]) -> None:
    batches       = [merchant_ids[i:i+BATCH_SIZE]
                     for i in range(0, len(merchant_ids), BATCH_SIZE)]
    batches_total = len(batches)
    total         = len(merchant_ids)

    # Build session once before the job starts
    logger.info(f"[job:{job_id}] Initialising browser session...")
    _get_session()

    meta = {
        "job_id": job_id, "status": "running", "total": total,
        "batches_total": batches_total, "batches_done": 0, "batches_failed": 0,
        "started_at": datetime.utcnow().isoformat(), "finished_at": None,
        "batches": [
            {"idx": i, "size": len(b), "status": "queued"}
            for i, b in enumerate(batches)
        ],
    }
    _save_metadata(job_id, meta)

    with _jobs_lock:
        _jobs[job_id].update({
            "status": "running", "total": total,
            "batches_total": batches_total, "batches_done": 0, "batches_failed": 0,
        })

    logger.info(f"[job:{job_id}] Start — {total} merchants | {batches_total} batches")

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
        pct            = round(processed / batches_total * 100, 1)
        merchants_done = min(processed * BATCH_SIZE, total)
        logger.info(
            f"[job:{job_id}] {processed}/{batches_total} "
            f"({merchants_done}/{total}, {pct}%)"
        )

        if idx < batches_total - 1:
            time.sleep(random.uniform(5, 15))

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
# PUBLIC API (unchanged interface — main.py needs no changes)
# ─────────────────────────────────────────────────────────────────────────────

def start_bulk_job(job_id: str, merchant_ids: List[str]) -> None:
    with _jobs_lock:
        _jobs[job_id] = {
            "status": "queued", "total": len(merchant_ids),
            "batches_total": 0, "batches_done": 0, "batches_failed": 0,
        }
    t = threading.Thread(
        target=_run_bulk_job, args=(job_id, merchant_ids),
        daemon=True, name=f"merchant-{job_id[:8]}"
    )
    t.start()


def get_job_status(job_id: str) -> Optional[Dict]:
    with _jobs_lock:
        mem = dict(_jobs.get(job_id, {}))
    disk = _load_metadata(job_id)

    if not mem and not disk:
        return None

    if disk:
        total_merchants = disk.get("total", 0)
        merchants_done  = 0
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
            "progress_pct":        round(merchants_done / total_merchants * 100, 1)
                                   if total_merchants else 0.0,
            "started_at":          disk.get("started_at"),
            "finished_at":         disk.get("finished_at"),
            "batches":             disk.get("batches", []),
            "download_ready":      disk.get("status") == "done",
            "download_url":        f"/merchant-download/{job_id}"
                                   if disk.get("status") == "done" else None,
        }
    return mem


def is_job_done(job_id: str) -> bool:
    meta = _load_metadata(job_id)
    return meta is not None and meta.get("status") == "done"


def get_output_path(job_id: str) -> Optional[Path]:
    path = _output_path(job_id)
    return path if path.exists() else None


def list_all_jobs() -> List[Dict]:
    result = []
    if not JOBS_DIR.exists():
        return result
    for job_dir in sorted(JOBS_DIR.iterdir(), reverse=True):
        if not job_dir.is_dir():
            continue
        meta = _load_metadata(job_dir.name)
        if not meta:
            continue
        total          = meta.get("total", 0)
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
            "started_at":          meta.get("started_at"),
            "finished_at":         meta.get("finished_at"),
            "download_url":        f"/merchant-download/{job_dir.name}"
                                   if meta.get("status") == "done" else None,
        })
    return result


# ─────────────────────────────────────────────────────────────────────────────
# EXPORTS NEEDED BY main.py debug endpoint
# ─────────────────────────────────────────────────────────────────────────────

# Keep these so main.py imports don't break
STORE_URL_TEMPLATE  = "https://www.aliexpress.com/store/{merchant_id}"
USER_AGENTS         = USER_AGENTS
PAGE_TIMEOUT        = SESSION_TIMEOUT
NETWORKIDLE_TIMEOUT = 20_000
POLL_TIMEOUT_MS     = 25_000
REAL_BLOCK_SIGNALS  = [
    'id="baxia-punish"', 'class="baxia-dialog"', 'nc_iconfont btn_slide',
    'grecaptcha', 'data-sitekey', 'verify you are human',
    '<title>access denied</title>', 'cf-challenge-running',
]

def _make_context(browser, ua: str):
    return browser.new_context(
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        timezone_id="Europe/Stockholm",
        user_agent=ua,
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                      "image/avif,image/webp,*/*;q=0.8",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
        },
    )

def _extract_store_id_from_url(url: str) -> Optional[str]:
    m = re.search(r'/store/(\d+)', url)
    return m.group(1) if m else None

def _extract_item_count_from_html(html: str) -> Optional[int]:
    for field in COUNT_JSON_FIELDS:
        m = re.search(rf'"{field}"\s*:\s*"?(\d+)"?', html, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            if 0 < val < 10_000_000:
                return val
    all_m = re.findall(r'\b(\d[\d,]*)\s+items?\b', html, re.IGNORECASE)
    if all_m:
        nums = [int(x.replace(",", "")) for x in all_m
                if 0 < int(x.replace(",", "")) < 10_000_000]
        if nums:
            return max(nums)
    return None

_JS_DOM_DUMP = """() => {
    const results = [];
    const all = document.querySelectorAll('span, div, p, h1, h2, h3, li');
    for (const el of all) {
        const t = el.textContent.trim();
        if (t.length < 60 && /item/i.test(t) && t.length > 0) {
            results.push({ tag: el.tagName.toLowerCase(), text: t,
                id: el.id || '', cls: el.className ? String(el.className).slice(0,80) : '' });
            if (results.length >= 20) break;
        }
    }
    return { dom_elements: results, script_matches: [] };
}"""

def _is_count_api_response(url: str) -> bool:
    patterns = ["merchandise/count", "store/async/", "mtop.aliexpress"]
    return any(p in url.lower() for p in patterns)

def _try_parse_response_body(response) -> Optional[int]:
    try:
        return _parse_api_response(response.text())
    except Exception:
        return None

def _wait_for_item_count_textnode(page, poll_timeout_ms: int = 25_000) -> Optional[int]:
    js = """() => {
        if (!document.body) return false;
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
        let node;
        while ((node = walker.nextNode())) {
            const t = node.textContent.trim();
            const m = t.match(/^([\d,]+)\s+items?$/i);
            if (m) { const v = parseInt(m[1].replace(/,/g,''),10); if (v>0) return v; }
        }
        return false;
    }"""
    try:
        result = page.wait_for_function(js, timeout=poll_timeout_ms, polling=100)
        count  = result.json_value()
        return int(count) if isinstance(count, (int, float)) and count > 0 else None
    except Exception:
        return None
