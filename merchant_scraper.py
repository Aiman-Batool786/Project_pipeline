"""
merchant_scraper.py v4.0 — Final Clean Version
───────────────────────────────────────────────
DESIGN DECISIONS:
  1. NO redirect following — scrape ONLY the given merchant ID.
     If AliExpress redirects to a different store ID, we abort and
     return error="Redirected" so the caller knows the ID is stale.
     Supervisor requirement: only report data for the given ID.

  2. NO locale cookies — Sweden/EUR cookies were causing geo-redirects
     and serving empty product sections. Removed entirely.

  3. CONCURRENCY = 1 — sequential processing prevents concurrent-session
     bot detection that caused 76KB lite pages.

  4. 8-20s delays between merchants — human-like pacing.

  5. Bot-detection by html_size: pages < 150KB = lite/blocked page.
     Normal AliExpress store pages are 200-350KB.

  6. DOM polling 60s with TreeWalker fallback covers all element shapes.
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

BATCH_SIZE          = 20
CONCURRENCY         = 1        # sequential only
MAX_RETRIES         = 3
PAGE_TIMEOUT        = 90_000   # 90s — covers slow pages
NETWORKIDLE_TIMEOUT = 25_000
POLL_TIMEOUT_MS     = 60_000   # 60s DOM polling
DELAY_MIN           = 8.0      # seconds between merchants
DELAY_MAX           = 20.0
BLOCKED_SIZE_BYTES  = 150_000  # pages under 150KB = bot-detection lite page
JOBS_DIR            = Path("./merchant_jobs")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# Minimal headers — no locale, no country forcing
BASE_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
}

REAL_BLOCK_SIGNALS = [
    'id="baxia-punish"', 'class="baxia-dialog"',
    'nc_iconfont btn_slide', 'grecaptcha', 'data-sitekey',
    'verify you are human', '<title>access denied</title>',
    'cf-challenge-running',
]

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
# JS EXTRACTORS
# ─────────────────────────────────────────────────────────────────────────────

# Diagnostic: shows every element containing "item" in the live DOM
_JS_DOM_DUMP = """() => {
    const results = [];
    const all = document.querySelectorAll('span, div, p, h1, h2, h3, li');
    for (const el of all) {
        const t = el.textContent.trim();
        if (t.length < 80 && /item/i.test(t) && t.length > 0) {
            results.push({
                tag: el.tagName.toLowerCase(), text: t,
                anchor: el.getAttribute('data-spm-anchor-id') || '',
                cls: el.className ? String(el.className).slice(0,60) : ''
            });
            if (results.length >= 15) break;
        }
    }
    const scriptMatches = [];
    for (const s of document.querySelectorAll('script')) {
        const m = (s.textContent||'').match(
            /"(?:totalProducts|itemCount|totalItems|storeItemCount)"\s*:\s*(\d+)/g
        );
        if (m) scriptMatches.push(...m.slice(0,3));
    }
    return {dom_elements: results, script_matches: scriptMatches.slice(0,5)};
}"""

# Poll function — returns count or false (keeps polling)
_JS_POLL_FOR_COUNT = """() => {
    // Anchor-based selectors (confirmed DOM shapes)
    for (const el of document.querySelectorAll(
            'span[data-spm-anchor-id*="store_pc_allItems_or_groupList"],' +
            'span[data-spm-anchor-id*="store_pc_allItems"]')) {
        const m = el.textContent.trim().match(/(\\d[\\d,]*)\\s+items?/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    // Parent div has anchor, child span has text
    for (const div of document.querySelectorAll(
            'div[data-spm-anchor-id*="store_pc_allItems_or_groupList"],' +
            'div[data-spm-anchor-id*="store_pc_allItems"]')) {
        for (const span of div.querySelectorAll('span')) {
            const m = span.textContent.trim().match(/^(\\d[\\d,]*)\\s+items?$/i);
            if (m) return parseInt(m[1].replace(/,/g,''), 10);
        }
    }
    // Any span/div whose full text is "N items"
    for (const el of document.querySelectorAll('span, div')) {
        const t = el.textContent.trim();
        const m = t.match(/^(\\d[\\d,]*)\\s+items?$/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    // Class/id hints
    for (const el of document.querySelectorAll(
            '[class*="total"],[class*="count"],[id*="total"],[id*="count"]')) {
        const m = el.textContent.trim().match(/(\\d[\\d,]*)\\s+items?/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    // SSR JSON in script tags
    for (const s of document.querySelectorAll('script')) {
        const m = (s.textContent||'').match(
            /"(?:totalProducts|itemCount|totalItems|storeItemCount)"\\s*:\\s*(\\d+)/
        );
        if (m) return parseInt(m[1], 10);
    }
    // TreeWalker: every text node in the document
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
    let node;
    while ((node = walker.nextNode())) {
        const t = node.textContent.trim();
        const m = t.match(/^(\\d[\\d,]*)\\s+items?$/i);
        if (m) { const v = parseInt(m[1].replace(/,/g,''), 10); if (v > 0) return v; }
    }
    return false;
}"""


def _wait_for_item_count(page, poll_timeout_ms: int = POLL_TIMEOUT_MS) -> Optional[int]:
    try:
        result = page.wait_for_function(
            _JS_POLL_FOR_COUNT, timeout=poll_timeout_ms, polling=100
        )
        count = result.json_value()
        if isinstance(count, (int, float)) and count > 0:
            return int(count)
    except Exception as e:
        logger.debug(f"[poll] timeout: {e}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER CONTEXT — no locale cookies, blocks navigation to other store IDs
# ─────────────────────────────────────────────────────────────────────────────

def _make_context(browser, ua: str, merchant_id: str):
    """
    Context that aborts navigation to any store ID other than merchant_id.
    This enforces "scrape only the given ID" at the network level.
    """
    ctx = browser.new_context(
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        user_agent=ua,
        extra_http_headers=BASE_HEADERS,
    )

    def _route_handler(route):
        url = route.request.url
        # Only intercept full-page navigations to store pages
        if "/store/" in url and "pages/all-items" in url:
            m = re.search(r'/store/(\d+)/', url)
            if m and m.group(1) != merchant_id:
                logger.info(
                    f"[block] {merchant_id}: blocked navigation to "
                    f"store/{m.group(1)} — staying on original ID"
                )
                route.abort("blockedbyclient")
                return
        route.continue_()

    ctx.route("**/*", _route_handler)
    return ctx


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE MERCHANT SCRAPER  v4.0
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_merchant(merchant_id: str) -> Dict:
    url = STORE_URL_TEMPLATE.format(merchant_id=merchant_id)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ua = random.choice(USER_AGENTS)
            with Camoufox(headless=True, os="windows") as browser:
                ctx  = _make_context(browser, ua, merchant_id)
                page = ctx.new_page()

                # ── Navigate ──────────────────────────────────────────────────
                nav_ok = False
                try:
                    page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                    nav_ok = True
                except Exception as nav_err:
                    err_str = str(nav_err)
                    if "blockedbyclient" in err_str or "ERR_BLOCKED" in err_str:
                        # Our route handler blocked a redirect — page may still have content
                        logger.info(f"[merchant] {merchant_id} redirect blocked, checking DOM")
                        nav_ok = True
                    elif "NS_BINDING_ABORTED" in err_str or "ERR_ABORTED" in err_str:
                        nav_ok = True
                    elif any(x in err_str for x in ["ERR_NAME_NOT_RESOLVED", "NS_ERROR_UNKNOWN_HOST"]):
                        page.close(); ctx.close()
                        return {"merchant_id": merchant_id, "total_items": None,
                                "error": "DNS failed / Page not found"}
                    else:
                        raise

                # ── Verify we stayed on the right store ID ────────────────────
                current_url = page.url
                m_cur = re.search(r'/store/(\d+)/', current_url)
                if m_cur and m_cur.group(1) != merchant_id:
                    # JS-driven navigation happened after domcontentloaded
                    # Navigate back to original
                    logger.info(
                        f"[merchant] {merchant_id} JS-redirected to "
                        f"{m_cur.group(1)}, navigating back"
                    )
                    try:
                        page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                    except Exception:
                        pass

                # ── networkidle ───────────────────────────────────────────────
                try:
                    page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT)
                except Exception:
                    pass

                # ── Human-like scroll ─────────────────────────────────────────
                for _ in range(3):
                    page.mouse.wheel(0, random.randint(400, 800))
                    page.wait_for_timeout(random.randint(300, 600))
                page.wait_for_timeout(random.randint(800, 1_500))

                # ── Final URL check ───────────────────────────────────────────
                final_url = page.url
                m_final   = re.search(r'/store/(\d+)/', final_url)
                final_id  = m_final.group(1) if m_final else merchant_id

                if final_id != merchant_id:
                    # Still on wrong store after all attempts to stay put
                    logger.warning(
                        f"[merchant] {merchant_id} ended on store/{final_id} "
                        f"— supervisor rule: report as Redirected"
                    )
                    page.close(); ctx.close()
                    return {
                        "merchant_id":  merchant_id,
                        "total_items":  None,
                        "error":        f"Redirected to {final_id} (stale ID)",
                        "redirected_to": final_id,
                    }

                # ── Poll DOM (60s) ────────────────────────────────────────────
                js_count = _wait_for_item_count(page, poll_timeout_ms=POLL_TIMEOUT_MS)

                # Check page size and block signals
                html      = page.content()
                html_size = len(html)
                lower     = html.lower()
                page.close()
                ctx.close()

                # Real CAPTCHA/block
                is_blocked = any(sig in lower for sig in REAL_BLOCK_SIGNALS)
                if is_blocked:
                    logger.warning(f"[merchant] {merchant_id} CAPTCHA (attempt {attempt})")
                    if attempt < MAX_RETRIES:
                        time.sleep(random.uniform(30, 60))
                        continue
                    return {"merchant_id": merchant_id, "total_items": None,
                            "error": "Blocked/CAPTCHA"}

                # Silent bot-detection: page too small
                if js_count is None and html_size < BLOCKED_SIZE_BYTES:
                    logger.warning(
                        f"[merchant] {merchant_id} lite page {html_size//1024}KB "
                        f"(bot detection, attempt {attempt})"
                    )
                    if attempt < MAX_RETRIES:
                        sleep_t = random.uniform(30, 60)
                        logger.info(f"[merchant] {merchant_id} sleeping {sleep_t:.0f}s")
                        time.sleep(sleep_t)
                        continue
                    return {"merchant_id": merchant_id, "total_items": None,
                            "error": f"Bot-detection lite page ({html_size//1024}KB)"}

                if js_count is not None:
                    logger.info(f"[merchant] {merchant_id} ✓ {js_count} items")
                    return {"merchant_id": merchant_id, "total_items": js_count,
                            "error": ""}

                # Normal retry
                if attempt < MAX_RETRIES:
                    logger.warning(
                        f"[merchant] {merchant_id} no count, {html_size//1024}KB, "
                        f"attempt {attempt}"
                    )
                    time.sleep(random.uniform(5, 15))
                    continue

                return {"merchant_id": merchant_id, "total_items": None,
                        "error": f"Selector missing ({html_size//1024}KB)"}

        except Exception as exc:
            err_str = str(exc)
            label   = "Timeout" if "timeout" in err_str.lower() else f"Error: {err_str[:80]}"
            logger.error(f"[merchant] {merchant_id} attempt {attempt} — {label}")
            if attempt < MAX_RETRIES:
                time.sleep(random.uniform(5, 15))
                continue
            return {"merchant_id": merchant_id, "total_items": None, "error": label}

    return {"merchant_id": merchant_id, "total_items": None,
            "error": "Max retries exceeded"}


# ─────────────────────────────────────────────────────────────────────────────
# BATCH WRITER
# ─────────────────────────────────────────────────────────────────────────────

def _write_batch_csv(job_id: str, batch_idx: int, rows: List[Dict]) -> None:
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
                next(reader, None)
                for row in reader:
                    while len(row) < 3:
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
            row = {"merchant_id": mid, "total_items": None, "error": str(e)[:120]}
        rows.append(row)

        status = (f"✓ {row['total_items']}" if row.get("total_items") is not None
                  else f"✗ {row.get('error','')[:40]}")
        logger.info(f"[job:{job_id}] [{i+1}/{len(merchant_ids)}] {mid} → {status}")

        if i < len(merchant_ids) - 1:
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    _write_batch_csv(job_id, batch_idx, rows)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN JOB RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def _run_bulk_job(job_id: str, merchant_ids: List[str]) -> None:
    batches       = [merchant_ids[i:i+BATCH_SIZE] for i in range(0, len(merchant_ids), BATCH_SIZE)]
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
        merchants_done = min(processed * BATCH_SIZE, total)
        pct            = round(processed / batches_total * 100, 1)
        logger.info(f"[job:{job_id}] {processed}/{batches_total} ({merchants_done}/{total}, {pct}%)")

        if idx < batches_total - 1:
            time.sleep(random.uniform(15, 30))

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
    with _jobs_lock:
        _jobs[job_id] = {"status": "queued", "total": len(merchant_ids),
                         "batches_total": 0, "batches_done": 0, "batches_failed": 0}
    t = threading.Thread(target=_run_bulk_job, args=(job_id, merchant_ids),
                         daemon=True, name=f"merchant-{job_id[:8]}")
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
