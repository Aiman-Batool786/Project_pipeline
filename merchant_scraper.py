"""
merchant_scraper.py — Batch-Safe Bulk Processor v3.7
─────────────────────────────────────────────────────
KEY FINDING (from debug output merchant 912058106):
  networkidle = REACHED   → page fully loaded
  html_size   = 257KB     → full page, not throttled, not blocked
  poll_result = null      → 60s polling found NOTHING
  
  This means: the page loaded completely BUT our JS selectors are not
  matching the actual DOM structure. The element shape must differ from
  what we expect.

  SOLUTION: Added _JS_DOM_DUMP that extracts ALL text containing "item"
  from the live DOM — this tells us EXACTLY what element shape AliExpress
  is using so we can fix the selector precisely.

  Also added broader fallback selectors:
  - Match "N item" (singular, no 's')  
  - Match inside elements with 'total' in their class/id
  - Match any element whose text is purely numeric followed by 'item'
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

BATCH_SIZE          = 20
CONCURRENCY         = 1
MAX_RETRIES         = 3
PAGE_TIMEOUT        = 90_000
NETWORKIDLE_TIMEOUT = 25_000
POLL_TIMEOUT_MS     = 60_000
DELAY_MIN           = 8.0
DELAY_MAX           = 20.0
THROTTLE_DELAY_MIN  = 30
THROTTLE_DELAY_MAX  = 60
THROTTLE_SIZE_KB    = 180
JOBS_DIR            = Path("./merchant_jobs")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]

ALIEXPRESS_LOCALE_COOKIES = [
    {"name": "aep_usuc_f",
     "value": "site=glo&c_tp=SEK&x_alimid=-&b_locale=en_US&ae_u_p_s=2",
     "domain": ".aliexpress.com", "path": "/"},
    {"name": "ali_apache_currency", "value": "EUR",
     "domain": ".aliexpress.com", "path": "/"},
    {"name": "ali_apache_lang",     "value": "en_US",
     "domain": ".aliexpress.com", "path": "/"},
    {"name": "intl_locale",         "value": "en_US",
     "domain": ".aliexpress.com", "path": "/"},
    {"name": "xman_us_f",           "value": "x_l=1&acs_rt=",
     "domain": ".aliexpress.com", "path": "/"},
    {"name": "aep_common_f",
     "value": "x_user_id=-&x_login_name=-&x_mbtype=&x_isnewuser=n",
     "domain": ".aliexpress.com", "path": "/"},
]

ALIEXPRESS_LOCALE_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "image/avif,image/webp,*/*;q=0.8",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
}

REAL_BLOCK_SIGNALS = [
    'id="baxia-punish"',
    'class="baxia-dialog"',
    'nc_iconfont btn_slide',
    'grecaptcha',
    'data-sitekey',
    'verify you are human',
    '<title>access denied</title>',
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
# HTML FALLBACK
# ─────────────────────────────────────────────────────────────────────────────

def _extract_item_count_from_html(html: str) -> Optional[int]:
    for pattern in [
        r'data-spm-anchor-id="[^"]*store_pc_allItems_or_groupList[^"]*"[^>]*>\s*(\d[\d,]*)\s*items?',
        r'data-spm-anchor-id="[^"]*store_pc_allItems[^"]*"[^>]*>\s*(\d[\d,]*)\s*items?',
        r'<span[^>]+font-size:\s*1[0-9]px[^>]*>\s*(\d[\d,]+)\s*items?\s*</span>',
        r'"(?:totalProducts|itemCount|totalItems|storeItemCount)"\s*:\s*(\d+)',
    ]:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(",", ""))

    all_matches = re.findall(r'\b(\d[\d,]+)\s+items?\b', html, re.IGNORECASE)
    if all_matches:
        nums = [int(x.replace(",", "")) for x in all_matches if int(x.replace(",", "")) > 0]
        if nums:
            return max(nums)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# DIAGNOSTIC JS — dumps what the DOM actually contains near "item"
# Used by /merchant-debug to show the EXACT element shape
# ─────────────────────────────────────────────────────────────────────────────

_JS_DOM_DUMP = """() => {
    const results = [];

    // 1. All elements whose text contains "item" (case insensitive)
    const all = document.querySelectorAll('span, div, p, h1, h2, h3, li');
    for (const el of all) {
        const t = el.textContent.trim();
        // Only leaf-like elements (short text, contains "item")
        if (t.length < 60 && /item/i.test(t) && t.length > 0) {
            results.push({
                tag:    el.tagName.toLowerCase(),
                text:   t,
                id:     el.id || '',
                cls:    el.className ? String(el.className).slice(0, 80) : '',
                anchor: el.getAttribute('data-spm-anchor-id') || '',
                style:  el.getAttribute('style') ? el.getAttribute('style').slice(0, 80) : ''
            });
            if (results.length >= 20) break;
        }
    }

    // 2. Also check script tags for JSON data with product counts
    const scriptMatches = [];
    for (const s of document.querySelectorAll('script')) {
        const src = s.textContent || '';
        const m = src.match(/"(?:totalProducts|itemCount|totalItems|storeItemCount|total)"\s*:\s*(\d+)/g);
        if (m) scriptMatches.push(...m.slice(0, 3));
    }

    return { dom_elements: results, script_matches: scriptMatches.slice(0, 10) };
}"""


# ─────────────────────────────────────────────────────────────────────────────
# PRIMARY JS EXTRACTOR — expanded to catch more element shapes
# ─────────────────────────────────────────────────────────────────────────────

_JS_EXTRACT_COUNT = """() => {
    // Shape 1: span with store_pc_allItems_or_groupList anchor
    for (const el of document.querySelectorAll(
            'span[data-spm-anchor-id*="store_pc_allItems_or_groupList"]')) {
        const m = el.textContent.trim().match(/(\\d[\\d,]*)\\s+items?/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    // Shape 2: span with store_pc_allItems anchor
    for (const el of document.querySelectorAll(
            'span[data-spm-anchor-id*="store_pc_allItems"]')) {
        const m = el.textContent.trim().match(/(\\d[\\d,]*)\\s+items?/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    // Shape 3: parent div has anchor, child span has text
    for (const div of document.querySelectorAll(
            'div[data-spm-anchor-id*="store_pc_allItems_or_groupList"],' +
            'div[data-spm-anchor-id*="store_pc_allItems"]')) {
        for (const span of div.querySelectorAll('span')) {
            const m = span.textContent.trim().match(/^(\\d[\\d,]*)\\s+items?$/i);
            if (m) return parseInt(m[1].replace(/,/g,''), 10);
        }
    }
    // Shape 4: any span whose full text is exactly "N items" or "N item"
    for (const el of document.querySelectorAll('span')) {
        const m = el.textContent.trim().match(/^(\\d[\\d,]*)\\s+items?$/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    // Shape 5: div direct text node is "N items"
    for (const el of document.querySelectorAll('div')) {
        const direct = Array.from(el.childNodes)
            .filter(n => n.nodeType === 3)
            .map(n => n.textContent.trim()).join('');
        const m = direct.match(/^(\\d[\\d,]*)\\s+items?$/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    // Shape 6: any element with class/id containing 'total' or 'count'
    for (const el of document.querySelectorAll('[class*="total"],[class*="count"],[id*="total"],[id*="count"]')) {
        const m = el.textContent.trim().match(/(\\d[\\d,]*)\\s+items?/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    // Shape 7: SSR JSON in script tags
    for (const s of document.querySelectorAll('script')) {
        const src = s.textContent || '';
        const m = src.match(
            /"(?:totalProducts|itemCount|totalItems|storeItemCount)"\s*:\\s*(\\d+)/
        );
        if (m) return parseInt(m[1], 10);
    }
    // Shape 8: broader text search — any element containing "N items" text
    // where N > 0. Last resort before giving up.
    const walker = document.createTreeWalker(
        document.body, NodeFilter.SHOW_TEXT, null
    );
    let node;
    while ((node = walker.nextNode())) {
        const t = node.textContent.trim();
        const m = t.match(/^(\\d[\\d,]*)\\s+items?$/i);
        if (m) {
            const val = parseInt(m[1].replace(/,/g,''), 10);
            if (val > 0) return val;
        }
    }
    return null;
}"""


# ─────────────────────────────────────────────────────────────────────────────
# DOM POLLING — same broad search, returns false to keep polling
# ─────────────────────────────────────────────────────────────────────────────

_JS_POLL_FOR_COUNT = """() => {
    for (const el of document.querySelectorAll(
            'span[data-spm-anchor-id*="store_pc_allItems_or_groupList"]')) {
        const m = el.textContent.trim().match(/(\\d[\\d,]*)\\s+items?/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    for (const el of document.querySelectorAll(
            'span[data-spm-anchor-id*="store_pc_allItems"]')) {
        const m = el.textContent.trim().match(/(\\d[\\d,]*)\\s+items?/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    for (const div of document.querySelectorAll(
            'div[data-spm-anchor-id*="store_pc_allItems_or_groupList"],' +
            'div[data-spm-anchor-id*="store_pc_allItems"]')) {
        for (const span of div.querySelectorAll('span')) {
            const m = span.textContent.trim().match(/^(\\d[\\d,]*)\\s+items?$/i);
            if (m) return parseInt(m[1].replace(/,/g,''), 10);
        }
    }
    for (const el of document.querySelectorAll('span')) {
        const m = el.textContent.trim().match(/^(\\d[\\d,]*)\\s+items?$/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    for (const el of document.querySelectorAll('div')) {
        const direct = Array.from(el.childNodes)
            .filter(n => n.nodeType === 3)
            .map(n => n.textContent.trim()).join('');
        const m = direct.match(/^(\\d[\\d,]*)\\s+items?$/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    for (const el of document.querySelectorAll('[class*="total"],[class*="count"],[id*="total"],[id*="count"]')) {
        const m = el.textContent.trim().match(/(\\d[\\d,]*)\\s+items?/i);
        if (m) return parseInt(m[1].replace(/,/g,''), 10);
    }
    for (const s of document.querySelectorAll('script')) {
        const src = s.textContent || '';
        const m = src.match(
            /"(?:totalProducts|itemCount|totalItems|storeItemCount)"\\s*:\\s*(\\d+)/
        );
        if (m) return parseInt(m[1], 10);
    }
    // TreeWalker: scan ALL text nodes
    const walker = document.createTreeWalker(
        document.body, NodeFilter.SHOW_TEXT, null
    );
    let node;
    while ((node = walker.nextNode())) {
        const t = node.textContent.trim();
        const m = t.match(/^(\\d[\\d,]*)\\s+items?$/i);
        if (m) {
            const val = parseInt(m[1].replace(/,/g,''), 10);
            if (val > 0) return val;
        }
    }
    return false;
}"""


def _wait_for_item_count(page, poll_timeout_ms: int = POLL_TIMEOUT_MS) -> Optional[int]:
    try:
        result = page.wait_for_function(
            _JS_POLL_FOR_COUNT,
            timeout=poll_timeout_ms,
            polling=100,
        )
        count = result.json_value()
        if isinstance(count, (int, float)) and count > 0:
            return int(count)
    except Exception as poll_err:
        logger.debug(f"[poll] timed out after {poll_timeout_ms}ms: {poll_err}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER CONTEXT FACTORY
# ─────────────────────────────────────────────────────────────────────────────

def _make_context(browser, ua: str):
    ctx = browser.new_context(
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        timezone_id="Europe/Stockholm",
        user_agent=ua,
        extra_http_headers=ALIEXPRESS_LOCALE_HEADERS,
    )
    ctx.add_cookies(ALIEXPRESS_LOCALE_COOKIES)
    return ctx


def _warmup_session(page) -> None:
    try:
        page.goto("https://www.aliexpress.com/", timeout=30_000,
                  wait_until="domcontentloaded")
        page.wait_for_timeout(random.randint(2_000, 4_000))
        page.mouse.move(random.randint(200, 800), random.randint(100, 500))
        page.wait_for_timeout(random.randint(500, 1_500))
    except Exception as e:
        logger.debug(f"[warmup] non-fatal: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE MERCHANT SCRAPER  v3.7
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_merchant(merchant_id: str) -> Dict:
    original_url = STORE_URL_TEMPLATE.format(merchant_id=merchant_id)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ua = random.choice(USER_AGENTS)
            with Camoufox(headless=True, os="windows") as browser:
                ctx  = _make_context(browser, ua)
                page = ctx.new_page()

                if attempt == 1:
                    _warmup_session(page)

                # ── Navigate ──────────────────────────────────────────────────
                try:
                    page.goto(original_url, timeout=PAGE_TIMEOUT,
                              wait_until="domcontentloaded")
                except Exception as nav_err:
                    err_str = str(nav_err)
                    if "NS_BINDING_ABORTED" in err_str or "ERR_ABORTED" in err_str:
                        logger.warning(f"[merchant] {merchant_id} nav aborted — continuing")
                    elif any(x in err_str for x in ["ERR_NAME_NOT_RESOLVED", "NS_ERROR"]):
                        page.close(); ctx.close()
                        return {"merchant_id": merchant_id, "total_items": None,
                                "error": "Page Not Found", "redirected_to": None}
                    else:
                        raise

                # ── ae:reload_path ────────────────────────────────────────────
                try:
                    reload_url = page.evaluate("""() => {
                        const m = document.querySelector('meta[property="ae:reload_path"]');
                        return m ? m.getAttribute('content') : null;
                    }""")
                    if reload_url and reload_url.strip() != page.url.strip():
                        logger.info(f"[merchant] {merchant_id} ae:reload_path → {reload_url}")
                        try:
                            page.goto(reload_url, timeout=PAGE_TIMEOUT,
                                      wait_until="domcontentloaded")
                        except Exception as re_err:
                            rs = str(re_err)
                            if "NS_BINDING_ABORTED" not in rs and "ERR_ABORTED" not in rs:
                                logger.warning(f"[merchant] {merchant_id} reload_path err: {rs[:80]}")
                except Exception:
                    pass

                # ── Detect redirect → extra wait ──────────────────────────────
                redirected_to = None
                m_redir = re.search(r'/store/(\d+)/', page.url)
                if m_redir and m_redir.group(1) != merchant_id:
                    redirected_to = m_redir.group(1)
                    logger.info(f"[merchant] {merchant_id} → {redirected_to} (redirect +5s wait)")
                    page.wait_for_timeout(5_000)

                # ── networkidle warm-up ───────────────────────────────────────
                try:
                    page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT)
                except Exception:
                    pass

                # ── Scroll ────────────────────────────────────────────────────
                for _ in range(3):
                    page.mouse.wheel(0, random.randint(500, 900))
                    page.wait_for_timeout(random.randint(300, 700))
                page.wait_for_timeout(random.randint(800, 1_500))

                # ── Poll DOM (60s) ────────────────────────────────────────────
                js_count = _wait_for_item_count(page, poll_timeout_ms=POLL_TIMEOUT_MS)

                if js_count is None:
                    try:
                        js_count = page.evaluate(_JS_EXTRACT_COUNT)
                    except Exception:
                        pass

                # Refresh redirect detection
                m_redir2 = re.search(r'/store/(\d+)/', page.url)
                if m_redir2 and m_redir2.group(1) != merchant_id:
                    redirected_to = m_redir2.group(1)

                html      = page.content()
                html_size = len(html)
                lower     = html.lower()
                page.close()
                ctx.close()

                # ── Block detection ───────────────────────────────────────────
                is_blocked = any(sig in lower for sig in REAL_BLOCK_SIGNALS)
                if is_blocked:
                    logger.warning(f"[merchant] {merchant_id} — CAPTCHA (attempt {attempt})")
                    if attempt < MAX_RETRIES:
                        time.sleep(random.uniform(THROTTLE_DELAY_MIN, THROTTLE_DELAY_MAX))
                        continue
                    return {"merchant_id": merchant_id, "total_items": None,
                            "error": "Blocked/CAPTCHA", "redirected_to": redirected_to}

                # ── Throttle detection ────────────────────────────────────────
                if js_count is None and html_size < THROTTLE_SIZE_KB * 1024:
                    logger.warning(
                        f"[merchant] {merchant_id} — throttled: {html_size//1024}KB "
                        f"(attempt {attempt})"
                    )
                    if attempt < MAX_RETRIES:
                        time.sleep(random.uniform(THROTTLE_DELAY_MIN, THROTTLE_DELAY_MAX))
                        continue
                    return {"merchant_id": merchant_id, "total_items": None,
                            "error": f"Throttled ({html_size//1024}KB)",
                            "redirected_to": redirected_to}

                # ── Success ───────────────────────────────────────────────────
                if js_count is not None:
                    logger.info(
                        f"[merchant] {merchant_id} ✓ {js_count} items"
                        + (f" → redir {redirected_to}" if redirected_to else "")
                    )
                    return {"merchant_id": merchant_id, "total_items": js_count,
                            "error": "", "redirected_to": redirected_to}

                count = _extract_item_count_from_html(html)
                if count is not None:
                    return {"merchant_id": merchant_id, "total_items": count,
                            "error": "", "redirected_to": redirected_to}

                if attempt < MAX_RETRIES:
                    logger.warning(
                        f"[merchant] {merchant_id} — no count found, "
                        f"{html_size//1024}KB, attempt {attempt}"
                    )
                    time.sleep(random.uniform(5, 10))
                    continue

                return {"merchant_id": merchant_id, "total_items": None,
                        "error": f"Selector Missing ({html_size//1024}KB)",
                        "redirected_to": redirected_to}

        except Exception as exc:
            err_str = str(exc)
            label   = "Timeout" if "timeout" in err_str.lower() else f"Error: {err_str[:80]}"
            logger.error(f"[merchant] {merchant_id} attempt {attempt} — {label}")
            if attempt < MAX_RETRIES:
                time.sleep(random.uniform(4, 10))
                continue
            return {"merchant_id": merchant_id, "total_items": None,
                    "error": label, "redirected_to": None}

    return {"merchant_id": merchant_id, "total_items": None,
            "error": "Max retries exceeded", "redirected_to": None}


# ─────────────────────────────────────────────────────────────────────────────
# BATCH WRITER
# ─────────────────────────────────────────────────────────────────────────────

def _write_batch_csv(job_id: str, batch_idx: int, rows: List[Dict]) -> None:
    path = _batch_path(job_id, batch_idx)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["MerchantID", "TotalItems", "RedirectedTo", "Error"])
        for row in rows:
            w.writerow([
                row.get("merchant_id", ""),
                "" if row.get("total_items") is None else row["total_items"],
                row.get("redirected_to") or "",
                row.get("error", ""),
            ])
    logger.info(f"[job:{job_id}] Batch {batch_idx:04d} → {path.name} ({len(rows)} rows)")


def _merge_batch_csvs(job_id: str, batches_total: int) -> Path:
    out_path = _output_path(job_id)
    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["MerchantID", "TotalItems", "RedirectedTo", "Error"])
        for idx in range(batches_total):
            bf = _batch_path(job_id, idx)
            if not bf.exists():
                continue
            with open(bf, newline="", encoding="utf-8") as in_f:
                reader = csv.reader(in_f)
                next(reader, None)
                for row in reader:
                    while len(row) < 4:
                        row.append("")
                    writer.writerow(row)
    logger.info(f"[job:{job_id}] Merged {batches_total} batches → output.csv")
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# BATCH RUNNER — SEQUENTIAL
# ─────────────────────────────────────────────────────────────────────────────

def _run_batch(job_id: str, batch_idx: int, merchant_ids: List[str]) -> None:
    logger.info(f"[job:{job_id}] Batch {batch_idx:04d} start — {len(merchant_ids)} merchants")
    rows = []
    for i, mid in enumerate(merchant_ids):
        try:
            row = _scrape_merchant(mid)
        except Exception as e:
            row = {"merchant_id": mid, "total_items": None,
                   "error": str(e)[:120], "redirected_to": None}
        rows.append(row)

        status = (f"✓ {row['total_items']}" if row.get("total_items") is not None
                  else f"✗ {row.get('error','?')[:40]}")
        logger.info(f"[job:{job_id}] [{i+1}/{len(merchant_ids)}] {mid} → {status}")

        if i < len(merchant_ids) - 1:
            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            time.sleep(delay)

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
        pct            = round(processed / batches_total * 100, 1)
        merchants_done = min(processed * BATCH_SIZE, total)
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

    logger.info(f"[job:{job_id}] ✓ Complete — {meta['batches_done']} ok | {meta['batches_failed']} failed")


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
