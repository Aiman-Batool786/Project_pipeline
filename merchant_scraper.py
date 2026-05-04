"""
merchant_scraper.py — Network-Intercept Edition v4.0
──────────────────────────────────────────────────────

ROOT CAUSE ANALYSIS (v3.7 → v4.0):
  The item count is NOT in the initial HTML. AliExpress loads it via an
  XHR/Fetch API call AFTER React hydrates. The API URL pattern is:

    POST https://aecommerce.aliexpress.com/store/async/merchandise/count
    or
    GET  https://server.ilecdn.com/mtop/... (newer CDN-backed endpoint)

  Strategy (in priority order):
    1. INTERCEPT — register page.on("response") BEFORE navigation, capture
                   the JSON from the count API directly. Zero DOM dependency.
    2. POLL TEXT  — tree-walk every text node for /^\d[\d,]* items?$/i.
                   Works regardless of class/id/anchor changes.
    3. HTML REGEX — scan raw HTML for JSON fields or inline text patterns.

  Redirect fix:
    AliExpress 301-redirects old merchant IDs at the NETWORK level. We now
    treat the landing URL as the canonical source of truth and never mistake
    it for a "wrong" redirect — we record it as redirected_to but always
    process the page we actually landed on.
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
from typing import List, Dict, Optional, Any
from datetime import datetime

from camoufox.sync_api import Camoufox
from playwright.sync_api import Response as PlaywrightResponse

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
NETWORKIDLE_TIMEOUT = 20_000
POLL_TIMEOUT_MS     = 30_000   # Reduced — intercept usually wins in <5s
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

# XHR/Fetch URL fragments that carry the item count
# AliExpress uses several CDN/API hosts depending on region & A/B test
COUNT_API_PATTERNS = [
    "merchandise/count",
    "store/async/merchandise",
    "mtop.aliexpress.store.pc.shop.item.count",
    "mtop.aliexpress.store.page",
    "aecommerce.aliexpress.com",
    "/store/async/",
    "storeFront/count",
    "storeItemCount",
    "shop_itemcount",
    "shop-item-count",
]

# JSON field names that hold the total count in the intercepted response
COUNT_JSON_FIELDS = [
    "totalProducts", "itemCount", "totalItems", "storeItemCount",
    "total", "count", "totalResults", "productCount", "allProductCount",
    "totalNum", "totalRecord", "totalCount", "itemTotal",
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
# STRATEGY 1 — XHR / FETCH INTERCEPTION
# Register BEFORE page.goto() so we never miss the request.
# ─────────────────────────────────────────────────────────────────────────────

def _is_count_api_response(url: str) -> bool:
    """Return True if this response URL looks like the item-count endpoint."""
    url_lower = url.lower()
    return any(pat.lower() in url_lower for pat in COUNT_API_PATTERNS)


def _extract_count_from_json(data: Any, depth: int = 0) -> Optional[int]:
    """
    Recursively search a parsed JSON object for known count field names.
    Returns the first plausible value (> 0).
    """
    if depth > 8:
        return None

    if isinstance(data, dict):
        for field in COUNT_JSON_FIELDS:
            if field in data:
                val = data[field]
                if isinstance(val, (int, float)) and val > 0:
                    return int(val)
                if isinstance(val, str) and val.isdigit() and int(val) > 0:
                    return int(val)
        # Recurse into values
        for v in data.values():
            result = _extract_count_from_json(v, depth + 1)
            if result is not None:
                return result

    elif isinstance(data, list):
        for item in data:
            result = _extract_count_from_json(item, depth + 1)
            if result is not None:
                return result

    return None


def _try_parse_response_body(response: PlaywrightResponse) -> Optional[int]:
    """
    Safely read a Playwright response body and extract the item count.
    Handles JSON, JSONP, and embedded JSON in JS assignment strings.
    """
    try:
        body = response.text()
    except Exception:
        return None

    if not body or len(body) < 10:
        return None

    # 1. Pure JSON
    try:
        data = json.loads(body)
        count = _extract_count_from_json(data)
        if count is not None:
            return count
    except json.JSONDecodeError:
        pass

    # 2. JSONP: callback({...}) or mtopjsonp1({...})
    jsonp_match = re.search(r'\w+\s*\(\s*(\{.+\})\s*\)\s*;?\s*$', body, re.DOTALL)
    if jsonp_match:
        try:
            data = json.loads(jsonp_match.group(1))
            count = _extract_count_from_json(data)
            if count is not None:
                return count
        except json.JSONDecodeError:
            pass

    # 3. Inline regex fallback — grab any count-like field from raw text
    for field in COUNT_JSON_FIELDS:
        m = re.search(
            rf'"{field}"\s*:\s*"?(\d+)"?',
            body, re.IGNORECASE
        )
        if m:
            val = int(m.group(1))
            if val > 0:
                return val

    return None


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY 2 — DOM POLL (pure text-node walker, no class/anchor dependency)
# ─────────────────────────────────────────────────────────────────────────────

# This JS walks every text node in the document and looks for "N items" text.
# It does NOT rely on any CSS class, data attribute, or element ID.
_JS_TEXTNODE_POLL = """() => {
    if (!document.body) return false;
    const walker = document.createTreeWalker(
        document.body, NodeFilter.SHOW_TEXT, null
    );
    let node;
    while ((node = walker.nextNode())) {
        const t = node.textContent.trim();
        // Match: "1234 items" or "1,234 items" or "1234 item"
        const m = t.match(/^([\d,]+)\s+items?$/i);
        if (m) {
            const val = parseInt(m[1].replace(/,/g, ''), 10);
            if (val > 0) return val;
        }
    }
    // Also search script tags for SSR JSON (React server-side data)
    for (const s of document.querySelectorAll('script[type="application/json"], script')) {
        const src = s.textContent || '';
        if (src.length < 50) continue;
        const patterns = [
            /"(?:totalProducts|itemCount|totalItems|storeItemCount|totalResults|allProductCount)"\s*:\s*(\d+)/,
            /"total"\s*:\s*(\d+)/,
        ];
        for (const pat of patterns) {
            const m2 = src.match(pat);
            if (m2) {
                const v = parseInt(m2[1], 10);
                if (v > 0 && v < 10_000_000) return v;
            }
        }
    }
    return false;
}"""

# DOM dump for diagnostics — unchanged from v3.7 but harmless to keep
_JS_DOM_DUMP = """() => {
    const results = [];
    const all = document.querySelectorAll('span, div, p, h1, h2, h3, li');
    for (const el of all) {
        const t = el.textContent.trim();
        if (t.length < 60 && /item/i.test(t) && t.length > 0) {
            results.push({
                tag: el.tagName.toLowerCase(), text: t,
                id: el.id || '', cls: el.className ? String(el.className).slice(0, 80) : '',
                anchor: el.getAttribute('data-spm-anchor-id') || '',
                style: el.getAttribute('style') ? el.getAttribute('style').slice(0, 80) : ''
            });
            if (results.length >= 20) break;
        }
    }
    const scriptMatches = [];
    for (const s of document.querySelectorAll('script')) {
        const src = s.textContent || '';
        const m = src.match(/"(?:totalProducts|itemCount|totalItems|storeItemCount|total)"\s*:\s*(\d+)/g);
        if (m) scriptMatches.push(...m.slice(0, 3));
    }
    return { dom_elements: results, script_matches: scriptMatches.slice(0, 10) };
}"""


def _wait_for_item_count_textnode(page, poll_timeout_ms: int = POLL_TIMEOUT_MS) -> Optional[int]:
    """Poll the DOM every 100ms using the text-node walker — no selector dependency."""
    try:
        result = page.wait_for_function(
            _JS_TEXTNODE_POLL,
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
# STRATEGY 3 — RAW HTML REGEX FALLBACK
# ─────────────────────────────────────────────────────────────────────────────

def _extract_item_count_from_html(html: str) -> Optional[int]:
    """Last-resort: regex scan over the raw HTML string."""
    # JSON-embedded fields (SSR data, window.__initialData__, etc.)
    for field in COUNT_JSON_FIELDS:
        m = re.search(rf'"{field}"\s*:\s*"?(\d+)"?', html, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            if 0 < val < 10_000_000:
                return val

    # Visible text pattern: "N items" anywhere in the HTML
    all_matches = re.findall(r'\b(\d[\d,]*)\s+items?\b', html, re.IGNORECASE)
    if all_matches:
        nums = [int(x.replace(",", "")) for x in all_matches
                if 0 < int(x.replace(",", "")) < 10_000_000]
        if nums:
            return max(nums)

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
# REDIRECT HANDLING
# ─────────────────────────────────────────────────────────────────────────────

def _extract_store_id_from_url(url: str) -> Optional[str]:
    """Pull the store/merchant ID out of any AliExpress store URL."""
    m = re.search(r'/store/(\d+)', url)
    return m.group(1) if m else None


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE MERCHANT SCRAPER  v4.0
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_merchant(merchant_id: str) -> Dict:
    original_url = STORE_URL_TEMPLATE.format(merchant_id=merchant_id)

    for attempt in range(1, MAX_RETRIES + 1):
        intercepted_count: Optional[int] = None
        intercepted_url:   Optional[str] = None

        try:
            ua = random.choice(USER_AGENTS)
            with Camoufox(headless=True, os="windows") as browser:
                ctx  = _make_context(browser, ua)
                page = ctx.new_page()

                # ── STRATEGY 1 SETUP: Register network interceptor ─────────────
                # Must be set BEFORE page.goto() so we catch the very first
                # XHR/Fetch that fires during page load.
                def on_response(response: PlaywrightResponse) -> None:
                    nonlocal intercepted_count, intercepted_url
                    if intercepted_count is not None:
                        return  # Already found, no need to keep processing
                    try:
                        url = response.url
                        if not _is_count_api_response(url):
                            return
                        status = response.status
                        if status not in (200, 304):
                            return
                        count = _try_parse_response_body(response)
                        if count is not None:
                            intercepted_count = count
                            intercepted_url   = url
                            logger.debug(
                                f"[intercept] {merchant_id} ✓ {count} "
                                f"from {url[:80]}"
                            )
                    except Exception as e:
                        logger.debug(f"[intercept] handler error: {e}")

                page.on("response", on_response)

                # ── Warmup (attempt 1 only) ────────────────────────────────────
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

                # ── ae:reload_path meta-redirect ──────────────────────────────
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
                                logger.warning(
                                    f"[merchant] {merchant_id} reload_path err: {rs[:80]}"
                                )
                except Exception:
                    pass

                # ── Determine final store ID (redirect detection) ──────────────
                # AliExpress 301s old IDs to new ones. We accept this — the
                # count we extract is for the SAME store, just a new ID.
                # We record it in redirected_to but DO NOT treat it as an error.
                final_url    = page.url
                landed_id    = _extract_store_id_from_url(final_url)
                redirected_to = (
                    landed_id
                    if landed_id and landed_id != merchant_id
                    else None
                )
                if redirected_to:
                    logger.info(
                        f"[merchant] {merchant_id} → {redirected_to} "
                        f"(ID migration, continuing)"
                    )

                # ── networkidle (don't depend on it, just let it settle) ───────
                try:
                    page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT)
                except Exception:
                    pass

                # ── If intercept already got the count, we're done early ───────
                if intercepted_count is not None:
                    html      = page.content()
                    html_size = len(html)
                    lower     = html.lower()
                    page.close(); ctx.close()

                    if any(sig in lower for sig in REAL_BLOCK_SIGNALS):
                        logger.warning(f"[merchant] {merchant_id} — CAPTCHA (attempt {attempt})")
                        if attempt < MAX_RETRIES:
                            time.sleep(random.uniform(THROTTLE_DELAY_MIN, THROTTLE_DELAY_MAX))
                            continue
                        return {"merchant_id": merchant_id, "total_items": None,
                                "error": "Blocked/CAPTCHA", "redirected_to": redirected_to}

                    logger.info(
                        f"[merchant] {merchant_id} ✓ {intercepted_count} items "
                        f"(intercept)"
                        + (f" → redir {redirected_to}" if redirected_to else "")
                    )
                    return {
                        "merchant_id": merchant_id,
                        "total_items": intercepted_count,
                        "error": "",
                        "redirected_to": redirected_to,
                        "source": "intercept",
                    }

                # ── Scroll to trigger lazy loaders ────────────────────────────
                for _ in range(3):
                    page.mouse.wheel(0, random.randint(500, 900))
                    page.wait_for_timeout(random.randint(300, 600))
                page.wait_for_timeout(random.randint(800, 1_500))

                # ── Check again after scroll (intercept fires async) ──────────
                if intercepted_count is not None:
                    html      = page.content()
                    html_size = len(html)
                    lower     = html.lower()
                    page.close(); ctx.close()

                    logger.info(
                        f"[merchant] {merchant_id} ✓ {intercepted_count} items "
                        f"(post-scroll intercept)"
                        + (f" → redir {redirected_to}" if redirected_to else "")
                    )
                    return {
                        "merchant_id": merchant_id,
                        "total_items": intercepted_count,
                        "error": "",
                        "redirected_to": redirected_to,
                        "source": "intercept_post_scroll",
                    }

                # ── STRATEGY 2: Poll DOM text nodes ───────────────────────────
                dom_count = _wait_for_item_count_textnode(
                    page, poll_timeout_ms=POLL_TIMEOUT_MS
                )

                html      = page.content()
                html_size = len(html)
                lower     = html.lower()
                page.close(); ctx.close()

                # ── Block detection ───────────────────────────────────────────
                if any(sig in lower for sig in REAL_BLOCK_SIGNALS):
                    logger.warning(f"[merchant] {merchant_id} — CAPTCHA (attempt {attempt})")
                    if attempt < MAX_RETRIES:
                        time.sleep(random.uniform(THROTTLE_DELAY_MIN, THROTTLE_DELAY_MAX))
                        continue
                    return {"merchant_id": merchant_id, "total_items": None,
                            "error": "Blocked/CAPTCHA", "redirected_to": redirected_to}

                # ── Throttle detection ────────────────────────────────────────
                if dom_count is None and html_size < THROTTLE_SIZE_KB * 1024:
                    logger.warning(
                        f"[merchant] {merchant_id} — throttled: {html_size // 1024}KB "
                        f"(attempt {attempt})"
                    )
                    if attempt < MAX_RETRIES:
                        time.sleep(random.uniform(THROTTLE_DELAY_MIN, THROTTLE_DELAY_MAX))
                        continue
                    return {"merchant_id": merchant_id, "total_items": None,
                            "error": f"Throttled ({html_size // 1024}KB)",
                            "redirected_to": redirected_to}

                if dom_count is not None:
                    logger.info(
                        f"[merchant] {merchant_id} ✓ {dom_count} items (dom_poll)"
                        + (f" → redir {redirected_to}" if redirected_to else "")
                    )
                    return {
                        "merchant_id": merchant_id,
                        "total_items": dom_count,
                        "error": "",
                        "redirected_to": redirected_to,
                        "source": "dom_poll",
                    }

                # ── STRATEGY 3: Raw HTML regex ────────────────────────────────
                html_count = _extract_item_count_from_html(html)
                if html_count is not None:
                    logger.info(
                        f"[merchant] {merchant_id} ✓ {html_count} items (html_regex)"
                        + (f" → redir {redirected_to}" if redirected_to else "")
                    )
                    return {
                        "merchant_id": merchant_id,
                        "total_items": html_count,
                        "error": "",
                        "redirected_to": redirected_to,
                        "source": "html_regex",
                    }

                # ── All strategies exhausted ──────────────────────────────────
                if attempt < MAX_RETRIES:
                    logger.warning(
                        f"[merchant] {merchant_id} — no count, "
                        f"{html_size // 1024}KB, attempt {attempt}"
                    )
                    time.sleep(random.uniform(5, 10))
                    continue

                return {
                    "merchant_id": merchant_id,
                    "total_items": None,
                    "error": f"All strategies failed ({html_size // 1024}KB)",
                    "redirected_to": redirected_to,
                }

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
        w.writerow(["MerchantID", "TotalItems", "RedirectedTo", "Error", "Source"])
        for row in rows:
            w.writerow([
                row.get("merchant_id", ""),
                "" if row.get("total_items") is None else row["total_items"],
                row.get("redirected_to") or "",
                row.get("error", ""),
                row.get("source", ""),
            ])
    logger.info(f"[job:{job_id}] Batch {batch_idx:04d} → {path.name} ({len(rows)} rows)")


def _merge_batch_csvs(job_id: str, batches_total: int) -> Path:
    out_path = _output_path(job_id)
    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["MerchantID", "TotalItems", "RedirectedTo", "Error", "Source"])
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
    logger.info(f"[job:{job_id}] Batch {batch_idx:04d} start — {len(merchant_ids)} merchants")
    rows = []
    for i, mid in enumerate(merchant_ids):
        try:
            row = _scrape_merchant(mid)
        except Exception as e:
            row = {"merchant_id": mid, "total_items": None,
                   "error": str(e)[:120], "redirected_to": None}
        rows.append(row)

        status = (f"✓ {row['total_items']} [{row.get('source','?')}]"
                  if row.get("total_items") is not None
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
