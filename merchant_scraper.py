"""
merchant_scraper.py v5.0 — Alias-Redirect + 3-Layer Extraction
───────────────────────────────────────────────────────────────
CHANGES FROM v4.0:
  1. REDIRECT = ALIAS, NOT FATAL
     AliExpress canonicalises old store IDs to new ones.
     We now scrape the landed page and return BOTH IDs.
     Null is never returned just because the store ID changed.

  2. THREE-LAYER EXTRACTION WATERFALL
     Layer 1 — JSON API response bodies (XHR / fetch intercept)
     Layer 2 — Hydrated global state (window.runParams, __NEXT_DATA__, etc.)
     Layer 3 — Broad DOM text fallback (TreeWalker + relaxed regex)

  3. NO REDIRECT ABORTING IN ROUTE HANDLER
     The old route handler that called route.abort() on redirects
     was breaking page hydration. Now it only observes / logs.

  4. SCREENSHOT ON FAILURE
     Any scrape that ends without a count saves a PNG screenshot
     so you can see exactly what the browser saw.
"""

import re
import csv
import json
import time
import random
import base64
import logging
import threading
import io
from pathlib import Path
from typing import List, Dict, Optional, Any
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
BLOCKED_SIZE_BYTES  = 150_000
JOBS_DIR            = Path("./merchant_jobs")
SCREENSHOTS_DIR     = Path("./merchant_screenshots")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

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


def _save_screenshot(page, merchant_id: str, label: str = "debug") -> Optional[str]:
    """
    Save a PNG screenshot and return the file path (relative).
    Returns None on any error so it never breaks the main flow.
    """
    try:
        SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        ts       = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{merchant_id}_{label}_{ts}.png"
        path     = SCREENSHOTS_DIR / filename
        page.screenshot(path=str(path), full_page=False)
        logger.info(f"[screenshot] saved → {path}")
        return str(path)
    except Exception as e:
        logger.debug(f"[screenshot] failed: {e}")
        return None


def _screenshot_to_b64(path: Optional[str]) -> Optional[str]:
    """Read a saved screenshot PNG and return base64 string."""
    if not path:
        return None
    try:
        return base64.b64encode(Path(path).read_bytes()).decode()
    except Exception:
        return None


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
# JSON HELPERS — Layer 1 & 2 extraction
# ─────────────────────────────────────────────────────────────────────────────

COUNT_HINT_RE = re.compile(
    r"(item|items|product|products|goods|result|results).{0,20}(count|total|num|size)"
    r"|"
    r"(count|total|num|size).{0,20}(item|items|product|products|goods|result|results)",
    re.I,
)
ID_HINT_RE = re.compile(r"(store|shop|seller|merchant).{0,10}id", re.I)


def _walk_json(obj: Any, path: str = "$", out: Optional[list] = None) -> list:
    if out is None:
        out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}"
            if isinstance(v, (dict, list)):
                _walk_json(v, p, out)
            else:
                out.append({"path": p, "value": v})
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            p = f"{path}[{i}]"
            if isinstance(v, (dict, list)):
                _walk_json(v, p, out)
            else:
                out.append({"path": p, "value": v})
    return out


def _best_count_from_json(data: Any, expected_ids: Optional[set] = None) -> Optional[Dict]:
    expected_ids = set(expected_ids or [])
    flat = _walk_json(data)

    ids_found: set = set()
    for item in flat:
        p = item["path"].lower()
        v = item["value"]
        if ID_HINT_RE.search(p) and str(v).isdigit():
            ids_found.add(str(v))

    candidates = []
    for item in flat:
        p = item["path"].lower()
        v = item["value"]

        if isinstance(v, str) and v.isdigit():
            v = int(v)
        if not isinstance(v, int):
            continue
        if v <= 0 or v > 500_000:
            continue

        score = 0
        if COUNT_HINT_RE.search(p):
            score += 10
        if any(x in p for x in ["item", "product", "goods", "result"]):
            score += 4
        if any(x in p for x in ["count", "total", "num", "size"]):
            score += 4
        if expected_ids and (expected_ids & ids_found):
            score += 2

        if score >= 10:
            candidates.append({
                "count": v,
                "path":  item["path"],
                "score": score,
                "matched_ids": sorted(expected_ids & ids_found),
            })

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x["score"], x["count"]), reverse=True)
    return candidates[0]


def _extract_store_id(url: str) -> Optional[str]:
    m = re.search(r'/store/(\d+)/', url or "")
    return m.group(1) if m else None


# ─────────────────────────────────────────────────────────────────────────────
# JS EXTRACTORS
# ─────────────────────────────────────────────────────────────────────────────

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

# Broadened poll — covers "324 items", "Products: 324", "324 results", etc.
_JS_POLL_FOR_COUNT = r"""() => {
    const patterns = [
        /(\d[\d,]*)\s*(?:items?|products?|results?)\b/i,
        /\b(?:items?|products?|results?)\s*[:(]?\s*(\d[\d,]*)\b/i,
    ];

    const norm = (s) => (s || "").replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();

    const tryText = (txt) => {
        txt = norm(txt);
        for (const re of patterns) {
            const m = txt.match(re);
            if (m) {
                const v = parseInt((m[1] || m[2]).replace(/,/g, ""), 10);
                if (v > 0) return v;
            }
        }
        return null;
    };

    // Anchor-based selectors (most specific first)
    for (const el of document.querySelectorAll(
            'span[data-spm-anchor-id*="store_pc_allItems_or_groupList"],' +
            'span[data-spm-anchor-id*="store_pc_allItems"]')) {
        const v = tryText(el.textContent);
        if (v) return v;
    }
    for (const div of document.querySelectorAll(
            'div[data-spm-anchor-id*="store_pc_allItems_or_groupList"],' +
            'div[data-spm-anchor-id*="store_pc_allItems"]')) {
        for (const span of div.querySelectorAll('span')) {
            const v = tryText(span.textContent);
            if (v) return v;
        }
    }

    // Class/id hints
    for (const el of document.querySelectorAll(
            '[class*="total"],[class*="count"],[id*="total"],[id*="count"]')) {
        const v = tryText(el.textContent);
        if (v) return v;
    }

    // Broad element scan
    for (const el of document.querySelectorAll('span, div, p, h1, h2, h3, li, strong, b')) {
        const v = tryText(el.textContent);
        if (v) return v;
    }

    // SSR JSON in script tags — extended key list
    for (const s of document.querySelectorAll('script')) {
        const m = (s.textContent||'').match(
            /"(?:totalProducts|itemCount|totalItems|storeItemCount|productCount|goodsCount|total)"\s*:\s*(\d+)/
        );
        if (m) return parseInt(m[1], 10);
    }

    // TreeWalker — every text node
    const walker = document.createTreeWalker(
        document.body || document.documentElement, NodeFilter.SHOW_TEXT, null
    );
    let node;
    while ((node = walker.nextNode())) {
        const v = tryText(node.textContent);
        if (v) return v;
    }
    return false;
}"""

# Layer 2: scan hydrated globals
_JS_GLOBAL_CANDIDATES = r"""
(expectedIds) => {
    const names = [
        "runParams", "__NEXT_DATA__", "__INITIAL_STATE__",
        "__PRELOADED_STATE__", "_page_config_"
    ];

    const countRe = /(item|items|product|products|goods|result|results).{0,20}(count|total|num|size)|(count|total|num|size).{0,20}(item|items|product|products|goods|result|results)/i;
    const idRe    = /(store|shop|seller|merchant).{0,10}id/i;

    const hits = [];
    const seen = new WeakSet();

    function walk(obj, path, meta) {
        if (!obj || typeof obj !== "object") return;
        if (seen.has(obj)) return;
        seen.add(obj);

        if (Array.isArray(obj)) {
            obj.forEach((v, i) => {
                if (v && typeof v === "object") walk(v, `${path}[${i}]`, meta);
                else if (typeof v === "number") {
                    const p = `${path}[${i}]`;
                    if (countRe.test(p) && v > 0 && v < 500000) {
                        hits.push({ source: path.split(".")[0], path: p, count: v, score: 10, matched_store_id: meta.storeId || null });
                    }
                }
            });
            return;
        }

        const nextMeta = { ...meta };
        for (const [k, v] of Object.entries(obj)) {
            const p = `${path}.${k}`;
            if (idRe.test(k) && (typeof v === "string" || typeof v === "number")) {
                nextMeta.storeId = String(v);
            }
            if (typeof v === "number") {
                let score = 0;
                if (countRe.test(p)) score += 10;
                if (nextMeta.storeId && expectedIds.includes(nextMeta.storeId)) score += 3;
                if (score >= 10 && v > 0 && v < 500000) {
                    hits.push({ source: path.split(".")[0], path: p, count: v, score, matched_store_id: nextMeta.storeId || null });
                }
            } else if (v && typeof v === "object") {
                walk(v, p, nextMeta);
            }
        }
    }

    for (const n of names) {
        try {
            if (window[n] && typeof window[n] === "object") {
                walk(window[n], n, {});
            }
        } catch(e) {}
    }

    hits.sort((a, b) => b.score - a.score);
    return hits.slice(0, 10);
}
"""


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
# BROWSER CONTEXT — observe-only, no blocking
# ─────────────────────────────────────────────────────────────────────────────

def _make_context(browser, ua: str, merchant_id: str = ""):
    """
    Context that OBSERVES store navigation but never aborts it.
    Aborting redirected store requests was breaking page hydration.
    """
    ctx = browser.new_context(
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        user_agent=ua,
        extra_http_headers=BASE_HEADERS,
    )

    def _route_handler(route):
        url = route.request.url
        if "/store/" in url and "pages/all-items" in url:
            m = re.search(r'/store/(\d+)/', url)
            if m and merchant_id and m.group(1) != merchant_id:
                logger.info(
                    f"[route] {merchant_id}: AliExpress is canonicalising "
                    f"→ store/{m.group(1)} (letting it through)"
                )
        route.continue_()

    ctx.route("**/*", _route_handler)
    return ctx


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE MERCHANT SCRAPER  v5.0 — 3-layer extraction
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_merchant(merchant_id: str) -> Dict:
    url = STORE_URL_TEMPLATE.format(merchant_id=merchant_id)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ua = random.choice(USER_AGENTS)
            with Camoufox(headless=True, os="windows") as browser:
                ctx  = _make_context(browser, ua, merchant_id)
                page = ctx.new_page()

                input_id  = merchant_id
                landed_id = merchant_id   # updated after navigation
                alias_warning = None

                # ── Layer 1: JSON response capture ────────────────────────────
                response_hits: List[Dict] = []

                def _on_response(resp):
                    try:
                        ct    = (resp.headers or {}).get("content-type", "")
                        url_l = resp.url.lower()
                        if "json" not in ct.lower() and not any(
                            x in url_l for x in ["/api/", "/ajax/", "search", "store", "mtop"]
                        ):
                            return
                        data  = resp.json()
                        cand  = _best_count_from_json(data, expected_ids={input_id, landed_id})
                        if cand:
                            response_hits.append({"source": "response", "url": resp.url, **cand})
                    except Exception:
                        pass

                page.on("response", _on_response)

                # ── Navigate ──────────────────────────────────────────────────
                try:
                    page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                except Exception as nav_err:
                    err_str = str(nav_err)
                    if any(x in err_str for x in [
                        "blockedbyclient", "ERR_BLOCKED",
                        "NS_BINDING_ABORTED", "ERR_ABORTED"
                    ]):
                        logger.info(f"[merchant] {merchant_id} nav aborted — checking DOM")
                    elif any(x in err_str for x in [
                        "ERR_NAME_NOT_RESOLVED", "NS_ERROR_UNKNOWN_HOST"
                    ]):
                        page.close(); ctx.close()
                        return {"merchant_id": merchant_id, "total_items": None,
                                "error": "DNS failed / Page not found"}
                    else:
                        raise

                # ── Detect landed store ID ────────────────────────────────────
                cur_url   = page.url
                landed_id = _extract_store_id(cur_url) or input_id
                if landed_id != input_id:
                    alias_warning = f"AliExpress canonicalised {input_id} → {landed_id}"
                    logger.info(f"[merchant] {merchant_id} alias detected: {alias_warning}")

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

                # Re-read landed ID after JS settle
                landed_id = _extract_store_id(page.url) or landed_id
                if landed_id != input_id and not alias_warning:
                    alias_warning = f"AliExpress canonicalised {input_id} → {landed_id}"

                # ── Layer 2: Hydrated globals ─────────────────────────────────
                global_hits: List[Dict] = []
                try:
                    global_hits = page.evaluate(_JS_GLOBAL_CANDIDATES, [input_id, landed_id])
                except Exception as e:
                    logger.debug(f"[merchant] {merchant_id} global scan failed: {e}")

                # ── Layer 3: DOM poll (60s) ───────────────────────────────────
                dom_hit = _wait_for_item_count(page, poll_timeout_ms=POLL_TIMEOUT_MS)

                # ── Check page for real block signals ─────────────────────────
                html      = page.content()
                html_size = len(html)
                lower     = html.lower()
                is_blocked = any(sig in lower for sig in REAL_BLOCK_SIGNALS)

                if is_blocked:
                    screenshot_path = _save_screenshot(page, merchant_id, "captcha")
                    page.close(); ctx.close()
                    logger.warning(f"[merchant] {merchant_id} CAPTCHA/block detected (attempt {attempt})")
                    if attempt < MAX_RETRIES:
                        time.sleep(random.uniform(30, 60))
                        continue
                    return {"merchant_id": merchant_id, "total_items": None,
                            "error": "Blocked/CAPTCHA",
                            "screenshot_path": screenshot_path}

                # ── Pick best candidate across all layers ─────────────────────
                best_count: Optional[int] = None
                best_source = None

                # Response JSON wins
                if response_hits:
                    best_count  = response_hits[0]["count"]
                    best_source = "response_json"

                # Global state next
                if best_count is None and global_hits:
                    best_count  = global_hits[0]["count"]
                    best_source = "global_state"

                # DOM fallback
                if best_count is None and dom_hit is not None:
                    best_count  = dom_hit
                    best_source = "dom_poll"

                if best_count is not None:
                    page.close(); ctx.close()
                    logger.info(
                        f"[merchant] {merchant_id} ✓ {best_count} items "
                        f"(source={best_source}"
                        + (f", canonical={landed_id}" if alias_warning else "")
                        + ")"
                    )
                    return {
                        "merchant_id":        input_id,
                        "canonical_store_id": landed_id,
                        "total_items":        best_count,
                        "error":              "",
                        "warning":            alias_warning,
                        "extraction_source":  best_source,
                    }

                # ── Silent bot-detection: page too small ──────────────────────
                if html_size < BLOCKED_SIZE_BYTES:
                    screenshot_path = _save_screenshot(page, merchant_id, "lite_page")
                    page.close(); ctx.close()
                    logger.warning(
                        f"[merchant] {merchant_id} lite page {html_size//1024}KB "
                        f"(bot detection, attempt {attempt})"
                    )
                    if attempt < MAX_RETRIES:
                        time.sleep(random.uniform(30, 60))
                        continue
                    return {"merchant_id": merchant_id, "total_items": None,
                            "error": f"Bot-detection lite page ({html_size//1024}KB)",
                            "screenshot_path": screenshot_path}

                # ── Full page but nothing found ───────────────────────────────
                screenshot_path = _save_screenshot(page, merchant_id, "no_count")
                page.close(); ctx.close()

                if attempt < MAX_RETRIES:
                    logger.warning(
                        f"[merchant] {merchant_id} no count found, {html_size//1024}KB, "
                        f"attempt {attempt}"
                    )
                    time.sleep(random.uniform(5, 15))
                    continue

                return {
                    "merchant_id":        input_id,
                    "canonical_store_id": landed_id,
                    "total_items":        None,
                    "error":              f"Selector missing ({html_size//1024}KB)",
                    "warning":            alias_warning,
                    "screenshot_path":    screenshot_path,
                }

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
        w.writerow(["MerchantID", "CanonicalStoreID", "TotalItems", "Warning", "Error"])
        for row in rows:
            w.writerow([
                row.get("merchant_id", ""),
                row.get("canonical_store_id", row.get("merchant_id", "")),
                "" if row.get("total_items") is None else row["total_items"],
                row.get("warning", ""),
                row.get("error", ""),
            ])
    logger.info(f"[job:{job_id}] Batch {batch_idx:04d} → {path.name} ({len(rows)} rows)")


def _merge_batch_csvs(job_id: str, batches_total: int) -> Path:
    out_path = _output_path(job_id)
    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["MerchantID", "CanonicalStoreID", "TotalItems", "Warning", "Error"])
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
# BATCH RUNNER
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
