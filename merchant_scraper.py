"""
merchant_scraper.py  v9.0  — Tor Proxy + Anti-Detection Hardening
═══════════════════════════════════════════════════════════════════
CHANGES FROM v8.0:

  TOR-1  — Tor SOCKS5 proxy support added. Each merchant attempt uses
            the proxy if TOR_ENABLED=True. On CAPTCHA or block, a new
            Tor circuit is requested via the control port before retry.
            Config: TOR_SOCKS_HOST, TOR_SOCKS_PORT, TOR_CONTROL_PORT,
                    TOR_CONTROL_PASSWORD, TOR_ENABLED.

  TOR-2  — _new_tor_circuit() sends SIGNAL NEWNYM to the Tor control
            port to get a fresh exit node before each retry after block.

  TOR-3  — Browser context now passes proxy settings when Tor is enabled.
            Camoufox os rotated randomly per attempt (windows/macos/linux)
            for better fingerprint diversity.

  TOR-4  — CAPTCHA/block retry wait reduced to 5-10s (was 30-60s) because
            with a new Tor circuit the IP changes immediately.

  TOR-5  — NS_BINDING_ABORTED now triggers circuit rotation + retry instead
            of propagating as an error. This was causing ~10% failures.

  TOR-6  — Inter-merchant delay reduced: DELAY_MIN=5s, DELAY_MAX=12s when
            Tor enabled (was 10-25s). Circuit rotation provides IP diversity
            so tight timing is less of a signal.

  RETRY-1 — MAX_RETRIES raised to 4 (was 3) to account for one circuit
             rotation attempt.

  RETRY-2 — "Selector Missing" errors now retry with a longer post-scroll
             wait (8s instead of 3s) on subsequent attempts — these are
             often timing issues where the item count loads late.

  FIX-A  — All fixes from v8.0 retained:
            _is_valid_response_url, raw_html before dom_poll,
            spm-anchor text-node patterns, dom_poll skip logic.
"""

import re
import csv
import json
import time
import random
import base64
import logging
import socket
import threading
import io
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime

from camoufox.sync_api import Camoufox

logger = logging.getLogger("merchant_scraper")

# ─────────────────────────────────────────────────────────────────────────────
# TOR CONFIG  — set TOR_ENABLED=True when your Tor instance is running
# ─────────────────────────────────────────────────────────────────────────────

TOR_ENABLED          = True          # flip to False to disable proxy
TOR_SOCKS_HOST       = "127.0.0.1"
TOR_SOCKS_PORT       = 9050          # default Tor SOCKS5 port
TOR_CONTROL_PORT     = 9051          # default Tor control port
TOR_CONTROL_PASSWORD = ""            # set if you configured HashedControlPassword

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

STORE_URL_TEMPLATE = (
    "https://www.aliexpress.com/store/{merchant_id}/pages/all-items.html"
    "?shop_sortType=bestmatch_sort&language=en"
)

BATCH_SIZE           = 20
CONCURRENCY          = 1
MAX_RETRIES          = 4             # TOR-1: +1 for circuit rotation attempt
PAGE_TIMEOUT         = 120_000
NETWORKIDLE_TIMEOUT  = 30_000
POLL_TIMEOUT_MS      = 90_000
DELAY_MIN            = 5.0 if TOR_ENABLED else 10.0   # TOR-6
DELAY_MAX            = 12.0 if TOR_ENABLED else 25.0  # TOR-6
JOBS_DIR             = Path("./merchant_jobs")
SCREENSHOTS_DIR      = Path("./merchant_screenshots")

# OS pool for fingerprint rotation — TOR-3
OS_POOL = ["windows", "macos", "linux"]

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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
]

BASE_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "sec-fetch-dest":  "document",
    "sec-fetch-mode":  "navigate",
    "sec-fetch-site":  "none",
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

# ─────────────────────────────────────────────────────────────────────────────
# URL FILTER LISTS (v8.0 FIX-A)
# ─────────────────────────────────────────────────────────────────────────────

JUNK_URL_FRAGMENTS = [
    "recom-acs", "recommend", "login", "signin", "passport",
    "/ad.", "/ads.", "analytics", "tracking", "beacon",
    "mtop.relationrecommend", "mtop.user", "mtop.login",
    "mtop.member", "countrylistforlogin", "renderpage",
    "captcha", "punish", "risk", "umeng", "aplus", "goldlog",
]

VALID_URL_FRAGMENTS = [
    "/store/", "allitems", "all-items", "shopstoreitem",
    "storeitem", "mtop.aliexpress.store", "mtop.aliexpress.search",
    "search.mtop", "itemcount", "storefront", "store_pc",
    "sellerprofile", "totalitem", "productlist",
]

# ─────────────────────────────────────────────────────────────────────────────
# RAW HTML PATTERNS (v8.0 FIX-C expanded)
# ─────────────────────────────────────────────────────────────────────────────

RAW_HTML_COUNT_PATTERNS = [
    r'store_pc_allItems[^"]*"[^>]*>\s*(\d+)\s*items?',
    r'>(\d+)\s+items?<',
    r'>(\d+)\s+products?<',
    r'"totalResults?"\s*:\s*(\d+)',
    r'"searchResultTotal"\s*:\s*(\d+)',
    r'"itemCount"\s*:\s*(\d+)',
    r'"totalItems?"\s*:\s*(\d+)',
    r'"storeItemCount"\s*:\s*(\d+)',
    r'"productCount"\s*:\s*(\d+)',
    r'"goodsCount"\s*:\s*(\d+)',
    r'"totalNum"\s*:\s*(\d+)',
    r'"feedCount"\s*:\s*(\d+)',
    r'"resultCount"\s*:\s*(\d+)',
    r'"displayItemCount"\s*:\s*(\d+)',
    r'"totalResult"\s*:\s*(\d+)',
    r'"totalProducts?"\s*:\s*(\d+)',
    r'"allItemsTotal"\s*:\s*(\d+)',
    r'"pageInfo"\s*:.*?"total"\s*:\s*(\d+)',
]

_jobs: Dict[str, Dict] = {}
_jobs_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# TOR HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _new_tor_circuit() -> bool:
    """
    TOR-2: Send SIGNAL NEWNYM to the Tor control port to request a new
    circuit (fresh exit node). Returns True on success, False on failure.
    Call this before retrying after a CAPTCHA or block signal.
    """
    if not TOR_ENABLED:
        return False
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((TOR_SOCKS_HOST, TOR_CONTROL_PORT))

        if TOR_CONTROL_PASSWORD:
            s.sendall(f'AUTHENTICATE "{TOR_CONTROL_PASSWORD}"\r\n'.encode())
        else:
            s.sendall(b'AUTHENTICATE ""\r\n')

        resp = s.recv(1024).decode()
        if "250" not in resp:
            logger.warning(f"[tor] auth failed: {resp.strip()}")
            s.close()
            return False

        s.sendall(b"SIGNAL NEWNYM\r\n")
        resp = s.recv(1024).decode()
        s.close()

        if "250" in resp:
            logger.info("[tor] new circuit requested ✓")
            time.sleep(2)   # give Tor time to build the new circuit
            return True
        logger.warning(f"[tor] NEWNYM failed: {resp.strip()}")
        return False
    except Exception as e:
        logger.debug(f"[tor] circuit rotation error: {e}")
        return False


def _tor_proxy_settings() -> Optional[Dict]:
    """
    TOR-3: Return Camoufox-compatible proxy dict when Tor is enabled.
    Returns None when Tor is disabled so callers can handle both cases.
    """
    if not TOR_ENABLED:
        return None
    return {
        "server": f"socks5://{TOR_SOCKS_HOST}:{TOR_SOCKS_PORT}",
    }


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
# JSON RESPONSE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

COUNT_HINT_RE = re.compile(
    r"(item|items|product|products|goods|result|results|feed|search|display)"
    r".{0,30}(count|total|num|size)"
    r"|"
    r"(count|total|num|size).{0,30}"
    r"(item|items|product|products|goods|result|results|feed|search|display)",
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
            elif isinstance(v, str):
                out.append({"path": p, "value": v})
                vv = v.strip()
                if (vv.startswith("{") and vv.endswith("}")) or \
                   (vv.startswith("[") and vv.endswith("]")):
                    try:
                        _walk_json(json.loads(vv), p + ".__decoded__", out)
                    except Exception:
                        pass
            else:
                out.append({"path": p, "value": v})
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            p = f"{path}[{i}]"
            if isinstance(v, (dict, list)):
                _walk_json(v, p, out)
            elif isinstance(v, str):
                out.append({"path": p, "value": v})
                vv = v.strip()
                if (vv.startswith("{") and vv.endswith("}")) or \
                   (vv.startswith("[") and vv.endswith("]")):
                    try:
                        _walk_json(json.loads(vv), p + ".__decoded__", out)
                    except Exception:
                        pass
            else:
                out.append({"path": p, "value": v})
    return out


def _best_count_from_json(
    data: Any,
    expected_ids: Optional[set] = None,
    source_url: str = "",
) -> Optional[Dict]:
    expected_ids = set(expected_ids or [])
    flat = _walk_json(data)

    ids_found: set = set()
    for item in flat:
        p = item["path"].lower()
        v = item["value"]
        if ID_HINT_RE.search(p) and str(v).isdigit():
            ids_found.add(str(v))

    store_id_confirmed = bool(expected_ids & ids_found)

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
        if any(x in p for x in ["item", "product", "goods", "result", "feed"]):
            score += 4
        if any(x in p for x in ["count", "total", "num", "size"]):
            score += 4
        if store_id_confirmed:
            score += 2

        if score >= 10:
            candidates.append({
                "count":              v,
                "path":               item["path"],
                "score":              score,
                "matched_ids":        sorted(expected_ids & ids_found),
                "store_id_confirmed": store_id_confirmed,
            })

    if not candidates:
        return None
    candidates.sort(key=lambda x: (x["score"], x["count"]), reverse=True)
    return candidates[0]


def _extract_store_id(url: str) -> Optional[str]:
    m = re.search(r'/store/(\d+)/', url or "")
    return m.group(1) if m else None


def _raw_html_count_scan(html: str) -> Optional[int]:
    """Layer 2.5 — scan raw HTML. Free, no timeout. Run before dom_poll."""
    for pattern in RAW_HTML_COUNT_PATTERNS:
        for m in re.finditer(pattern, html, re.I | re.S):
            try:
                val = int(m.group(1))
                if 1 <= val <= 500_000:
                    logger.debug(f"[raw_html] pattern='{pattern[:40]}' val={val}")
                    return val
            except Exception:
                pass
    return None


def _is_valid_response_url(url: str, content_type: str) -> bool:
    """Reject junk URLs (recommendation, login, analytics, ads)."""
    url_l = url.lower()
    ct_l  = content_type.lower()
    if any(frag in url_l for frag in JUNK_URL_FRAGMENTS):
        return False
    is_json      = "json" in ct_l
    is_valid_path = any(frag in url_l for frag in VALID_URL_FRAGMENTS)
    if is_json and is_valid_path:
        return True
    if is_valid_path and any(x in url_l for x in ["/api/", "/ajax/", "mtop", "gw."]):
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 2: HYDRATED GLOBALS
# ─────────────────────────────────────────────────────────────────────────────

_JS_GLOBAL_CANDIDATES = r"""
(expectedIds) => {
    const names = [
        "runParams", "__NEXT_DATA__", "__INITIAL_STATE__",
        "__PRELOADED_STATE__", "_page_config_", "_init_data_"
    ];
    const directKeys = [
        "totalItems", "totalCount", "itemCount", "productTotal",
        "storeItemCount", "totalResults", "searchResultTotal",
        "feedCount", "resultCount", "displayItemCount", "totalResult",
        "totalProducts", "goodsCount", "totalNum", "allItemsTotal"
    ];
    if (window.runParams && window.runParams.data) {
        for (const key of directKeys) {
            const v = window.runParams.data[key];
            if (typeof v === "number" && v > 0 && v < 500000)
                return [{ source:"runParams.data", path:"runParams.data."+key, count:v, score:20, matched_store_id:null }];
            if (typeof v === "string" && /^\d+$/.test(v)) {
                const n = parseInt(v,10);
                if (n > 0 && n < 500000)
                    return [{ source:"runParams.data", path:"runParams.data."+key, count:n, score:20, matched_store_id:null }];
            }
        }
    }
    const countRe = /(item|items|product|products|goods|result|results|feed|search|display).{0,30}(count|total|num|size)|(count|total|num|size).{0,30}(item|items|product|products|goods|result|results|feed|search|display)/i;
    const idRe    = /(store|shop|seller|merchant).{0,10}id/i;
    const hits = []; const seen = new WeakSet();
    function walk(obj, path, meta) {
        if (!obj || typeof obj !== "object") return;
        if (seen.has(obj)) return; seen.add(obj);
        if (Array.isArray(obj)) {
            obj.forEach((v,i) => {
                if (v && typeof v === "object") walk(v, `${path}[${i}]`, meta);
                else if (typeof v === "number") {
                    const p = `${path}[${i}]`;
                    if (countRe.test(p) && v > 0 && v < 500000)
                        hits.push({ source:path.split(".")[0], path:p, count:v, score:10, matched_store_id:meta.storeId||null });
                }
            });
            return;
        }
        const nextMeta = {...meta};
        for (const [k, v] of Object.entries(obj)) {
            const p = `${path}.${k}`;
            if (directKeys.includes(k)) {
                const n = typeof v === "number" ? v : (typeof v === "string" && /^\d+$/.test(v) ? parseInt(v,10) : null);
                if (n && n > 0 && n < 500000)
                    hits.push({ source:path.split(".")[0], path:p, count:n, score:20, matched_store_id:nextMeta.storeId||null });
            }
            if (idRe.test(k) && (typeof v === "string" || typeof v === "number")) nextMeta.storeId = String(v);
            if (typeof v === "number") {
                let score = 0;
                if (countRe.test(p)) score += 10;
                if (nextMeta.storeId && expectedIds.includes(nextMeta.storeId)) score += 3;
                if (score >= 10 && v > 0 && v < 500000)
                    hits.push({ source:path.split(".")[0], path:p, count:v, score, matched_store_id:nextMeta.storeId||null });
            } else if (typeof v === "string") {
                const vv = v.trim();
                if ((vv.startsWith("{") && vv.endsWith("}")) || (vv.startsWith("[") && vv.endsWith("]")))
                    try { walk(JSON.parse(vv), p+".__decoded__", nextMeta); } catch(e) {}
            } else if (v && typeof v === "object") walk(v, p, nextMeta);
        }
    }
    for (const n of names) {
        try { if (window[n] && typeof window[n] === "object") walk(window[n], n, {}); } catch(e) {}
    }
    hits.sort((a,b) => b.score - a.score);
    return hits.slice(0, 10);
}
"""


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 3: DOM POLL
# ─────────────────────────────────────────────────────────────────────────────

_JS_POLL_FOR_COUNT = r"""() => {
    const patterns = [
        /(\d{1,3}(?:[,\s]\d{3})*)\s*(?:items?|products?|goods?|results?)\b/i,
        /\b(?:items?|products?|goods?|results?)\s*[：:(-]?\s*(\d{1,3}(?:[,\s]\d{3})*)\b/i,
        /\bof\s+(\d{1,3}(?:[,\s]\d{3})*)\s*(?:items?|products?|results?)?\b/i,
        /(\d{1,3}(?:[,\s]\d{3})*)\s*(?:item|product|result)s?\s+found\b/i,
    ];
    const norm = (s) => (s||"").replace(/\u00a0/g," ").replace(/\s+/g," ").trim();
    const tryText = (txt) => {
        txt = norm(txt);
        if (!txt || txt.length > 120) return null;
        for (const re of patterns) {
            const m = txt.match(re);
            if (m) { const raw=(m[1]||m[2]||"").replace(/[\s,]/g,""); if(!raw)continue; const v=parseInt(raw,10); if(v>0&&v<1000000)return v; }
        }
        return null;
    };
    const directKeys=["totalItems","totalCount","itemCount","productTotal","storeItemCount","totalResults","searchResultTotal","feedCount","resultCount","displayItemCount","totalResult","totalProducts","goodsCount","totalNum","allItemsTotal"];
    if (window.runParams && window.runParams.data) {
        for (const k of directKeys) {
            const v=window.runParams.data[k];
            if (typeof v==="number"&&v>0&&v<1e6) return v;
            if (typeof v==="string"&&/^\d+$/.test(v)){const n=parseInt(v,10);if(n>0&&n<1e6)return n;}
        }
    }
    for (const el of document.querySelectorAll('span[data-spm-anchor-id*="store_pc_allItems_or_groupList"],span[data-spm-anchor-id*="store_pc_allItems"],div[data-spm-anchor-id*="store_pc_allItems_or_groupList"],div[data-spm-anchor-id*="store_pc_allItems"]')) {
        const v=tryText(el.textContent); if(v)return v;
        for(const span of el.querySelectorAll('span')){const sv=tryText(span.textContent);if(sv)return sv;}
    }
    for (const el of document.querySelectorAll('[class*="total-items"],[class*="total-product"],[class*="item-count"],[class*="product-count"],[class*="total"],[class*="count"],[id*="total"],[id*="count"],[data-widget-cid*="total"],.store-overview-total-product-count')) {
        const v=tryText(el.textContent); if(v)return v;
    }
    for (const s of document.querySelectorAll('script')) {
        const m=(s.textContent||'').match(/"(?:totalResults?|itemCount|totalItems?|storeItemCount|productCount|goodsCount|totalNum|searchResultTotal|feedCount|resultCount|displayItemCount|totalResult|totalProducts?|allItemsTotal)"\s*:\s*(\d+)/);
        if(m)return parseInt(m[1],10);
    }
    for (const s of document.querySelectorAll('script')) {
        const txt=s.textContent||'';
        if(txt.includes('_init_data_')||txt.includes('runParams')){
            const m=txt.match(/"(?:total(?:Items?|Count|Products?)|itemCount|storeItemCount)"\s*:\s*(\d+)/);
            if(m)return parseInt(m[1],10);
        }
    }
    for (const script of document.querySelectorAll('script[type="application/ld+json"]')) {
        try{const data=JSON.parse(script.textContent);if(data.numberOfItems)return parseInt(data.numberOfItems,10);if(data.mainEntity&&data.mainEntity.numberOfItems)return parseInt(data.mainEntity.numberOfItems,10);}catch(e){}
    }
    const container=document.querySelector('[data-total-items]');
    if(container){const m=String(container.getAttribute('data-total-items')).match(/\d+/);if(m)return parseInt(m[0],10);}
    for (const el of document.querySelectorAll('span,div,p,h1,h2,h3,li,strong,b')) {
        const v=tryText(el.textContent); if(v)return v;
    }
    const walker=document.createTreeWalker(document.body||document.documentElement,NodeFilter.SHOW_TEXT,null);
    let node;
    while((node=walker.nextNode())){const v=tryText(node.textContent);if(v)return v;}
    return false;
}"""


_JS_DOM_DUMP = """() => {
    const results = [];
    const all = document.querySelectorAll('span, div, p, h1, h2, h3, li');
    for (const el of all) {
        const t = el.textContent.trim();
        if (t.length < 80 && /item/i.test(t) && t.length > 0) {
            results.push({ tag:el.tagName.toLowerCase(), text:t, anchor:el.getAttribute('data-spm-anchor-id')||'', cls:el.className?String(el.className).slice(0,60):'' });
            if (results.length >= 15) break;
        }
    }
    const scriptMatches = [];
    for (const s of document.querySelectorAll('script')) {
        const m=(s.textContent||'').match(/"(?:totalResults?|itemCount|totalItems?|storeItemCount|productCount|goodsCount|totalNum|searchResultTotal|feedCount|resultCount|displayItemCount|totalResult|totalProducts?|allItemsTotal)"\s*:\s*(\d+)/g);
        if(m)scriptMatches.push(...m.slice(0,3));
    }
    return { dom_elements: results, script_matches: scriptMatches.slice(0,5) };
}"""


def _wait_for_item_count(page, poll_timeout_ms: int = POLL_TIMEOUT_MS) -> Optional[int]:
    try:
        result = page.wait_for_function(_JS_POLL_FOR_COUNT, timeout=poll_timeout_ms, polling=150)
        count  = result.json_value()
        if isinstance(count, (int, float)) and count > 0:
            return int(count)
    except Exception as e:
        logger.debug(f"[poll] timeout/error: {e}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER CONTEXT
# ─────────────────────────────────────────────────────────────────────────────

def _make_context(browser, ua: str, merchant_id: str = ""):
    proxy = _tor_proxy_settings()   # TOR-3: inject proxy when enabled

    ctx_kwargs = dict(
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        user_agent=ua,
        extra_http_headers=BASE_HEADERS,
    )
    if proxy:
        ctx_kwargs["proxy"] = proxy

    ctx = browser.new_context(**ctx_kwargs)

    def _route_handler(route):
        url = route.request.url
        if "/store/" in url and "pages/all-items" in url:
            m = re.search(r'/store/(\d+)/', url)
            if m and merchant_id and m.group(1) != merchant_id:
                logger.info(f"[route] {merchant_id}: canonicalising → store/{m.group(1)} — allowing")
        route.continue_()

    ctx.route("**/*", _route_handler)
    return ctx


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE MERCHANT SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_merchant(merchant_id: str) -> Dict:
    url      = STORE_URL_TEMPLATE.format(merchant_id=merchant_id)
    input_id = merchant_id

    # Track whether the previous attempt was blocked — used to decide
    # whether to rotate the Tor circuit before the next attempt.
    prev_was_blocked = False

    for attempt in range(1, MAX_RETRIES + 1):
        # TOR-1/TOR-2: rotate circuit if last attempt was blocked
        if prev_was_blocked and TOR_ENABLED:
            logger.info(f"[merchant] {merchant_id} rotating Tor circuit before attempt {attempt}")
            _new_tor_circuit()
            prev_was_blocked = False

        # TOR-3: rotate OS fingerprint each attempt
        os_choice = random.choice(OS_POOL)

        try:
            ua = random.choice(USER_AGENTS)
            with Camoufox(headless=True, os=os_choice) as browser:
                ctx  = _make_context(browser, ua, merchant_id)
                page = ctx.new_page()

                landed_id     = input_id
                alias_warning = None
                response_hits: List[Dict] = []

                def _on_response(resp):
                    try:
                        ct    = (resp.headers or {}).get("content-type", "")
                        url_r = resp.url
                        if not _is_valid_response_url(url_r, ct):
                            return
                        data = None
                        try:
                            data = resp.json()
                        except Exception:
                            pass
                        if data is not None:
                            cand = _best_count_from_json(data, expected_ids={input_id, landed_id}, source_url=url_r)
                            if cand:
                                response_hits.append({"source": "response_json", "url": url_r, **cand})
                        else:
                            try:
                                raw_text = resp.text()
                                for pattern in RAW_HTML_COUNT_PATTERNS:
                                    m = re.search(pattern, raw_text, re.I)
                                    if m:
                                        val = int(m.group(1))
                                        if 1 <= val <= 500_000:
                                            response_hits.append({"source": "response_text_regex", "url": url_r, "count": val, "path": "raw_text_regex", "score": 15})
                                            break
                            except Exception:
                                pass
                    except Exception:
                        pass

                page.on("response", _on_response)

                # ── Navigate ──────────────────────────────────────────────
                nav_aborted = False
                try:
                    page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                except Exception as nav_err:
                    err_str = str(nav_err)
                    # TOR-5: NS_BINDING_ABORTED now triggers circuit rotation + retry
                    if any(x in err_str for x in ["NS_BINDING_ABORTED", "ERR_ABORTED", "blockedbyclient", "ERR_BLOCKED"]):
                        logger.info(f"[merchant] {merchant_id} nav aborted (attempt {attempt}) — will retry with new circuit")
                        page.close(); ctx.close()
                        prev_was_blocked = True
                        if attempt < MAX_RETRIES:
                            time.sleep(random.uniform(3, 7))
                            continue
                        return {"merchant_id": input_id, "canonical_store_id": input_id, "total_items": None, "error": "NS_BINDING_ABORTED after all retries", "warning": None, "extraction_source": None, "screenshot_path": None}
                    elif any(x in err_str for x in ["ERR_NAME_NOT_RESOLVED", "NS_ERROR_UNKNOWN_HOST"]):
                        page.close(); ctx.close()
                        return {"merchant_id": input_id, "canonical_store_id": input_id, "total_items": None, "error": "DNS failed / page not found", "warning": None, "extraction_source": None, "screenshot_path": None}
                    else:
                        raise

                # ── Detect alias ──────────────────────────────────────────
                landed_id = _extract_store_id(page.url) or input_id
                if landed_id != input_id:
                    alias_warning = f"AliExpress canonicalised {input_id} → {landed_id}"
                    logger.info(f"[merchant] {merchant_id}: {alias_warning}")

                # ── networkidle ───────────────────────────────────────────
                try:
                    page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT)
                except Exception:
                    pass

                # ── Wait for product grid ─────────────────────────────────
                try:
                    page.wait_for_selector('img, [class*="product"], [class*="item"], [class*="card"]', timeout=20_000)
                except Exception:
                    pass

                # ── Scroll-to-bottom ──────────────────────────────────────
                try:
                    last_height = page.evaluate("document.body.scrollHeight")
                    for _ in range(8):
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(random.randint(800, 1_500))
                        new_height = page.evaluate("document.body.scrollHeight")
                        if new_height == last_height:
                            break
                        last_height = new_height
                except Exception:
                    pass

                # ── Mouse-wheel behavioral signals ────────────────────────
                for _ in range(5):
                    page.mouse.move(random.randint(100, 1200), random.randint(100, 800))
                    page.mouse.wheel(0, random.randint(600, 1_200))
                    page.wait_for_timeout(random.randint(400, 800))

                # RETRY-2: longer post-scroll wait on 2nd+ attempt
                # "Selector Missing" errors are often timing — the item count
                # element loads late; waiting longer helps significantly.
                post_wait_ms = random.randint(6_000, 9_000) if attempt > 1 else random.randint(3_000, 5_000)
                page.wait_for_timeout(post_wait_ms)

                # Re-read alias after JS settle
                landed_id = _extract_store_id(page.url) or landed_id
                if landed_id != input_id and not alias_warning:
                    alias_warning = f"AliExpress canonicalised {input_id} → {landed_id}"

                # ── HTML snapshot ─────────────────────────────────────────
                html      = page.content()
                html_size = len(html)
                lower     = html.lower()
                is_blocked = any(sig in lower for sig in REAL_BLOCK_SIGNALS)

                if is_blocked:
                    screenshot_path = _save_screenshot(page, merchant_id, "captcha")
                    page.close(); ctx.close()
                    logger.warning(f"[merchant] {merchant_id} CAPTCHA/block (attempt {attempt})")
                    prev_was_blocked = True
                    if attempt < MAX_RETRIES:
                        # TOR-4: short wait — circuit rotation handles the IP change
                        time.sleep(random.uniform(5, 10))
                        continue
                    return {"merchant_id": input_id, "canonical_store_id": landed_id, "total_items": None, "error": "Blocked/CAPTCHA", "warning": alias_warning, "extraction_source": None, "screenshot_path": screenshot_path}

                # ── Layer 2: hydrated globals ─────────────────────────────
                global_hits: List[Dict] = []
                try:
                    global_hits = page.evaluate(_JS_GLOBAL_CANDIDATES, [input_id, landed_id])
                except Exception as e:
                    logger.debug(f"[merchant] {merchant_id} global scan failed: {e}")

                # ── Layer 2.5: raw HTML regex (free, no timeout) ──────────
                raw_html_hit = _raw_html_count_scan(html)

                # ── Layer 3: DOM poll — skip if cheaper layers found it ───
                dom_hit = None
                if raw_html_hit is None and not global_hits and not response_hits:
                    dom_hit = _wait_for_item_count(page, poll_timeout_ms=POLL_TIMEOUT_MS)
                elif raw_html_hit is None:
                    dom_hit = _wait_for_item_count(page, poll_timeout_ms=15_000)

                page.close()
                ctx.close()

                # ── Pick best count — priority order ──────────────────────
                # Priority: response_json (validated) → global_state →
                #           raw_html_regex → dom_poll
                best_count:  Optional[int] = None
                best_source: Optional[str] = None

                confirmed_response   = [h for h in response_hits if h.get("store_id_confirmed")]
                unconfirmed_response = [h for h in response_hits if not h.get("store_id_confirmed")]

                if confirmed_response:
                    best_count  = confirmed_response[0]["count"]
                    best_source = "response_json_confirmed"
                elif global_hits:
                    best_count  = global_hits[0]["count"]
                    best_source = "global_state"
                elif raw_html_hit is not None:
                    best_count  = raw_html_hit
                    best_source = "raw_html_regex"
                elif dom_hit is not None:
                    best_count  = dom_hit
                    best_source = "dom_poll"
                elif unconfirmed_response:
                    best_count  = unconfirmed_response[0]["count"]
                    best_source = "response_json_unconfirmed"

                if best_count is not None:
                    canonical_note = f" (canonical={landed_id})" if alias_warning else ""
                    logger.info(f"[merchant] {merchant_id} ✓ {best_count} items [{best_source}]{canonical_note}")
                    return {"merchant_id": input_id, "canonical_store_id": landed_id, "total_items": best_count, "error": "", "warning": alias_warning, "extraction_source": best_source, "screenshot_path": None}

                # ── Nothing found ─────────────────────────────────────────
                logger.warning(f"[merchant] {merchant_id} no count found ({html_size//1024}KB, attempt {attempt})")

                # RETRY-2: "Selector Missing" — retry with longer wait
                if attempt < MAX_RETRIES:
                    time.sleep(random.uniform(5, 15))
                    continue

                # Last attempt — screenshot for diagnosis
                screenshot_path = None
                try:
                    with Camoufox(headless=True, os=os_choice) as browser2:
                        ctx2  = _make_context(browser2, ua, merchant_id)
                        page2 = ctx2.new_page()
                        try:
                            page2.goto(url, timeout=30_000, wait_until="domcontentloaded")
                        except Exception:
                            pass
                        screenshot_path = _save_screenshot(page2, merchant_id, "no_count")
                        page2.close(); ctx2.close()
                except Exception:
                    pass

                return {"merchant_id": input_id, "canonical_store_id": landed_id, "total_items": None, "error": f"Selector Missing — no count found after {MAX_RETRIES} attempts ({html_size//1024}KB)", "warning": alias_warning, "extraction_source": None, "screenshot_path": screenshot_path}

        except Exception as exc:
            err_str = str(exc)
            label   = "Timeout" if "timeout" in err_str.lower() else f"Error: {err_str[:80]}"
            logger.error(f"[merchant] {merchant_id} attempt {attempt} — {label}")
            prev_was_blocked = "timeout" not in err_str.lower()  # timeouts don't need circuit rotation
            if attempt < MAX_RETRIES:
                time.sleep(random.uniform(5, 15))
                continue
            return {"merchant_id": input_id, "canonical_store_id": input_id, "total_items": None, "error": label, "warning": None, "extraction_source": None, "screenshot_path": None}

    return {"merchant_id": input_id, "canonical_store_id": input_id, "total_items": None, "error": "Max retries exceeded", "warning": None, "extraction_source": None, "screenshot_path": None}


# ─────────────────────────────────────────────────────────────────────────────
# BATCH WRITER / MERGER
# ─────────────────────────────────────────────────────────────────────────────

def _write_batch_csv(job_id: str, batch_idx: int, rows: List[Dict]) -> None:
    path = _batch_path(job_id, batch_idx)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["MerchantID", "CanonicalStoreID", "TotalItems", "ExtractionSource", "Warning", "Error"])
        for row in rows:
            w.writerow([
                row.get("merchant_id", ""),
                row.get("canonical_store_id", row.get("merchant_id", "")),
                "" if row.get("total_items") is None else row["total_items"],
                row.get("extraction_source", ""),
                row.get("warning", ""),
                row.get("error", ""),
            ])
    logger.info(f"[job:{job_id}] Batch {batch_idx:04d} → {path.name} ({len(rows)} rows)")


def _merge_batch_csvs(job_id: str, batches_total: int) -> Path:
    out_path = _output_path(job_id)
    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["MerchantID", "CanonicalStoreID", "TotalItems", "ExtractionSource", "Warning", "Error"])
        for idx in range(batches_total):
            bf = _batch_path(job_id, idx)
            if not bf.exists():
                continue
            with open(bf, newline="", encoding="utf-8") as in_f:
                reader = csv.reader(in_f)
                next(reader, None)
                for row in reader:
                    while len(row) < 6:
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
            row = {"merchant_id": mid, "canonical_store_id": mid, "total_items": None, "error": str(e)[:120], "warning": None, "extraction_source": None}
        rows.append(row)

        status = f"✓ {row['total_items']} [{row.get('extraction_source','')}]" if row.get("total_items") is not None else f"✗ {row.get('error','')[:50]}"
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
        _jobs[job_id].update({"status": "running", "total": total, "batches_total": batches_total, "batches_done": 0, "batches_failed": 0})

    logger.info(f"[job:{job_id}] Start — {total} merchants | {batches_total} batches | Tor={'on' if TOR_ENABLED else 'off'}")

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

    logger.info(f"[job:{job_id}] ✓ Complete — {meta['batches_done']} ok | {meta['batches_failed']} failed")


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

def start_bulk_job(job_id: str, merchant_ids: List[str]) -> None:
    with _jobs_lock:
        _jobs[job_id] = {"status": "queued", "total": len(merchant_ids), "batches_total": 0, "batches_done": 0, "batches_failed": 0}
    t = threading.Thread(target=_run_bulk_job, args=(job_id, merchant_ids), daemon=True, name=f"merchant-{job_id[:8]}")
    t.start()


def get_job_status(job_id: str) -> Optional[Dict]:
    with _jobs_lock:
        mem = dict(_jobs.get(job_id, {}))
    disk = _load_metadata(job_id)
    if not mem and not disk:
        return None
    if disk:
        total_merchants = disk.get("total", 0)
        merchants_done  = sum(b.get("size", BATCH_SIZE) for b in disk.get("batches", []) if b.get("status") in ("done", "failed"))
        merchants_done  = min(merchants_done, total_merchants)
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
        merchants_done = sum(b.get("size", BATCH_SIZE) for b in meta.get("batches", []) if b.get("status") in ("done", "failed"))
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
            "download_url":        f"/merchant-download/{job_dir.name}" if meta.get("status") == "done" else None,
        })
    return result
