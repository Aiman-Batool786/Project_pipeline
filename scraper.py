"""
scraper.py v6.5
────────────────
Key changes vs v6.4:
  - scrape_search_results: ALWAYS navigates all max_pages via URL — never
    stops early due to missing pagination DOM. The _has_next_page / total_pages
    checks were the root cause of the "33 products" early-stop bug.
  - Each page now uses direct URL navigation (_build_page_url) so pages
    1 → 5 are always visited even when pagination buttons are not rendered.
  - Pagination DOM checks are kept as SOFT logging only, not hard stops.
  - Deduplication guaranteed across all pages.
"""

import re
import json
import random
import time
import urllib.request
import concurrent.futures
from typing import List, Dict, Optional
from camoufox.sync_api import Camoufox

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

SPEC_MAPPING = {
    'brand':             ['brand', 'brand name', 'marque', 'manufacturer', 'marka'],
    'color':             ['color', 'colour', 'main color', 'couleur', 'kolor'],
    'dimensions':        ['dimensions', 'size', 'product size', 'package size',
                          'item size', 'product dimensions', 'wymiary'],
    'weight':            ['weight', 'net weight', 'gross weight', 'poids', 'waga'],
    'material':          ['material', 'materials', 'composition', 'matiere', 'material'],
    'country_of_origin': ['origin', 'country of origin', 'made in',
                          'country/region of manufacture', 'kraj pochodzenia'],
    'warranty':          ['warranty', 'garantie', 'warranty period',
                          'warranty type', 'gwarancja'],
    'certifications':    ['certification', 'certifications', 'certificate',
                          'compliance', 'standard', 'ce', 'rohs', 'certyfikat'],
    'product_type':      ['product type', 'type', 'item type', 'style',
                          'category', 'typ produktu'],
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

MAX_RETRIES       = 3
MAX_SEARCH_PAGES  = 50
PAGE_TIMEOUT      = 90_000
SEARCH_TIMEOUT    = 30_000
SCROLL_PAUSE      = 600

# ── Task 3: Target countries for shipping extraction ──────────────────────────
SHIPPING_TARGET_COUNTRIES = {
    "PL": "Poland",
    "DE": "Germany",
    "CZ": "Czech Republic",
    "AT": "Austria",
    "BG": "Bulgaria",
}

SERVER_IS_EU = False
REGIONS_SAFE = ["AE", "US", "AU", "CA", "PK", "SA", "TR"]
REGIONS_EU   = ["DE", "FR", "NL", "IT", "ES"]

CATEGORY_ID_MAP = {
    '5090301':   'Cell Phones',
    '509':       'Phones & Telecommunications',
    '202238810': 'Cell Phones (Refurbished)',
    '202238004': 'Consumer Electronics',
    '200000345': "Women's Clothing",
    '200000346': "Men's Clothing",
    '200003655': 'Tablets',
    '100006654': 'Smart Watches',
}


def _get_rotated_url(url: str) -> str:
    region = random.choice(REGIONS_EU if SERVER_IS_EU else REGIONS_SAFE)
    sep = '&' if '?' in url else '?'
    print(f"[scraper] Region -> {region}")
    return f"{url}{sep}shipFromCountry={region}"


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _s(v) -> str:
    if v is None:
        return ''
    s = str(v).strip()
    return '' if s in ('None', 'null', '0', 'false', 'undefined') else s


def _safe(d, *keys, default=None):
    cur = d
    for k in keys:
        if isinstance(cur, dict):
            cur = cur.get(k, default)
        elif isinstance(cur, list) and isinstance(k, int):
            cur = cur[k] if k < len(cur) else default
        else:
            return default
        if cur is None:
            return default
    return cur


def _safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if isinstance(cur, dict):
            cur = cur.get(k, default)
        elif isinstance(cur, list) and isinstance(k, int):
            cur = cur[k] if k < len(cur) else default
        else:
            return default
        if cur is None:
            return default
    return cur


def _clean_title(title: str) -> str:
    if not title:
        return title
    title = re.sub(r'\s*[-]\s*AliExpress\.?\s*\d*\s*$', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*[-]?\s*AliExpress\.?com?\s*$', '', title, flags=re.IGNORECASE)
    return title.strip()


# ─────────────────────────────────────────────────────────────────────────────
# SELLER BLOCK PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_seller_block(store: dict) -> dict:
    if not store or not isinstance(store, dict):
        return {}
    sid = _s(
        store.get('storeNum') or store.get('sellerId') or
        store.get('storeId') or store.get('shopId') or
        store.get('userId') or store.get('memberId')
    )
    result = {
        'store_name':            _s(store.get('storeName') or store.get('sellerName') or
                                    store.get('shopName') or store.get('name')),
        'store_id':              sid,
        'seller_id':             _s(store.get('sellerId') or store.get('userId') or
                                    store.get('memberId')),
        'store_url':             _s(store.get('storeUrl') or store.get('shopUrl') or
                                    (f"https://www.aliexpress.com/store/{sid}" if sid else '')),
        'seller_country':        _s(store.get('country') or store.get('countryCompleteName') or
                                    store.get('shopCountry')),
        'seller_rating':         _s(store.get('positiveRate') or store.get('itemAs') or
                                    store.get('sellerRating')),
        'seller_positive_rate':  _s(store.get('positiveRate') or
                                    store.get('positiveFeedbackRate')),
        'seller_communication':  _s(store.get('communicationRating') or
                                    store.get('serviceAs') or store.get('communicationScore')),
        'seller_shipping_speed': _s(store.get('shippingRating') or store.get('shippingAs') or
                                    store.get('shippingScore')),
        'store_open_date':       _s(store.get('openTime') or store.get('openDate') or
                                    store.get('establishedDate')),
        'seller_level':          _s(store.get('sellerLevel') or store.get('shopLevel') or
                                    store.get('level')),
        'seller_total_reviews':  _s(store.get('totalEvaluationNum') or store.get('reviewNum') or
                                    store.get('feedbackCount')),
        'seller_positive_num':   _s(store.get('positiveNum') or
                                    store.get('positiveFeedbackNum')),
        'is_top_rated':          _s(store.get('isTopRatedSeller') or
                                    store.get('topRatedSeller')),
    }
    return {k: v for k, v in result.items() if v}


# ─────────────────────────────────────────────────────────────────────────────
# MTOP API RESPONSE PARSER
# ─────────────────────────────────────────────────────────────────────────────

def parse_mtop_response(text: str) -> dict | None:
    try:
        m = re.match(r'^[a-zA-Z0-9_$]+\((.*)?\);?\s*$', text.strip(), re.DOTALL)
        if m:
            text = m.group(1)

        outer  = json.loads(text)
        result = (
            _safe(outer, 'data', 'result') or
            _safe(outer, 'data', 'data') or
            _safe(outer, 'data') or {}
        )
        if not result or not isinstance(result, dict):
            return None

        print(f"[scraper] mtop result keys: {list(result.keys())[:15]}")
        extracted = {}

        for tk in ['titleModule', 'TITLE', 'GLOBAL_DATA']:
            tb = result.get(tk, {})
            if isinstance(tb, dict):
                t = (_s(tb.get('subject')) or _s(tb.get('title')) or
                     _s(_safe(tb, 'globalData', 'subject')))
                if t:
                    extracted['title'] = _clean_title(t)
                    break

        pm = result.get('priceModule') or result.get('PRICE') or {}
        if isinstance(pm, dict):
            extracted['price'] = _s(
                pm.get('formatedActivityPrice') or pm.get('formatedPrice') or
                pm.get('formattedPrice') or
                _safe(pm, 'minActivityAmount', 'formattedAmount')
            )

        im = result.get('imageModule') or result.get('IMAGE') or {}
        if isinstance(im, dict):
            for idx, url in enumerate((im.get('imagePathList') or [])[:20], 1):
                if url:
                    url = ('https:' + url) if str(url).startswith('//') else url
                    extracted[f'image_{idx}'] = url

        if not extracted.get('image_1'):
            tm = result.get('titleModule') or {}
            for idx, url in enumerate((tm.get('images') or [])[:20], 1):
                if url:
                    extracted[f'image_{idx}'] = url

        props = []
        for sk in ['PRODUCT_PROP_PC', 'specsModule', 'productPropComponent', 'skuModule']:
            sb = result.get(sk, {})
            if isinstance(sb, dict):
                raw = (sb.get('props') or sb.get('showedProps') or
                       sb.get('specsList') or sb.get('productSKUPropertyList') or [])
                if raw:
                    props = raw
                    print(f"[scraper] {len(props)} specs from '{sk}'")
                    break

        if props:
            extracted['specs_raw'] = {}
            for prop in props:
                if not isinstance(prop, dict):
                    continue
                name  = _s(prop.get('attrName') or prop.get('name')).lower()
                value = _s(prop.get('attrValue') or prop.get('value'))
                if not name or not value:
                    continue
                for field, keywords in SPEC_MAPPING.items():
                    if any(kw in name for kw in keywords):
                        extracted.setdefault(field, value)
                        break
                extracted['specs_raw'][name] = value

        for sk in ['storeModule', 'SHOP_CARD_PC', 'sellerModule', 'shopInfo']:
            sb = result.get(sk, {})
            if isinstance(sb, dict) and sb:
                seller = _parse_seller_block(sb)
                if seller.get('store_name') or seller.get('store_id'):
                    extracted.update(seller)
                    print(f"[scraper] Seller from mtop '{sk}': {list(seller.keys())}")
                    break

        cat_mod = result.get('categoryModule') or result.get('CATEGORY') or {}
        if isinstance(cat_mod, dict):
            cat_list = (cat_mod.get('categories') or cat_mod.get('breadcrumbList') or
                        cat_mod.get('list') or [])
            if cat_list and isinstance(cat_list, list):
                leaf = cat_list[-1]
                if isinstance(leaf, dict):
                    extracted['category_id']   = _s(leaf.get('categoryId') or leaf.get('id'))
                    extracted['category_name'] = _s(leaf.get('name') or leaf.get('title'))
                    extracted['category_path'] = ' > '.join(
                        _s(c.get('name') or c.get('title'))
                        for c in cat_list if isinstance(c, dict)
                    )

        fb = result.get('feedbackModule') or result.get('FEEDBACK') or {}
        if isinstance(fb, dict):
            extracted['rating']  = _s(fb.get('trialRating') or fb.get('averageStar'))
            extracted['reviews'] = _s(fb.get('trialNum') or fb.get('totalCount'))

        # ── Task 6: Stock remaining ───────────────────────────────────────
        stock_found = False
        for sk in ['quantityModule', 'QUANTITY', 'stockModule', 'tradeModule', 'TRADE']:
            sb = result.get(sk, {})
            if isinstance(sb, dict):
                stock_val = (
                    sb.get('availStock') or sb.get('totalStock') or
                    sb.get('stockCount') or sb.get('quantity') or
                    sb.get('inventory') or sb.get('availableQuantity') or
                    _safe(sb, 'skuStocks', 0, 'quantity')
                )
                if stock_val is not None:
                    extracted['stock_remaining'] = _s(stock_val)
                    stock_found = True
                    print(f"[scraper] Stock from '{sk}': {stock_val}")
                    break
        if not stock_found:
            extracted.setdefault('stock_remaining', '')

        dm = result.get('descriptionModule') or result.get('DESCRIPTION') or {}
        if isinstance(dm, dict):
            du = _s(dm.get('descriptionUrl'))
            if du:
                extracted['_description_url'] = du

        for path_fn in [
            lambda r: r.get('highlights') or _safe(r, 'highlightModule', 'highlightList'),
            lambda r: _safe(r, 'tradeModule', 'highlights'),
        ]:
            try:
                items = path_fn(result)
                if items and isinstance(items, list):
                    bullets = [_s(h.get('title') or h.get('text') or h) for h in items if h]
                    bullets = [b for b in bullets if b]
                    if bullets:
                        extracted['bullet_points'] = bullets[:10]
                        break
            except Exception:
                pass

        return extracted if extracted.get('title') else None

    except Exception as e:
        print(f"[scraper] mtop parse error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# HTML INIT_DATA PARSER (SSR fallback)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_from_html_init_data(html: str) -> dict:
    if not html:
        return {}
    try:
        start_marker = '/*!-->init-data-start--*/'
        end_marker   = '/*!-->init-data-end--*/'
        s = html.find(start_marker)
        e = html.find(end_marker)
        if s != -1 and e != -1:
            block  = html[s + len(start_marker):e]
            assign = block.find('window._dida_config_._init_data_=')
            if assign != -1:
                json_start = block.index('{', assign)
                depth = 0
                json_end = json_start
                for i, ch in enumerate(block[json_start:], json_start):
                    if ch == '{':
                        depth += 1
                    elif ch == '}':
                        depth -= 1
                        if depth == 0:
                            json_end = i + 1
                            break
                outer = json.loads(block[json_start:json_end])
                inner = _safe(outer, 'data', 'data') or _safe(outer, 'data') or outer
                print(f"[scraper] init_data keys: {list(inner.keys())[:10]}")
                return parse_mtop_response(
                    json.dumps({'data': {'result': inner}})
                ) or {}
    except Exception as e:
        print(f"[scraper] init_data parse error: {e}")
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# COMPLIANCE
# ─────────────────────────────────────────────────────────────────────────────

def _parse_compliance_text(text: str) -> dict:
    result = {}
    text   = text.replace('\r\n', '\n').replace('\r', '\n')

    def extract_field(label_patterns, src, stop_patterns=None):
        for pat in label_patterns:
            m = re.search(r'(?:' + re.escape(pat) + r')\s*[:\n]\s*([^\n]+)', src, re.IGNORECASE)
            if m:
                val = m.group(1).strip()
                if stop_patterns:
                    for sp in stop_patterns:
                        pos = re.search(re.escape(sp), val, re.IGNORECASE)
                        if pos:
                            val = val[:pos.start()].strip()
                return val
        return ''

    result['manufacturer_name']    = extract_field(['Name', 'Manufacturer Name'], text)
    result['manufacturer_address'] = extract_field(['Address', 'Adres'], text,
                                                    stop_patterns=['Email', 'Telephone', 'Phone'])
    result['manufacturer_email']   = extract_field(['Email address', 'Email'], text)
    result['manufacturer_phone']   = extract_field(['Telephone number', 'Phone', 'Tel'], text)

    eu_match = re.search(
        r'(Details of the person responsible|EU Representative|EU Responsible)',
        text, re.IGNORECASE
    )
    if eu_match:
        eu_text = text[eu_match.start():]
        result['eu_responsible_name']    = extract_field(['Name', 'Nazwa'], eu_text)
        result['eu_responsible_address'] = extract_field(['Address', 'Adres'], eu_text,
                                                          stop_patterns=['Email', 'Telephone'])
        result['eu_responsible_email']   = extract_field(['Email address', 'Email'], eu_text)
        result['eu_responsible_phone']   = extract_field(['Telephone number', 'Phone'], eu_text)

    m = re.search(r'(\d{13,20}(?:-\d{13,20})?)', text)
    if m:
        result['compliance_product_id'] = m.group(1)

    return {k: v for k, v in result.items() if v}


def _extract_compliance_info(page) -> dict:
    for sel in [
        'text="Product Compliance Information"',
        'span:has-text("Product Compliance Information")',
        '[class*="compliance"]',
    ]:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=2000):
                el.click(timeout=3000)
                page.wait_for_timeout(2000)
                break
        except Exception:
            continue

    try:
        page.wait_for_selector('.comet-v2-modal, .comet-modal', timeout=6000)
        page.wait_for_timeout(1000)
        modal = page.locator('.comet-v2-modal, .comet-modal').first
        if modal.count() == 0:
            return {}
        compliance = _parse_compliance_text(modal.inner_text())
        try:
            page.locator('.comet-v2-modal-close, .comet-modal-close').first.click(timeout=2000)
        except Exception:
            pass
        return compliance
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 — DELIVERY DAYS CALCULATOR
# ─────────────────────────────────────────────────────────────────────────────

def _delivery_days_from_text(delivery_text: str) -> dict:
    """
    Parse a delivery-time string like 'Pick up by Sunday, April 22 - 26'
    and return the max delivery days from today plus a within-16-days flag.

    Returns:
        {
          "raw":              "Pick up by Sunday, April 22 - 26",
          "max_delivery_days": 4,
          "within_16_days":   True,
          "note":             "Delivery OK (4 days)"    # or "Delivery above 16 days (X days)"
        }
    """
    import datetime
    result = {
        "raw": delivery_text,
        "max_delivery_days": None,
        "within_16_days": None,
        "note": "",
    }
    if not delivery_text:
        return result

    today = datetime.date.today()
    text  = delivery_text.strip()

    # Pattern: "April 22 - 26"  or  "Apr 22-26"  or  "April 22"
    month_names = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10,
        "november": 11, "december": 12,
    }

    # Try "Month DD - DD"
    m = re.search(
        r'([A-Za-z]+)\s+(\d{1,2})\s*[-–]\s*(\d{1,2})',
        text
    )
    if m:
        month_str = m.group(1).lower()
        day_start = int(m.group(2))
        day_end   = int(m.group(3))
        month_num = month_names.get(month_str)
        if month_num:
            year = today.year
            try:
                end_date = datetime.date(year, month_num, day_end)
                if end_date < today:
                    end_date = datetime.date(year + 1, month_num, day_end)
                days = (end_date - today).days
                result["max_delivery_days"] = days
                result["within_16_days"]    = days <= 16
                if days <= 16:
                    result["note"] = f"Delivery OK ({days} days)"
                else:
                    result["note"] = f"Delivery above 16 days ({days} days)"
                return result
            except ValueError:
                pass

    # Try "Month DD" (single date)
    m = re.search(r'([A-Za-z]+)\s+(\d{1,2})(?!\s*[-–]\s*\d)', text)
    if m:
        month_str = m.group(1).lower()
        day       = int(m.group(2))
        month_num = month_names.get(month_str)
        if month_num:
            year = today.year
            try:
                end_date = datetime.date(year, month_num, day)
                if end_date < today:
                    end_date = datetime.date(year + 1, month_num, day)
                days = (end_date - today).days
                result["max_delivery_days"] = days
                result["within_16_days"]    = days <= 16
                if days <= 16:
                    result["note"] = f"Delivery OK ({days} days)"
                else:
                    result["note"] = f"Delivery above 16 days ({days} days)"
                return result
            except ValueError:
                pass

    # Fallback: look for "X days" or "X-Y days"
    m = re.search(r'(\d+)\s*[-–]?\s*(\d*)\s*days?', text, re.IGNORECASE)
    if m:
        days = int(m.group(2) or m.group(1))
        result["max_delivery_days"] = days
        result["within_16_days"]    = days <= 16
        if days <= 16:
            result["note"] = f"Delivery OK ({days} days)"
        else:
            result["note"] = f"Delivery above 16 days ({days} days)"
        return result

    result["note"] = "Could not parse delivery date"
    return result


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 — SHIPPING EXTRACTION FOR TARGET COUNTRIES (PL, DE, CZ, AT, BG)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_shipping_by_countries(page, countries: dict = None) -> dict:
    """
    For each target country (PL, DE, CZ, AT, BG):
      1. Navigate to the product URL with ?shipToCountry=XX
      2. Read 'div.dynamic-shipping-line' elements (always visible on page — no popup needed)
         These carry "Free shipping · Ships from Germany" style text.
      3. Read 'table.dynamic-shipping-table' rows for cost + delivery time.
      4. If table is hidden, try clicking the shipping/delivery section to expand it.
      5. Apply Task-4 16-day check on the parsed delivery date.

    Returns:
        {
          "PL": {"country": "Poland", "cost": "Free delivery", "ships_from": "Germany",
                 "delivery_time": "Pick up by ...", "max_delivery_days": 4,
                 "within_16_days": True, "note": "Delivery OK (4 days)"},
          ...
        }
    """
    if countries is None:
        countries = SHIPPING_TARGET_COUNTRIES

    shipping_results = {}
    # Capture the base URL before we start mutating it per country
    base_url = page.url

    for country_code, country_name in countries.items():
        try:
            print(f"[scraper] Extracting shipping → {country_code} ({country_name})")

            # ── Step 1: navigate with shipToCountry param ─────────────────
            if re.search(r'[?&]shipToCountry=[A-Z]+', base_url):
                nav_url = re.sub(r'(shipToCountry=)[A-Z]+', rf'\g<1>{country_code}', base_url)
            else:
                sep     = '&' if '?' in base_url else '?'
                nav_url = f"{base_url}{sep}shipToCountry={country_code}"

            page.goto(nav_url, timeout=PAGE_TIMEOUT, wait_until='domcontentloaded')
            # Wait for shipping section to appear
            try:
                page.wait_for_selector(
                    'div.dynamic-shipping-line, table.dynamic-shipping-table',
                    timeout=12_000
                )
            except Exception:
                page.wait_for_timeout(5000)

            cost       = ""
            ships_from = ""
            delivery_time = ""

            # ── Step 2: read dynamic-shipping-line divs (no popup required) ──
            # These contain inline text like "Free shipping · Ships from Germany"
            try:
                lines = page.locator('div.dynamic-shipping-line').all()
                for line in lines[:8]:
                    try:
                        raw_text = line.inner_text().strip()
                        raw_text = re.sub(r'\s+', ' ', raw_text).strip()
                        if not raw_text:
                            continue
                        # Grab cost/free-shipping info from title-layout line
                        classes = line.get_attribute('class') or ''
                        if 'titleLayout' in classes or 'title' in classes.lower():
                            if not cost:
                                cost = raw_text
                        # Grab "Ships from" country
                        m = re.search(r'ships?\s+from\s+([A-Za-z ]+)', raw_text, re.IGNORECASE)
                        if m and not ships_from:
                            ships_from = m.group(1).strip()
                        # Also try to grab delivery date inline
                        if re.search(r'(pick up|deliver|arrival|expected)', raw_text, re.IGNORECASE):
                            if not delivery_time:
                                delivery_time = raw_text
                    except Exception:
                        continue
            except Exception as e:
                print(f"[scraper] dynamic-shipping-line error ({country_code}): {e}")

            # ── Step 3: read dynamic-shipping-table (structured rows) ─────
            def _read_shipping_table(scope):
                """Read cost + delivery_time from table rows within `scope`."""
                nonlocal cost, delivery_time
                try:
                    tables = scope.locator('table.dynamic-shipping-table').all()
                    for tbl in tables[:5]:
                        rows = tbl.locator('tr').all()
                        for row in rows:
                            th_el = row.locator('th')
                            td_el = row.locator('td')
                            if th_el.count() == 0 or td_el.count() == 0:
                                continue
                            th_text = re.sub(r'\s+', ' ', th_el.inner_text()).strip().lower()
                            td_text = re.sub(r'\s+', ' ', td_el.inner_text()).strip()
                            if not td_text:
                                continue
                            if 'cost' in th_text or 'price' in th_text or 'fee' in th_text:
                                if not cost:
                                    cost = td_text
                            elif ('delivery' in th_text or 'time' in th_text
                                  or 'arrival' in th_text or 'pick' in th_text.lower()):
                                if not delivery_time:
                                    delivery_time = td_text
                except Exception:
                    pass

            _read_shipping_table(page)

            # ── Step 4: if still missing, try expanding shipping section ──
            if not delivery_time:
                expand_selectors = [
                    # Clicking the first "Delivery method" text row expands the table
                    'div.dynamic-shipping-contentLayout',
                    'div[class*="dynamic-shipping"] div[class*="content"]',
                    'div[class*="shippingInfo"]',
                    'div[class*="delivery-info"]',
                    '[data-spm-anchor-id*="i27"]',
                    '[data-spm-anchor-id*="i4"]',
                ]
                for sel in expand_selectors:
                    try:
                        el = page.locator(sel).first
                        if el.count() > 0 and el.is_visible(timeout=1500):
                            el.click(timeout=2000)
                            page.wait_for_timeout(1500)
                            _read_shipping_table(page)
                            if delivery_time:
                                break
                    except Exception:
                        continue

            # ── Step 5: clean + Task-4 check ─────────────────────────────
            cost          = re.sub(r'<[^>]+>', '', cost).strip()
            delivery_time = re.sub(r'<[^>]+>', '', delivery_time).strip()

            delivery_info = _delivery_days_from_text(delivery_time)

            shipping_results[country_code] = {
                "country":           country_name,
                "cost":              cost          or "N/A",
                "ships_from":        ships_from    or "N/A",
                "delivery_time":     delivery_time or "N/A",
                "max_delivery_days": delivery_info["max_delivery_days"],
                "within_16_days":    delivery_info["within_16_days"],
                "note":              delivery_info["note"],
            }
            print(f"[scraper] Shipping {country_code}: cost='{cost}' | delivery='{delivery_time}'")

        except Exception as exc:
            print(f"[scraper] Shipping extraction error for {country_code}: {exc}")
            shipping_results[country_code] = {
                "country":           country_name,
                "cost":              "N/A",
                "ships_from":        "N/A",
                "delivery_time":     "N/A",
                "max_delivery_days": None,
                "within_16_days":    None,
                "note":              f"Error: {exc}",
            }

    return shipping_results



def _dismiss_gdpr_banner(page) -> bool:
    selectors = [
        'button:has-text("Accept All")', 'button:has-text("Accept all cookies")',
        'button:has-text("Agree")', 'button:has-text("I Accept")',
        '#accept-all', '.accept-all',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=1500):
                el.click(timeout=2000)
                page.wait_for_timeout(800)
                return True
        except Exception:
            continue
    return False


def _dismiss_banners(page) -> None:
    _dismiss_gdpr_banner(page)


def _is_eu_url(url: str) -> bool:
    eu_domains = ['pl.aliexpress.com', 'de.aliexpress.com', 'fr.aliexpress.com',
                  'it.aliexpress.com', 'es.aliexpress.com', 'nl.aliexpress.com']
    return any(d in url for d in eu_domains)


def _detect_eu_page(url: str, html_snippet: str) -> bool:
    indicators = ['gdpr', 'cookie-consent', 'Trader', 'DSA',
                  'de.aliexpress.com', 'fr.aliexpress.com']
    return any(ind in (url + html_snippet[:5000]) for ind in indicators)


# ─────────────────────────────────────────────────────────────────────────────
# DESCRIPTION FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_description(url: str) -> str:
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': random.choice(USER_AGENTS),
                     'Referer': 'https://www.aliexpress.com/'}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode('utf-8', errors='replace')
        clean = re.sub(r'<style[^>]*>.*?</style>', ' ', raw, flags=re.DOTALL)
        clean = re.sub(r'<script[^>]*>.*?</script>', ' ', clean, flags=re.DOTALL)
        clean = re.sub(r'<[^>]+>', ' ', clean)
        return re.sub(r'\s+', ' ', clean).strip()[:3000]
    except Exception as e:
        print(f"[scraper] Description fetch failed: {e}")
        return ''


# ─────────────────────────────────────────────────────────────────────────────
# PRODUCT DETAIL SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_in_thread(url: str, try_compliance: bool = False) -> dict:
    captured   = []
    html       = ''
    dom_seller = {}
    compliance = {}
    ua         = random.choice(USER_AGENTS)
    is_eu      = _is_eu_url(url)

    try:
        with Camoufox(headless=True, os='windows') as browser:
            context = browser.new_context(
                viewport={'width': 1440, 'height': 900},
                locale='en-US',
                user_agent=ua,
                extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'}
            )
            page = context.new_page()

            def handle_response(response):
                try:
                    resp_url = response.url
                    if (('mtop.aliexpress.pdp.pc.query' in resp_url or
                         'pdp.pc.query' in resp_url) and response.status == 200):
                        body = response.body()
                        if len(body) < 1000:
                            return
                        text = body.decode('utf-8', errors='replace')
                        if any(x in text for x in ['titleModule', 'imageModule', '"subject"']):
                            captured.append(text)
                            print(f"[scraper] Captured API ({len(text):,} bytes)")
                except Exception:
                    pass

            page.on('response', handle_response)
            page.goto(url, timeout=90_000, wait_until='domcontentloaded')

            detected_eu = _detect_eu_page(page.url, page.content()[:5000]) or is_eu
            if detected_eu:
                page.wait_for_timeout(2000)
                _dismiss_gdpr_banner(page)
                page.wait_for_timeout(1000)

            page.wait_for_timeout(random.randint(5000, 7000))
            for _ in range(4):
                page.mouse.wheel(0, random.randint(400, 800))
                page.wait_for_timeout(random.randint(500, 900))
            page.wait_for_timeout(2000)

            # Simple seller extraction from href
            try:
                for link in page.locator('a[href*="/store/"]').all()[:5]:
                    href = link.get_attribute('href') or ''
                    m = re.search(r'/store/(\d+)', href)
                    if m:
                        dom_seller['store_id'] = m.group(1)
                        dom_seller['store_url'] = f"https://www.aliexpress.com/store/{m.group(1)}"
                        break
            except Exception:
                pass

            if try_compliance:
                compliance = _extract_compliance_info(page)

            # ── Task 3 & 4: Extract shipping info for target EU countries ─
            shipping_by_country = {}
            try:
                shipping_by_country = _extract_shipping_by_countries(page)
            except Exception as _ship_exc:
                print(f"[scraper] Shipping-by-country extraction failed: {_ship_exc}")

            html = page.content()
            page.close()
            context.close()

    except Exception as e:
        print(f"[scraper] Browser error: {e}")
        import traceback
        traceback.print_exc()

    return {'captured': captured, 'html': html,
            'dom_seller': dom_seller, 'compliance': compliance,
            'shipping_by_country': shipping_by_country}


def _scrape_with_retry(url: str, try_compliance: bool = False) -> dict:
    best = {'captured': [], 'html': '', 'dom_seller': {}, 'compliance': {}}

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n[scraper] Attempt {attempt}/{MAX_RETRIES}")
        if attempt > 1:
            time.sleep(random.uniform(4, 9))

        attempt_url = _get_rotated_url(url)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                result = ex.submit(
                    _scrape_in_thread, attempt_url,
                    try_compliance and attempt == 1
                ).result(timeout=200)
        except Exception as e:
            print(f"[scraper] Attempt {attempt} error: {e}")
            continue

        for text in sorted(result['captured'], key=len, reverse=True):
            parsed = parse_mtop_response(text)
            if parsed and parsed.get('title'):
                result['best_parsed'] = parsed
                return result

        if len(result['captured']) >= len(best['captured']):
            best = result

    return best


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY RESOLVER
# ─────────────────────────────────────────────────────────────────────────────

def resolve_category(extracted: dict) -> dict:
    cat_id   = extracted.get('category_id', '').split(',')[-1].strip()
    cat_name = extracted.get('category_name', '')
    cat_path = extracted.get('category_path', '')
    resolved = CATEGORY_ID_MAP.get(cat_id, '') or cat_name
    if not resolved and cat_path:
        resolved = cat_path.split(' > ')[-1].strip()
    return {
        'category_id':   cat_id or '0',
        'category_name': resolved or 'Uncategorized',
        'category_leaf': cat_name or (cat_path.split(' > ')[-1] if cat_path else 'Uncategorized'),
        'category_path': cat_path,
        'confidence':    0.95 if cat_id and cat_id != '0' else (0.7 if cat_name else 0.3),
    }


resolve_category_from_init_data = resolve_category


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PUBLIC FUNCTION — product detail
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str, extract_compliance: bool = True) -> dict | None:
    print(f"\n[scraper] Starting: {url}")

    data      = _scrape_with_retry(url, try_compliance=extract_compliance)
    extracted = {}

    if data.get('best_parsed'):
        extracted = data['best_parsed']
    else:
        for text in sorted(data['captured'], key=len, reverse=True):
            parsed = parse_mtop_response(text)
            if parsed:
                for k, v in parsed.items():
                    if v and k not in extracted:
                        extracted[k] = v
                if extracted.get('title'):
                    break

    if not extracted.get('title') and data.get('html'):
        for k, v in _extract_from_html_init_data(data['html']).items():
            if v and k not in extracted:
                extracted[k] = v

    if not extracted.get('title') and data.get('html'):
        m = re.search(
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']{5,300})["\']',
            data['html']
        )
        if m:
            extracted['title'] = _clean_title(m.group(1).strip())

    if not extracted.get('title'):
        return None

    extracted['title'] = _clean_title(extracted['title'])

    for k, v in data.get('dom_seller', {}).items():
        if v and not extracted.get(k):
            extracted[k] = v

    if data.get('compliance'):
        extracted['compliance'] = data['compliance']

    # Task 3: attach shipping-by-country data collected during browser session
    if data.get('shipping_by_country'):
        extracted['shipping_by_country'] = data['shipping_by_country']

    desc_url = extracted.pop('_description_url', '')
    if desc_url and not extracted.get('description'):
        extracted['description'] = fetch_description(desc_url)

    defaults = {
        'description': '', 'brand': '', 'color': '', 'dimensions': '',
        'weight': '', 'material': '', 'certifications': '',
        'country_of_origin': '', 'warranty': '', 'product_type': '',
        'shipping': '', 'price': '', 'rating': '', 'reviews': '',
        'bullet_points': [], 'age_from': '', 'age_to': '',
        'gender': '', 'safety_warning': '',
        'store_name': '', 'store_id': '', 'store_url': '',
        'seller_id': '', 'seller_positive_rate': '', 'seller_rating': '',
        'seller_communication': '', 'seller_shipping_speed': '',
        'seller_country': '', 'store_open_date': '', 'seller_level': '',
        'seller_total_reviews': '', 'seller_positive_num': '', 'is_top_rated': '',
        'category_id': '', 'category_name': '', 'category_path': '',
        'compliance': {},
        # Task 6: stock
        'stock_remaining': '',
        # Task 3: shipping per country (populated after DOM phase)
        'shipping_by_country': {},
    }
    for key, default in defaults.items():
        extracted.setdefault(key, default)
    for i in range(1, 21):
        extracted.setdefault(f'image_{i}', '')

    return extracted


# =============================================================================
# SEARCH-RESULTS SCRAPER
# =============================================================================

def _normalize_product_url(product_id: str) -> str:
    return f"https://www.aliexpress.com/item/{product_id}.html" if product_id else ""


def _extract_product_id(href: str) -> Optional[str]:
    if not href:
        return None
    for pat in [
        r'/item/(\d{10,20})(?:\.html)?',
        r'[?&]productIds?=(\d{10,20})',
        r'[Ii]tem[Ii]d(?:%3D|=)(\d{10,20})',
    ]:
        m = re.search(pat, href)
        if m:
            return m.group(1)
    return None


def _find_item_list_recursive(data, depth: int = 0) -> Optional[list]:
    if depth > 10 or not isinstance(data, (dict, list)):
        return None
    if isinstance(data, dict):
        if 'itemList' in data:
            content = data['itemList']
            if isinstance(content, dict):
                content = content.get('content', [])
            if isinstance(content, list) and content:
                return content
        for v in data.values():
            result = _find_item_list_recursive(v, depth + 1)
            if result:
                return result
    elif isinstance(data, list):
        for item in data[:5]:
            result = _find_item_list_recursive(item, depth + 1)
            if result:
                return result
    return None


def _extract_products_from_page(page) -> List[Dict]:
    """
    4-layer extraction: multi-path JSON → DOM card divs → DOM anchors → HTML regex.
    Returns list with keys: product_id, product_url, title, rating, sold_count

    KEY FIXES:
    - Tries window.__INIT_DATA__, window.runParams, window._dida_config_._init_data_
      so Polish/EU AliExpress pages (which don't use _dida_config_) are covered.
    - Does NOT return early from JSON layer unless 10+ products found — falls through
      to DOM layers to combine results when JSON only returns sponsored/partial set.
    - Layer 2 uses 'div.lw_v' card structure (the real search card on pl.aliexpress.com).
    - Extracts rating (.lw_km) and sold count (.lw_kk) from DOM.
    """
    products: List[Dict] = []
    seen_ids: set = set()

    # ── Helper: parse one JSON item dict ─────────────────────────────────────
    def _parse_item(item: dict) -> Optional[Dict]:
        if not isinstance(item, dict):
            return None
        pid = str(
            item.get('productId', '') or item.get('redirectedId', '') or
            item.get('itemId', '')    or item.get('id', '')
        ).strip()
        if not pid or pid in seen_ids:
            return None

        title_raw = (
            _safe_get(item, 'title', 'displayTitle') or
            _safe_get(item, 'title', 'seoTitle')     or
            item.get('title') or item.get('productTitle') or item.get('name') or ''
        )
        if isinstance(title_raw, dict):
            title_raw = title_raw.get('displayTitle', '') or title_raw.get('title', '')
        title = _clean_title(str(title_raw).strip())

        # Task 5: rating from JSON
        rating_raw = (
            _safe_get(item, 'starRating')             or
            _safe_get(item, 'averageStar')            or
            _safe_get(item, 'feedback', 'starRating') or
            _safe_get(item, 'trade', 'starRating')    or
            _safe_get(item, 'ratings', 'averageScore') or ''
        )
        # Task 6 (search): sold count from JSON
        sold_raw = (
            _safe_get(item, 'trade', 'tradeCount') or
            _safe_get(item, 'tradeCount')          or
            _safe_get(item, 'sold')                or
            _safe_get(item, 'salesCount')          or ''
        )
        seen_ids.add(pid)
        return {
            'product_id':  pid,
            'product_url': _normalize_product_url(pid),
            'title':       title,
            'rating':      _s(rating_raw),
            'sold_count':  _s(sold_raw),
        }

    # ── Layer 1: JSON — try ALL three window-level data sources ──────────────
    json_js = """() => {
        const results = {};
        try {
            if (window.__INIT_DATA__) results.INIT_DATA = JSON.stringify(window.__INIT_DATA__);
        } catch(e) {}
        try {
            if (window.runParams) results.runParams = JSON.stringify(window.runParams);
        } catch(e) {}
        try {
            const cfg = window._dida_config_;
            if (cfg && cfg._init_data_) results.dida = JSON.stringify(cfg._init_data_);
        } catch(e) {}
        return JSON.stringify(results);
    }"""
    try:
        raw_sources = page.evaluate(json_js)
        sources = json.loads(raw_sources) if raw_sources else {}

        for src_name, src_json in sources.items():
            if not src_json:
                continue
            try:
                data = json.loads(src_json)
            except Exception:
                continue

            item_list = (
                _safe_get(data, 'data', 'root', 'fields', 'mods', 'itemList', 'content') or
                _safe_get(data, 'data', 'data', 'root', 'fields', 'mods', 'itemList', 'content') or
                _safe_get(data, 'result', 'mods', 'itemList', 'content') or
                _safe_get(data, 'itemList', 'content') or
                _safe_get(data, 'data', 'itemList', 'content') or
                _find_item_list_recursive(data) or []
            )
            for item in item_list:
                p = _parse_item(item)
                if p:
                    products.append(p)

            if products:
                print(f"[search_scraper] JSON ({src_name}): {len(products)} products")
                break  # found items — no need to try next source

    except Exception as e:
        print(f"[search_scraper] JSON layer error: {e}")

    # Only skip DOM layers if JSON gave a full page (≥ 10 products)
    if len(products) >= 10:
        return products

    # ── Layer 2: DOM — div.lw_v cards + classic anchor selectors ─────────────
    # Scroll first so lazy-loaded cards are rendered
    try:
        for _ in range(3):
            page.mouse.wheel(0, 1200)
            page.wait_for_timeout(400)
        page.mouse.wheel(0, -9999)   # back to top so links are reachable
        page.wait_for_timeout(600)
    except Exception:
        pass

    try:
        # Primary: product-card wrappers that contain an item link
        card_selectors = [
            # Polish / EU AliExpress search card
            'div.lw_v',
            # Classic selector
            'a.search-card-item, a.lw_b.h7_ic.search-card-item, [class*="search-card-item"]',
        ]

        for card_sel in card_selectors:
            cards = page.locator(card_sel).all()
            if not cards:
                continue

            for card in cards:
                try:
                    # Find the product link inside / as the card itself
                    href = ''
                    pid  = ''

                    # If card is an <a>, use it directly
                    tag = card.evaluate('el => el.tagName').lower()
                    if tag == 'a':
                        href = card.get_attribute('href') or ''
                        pid  = _extract_product_id(href)
                    else:
                        # Otherwise find the first item anchor inside
                        for link_sel in [
                            'a[href*="/item/"]',
                            'a[href*="aliexpress.com/item/"]',
                            'a',
                        ]:
                            link = card.locator(link_sel).first
                            if link.count() > 0:
                                href = link.get_attribute('href') or ''
                                pid  = _extract_product_id(href)
                                if pid:
                                    break

                    if not pid or pid in seen_ids:
                        continue

                    # Title
                    title = ''
                    for title_sel in [
                        'h3.lw_k4', '[class*="lw_k4"]',
                        '[role="heading"] h3', 'h3',
                        '[class*="title"]',
                    ]:
                        try:
                            te = card.locator(title_sel).first
                            if te.count() > 0:
                                raw = te.inner_text().strip()
                                if raw:
                                    title = _clean_title(raw)
                                    break
                        except Exception:
                            pass
                    if not title:
                        title = _clean_title(card.get_attribute('aria-label') or '')

                    # Rating (.lw_km)
                    rating = ''
                    try:
                        rel = card.locator('.lw_km, [class*="lw_km"]').first
                        if rel.count() > 0:
                            rating = rel.inner_text().strip()
                    except Exception:
                        pass

                    # Sold count (.lw_kk) — Task 6 (search level)
                    sold_count = ''
                    try:
                        sel_el = card.locator('.lw_kk, [class*="lw_kk"]').first
                        if sel_el.count() > 0:
                            raw_sold = sel_el.inner_text().strip()
                            # "81 sold" → keep as-is; also handle "373 sold"
                            m_sold = re.search(r'(\d[\d,]*)\s*sold', raw_sold, re.IGNORECASE)
                            sold_count = m_sold.group(1).replace(',', '') if m_sold else raw_sold
                    except Exception:
                        pass

                    seen_ids.add(pid)
                    products.append({
                        'product_id':  pid,
                        'product_url': _normalize_product_url(pid),
                        'title':       title[:300],
                        'rating':      rating,
                        'sold_count':  sold_count,
                    })
                except Exception:
                    continue

            if len(products) >= 10:
                break  # got enough from this selector set

        if products:
            print(f"[search_scraper] DOM: {len(products)} products")
            return products
    except Exception as e:
        print(f"[search_scraper] DOM error: {e}")

    # ── Layer 3: HTML regex fallback ─────────────────────────────────────────
    try:
        html = page.content()
        for pat in [
            r'href=["\'](?:https?:)?//[^"\']*?aliexpress\\.com/item/(\\d{10,20})\\.html',
            r'href=[^"\']*?/item/(\\d{10,20})\\.html',
            r'"productId"\\s*:\\s*"(\\d{10,20})"',
            r'"redirectedId"\\s*:\\s*"(\\d{10,20})"',
        ]:
            for pid in re.findall(pat, html):
                if pid not in seen_ids:
                    seen_ids.add(pid)
                    products.append({
                        'product_id':  pid,
                        'product_url': _normalize_product_url(pid),
                        'title':       '',
                        'rating':      '',
                        'sold_count':  '',
                    })
        if products:
            print(f"[search_scraper] HTML regex: {len(products)} products")
    except Exception as e:
        print(f"[search_scraper] HTML regex error: {e}")

    return products


def _build_page_url(base_url: str, page_num: int) -> str:
    """
    Build paginated URL for any page number.
    Replaces existing page= param or appends one.
    """
    if page_num == 1:
        # Remove any existing page= so page 1 is canonical
        url = re.sub(r'[?&]page=\d+', '', base_url).rstrip('?&')
        return url
    if re.search(r'[?&]page=\d+', base_url):
        return re.sub(r'([?&]page=)\d+', rf'\g<1>{page_num}', base_url)
    sep = '&' if '?' in base_url else '?'
    return f"{base_url}{sep}page={page_num}"


def _wait_for_page_load(page, timeout: int = 12_000) -> bool:
    try:
        page.wait_for_load_state('networkidle', timeout=8_000)
    except Exception:
        pass
    try:
        page.wait_for_selector(
            '#card-list, .hm_hn, a.search-card-item, '
            'a.lw_b.h7_ic.search-card-item, [class*="search-card-item"]',
            timeout=timeout
        )
        page.wait_for_timeout(random.randint(800, 1500))
        return True
    except Exception:
        print("[search_scraper] Card selector not found — attempting extraction anyway")
        return True


def scrape_search_results(
    search_url:   str,
    max_pages:    int   = 5,
    max_products: int   = 0,
    deduplicate:  bool  = True,
    delay:        float = 1.5,
) -> Dict:
    """
    Scrape AliExpress search pages via DIRECT URL NAVIGATION for every page.

    CRITICAL DESIGN: This function ALWAYS navigates all max_pages by building
    the URL explicitly (_build_page_url). It does NOT rely on pagination DOM
    buttons, which was the root cause of the "33 products / early stop" bug.
    The _has_next_page check is removed as a hard stop — all pages are visited
    unless they genuinely return zero products (indicating the site ran out).

    Returns:
        {
          "products":       List[{product_id, product_url, title}],
          "total_products": int,
          "pages_scraped":  int,
          "search_url":     str,
        }
    """
    max_pages     = min(max_pages, MAX_SEARCH_PAGES)
    all_products: List[Dict] = []
    seen_ids:     set        = set()
    pages_scraped             = 0
    ua                        = random.choice(USER_AGENTS)

    print(f"\n[search_scraper] Starting: {search_url[:100]}")
    print(f"[search_scraper] Target: {max_pages} pages | Products cap: {max_products or 'unlimited'}")

    try:
        with Camoufox(headless=True, os='windows') as browser:
            context = browser.new_context(
                viewport={'width': 1440, 'height': 900},
                locale='en-US',
                user_agent=ua,
                extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'}
            )
            page = context.new_page()

            for page_num in range(1, max_pages + 1):
                print(f"\n[search_scraper] ══ Page {page_num}/{max_pages} ══")

                nav_url = _build_page_url(search_url, page_num)
                print(f"[search_scraper] URL: {nav_url[:110]}")

                try:
                    page.goto(nav_url, timeout=SEARCH_TIMEOUT, wait_until='domcontentloaded')
                    _wait_for_page_load(page)
                except Exception as e:
                    print(f"[search_scraper] Navigation error page {page_num}: {e}")
                    # Don't break — try next page
                    pages_scraped = page_num
                    continue

                # Dismiss banners on page 1
                if page_num == 1:
                    _dismiss_banners(page)
                    page.wait_for_timeout(800)

                # Extract products BEFORE any scrolling
                page_products = _extract_products_from_page(page)

                new_count = 0
                for prod in page_products:
                    pid = prod['product_id']
                    if deduplicate and pid in seen_ids:
                        continue
                    seen_ids.add(pid)
                    all_products.append({
                        'product_id':  prod['product_id'],
                        'product_url': prod['product_url'],
                        'title':       prod.get('title', ''),
                        'rating':      prod.get('rating', ''),
                        'sold_count':  prod.get('sold_count', ''),
                    })
                    new_count += 1

                pages_scraped = page_num
                print(f"[search_scraper] Page {page_num}: +{new_count} new products | "
                      f"running total = {len(all_products)}")

                # Only stop early if the page genuinely had zero new products
                # AND it's not the first page (first page can be slow)
                if new_count == 0 and page_num > 1:
                    print(f"[search_scraper] Page {page_num} returned 0 new products — "
                          f"site may have run out of results. Stopping.")
                    break

                # Hard product cap (if requested)
                if max_products > 0 and len(all_products) >= max_products:
                    print(f"[search_scraper] Product cap ({max_products}) reached")
                    break

                # Polite delay between pages (skip after last page)
                if page_num < max_pages:
                    sleep_time = random.uniform(delay, delay * 1.5)
                    print(f"[search_scraper] Waiting {sleep_time:.1f}s...")
                    time.sleep(sleep_time)

            page.close()
            context.close()

    except Exception as e:
        print(f"[search_scraper] Browser session error: {e}")
        import traceback
        traceback.print_exc()

    if max_products > 0:
        all_products = all_products[:max_products]

    print(f"\n[search_scraper] ✓ Complete: {len(all_products)} products from {pages_scraped} pages")

    return {
        "products":       all_products,
        "total_products": len(all_products),
        "pages_scraped":  pages_scraped,
        "search_url":     search_url,
    }


# =============================================================================
# CLI
# =============================================================================

if __name__ == '__main__':
    import sys
    args = sys.argv[1:]

    if args and ('aliexpress.com/w/' in args[0] or 'SearchText=' in args[0]):
        result = scrape_search_results(args[0], max_pages=int(args[1]) if len(args) > 1 else 5)
        print(f"\nTotal: {result['total_products']} products from {result['pages_scraped']} pages")
        for p in result['products'][:20]:
            print(f"  [{p['product_id']}] {p['title'][:70]}")
    else:
        test_url = args[0] if args else "https://www.aliexpress.com/item/1005010089125608.html"
        result = get_product_info(test_url, extract_compliance=True)
        if result:
            print("\n" + "=" * 70)
            for k, v in sorted(result.items()):
                if k.startswith('image_') and not v:
                    continue
                print(f"  {k:30}: {str(v)[:100]}")
        else:
            print("FAILED")
