"""
scraper.py v6.4
────────────────
Changes vs v6.3:
  - Correct AliExpress search-page selectors (from live HTML analysis)
  - Pagination hardened: _build_page_url, _has_next_page, _get_total_pages
  - scrape_search_results: default max_pages=5, extracting title/product_id/url only
  - init_data path fixed to also look at root-level 'itemList' content
  - _extract_products_from_page: stricter selector ordering per live HTML
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

        # ── Title extraction — covers ALL known AliExpress PDP formats ──
        # Order matters: try most reliable paths first.
        title_found = None

        # 1. Classic mtop module keys
        for tk in ['titleModule', 'TITLE', 'GLOBAL_DATA']:
            tb = result.get(tk, {})
            if isinstance(tb, dict):
                t = (_s(tb.get('subject')) or _s(tb.get('title')) or
                     _s(_safe(tb, 'globalData', 'subject')))
                if t:
                    title_found = t
                    break

        # 2. PDP v2 / Polish storefront — result['data']['subject']
        if not title_found:
            for path in [
                ('data', 'subject'),
                ('data', 'title'),
                ('productInfoComponent', 'subject'),
                ('productInfoComponent', 'title'),
                ('commonModule', 'subject'),
                ('commonModule', 'title'),
                ('webEnv', 'subject'),
                ('webEnv', 'title'),
                ('metaModule', 'title'),
                ('metaModule', 'subject'),
                ('globalData', 'subject'),
                ('globalData', 'title'),
                ('pageData', 'subject'),
                ('pageData', 'title'),
                ('productDetail', 'subject'),
                ('productDetail', 'title'),
                ('itemDetailComponent', 'subject'),
                ('itemDetailComponent', 'title'),
            ]:
                t = _s(_safe(result, *path))
                if t and len(t) > 4:
                    title_found = t
                    print(f"[scraper] Title found via path {path}")
                    break

        # 3. Direct top-level 'subject' or 'title' on result
        if not title_found:
            title_found = _s(result.get('subject')) or _s(result.get('title'))

        # 4. Recursive search for 'subject' key anywhere in the result
        if not title_found:
            def _find_subject(obj, depth=0):
                if depth > 6 or not isinstance(obj, (dict, list)):
                    return None
                if isinstance(obj, dict):
                    for k in ('subject', 'displayTitle', 'productTitle'):
                        v = _s(obj.get(k, ''))
                        if v and len(v) > 4:
                            return v
                    for v in obj.values():
                        found = _find_subject(v, depth + 1)
                        if found:
                            return found
                elif isinstance(obj, list):
                    for item in obj[:5]:
                        found = _find_subject(item, depth + 1)
                        if found:
                            return found
                return None
            title_found = _find_subject(result)
            if title_found:
                print(f"[scraper] Title found via recursive subject search")

        if title_found:
            extracted['title'] = _clean_title(title_found)

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

    results = {}

    # ── Strategy A: init-data-start/end markers ───────────────────────────
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
                # Try multiple inner paths — PDP v2 stores data differently
                for inner_path in [
                    ('data', 'data'),
                    ('data',),
                    (),
                ]:
                    inner = _safe(outer, *inner_path) if inner_path else outer
                    if inner and isinstance(inner, dict):
                        parsed = parse_mtop_response(
                            json.dumps({'data': {'result': inner}})
                        )
                        if parsed and parsed.get('title'):
                            print(f"[scraper] init_data path {inner_path}: title found")
                            return parsed
                        if parsed:
                            results.update(parsed)
    except Exception as e:
        print(f"[scraper] init_data parse error: {e}")

    # ── Strategy B: window._dida_config_ anywhere in the page ────────────
    try:
        m = re.search(r'window\._dida_config_\s*=\s*(\{)', html)
        if m:
            start = m.start(1)
            depth = 0
            end   = start
            for i, ch in enumerate(html[start:], start):
                if ch == '{':
                    depth += 1
                elif ch == '}':
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            outer  = json.loads(html[start:end])
            inner  = (_safe(outer, '_init_data_', 'data', 'data') or
                      _safe(outer, '_init_data_', 'data') or
                      _safe(outer, 'data', 'data') or
                      _safe(outer, 'data') or outer)
            if isinstance(inner, dict):
                parsed = parse_mtop_response(json.dumps({'data': {'result': inner}}))
                if parsed and parsed.get('title'):
                    print("[scraper] init_data Strategy B: title found")
                    return parsed
                if parsed:
                    results.update(parsed)
    except Exception as e:
        print(f"[scraper] init_data Strategy B error: {e}")

    # ── Strategy C: scan for "subject" anywhere in embedded JSON blobs ───
    if not results.get('title'):
        for m in re.finditer(r'"subject"\s*:\s*"([^"]{5,300})"', html):
            t = _clean_title(m.group(1))
            if t and 'aliexpress' not in t.lower():
                results['title'] = t
                print(f"[scraper] init_data Strategy C: subject = {t[:60]}")
                break

    return results


# ─────────────────────────────────────────────────────────────────────────────
# COMPLIANCE MODAL EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────

def _parse_compliance_text(text: str) -> dict:
    result = {}
    text   = text.replace('\r\n', '\n').replace('\r', '\n')

    def extract_field(label_patterns: list, src: str, stop_patterns: list = None) -> str:
        for pat in label_patterns:
            m = re.search(
                r'(?:' + re.escape(pat) + r')\s*[:\n]\s*([^\n]+)',
                src, re.IGNORECASE
            )
            if m:
                val = m.group(1).strip()
                if stop_patterns:
                    for sp in stop_patterns:
                        pos = re.search(re.escape(sp), val, re.IGNORECASE)
                        if pos:
                            val = val[:pos.start()].strip()
                return val
        return ''

    result['manufacturer_name']    = extract_field(
        ['Name', 'Imie i nazwisko', 'Manufacturer Name'], text)
    result['manufacturer_address'] = extract_field(
        ['Address', 'Adres'], text,
        stop_patterns=['Email', 'Telephone', 'Phone', 'Numer', 'Adres e-mail'])
    result['manufacturer_email']   = extract_field(
        ['Email address', 'Adres e-mail', 'Email'], text)
    result['manufacturer_phone']   = extract_field(
        ['Telephone number', 'Numer telefonu', 'Phone', 'Tel'], text)

    eu_match = re.search(
        r'(Details of the person responsible|person responsible for compliance in the EU|'
        r'Osoba odpowiedzialna|EU Representative|EU Responsible)',
        text, re.IGNORECASE
    )
    if eu_match:
        eu_text = text[eu_match.start():]
        result['eu_responsible_name']    = extract_field(
            ['Name', 'Imie i nazwisko', 'Nazwa'], eu_text)
        result['eu_responsible_address'] = extract_field(
            ['Address', 'Adres'], eu_text,
            stop_patterns=['Email', 'Telephone', 'Numer', 'Adres e-mail'])
        result['eu_responsible_email']   = extract_field(
            ['Email address', 'Adres e-mail', 'Email'], eu_text)
        result['eu_responsible_phone']   = extract_field(
            ['Telephone number', 'Numer telefonu', 'Phone'], eu_text)

    pid_m = re.search(r'Product ID[^\n]*\n\s*([0-9][-0-9]+)', text, re.IGNORECASE)
    if pid_m:
        result['compliance_product_id'] = pid_m.group(1).strip()
    else:
        m = re.search(r'(\d{13,20}(?:-\d{13,20})?)', text)
        if m:
            result['compliance_product_id'] = m.group(1)

    return {k: v for k, v in result.items() if v}


def _extract_compliance_info(page) -> dict:
    compliance = {}
    selectors = [
        'text="Product Compliance Information"',
        'span:has-text("Product Compliance Information")',
        'a:has-text("Product Compliance Information")',
        'text="Informacje o zgodnosci produktu"',
        'span:has-text("Informacje o zgodnosci")',
        '[data-spm-anchor-id*="i7"]',
        '[class*="compliance"]',
        '[class*="Compliance"]',
    ]
    clicked = False
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=2000):
                el.click(timeout=3000)
                page.wait_for_timeout(2000)
                print(f"[scraper] Clicked compliance: {sel}")
                clicked = True
                break
        except Exception:
            continue

    if not clicked:
        print("[scraper] No compliance link found")
        return {}

    try:
        page.wait_for_selector('.comet-v2-modal, .comet-modal', timeout=6000)
        page.wait_for_timeout(1000)
        modal = page.locator('.comet-v2-modal, .comet-modal').first
        if modal.count() == 0:
            return {}
        modal_text = modal.inner_text()
        print(f"[scraper] Compliance modal ({len(modal_text)} chars)")
        compliance = _parse_compliance_text(modal_text)
        print(f"[scraper] Compliance fields: {list(compliance.keys())}")
    except Exception as e:
        print(f"[scraper] Compliance modal error: {e}")

    try:
        page.locator(
            '.comet-v2-modal-close, .comet-modal-close, button[aria-label="Close"]'
        ).first.click(timeout=2000)
    except Exception:
        pass

    return compliance


# ─────────────────────────────────────────────────────────────────────────────
# SELLER LABEL MAPPER
# ─────────────────────────────────────────────────────────────────────────────

def _map_seller_label(key: str, value: str, seller: dict):
    if not value:
        return
    if any(k in key for k in ['name', 'store name', 'nom', 'nombre', 'naam', 'nome', 'nazwa']):
        seller.setdefault('store_name', value)
    elif any(k in key for k in ['no.', 'number', 'store id', 'numero', 'nr', 'store no']):
        if re.match(r'^\d+$', value.strip()):
            seller.setdefault('store_id', value.strip())
            seller.setdefault('store_url',
                              f"https://www.aliexpress.com/store/{value.strip()}")
    elif any(k in key for k in ['location', 'country', 'pays', 'pais', 'land',
                                'paese', 'kraj']):
        seller.setdefault('seller_country', value.strip())
    elif any(k in key for k in ['open', 'since', 'date', 'ouvert', 'abierto',
                                'data otwarcia']):
        seller.setdefault('store_open_date', value)


def _extract_seller_from_popup(page, is_eu_page: bool = False) -> dict:
    seller: dict = {}

    try:
        for link in page.locator('a[href*="/store/"]').all()[:5]:
            href = link.get_attribute('href') or ''
            if '/store/' in href:
                if href.startswith('//'):
                    href = 'https:' + href
                m = re.search(r'/store/(\d+)', href)
                if m:
                    seller['store_id']  = m.group(1)
                    seller['store_url'] = f"https://www.aliexpress.com/store/{m.group(1)}"
                    print(f"[scraper] Store ID from link: {m.group(1)}")
                    break
    except Exception:
        pass

    for sel in [
        '[class*="store-header--storeName"]', '[class*="shopName"]',
        '[class*="shop-name"]',               '[class*="sellerName"]',
        '[class*="StoreName"]',               '[class*="storeInfo"] h3',
        'a[href*="/store/"] span',            '[class*="store-header"] span',
    ]:
        try:
            el = page.locator(sel).first
            if el.count() > 0:
                text = el.inner_text().strip()
                if text and 2 < len(text) < 100:
                    seller['store_name'] = text
                    print(f"[scraper] Store name from DOM: {text}")
                    break
        except Exception:
            continue

    return {k: v for k, v in seller.items() if v}


# ─────────────────────────────────────────────────────────────────────────────
# GDPR / COOKIE BANNER HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _dismiss_gdpr_banner(page) -> bool:
    selectors = [
        'button:has-text("Accept All")', 'button:has-text("Accept all cookies")',
        'button:has-text("Agree")', 'button:has-text("I Accept")',
        'button:has-text("Akceptuj")', 'button:has-text("Akceptuje")',
        '#accept-all', '.accept-all', '[data-testid="accept-all"]',
        '[class*="gdpr"] button', '[class*="cookie"] button',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=1500):
                el.click(timeout=2000)
                page.wait_for_timeout(800)
                print(f"[scraper] GDPR dismissed: {sel}")
                return True
        except Exception:
            continue
    return False


def _dismiss_banners(page) -> None:
    selectors = [
        'button:has-text("Accept All")', 'button:has-text("Accept all cookies")',
        'button:has-text("Agree")', 'button:has-text("I Accept")',
        'button:has-text("Akceptuj")',
        '#accept-all', '.accept-all', '[data-testid="accept-all"]',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=1500):
                el.click(timeout=2000)
                page.wait_for_timeout(800)
                return
        except Exception:
            continue


def _is_eu_url(url: str) -> bool:
    eu_domains = ['pl.aliexpress.com', 'de.aliexpress.com', 'fr.aliexpress.com',
                  'it.aliexpress.com', 'es.aliexpress.com', 'nl.aliexpress.com']
    return any(d in url for d in eu_domains)


def _detect_eu_page(url: str, html_snippet: str) -> bool:
    indicators = [
        'gdpr', 'cookie-consent', 'Trader', 'DSA',
        'de.aliexpress.com', 'fr.aliexpress.com', 'it.aliexpress.com',
        'es.aliexpress.com', 'nl.aliexpress.com', 'pl.aliexpress.com',
    ]
    return any(ind in (url + html_snippet[:5000]) for ind in indicators)


# ─────────────────────────────────────────────────────────────────────────────
# DESCRIPTION FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_description(url: str) -> str:
    try:
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.aliexpress.com/',
            }
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode('utf-8', errors='replace')
        clean = re.sub(r'<style[^>]*>.*?</style>', ' ', raw, flags=re.DOTALL)
        clean = re.sub(r'<script[^>]*>.*?</script>', ' ', clean, flags=re.DOTALL)
        clean = re.sub(r'<[^>]+>', ' ', clean)
        clean = re.sub(r'\s+', ' ', clean).strip()
        return clean[:3000]
    except Exception as e:
        print(f"[scraper] Description fetch failed: {e}")
        return ''


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER SCRAPE — product detail
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_in_thread(url: str, try_compliance: bool = False) -> dict:
    captured   = []
    html       = ''
    dom_seller = {}
    compliance = {}

    ua    = random.choice(USER_AGENTS)
    is_eu = _is_eu_url(url)
    print(f"[scraper] EU={is_eu} | {url[:80]}")

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
                    if response.status != 200:
                        return

                    # Deliberately broad — pl./de./fr. storefronts fire different
                    # endpoint names than www.aliexpress.com
                    is_api = any(x in resp_url for x in [
                        'mtop.aliexpress',
                        'pdp.pc.query',
                        'pdp.pc.get',
                        'item.detail',
                        'itemdetail',
                        'ae.page.detail',
                        'page.detail',
                        'gsp.aliexpress',
                        'api.aliexpress',
                        '/fn/detail',
                        '/fn/product',
                        'product.info',
                        'aliexpress.com/fn/',
                        'datahub',
                    ])
                    if not is_api:
                        return

                    body = response.body()
                    if len(body) < 500:
                        return
                    text = body.decode('utf-8', errors='replace')

                    # Accept any response that looks like product data
                    has_data = any(x in text for x in [
                        'titleModule', 'TITLE', '"subject"',
                        'imageModule', 'IMAGE', 'imagePathList',
                        'priceModule', 'PRICE',
                        'storeModule', 'SHOP_CARD_PC', '"storeName"',
                        'productInfoComponent', 'commonModule',
                        'webEnv', 'metaModule', 'pageData',
                    ])
                    if not has_data:
                        return

                    captured.append(text)
                    print(f"[scraper] Captured API ({len(text):,} bytes) ← {resp_url[8:70]}")
                except Exception:
                    pass

            page.on('response', handle_response)
            page.goto(url, timeout=90_000, wait_until='domcontentloaded')

            page_html_snippet = page.content()[:5000]
            detected_eu = _detect_eu_page(page.url, page_html_snippet) or is_eu

            if detected_eu:
                page.wait_for_timeout(2000)
                _dismiss_gdpr_banner(page)
                page.wait_for_timeout(1000)

            page.wait_for_timeout(random.randint(5000, 7000))

            for _ in range(4):
                page.mouse.wheel(0, random.randint(400, 800))
                page.wait_for_timeout(random.randint(500, 900))
            page.wait_for_timeout(2000)

            print("[scraper] Extracting seller info...")
            dom_seller = _extract_seller_from_popup(page, is_eu_page=detected_eu)
            print(f"[scraper] Seller fields: {list(dom_seller.keys())}")

            if try_compliance:
                print("[scraper] Extracting compliance info...")
                compliance = _extract_compliance_info(page)

            html = page.content()
            page.close()
            context.close()

    except Exception as e:
        print(f"[scraper] Browser error: {e}")
        import traceback
        traceback.print_exc()

    return {
        'captured':   captured,
        'html':       html,
        'dom_seller': dom_seller,
        'compliance': compliance,
    }


def _scrape_with_retry(url: str, try_compliance: bool = False) -> dict:
    best = {'captured': [], 'html': '', 'dom_seller': {}, 'compliance': {}}

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n[scraper] Attempt {attempt}/{MAX_RETRIES}")

        if attempt > 1:
            delay = random.uniform(4, 9)
            print(f"[scraper] Back-off {delay:.1f}s...")
            time.sleep(delay)

        attempt_url = _get_rotated_url(url)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(
                    _scrape_in_thread, attempt_url,
                    try_compliance and attempt == 1
                )
                result = future.result(timeout=200)
        except Exception as e:
            print(f"[scraper] Attempt {attempt} error: {e}")
            continue

        for text in sorted(result['captured'], key=len, reverse=True):
            parsed = parse_mtop_response(text)
            if parsed and parsed.get('title'):
                print(f"[scraper] Success on attempt {attempt}")
                result['best_parsed'] = parsed
                return result

        if len(result['captured']) >= len(best['captured']):
            best = result
        print(f"[scraper] Attempt {attempt}: no title in API response")

    print(f"[scraper] All {MAX_RETRIES} attempts done")
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
        print("[scraper] Using pre-parsed mtop result")
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
        print("[scraper] Trying HTML init_data fallback...")
        for k, v in _extract_from_html_init_data(data['html']).items():
            if v and k not in extracted:
                extracted[k] = v

    if not extracted.get('title') and data.get('html'):
        # og:title — try both attribute orderings
        m = re.search(
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']{5,300})["\']',
            data['html']
        )
        if not m:
            m = re.search(
                r'<meta[^>]+content=["\']([^"\']{5,300})["\'][^>]+property=["\']og:title["\']',
                data['html']
            )
        if m:
            extracted['title'] = _clean_title(m.group(1).strip())
            print(f"[scraper] Title from og:title: {extracted['title'][:60]}")

    # H1 tag — very common on Polish / EU storefronts
    if not extracted.get('title') and data.get('html'):
        m = re.search(r'<h1[^>]*>\s*([^<]{5,300})\s*</h1>', data['html'], re.IGNORECASE)
        if m:
            t = _clean_title(m.group(1).strip())
            if t and 'aliexpress' not in t.lower():
                extracted['title'] = t
                print(f"[scraper] Title from <h1>: {t[:60]}")

    # <title> tag — absolute last resort
    if not extracted.get('title') and data.get('html'):
        m = re.search(r'<title[^>]*>\s*([^<]{5,300})\s*</title>', data['html'], re.IGNORECASE)
        if m:
            t = _clean_title(m.group(1).strip())
            if t and 'aliexpress' not in t.lower() and '404' not in t:
                extracted['title'] = t
                print(f"[scraper] Title from <title> tag: {t[:60]}")

    if not extracted.get('image_1') and data.get('html'):
        m = re.search(r'"imagePathList"\s*:\s*(\[[^\]]+\])', data['html'])
        if m:
            try:
                urls = json.loads(m.group(1))
                for idx, img_url in enumerate(urls[:20], 1):
                    if img_url and not extracted.get(f'image_{idx}'):
                        extracted[f'image_{idx}'] = str(img_url)
            except Exception:
                pass

    if not extracted.get('title'):
        print("[scraper] No product title after all attempts")
        return None

    if extracted.get('title'):
        extracted['title'] = _clean_title(extracted['title'])

    for k, v in data.get('dom_seller', {}).items():
        if v and not extracted.get(k):
            extracted[k] = v
            print(f"[scraper] Seller merged: {k} = {v}")

    compliance = data.get('compliance', {})
    if compliance:
        extracted['compliance'] = compliance

    desc_url = extracted.pop('_description_url', '')
    if desc_url and not extracted.get('description'):
        print("[scraper] Fetching description...")
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
    }
    for key, default in defaults.items():
        extracted.setdefault(key, default)
    for i in range(1, 21):
        extracted.setdefault(f'image_{i}', '')

    print(f"\n[scraper] Title:    {extracted['title'][:70]}")
    print(f"[scraper] Specs:    {[k for k in SPEC_MAPPING if extracted.get(k)]}")
    print(f"[scraper] Images:   {sum(1 for i in range(1,21) if extracted.get(f'image_{i}'))}")

    return extracted


# =============================================================================
# SEARCH-RESULTS SCRAPER
# =============================================================================

def _normalize_product_url(href: str, product_id: str) -> str:
    if not product_id:
        return ""
    return f"https://www.aliexpress.com/item/{product_id}.html"


def _extract_product_id(href: str) -> Optional[str]:
    if not href:
        return None
    m = re.search(r'/item/(\d{10,20})(?:\.html)?', href)
    if m:
        return m.group(1)
    m = re.search(r'[?&]productIds?=(\d{10,20})', href)
    if m:
        return m.group(1)
    m = re.search(r'[Ii]tem[Ii]d(?:%3D|=)(\d{10,20})', href)
    if m:
        return m.group(1)
    return None


def _extract_products_from_page(page) -> List[Dict]:
    """
    3-layer extraction strategy:
      Layer 1: window._dida_config_._init_data_ JSON  (most reliable, has title+price)
      Layer 2: DOM anchors with correct live selectors
      Layer 3: HTML regex fallback

    Returns list of dicts with keys: product_id, product_url, title
    """
    products = []
    seen_ids: set = set()

    # ── Layer 1: init_data JSON ──────────────────────────────────────────────
    try:
        init_data_json = page.evaluate("""() => {
            try {
                const cfg = window._dida_config_;
                if (cfg && cfg._init_data_) return JSON.stringify(cfg._init_data_);
            } catch(e) {}
            return null;
        }""")
        if init_data_json:
            init_data = json.loads(init_data_json)

            # Try multiple known paths for itemList content
            item_list = (
                _safe_get(init_data, 'data', 'data', 'root', 'fields', 'mods', 'itemList', 'content') or
                _safe_get(init_data, 'data', 'root', 'fields', 'mods', 'itemList', 'content') or
                # Fallback: search all nested dicts for itemList
                _find_item_list_recursive(init_data) or
                []
            )
            for item in item_list:
                if not isinstance(item, dict):
                    continue
                pid = str(
                    item.get('productId', '') or
                    item.get('redirectedId', '') or
                    item.get('itemId', '')
                ).strip()
                if not pid or pid in seen_ids:
                    continue

                # Title extraction — handle both string and nested dict forms
                title_raw = (
                    _safe_get(item, 'title', 'displayTitle') or
                    _safe_get(item, 'title', 'seoTitle') or
                    item.get('title') or ''
                )
                if isinstance(title_raw, dict):
                    title_raw = title_raw.get('displayTitle', '') or title_raw.get('title', '')
                title = _clean_title(str(title_raw).strip())

                seen_ids.add(pid)
                products.append({
                    'product_id':  pid,
                    'product_url': _normalize_product_url('', pid),
                    'title':       title,
                })

            if products:
                print(f"[search_scraper] init_data: {len(products)} products")
                return products
    except Exception as e:
        print(f"[search_scraper] init_data error: {e}")

    # ── Layer 2: DOM anchors (selectors from live HTML analysis) ─────────────
    # Correct selectors observed in AliExpress search HTML:
    #   <a class="lw_b h7_ic search-card-item" href="...">
    #   <h3 class="lw_k4">  (product title inside the anchor)
    try:
        anchors = page.locator(
            'a.search-card-item, '
            'a.lw_b.h7_ic.search-card-item, '
            '[class*="search-card-item"]'
        ).all()

        for anchor in anchors:
            try:
                href = anchor.get_attribute('href') or ''
                pid  = _extract_product_id(href)
                if not pid or pid in seen_ids:
                    continue

                # Primary title selector: h3.lw_k4 inside the card
                title = ''
                for title_sel in [
                    'h3.lw_k4',
                    '[role="heading"] h3',
                    '[class*="lw_k4"]',
                ]:
                    try:
                        te = anchor.locator(title_sel).first
                        if te.count() > 0:
                            raw = te.inner_text().strip()
                            if raw:
                                title = _clean_title(raw)
                                break
                    except Exception:
                        pass

                # Fallback to aria-label
                if not title:
                    title = _clean_title(anchor.get_attribute('aria-label') or '')

                seen_ids.add(pid)
                products.append({
                    'product_id':  pid,
                    'product_url': _normalize_product_url(href, pid),
                    'title':       title[:300],
                })
            except Exception:
                continue

        if products:
            print(f"[search_scraper] DOM: {len(products)} products")
            return products
    except Exception as e:
        print(f"[search_scraper] DOM error: {e}")

    # ── Layer 3: HTML regex fallback ─────────────────────────────────────────
    try:
        html = page.content()
        # Extract product IDs from all href patterns
        patterns = [
            r'href=["\'](?:https?:)?//[^"\']*?aliexpress\.com/item/(\d{10,20})\.html',
            r'href=[^"\']*?/item/(\d{10,20})\.html',
            r'[?&]productIds?=(\d{10,20})',
            r'"productId"\s*:\s*"(\d{10,20})"',
            r'"redirectedId"\s*:\s*"(\d{10,20})"',
        ]
        for pat in patterns:
            for pid in re.findall(pat, html):
                if pid not in seen_ids:
                    seen_ids.add(pid)
                    products.append({
                        'product_id':  pid,
                        'product_url': _normalize_product_url('', pid),
                        'title':       '',
                    })

        if products:
            print(f"[search_scraper] HTML regex: {len(products)} products")
    except Exception as e:
        print(f"[search_scraper] HTML regex error: {e}")

    return products


def _find_item_list_recursive(data, depth: int = 0) -> Optional[list]:
    """
    Recursively search nested dicts for an 'itemList' key containing
    a 'content' list. Stops at depth 10 to avoid runaway recursion.
    """
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
        for item in data[:5]:  # only check first few list items
            result = _find_item_list_recursive(item, depth + 1)
            if result:
                return result
    return None


def _get_current_page_number(page) -> int:
    try:
        active = page.locator(
            '.comet-pagination-item-active a, '
            'li.comet-pagination-item.comet-pagination-item-active a'
        ).first
        if active.count() > 0:
            text = active.inner_text().strip()
            if text.isdigit():
                return int(text)
    except Exception:
        pass
    try:
        m = re.search(r'[?&]page=(\d+)', page.url)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return 1


def _get_total_pages(page) -> int:
    try:
        items = page.locator(
            'li.comet-pagination-item:not(.comet-pagination-prev)'
            ':not(.comet-pagination-next) a'
        ).all()
        nums = []
        for item in items:
            try:
                t = item.inner_text().strip()
                if t.isdigit():
                    nums.append(int(t))
            except Exception:
                pass
        if nums:
            return max(nums)
    except Exception:
        pass
    # Fallback: parse pagination HTML
    try:
        html = page.locator('.comet-pagination').inner_html()
        nums = re.findall(r'>(\d+)<', html)
        if nums:
            return max(int(n) for n in nums)
    except Exception:
        pass
    return 1


def _has_next_page(page) -> bool:
    """Check whether a non-disabled Next page button exists."""
    for sel in [
        'li.comet-pagination-next:not(.comet-pagination-disabled)',
        'li.comet-pagination-next:not([class*="disabled"])',
        '.comet-pagination-next:not(.comet-pagination-disabled)',
        'button[aria-label="Next Page"]:not([disabled])',
    ]:
        try:
            btn = page.locator(sel).first
            if btn.count() > 0:
                # Extra check: make sure it's not visually disabled
                classes = btn.get_attribute('class') or ''
                if 'disabled' not in classes.lower():
                    return True
        except Exception:
            continue
    return False


def _click_next_page(page) -> bool:
    for sel in [
        'li.comet-pagination-next:not(.comet-pagination-disabled)',
        '.comet-pagination-next:not(.comet-pagination-disabled)',
        'li.comet-pagination-next button:not([disabled])',
    ]:
        try:
            btn = page.locator(sel).first
            if btn.count() > 0 and btn.is_visible(timeout=3000):
                btn.click(timeout=5000)
                return True
        except Exception:
            continue
    return False


def _build_page_url(base_url: str, page_num: int) -> str:
    """
    Build a paginated URL.  Handles three cases:
      1. 'page=' already present → replace value
      2. URL has query string    → append &page=N
      3. Clean URL               → append ?page=N
    """
    if page_num == 1:
        return base_url
    if re.search(r'[?&]page=\d+', base_url):
        return re.sub(r'([?&]page=)\d+', rf'\g<1>{page_num}', base_url)
    sep = '&' if '?' in base_url else '?'
    return f"{base_url}{sep}page={page_num}"


def _scroll_page(page, steps: int = 8) -> None:
    """Scroll with navigation-safety. Stops cleanly if context is destroyed."""
    try:
        for i in range(steps):
            try:
                page.mouse.wheel(0, random.randint(500, 900))
                page.wait_for_timeout(random.randint(SCROLL_PAUSE - 100, SCROLL_PAUSE + 200))
            except Exception as scroll_err:
                if "Execution context was destroyed" in str(scroll_err):
                    print(f"[search_scraper] Context destroyed at step {i} - scroll stopped")
                    return
                raise
        try:
            page.evaluate("window.scrollTo(0, 0)")
        except Exception:
            pass
        page.wait_for_timeout(500)
        for _ in range(2):
            try:
                page.mouse.wheel(0, random.randint(400, 600))
                page.wait_for_timeout(400)
            except Exception:
                return
    except Exception as e:
        print(f"[search_scraper] Scroll error: {e}")


def _wait_for_page_load(page, timeout: int = 12_000) -> bool:
    """
    Wait for search-result cards to appear.
    Selector list matches the actual AliExpress HTML structure.
    """
    try:
        page.wait_for_load_state('networkidle', timeout=8_000)
    except Exception:
        pass

    # Selectors that are reliably present on AliExpress search pages
    try:
        page.wait_for_selector(
            '#card-list, '
            '.hm_hn, '
            'a.search-card-item, '
            'a.lw_b.h7_ic.search-card-item, '
            '[class*="search-card-item"]',
            timeout=timeout
        )
        page.wait_for_timeout(random.randint(800, 1500))
        return True
    except Exception:
        print("[search_scraper] Card selector not found — attempting extraction anyway")
        return True


def scrape_search_results(
    search_url: str,
    max_pages:    int   = 5,           # ← Changed default to 5
    max_products: int   = 0,
    deduplicate:  bool  = True,
    delay:        float = 1.5,
) -> Dict:
    """
    Scrape AliExpress search result pages.

    Extracts per product:
      - product_id   (AliExpress item ID)
      - product_url  (canonical URL)
      - title        (display title, cleaned)

    Args:
        search_url:   Full AliExpress search / wholesale URL
        max_pages:    Maximum pages to scrape (default 5, hard cap MAX_SEARCH_PAGES)
        max_products: Stop after N unique products; 0 = unlimited
        deduplicate:  Skip duplicate product IDs across pages
        delay:        Base seconds between page navigations (actual = delay .. delay*1.5)

    Returns a dict:
        {
          "products":       List[{product_id, product_url, title}],
          "total_products": int,
          "pages_scraped":  int,
          "search_url":     str,
        }
    """
    max_pages    = min(max_pages, MAX_SEARCH_PAGES)
    all_products: List[Dict] = []
    seen_ids:     set        = set()
    pages_scraped            = 0
    ua                       = random.choice(USER_AGENTS)

    print(f"\n[search_scraper] URL: {search_url[:100]}")
    print(f"[search_scraper] Max pages: {max_pages} | Products cap: {max_products or 'unlimited'} | Delay: {delay}s")

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
                print(f"\n[search_scraper] ── Page {page_num} ──")

                try:
                    nav_url = _build_page_url(search_url, page_num)
                    print(f"[search_scraper] Navigating to: {nav_url[:100]}")
                    page.goto(nav_url, timeout=SEARCH_TIMEOUT, wait_until='domcontentloaded')
                    _wait_for_page_load(page)

                    if page_num == 1:
                        _dismiss_banners(page)
                        page.wait_for_timeout(800)
                except Exception as e:
                    print(f"[search_scraper] Navigation error page {page_num}: {e} — skipping")
                    pages_scraped = page_num
                    continue

                # Extract BEFORE scrolling (avoids context-destroyed errors)
                page_products = _extract_products_from_page(page)

                new_count = 0
                for prod in page_products:
                    pid = prod['product_id']
                    if deduplicate and pid in seen_ids:
                        continue
                    seen_ids.add(pid)
                    # Keep only the three required fields
                    all_products.append({
                        'product_id':  prod['product_id'],
                        'product_url': prod['product_url'],
                        'title':       prod.get('title', ''),
                    })
                    new_count += 1

                pages_scraped = page_num
                print(f"[search_scraper] Page {page_num}: +{new_count} new | total={len(all_products)}")

                # Only hard stop: optional products cap
                if max_products > 0 and len(all_products) >= max_products:
                    print(f"[search_scraper] Max products ({max_products}) reached")
                    break

                # Log pagination info for diagnostics — but NEVER stop early on it.
                # AliExpress renders the next-page button lazily; checking it here
                # causes premature termination (the "0 products / stops at page 1" bug).
                has_next    = _has_next_page(page)
                total_pages = _get_total_pages(page)
                print(
                    f"[search_scraper] Pagination: has_next={has_next} "
                    f"total_pages={total_pages} — continuing to page {page_num + 1}"
                )

                # Polite inter-page delay
                if page_num < max_pages:
                    sleep_time = random.uniform(delay, delay * 1.5)
                    print(f"[search_scraper] Waiting {sleep_time:.1f}s before next page...")
                    time.sleep(sleep_time)

            page.close()
            context.close()

    except Exception as e:
        print(f"[search_scraper] Browser session error: {e}")
        import traceback
        traceback.print_exc()

    if max_products > 0:
        all_products = all_products[:max_products]

    print(f"\n[search_scraper] Done: {len(all_products)} products from {pages_scraped} pages")

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
        for p in result['products'][:15]:
            print(f"  [{p['product_id']}] {p['title'][:60]}")
    else:
        test_url = args[0] if args else "https://www.aliexpress.com/item/1005010089125608.html"
        result = get_product_info(test_url, extract_compliance=True)
        if result:
            print("\n" + "=" * 70)
            for k, v in sorted(result.items()):
                if k.startswith('image_') and not v:
                    continue
                if k == 'compliance' and v:
                    for ck, cv in v.items():
                        print(f"  compliance.{ck:30}: {cv}")
                    continue
                print(f"  {k:30}: {str(v)[:100]}")
        else:
            print("FAILED")
