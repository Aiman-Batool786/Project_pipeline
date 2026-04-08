"""
scraper.py v6.1 — FIXED VERSION
─────────────────────────────────
Navigation safety improvements for search results scraping.
Fixes: "Execution context was destroyed" error.
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
# CONFIG (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

SPEC_MAPPING = {
    'brand':             ['brand', 'brand name', 'marque', 'manufacturer', 'marka'],
    'color':             ['color', 'colour', 'main color', 'couleur', 'kolor'],
    'dimensions':        ['dimensions', 'size', 'product size', 'package size',
                          'item size', 'product dimensions', 'wymiary'],
    'weight':            ['weight', 'net weight', 'gross weight', 'poids', 'waga'],
    'material':          ['material', 'materials', 'composition', 'matiere', 'materiał'],
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
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

MAX_RETRIES       = 3
MAX_SEARCH_PAGES  = 50
PAGE_TIMEOUT      = 90_000
SCROLL_PAUSE      = 600

CATEGORY_ID_MAP = {
    '5090301':   'Cell Phones',
    '509':       'Phones & Telecommunications',
    '202238810': 'Cell Phones (Refurbished)',
    '202238004': 'Consumer Electronics',
    '200000345': 'Women\'s Clothing',
    '200000346': 'Men\'s Clothing',
    '200003655': 'Tablets',
    '100006654': 'Smart Watches',
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _s(v) -> str:
    if v is None:
        return ''
    s = str(v).strip()
    return '' if s in ('None', 'null', '0', 'false', 'undefined') else s


def _safe(d, *keys, default=None):
    """Deep-get for product-detail dicts."""
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
    """Deep-get helper for search-results scraper."""
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


# ─────────────────────────────────────────────────────────────────────────────
# SELLER PARSER (unchanged)
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
# MTOP PARSER (unchanged — product detail only)
# ─────────────────────────────────────────────────────────────────────────────

def parse_mtop_response(text: str) -> dict | None:
    try:
        m = re.match(r'^[a-zA-Z0-9_$]+\((.*)\);?\s*$', text.strip(), re.DOTALL)
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

        print(f"[scraper] 📦 mtop result keys: {list(result.keys())[:15]}")
        extracted = {}

        # Title
        for tk in ['titleModule', 'TITLE', 'GLOBAL_DATA']:
            tb = result.get(tk, {})
            if isinstance(tb, dict):
                t = (_s(tb.get('subject')) or _s(tb.get('title')) or
                     _s(_safe(tb, 'globalData', 'subject')))
                if t:
                    extracted['title'] = t
                    break

        # Price
        pm = result.get('priceModule') or result.get('PRICE') or {}
        if isinstance(pm, dict):
            extracted['price'] = _s(
                pm.get('formatedActivityPrice') or pm.get('formatedPrice') or
                pm.get('formattedPrice') or
                _safe(pm, 'minActivityAmount', 'formattedAmount')
            )

        # Images
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

        # Specs
        props = []
        for sk in ['PRODUCT_PROP_PC', 'specsModule', 'productPropComponent', 'skuModule']:
            sb = result.get(sk, {})
            if isinstance(sb, dict):
                raw = (sb.get('props') or sb.get('showedProps') or
                       sb.get('specsList') or sb.get('productSKUPropertyList') or [])
                if raw:
                    props = raw
                    print(f"[scraper] 📋 {len(props)} specs from '{sk}'")
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

        # Seller from API
        for sk in ['storeModule', 'SHOP_CARD_PC', 'sellerModule', 'shopInfo']:
            sb = result.get(sk, {})
            if isinstance(sb, dict) and sb:
                seller = _parse_seller_block(sb)
                if seller.get('store_name') or seller.get('store_id'):
                    extracted.update(seller)
                    print(f"[scraper] 🏪 Seller from mtop '{sk}': {list(seller.keys())}")
                    break

        # Category
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

        # Rating/Reviews
        fb = result.get('feedbackModule') or result.get('FEEDBACK') or {}
        if isinstance(fb, dict):
            extracted['rating']  = _s(fb.get('trialRating') or fb.get('averageStar'))
            extracted['reviews'] = _s(fb.get('trialNum') or fb.get('totalCount'))

        # Description URL
        dm = result.get('descriptionModule') or result.get('DESCRIPTION') or {}
        if isinstance(dm, dict):
            du = _s(dm.get('descriptionUrl'))
            if du:
                extracted['_description_url'] = du

        # Bullet points
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
        print(f"[scraper] ⚠️ mtop parse error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# HTML INIT_DATA PARSER (unchanged)
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
                print(f"[scraper] 📄 init_data keys: {list(inner.keys())[:10]}")
                return parse_mtop_response(
                    json.dumps({'data': {'result': inner}})
                ) or {}
    except Exception as e:
        print(f"[scraper] ⚠️ init_data parse error: {e}")
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# COMPLIANCE MODAL EXTRACTOR (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_compliance_text(text: str) -> dict:
    """Parse raw compliance modal text into structured fields."""
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

    # Manufacturer
    result['manufacturer_name']    = extract_field(
        ['Name', 'Imię i nazwisko', 'Manufacturer Name'], text)
    result['manufacturer_address'] = extract_field(
        ['Address', 'Adres'], text,
        stop_patterns=['Email', 'Telephone', 'Phone', 'Numer', 'Adres e-mail'])
    result['manufacturer_email']   = extract_field(
        ['Email address', 'Adres e-mail', 'Email'], text)
    result['manufacturer_phone']   = extract_field(
        ['Telephone number', 'Numer telefonu', 'Phone', 'Tel'], text)

    # EU Responsible Person
    eu_match = re.search(
        r'(Details of the person responsible|person responsible for compliance in the EU|'
        r'Osoba odpowiedzialna|EU Representative|EU Responsible)',
        text, re.IGNORECASE
    )
    if eu_match:
        eu_text = text[eu_match.start():]
        result['eu_responsible_name']    = extract_field(
            ['Name', 'Imię i nazwisko', 'Nazwa'], eu_text)
        result['eu_responsible_address'] = extract_field(
            ['Address', 'Adres'], eu_text,
            stop_patterns=['Email', 'Telephone', 'Numer', 'Adres e-mail'])
        result['eu_responsible_email']   = extract_field(
            ['Email address', 'Adres e-mail', 'Email'], eu_text)
        result['eu_responsible_phone']   = extract_field(
            ['Telephone number', 'Numer telefonu', 'Phone'], eu_text)

    # Product ID
    pid_m = re.search(r'Product ID[^\n]*\n\s*([0-9][-0-9]+)', text, re.IGNORECASE)
    if pid_m:
        result['compliance_product_id'] = pid_m.group(1).strip()
    else:
        m = re.search(r'(\d{13,20}(?:-\d{13,20})?)', text)
        if m:
            result['compliance_product_id'] = m.group(1)

    return {k: v for k, v in result.items() if v}


def _extract_compliance_info(page) -> dict:
    """Click compliance link and parse the modal."""
    compliance = {}

    selectors = [
        'text="Product Compliance Information"',
        'span:has-text("Product Compliance Information")',
        'a:has-text("Product Compliance Information")',
        'text="Informacje o zgodności produktu"',
        'span:has-text("Informacje o zgodności")',
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
                print(f"[scraper] 🔒 Clicked compliance: {sel}")
                clicked = True
                break
        except Exception:
            continue

    if not clicked:
        print("[scraper] ℹ️ No compliance link found")
        return {}

    try:
        page.wait_for_selector('.comet-v2-modal, .comet-modal', timeout=6000)
        page.wait_for_timeout(1000)
        modal = page.locator('.comet-v2-modal, .comet-modal').first
        if modal.count() == 0:
            return {}
        modal_text = modal.inner_text()
        print(f"[scraper] 🔒 Compliance modal ({len(modal_text)} chars)")
        compliance = _parse_compliance_text(modal_text)
        print(f"[scraper] ✅ Compliance fields: {list(compliance.keys())}")
    except Exception as e:
        print(f"[scraper] ⚠️ Compliance modal error: {e}")

    # Close modal
    try:
        page.locator(
            '.comet-v2-modal-close, .comet-modal-close, button[aria-label="Close"]'
        ).first.click(timeout=2000)
    except Exception:
        pass

    return compliance


# ─────────────────────────────────────────────────────────────────────────────
# SELLER INFO FROM DOM (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_seller_from_dom(page) -> dict:
    """Extract seller info visible on the page."""
    seller = {}

    # Store ID + URL from store links
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
                    print(f"[scraper] ✅ DOM store ID: {m.group(1)}")
                    break
    except Exception:
        pass

    # Store name
    for sel in [
        '[class*="store-header--storeName"]', '[class*="shopName"]',
        '[class*="shop-name"]', '[class*="sellerName"]',
        'a[href*="/store/"] span', '[class*="store-info"] h3',
    ]:
        try:
            el = page.locator(sel).first
            if el.count() > 0:
                text = el.inner_text().strip()
                if text and 2 < len(text) < 100:
                    seller['store_name'] = text
                    print(f"[scraper] ✅ DOM store name: {text}")
                    break
        except Exception:
            continue

    # Store open date, location from info blocks
    info_patterns = {
        'store_open_date': [
            r'(?:Opening date|Opened|Since|Data otwarcia)[:\s]+'
            r'([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})',
        ],
        'seller_country': [
            r'(?:Location|Country|Kraj)[:\s]+([A-Za-z\s]+)',
        ],
        'store_id': [
            r'(?:Shop No\.|Store No\.|No\.)[:\s]+(\d+)',
        ],
    }

    try:
        page_text = page.locator('body').inner_text()
        for field, patterns in info_patterns.items():
            if field in seller:
                continue
            for pat in patterns:
                m = re.search(pat, page_text, re.IGNORECASE)
                if m:
                    val = m.group(1).strip()
                    if val:
                        seller[field] = val
                        print(f"[scraper] ✅ DOM {field}: {val}")
                        break
    except Exception:
        pass

    return {k: v for k, v in seller.items() if v}


# ─────────────────────────────────────────────────────────────────────────────
# GDPR / COOKIE BANNER HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _dismiss_gdpr_banner(page) -> bool:
    """Dismiss GDPR / cookie banners on product-detail pages."""
    selectors = [
        'button:has-text("Accept All")', 'button:has-text("Accept all cookies")',
        'button:has-text("Agree")', 'button:has-text("I Accept")',
        'button:has-text("Akceptuj")', 'button:has-text("Akceptuję")',
        '#accept-all', '.accept-all', '[data-testid="accept-all"]',
        '[class*="gdpr"] button', '[class*="cookie"] button',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=1500):
                el.click(timeout=2000)
                page.wait_for_timeout(800)
                print(f"[scraper] 🍪 GDPR dismissed: {sel}")
                return True
        except Exception:
            continue
    return False


def _dismiss_banners(page) -> None:
    """Dismiss GDPR / cookie banners on search-result pages."""
    selectors = [
        'button:has-text("Accept All")',
        'button:has-text("Accept all cookies")',
        'button:has-text("Agree")',
        'button:has-text("I Accept")',
        'button:has-text("Akceptuj")',
        '#accept-all',
        '.accept-all',
        '[data-testid="accept-all"]',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=1500):
                el.click(timeout=2000)
                page.wait_for_timeout(800)
                print(f"[search_scraper] 🍪 Banner dismissed: {sel}")
                return
        except Exception:
            continue


def _is_eu_url(url: str) -> bool:
    eu_domains = ['pl.aliexpress.com', 'de.aliexpress.com', 'fr.aliexpress.com',
                  'it.aliexpress.com', 'es.aliexpress.com', 'nl.aliexpress.com']
    return any(d in url for d in eu_domains)


# ─────────────────────────────────────────────────────────────────────────────
# DESCRIPTION FETCHER (unchanged)
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
        print(f"[scraper] ⚠️ Description fetch failed: {e}")
        return ''


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER SCRAPE — product detail (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_in_thread(url: str, try_compliance: bool = False) -> dict:
    captured   = []
    html       = ''
    dom_seller = {}
    compliance = {}

    ua    = random.choice(USER_AGENTS)
    is_eu = _is_eu_url(url)
    print(f"[scraper] 🌐 EU={is_eu} | {url[:80]}")

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
                         'mtop.aliexpress.itemdetail' in resp_url or
                         'pdp.pc.query' in resp_url) and
                            response.status == 200):
                        body = response.body()
                        if len(body) < 1000:
                            return
                        text = body.decode('utf-8', errors='replace')
                        if any(x in text for x in [
                            'titleModule', 'storeModule', 'SHOP_CARD_PC',
                            'imageModule', 'priceModule', '"subject"', '"storeName"'
                        ]):
                            captured.append(text)
                            print(f"[scraper] 📡 Captured API ({len(text):,} bytes)")
                except Exception:
                    pass

            page.on('response', handle_response)
            page.goto(url, timeout=90_000, wait_until='domcontentloaded')

            if is_eu:
                page.wait_for_timeout(2000)
                _dismiss_gdpr_banner(page)
                page.wait_for_timeout(1000)

            page.wait_for_timeout(random.randint(5000, 7000))

            for _ in range(4):
                page.mouse.wheel(0, random.randint(400, 800))
                page.wait_for_timeout(random.randint(500, 900))
            page.wait_for_timeout(2000)

            # Extract seller from DOM (always)
            dom_seller = _extract_seller_from_dom(page)

            # Extract compliance (EU only)
            if try_compliance:
                print("[scraper] 🔒 Extracting compliance info...")
                compliance = _extract_compliance_info(page)

            html = page.content()
            page.close()
            context.close()

    except Exception as e:
        print(f"[scraper] ❌ Browser error: {e}")
        import traceback
        traceback.print_exc()

    return {
        'captured':   captured,
        'html':       html,
        'dom_seller': dom_seller,
        'compliance': compliance,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RETRY WRAPPER — product detail (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_with_retry(url: str, try_compliance: bool = False) -> dict:
    best = {'captured': [], 'html': '', 'dom_seller': {}, 'compliance': {}}

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n[scraper] 🔄 Attempt {attempt}/{MAX_RETRIES}")

        if attempt > 1:
            delay = random.uniform(4, 9)
            print(f"[scraper] ⏳ Back-off {delay:.1f}s...")
            time.sleep(delay)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(
                    _scrape_in_thread, url,
                    try_compliance and attempt == 1
                )
                result = future.result(timeout=200)
        except Exception as e:
            print(f"[scraper] ❌ Attempt {attempt} error: {e}")
            continue

        for text in sorted(result['captured'], key=len, reverse=True):
            parsed = parse_mtop_response(text)
            if parsed and parsed.get('title'):
                print(f"[scraper] ✅ Success on attempt {attempt}")
                result['best_parsed'] = parsed
                return result

        if len(result['captured']) >= len(best['captured']):
            best = result
        print(f"[scraper] ⚠️ Attempt {attempt}: no title in API response")

    print(f"[scraper] ⚠️ All {MAX_RETRIES} attempts done")
    return best


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY RESOLVER (unchanged)
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
# MAIN PUBLIC FUNCTION — product detail (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str, extract_compliance: bool = True) -> dict | None:
    print(f"\n[scraper] ══════════════════════════")
    print(f"[scraper] Starting: {url}")
    print(f"[scraper] ══════════════════════════")

    data      = _scrape_with_retry(url, try_compliance=extract_compliance)
    extracted = {}

    # Primary: pre-parsed mtop result
    if data.get('best_parsed'):
        extracted = data['best_parsed']
        print("[scraper] ✅ Using pre-parsed mtop result")
    else:
        for text in sorted(data['captured'], key=len, reverse=True):
            parsed = parse_mtop_response(text)
            if parsed:
                for k, v in parsed.items():
                    if v and k not in extracted:
                        extracted[k] = v
                if extracted.get('title'):
                    break

    # Fallback: init_data from HTML
    if not extracted.get('title') and data.get('html'):
        print("[scraper] 🔍 Trying HTML init_data fallback...")
        for k, v in _extract_from_html_init_data(data['html']).items():
            if v and k not in extracted:
                extracted[k] = v

    # Fallback: og:title
    if not extracted.get('title') and data.get('html'):
        m = re.search(
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']{5,300})["\']',
            data['html']
        )
        if m:
            extracted['title'] = m.group(1).strip()

    # Fallback: images from HTML DCData
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
        print("[scraper] ❌ No product title after all attempts")
        return None

    # Merge DOM seller (fills gaps only)
    for k, v in data.get('dom_seller', {}).items():
        if v and not extracted.get(k):
            extracted[k] = v
            print(f"[scraper] ✅ Seller from DOM: {k} = {v}")

    # Attach compliance
    compliance = data.get('compliance', {})
    if compliance:
        extracted['compliance'] = compliance
        print(f"[scraper] ✅ Compliance: {list(compliance.keys())}")

    # Fetch description
    desc_url = extracted.pop('_description_url', '')
    if desc_url and not extracted.get('description'):
        print("[scraper] 📄 Fetching description...")
        extracted['description'] = fetch_description(desc_url)

    # Apply defaults
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

    # Summary
    print(f"\n[scraper] ── Summary ──────────────")
    print(f"  Title:      {extracted['title'][:70]}")
    print(f"  Seller:     {[k for k in ['store_name','store_id','seller_rating','seller_country'] if extracted.get(k)]}")
    print(f"  Specs:      {[k for k in SPEC_MAPPING if extracted.get(k)]}")
    print(f"  Images:     {sum(1 for i in range(1,21) if extracted.get(f'image_{i}'))}")
    print(f"  Compliance: {list(extracted.get('compliance', {}).keys())}")
    print(f"[scraper] ────────────────────────")

    return extracted


# ═════════════════════════════════════════════════════════════════════════════
# ▼▼▼  SEARCH-RESULTS SCRAPER — FIXED v6.1  ▼▼▼
# ═════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# URL helpers (search)
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_product_url(href: str, product_id: str) -> str:
    """Return canonical /item/<id>.html URL on www.aliexpress.com."""
    if not product_id:
        return ""
    return f"https://www.aliexpress.com/item/{product_id}.html"


def _extract_product_id(href: str) -> Optional[str]:
    """Extract product ID from AliExpress product href."""
    if not href:
        return None

    # Pattern 1: /item/<id>.html
    m = re.search(r'/item/(\d{10,20})(?:\.html)?', href)
    if m:
        return m.group(1)

    # Pattern 2: productIds=<id>
    m = re.search(r'[?&]productIds?=(\d{10,20})', href)
    if m:
        return m.group(1)

    # Pattern 3: itemId%3D<id>
    m = re.search(r'[Ii]tem[Ii]d(?:%3D|=)(\d{10,20})', href)
    if m:
        return m.group(1)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Product extractor — search page (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_products_from_page(page) -> List[Dict]:
    """Extract all products visible on current search results page."""
    products = []
    seen_ids: set = set()

    # ── Strategy 1: init_data JSON ──
    try:
        init_data_json = page.evaluate("""() => {
            try {
                const cfg = window._dida_config_;
                if (cfg && cfg._init_data_) {
                    return JSON.stringify(cfg._init_data_);
                }
            } catch(e) {}
            return null;
        }""")

        if init_data_json:
            init_data = json.loads(init_data_json)
            item_list = (
                _safe_get(init_data, 'data', 'data', 'root', 'fields',
                          'mods', 'itemList', 'content') or
                _safe_get(init_data, 'data', 'root', 'fields',
                          'mods', 'itemList', 'content') or
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

                # Title
                title = (
                    _safe_get(item, 'title', 'displayTitle') or
                    _safe_get(item, 'title', 'seoTitle') or
                    _safe_get(item, 'title') or ''
                )
                if isinstance(title, dict):
                    title = title.get('displayTitle', '') or title.get('title', '')

                # Price
                price = (
                    _safe_get(item, 'prices', 'salePrice', 'formattedPrice') or
                    _safe_get(item, 'prices', 'originalPrice', 'formattedPrice') or ''
                )

                # Rating & sold
                rating = _safe_get(item, 'evaluation', 'starRating') or ''
                sold   = _safe_get(item, 'trade', 'tradeDesc') or ''

                seen_ids.add(pid)
                products.append({
                    'product_id':  pid,
                    'product_url': _normalize_product_url('', pid),
                    'title':       str(title).strip(),
                    'price':       str(price).strip(),
                    'rating':      str(rating).strip(),
                    'sold':        str(sold).strip(),
                })

            if products:
                print(f"[search_scraper] ✅ init_data: {len(products)} products")
                return products

    except Exception as e:
        print(f"[search_scraper] ⚠️ init_data extraction error: {e}")

    # ── Strategy 2: DOM anchor scraping ─────────────────────────────────
    try:
        anchors = page.locator(
            'a.search-card-item, a.lw_b.h7_ic, [data-card-type] a'
        ).all()
        print(f"[search_scraper] 🔍 DOM: found {len(anchors)} anchors")

        for anchor in anchors:
            try:
                href = anchor.get_attribute('href') or ''
                if not href:
                    continue

                pid = _extract_product_id(href)
                if not pid or pid in seen_ids:
                    continue

                # Title
                title = ''
                try:
                    title_el = anchor.locator('h3.lw_k4, [role="heading"] h3').first
                    if title_el.count() > 0:
                        title = title_el.inner_text().strip()
                except Exception:
                    pass
                if not title:
                    try:
                        title = anchor.get_attribute('aria-label') or ''
                    except Exception:
                        pass

                # Price
                price = ''
                try:
                    price_el = anchor.locator('.lw_kt, .lw_el').first
                    if price_el.count() > 0:
                        price = price_el.inner_text().strip().replace('\n', ' ')
                except Exception:
                    pass

                seen_ids.add(pid)
                products.append({
                    'product_id':  pid,
                    'product_url': _normalize_product_url(href, pid),
                    'title':       title[:300],
                    'price':       price[:100],
                    'rating':      '',
                    'sold':        '',
                })
            except Exception:
                continue

        if products:
            print(f"[search_scraper] ✅ DOM: {len(products)} products")
            return products

    except Exception as e:
        print(f"[search_scraper] ⚠️ DOM extraction error: {e}")

    # ── Strategy 3: Raw HTML regex ───────────────────────────────────────
    try:
        html = page.content()

        ids_found = re.findall(
            r'href=["\'](?:https?:)?//[^"\']*?aliexpress\.com/item/(\d{10,20})\.html',
            html
        )
        ids_found += re.findall(r'href=["\'][^"\']*?/item/(\d{10,20})\.html', html)

        for pid in ids_found:
            if pid not in seen_ids:
                seen_ids.add(pid)
                products.append({
                    'product_id':  pid,
                    'product_url': _normalize_product_url('', pid),
                    'title': '', 'price': '', 'rating': '', 'sold': '',
                })

        # Grab productIds= from redirect/bundle links
        redir_ids = re.findall(r'[?&]productIds?=(\d{10,20})', html)
        for pid in redir_ids:
            if pid not in seen_ids:
                seen_ids.add(pid)
                products.append({
                    'product_id':  pid,
                    'product_url': _normalize_product_url('', pid),
                    'title': '', 'price': '', 'rating': '', 'sold': '',
                })

        if products:
            print(f"[search_scraper] ✅ HTML regex: {len(products)} products")

    except Exception as e:
        print(f"[search_scraper] ⚠️ HTML regex error: {e}")

    return products


# ─────────────────────────────────────────────────────────────────────────────
# Pagination helpers (search) — UNCHANGED
# ─────────────────────────────────────────────────────────────────────────────

def _get_current_page_number(page) -> int:
    """Return the current page number (1-based)."""
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
        url = page.url
        m = re.search(r'[?&]page=(\d+)', url)
        if m:
            return int(m.group(1))
    except Exception:
        pass

    return 1


def _get_total_pages(page) -> int:
    """Return total number of pages available."""
    try:
        items = page.locator(
            'li.comet-pagination-item:not(.comet-pagination-prev)'
            ':not(.comet-pagination-next) a'
        ).all()
        nums = []
        for item in items:
            try:
                text = item.inner_text().strip()
                if text.isdigit():
                    nums.append(int(text))
            except Exception:
                pass
        if nums:
            return max(nums)
    except Exception:
        pass

    try:
        html = page.locator('.comet-pagination').inner_html()
        nums = re.findall(r'>(\d+)<', html)
        if nums:
            return max(int(n) for n in nums)
    except Exception:
        pass

    return 1


def _has_next_page(page) -> bool:
    """Check if a 'next page' button exists and is not disabled."""
    try:
        btn = page.locator(
            'li.comet-pagination-next:not(.comet-pagination-disabled)'
        ).first
        if btn.count() > 0:
            return True
    except Exception:
        pass

    try:
        btn = page.locator(
            '.comet-pagination-next button:not([disabled])'
        ).first
        if btn.count() > 0:
            return True
    except Exception:
        pass

    return False


def _click_next_page(page) -> bool:
    """Click the next-page button. Returns True on success."""
    selectors = [
        'li.comet-pagination-next:not(.comet-pagination-disabled)',
        '.comet-pagination-next:not(.comet-pagination-disabled)',
        'li.comet-pagination-next button:not([disabled])',
        '[title="Next"]:not([disabled])',
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if btn.count() > 0 and btn.is_visible(timeout=3000):
                btn.click(timeout=5000)
                return True
        except Exception:
            continue

    # Fallback: build next-page URL and navigate
    try:
        current_url = page.url
        m = re.search(r'[?&]page=(\d+)', current_url)
        if m:
            next_pg  = int(m.group(1)) + 1
            next_url = re.sub(r'([?&]page=)\d+', rf'\g<1>{next_pg}', current_url)
        else:
            sep      = '&' if '?' in current_url else '?'
            next_url = current_url + f'{sep}page=2'

        page.goto(next_url, timeout=PAGE_TIMEOUT, wait_until='domcontentloaded')
        return True
    except Exception:
        pass

    return False


def _build_page_url(base_url: str, page_num: int) -> str:
    """Build URL for a specific page number."""
    if page_num == 1:
        return base_url
    if 'page=' in base_url:
        return re.sub(r'([?&]page=)\d+', rf'\g<1>{page_num}', base_url)
    sep = '&' if '?' in base_url else '?'
    return base_url + f'{sep}page={page_num}'


# ─────────────────────────────────────────────────────────────────────────────
# Scroll helper (search) — FIXED v6.1
# ─────────────────────────────────────────────────────────────────────────────

def _scroll_page(page, steps: int = 8) -> None:
    """Scroll down gradually with navigation safety."""
    try:
        for i in range(steps):
            try:
                page.mouse.wheel(0, random.randint(500, 900))
                page.wait_for_timeout(random.randint(SCROLL_PAUSE - 100, SCROLL_PAUSE + 200))
            except Exception as scroll_err:
                # Navigation may have occurred during scroll
                if "Execution context was destroyed" in str(scroll_err):
                    print(f"[search_scraper] ⚠️ Context destroyed at step {i}, stopping scroll")
                    return
                raise
        
        # Return to top safely
        try:
            page.evaluate("window.scrollTo(0, 0)")
        except:
            pass
        
        page.wait_for_timeout(500)
        
        # Light re-scroll (fewer steps)
        for _ in range(2):
            try:
                page.mouse.wheel(0, random.randint(400, 600))
                page.wait_for_timeout(400)
            except:
                return
    except Exception as e:
        print(f"[search_scraper] ⚠️ Scroll error: {e}")


def _wait_for_page_load(page, timeout: int = 20000) -> bool:
    """Wait for page to fully load after navigation."""
    try:
        # Wait for main content
        page.wait_for_selector(
            '#card-list, .hm_hn, a.search-card-item, a.lw_b.h7_ic',
            timeout=timeout
        )
        # Additional wait for lazy-loaded content
        page.wait_for_timeout(random.randint(2000, 3500))
        return True
    except Exception as e:
        print(f"[search_scraper] ⚠️ Page load wait failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PUBLIC FUNCTION — search results scraping (FIXED v6.1)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_search_results(
    search_url: str,
    max_pages: int = MAX_SEARCH_PAGES,
    max_products: int = 0,
    deduplicate: bool = True,
) -> Dict:
    """
    Scrape AliExpress search result pages and return product IDs + URLs.

    Args:
        search_url:   Full AliExpress search URL.
        max_pages:    Safety cap on pages scraped (default 50).
        max_products: Stop after N unique products; 0 = unlimited.
        deduplicate:  Skip already-seen product IDs.

    Returns:
        {
            "products":       List[{product_id, product_url, title, price, rating, sold}],
            "total_products": int,
            "pages_scraped":  int,
            "search_url":     str,
        }
    """
    all_products: List[Dict] = []
    seen_ids:     set        = set()
    pages_scraped            = 0
    ua                       = random.choice(USER_AGENTS)

    print(f"\n[search_scraper] ══════════════════════════════")
    print(f"[search_scraper] URL: {search_url[:100]}")
    print(f"[search_scraper] Max pages: {max_pages} | Max products: {max_products or '∞'}")
    print(f"[search_scraper] ══════════════════════════════")

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
                print(f"\n[search_scraper] 📄 Page {page_num}...")

                # ── Navigate ────────────────────────────────────────────
                try:
                    if page_num == 1:
                        page.goto(search_url, timeout=PAGE_TIMEOUT,
                                  wait_until='domcontentloaded')
                    else:
                        next_url = _build_page_url(search_url, page_num)
                        page.goto(next_url, timeout=PAGE_TIMEOUT,
                                  wait_until='domcontentloaded')
                    
                    # CRITICAL: Wait for page to fully stabilize
                    if not _wait_for_page_load(page):
                        print(f"[search_scraper] ⚠️ Page load verification failed")
                        break
                    
                    _dismiss_banners(page)
                    page.wait_for_timeout(1500)
                    
                except Exception as e:
                    print(f"[search_scraper] ❌ Navigation error page {page_num}: {e}")
                    break

                # ── Extract products FIRST (BEFORE scroll) ──────────────────
                page_products = _extract_products_from_page(page)

                new_count = 0
                for prod in page_products:
                    pid = prod['product_id']
                    if deduplicate and pid in seen_ids:
                        continue
                    seen_ids.add(pid)
                    all_products.append(prod)
                    new_count += 1

                pages_scraped = page_num
                print(f"[search_scraper] ✅ Page {page_num}: "
                      f"+{new_count} new | total={len(all_products)}")

                # ── Check pagination BEFORE scrolling ────────────────────
                if not _has_next_page(page):
                    print(f"[search_scraper] 🏁 No more pages after page {page_num}")
                    break

                total_pages = _get_total_pages(page)
                if page_num >= total_pages:
                    print(f"[search_scraper] 🏁 Reached last page ({total_pages})")
                    break

                # ── Max products cap ─────────────────────────────────────
                if max_products > 0 and len(all_products) >= max_products:
                    print(f"[search_scraper] 🛑 Max products ({max_products}) reached")
                    break

                page.wait_for_timeout(random.randint(2000, 3500))

            page.close()
            context.close()

    except Exception as e:
        print(f"[search_scraper] ❌ Browser session error: {e}")
        import traceback
        traceback.print_exc()

    # Trim to cap if needed
    if max_products > 0:
        all_products = all_products[:max_products]

    print(f"\n[search_scraper] ── Final ───────────────────────")
    print(f"  Products:   {len(all_products)}")
    print(f"  Pages:      {pages_scraped}")
    print(f"  Unique IDs: {len(seen_ids)}")
    print(f"[search_scraper] ──────────────────────────────────")

    return {
        "products":       all_products,
        "total_products": len(all_products),
        "pages_scraped":  pages_scraped,
        "search_url":     search_url,
    }


# ═════════════════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import sys

    args = sys.argv[1:]

    if args and ('aliexpress.com/w/' in args[0] or 'SearchText=' in args[0]):
        result = scrape_search_results(args[0], max_pages=int(args[1]) if len(args) > 1 else 3)
        print(f"\n{'='*60}")
        print(f"Total: {result['total_products']} products from {result['pages_scraped']} pages")
        for p in result['products'][:15]:
            print(f"  [{p['product_id']}] {p['title'][:60]}")
            print(f"    Price: {p['price']}  Rating: {p['rating']}")
    else:
        test_url = args[0] if args else \
            "https://www.aliexpress.com/item/1005010089125608.html"
        result = get_product_info(test_url, extract_compliance=False)
        if result:
            print("\n" + "=" * 70)
            for k, v in sorted(result.items())[:20]:
                if not str(v).strip():
                    continue
                print(f"  {k:30}: {str(v)[:80]}")
        else:
            print("❌ FAILED")
