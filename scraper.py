"""
scraper.py  v4
──────────────────────────────────────────────────────────────────────────────
Root-cause analysis of the Poland / EU region issue
────────────────────────────────────────────────────
Pakistan region  (www.aliexpress.com):
  • Page is SSR — HTML already contains window._dida_config_._init_data_
  • All data (title, seller, specs, category) is embedded in the HTML.
  • Strategy: parse init_data JSON from HTML.

Poland / EU region (pl.aliexpress.com, de.aliexpress.com, etc.):
  • Page is CSR — HTML is almost empty (just a <div id="root">).
  • Data comes from an API call: mtop.aliexpress.pdp.pc.query
  • The browser JS fetches the data AFTER page load.
  • Strategy: intercept the mtop API response via Playwright's response listener.
  • API response JSON has the same module structure (storeModule, titleModule…)
    but wrapped differently: data.data.result.<module>

Compliance Information (EU pages only):
  • Shown in a modal popup (class comet-v2-modal) when user clicks a link.
  • Triggered by: clicking text matching "Product Compliance Information" or
    "Informacje o zgodności produktu" (Polish).
  • The modal is rendered on the same page — no API call needed.
  • Strategy: click the link, wait for modal, parse its text.

Category fix:
  • The category ID is available in categoryModule.categories[] in the API JSON.
  • Also extractable from breadcrumb HTML elements after page renders.

Fix summary:
  1. ALWAYS intercept the mtop API call (works for both SSR and CSR pages).
  2. Fall back to init_data from HTML (for SSR pages only).
  3. Added compliance info extractor (EU pages).
  4. Added category breadcrumb parser from both API and DOM.
"""

import re
import json
import random
import time
import urllib.request
import concurrent.futures
from camoufox.sync_api import Camoufox

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

SPEC_MAPPING = {
    'brand':             ['brand', 'brand name', 'marque', 'manufacturer',
                          'marka', 'producent'],
    'color':             ['color', 'colour', 'main color', 'couleur',
                          'kolor', 'farbe', 'couleur principale'],
    'dimensions':        ['dimensions', 'size', 'product size', 'package size',
                          'item size', 'product dimensions', 'wymiary', 'rozmiar'],
    'weight':            ['weight', 'net weight', 'gross weight', 'poids',
                          'waga', 'gewicht', 'peso'],
    'material':          ['material', 'materials', 'composition', 'matiere',
                          'fabric type', 'materiał'],
    'country_of_origin': ['origin', 'country of origin', 'made in',
                          'country/region of manufacture', 'kraj pochodzenia'],
    'warranty':          ['warranty', 'garantie', 'warranty period',
                          'warranty type', 'gwarancja'],
    'certifications':    ['certification', 'certifications', 'certificate',
                          'compliance', 'standard', 'ce', 'rohs', 'certyfikat'],
    'product_type':      ['product type', 'type', 'item type', 'style', 'category',
                          'typ produktu'],
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

MAX_RETRIES = 3

# Category ID → name lookup (extend as you encounter new IDs)
CATEGORY_ID_MAP = {
    '5090301':   'Cell Phones',
    '509':       'Phones & Telecommunications',
    '202238810': 'Cell Phones (Refurbished)',
    '202238004': 'Consumer Electronics',
    '202237809': 'Cell Phones',
    '201768104': 'Men\'s Pants',        # joggers / sweatpants
    '200000345': 'Women\'s Clothing',
    '200000346': 'Men\'s Clothing',
    '200003655': 'Tablets',
    '100006654': 'Smart Watches',
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _s(v) -> str:
    """Safe string: returns '' for None/falsy/literal 'None'/'0'."""
    if v is None:
        return ''
    s = str(v).strip()
    return '' if s in ('None', 'null', '0', 'false', 'undefined') else s


def _safe(d, *keys, default=None):
    """Safely traverse nested dict/list."""
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
# SELLER PARSER  (shared by API and init_data paths)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_seller_block(store: dict) -> dict:
    """Extract all seller fields from any store/shop dict."""
    if not store or not isinstance(store, dict):
        return {}

    sid = _s(
        store.get('storeNum') or store.get('sellerId') or
        store.get('storeId') or store.get('shopId') or
        store.get('userId') or store.get('memberId')
    )

    result = {
        'store_name': _s(
            store.get('storeName') or store.get('sellerName') or
            store.get('shopName') or store.get('name') or store.get('shopTitle')
        ),
        'store_id':              sid,
        'seller_id':             _s(store.get('sellerId') or store.get('userId') or store.get('memberId')),
        'store_url':             _s(store.get('storeUrl') or store.get('shopUrl') or
                                    (f"https://www.aliexpress.com/store/{sid}" if sid else '')),
        'seller_country':        _s(store.get('country') or store.get('countryCompleteName') or store.get('shopCountry')),
        'seller_rating':         _s(store.get('positiveRate') or store.get('itemAs') or store.get('sellerRating')),
        'seller_positive_rate':  _s(store.get('positiveRate') or store.get('positiveFeedbackRate')),
        'seller_communication':  _s(store.get('communicationRating') or store.get('serviceAs') or store.get('communicationScore')),
        'seller_shipping_speed': _s(store.get('shippingRating') or store.get('shippingAs') or store.get('shippingScore')),
        'store_open_date':       _s(store.get('openTime') or store.get('openDate') or store.get('establishedDate')),
        'seller_level':          _s(store.get('sellerLevel') or store.get('shopLevel') or store.get('level')),
        'seller_total_reviews':  _s(store.get('totalEvaluationNum') or store.get('reviewNum') or store.get('feedbackCount')),
        'seller_positive_num':   _s(store.get('positiveNum') or store.get('positiveFeedbackNum')),
        'is_top_rated':          _s(store.get('isTopRatedSeller') or store.get('topRatedSeller')),
    }
    return {k: v for k, v in result.items() if v}


# ─────────────────────────────────────────────────────────────────────────────
# MTOP API RESPONSE PARSER
# This is the PRIMARY data source for BOTH Pakistan (SSR) AND Poland (CSR) pages.
# The mtop API is always called by the browser JS, even on SSR pages.
# Response format: {"ret":["SUCCESS::..."],"data":{"result":{...modules...}}}
# ─────────────────────────────────────────────────────────────────────────────

def parse_mtop_response(text: str) -> dict | None:
    """
    Parse the mtop.aliexpress.pdp.pc.query API response.
    Returns a flat extracted dict or None if parsing fails / no title.
    """
    try:
        # Strip JSONP wrapper if present (e.g. mtopjsonp1({...}))
        m = re.match(r'^[a-zA-Z0-9_$]+\((.*)\);?\s*$', text.strip(), re.DOTALL)
        if m:
            text = m.group(1)

        outer = json.loads(text)

        # Navigate to the result object
        # Structure: {ret:[], data:{result:{titleModule, storeModule, ...}}}
        result = (
            _safe(outer, 'data', 'result') or
            _safe(outer, 'data', 'data') or
            _safe(outer, 'data') or
            {}
        )
        if not result or not isinstance(result, dict):
            return None

        print(f"[scraper] 📦 mtop result keys: {list(result.keys())[:15]}")
        extracted = {}

        # ── TITLE ──
        for tk in ['titleModule', 'TITLE', 'GLOBAL_DATA']:
            tb = result.get(tk, {})
            if isinstance(tb, dict):
                t = (_s(tb.get('subject')) or _s(tb.get('title')) or
                     _s(_safe(tb, 'globalData', 'subject')))
                if t:
                    extracted['title'] = t
                    break

        # ── PRICE ──
        pm = result.get('priceModule') or result.get('PRICE') or {}
        if isinstance(pm, dict):
            extracted['price'] = _s(
                pm.get('formatedActivityPrice') or pm.get('formatedPrice') or
                pm.get('formattedPrice') or _safe(pm, 'minActivityAmount', 'formattedAmount')
            )

        # ── IMAGES ──
        im = result.get('imageModule') or result.get('IMAGE') or {}
        if isinstance(im, dict):
            imgs = im.get('imagePathList') or im.get('imageList') or []
            for idx, url in enumerate(imgs[:20], 1):
                if url:
                    url = ('https:' + url) if str(url).startswith('//') else url
                    extracted[f'image_{idx}'] = url

        # Fallback image from titleModule
        if not extracted.get('image_1'):
            tm = result.get('titleModule') or {}
            imgs = tm.get('images') or []
            for idx, url in enumerate(imgs[:20], 1):
                if url:
                    extracted[f'image_{idx}'] = url

        # ── SPECS ──
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

        # ── SELLER ── (this is the main fix: storeModule in mtop response)
        for sk in ['storeModule', 'SHOP_CARD_PC', 'sellerModule', 'shopInfo']:
            sb = result.get(sk, {})
            if isinstance(sb, dict) and sb:
                seller = _parse_seller_block(sb)
                if seller.get('store_name') or seller.get('store_id'):
                    extracted.update(seller)
                    print(f"[scraper] 🏪 Seller from mtop '{sk}': {list(seller.keys())}")
                    break

        # ── CATEGORY ── from categoryModule
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
                    print(f"[scraper] 📂 Category: {extracted.get('category_path')}")

        # ── RATING / REVIEWS ──
        fb = result.get('feedbackModule') or result.get('FEEDBACK') or {}
        if isinstance(fb, dict):
            extracted['rating']  = _s(fb.get('trialRating') or fb.get('averageStar'))
            extracted['reviews'] = _s(fb.get('trialNum') or fb.get('totalCount'))

        # ── DESCRIPTION URL ──
        dm = result.get('descriptionModule') or result.get('DESCRIPTION') or {}
        if isinstance(dm, dict):
            du = _s(dm.get('descriptionUrl'))
            if du:
                extracted['_description_url'] = du

        # ── BULLET POINTS ──
        for path in [
            lambda r: r.get('highlights') or _safe(r, 'highlightModule', 'highlightList'),
            lambda r: _safe(r, 'tradeModule', 'highlights'),
        ]:
            try:
                items = path(result)
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
# HTML INIT_DATA PARSER (fallback for SSR pages only)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_from_html_init_data(html: str) -> dict:
    """
    Parse window._dida_config_._init_data_ from HTML.
    Only present on SSR pages (Pakistan/US region typically).
    """
    if not html:
        return {}

    # Find the init_data block between markers
    try:
        start_marker = '/*!-->init-data-start--*/'
        end_marker   = '/*!-->init-data-end--*/'
        s = html.find(start_marker)
        e = html.find(end_marker)
        if s != -1 and e != -1:
            block = html[s + len(start_marker):e]
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
                # The data is nested: { data: { data: { root: ..., titleModule: ... } } }
                inner = _safe(outer, 'data', 'data') or _safe(outer, 'data') or outer
                print(f"[scraper] 📄 init_data keys: {list(inner.keys())[:10]}")
                # Re-use the mtop parser by wrapping inner as a fake mtop result
                return parse_mtop_response(json.dumps({'data': {'result': inner}})) or {}
    except Exception as e:
        print(f"[scraper] ⚠️ init_data parse error: {e}")

    return {}


# ─────────────────────────────────────────────────────────────────────────────
# COMPLIANCE INFORMATION EXTRACTOR
# Clicks the "Product Compliance Information" / "Informacje o zgodności produktu"
# link and parses the modal popup text.
# Works for both English and Polish pages.
# ─────────────────────────────────────────────────────────────────────────────

def _extract_compliance_info(page) -> dict:
    """
    Click the compliance link and parse the modal.
    Returns dict with manufacturer/EU responsible person fields.
    Returns {} if not found (non-EU pages don't have this).
    """
    compliance = {}

    # --- Step 1: Find and click the compliance link ---
    # AliExpress renders this as a span or div with specific text
    # The data-spm-anchor-id="a2g0o.detail.0.i7.*" is on the modal title
    # The clickable trigger is usually a link/button near the product info

    compliance_link_selectors = [
        # English
        'text="Product Compliance Information"',
        'span:has-text("Product Compliance Information")',
        'a:has-text("Product Compliance Information")',
        '[data-spm-anchor-id*="i7"]',
        # Polish
        'text="Informacje o zgodności produktu"',
        'span:has-text("Informacje o zgodności")',
        'a:has-text("Informacje o zgodności")',
        # Generic compliance link patterns
        '[class*="compliance"]',
        '[class*="Compliance"]',
    ]

    clicked = False
    for sel in compliance_link_selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=2000):
                el.click(timeout=3000)
                page.wait_for_timeout(2000)
                print(f"[scraper] 🔒 Clicked compliance link: {sel}")
                clicked = True
                break
        except Exception:
            continue

    if not clicked:
        print("[scraper] ℹ️ No compliance link found (non-EU page or not visible)")
        return {}

    # --- Step 2: Wait for modal and extract text ---
    try:
        page.wait_for_selector('.comet-v2-modal, .comet-modal', timeout=6000)
        page.wait_for_timeout(1000)

        # Get all text from the modal
        modal = page.locator('.comet-v2-modal, .comet-modal').first
        if modal.count() == 0:
            return {}

        modal_text = modal.inner_text()
        print(f"[scraper] 🔒 Compliance modal text ({len(modal_text)} chars):\n{modal_text[:500]}")

        # --- Step 3: Parse the modal text ---
        # The modal has sections separated by headers:
        # "Manufacturer Details" / "Dane producenta"
        # "Details of the person responsible for compliance in the EU"
        # "Product ID"
        #
        # Each section has: Name, Address, Email address, Telephone number

        compliance = _parse_compliance_text(modal_text)
        print(f"[scraper] ✅ Compliance fields extracted: {list(compliance.keys())}")

    except Exception as e:
        print(f"[scraper] ⚠️ Compliance modal parse error: {e}")

    # --- Step 4: Close modal ---
    try:
        close_btn = page.locator(
            '.comet-v2-modal-close, .comet-modal-close, button[aria-label="Close"]'
        ).first
        if close_btn.count() > 0:
            close_btn.click(timeout=2000)
    except Exception:
        pass

    return compliance


def _parse_compliance_text(text: str) -> dict:
    """
    Parse the raw text of the compliance modal into structured fields.
    Handles both English and Polish text, and both manufacturer and EU rep sections.
    """
    result = {}

    # Normalize newlines
    text = text.replace('\r\n', '\n').replace('\r', '\n')

    # Helper: extract a value after a label (multi-language)
    def extract_field(label_patterns: list, text: str, stop_patterns: list = None) -> str:
        for pat in label_patterns:
            m = re.search(
                r'(?:' + re.escape(pat) + r')\s*[:\n]\s*([^\n]+)',
                text, re.IGNORECASE
            )
            if m:
                val = m.group(1).strip()
                # Stop at the next label if stop patterns provided
                if stop_patterns:
                    for sp in stop_patterns:
                        pos = re.search(re.escape(sp), val, re.IGNORECASE)
                        if pos:
                            val = val[:pos.start()].strip()
                return val
        return ''

    # ── Manufacturer ──
    # English labels
    result['manufacturer_name'] = extract_field(
        ['Name', 'Imię i nazwisko', 'Manufacturer Name'], text
    )
    result['manufacturer_address'] = extract_field(
        ['Address', 'Adres'], text,
        stop_patterns=['Email', 'Telephone', 'Phone', 'Numer', 'Adres e-mail']
    )
    result['manufacturer_email'] = extract_field(
        ['Email address', 'Adres e-mail', 'Email'], text
    )
    result['manufacturer_phone'] = extract_field(
        ['Telephone number', 'Numer telefonu', 'Phone', 'Tel'], text
    )

    # ── EU Responsible Person ──
    # Find the EU section (after "Details of the person responsible" or "person responsible")
    eu_section_match = re.search(
        r'(Details of the person responsible|person responsible for compliance in the EU|'
        r'Osoba odpowiedzialna|EU Representative|EU Responsible)',
        text, re.IGNORECASE
    )

    if eu_section_match:
        eu_text = text[eu_section_match.start():]
        result['eu_responsible_name'] = extract_field(
            ['Name', 'Imię i nazwisko', 'Nazwa'], eu_text
        )
        result['eu_responsible_address'] = extract_field(
            ['Address', 'Adres'], eu_text,
            stop_patterns=['Email', 'Telephone', 'Numer', 'Adres e-mail']
        )
        result['eu_responsible_email'] = extract_field(
            ['Email address', 'Adres e-mail', 'Email'], eu_text
        )
        result['eu_responsible_phone'] = extract_field(
            ['Telephone number', 'Numer telefonu', 'Phone'], eu_text
        )

    # ── Product ID ──
    # Format: "1005009197804507-12000048269218317" or just a number
    prod_id_match = re.search(
        r'Product ID[^\n]*\n\s*([0-9][-0-9]+)',
        text, re.IGNORECASE
    )
    if prod_id_match:
        result['compliance_product_id'] = prod_id_match.group(1).strip()
    else:
        # Try to find any long product-ID-like string
        m = re.search(r'(\d{13,20}(?:-\d{13,20})?)', text)
        if m:
            result['compliance_product_id'] = m.group(1)

    # Remove empty fields
    return {k: v for k, v in result.items() if v}


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
        print(f"[scraper] ⚠️ Description fetch failed: {e}")
        return ''


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER SCRAPE FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def _is_eu_url(url: str) -> bool:
    """Detect if URL is for an EU-region subdomain."""
    eu_domains = ['pl.aliexpress.com', 'de.aliexpress.com', 'fr.aliexpress.com',
                  'it.aliexpress.com', 'es.aliexpress.com', 'nl.aliexpress.com',
                  'pt.aliexpress.com', 'ko.aliexpress.com']
    return any(d in url for d in eu_domains)


def _dismiss_gdpr_banner(page):
    """Click through GDPR cookie consent banners on EU pages."""
    selectors = [
        'button:has-text("Accept All")', 'button:has-text("Accept all cookies")',
        'button:has-text("Agree")', 'button:has-text("I Accept")',
        'button:has-text("Akceptuj")',   # Polish: Accept
        'button:has-text("Akceptuję")',  # Polish: I accept
        '#accept-all', '.accept-all', '[data-testid="accept-all"]',
        '[class*="gdpr"] button', '[class*="cookie"] button',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=1500):
                el.click(timeout=2000)
                page.wait_for_timeout(800)
                print(f"[scraper] 🍪 GDPR dismissed via: {sel}")
                return True
        except Exception:
            continue
    return False


def _scrape_in_thread(url: str, try_compliance: bool = False) -> dict:
    """
    Open URL in Camoufox, intercept mtop API call, parse data.
    Also extracts compliance info if EU page and try_compliance=True.
    """
    captured_responses = []  # list of (text, url) tuples
    html     = ''
    seller   = {}
    compliance = {}

    ua = random.choice(USER_AGENTS)
    is_eu = _is_eu_url(url)
    print(f"[scraper] 🌐 EU page: {is_eu} | URL: {url[:80]}")

    try:
        with Camoufox(headless=True, os='windows') as browser:
            context = browser.new_context(
                viewport={'width': 1440, 'height': 900},
                locale='en-US',
                user_agent=ua,
                extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'}
            )
            page = context.new_page()

            # ── Intercept ALL API responses that look like product data ──
            def handle_response(response):
                try:
                    resp_url = response.url
                    # Match the main product API (both SSR and CSR pages call this)
                    if (('mtop.aliexpress.pdp.pc.query' in resp_url or
                         'mtop.aliexpress.itemdetail' in resp_url or
                         'pdp.pc.query' in resp_url) and
                            response.status == 200):
                        body = response.body()
                        if len(body) < 1000:
                            return
                        text = body.decode('utf-8', errors='replace')
                        # Only capture if it contains product module markers
                        if any(x in text for x in [
                            'titleModule', 'storeModule', 'SHOP_CARD_PC',
                            'imageModule', 'priceModule', 'categoryModule',
                            '"subject"', '"storeName"'
                        ]):
                            captured_responses.append(text)
                            print(f"[scraper] 📡 Captured mtop API "
                                  f"({len(text):,} bytes) from {resp_url[:80]}")
                except Exception:
                    pass

            page.on('response', handle_response)

            # Navigate to the page
            page.goto(url, timeout=90_000, wait_until='domcontentloaded')

            # EU pages need extra wait for CSR hydration + GDPR dismissal
            if is_eu:
                page.wait_for_timeout(2000)
                _dismiss_gdpr_banner(page)
                page.wait_for_timeout(1000)

            # Wait for the page to fully load (API call happens during this)
            page.wait_for_timeout(random.randint(5000, 7000))

            # Scroll to trigger lazy-loaded content
            for _ in range(4):
                page.mouse.wheel(0, random.randint(400, 800))
                page.wait_for_timeout(random.randint(600, 1000))
            page.wait_for_timeout(2000)

            # ── Extract compliance info from EU pages ──
            if is_eu and try_compliance:
                print("[scraper] 🔒 Attempting compliance info extraction...")
                compliance = _extract_compliance_info(page)

            # ── Try to get seller from DOM as backup ──
            # (covers cases where storeModule is empty in API but DOM has store link)
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
                            break
            except Exception:
                pass

            # ── Try DOM breadcrumb for category ──
            breadcrumb_text = ''
            try:
                bc = page.locator('[class*="breadcrumb"], [class*="Breadcrumb"], nav[aria-label*="breadcrumb"]')
                if bc.count() > 0:
                    breadcrumb_text = bc.first.inner_text()
            except Exception:
                pass

            html = page.content()
            page.close()
            context.close()

    except Exception as e:
        print(f"[scraper] ❌ Browser error: {e}")
        import traceback
        traceback.print_exc()

    return {
        'captured': captured_responses,
        'html': html,
        'dom_seller': seller,
        'compliance': compliance,
        'breadcrumb_text': breadcrumb_text,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RETRY WRAPPER
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_with_retry(url: str, try_compliance: bool = False) -> dict:
    best = {'captured': [], 'html': '', 'dom_seller': {}, 'compliance': {}, 'breadcrumb_text': ''}

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n[scraper] 🔄 Attempt {attempt}/{MAX_RETRIES}")

        if attempt > 1:
            delay = random.uniform(4, 9)
            print(f"[scraper] ⏳ Back-off {delay:.1f}s...")
            time.sleep(delay)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                # Only try compliance on first attempt to save time
                future = ex.submit(_scrape_in_thread, url, try_compliance and attempt == 1)
                result = future.result(timeout=200)
        except Exception as e:
            print(f"[scraper] ❌ Attempt {attempt} error: {e}")
            continue

        # Check if we got a usable API response
        for text in sorted(result['captured'], key=len, reverse=True):
            parsed = parse_mtop_response(text)
            if parsed and parsed.get('title'):
                print(f"[scraper] ✅ Got title from mtop API on attempt {attempt}")
                result['best_parsed'] = parsed
                return result

        # Keep the best result (most captured bytes)
        if len(result['captured']) >= len(best['captured']):
            best = result
        print(f"[scraper] ⚠️ Attempt {attempt}: no product title in API response")

    print(f"[scraper] ⚠️ All {MAX_RETRIES} attempts done, using best available")
    return best


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY RESOLVER
# ─────────────────────────────────────────────────────────────────────────────

def resolve_category(extracted: dict) -> dict:
    """
    Build a category dict from extracted data.
    Compatible with main.py category format.
    """
    cat_id   = extracted.get('category_id', '').split(',')[-1].strip()  # Take leaf ID
    cat_name = extracted.get('category_name', '')
    cat_path = extracted.get('category_path', '')

    resolved_name = CATEGORY_ID_MAP.get(cat_id, '') or cat_name

    if not resolved_name and cat_path:
        # Use the last segment of the path as the name
        resolved_name = cat_path.split(' > ')[-1].strip()

    return {
        'id':         cat_id or '0',
        'name':       resolved_name or 'Uncategorized',
        'leaf':       cat_name or (cat_path.split(' > ')[-1] if cat_path else 'Uncategorized'),
        'path':       cat_path,
        'confidence': 0.95 if cat_id and cat_id != '0' else (0.7 if cat_name else 0.3),
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PUBLIC FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str, extract_compliance: bool = True) -> dict | None:
    """
    Main entry point. Returns a flat dict with all extracted product fields.
    Set extract_compliance=True to also scrape the EU compliance popup.
    """
    print(f"\n[scraper] ══════════════════════════════════")
    print(f"[scraper] Starting: {url}")
    print(f"[scraper] ══════════════════════════════════")

    data = _scrape_with_retry(url, try_compliance=extract_compliance)

    extracted = {}

    # ── PRIMARY: Use pre-parsed mtop API result ──
    if data.get('best_parsed'):
        extracted = data['best_parsed']
        print("[scraper] ✅ Using pre-parsed mtop result")
    else:
        # Parse captured responses ourselves
        for text in sorted(data['captured'], key=len, reverse=True):
            parsed = parse_mtop_response(text)
            if parsed:
                for k, v in parsed.items():
                    if v and k not in extracted:
                        extracted[k] = v
                if extracted.get('title'):
                    break

    # ── FALLBACK: init_data from HTML (SSR pages) ──
    if not extracted.get('title') and data.get('html'):
        print("[scraper] 🔍 Trying HTML init_data fallback...")
        html_data = _extract_from_html_init_data(data['html'])
        for k, v in html_data.items():
            if v and k not in extracted:
                extracted[k] = v

    # ── FALLBACK: og:title from HTML ──
    if not extracted.get('title') and data.get('html'):
        m = re.search(
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']{5,300})["\']',
            data['html']
        )
        if m:
            extracted['title'] = m.group(1).strip()
            print(f"[scraper]    Title from og:title: {extracted['title'][:60]}")

    # ── FALLBACK: images from HTML ──
    if not extracted.get('image_1') and data.get('html'):
        # Try imagePathList in DCData
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
        print("[scraper] ❌ Could not extract product title after all attempts")
        return None

    # ── MERGE DOM seller data (fills gaps only) ──
    for k, v in data.get('dom_seller', {}).items():
        if v and not extracted.get(k):
            extracted[k] = v
            print(f"[scraper]    ✅ Seller from DOM: {k} = {v}")

    # ── MERGE compliance data ──
    compliance = data.get('compliance', {})
    if compliance:
        extracted['compliance'] = compliance
        print(f"[scraper] ✅ Compliance info: {list(compliance.keys())}")

    # ── FETCH DESCRIPTION ──
    desc_url = extracted.pop('_description_url', '')
    if desc_url and not extracted.get('description'):
        print(f"[scraper] 📄 Fetching description from URL...")
        extracted['description'] = fetch_description(desc_url)
        if extracted['description']:
            print(f"[scraper] ✅ Description: {len(extracted['description'])} chars")

    # ── APPLY DEFAULTS ──
    defaults = {
        'description': '', 'brand': '', 'color': '', 'dimensions': '',
        'weight': '', 'material': '', 'certifications': '',
        'country_of_origin': '', 'warranty': '', 'product_type': '',
        'shipping': '', 'price': '', 'rating': '', 'reviews': '',
        'bullet_points': [], 'age_from': '', 'age_to': '',
        'gender': '', 'safety_warning': '',
        # seller
        'store_name': '', 'store_id': '', 'store_url': '',
        'seller_id': '', 'seller_positive_rate': '', 'seller_rating': '',
        'seller_communication': '', 'seller_shipping_speed': '',
        'seller_country': '', 'store_open_date': '', 'seller_level': '',
        'seller_total_reviews': '', 'seller_positive_num': '', 'is_top_rated': '',
        # category
        'category_id': '', 'category_name': '', 'category_path': '',
        # compliance (nested dict)
        'compliance': {},
    }
    for key, default in defaults.items():
        extracted.setdefault(key, default)

    for i in range(1, 21):
        extracted.setdefault(f'image_{i}', '')

    # ── PRINT SUMMARY ──
    seller_fields = [k for k in ['store_name', 'store_id', 'seller_rating',
                                  'seller_country', 'seller_communication',
                                  'seller_shipping_speed'] if extracted.get(k)]
    spec_fields   = [k for k in SPEC_MAPPING if extracted.get(k)]

    print(f"\n[scraper] ── Summary ──────────────────────")
    print(f"  Title:      {extracted['title'][:70]}")
    print(f"  Category:   {extracted.get('category_path') or extracted.get('category_name') or '—'}")
    print(f"  Seller:     {seller_fields}")
    print(f"  Specs:      {spec_fields}")
    print(f"  Images:     {sum(1 for i in range(1, 21) if extracted.get(f'image_{i}'))}")
    print(f"  Compliance: {list(extracted.get('compliance', {}).keys())}")
    print(f"[scraper] ────────────────────────────────")

    return extracted


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE EXPORTS  (for backward compatibility with main.py)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_category_from_init_data(extracted: dict) -> dict:
    """Alias for resolve_category — backward compat with main.py."""
    return resolve_category(extracted)


# ─────────────────────────────────────────────────────────────────────────────
# CLI TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys

    test_url = sys.argv[1] if len(sys.argv) > 1 else \
        "https://pl.aliexpress.com/item/1005005386754780.html"

    result = get_product_info(test_url, extract_compliance=True)
    if result:
        print("\n" + "=" * 80)
        print("EXTRACTED FIELDS:")
        print("=" * 80)
        for k, v in sorted(result.items()):
            if k.startswith('image_') and not v:
                continue
            if k == 'specs_raw':
                print(f"  specs_raw ({len(v)} items):")
                for sk, sv in list(v.items())[:10]:
                    print(f"    {sk:30}: {sv}")
                continue
            if k == 'compliance' and v:
                print(f"  compliance:")
                for ck, cv in v.items():
                    print(f"    {ck:35}: {cv}")
                continue
            val = str(v)
            print(f"  {k:35}: {val[:100]}{'...' if len(val) > 100 else ''}")

        cat = resolve_category(result)
        print(f"\n  CATEGORY RESOLVED: {cat}")
    else:
        print("❌ No result extracted")
