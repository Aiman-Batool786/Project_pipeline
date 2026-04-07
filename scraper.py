"""
scraper.py v6
─────────────
Camoufox browser — intercepts mtop API + extracts compliance popup.
Stores seller info, specs, compliance in structured format.
UPDATED: Better API capture, direct API fallback, improved selectors
"""

import re
import json
import random
import time
import urllib.request
import urllib.error
import concurrent.futures
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

MAX_RETRIES = 3

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


def extract_item_id(url: str) -> str | None:
    """Extract product ID from URL"""
    patterns = [
        r'/item/(\d+)\.html',
        r'itemId=(\d+)',
        r'productId=(\d+)',
        r'/(\d+)\.html'
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# SELLER PARSER
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
        # Remove JSONP wrapper if present
        m = re.match(r'^[a-zA-Z0-9_$]+\((.*)\);?\s*$', text.strip(), re.DOTALL)
        if m:
            text = m.group(1)

        outer = json.loads(text)
        
        # Try different response structures
        result = None
        if isinstance(outer, dict):
            result = (_safe(outer, 'data', 'result') or 
                     _safe(outer, 'data', 'data') or 
                     _safe(outer, 'data') or 
                     outer.get('result') or
                     outer)
        
        if not result or not isinstance(result, dict):
            return None

        print(f"[scraper] 📦 mtop result keys: {list(result.keys())[:15]}")
        extracted = {}

        # Title - try multiple paths
        for tk in ['titleModule', 'TITLE', 'GLOBAL_DATA', 'title', 'productTitle']:
            tb = result.get(tk, {})
            if isinstance(tb, dict):
                t = (_s(tb.get('subject')) or _s(tb.get('title')) or
                     _s(_safe(tb, 'globalData', 'subject')) or
                     _s(tb.get('productTitle')))
                if t:
                    extracted['title'] = t
                    break
        if not extracted.get('title') and isinstance(result.get('title'), str):
            extracted['title'] = _s(result.get('title'))

        # Price
        pm = result.get('priceModule') or result.get('PRICE') or result.get('price') or {}
        if isinstance(pm, dict):
            extracted['price'] = _s(
                pm.get('formatedActivityPrice') or pm.get('formatedPrice') or
                pm.get('formattedPrice') or pm.get('price') or
                _safe(pm, 'minActivityAmount', 'formattedAmount')
            )

        # Images
        im = result.get('imageModule') or result.get('IMAGE') or result.get('images') or {}
        if isinstance(im, dict):
            for idx, url in enumerate((im.get('imagePathList') or im.get('imageList') or [])[:20], 1):
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
        for sk in ['PRODUCT_PROP_PC', 'specsModule', 'productPropComponent', 
                   'skuModule', 'props', 'specifications']:
            sb = result.get(sk, {})
            if isinstance(sb, dict):
                raw = (sb.get('props') or sb.get('showedProps') or
                       sb.get('specsList') or sb.get('productSKUPropertyList') or
                       sb.get('specificationList') or [])
                if raw:
                    props = raw
                    print(f"[scraper] 📋 {len(props)} specs from '{sk}'")
                    break

        if props:
            extracted['specs_raw'] = {}
            for prop in props:
                if not isinstance(prop, dict):
                    continue
                name = _s(prop.get('attrName') or prop.get('name') or prop.get('key')).lower()
                value = _s(prop.get('attrValue') or prop.get('value') or prop.get('val'))
                if not name or not value:
                    continue
                for field, keywords in SPEC_MAPPING.items():
                    if any(kw in name for kw in keywords):
                        extracted.setdefault(field, value)
                        break
                extracted['specs_raw'][name] = value

        # Seller from API
        for sk in ['storeModule', 'SHOP_CARD_PC', 'sellerModule', 'shopInfo', 'seller']:
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
        fb = result.get('feedbackModule') or result.get('FEEDBACK') or result.get('rating') or {}
        if isinstance(fb, dict):
            extracted['rating']  = _s(fb.get('trialRating') or fb.get('averageStar') or fb.get('score'))
            extracted['reviews'] = _s(fb.get('trialNum') or fb.get('totalCount') or fb.get('count'))

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
            lambda r: r.get('bulletPoints'),
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
# DIRECT API FALLBACK
# ─────────────────────────────────────────────────────────────────────────────

def direct_api_request(item_id: str) -> dict | None:
    """Fallback: Try direct API request to AliExpress"""
    try:
        # Try multiple API endpoints
        endpoints = [
            f"https://gpservice.aliexpress.com/pcdetail/queryDetail?productId={item_id}&locale=en_US",
            f"https://api.aliexpress.com/pcdetail/queryDetail?productId={item_id}&locale=en_US",
            f"https://acs.aliexpress.com/pcdetail/queryDetail?productId={item_id}&locale=en_US",
        ]
        
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.aliexpress.com/',
            'Origin': 'https://www.aliexpress.com',
        }
        
        for endpoint in endpoints:
            try:
                print(f"[scraper] 🔌 Trying direct API: {endpoint[:80]}...")
                req = urllib.request.Request(endpoint, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as response:
                    data = json.loads(response.read().decode('utf-8'))
                    if data.get('data', {}).get('result'):
                        result = parse_mtop_response(json.dumps(data))
                        if result and result.get('title'):
                            return result
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"[scraper] Direct API failed: {e}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# HTML INIT_DATA PARSER (SSR fallback)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_from_html_init_data(html: str) -> dict:
    if not html:
        return {}
    try:
        # Try multiple init data patterns
        patterns = [
            (r'window\._dida_config_\._init_data_\s*=\s*({.*?});', 1),
            (r'window\.runParams\s*=\s*({.*?});', 1),
            (r'data:\s*({.*?}),\s*"retCode"', 1),
            (r'__INITIAL_STATE__\s*=\s*({.*?});', 1),
        ]
        
        for pattern, group in patterns:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(group))
                    if isinstance(data, dict):
                        # Extract product data
                        product_data = (_safe(data, 'data', 'data') or 
                                       _safe(data, 'data', 'result') or
                                       data.get('result') or
                                       data.get('product') or
                                       data)
                        if product_data and isinstance(product_data, dict):
                            parsed = parse_mtop_response(
                                json.dumps({'data': {'result': product_data}})
                            )
                            if parsed and parsed.get('title'):
                                print("[scraper] ✅ Extracted from init_data")
                                return parsed
                except json.JSONDecodeError:
                    continue
                    
    except Exception as e:
        print(f"[scraper] ⚠️ init_data parse error: {e}")
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# COMPLIANCE MODAL EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────

def _parse_compliance_text(text: str) -> dict:
    result = {}
    text = text.replace('\r\n', '\n').replace('\r', '\n')

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

    result['manufacturer_name'] = extract_field(
        ['Name', 'Imię i nazwisko', 'Manufacturer Name', 'Manufacturer'], text)
    result['manufacturer_address'] = extract_field(
        ['Address', 'Adres'], text,
        stop_patterns=['Email', 'Telephone', 'Phone', 'Numer', 'Adres e-mail'])
    result['manufacturer_email'] = extract_field(
        ['Email address', 'Adres e-mail', 'Email'], text)
    result['manufacturer_phone'] = extract_field(
        ['Telephone number', 'Numer telefonu', 'Phone', 'Tel'], text)

    eu_match = re.search(
        r'(Details of the person responsible|person responsible for compliance in the EU|'
        r'Osoba odpowiedzialna|EU Representative|EU Responsible|Importer)',
        text, re.IGNORECASE
    )
    if eu_match:
        eu_text = text[eu_match.start():]
        result['eu_responsible_name'] = extract_field(
            ['Name', 'Imię i nazwisko', 'Nazwa'], eu_text)
        result['eu_responsible_address'] = extract_field(
            ['Address', 'Adres'], eu_text,
            stop_patterns=['Email', 'Telephone', 'Numer', 'Adres e-mail'])
        result['eu_responsible_email'] = extract_field(
            ['Email address', 'Adres e-mail', 'Email'], eu_text)
        result['eu_responsible_phone'] = extract_field(
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
        'text="Informacje o zgodności produktu"',
        'span:has-text("Informacje o zgodności")',
        'button:has-text("Compliance")',
        'a:has-text("Compliance")',
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
        page.wait_for_selector('.comet-v2-modal, .comet-modal, [role="dialog"]', timeout=8000)
        page.wait_for_timeout(1500)
        modal = page.locator('.comet-v2-modal, .comet-modal, [role="dialog"]').first
        if modal.count() == 0:
            return {}
        modal_text = modal.inner_text()
        print(f"[scraper] 🔒 Compliance modal ({len(modal_text)} chars)")
        compliance = _parse_compliance_text(modal_text)
        print(f"[scraper] ✅ Compliance fields: {list(compliance.keys())}")
    except Exception as e:
        print(f"[scraper] ⚠️ Compliance modal error: {e}")

    try:
        page.locator(
            '.comet-v2-modal-close, .comet-modal-close, button[aria-label="Close"], [class*="close"]'
        ).first.click(timeout=2000)
    except Exception:
        pass

    return compliance


# ─────────────────────────────────────────────────────────────────────────────
# SELLER INFO FROM DOM (always runs — no popup needed)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_seller_from_dom(page) -> dict:
    """Extract seller info visible on the page without opening any popup."""
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
                    seller['store_id'] = m.group(1)
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
        '[data-spm="seller"]',
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

    # Regex patterns against full page text for open date, country, shop number
    info_patterns = {
        'store_open_date': [
            r'(?:Opening date|Opened|Since|Data otwarcia)[:\s]+([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})',
        ],
        'seller_country': [
            r'(?:Location|Country|Kraj|From)[:\s]+([A-Za-z\s]{2,30})',
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
# SELLER INFO FROM EU TRADER POPUP
# ─────────────────────────────────────────────────────────────────────────────

def _extract_seller_from_popup(page) -> dict:
    """Extract seller info by opening EU Trader popup modal."""
    seller = {}

    # Click Trader/Sprzedawca button to open popup
    click_selectors = [
        'span:text("Trader")', 'span:text("Sprzedawca")', 'span:text("Vendeur")',
        'button:text("Trader")', 'a:text("Trader")',
        '[class*="trader"]', '[class*="store-header--storeName"]',
        '[data-spm="seller"]',
    ]
    for sel in click_selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=3000):
                el.click(timeout=4000)
                page.wait_for_timeout(2500)
                print(f"[scraper] ✅ Clicked seller button: {sel}")
                break
        except Exception:
            continue

    # Parse popup modal text
    try:
        modal = page.locator('.comet-v2-modal, .storeInfo, .traderInfo, [role="dialog"]').first
        modal.wait_for(timeout=8000)
        text = modal.inner_text()
        for line in text.split('\n'):
            low = line.lower().strip()
            if ':' in line:
                val = line.split(':', 1)[-1].strip()
                if not val:
                    continue
                if 'name' in low or 'store' in low:
                    seller.setdefault('store_name', val)
                elif 'country' in low or 'location' in low:
                    seller.setdefault('seller_country', val)
                elif 'open' in low or 'since' in low:
                    seller.setdefault('store_open_date', val)
                elif 'no.' in low or 'number' in low:
                    if re.match(r'^\d+$', val):
                        seller.setdefault('store_id', val)
                        seller.setdefault(
                            'store_url',
                            f"https://www.aliexpress.com/store/{val}"
                        )
    except Exception as e:
        print(f"[scraper] ⚠️ Popup parse failed: {e}")

    return {k: v for k, v in seller.items() if v}


# ─────────────────────────────────────────────────────────────────────────────
# GDPR DISMISSER
# ─────────────────────────────────────────────────────────────────────────────

def _dismiss_gdpr_banner(page) -> bool:
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


def _is_eu_url(url: str) -> bool:
    eu_domains = ['pl.aliexpress.com', 'de.aliexpress.com', 'fr.aliexpress.com',
                  'it.aliexpress.com', 'es.aliexpress.com', 'nl.aliexpress.com']
    return any(d in url for d in eu_domains)


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
# BROWSER SCRAPE
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_in_thread(url: str, try_compliance: bool = False) -> dict:
    captured = []
    html = ''
    dom_seller = {}
    compliance = {}

    ua = random.choice(USER_AGENTS)
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
                    # Expanded API patterns
                    if any(pattern in resp_url for pattern in [
                        'mtop.aliexpress.pdp.pc.query',
                        'mtop.aliexpress.itemdetail',
                        'pdp.pc.query',
                        'mtop.aliexpress.pcdetail',
                        'mtop.taobao.pcdetail.data',
                        'detail.getDetail',
                        'queryDetail',
                        'getProductDetail'
                    ]):
                        body = response.body()
                        if len(body) < 500:
                            return
                        text = body.decode('utf-8', errors='replace')
                        # Look for product data indicators
                        if any(x in text for x in [
                            'titleModule', 'storeModule', 'SHOP_CARD_PC',
                            'imageModule', 'priceModule', '"subject"', '"storeName"',
                            '"title"', '"productTitle"', 'result'
                        ]):
                            captured.append(text)
                            print(f"[scraper] 📡 Captured API ({len(text):,} bytes) from {resp_url[:80]}")
                except Exception:
                    pass

            page.on('response', handle_response)
            page.goto(url, timeout=120000, wait_until='networkidle')
            page.wait_for_timeout(5000)

            if is_eu:
                page.wait_for_timeout(2000)
                _dismiss_gdpr_banner(page)
                page.wait_for_timeout(1000)

            # Scroll to trigger lazy loading
            for _ in range(6):
                page.mouse.wheel(0, random.randint(400, 800))
                page.wait_for_timeout(random.randint(500, 900))
            page.wait_for_timeout(3000)

            # Always extract seller from DOM first
            dom_seller = _extract_seller_from_dom(page)

            # If DOM didn't get store name, try EU Trader popup
            if not dom_seller.get('store_name'):
                print("[scraper] 🏪 Trying Trader popup mode...")
                popup_seller = _extract_seller_from_popup(page)
                for k, v in popup_seller.items():
                    dom_seller.setdefault(k, v)

            # Extract compliance info
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
        'captured': captured,
        'html': html,
        'dom_seller': dom_seller,
        'compliance': compliance,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RETRY WRAPPER
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_with_retry(url: str, try_compliance: bool = False) -> dict:
    best = {'captured': [], 'html': '', 'dom_seller': {}, 'compliance': {}}
    
    # Try direct API first (fastest)
    item_id = extract_item_id(url)
    if item_id:
        print(f"[scraper] 🔍 Trying direct API for item {item_id}")
        direct_result = direct_api_request(item_id)
        if direct_result and direct_result.get('title'):
            print("[scraper] ✅ Direct API succeeded")
            best['best_parsed'] = direct_result
            return best

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n[scraper] 🔄 Attempt {attempt}/{MAX_RETRIES}")

        if attempt > 1:
            delay = random.uniform(8, 15)
            print(f"[scraper] ⏳ Back-off {delay:.1f}s...")
            time.sleep(delay)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(
                    _scrape_in_thread, url,
                    try_compliance and attempt == 1
                )
                result = future.result(timeout=300)
        except Exception as e:
            print(f"[scraper] ❌ Attempt {attempt} error: {e}")
            continue

        # Try to parse captured responses
        for text in sorted(result['captured'], key=len, reverse=True):
            parsed = parse_mtop_response(text)
            if parsed and parsed.get('title'):
                print(f"[scraper] ✅ Success on attempt {attempt}")
                result['best_parsed'] = parsed
                return result

        if len(result['captured']) >= len(best['captured']):
            best = result
        print(f"[scraper] ⚠️ Attempt {attempt}: no title in API response")

    print(f"[scraper]
