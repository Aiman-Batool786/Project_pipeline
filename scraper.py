"""
scraper.py v5
─────────────
Camoufox browser — intercepts mtop API + extracts compliance popup.
Stores seller info, specs, compliance in structured format.
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

MAX_RETRIES  = 3

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
            block    = html[s + len(start_marker):e]
            assign   = block.find('window._dida_config_._init_data_=')
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
# COMPLIANCE MODAL EXTRACTOR
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
# SELLER INFO FROM DOM
# ─────────────────────────────────────────────────────────────────────────────

def _extract_seller_from_popup(page) -> dict:
    """Extract seller info by opening EU 'Trader' popup."""
    seller = {}

    # Try clicking any Trader/Sprzedawca/Vendeur button
    click_selectors = [
        'span:text("Trader")', 'span:text("Sprzedawca")', 'span:text("Vendeur")',
        '[class*="trader"]', '[class*="store-header--storeName"]'
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

    # Parse popup modal text blocks
    try:
        modal = page.locator('.comet-v2-modal, .storeInfo, .traderInfo').first
        modal.wait_for(timeout=5000)
        text = modal.inner_text()
        for line in text.split('\n'):
            low = line.lower().strip()
            if 'name' in low or 'store' in low:
                seller['store_name'] = line.split(':', 1)[-1].strip()
            elif 'country' in low or 'location' in low:
                seller['seller_country'] = line.split(':', 1)[-1].strip()
            elif 'open' in low or 'since' in low:
                seller['store_open_date'] = line.split(':', 1)[-1].strip()
    except Exception as e:
        print(f"[scraper] ⚠️ Popup parse failed: {e}")

    return {k: v for k, v in seller.items() if v}

    # Store open date, location, shop number from info blocks
    info_patterns = {
        'store_open_date': [
            r'(?:Opening date|Opened|Since|Data otwarcia)[:\s]+([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})',
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
# RETRY WRAPPER
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


# Backward compatibility alias
resolve_category_from_init_data = resolve_category


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PUBLIC FUNCTION
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


if __name__ == '__main__':
    import sys
    test_url = sys.argv[1] if len(sys.argv) > 1 else \
        "https://www.aliexpress.com/item/1005010089125608.html"
    result = get_product_info(test_url, extract_compliance=True)
    if result:
        print("\n" + "=" * 70)
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
            print(f"  {k:30}: {str(v)[:100]}")
        cat = resolve_category(result)
        print(f"\n  CATEGORY: {cat}")
    else:
        print("❌ FAILED")
