"""
scraper.py
──────────
Camoufox-based AliExpress product detail page (PDP) scraper.

How it works:
  1. Opens the product URL in a real browser (Camoufox/Firefox)
  2. Intercepts the mtop.aliexpress.pdp.pc.query API response
     -> Contains title, price, specs, images, seller info, descriptionUrl
  3. Extracts seller details from the store-info popup (DOM click)
  4. Fetches description from the descriptionUrl (separate HTTP call)
  5. Falls back to parsing window._dida_config_._init_data_ if API not captured

Seller popup selectors (from live HTML inspection):
  - Trigger: a[href*='/store/']  or  [class*='store-header']
  - Info table container: [class*='storeInfo'] (hash suffix changes per deploy)
  - Rating table container: [class*='storeRating']
  - Table rows: td[0] = label, td[1] = value (ratings have <b> around number)

Description:
  - From API: result.descriptionModule.descriptionUrl -> fetch + strip HTML tags
  - Fallback: parse text from page DOM
"""

import re
import json
import random
import time
import urllib.request
import concurrent.futures
from camoufox.sync_api import Camoufox


# ─────────────────────────────────────────────────────────────────────────────
# SPEC MAPPING  (certifications + product_type added)
# ─────────────────────────────────────────────────────────────────────────────
SPEC_MAPPING = {
    'brand':             ['brand', 'brand name', 'marque', 'manufacturer'],
    'color':             ['color', 'colour', 'main color', 'couleur'],
    'dimensions':        ['dimensions', 'size', 'product size', 'package size',
                          'item size', 'product dimensions'],
    'weight':            ['weight', 'net weight', 'gross weight', 'poids'],
    'material':          ['material', 'materials', 'composition', 'matiere',
                          'fabric type'],
    'country_of_origin': ['origin', 'country of origin', 'made in',
                          'country/region of manufacture'],
    'warranty':          ['warranty', 'garantie', 'warranty period',
                          'warranty type', 'warranty information'],
    'certifications':    ['certification', 'certifications', 'certificate',
                          'compliance', 'standard', 'normes', 'ce', 'rohs'],
    'product_type':      ['product type', 'type', 'item type',
                          'type de produit', 'style', 'category'],
}

# ─────────────────────────────────────────────────────────────────────────────
# ANTI-BOT: REGION + USER-AGENT ROTATION
# ─────────────────────────────────────────────────────────────────────────────
REGIONS = ["US", "GB", "DE", "FR", "AE", "AU", "CA"]

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

MAX_RETRIES = 3


def _get_rotated_url(url: str) -> str:
    region = random.choice(REGIONS)
    sep    = '&' if '?' in url else '?'
    print(f"[scraper] 🌍 Region → {region}")
    return f"{url}{sep}shipFromCountry={region}"


# ─────────────────────────────────────────────────────────────────────────────
# SPEC HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def map_props_to_fields(props: list) -> tuple:
    raw = {}
    for item in props:
        name  = str(item.get('attrName') or item.get('name')  or '').strip().lower()
        value = str(item.get('attrValue') or item.get('value') or '').strip()
        if name and value and value.lower() != 'none':
            raw[name] = value

    mapped = {}
    for field, keywords in SPEC_MAPPING.items():
        for k, v in raw.items():
            if any(kw in k for kw in keywords):
                mapped[field] = v
                break
    return mapped, raw


# ─────────────────────────────────────────────────────────────────────────────
# SELLER BLOCK PARSER  (covers all 14 SELLER_FIELDS expected by main.py)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_seller_block(store: dict) -> dict:
    sid = str(store.get('storeNum') or store.get('sellerId') or
              store.get('storeId') or '')
    seller = {
        'store_name':            store.get('storeName') or store.get('sellerName') or '',
        'store_id':              sid,
        'seller_id':             str(store.get('sellerId') or store.get('userId') or ''),
        'store_url':             (store.get('storeUrl') or
                                  (f"https://www.aliexpress.com/store/{sid}" if sid else '')),
        'seller_country':        store.get('country') or store.get('countryCompleteName') or '',
        'seller_rating':         str(store.get('positiveRate') or store.get('itemAs') or ''),
        'seller_positive_rate':  str(store.get('positiveRate') or ''),
        'seller_communication':  str(store.get('communicationRating') or store.get('serviceAs') or ''),
        'seller_shipping_speed': str(store.get('shippingRating') or store.get('shippingAs') or ''),
        'store_open_date':       str(store.get('openTime') or store.get('openDate') or ''),
        'seller_level':          str(store.get('sellerLevel') or store.get('shopLevel') or ''),
        'seller_total_reviews':  str(store.get('totalEvaluationNum') or store.get('reviewNum') or ''),
        'seller_positive_num':   str(store.get('positiveNum') or ''),
        'is_top_rated':          str(store.get('isTopRatedSeller') or store.get('topRatedSeller') or ''),
    }
    return {k: v for k, v in seller.items() if v and str(v).strip()}


# ─────────────────────────────────────────────────────────────────────────────
# BULLET POINTS EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def _extract_bullet_points(result: dict) -> list:
    bullets = []
    for path in [
        lambda r: r.get('highlights') or r.get('highlightModule', {}).get('highlightList'),
        lambda r: r.get('tradeModule', {}).get('highlights'),
        lambda r: r.get('descriptionModule', {}).get('features'),
        lambda r: r.get('PRODUCT_PROP_PC', {}).get('features'),
        lambda r: r.get('sellingPoints') or r.get('keyPoints') or r.get('productFeatures'),
    ]:
        try:
            items = path(result)
            if items and isinstance(items, list):
                bullets = [str(h.get('title') or h.get('text') or h)
                           for h in items if h]
                if bullets:
                    break
        except Exception:
            pass
    return [b.strip() for b in bullets if b.strip()][:10]


# ─────────────────────────────────────────────────────────────────────────────
# DESCRIPTION FETCHER
# Fetches the description from the separate descriptionUrl endpoint.
# AliExpress loads product descriptions from a separate CDN URL like:
#   https://aeproductsourcesite.alicdn.com/product/description/pc/v2/en_US/desc.htm?productId=XXX
# This returns an HTML page with images and text blocks.
# ─────────────────────────────────────────────────────────────────────────────

def fetch_description(url: str) -> str:
    """Fetch AliExpress description URL and return cleaned text (max 3000 chars)."""
    try:
        ua  = random.choice(USER_AGENTS)
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': ua,
                'Accept': 'text/html,application/xhtml+xml',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.aliexpress.com/',
            }
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode('utf-8', errors='replace')
        # Strip HTML tags
        clean = re.sub(r'<style[^>]*>.*?</style>', ' ', raw, flags=re.DOTALL)
        clean = re.sub(r'<script[^>]*>.*?</script>', ' ', clean, flags=re.DOTALL)
        clean = re.sub(r'<[^>]+>', ' ', clean)
        clean = re.sub(r'\s+', ' ', clean).strip()
        return clean[:3000]
    except Exception as e:
        print(f"[scraper] ⚠️  Description fetch failed: {e}")
        return ''


# ─────────────────────────────────────────────────────────────────────────────
# API RESPONSE PARSER
# Parses the mtop.aliexpress.pdp.pc.query intercepted response.
# ─────────────────────────────────────────────────────────────────────────────

def parse_pdp_response(text: str) -> dict | None:
    try:
        # Remove JSONP wrapper if present (e.g. mtopjsonp1({...}))
        m = re.match(r'^[a-zA-Z0-9_$]+\((.*)\);?\s*$', text.strip(), re.DOTALL)
        if m:
            text = m.group(1)

        data   = json.loads(text)
        result = (data.get('data', {}).get('result', {})
                  or data.get('data', {}) or {})
        if not result:
            return None

        extracted = {}

        # ── TITLE ──────────────────────────────────────────────────────────
        try:
            extracted['title'] = result['titleModule']['subject']
        except Exception:
            try:
                extracted['title'] = (
                    result.get('GLOBAL_DATA', {}).get('globalData', {}).get('subject', '')
                )
            except Exception:
                pass

        # ── PRICE ──────────────────────────────────────────────────────────
        try:
            pm = result.get('priceModule') or {}
            extracted['price'] = (pm.get('formatedActivityPrice')
                                  or pm.get('formatedPrice') or '')
        except Exception:
            pass

        # ── SELLER (from API response) ──────────────────────────────────────
        # AliExpress embeds seller info in storeModule or SHOP_CARD_PC
        try:
            store = (result.get('storeModule') or
                     result.get('SHOP_CARD_PC') or
                     result.get('sellerModule') or {})
            if store:
                seller_data = _parse_seller_block(store)
                extracted.update(seller_data)
                print(f"[scraper] 🏪 API seller fields: {list(seller_data.keys())}")
        except Exception as e:
            print(f"[scraper] ⚠️  API seller parse error: {e}")

        # ── SPECIFICATIONS ──────────────────────────────────────────────────
        # Try multiple paths AliExpress uses across different page versions
        props = []
        try:
            props = (
                result.get('PRODUCT_PROP_PC', {}).get('showedProps', []) or
                result.get('skuModule', {}).get('productSKUPropertyList', []) or
                result.get('productProp', {}).get('props', []) or
                result.get('specsModule', {}).get('props', [])
            )
        except Exception:
            pass

        if props:
            mapped, raw_specs = map_props_to_fields(props)
            extracted.update(mapped)
            print(f"[scraper] ✅ {len(props)} spec items → {list(mapped.keys())}")

        # ── BULLET POINTS ───────────────────────────────────────────────────
        bullets = _extract_bullet_points(result)
        if bullets:
            extracted['bullet_points'] = bullets
            print(f"[scraper] ✅ {len(bullets)} bullet points")

        # ── IMAGES ─────────────────────────────────────────────────────────
        images = []
        try:
            images = (result.get('imageModule', {}).get('imagePathList', []) or
                      result.get('titleModule', {}).get('images', []))
        except Exception:
            pass

        for idx, img in enumerate(images[:20], 1):
            if img:
                img = 'https:' + img if str(img).startswith('//') else img
                extracted[f'image_{idx}'] = re.sub(r'_\d+x\d+', '', img)

        # ── DESCRIPTION URL ─────────────────────────────────────────────────
        # Store the URL so get_product_info() can fetch it after parsing
        try:
            desc_url = result.get('descriptionModule', {}).get('descriptionUrl', '')
            if desc_url:
                extracted['_description_url'] = desc_url
        except Exception:
            pass

        return extracted if extracted.get('title') else None

    except Exception as e:
        print(f"[scraper] ⚠️  API parse error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# INIT_DATA PARSER
# Fallback: parse window._dida_config_._init_data_ from the HTML source.
# AliExpress embeds all page data as JSON between marker comments.
# ─────────────────────────────────────────────────────────────────────────────

def _parse_init_data(html: str) -> dict:
    """
    Extract product data from the embedded _init_data_ JSON block.
    Marker pattern:  /*!-->init-data-start--*/  ...JSON...  /*!-->init-data-end--*/
    """
    try:
        # Find the embedded init data block
        m = re.search(
            r'window\._dida_config_\._init_data_\s*=\s*\{[^{]*data:\s*(\{.*?\})\s*\}',
            html,
            re.DOTALL
        )
        if not m:
            # Alternative: between marker comments
            m = re.search(
                r'/\*!-->init-data-start--\*/\s*window\._dida_config_\._init_data_=\s*\{[^{]*data:\s*(\{.*?)\s*\};\s*/\*!-->init-data-end--\*/',
                html,
                re.DOTALL
            )
        if not m:
            return {}

        raw_json = m.group(1)
        # The JSON may be cut off — try to parse what we have
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            # Try to find the result object directly
            return {}

        # For PDP pages, result is nested under data.data
        result = (data.get('data', {}) or {})
        if not result:
            return {}

        extracted = {}

        # Title
        title = (result.get('titleModule', {}).get('subject') or
                 result.get('GLOBAL_DATA', {}).get('globalData', {}).get('subject', ''))
        if title:
            extracted['title'] = title

        # Price
        pm = result.get('priceModule') or {}
        price = pm.get('formatedActivityPrice') or pm.get('formatedPrice')
        if price:
            extracted['price'] = price

        # Seller
        store = (result.get('storeModule') or result.get('SHOP_CARD_PC') or {})
        if store:
            extracted.update(_parse_seller_block(store))

        # Specs
        props = (result.get('PRODUCT_PROP_PC', {}).get('showedProps', []) or
                 result.get('skuModule', {}).get('productSKUPropertyList', []) or [])
        if props:
            mapped, _ = map_props_to_fields(props)
            extracted.update(mapped)

        # Images
        images = result.get('imageModule', {}).get('imagePathList', [])
        for idx, img in enumerate(images[:20], 1):
            if img:
                img = 'https:' + img if str(img).startswith('//') else img
                extracted[f'image_{idx}'] = re.sub(r'_\d+x\d+', '', img)

        # Description URL
        desc_url = result.get('descriptionModule', {}).get('descriptionUrl', '')
        if desc_url:
            extracted['_description_url'] = desc_url

        print(f"[scraper] 📋 init_data fallback: {len(extracted)} fields")
        return extracted

    except Exception as e:
        print(f"[scraper] ⚠️  init_data parse error: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# SELLER POPUP EXTRACTOR  (DOM-based)
# 
# AliExpress seller popup structure (confirmed from live HTML):
#
# Trigger:  Click on the store link  a[href*="/store/"]
#           or hover over [class*="store-header"]
#
# Popup info table:
#   Container: [class*="storeInfo"]  (e.g. .store-detail--storeInfo--BMDFsTB)
#   Rows: <tr><td>Label:</td><td>Value</td></tr>
#   Known labels: "Store Name", "Store ID No.", "Location", "Open Since"
#
# Popup rating table:  
#   Container: [class*="storeRating"]  (e.g. .store-detail--storeRating--Z2j7q9u)
#   Rows: <tr><td>Label</td><td><b>4.8</b> (above average)</td></tr>
#   Known labels: "Item as Described", "Communication", "Shipping Speed"
# ─────────────────────────────────────────────────────────────────────────────

def _extract_seller_from_popup(page) -> dict:
    """
    Click the store link to open the seller info popup,
    then parse the info and rating tables.
    Returns a dict with whatever seller fields were found.
    """
    seller = {}

    # ── Step 1: Get store URL + ID from page links (no click needed) ────────
    try:
        links = page.locator('a[href*="/store/"]').all()
        for link in links[:5]:
            href = link.get_attribute('href') or ''
            if '/store/' in href:
                if href.startswith('//'):
                    href = 'https:' + href
                m = re.search(r'/store/(\d+)', href)
                if m:
                    seller['store_id']  = m.group(1)
                    seller['store_url'] = f"https://www.aliexpress.com/store/{m.group(1)}"
                    print(f"[scraper]    ✅ Store ID from link: {m.group(1)}")
                    break
    except Exception as e:
        print(f"[scraper]    ⚠️  Store link error: {e}")

    # ── Step 2: Get store name from visible page elements ───────────────────
    # These class names are from inspecting live AliExpress PDP pages.
    # Using partial class matching ([class*=...]) because AliExpress
    # appends random hashes to class names that change per deployment.
    store_name_selectors = [
        '[class*="store-header--storeName"]',
        '[class*="shopName"]',
        '[class*="shop-name"]',
        '[class*="sellerName"]',
        '[class*="StoreName"]',
        'a[href*="/store/"] span',
        '[class*="store-info"] h3',
        '[class*="storeInfo"] h3',
    ]
    for sel in store_name_selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0:
                text = el.inner_text().strip()
                if text and 2 < len(text) < 100:
                    seller['store_name'] = text
                    print(f"[scraper]    ✅ Store name: {text}")
                    break
        except Exception:
            continue

    # ── Step 3: Click the store info trigger to open popup ──────────────────
    # Try multiple selectors — AliExpress changes these with redesigns.
    click_selectors = [
        # The "Trader" badge (EU regulation label shown on EU pages)
        'span:text("Trader")',
        'span:text("trader")',
        '[class*="trader"]',
        # Store header link / button
        '[class*="store-header"]',
        '[class*="shopHeader"]',
        # Direct store link (most reliable)
        'a[href*="/store/"]',
        # Fallback generic seller info areas
        '[class*="sellerInfo"]',
        '[class*="seller-info"]',
        '[class*="storeScore"]',
    ]

    popup_opened = False
    for sel in click_selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=2000):
                el.click(timeout=3000)
                page.wait_for_timeout(2500)
                print(f"[scraper]    ✅ Clicked: {sel}")
                popup_opened = True
                break
        except Exception:
            continue

    if not popup_opened:
        print("[scraper]    ⚠️  Could not click store trigger — popup not opened")

    # ── Step 4: Parse seller info table from popup ──────────────────────────
    # The popup container uses partial class names with hash suffixes.
    # We use [class*='storeInfo'] to match regardless of the hash.
    info_container_selectors = [
        '[class*="storeInfo"]',
        '[class*="store-detail"]',
        '[class*="shopInfo"]',
        '.store-detail--storeInfo--BMDFsTB',   # specific hash (may change)
    ]

    for container_sel in info_container_selectors:
        try:
            container = page.locator(container_sel).first
            if container.count() > 0:
                print(f"[scraper]    ✅ Seller info container: {container_sel}")
                rows = container.locator('table tr').all()
                for row in rows:
                    try:
                        cells = row.locator('td').all()
                        if len(cells) >= 2:
                            key   = cells[0].inner_text().strip().lower().rstrip(':')
                            value = cells[1].inner_text().strip()
                            print(f"[scraper]       info: [{key}] = [{value}]")
                            # Map label text to field names.
                            # AliExpress uses different labels per language/region.
                            if any(k in key for k in ['name', 'store name', 'nom', 'nombre']):
                                if value:
                                    seller['store_name'] = value
                            elif any(k in key for k in ['id', 'no.', 'number', 'store id', 'numero']):
                                if value and value.isdigit():
                                    seller['store_id']  = value
                                    seller['store_url'] = f"https://www.aliexpress.com/store/{value}"
                            elif any(k in key for k in ['location', 'country', 'pays', 'pais', 'land']):
                                if value:
                                    seller['seller_country'] = value.strip()
                            elif any(k in key for k in ['open', 'since', 'date', 'ouvert', 'abierto']):
                                if value:
                                    seller['store_open_date'] = value
                    except Exception:
                        continue
                break   # found container, no need to try other selectors
        except Exception:
            continue

    # ── Step 5: Parse seller rating table from popup ─────────────────────────
    # Rating tables have a <b> tag inside td[1] for the numeric value.
    rating_container_selectors = [
        '[class*="storeRating"]',
        '[class*="shopRating"]',
        '.store-detail--storeRating--Z2j7q9u',  # specific hash (may change)
    ]

    for container_sel in rating_container_selectors:
        try:
            container = page.locator(container_sel).first
            if container.count() > 0:
                print(f"[scraper]    ✅ Rating container: {container_sel}")
                rows = container.locator('table tr').all()
                for row in rows:
                    try:
                        cells = row.locator('td').all()
                        if len(cells) >= 2:
                            key  = cells[0].inner_text().strip().lower()
                            # Rating number is in <b> tag
                            b_el = cells[1].locator('b').first
                            value = (b_el.inner_text().strip()
                                     if b_el.count() > 0
                                     else cells[1].inner_text().strip())
                            # Keep only the numeric rating part
                            num_match = re.search(r'[\d.]+', value)
                            value = num_match.group(0) if num_match else value
                            print(f"[scraper]       rating: [{key}] = [{value}]")
                            if any(k in key for k in ['item', 'described', 'produit', 'artículo']):
                                seller['seller_rating'] = value
                            elif any(k in key for k in ['communic', 'contact', 'kommunik']):
                                seller['seller_communication'] = value
                            elif any(k in key for k in ['ship', 'deliver', 'livr', 'envío', 'versand']):
                                seller['seller_shipping_speed'] = value
                    except Exception:
                        continue
                break
        except Exception:
            continue

    # ── Step 6: Save debug HTML if popup failed ─────────────────────────────
    if not seller.get('store_name') and not seller.get('seller_rating'):
        print("[scraper]    ⚠️  Popup parse yielded no data — saving debug HTML")
        try:
            with open('/tmp/seller_popup_debug.html', 'w', encoding='utf-8') as f:
                f.write(page.content())
            print("[scraper]    💾 Saved /tmp/seller_popup_debug.html")
        except Exception:
            pass

    return {k: v for k, v in seller.items() if v}


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER SCRAPE FUNCTION  (runs in thread due to Camoufox sync API)
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_in_thread(url: str) -> dict:
    captured = []
    html     = ''
    seller   = {}

    ua = random.choice(USER_AGENTS)
    print(f"[scraper] 🕵️  UA: {ua[:65]}...")

    try:
        with Camoufox(headless=True, os='windows') as browser:
            context = browser.new_context(
                viewport={'width': 1366, 'height': 900},
                locale='en-US',
                user_agent=ua,
                extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'}
            )
            page = context.new_page()

            # ── Intercept the main product data API ─────────────────────────
            # AliExpress calls mtop.aliexpress.pdp.pc.query which returns
            # ALL product data (title, specs, seller, images, descriptionUrl)
            # as one large JSON response.
            def handle_response(response):
                try:
                    url_str = response.url
                    if ('mtop.aliexpress.pdp.pc.query' in url_str or
                            'mtop.aliexpress.itemdetail' in url_str or
                            'mtop.aliexpress.ae.trade.order.detail' in url_str):
                        body = response.body()
                        if len(body) < 5000:   # too small to be useful
                            return
                        text = body.decode('utf-8', errors='replace')
                        # Quick sanity check: must contain key product fields
                        if any(x in text for x in ['titleModule', 'storeModule',
                                                    'PRODUCT_PROP_PC', 'SHOP_CARD_PC',
                                                    'imageModule', 'descriptionModule']):
                            captured.append(text)
                            print(f"[scraper] 📡 Captured API response ({len(text):,} bytes)")
                except Exception:
                    pass

            page.on('response', handle_response)

            # ── Load the product page ────────────────────────────────────────
            page.goto(url, timeout=90_000, wait_until='domcontentloaded')

            # Wait for page to fully render and JS to execute
            page.wait_for_timeout(random.randint(4000, 6000))

            # Simulate human scrolling to trigger lazy-loaded content
            for _ in range(5):
                page.mouse.wheel(0, random.randint(300, 700))
                page.wait_for_timeout(random.randint(500, 1000))

            # Extra wait for API responses to arrive
            page.wait_for_timeout(2000)

            # ── Extract seller info from popup ───────────────────────────────
            print("[scraper] 🏪 Extracting seller info from popup...")
            seller = _extract_seller_from_popup(page)
            print(f"[scraper]    DOM seller result: {seller}")

            html = page.content()
            page.close()
            context.close()

    except Exception as e:
        print(f'[scraper] ❌ Browser error: {e}')
        import traceback
        traceback.print_exc()

    return {'captured': captured, 'html': html, 'seller': seller}


# ─────────────────────────────────────────────────────────────────────────────
# RETRY WRAPPER
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_with_retry(url: str) -> dict:
    best_result = {'captured': [], 'html': '', 'seller': {}}

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n[scraper] 🔄 Attempt {attempt}/{MAX_RETRIES}")
        attempt_url = _get_rotated_url(url)

        if attempt > 1:
            delay = random.uniform(3, 8)
            print(f"[scraper] ⏳ Back-off {delay:.1f}s...")
            time.sleep(delay)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(_scrape_in_thread, attempt_url)
                result = future.result(timeout=180)

            # Check if we captured useful data
            for text in sorted(result['captured'], key=len, reverse=True):
                parsed = parse_pdp_response(text)
                if parsed and parsed.get('title'):
                    print(f"[scraper] ✅ Good data on attempt {attempt}")
                    return result

            if len(result['captured']) >= len(best_result['captured']):
                best_result = result

            print(f"[scraper] ⚠️  Attempt {attempt}: no title found, retrying...")

        except Exception as e:
            print(f"[scraper] ❌ Attempt {attempt} error: {e}")

    print(f"[scraper] ⚠️  All {MAX_RETRIES} attempts done — using best result")
    return best_result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PUBLIC FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    """
    Scrape a single AliExpress product URL and return all extracted data.
    Returns None if no title could be extracted.
    """
    print(f"[scraper] Starting: {url}")
    data = _scrape_with_retry(url)

    extracted = {}

    # ── Parse captured API responses ────────────────────────────────────────
    for text in sorted(data['captured'], key=len, reverse=True):
        parsed = parse_pdp_response(text)
        if parsed:
            for k, v in parsed.items():
                if v and k not in extracted:
                    extracted[k] = v
            if extracted.get('title'):
                break

    # ── Fallback: parse from embedded init_data in HTML ─────────────────────
    if not extracted.get('title') and data['html']:
        print("[scraper] 🔍 Trying init_data fallback...")
        init_data = _parse_init_data(data['html'])
        for k, v in init_data.items():
            if v and k not in extracted:
                extracted[k] = v

    # ── Title fallback: og:title meta tag ───────────────────────────────────
    if not extracted.get('title') and data['html']:
        m = re.search(
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']{5,200})["\']',
            data['html']
        )
        if m:
            extracted['title'] = m.group(1).strip()
            print(f"[scraper] 📋 Title from og:title: {extracted['title'][:60]}")

    # ── Image fallback: parse imagePathList from HTML ────────────────────────
    if not extracted.get('image_1') and data['html']:
        m = re.search(r'"imagePathList"\s*:\s*(\[[^\]]{10,}\])', data['html'])
        if m:
            try:
                urls = json.loads(m.group(1))
                for idx, img_url in enumerate(urls[:20], 1):
                    if img_url and not extracted.get(f'image_{idx}'):
                        extracted[f'image_{idx}'] = img_url
            except Exception:
                pass

    # ── Fetch description from URL ───────────────────────────────────────────
    desc_url = extracted.pop('_description_url', '')
    if desc_url and not extracted.get('description'):
        print(f"[scraper] 📄 Fetching description from URL...")
        extracted['description'] = fetch_description(desc_url)
        if extracted['description']:
            print(f"[scraper] ✅ Description: {len(extracted['description'])} chars")

    # ── Merge DOM seller info (fills gaps not in API response) ───────────────
    dom_seller = data.get('seller', {})
    for k, v in dom_seller.items():
        if v and not extracted.get(k):
            extracted[k] = v
            print(f"[scraper]    ✅ Seller from DOM: {k} = {v}")

    # ── Final summary ────────────────────────────────────────────────────────
    seller_fields = ['store_name','store_id','seller_rating','seller_country',
                     'seller_positive_rate','seller_communication',
                     'seller_shipping_speed','is_top_rated']
    spec_fields   = ['brand','color','dimensions','weight','material',
                     'certifications','country_of_origin','warranty','product_type']

    print(f"[scraper] ═══════════════════════════════")
    print(f"[scraper] Final fields:  {len(extracted)}")
    print(f"[scraper] Title:         {str(extracted.get('title',''))[:70]}")
    print(f"[scraper] Description:   {len(extracted.get('description',''))} chars")
    print(f"[scraper] Seller fields: {[k for k in seller_fields if extracted.get(k)]}")
    print(f"[scraper] Spec fields:   {[k for k in spec_fields if extracted.get(k)]}")
    print(f"[scraper] Bullet pts:    {len(extracted.get('bullet_points', []))}")
    print(f"[scraper] Images:        {sum(1 for i in range(1,21) if extracted.get(f'image_{i}'))}")
    print(f"[scraper] ═══════════════════════════════")

    # ── Apply defaults (ensures all expected keys exist) ─────────────────────
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
    }
    for key, default in defaults.items():
        if key not in extracted or not extracted[key]:
            extracted[key] = default

    for i in range(1, 21):
        extracted.setdefault(f'image_{i}', '')

    return extracted if extracted.get('title') else None


if __name__ == '__main__':
    test_url = "https://www.aliexpress.com/item/1005010089125608.html"
    result   = get_product_info(test_url)
    if result:
        print("\n" + "=" * 80)
        for k, v in sorted(result.items()):
            if not k.startswith('image_'):
                val = str(v)
                print(f"{k:28}: {val[:100]}{'...' if len(val) > 100 else ''}")
    else:
        print("❌ No result extracted")
