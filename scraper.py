"""
scraper.py
──────────
Camoufox-based AliExpress product detail page (PDP) scraper.

REGION ISSUE FIX:
  When your server IP is in Pakistan (or any non-EU country), using EU regions
  (DE, FR, PL, GB) causes AliExpress to:
    1. Trigger bot-detection (IP country ≠ requested region)
    2. Show GDPR consent banners that block all popup clicks
    3. Load EU DSA-compliant 'Trader' popup (different structure)
    4. Possibly redirect to country subdomains (de.aliexpress.com)

  FIX: Only rotate between non-EU regions that work from any IP.
       AE (UAE), US, AU, CA, PK all work reliably from Pakistan.
       If you move server to EU, re-add EU regions and handle GDPR banner.
"""

import re
import json
import random
import time
import urllib.request
import concurrent.futures
from camoufox.sync_api import Camoufox


# ─────────────────────────────────────────────────────────────────────────────
# SPEC MAPPING
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
# REGION LIST — NON-EU ONLY
#
# WHY NO EU REGIONS (DE, FR, PL, GB, etc.):
#   EU regions trigger GDPR consent banners and DSA "Trader" popup requirements.
#   These have completely different page structures that break our selectors.
#   From a Pakistan server IP, EU region params also trigger bot-detection
#   because AliExpress sees IP country ≠ requested region.
#
#   SAFE regions from any non-EU server: AE, US, AU, CA, PK, SA, TR, JP
# ─────────────────────────────────────────────────────────────────────────────
REGIONS_SAFE = ["AE", "US", "AU", "CA", "PK", "SA", "TR"]

# EU regions — only use if your server IP is inside EU
REGIONS_EU   = ["DE", "FR", "NL", "IT", "ES"]

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

# Set to True if your server is inside the EU
SERVER_IS_EU = False


def _get_regions():
    """Return safe region list based on server location."""
    return REGIONS_EU if SERVER_IS_EU else REGIONS_SAFE


def _get_rotated_url(url: str) -> str:
    """Append a non-EU shipFromCountry param. Never adds EU regions from non-EU IPs."""
    region = random.choice(_get_regions())
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
# SELLER BLOCK PARSER
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
                bullets = [str(h.get('title') or h.get('text') or h) for h in items if h]
                if bullets:
                    break
        except Exception:
            pass
    return [b.strip() for b in bullets if b.strip()][:10]


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
        print(f"[scraper] ⚠️  Description fetch failed: {e}")
        return ''


# ─────────────────────────────────────────────────────────────────────────────
# API RESPONSE PARSER
# ─────────────────────────────────────────────────────────────────────────────

def parse_pdp_response(text: str) -> dict | None:
    try:
        m = re.match(r'^[a-zA-Z0-9_$]+\((.*)\);?\s*$', text.strip(), re.DOTALL)
        if m:
            text = m.group(1)

        data   = json.loads(text)
        result = (data.get('data', {}).get('result', {}) or data.get('data', {}) or {})
        if not result:
            return None

        extracted = {}

        # Title
        try:
            extracted['title'] = result['titleModule']['subject']
        except Exception:
            try:
                extracted['title'] = result.get('GLOBAL_DATA', {}).get('globalData', {}).get('subject', '')
            except Exception:
                pass

        # Price
        try:
            pm = result.get('priceModule') or {}
            extracted['price'] = pm.get('formatedActivityPrice') or pm.get('formatedPrice') or ''
        except Exception:
            pass

        # Seller
        try:
            store = (result.get('storeModule') or result.get('SHOP_CARD_PC') or
                     result.get('sellerModule') or {})
            if store:
                seller_data = _parse_seller_block(store)
                extracted.update(seller_data)
                print(f"[scraper] 🏪 API seller: {list(seller_data.keys())}")
        except Exception as e:
            print(f"[scraper] ⚠️  API seller parse error: {e}")

        # Specs
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
            mapped, _ = map_props_to_fields(props)
            extracted.update(mapped)
            print(f"[scraper] ✅ {len(props)} specs → {list(mapped.keys())}")

        # Bullet points
        bullets = _extract_bullet_points(result)
        if bullets:
            extracted['bullet_points'] = bullets

        # Images
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

        # Description URL (fetched later)
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
# INIT_DATA FALLBACK PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_init_data(html: str) -> dict:
    try:
        m = re.search(
            r'window\._dida_config_\._init_data_\s*=\s*\{[^{]*data:\s*(\{.*?\})\s*\}',
            html, re.DOTALL
        )
        if not m:
            return {}
        data   = json.loads(m.group(1))
        result = data.get('data', {}) or {}
        if not result:
            return {}

        extracted = {}
        title = (result.get('titleModule', {}).get('subject') or
                 result.get('GLOBAL_DATA', {}).get('globalData', {}).get('subject', ''))
        if title:
            extracted['title'] = title

        pm = result.get('priceModule') or {}
        price = pm.get('formatedActivityPrice') or pm.get('formatedPrice')
        if price:
            extracted['price'] = price

        store = result.get('storeModule') or result.get('SHOP_CARD_PC') or {}
        if store:
            extracted.update(_parse_seller_block(store))

        props = (result.get('PRODUCT_PROP_PC', {}).get('showedProps', []) or
                 result.get('skuModule', {}).get('productSKUPropertyList', []) or [])
        if props:
            mapped, _ = map_props_to_fields(props)
            extracted.update(mapped)

        for idx, img in enumerate(result.get('imageModule', {}).get('imagePathList', [])[:20], 1):
            if img:
                img = 'https:' + img if str(img).startswith('//') else img
                extracted[f'image_{idx}'] = re.sub(r'_\d+x\d+', '', img)

        desc_url = result.get('descriptionModule', {}).get('descriptionUrl', '')
        if desc_url:
            extracted['_description_url'] = desc_url

        return extracted
    except Exception as e:
        print(f"[scraper] ⚠️  init_data parse error: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# GDPR CONSENT BANNER DISMISSER
# Only needed for EU pages. Tries to click "Accept" on consent banners.
# ─────────────────────────────────────────────────────────────────────────────

def _dismiss_gdpr_banner(page) -> bool:
    """
    Dismiss GDPR cookie consent banners that block interactions on EU pages.
    Returns True if a banner was found and dismissed.
    """
    gdpr_accept_selectors = [
        # AliExpress specific
        '[class*="gdpr"] button:has-text("Accept")',
        '[class*="gdpr"] button:has-text("Accept All")',
        '[class*="cookie"] button:has-text("Accept")',
        '[class*="cookie"] button:has-text("OK")',
        '[id*="gdpr"] button:has-text("Accept")',
        '[id*="cookie"] button:has-text("Accept")',
        # Generic EU consent banners
        'button:has-text("Accept all cookies")',
        'button:has-text("Accept All Cookies")',
        'button:has-text("Agree")',
        'button:has-text("I Accept")',
        '#accept-all',
        '.accept-all',
        '[data-testid="accept-all"]',
    ]
    for sel in gdpr_accept_selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=1500):
                el.click(timeout=2000)
                page.wait_for_timeout(1000)
                print(f"[scraper] 🍪 GDPR banner dismissed: {sel}")
                return True
        except Exception:
            continue
    return False


# ─────────────────────────────────────────────────────────────────────────────
# SELLER POPUP EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────

def _extract_seller_from_popup(page, is_eu_page: bool = False) -> dict:
    """
    Extract seller info from the store popup.
    Handles both standard and EU DSA "Trader" popup structures.
    """
    seller = {}

    # ── Step 1: Store ID + URL from links (no click needed) ─────────────────
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
                    print(f"[scraper]    ✅ Store ID: {m.group(1)}")
                    break
    except Exception as e:
        print(f"[scraper]    ⚠️  Store link error: {e}")

    # ── Step 2: Store name from visible page elements ────────────────────────
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

    # ── Step 3: Click the store info trigger ────────────────────────────────
    # EU pages show a "Trader" badge (required by DSA law) instead of store name.
    # Non-EU pages show the store name as a clickable link.
    click_selectors = [
        # EU DSA "Trader" badge (Poland/EU pages)
        'span:text("Trader")',
        'span:text("Verkäufer")',      # German "Trader"
        'span:text("Vendeur")',        # French "Trader"
        'span:text("Venditore")',      # Italian "Trader"
        '[class*="trader"]',
        '[class*="Trader"]',
        # Standard (non-EU) — store header link
        '[class*="store-header"]',
        '[class*="shopHeader"]',
        # Universal fallback
        'a[href*="/store/"]',
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
        print("[scraper]    ⚠️  Could not open seller popup")

    # ── Step 4: Parse seller info table ─────────────────────────────────────
    info_container_selectors = [
        '[class*="storeInfo"]',
        '[class*="store-detail"]',
        '[class*="shopInfo"]',
        # EU DSA Trader popup has different container class names
        '[class*="traderInfo"]',
        '[class*="trader-info"]',
        '[class*="sellerDetail"]',
    ]

    for container_sel in info_container_selectors:
        try:
            container = page.locator(container_sel).first
            if container.count() > 0:
                print(f"[scraper]    ✅ Info container: {container_sel}")
                rows = container.locator('table tr').all()

                if not rows:
                    # EU Trader popup may use dl/dt/dd instead of table
                    rows_dl = container.locator('dl').all()
                    if rows_dl:
                        dts = container.locator('dt').all()
                        dds = container.locator('dd').all()
                        for dt, dd in zip(dts, dds):
                            key   = dt.inner_text().strip().lower().rstrip(':')
                            value = dd.inner_text().strip()
                            _map_seller_label(key, value, seller)
                        break

                for row in rows:
                    try:
                        cells = row.locator('td').all()
                        if len(cells) >= 2:
                            key   = cells[0].inner_text().strip().lower().rstrip(':')
                            value = cells[1].inner_text().strip()
                            print(f"[scraper]       [{key}] = [{value}]")
                            _map_seller_label(key, value, seller)
                    except Exception:
                        continue
                break
        except Exception:
            continue

    # ── Step 5: Parse rating table ───────────────────────────────────────────
    rating_container_selectors = [
        '[class*="storeRating"]',
        '[class*="shopRating"]',
        '[class*="sellerRating"]',
    ]

    for container_sel in rating_container_selectors:
        try:
            container = page.locator(container_sel).first
            if container.count() > 0:
                print(f"[scraper]    ✅ Rating container: {container_sel}")
                for row in container.locator('table tr').all():
                    try:
                        cells = row.locator('td').all()
                        if len(cells) >= 2:
                            key  = cells[0].inner_text().strip().lower()
                            b_el = cells[1].locator('b').first
                            raw_val = (b_el.inner_text().strip()
                                       if b_el.count() > 0
                                       else cells[1].inner_text().strip())
                            num_m = re.search(r'[\d.]+', raw_val)
                            value = num_m.group(0) if num_m else raw_val
                            print(f"[scraper]       rating [{key}] = [{value}]")
                            if any(k in key for k in ['item', 'described', 'produit', 'articulo']):
                                seller['seller_rating'] = value
                            elif any(k in key for k in ['communic', 'contact', 'kommunik']):
                                seller['seller_communication'] = value
                            elif any(k in key for k in ['ship', 'deliver', 'livr', 'envio', 'versand']):
                                seller['seller_shipping_speed'] = value
                    except Exception:
                        continue
                break
        except Exception:
            continue

    # ── Step 6: Save debug HTML if nothing was found ─────────────────────────
    if not seller.get('store_name') and not seller.get('seller_rating'):
        print("[scraper]    ⚠️  Popup yielded no data — saving debug HTML")
        try:
            with open('/tmp/seller_popup_debug.html', 'w', encoding='utf-8') as f:
                f.write(page.content())
            print("[scraper]    💾 /tmp/seller_popup_debug.html")
        except Exception:
            pass

    return {k: v for k, v in seller.items() if v}


def _map_seller_label(key: str, value: str, seller: dict):
    """Map a label→value pair to the correct seller field. Supports multiple languages."""
    if not value:
        return
    # Store name
    if any(k in key for k in ['name', 'store name', 'nom', 'nombre', 'naam', 'nome']):
        seller.setdefault('store_name', value)
    # Store ID
    elif any(k in key for k in ['id', 'no.', 'number', 'store id', 'numero', 'nr']):
        if re.match(r'^\d+$', value.strip()):
            seller.setdefault('store_id', value.strip())
            seller.setdefault('store_url', f"https://www.aliexpress.com/store/{value.strip()}")
    # Country
    elif any(k in key for k in ['location', 'country', 'pays', 'pais', 'land', 'paese', 'kraj']):
        seller.setdefault('seller_country', value.strip())
    # Open date
    elif any(k in key for k in ['open', 'since', 'date', 'ouvert', 'abierto', 'geöffnet', 'aperto']):
        seller.setdefault('store_open_date', value)


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER SCRAPE FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def _detect_eu_page(url: str, html: str) -> bool:
    """
    Detect if the loaded page is an EU-regulated page.
    EU pages have GDPR banners, Trader badges, and different structure.
    """
    eu_indicators = [
        'gdpr', 'cookie-consent', 'Trader', 'DSA', 'digital-services',
        'de.aliexpress.com', 'fr.aliexpress.com', 'it.aliexpress.com',
        'es.aliexpress.com', 'nl.aliexpress.com',
    ]
    check_str = url + html[:5000]
    return any(indicator in check_str for indicator in eu_indicators)


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

            def handle_response(response):
                try:
                    url_str = response.url
                    if ('mtop.aliexpress.pdp.pc.query' in url_str or
                            'mtop.aliexpress.itemdetail' in url_str):
                        body = response.body()
                        if len(body) < 5000:
                            return
                        text = body.decode('utf-8', errors='replace')
                        if any(x in text for x in ['titleModule', 'storeModule',
                                                    'PRODUCT_PROP_PC', 'SHOP_CARD_PC',
                                                    'imageModule', 'descriptionModule']):
                            captured.append(text)
                            print(f"[scraper] 📡 Captured API ({len(text):,} bytes)")
                except Exception:
                    pass

            page.on('response', handle_response)
            page.goto(url, timeout=90_000, wait_until='domcontentloaded')
            page.wait_for_timeout(random.randint(4000, 6000))

            # ── Detect if page is EU-regulated ───────────────────────────────
            current_url = page.url
            page_html_snippet = page.content()[:5000]
            is_eu = _detect_eu_page(current_url, page_html_snippet)

            if is_eu:
                print("[scraper] 🇪🇺 EU page detected — dismissing GDPR banner...")
                _dismiss_gdpr_banner(page)
                page.wait_for_timeout(1000)

            # Simulate human scrolling
            for _ in range(5):
                page.mouse.wheel(0, random.randint(300, 700))
                page.wait_for_timeout(random.randint(500, 1000))
            page.wait_for_timeout(2000)

            # ── Extract seller from popup ─────────────────────────────────────
            print("[scraper] 🏪 Extracting seller info...")
            seller = _extract_seller_from_popup(page, is_eu_page=is_eu)
            print(f"[scraper]    DOM seller: {seller}")

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

            for text in sorted(result['captured'], key=len, reverse=True):
                parsed = parse_pdp_response(text)
                if parsed and parsed.get('title'):
                    print(f"[scraper] ✅ Success on attempt {attempt}")
                    return result

            if len(result['captured']) >= len(best_result['captured']):
                best_result = result
            print(f"[scraper] ⚠️  Attempt {attempt}: no title found")

        except Exception as e:
            print(f"[scraper] ❌ Attempt {attempt} error: {e}")

    print(f"[scraper] ⚠️  All {MAX_RETRIES} attempts done")
    return best_result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PUBLIC FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    print(f"[scraper] Starting: {url}")
    data = _scrape_with_retry(url)

    extracted = {}

    # Parse API responses
    for text in sorted(data['captured'], key=len, reverse=True):
        parsed = parse_pdp_response(text)
        if parsed:
            for k, v in parsed.items():
                if v and k not in extracted:
                    extracted[k] = v
            if extracted.get('title'):
                break

    # Fallback: init_data from HTML
    if not extracted.get('title') and data['html']:
        print("[scraper] 🔍 Trying init_data fallback...")
        for k, v in _parse_init_data(data['html']).items():
            if v and k not in extracted:
                extracted[k] = v

    # Fallback: og:title
    if not extracted.get('title') and data['html']:
        m = re.search(
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']{5,200})["\']',
            data['html']
        )
        if m:
            extracted['title'] = m.group(1).strip()

    # Fallback: images from HTML
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

    # Fetch description
    desc_url = extracted.pop('_description_url', '')
    if desc_url and not extracted.get('description'):
        print(f"[scraper] 📄 Fetching description...")
        extracted['description'] = fetch_description(desc_url)
        if extracted['description']:
            print(f"[scraper] ✅ Description: {len(extracted['description'])} chars")

    # Merge DOM seller (fills gaps)
    for k, v in data.get('seller', {}).items():
        if v and not extracted.get(k):
            extracted[k] = v
            print(f"[scraper]    ✅ Seller from DOM: {k} = {v}")

    # Summary
    print(f"[scraper] ═══════════════════════════════")
    print(f"[scraper] Title:       {str(extracted.get('title',''))[:70]}")
    print(f"[scraper] Description: {len(extracted.get('description',''))} chars")
    print(f"[scraper] Seller:      {[k for k in ['store_name','store_id','seller_rating','seller_country'] if extracted.get(k)]}")
    print(f"[scraper] Specs:       {[k for k in ['brand','color','material','warranty','certifications'] if extracted.get(k)]}")
    print(f"[scraper] Images:      {sum(1 for i in range(1,21) if extracted.get(f'image_{i}'))}")
    print(f"[scraper] ═══════════════════════════════")

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
