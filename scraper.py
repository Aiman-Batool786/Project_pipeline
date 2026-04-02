"""
scraper.py
──────────
Camoufox-based AliExpress product scraper.

Changes vs previous version:
  - FIX 1: Corrupted markdown text removed (caused SyntaxError on import)
  - FIX 2: SPEC_MAPPING now includes 'certifications' and 'product_type'
  - FIX 3: bullet_points extraction added (3 API path attempts)
  - FIX 4: All 14 seller fields extracted from SHOP_CARD_PC / storeModule
  - FIX 5: Region rotation, user-agent rotation, retry mechanism added
"""

import re
import json
import random
import time
import urllib.request
import concurrent.futures
from camoufox.sync_api import Camoufox


# ─────────────────────────────────────────────────────────────────────────────
# FIX 2 — COMPLETE SPEC MAPPING
# Added: 'certifications', 'product_type'  (were missing, always blank before)
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
                          'compliance', 'standard', 'normes', 'ce', 'rohs'],   # NEW
    'product_type':      ['product type', 'type', 'item type',
                          'type de produit', 'style', 'category'],             # NEW
}


# ─────────────────────────────────────────────────────────────────────────────
# FIX 5 — REGION ROTATION + USER-AGENT ROTATION
# ─────────────────────────────────────────────────────────────────────────────
REGIONS = ["US", "GB", "DE", "FR", "AE", "AU", "CA"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

MAX_RETRIES = 3


def _get_rotated_url(url: str) -> str:
    """Append a random shipFromCountry param to reduce region-based blocks."""
    region    = random.choice(REGIONS)
    separator = '&' if '?' in url else '?'
    print(f"[scraper] 🌍 Region rotated → {region}")
    return f"{url}{separator}shipFromCountry={region}"


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
# FIX 4 — FULL SELLER EXTRACTION  (all 14 SELLER_FIELDS from main.py)
# Old code only grabbed 4 fields; this grabs all 14.
# ─────────────────────────────────────────────────────────────────────────────

def _parse_seller_block(store: dict) -> dict:
    """
    Extract every seller field from storeModule or SHOP_CARD_PC.
    Covers all 14 keys expected by main.py SELLER_FIELDS.
    """
    sid = str(store.get('storeNum') or store.get('sellerId') or
              store.get('storeId') or '')

    seller = {
        'store_name':            (store.get('storeName')
                                  or store.get('sellerName')
                                  or store.get('name') or ''),
        'store_id':              sid,
        'seller_id':             str(store.get('sellerId')
                                  or store.get('userId') or ''),
        'store_url':             (store.get('storeUrl')
                                  or (f"https://www.aliexpress.com/store/{sid}"
                                      if sid else '')),
        'seller_country':        (store.get('country')
                                  or store.get('countryCompleteName') or ''),
        'seller_rating':         str(store.get('positiveRate')
                                  or store.get('itemAs') or ''),
        'seller_positive_rate':  str(store.get('positiveRate') or ''),
        'seller_communication':  str(store.get('communicationRating')
                                  or store.get('serviceAs') or ''),
        'seller_shipping_speed': str(store.get('shippingRating')
                                  or store.get('shippingAs') or ''),
        'store_open_date':       str(store.get('openTime')
                                  or store.get('openDate') or ''),
        'seller_level':          str(store.get('sellerLevel')
                                  or store.get('shopLevel') or ''),
        'seller_total_reviews':  str(store.get('totalEvaluationNum')
                                  or store.get('reviewNum') or ''),
        'seller_positive_num':   str(store.get('positiveNum') or ''),
        'is_top_rated':          str(store.get('isTopRatedSeller')
                                  or store.get('topRatedSeller') or ''),
    }
    # Return only non-empty values
    return {k: v for k, v in seller.items() if v and str(v).strip()}


# ─────────────────────────────────────────────────────────────────────────────
# FIX 3 — BULLET POINTS EXTRACTION  (was completely missing before)
# Tries 3 different API paths used by AliExpress.
# ─────────────────────────────────────────────────────────────────────────────

def _extract_bullet_points(result: dict) -> list:
    bullets = []

    # Path 1: highlightModule
    try:
        highlights = (result.get('highlights') or
                      result.get('highlightModule', {}).get('highlightList') or
                      result.get('tradeModule', {}).get('highlights') or [])
        if isinstance(highlights, list) and highlights:
            bullets = [str(h.get('title') or h.get('text') or h)
                       for h in highlights if h]
    except Exception:
        pass

    # Path 2: descriptionModule features
    if not bullets:
        try:
            features = (result.get('descriptionModule', {}).get('features') or
                        result.get('PRODUCT_PROP_PC', {}).get('features') or [])
            if isinstance(features, list) and features:
                bullets = [str(f) for f in features if f]
        except Exception:
            pass

    # Path 3: sellingPoints / keyPoints
    if not bullets:
        try:
            points = (result.get('sellingPoints') or
                      result.get('keyPoints') or
                      result.get('productFeatures') or [])
            if isinstance(points, list) and points:
                bullets = [str(p.get('text') or p.get('point') or p)
                           for p in points if p]
        except Exception:
            pass

    cleaned = [b.strip() for b in bullets if b.strip()]
    return cleaned[:10]


# ─────────────────────────────────────────────────────────────────────────────
# API RESPONSE PARSER
# ─────────────────────────────────────────────────────────────────────────────

def parse_pdp_response(text: str):
    try:
        # Remove JSONP wrapper if present
        m = re.match(r'^[a-zA-Z0-9_$]+\((.*)\);?$', text, re.DOTALL)
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
                extracted['title'] = result['GLOBAL_DATA']['globalData']['subject']
            except Exception:
                pass

        # ── PRICE ──────────────────────────────────────────────────────────
        try:
            pm = result.get('priceModule') or {}
            extracted['price'] = (pm.get('formatedActivityPrice')
                                  or pm.get('formatedPrice'))
        except Exception:
            pass

        # ── SELLER — FIX 4 ─────────────────────────────────────────────────
        try:
            store = (result.get('storeModule')
                     or result.get('SHOP_CARD_PC')
                     or result.get('sellerModule') or {})
            if store:
                seller_data = _parse_seller_block(store)
                extracted.update(seller_data)
                print(f"[scraper] 🏪 Seller from API: {list(seller_data.keys())}")
        except Exception as e:
            print(f"[scraper] ⚠️  Seller parse error: {e}")

        # ── SPECIFICATIONS — FIX 2 ─────────────────────────────────────────
        props = []
        try:
            props = (
                result.get('skuModule', {}).get('productSKUPropertyList', []) or
                result.get('productProp', {}).get('props', []) or
                result.get('PRODUCT_PROP_PC', {}).get('showedProps', []) or
                result.get('specsModule', {}).get('props', [])
            )
        except Exception:
            pass

        if props:
            mapped, _ = map_props_to_fields(props)
            extracted.update(mapped)
            print(f"[scraper] ✅ {len(props)} spec items → "
                  f"mapped fields: {list(mapped.keys())}")

        # ── BULLET POINTS — FIX 3 ──────────────────────────────────────────
        bullets = _extract_bullet_points(result)
        if bullets:
            extracted['bullet_points'] = bullets
            print(f"[scraper] ✅ Bullet points: {len(bullets)} items")

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

        # ── DESCRIPTION ────────────────────────────────────────────────────
        try:
            desc_url = result.get('descriptionModule', {}).get('descriptionUrl', '')
            if desc_url:
                extracted['description'] = fetch_description(desc_url)
        except Exception:
            pass

        return extracted if extracted.get('title') else None

    except Exception as e:
        print(f"[scraper] Parse error: {e}")
        return None


def fetch_description(url: str) -> str:
    try:
        ua  = random.choice(USER_AGENTS)
        req = urllib.request.Request(url, headers={'User-Agent': ua})
        with urllib.request.urlopen(req, timeout=10) as r:
            raw   = r.read().decode('utf-8', errors='replace')
            clean = re.sub(r'<[^>]+>', ' ', raw)
            return ' '.join(clean.split())[:3000]
    except Exception:
        return ''


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER INTERCEPTOR  — FIX 5: user-agent rotation added
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_in_thread(url: str) -> dict:
    captured = []
    html     = ''
    seller   = {}

    # FIX 5 — rotate user-agent per browser session
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
                    if ('mtop.aliexpress.pdp.pc.query' in response.url or
                            'mtop.aliexpress.itemdetail' in response.url):
                        body = response.body()
                        if len(body) < 15000:
                            return
                        text = body.decode('utf-8', errors='replace')
                        if any(x in text for x in ['titleModule', 'storeModule',
                                                    'PRODUCT_PROP_PC', 'SHOP_CARD_PC']):
                            captured.append(text)
                            print(f"[scraper] 📡 Captured PDP ({len(text)} bytes)")
                except Exception:
                    pass

            page.on('response', handle_response)
            page.goto(url, timeout=90000, wait_until='domcontentloaded')

            # Human-like random delays (FIX 5)
            page.wait_for_timeout(random.randint(4000, 7000))
            for _ in range(4):
                page.mouse.wheel(0, random.randint(400, 800))
                page.wait_for_timeout(random.randint(600, 1200))
            page.wait_for_timeout(2000)

            # ── Step 1: Store name from visible page ────────────────────────
            print("[scraper]    🏪 Trying seller info from page...")
            try:
                store_name_selectors = [
                    '[class*="store-header--storeName"]',
                    '[class*="shop-name"]',
                    'a[href*="/store/"] span',
                    '[class*="sellerName"]',
                    '[class*="seller-name"]',
                    '.store-name',
                ]
                for sel in store_name_selectors:
                    try:
                        el = page.locator(sel).first
                        if el.count() > 0:
                            text = el.inner_text().strip()
                            if text and len(text) > 1:
                                seller['store_name'] = text
                                print(f"[scraper]    ✅ Store name: {text}")
                                break
                    except Exception:
                        continue
            except Exception as e:
                print(f"[scraper]    ⚠️ Store name error: {e}")

            # ── Step 2: Store URL + ID from links ───────────────────────────
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
                            seller['store_url'] = (
                                f"https://www.aliexpress.com/store/{m.group(1)}"
                            )
                            print(f"[scraper]    ✅ Store ID: {m.group(1)}")
                            break
            except Exception as e:
                print(f"[scraper]    ⚠️ Store URL error: {e}")

            # ── Step 3: Click store info to open popup ───────────────────────
            print("[scraper]    🖱️  Clicking store info...")
            try:
                page.wait_for_selector(
                    'a[href*="/store/"], [class*="store"], [class*="seller"]',
                    timeout=10000
                )
                click_selectors = [
                    'span:text("Trader")', 'span:text("trader")',
                    '[class*="trader"]',   'a[href*="/store/"]',
                    '[class*="store-header"]', '[class*="sellerInfo"]',
                    '[class*="seller-info"]',
                ]
                for sel in click_selectors:
                    try:
                        el = page.locator(sel).first
                        if el.count() > 0:
                            el.click(timeout=3000)
                            page.wait_for_timeout(3000)
                            print(f"[scraper]    ✅ Clicked: {sel}")
                            break
                    except Exception:
                        continue
            except Exception as e:
                print(f"[scraper]    ⚠️ Click error: {e}")

            # ── Step 4: Extract from popup ───────────────────────────────────
            try:
                popup_selectors = [
                    '.store-detail--storeInfo--BMDFsTB',
                    '[class*="storeInfo"]',
                    '[class*="store-detail"]',
                ]
                popup_found = False
                for sel in popup_selectors:
                    if page.locator(sel).count() > 0:
                        popup_found = True
                        print(f"[scraper]    ✅ Popup: {sel}")
                        break

                if popup_found:
                    rows = page.locator(
                        '.store-detail--storeInfo--BMDFsTB table tr, '
                        '[class*="storeInfo"] table tr'
                    ).all()
                    for row in rows:
                        try:
                            cells = row.locator('td').all()
                            if len(cells) >= 2:
                                key   = cells[0].inner_text().strip().lower().rstrip(':')
                                value = cells[1].inner_text().strip()
                                print(f"[scraper]       Row: {key} = {value}")
                                if 'name' in key:
                                    seller['store_name'] = value
                                elif 'store no' in key or 'no.' in key:
                                    seller['store_id']  = value
                                    seller['store_url'] = (
                                        f"https://www.aliexpress.com/store/{value}"
                                    )
                                elif 'location' in key or 'country' in key:
                                    seller['seller_country'] = value.strip()
                                elif 'open' in key or 'since' in key:
                                    seller['store_open_date'] = value
                        except Exception:
                            continue

                    rating_rows = page.locator(
                        '.store-detail--storeRating--Z2j7q9u table tr, '
                        '[class*="storeRating"] table tr'
                    ).all()
                    for row in rating_rows:
                        try:
                            cells = row.locator('td').all()
                            if len(cells) >= 2:
                                key  = cells[0].inner_text().strip().lower()
                                b_el = cells[1].locator('b').first
                                value = (b_el.inner_text().strip()
                                         if b_el.count() > 0
                                         else cells[1].inner_text().strip())
                                print(f"[scraper]       Rating: {key} = {value}")
                                if 'item' in key or 'described' in key:
                                    seller['seller_rating'] = value
                                elif 'communication' in key:
                                    seller['seller_communication'] = value
                                elif 'shipping' in key:
                                    seller['seller_shipping_speed'] = value
                        except Exception:
                            continue
                else:
                    print("[scraper]    ⚠️ Popup not found — saving debug HTML")
                    with open('/tmp/seller_debug.html', 'w') as f:
                        f.write(page.content())
                    print("[scraper]    💾 Saved /tmp/seller_debug.html")

            except Exception as e:
                print(f"[scraper]    ⚠️ Popup extraction error: {e}")

            print(f"[scraper]    📊 DOM seller: {seller}")
            html = page.content()
            page.close()
            context.close()

    except Exception as e:
        print(f'[scraper] ❌ Browser error: {e}')
        import traceback
        traceback.print_exc()

    return {'captured': captured, 'html': html, 'seller': seller}


# ─────────────────────────────────────────────────────────────────────────────
# FIX 5 — RETRY MECHANISM  (wraps browser call, rotates region each attempt)
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_with_retry(url: str) -> dict:
    """
    Try up to MAX_RETRIES times.
    Each attempt uses a different region + fresh random user-agent.
    Returns the best result obtained (or last attempt if all fail).
    """
    best_result = {'captured': [], 'html': '', 'seller': {}}

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n[scraper] 🔄 Attempt {attempt}/{MAX_RETRIES}")

        attempt_url = _get_rotated_url(url)

        # Back-off between retries
        if attempt > 1:
            delay = random.uniform(3, 7)
            print(f"[scraper] ⏳ Back-off {delay:.1f}s...")
            time.sleep(delay)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(_scrape_in_thread, attempt_url)
                result = future.result(timeout=180)

            # Check if we got useful data
            got_data = False
            for text in sorted(result['captured'], key=len, reverse=True):
                parsed = parse_pdp_response(text)
                if parsed and parsed.get('title'):
                    got_data = True
                    break

            if got_data:
                print(f"[scraper] ✅ Success on attempt {attempt}")
                return result

            # Keep the attempt with the most captured data as fallback
            if len(result['captured']) >= len(best_result['captured']):
                best_result = result

            print(f"[scraper] ⚠️  Attempt {attempt} incomplete — retrying...")

        except Exception as e:
            print(f"[scraper] ❌ Attempt {attempt} error: {e}")

    print(f"[scraper] ⚠️  All {MAX_RETRIES} attempts done — using best result")
    return best_result


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    print(f"[scraper] Starting: {url}")

    # FIX 5: use retry wrapper
    data = _scrape_with_retry(url)

    extracted = {}

    # Parse API responses (longest = most complete, try first)
    for text in sorted(data['captured'], key=len, reverse=True):
        parsed = parse_pdp_response(text)
        if parsed:
            for k, v in parsed.items():
                if v and (k not in extracted or not extracted[k]):
                    extracted[k] = v
            break

    # Merge DOM seller info (fills any gaps not covered by API)
    dom_seller = data.get('seller', {})
    for k, v in dom_seller.items():
        if v and (k not in extracted or not extracted[k]):
            extracted[k] = v
            print(f"[scraper]    ✅ Seller from DOM: {k} = {v}")

    # Title fallback from HTML <meta> tag
    if not extracted.get('title') and data['html']:
        m = re.search(
            r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)["\']',
            data['html']
        )
        if m:
            extracted['title'] = m.group(1).strip()

    # Image fallback from embedded HTML JSON
    if not extracted.get('image_1') and data['html']:
        m = re.search(r'"imagePathList"\s*:\s*(\[[^\]]+\])', data['html'])
        if m:
            try:
                urls = json.loads(m.group(1))
                for idx, img_url in enumerate(urls[:20], 1):
                    if img_url:
                        extracted[f'image_{idx}'] = img_url
            except Exception:
                pass

    # Summary log
    print(f"[scraper] Final extracted fields: {len(extracted)}")
    print(f"[scraper] Seller fields:  "
          f"{[k for k in ['store_name','store_id','seller_rating','seller_country','seller_positive_rate','seller_communication','seller_shipping_speed','is_top_rated'] if extracted.get(k)]}")
    print(f"[scraper] Spec fields:    "
          f"{[k for k in ['brand','color','dimensions','weight','material','certifications','country_of_origin','warranty','product_type'] if extracted.get(k)]}")
    print(f"[scraper] Bullet points:  {len(extracted.get('bullet_points', []))} items")

    # Apply defaults (ensures all expected keys exist)
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
                print(f"{k:25}: {str(v)[:120]}")
