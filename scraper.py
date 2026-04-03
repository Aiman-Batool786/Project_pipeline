"""
scraper.py — CORRECTED SELLER EXTRACTION
──────────────────────────────────────────

Changes vs previous version:
  - FIX 1: Corrupted markdown text removed (caused SyntaxError on import)
  - FIX 2: SPEC_MAPPING now includes 'certifications' and 'product_type'
  - FIX 3: bullet_points extraction added (3 API path attempts)
  - FIX 4: All 14 seller fields extracted from SHOP_CARD_PC / storeModule
  - FIX 5: Region rotation, user-agent rotation, retry mechanism added
  - FIX 6: IMPROVED DOM seller extraction with multi-language support (Polish, English, Chinese)
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

def parse_pdp_response(text: str) -> dict:
    """
    Parse AliExpress API JSON response.
    Extracts product title, description, specs, images, seller, bullet points.
    """
    if not text or not isinstance(text, str):
        return {}

    # Remove common prefixes
    text = text.lstrip('🛒 ')

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return {}

    result = data.get('result', {}) if isinstance(data, dict) else {}

    if not result:
        return {}

    # ──── TITLE / DESCRIPTION ────
    title = (result.get('title') or
             result.get('productTitle') or
             result.get('PRODUCT_PROPS_PC', {}).get('subject') or '')

    description = (result.get('description') or
                   result.get('productDescription') or
                   result.get('descriptionModule', {}).get('description') or '')

    # ──── SPECIFICATIONS ────
    specs_extracted = {}

    props = (result.get('props') or
             result.get('attributes') or
             result.get('skuModule', {}).get('props') or
             result.get('PRODUCT_PROPS_PC', {}).get('props') or [])

    if props:
        specs_extracted, _ = map_props_to_fields(props)

    # ──── IMAGES ────
    images = {}
    image_list = (result.get('imagePathList') or
                  result.get('images') or
                  result.get('imageModule', {}).get('imageList') or
                  result.get('PRODUCT_DETAILS', {}).get('imageList') or [])

    for idx, img_url in enumerate(image_list[:20], 1):
        if img_url:
            images[f'image_{idx}'] = img_url

    # ──── SELLER INFO (from API) ────
    seller_from_api = {}

    store = (result.get('storeModule', {}) or
             result.get('SHOP_CARD_PC', {}) or
             result.get('store') or {})

    if store:
        seller_from_api = _parse_seller_block(store)
        print(f"[scraper] 📊 API seller: {seller_from_api}")

    # ──── BULLET POINTS ────
    bullet_points = _extract_bullet_points(result)

    # ──── COMPILE RESULT ────
    extracted = {
        'title': title,
        'description': description,
        **specs_extracted,
        **images,
        **seller_from_api,
        'bullet_points': bullet_points,
    }

    return {k: v for k, v in extracted.items() if v}


# ─────────────────────────────────────────────────────────────────────────────
# RESPONSE INTERCEPTOR
# ─────────────────────────────────────────────────────────────────────────────

def _on_response(response, captured: list):
    """Intercept API responses and parse them."""
    if "api" in response.url and "getSearchProductPageAC" not in response.url:
        try:
            text = response.text()
            if text and len(text) > 100:
                captured.append(text)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# FIX 6 — IMPROVED DOM SELLER EXTRACTION (Multi-language support)
# Handles Polish, English, Chinese HTML with robust CSS parsing
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_in_thread(url: str) -> dict:
    """
    Enhanced seller extraction with multi-language HTML parsing.
    Properly handles Polish, English, Chinese AliExpress pages.
    """
    captured = []
    html = ''
    seller = {}

    try:
        with Camoufox() as browser:
            context = browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={'width': 1440, 'height': 900},
            )
            page = context.new_page()

            # Intercept API responses
            page.on("response", lambda res: _on_response(res, captured))

            print(f"[scraper] 🌐 Opening {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)

            # ── TRY TO FIND AND HOVER SELLER INFO TRIGGER ──
            print("[scraper] 🔍 Looking for seller info popup...")
            try:
                # Try multiple selector strategies
                trigger_selectors = [
                    '[class*="shopHead"] button, [class*="shopHead"] a',
                    '[class*="seller-info"] a, [class*="seller-info"] button',
                    '[data-spm-anchor-id="a2g0o.detail.0.i4"]',
                    '[class*="store-card"]',
                    '.seller-info',
                ]

                trigger_found = False
                for selector in trigger_selectors:
                    try:
                        elements = page.locators(selector).all()
                        if elements:
                            print(f"[scraper]    Found {len(elements)} with selector: {selector}")
                            # Hover on the first element
                            elements[0].hover(timeout=2000)
                            trigger_found = True
                            break
                    except Exception:
                        continue

                if trigger_found:
                    page.wait_for_timeout(1500)
                    
                    # ── WAIT FOR AND PARSE POPUP ──
                    popup_selectors = [
                        '.store-detail--storeDesc--zjMyBuV',
                        '[class*="storeDesc"]',
                        '[class*="store-detail"]',
                        '[class*="shopinfo"]',
                    ]

                    popup_found = False
                    for popup_sel in popup_selectors:
                        try:
                            popup = page.locator(popup_sel).first
                            if popup.is_visible(timeout=2000):
                                print(f"[scraper] ✅ Popup visible using: {popup_sel}")
                                popup_found = True
                                break
                        except Exception:
                            continue

                    if popup_found:
                        # ── PARSE SELLER INFO TABLE ──
                        print("[scraper] 📋 Parsing seller info table...")
                        info_row_selectors = [
                            '.store-detail--storeInfo--BMDFsTB table tr',
                            '[class*="storeInfo"] table tr',
                            '.store-detail--storeDesc--zjMyBuV table tr:first-of-type',
                        ]

                        for info_sel in info_row_selectors:
                            try:
                                info_rows = page.locators(info_sel).all()
                                if info_rows:
                                    print(f"[scraper]    Found {len(info_rows)} info rows")
                                    
                                    for row in info_rows:
                                        try:
                                            cells = row.locator('td').all()
                                            if len(cells) >= 2:
                                                key_text = cells[0].inner_text().strip().lower()
                                                val_text = cells[1].inner_text().strip()
                                                
                                                print(f"[scraper]       {key_text} = {val_text}")
                                                
                                                # Map keys (handles Polish, English, Chinese)
                                                if any(k in key_text for k in ['nazwa', 'name', '店铺名', '店名']):
                                                    seller['store_name'] = val_text
                                                elif any(k in key_text for k in ['sklep nr', 'store id', 'storenum', '店铺编号', 'seller id']):
                                                    seller['store_id'] = val_text
                                                elif any(k in key_text for k in ['lokalizacja', 'location', 'country', '国家', 'pays']):
                                                    seller['seller_country'] = val_text
                                                elif any(k in key_text for k in ['data otwarcia', 'open', 'date', '开店时间', 'depuis']):
                                                    seller['store_open_date'] = val_text
                                        except Exception as e:
                                            print(f"[scraper]      Row error: {e}")
                                            continue
                                    break
                            except Exception:
                                continue

                        # ── PARSE RATING TABLE ──
                        print("[scraper] ⭐ Parsing ratings...")
                        rating_row_selectors = [
                            '.store-detail--storeRating--Z2j7q9u table tr',
                            '[class*="storeRating"] table tr',
                        ]

                        for rating_sel in rating_row_selectors:
                            try:
                                rating_rows = page.locators(rating_sel).all()
                                if rating_rows:
                                    print(f"[scraper]    Found {len(rating_rows)} rating rows")
                                    
                                    for row in rating_rows:
                                        try:
                                            cells = row.locator('td').all()
                                            if len(cells) >= 2:
                                                key_text = cells[0].inner_text().strip().lower()
                                                
                                                # Try <b> tag first (contains the numeric rating)
                                                b_element = cells[1].locator('b').first
                                                if b_element.count() > 0:
                                                    rating_val = b_element.inner_text().strip()
                                                else:
                                                    rating_val = cells[1].inner_text().strip()
                                                    # Extract just the number if there's extra text
                                                    match = re.search(r'([\d.]+)', rating_val)
                                                    if match:
                                                        rating_val = match.group(1)
                                                
                                                print(f"[scraper]       {key_text} = {rating_val}")
                                                
                                                # Map rating keys (Polish, English, Chinese)
                                                if any(k in key_text for k in ['produkt', 'described', 'item', '产品', 'produit']):
                                                    seller['seller_rating'] = rating_val
                                                elif any(k in key_text for k in ['komunikacja', 'communication', '沟通', 'communication']):
                                                    seller['seller_communication'] = rating_val
                                                elif any(k in key_text for k in ['szybkość', 'shipping', 'dostawy', '物流', 'livraison']):
                                                    seller['seller_shipping_speed'] = rating_val
                                        except Exception as e:
                                            print(f"[scraper]      Rating error: {e}")
                                            continue
                                    break
                            except Exception:
                                continue
                    else:
                        print("[scraper] ⚠️ Popup not visible after hover")
                else:
                    print("[scraper] ⚠️ No trigger found")

            except Exception as e:
                print(f"[scraper] ⚠️ Popup extraction error: {e}")
                import traceback
                traceback.print_exc()

            print(f"[scraper] 📊 DOM seller extracted: {seller}")
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
