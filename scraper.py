"""
scraper.py
──────────
Camoufox (Firefox) — intercepts mtop.aliexpress.pdp.pc.query API response
No Tor — uses direct connection (GCP VM IP is fine for this approach)
"""

import re
import json
import time
import urllib.request
from camoufox.sync_api import Camoufox


# ─────────────────────────────────────────────────────────────────────────────
# SPEC MAPPING
# ─────────────────────────────────────────────────────────────────────────────

SPEC_MAPPING = {
    'brand':             ['brand name', 'brand', 'marque'],
    'color':             ['main color', 'color', 'colour', 'couleur'],
    'dimensions':        ['dimensions (l x w x h', 'dimensions (cm)', 'dimensions',
                          'size', 'taille', 'product size', 'item size',
                          'package size', 'product dimensions'],
    'weight':            ['net weight', 'weight (kg)', 'weight', 'poids',
                          'gross weight', 'item weight', 'package weight'],
    'material':          ['material composition', 'material', 'matière', 'materials',
                          'body material', 'upper material', 'insole material',
                          'outsole material'],
    'certifications':    ['certification', 'certifications', 'normes', 'standards',
                          'energy efficiency rating', 'energy consumption grade',
                          'energy rating', 'energy class'],
    'country_of_origin': ['place of origin', 'country of origin', 'origin',
                          'country', 'pays', 'made in', 'manufactured in'],
    'warranty':          ['warranty', 'garantie', 'guarantee', 'warranty period'],
    'product_type':      ['refrigeration type', 'cooling method', 'defrost type',
                          'product type', 'type de produit', 'item type', 'type',
                          'application', 'design', 'operation system', 'use',
                          'shoes type'],
    'age_from':          ['age from', 'recommended age from', 'age (from)', 'minimum age'],
    'age_to':            ['age to', 'recommended age to', 'age (to)', 'maximum age'],
    'gender':            ['gender', 'suitable for', 'sexe', 'department name'],
    'capacity':          ['capacity', 'fridge capacity', 'net capacity', 'total capacity'],
    'freezer_capacity':  ['freezer capacity'],
    'voltage':           ['voltage', 'rated voltage', 'operating voltage'],
    'model_number':      ['model number', 'model no', 'model', 'item model'],
    'power_source':      ['power source', 'power supply'],
    'installation':      ['installation', 'mounting type'],
    'style':             ['style', 'closure type', 'feature'],
    'battery':           ['battery capacity', 'battery capacity(mah)', 'battery capacity range'],
    'display':           ['display size', 'screen size', 'display resolution', 'screen material'],
    'camera':            ['rear camera pixel', 'front camera pixel'],
    'connectivity':      ['cellular', 'wifi', 'nfc', 'bluetooth'],
    'os':                ['operation system', 'android version'],
    'height':            ['height'],
    'width':             ['width'],
    'season':            ['season'],
    'fit':               ['fit'],
}


def map_props_to_fields(props: list) -> tuple:
    raw = {}
    for item in props:
        name  = str(item.get('attrName',  '') or '').strip()
        value = str(item.get('attrValue', '') or '').strip()
        if name and value and value.lower() != 'none':
            raw[name.lower()] = value

    mapped = {}
    for field, keywords in SPEC_MAPPING.items():
        for raw_key, raw_val in raw.items():
            if any(kw in raw_key for kw in keywords):
                if field not in mapped:
                    mapped[field] = raw_val
                    break

    if not mapped.get('dimensions'):
        h = mapped.get('height', raw.get('height', ''))
        w = mapped.get('width',  raw.get('width',  ''))
        if h and w:
            mapped['dimensions'] = f"{h} x {w}"

    cap = mapped.pop('capacity', '')
    if cap:
        mapped['dimensions'] = (mapped.get('dimensions', '') + f" | Capacity: {cap}").strip(' |')

    fcap = mapped.pop('freezer_capacity', '')
    if fcap and mapped.get('dimensions'):
        mapped['dimensions'] += f" | Freezer: {fcap}"

    return mapped, raw


# ─────────────────────────────────────────────────────────────────────────────
# SELLER INFO EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────

def extract_seller_info(result: dict) -> dict:
    seller = {
        'store_name': '', 'store_id': '', 'store_url': '',
        'seller_id': '', 'seller_positive_rate': '', 'seller_rating': '',
        'seller_communication': '', 'seller_shipping_speed': '',
        'seller_country': '', 'store_open_date': '', 'seller_level': '',
        'seller_total_reviews': '', 'seller_positive_num': '', 'is_top_rated': '',
    }
    try:
        shop = result.get('SHOP_CARD_PC', {}) or {}
        if not shop:
            print("[scraper]    ⚠️  SHOP_CARD_PC not found")
            return seller

        seller['store_name']           = str(shop.get('storeName', '') or '')
        seller['seller_level']         = str(shop.get('sellerLevel', '') or '')
        seller['seller_positive_rate'] = str(shop.get('sellerPositiveRate', '') or '')
        seller['seller_total_reviews'] = str(shop.get('sellerTotalNum', '') or '')
        seller['seller_positive_num']  = str(shop.get('sellerPositiveNum', '') or '')

        for item in (shop.get('benefitInfoList', []) or []):
            title = str(item.get('title', '') or '').lower().strip()
            value = str(item.get('value', '') or '').strip()
            if 'store rating' in title:
                seller['seller_rating'] = value
            elif 'communication' in title:
                seller['seller_communication'] = value
            elif 'positive' in title:
                if not seller['seller_positive_rate']:
                    seller['seller_positive_rate'] = value

        si = shop.get('sellerInfo', {}) or {}
        if si:
            seller['seller_id']       = str(si.get('adminSeq', '') or '')
            seller['store_id']        = str(si.get('storeNum', '') or '')
            seller['seller_country']  = str(si.get('countryCompleteName', '') or '')
            seller['store_open_date'] = str(si.get('formatOpenTime', '') or '')
            seller['is_top_rated']    = str(si.get('topRatedSeller', '') or '')
            raw_url = str(si.get('storeURL', '') or '')
            if raw_url:
                seller['store_url'] = ('https:' + raw_url
                                       if raw_url.startswith('//') else raw_url)

        if not seller['store_name']:
            try:
                seller['store_name'] = str(
                    result['GLOBAL_DATA']['globalData']['storeName'] or '')
            except (KeyError, TypeError):
                pass
        if not seller['store_id']:
            try:
                seller['store_id'] = str(
                    result['GLOBAL_DATA']['globalData']['storeId'] or '')
            except (KeyError, TypeError):
                pass

        if seller['store_name']:
            print(f"[scraper]    ✅ Seller: {seller['store_name']} | "
                  f"Rating: {seller.get('seller_rating')} | "
                  f"Country: {seller.get('seller_country')}")
        else:
            print("[scraper]    ⚠️  Seller info empty")

    except Exception as e:
        print(f"[scraper]    ⚠️  Seller error: {e}")

    return seller


# ─────────────────────────────────────────────────────────────────────────────
# PRICE EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────

def extract_price(result: dict) -> str:
    try:
        sku_map = (result.get('PRICE', {}) or {}).get('skuIdStrPriceInfoMap', {})
        if sku_map:
            first = next(iter(sku_map.values()), {})
            p = (first.get('actSkuCalPrice') or
                 first.get('actSkuMultiCurrencyCalPrice') or
                 first.get('skuCalPrice', ''))
            if p:
                return f"${p}"
    except Exception:
        pass
    try:
        pm = result.get('priceModule', {}) or result.get('PRICE_MODULE', {}) or {}
        p  = (pm.get('formatedActivityPrice') or
              pm.get('formatedPrice') or
              (pm.get('minActivityAmount') or {}).get('formatedAmount', ''))
        if p:
            return str(p)
    except Exception:
        pass
    return ''


# ─────────────────────────────────────────────────────────────────────────────
# DESCRIPTION FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_description_url(result: dict) -> str:
    try:
        desc_module = result.get('DESC', {}) or {}
        url = desc_module.get('nativeDescUrl') or desc_module.get('pcDescUrl') or ''
        if not url:
            return ''
        print(f"[scraper]    📥 Fetching desc: {url[:80]}")
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        if 'desc.json' in url:
            with urllib.request.urlopen(req, timeout=10) as r:
                return _parse_desc_json(json.loads(r.read().decode('utf-8', errors='replace')))
        elif 'desc.htm' in url:
            with urllib.request.urlopen(req, timeout=10) as r:
                raw   = r.read().decode('utf-8', errors='replace')
                clean = ' '.join(re.sub(r'<[^>]+>', ' ', raw).split()).strip()
                return clean[:2000] if len(clean) > 50 else ''
    except Exception as e:
        print(f"[scraper]    ⚠️  Desc URL error: {e}")
    return ''


def _parse_desc_json(data) -> str:
    if isinstance(data, str):
        c = ' '.join(re.sub(r'<[^>]+>', ' ', data).split()).strip()
        return c if len(c) > 10 else ''
    if isinstance(data, dict):
        for k in ['text', 'content', 'value', 'description', 'html']:
            v = data.get(k)
            if isinstance(v, str) and len(v) > 10:
                c = ' '.join(re.sub(r'<[^>]+>', ' ', v).split()).strip()
                if len(c) > 10:
                    return c
        parts = [t for t in (_parse_desc_json(v) for v in data.values()) if t]
        return ' '.join(parts)[:2000] if parts else ''
    if isinstance(data, list):
        parts = [t for t in (_parse_desc_json(i) for i in data) if t]
        return ' '.join(parts)[:2000] if parts else ''
    return ''


# ─────────────────────────────────────────────────────────────────────────────
# PARSE API RESPONSE
# ─────────────────────────────────────────────────────────────────────────────

def parse_pdp_response(text: str) -> dict | None:
    try:
        text = text.strip()

        # Strip JSONP wrapper if present e.g. mtopjsonp1({...});
        m = re.match(r'^[a-zA-Z_$][a-zA-Z0-9_$]*\s*\((.*)\)\s*;?\s*$', text, re.DOTALL)
        if m:
            text = m.group(1)

        data   = json.loads(text)
        result = (data.get('data', {}) or {}).get('result', {}) or {}

        if not result:
            return None

        extracted = {}

        # Title
        try:
            extracted['title'] = result['GLOBAL_DATA']['globalData']['subject']
        except (KeyError, TypeError):
            pass
        if not extracted.get('title'):
            try:
                extracted['title'] = result['titleModule']['subject']
            except (KeyError, TypeError):
                pass

        # Specs
        props = []
        try:
            props = result['PRODUCT_PROP_PC']['showedProps'] or []
        except (KeyError, TypeError):
            pass
        if not props:
            try:
                props = result['PRODUCT_PROP_PC']['outerProps'] or []
            except (KeyError, TypeError):
                pass
        if props:
            print(f"[scraper]    ✅ Specs: {len(props)} items")
            mapped, raw = map_props_to_fields(props)
            extracted.update(mapped)
            for k, v in raw.items():
                print(f"[scraper]       {k}: {v[:60]}")
        else:
            print("[scraper]    ⚠️  No specs found")

        # Seller
        extracted.update(extract_seller_info(result))

        # Images
        images = []
        for img_key in ['HEADER_IMAGE_PC', 'imageModule']:
            try:
                images = result[img_key]['imagePathList'] or []
                if images:
                    print(f"[scraper]    ✅ Images from {img_key}: {len(images)}")
                    break
            except (KeyError, TypeError):
                pass

        if not images:
            def find_images(obj, depth=0):
                if depth > 6 or not isinstance(obj, dict):
                    return []
                if 'imagePathList' in obj:
                    v = obj['imagePathList']
                    if isinstance(v, list) and v:
                        return v
                for v in obj.values():
                    r = find_images(v, depth + 1)
                    if r:
                        return r
                return []
            images = find_images(result)

        for idx, img in enumerate(images[:20], 1):
            if img:
                img = 'https:' + img if img.startswith('//') else img
                extracted[f'image_{idx}'] = re.sub(r'_\d+x\d+', '', img)

        # Price
        price = extract_price(result)
        if price:
            extracted['price'] = price

        # Description
        desc = fetch_description_url(result)
        if desc:
            extracted['description'] = desc

        return extracted if extracted.get('title') else None

    except Exception as e:
        print(f"[scraper]    ⚠️  Parse error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# HTML FALLBACKS
# ─────────────────────────────────────────────────────────────────────────────

def get_images_from_html(html: str) -> dict:
    images = {}
    try:
        m = re.search(r'"imagePathList"\s*:\s*(\[[^\]]+\])', html)
        if m:
            urls = json.loads(m.group(1))
            for idx, url in enumerate(urls[:20], 1):
                if url:
                    images[f'image_{idx}'] = url
            if images:
                print(f"[scraper]    ✅ {len(images)} images from HTML")
    except Exception:
        pass
    return images


def get_title_from_html(html: str) -> str:
    m = re.search(r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)["\']', html)
    if m:
        return m.group(1).strip()
    return ''


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    print(f'\n[scraper] 🚀 Starting: {url}')

    captured_pdp = []
    html         = ''

    try:
        with Camoufox(
            headless=True,
            geoip=True,
            os='windows',
        ) as browser:

            context = browser.new_context(
                viewport={'width': 1366, 'height': 900},
                locale='en-US',
                timezone_id='America/New_York',
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9',
                }
            )

            page = context.new_page()

            # Intercept AliExpress product API responses
            def handle_response(response):
                try:
                    url_r = response.url
                    if ('mtop.aliexpress.pdp.pc.query'    in url_r or
                            'mtop.aliexpress.itemdetail'  in url_r):

                        if '_____tmd_____' in url_r or 'punish' in url_r:
                            print("[scraper]    ⛔ Skipped punish URL")
                            return

                        body = response.body()
                        if len(body) < 500:
                            return

                        text = body.decode('utf-8', errors='replace')

                        if 'FAIL_SYS_TOKEN_EMPTY' in text[:200]:
                            print("[scraper]    ⏭  Token empty — skip")
                            return

                        captured_pdp.append(text)
                        print(f"[scraper]    📡 API captured: {len(text)} bytes")

                except Exception as e:
                    print(f"[scraper]    ⚠️  Response handler error: {e}")

            page.on('response', handle_response)

            print(f'[scraper]    🌐 Navigating to: {url}')
            try:
                page.goto(url, timeout=90000, wait_until='domcontentloaded')
            except Exception as e:
                print(f"[scraper]    ⚠️  Navigation warning: {e}")

            # Wait up to 30s for a good API response
            print("[scraper]    ⏳ Waiting for product API response...")
            for i in range(30):
                page.wait_for_timeout(1000)

                good = [t for t in captured_pdp
                        if 'PRODUCT_PROP_PC' in t or 'SHOP_CARD_PC' in t
                        or len(t) > 50000]
                if good:
                    print(f"[scraper]    ✅ Got full API response at {i+1}s")
                    break

                # Simulate user interaction to trigger lazy API calls
                if i == 5:
                    page.mouse.wheel(0, 300)
                if i == 10:
                    page.mouse.wheel(0, 600)
                    page.mouse.move(400, 400)
                if i == 15:
                    page.mouse.wheel(0, 1000)
                if i == 20:
                    page.mouse.wheel(0, 1500)

                if (i + 1) % 5 == 0:
                    print(f"[scraper]    ⏳ {i+1}s | captured: {len(captured_pdp)}")

            print(f"[scraper]    📦 Total API responses: {len(captured_pdp)}")

            # Get HTML for fallbacks
            try:
                html = page.content()
            except Exception:
                pass

            page.close()
            context.close()

    except Exception as e:
        print(f'[scraper] ❌ Browser error: {e}')
        import traceback
        traceback.print_exc()
        return None

    # Parse captured API responses — largest first
    extracted = {}
    captured_pdp.sort(key=len, reverse=True)

    for resp_text in captured_pdp:
        parsed = parse_pdp_response(resp_text)
        if parsed:
            print(f'[scraper]    ✅ Parsed {len(parsed)} fields from API response')
            for k, v in parsed.items():
                if k not in extracted or not extracted[k]:
                    extracted[k] = v

    # HTML fallbacks
    if not extracted.get('title'):
        extracted['title'] = get_title_from_html(html)
        if extracted.get('title'):
            print(f"[scraper]    ✅ Title from HTML: {extracted['title'][:60]}")

    if not extracted.get('image_1'):
        extracted.update(get_images_from_html(html))

    # Validate
    if not extracted.get('title'):
        print('[scraper] ❌ No title — aborting')
        return None

    # Summary
    core          = ['brand', 'color', 'dimensions', 'weight', 'material',
                     'certifications', 'country_of_origin', 'warranty', 'product_type']
    seller_fields = ['store_name', 'store_id', 'seller_positive_rate',
                     'seller_rating', 'seller_country']

    print(f'\n[scraper] ✅ Extraction complete')
    print(f'[scraper]    Title  : {extracted.get("title", "")[:70]}')
    print(f'[scraper]    Price  : {extracted.get("price", "")}')
    print(f'[scraper]    Desc   : {len(extracted.get("description", ""))} chars')
    print(f'[scraper]    Specs  : {[k for k in core if extracted.get(k)]}')
    print(f'[scraper]    Seller : {[k for k in seller_fields if extracted.get(k)]}')
    print(f'[scraper]    Images : {sum(1 for i in range(1, 21) if extracted.get(f"image_{i}"))}')

    # Apply defaults
    defaults = {
        'description': '', 'brand': '', 'color': '', 'dimensions': '',
        'weight': '', 'material': '', 'certifications': '',
        'country_of_origin': '', 'warranty': '', 'product_type': '',
        'shipping': '', 'price': '', 'rating': '', 'reviews': '',
        'bullet_points': [], 'age_from': '', 'age_to': '',
        'gender': '', 'safety_warning': '',
        'capacity': '', 'freezer_capacity': '', 'voltage': '',
        'model_number': '', 'power_source': '', 'installation': '',
        'battery': '', 'display': '', 'camera': '',
        'connectivity': '', 'memory': '', 'os': '',
        'style': '', 'season': '', 'fit': '',
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

    return extracted


if __name__ == '__main__':
    test_url = 'https://www.aliexpress.com/item/1005009130457901.html'
    result   = get_product_info(test_url)
    if result:
        print('\n' + '=' * 60)
        for k, v in result.items():
            if v and not k.startswith('image_'):
                print(f'  {k:25s}: {str(v)[:100]}')
        print(f'  {"images":25s}: {sum(1 for i in range(1, 21) if result.get(f"image_{i}"))}')
    else:
        print('FAILED')
