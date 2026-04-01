import re
import json
import urllib.request
import concurrent.futures
from camoufox.sync_api import Camoufox

# ─────────────────────────────────────────────────────────────────────────────
# UPDATED SPEC MAPPING (2026)
# ─────────────────────────────────────────────────────────────────────────────
SPEC_MAPPING = {
    'brand': ['brand', 'brand name', 'marque'],
    'color': ['color', 'colour', 'main color'],
    'dimensions': ['dimensions', 'size', 'product size', 'package size'],
    'weight': ['weight', 'net weight', 'gross weight'],
    'material': ['material', 'materials', 'composition'],
    'country_of_origin': ['origin', 'country of origin', 'made in'],
    'warranty': ['warranty', 'garantie'],
    # Add more as needed
}

def map_props_to_fields(props):
    raw = {}
    for item in props:
        name = str(item.get('attrName') or item.get('name') or '').strip().lower()
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


def parse_pdp_response(text: str):
    try:
        # Remove JSONP wrapper if present
        m = re.match(r'^[a-zA-Z0-9_$]+\((.*)\);?$', text, re.DOTALL)
        if m:
            text = m.group(1)

        data = json.loads(text)
        result = data.get('data', {}).get('result', {}) or data.get('data', {}) or {}

        if not result:
            return None

        extracted = {}

        # === TITLE ===
        try:
            extracted['title'] = result['titleModule']['subject']
        except:
            try:
                extracted['title'] = result['GLOBAL_DATA']['globalData']['subject']
            except:
                pass

        # === PRICE ===
        try:
            price_mod = result.get('priceModule') or {}
            extracted['price'] = price_mod.get('formatedActivityPrice') or price_mod.get('formatedPrice')
        except:
            pass

        # === SELLER INFO (Updated 2026) ===
        seller = {}
        try:
            store = result.get('storeModule') or result.get('SHOP_CARD_PC', {})
            seller['store_name'] = store.get('storeName') or store.get('sellerName')
            seller['store_id'] = store.get('storeNum') or store.get('sellerId')
            seller['seller_country'] = store.get('country')
            seller['seller_rating'] = store.get('positiveRate')
        except:
            pass

        extracted.update({k: v for k, v in seller.items() if v})

        # === SPECIFICATIONS (New paths) ===
        props = []
        try:
            # Common 2026 paths
            props = (result.get('skuModule', {}).get('productSKUPropertyList', []) or
                    result.get('productProp', {}).get('props', []) or
                    result.get('PRODUCT_PROP_PC', {}).get('showedProps', []))
        except:
            pass

        if props:
            mapped, _ = map_props_to_fields(props)
            extracted.update(mapped)
            print(f"[scraper] ✅ Found {len(props)} specification items")

        # === IMAGES ===
        images = []
        try:
            images = result.get('imageModule', {}).get('imagePathList', [])
            if not images:
                images = result.get('titleModule', {}).get('images', [])
        except:
            pass

        for idx, img in enumerate(images[:20], 1):
            if img:
                img = 'https:' + img if str(img).startswith('//') else img
                extracted[f'image_{idx}'] = re.sub(r'_\d+x\d+', '', img)

        # === DESCRIPTION ===
        try:
            desc_url = result.get('descriptionModule', {}).get('descriptionUrl') or ""
            if desc_url:
                extracted['description'] = fetch_description(desc_url)
        except:
            pass

        return extracted if extracted.get('title') else None

    except Exception as e:
        print(f"[scraper] Parse error: {e}")
        return None


def fetch_description(url):
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read().decode('utf-8', errors='replace')
            clean = re.sub(r'<[^>]+>', ' ', raw)
            return ' '.join(clean.split())[:3000]
    except:
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER INTERCEPTOR
# ─────────────────────────────────────────────────────────────────────────────
def _scrape_in_thread(url: str):
    captured = []
    html = ""

    try:
        with Camoufox(headless=True, geoip=True, os='windows') as browser:
            context = browser.new_context(
                viewport={'width': 1366, 'height': 900},
                locale='en-US',
                extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'}
            )
            page = context.new_page()

            def handle_response(response):
                try:
                    if 'mtop.aliexpress.pdp.pc.query' in response.url or 'pdp' in response.url:
                        body = response.body()
                        if len(body) < 15000:
                            return
                        text = body.decode('utf-8', errors='replace')
                        if any(x in text for x in ['titleModule', 'storeModule', 'skuModule']):
                            captured.append(text)
                            print(f"[scraper] 📡 Captured rich PDP response ({len(text)} bytes)")
                except:
                    pass

            page.on('response', handle_response)
            page.goto(url, timeout=90000, wait_until='domcontentloaded')

            # Scroll & interact to trigger full load
            for _ in range(8):
                page.wait_for_timeout(800)
                page.mouse.wheel(0, 600)

            page.wait_for_timeout(3000)
            html = page.content()

    except Exception as e:
        print(f"[scraper] Browser error: {e}")

    return {'captured': captured, 'html': html}


def get_product_info(url: str):
    print(f"[scraper] Starting: {url}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_scrape_in_thread, url)
        data = future.result(timeout=180)

    extracted = {}
    for text in sorted(data['captured'], key=len, reverse=True):
        parsed = parse_pdp_response(text)
        if parsed:
            for k, v in parsed.items():
                if v and (k not in extracted or not extracted[k]):
                    extracted[k] = v
            break

    # Fallbacks
    if not extracted.get('title') and data['html']:
        m = re.search(r'<title>([^<]+)', data['html'])
        if m:
            extracted['title'] = m.group(1).strip()

    print(f"[scraper] Final extracted fields: {len(extracted)}")
    return extracted


if __name__ == '__main__':
    test_url = "https://www.aliexpress.com/item/1005010089125608.html"
    result = get_product_info(test_url)
    
    if result:
        print("\n" + "="*80)
        for k, v in sorted(result.items()):
            if not k.startswith('image_'):
                print(f"{k:20}: {str(v)[:120]}")
