"""
scraper.py
─────────
Captures the mtop.aliexpress.pdp.pc.query API response which contains
ALL product data including specifications.

Confirmed from debug output:
  URL: https://acs.aliexpress.us/h5/mtop.aliexpress.pdp.pc.query/1.0/...
  Key path: data.result.PRODUCT_PROP_PC.showedProps
  Format:   [{"attrName": "Brand Name", "attrValue": "XMSJ"}, ...]

  Images: data.result.IMAGE_MODULE.imagePathList or mediaPathList
  Title:  data.result.GLOBAL_DATA.globalData.subject
  Price:  data.result.PRICE_MODULE (various sub-keys)
"""

from playwright.sync_api import sync_playwright
import re
import json


# ─────────────────────────────────────────────────────────────────────────────
# SPEC MAPPING — maps attrName (lowercase) → internal field
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
                          'body material'],
    'certifications':    ['certification', 'certifications', 'normes', 'standards',
                          'energy efficiency rating', 'energy consumption grade',
                          'energy rating', 'energy class'],
    'country_of_origin': ['place of origin', 'country of origin', 'origin',
                          'country', 'pays', 'made in', 'manufactured in'],
    'warranty':          ['warranty', 'garantie', 'guarantee', 'warranty period'],
    'product_type':      ['refrigeration type', 'cooling method', 'defrost type',
                          'product type', 'type de produit', 'item type', 'type',
                          'application', 'design', 'operation system', 'use'],
    'age_from':          ['age from', 'recommended age from', 'age (from)', 'minimum age'],
    'age_to':            ['age to',   'recommended age to',   'age (to)',   'maximum age'],
    'gender':            ['gender', 'suitable for', 'sexe'],
    # Extra fields
    'capacity':          ['capacity', 'fridge capacity', 'net capacity', 'total capacity'],
    'freezer_capacity':  ['freezer capacity'],
    'voltage':           ['voltage', 'rated voltage', 'operating voltage'],
    'model_number':      ['model number', 'model no', 'model', 'item model'],
    'power_source':      ['power source', 'power supply'],
    'installation':      ['installation', 'mounting type'],
    'style':             ['style'],
    'battery':           ['battery capacity', 'battery capacity(mah)', 'battery capacity range'],
    'display':           ['display size', 'screen size', 'display resolution', 'screen material'],
    'camera':            ['rear camera pixel', 'front camera pixel'],
    'connectivity':      ['cellular', 'wifi', 'nfc', 'bluetooth'],
    'os':                ['operation system', 'android version'],
    'height':            ['height'],
    'width':             ['width'],
}


def map_props_to_fields(props: list) -> tuple[dict, dict]:
    """
    Map list of {attrName, attrValue} to internal fields.
    Returns (mapped_fields, raw_dict).
    """
    raw = {}
    for item in props:
        name  = str(item.get('attrName',  '') or '').strip()
        value = str(item.get('attrValue', '') or '').strip()
        if name and value:
            raw[name.lower()] = value

    mapped = {}
    for field, keywords in SPEC_MAPPING.items():
        for raw_key, raw_val in raw.items():
            if any(kw in raw_key for kw in keywords):
                if field not in mapped:
                    mapped[field] = raw_val
                    break

    # Build dimensions from H/W if no direct match
    if not mapped.get('dimensions'):
        h = mapped.get('height', raw.get('height', ''))
        w = mapped.get('width',  raw.get('width',  ''))
        if h and w:
            mapped['dimensions'] = f"{h} x {w}"

    # Append capacity to dimensions
    cap = mapped.pop('capacity', '')
    if cap:
        if mapped.get('dimensions'):
            mapped['dimensions'] += f" | Capacity: {cap}"
        else:
            mapped['dimensions'] = f"Capacity: {cap}"

    # Freezer capacity → append to dimensions
    fcap = mapped.pop('freezer_capacity', '')
    if fcap and mapped.get('dimensions'):
        mapped['dimensions'] += f" | Freezer: {fcap}"

    return mapped, raw


# ─────────────────────────────────────────────────────────────────────────────
# MAIN API RESPONSE PARSER
# Parses the mtop.aliexpress.pdp.pc.query JSON
# ─────────────────────────────────────────────────────────────────────────────

def parse_pdp_response(text: str) -> dict | None:
    """
    Parse the mtop.aliexpress.pdp.pc.query JSONP/JSON response.
    Returns flat product dict or None.
    """
    try:
        # Strip JSONP wrapper: mtopjsonpN({...})
        text = text.strip()
        m = re.match(r'^[a-zA-Z_$][a-zA-Z0-9_$]*\s*\((.*)\)\s*;?\s*$', text, re.DOTALL)
        if m:
            text = m.group(1)

        data = json.loads(text)
        result = (data.get('data', {}) or {}).get('result', {}) or {}

        if not result:
            print("[scraper]    ⚠️  Empty result in API response")
            return None

        extracted = {}

        # ── Title ──────────────────────────────────────────────────────────
        title = ''
        # Path 1: GLOBAL_DATA
        try:
            title = result['GLOBAL_DATA']['globalData']['subject']
        except (KeyError, TypeError):
            pass
        # Path 2: titleModule
        if not title:
            try:
                title = result['titleModule']['subject']
            except (KeyError, TypeError):
                pass
        if title:
            extracted['title'] = str(title).strip()

        # ── Specifications ─────────────────────────────────────────────────
        # Path: result.PRODUCT_PROP_PC.showedProps (confirmed from debug)
        props = []
        try:
            props = result['PRODUCT_PROP_PC']['showedProps'] or []
        except (KeyError, TypeError):
            pass

        # Fallback paths
        if not props:
            for path in [
                ['PRODUCT_PROP_PC', 'outerProps'],
                ['specsModule', 'props'],
                ['specs', 'props'],
            ]:
                try:
                    node = result
                    for k in path:
                        node = node[k]
                    if node and isinstance(node, list):
                        props = node
                        break
                except (KeyError, TypeError):
                    pass

        if props:
            print(f"[scraper]    ✅ Found {len(props)} spec props in API JSON")
            mapped, raw = map_props_to_fields(props)
            extracted.update(mapped)

            # Log what was found
            for k, v in raw.items():
                print(f"[scraper]       {k}: {v[:60]}")
        else:
            print("[scraper]    ⚠️  No specs found in PRODUCT_PROP_PC")

        # ── Images ─────────────────────────────────────────────────────────
        images = []
        try:
            images = result['imageModule']['imagePathList'] or []
        except (KeyError, TypeError):
            pass
        if not images:
            try:
                images = result['IMAGE_MODULE']['imagePathList'] or []
            except (KeyError, TypeError):
                pass
        if not images:
            # Deep search for imagePathList
            def find_images(obj, depth=0):
                if depth > 6 or not isinstance(obj, dict):
                    return []
                if 'imagePathList' in obj:
                    v = obj['imagePathList']
                    if isinstance(v, list) and v:
                        return v
                for v in obj.values():
                    result_imgs = find_images(v, depth + 1)
                    if result_imgs:
                        return result_imgs
                return []
            images = find_images(result)

        for idx, img in enumerate(images[:20], 1):
            if img:
                # Ensure full URL
                if img.startswith('//'):
                    img = 'https:' + img
                elif not img.startswith('http'):
                    img = 'https://' + img
                # Remove size suffix
                img = re.sub(r'_\d+x\d+', '', img)
                extracted[f'image_{idx}'] = img

        if images:
            print(f"[scraper]    ✅ Found {len(images)} images")

        # ── Price ──────────────────────────────────────────────────────────
        try:
            price_module = result.get('priceModule', {}) or result.get('PRICE_MODULE', {}) or {}
            price = (price_module.get('formatedActivityPrice') or
                     price_module.get('formatedPrice') or
                     price_module.get('minActivityAmount', {}).get('formatedAmount', '') or '')
            if price:
                extracted['price'] = str(price)
        except Exception:
            pass

        # ── Description ────────────────────────────────────────────────────
        try:
            desc_module = result.get('descriptionModule', {}) or {}
            desc = desc_module.get('description', '') or desc_module.get('content', '')
            if desc and len(desc) > 50:
                # Strip HTML
                desc = re.sub(r'<[^>]+>', ' ', desc)
                desc = ' '.join(desc.split())
                extracted['description'] = desc[:2000]
        except Exception:
            pass

        return extracted if (extracted.get('title') or extracted.get('image_1')) else None

    except json.JSONDecodeError as e:
        print(f"[scraper]    ⚠️  JSON parse error: {e}")
        return None
    except Exception as e:
        print(f"[scraper]    ⚠️  Parse error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# DOM FALLBACKS
# ─────────────────────────────────────────────────────────────────────────────

def get_title_from_dom(page) -> str:
    for selector in [
        'meta[property="og:title"]',
        'h1[data-pl="product-title"]',
        'h1.product-title-text',
    ]:
        try:
            el = page.locator(selector).first
            if el.count() > 0:
                val = (el.get_attribute('content') or el.inner_text() or '').strip()
                if val:
                    return val
        except Exception:
            pass
    return ''


def get_images_from_dom(page) -> dict:
    """Extract images from JS imagePathList variable in page scripts."""
    images = {}
    try:
        for script in page.locator('script').all():
            try:
                txt = script.text_content() or ''
                m = re.search(r'"imagePathList"\s*:\s*\[(.*?)\]', txt, re.DOTALL)
                if m:
                    urls = re.findall(r'"(https?://[^"]+\.jpg[^"]*)"', m.group(1))
                    if not urls:
                        urls = re.findall(r'"(//[^"]+\.jpg[^"]*)"', m.group(1))
                    for idx, url in enumerate(urls[:20], 1):
                        if url.startswith('//'):
                            url = 'https:' + url
                        images[f'image_{idx}'] = re.sub(r'_\d+x\d+', '', url)
                    if images:
                        print(f"[scraper]    ✅ {len(images)} images from DOM script")
                        return images
            except Exception:
                pass
    except Exception:
        pass

    # og:image fallback
    try:
        og = page.locator('meta[property="og:image"]').get_attribute('content')
        if og:
            images['image_1'] = og
    except Exception:
        pass
    return images


def get_description_from_dom(page) -> str:
    for selector in [
        'div.richTextContainer[data-rich-text-render="true"]',
        'div[id="product-description"]',
        'div[id="nav-description"]',
        'div.detailmodule_text',
    ]:
        try:
            el = page.locator(selector).first
            if el.count() > 0:
                text = el.inner_text().strip()
                text = re.sub(r'^.*?Description\s+report\s+', '', text,
                              flags=re.IGNORECASE | re.DOTALL)
                if len(text) > 50 and 'Smarter Shopping' not in text:
                    return text[:2000]
        except Exception:
            pass
    return ''


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    """
    Scrape AliExpress product by intercepting the PDP API response.
    The API (mtop.aliexpress.pdp.pc.query) returns all product data as JSON.
    """
    captured_pdp = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                ]
            )
            context = browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
                viewport={'width': 1366, 'height': 900},
                locale='en-US',
                timezone_id='Asia/Karachi',
            )
            page = context.new_page()

            # ── Intercept PDP API response ──────────────────────────────────
            def handle_response(response):
                url_r = response.url
                # Match the confirmed API endpoint
                if 'mtop.aliexpress.pdp.pc.query' in url_r or \
                   'mtop.aliexpress.itemdetail.pc' in url_r or \
                   ('pdp' in url_r.lower() and 'aliexpress' in url_r and
                    response.status == 200):
                    try:
                        body = response.body()
                        if len(body) > 1000:
                            text = body.decode('utf-8', errors='replace')
                            # Quick check for product data
                            if 'PRODUCT_PROP_PC' in text or 'imagePathList' in text or \
                               'subject' in text:
                                captured_pdp.append(text)
                                print(f"[scraper]    📡 Captured PDP: {url_r[:80]}")
                    except Exception:
                        pass

            page.on('response', handle_response)

            # ── Load page ───────────────────────────────────────────────────
            print(f'\n[scraper] 🌐 Opening: {url}')
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(5000)

            # Scroll a bit to trigger any lazy loads
            page.mouse.wheel(0, 800)
            page.wait_for_timeout(1000)

            # ── Parse API responses ─────────────────────────────────────────
            extracted = {}
            print(f'[scraper]    📦 Captured {len(captured_pdp)} PDP responses')

            for resp_text in captured_pdp:
                parsed = parse_pdp_response(resp_text)
                if parsed:
                    print(f'[scraper]    ✅ Parsed {len(parsed)} fields from API')
                    for k, v in parsed.items():
                        if k not in extracted or not extracted[k]:
                            extracted[k] = v

            # ── DOM fallbacks ───────────────────────────────────────────────
            if not extracted.get('title'):
                t = get_title_from_dom(page)
                if t:
                    extracted['title'] = t

            if not extracted.get('image_1'):
                imgs = get_images_from_dom(page)
                extracted.update(imgs)

            if not extracted.get('description'):
                desc = get_description_from_dom(page)
                if desc:
                    extracted['description'] = desc

            # Price from JS if not in API
            if not extracted.get('price'):
                try:
                    for script in page.locator('script').all():
                        txt = script.text_content() or ''
                        m = re.search(r'"price"\s*:\s*"([^"]+)"', txt)
                        if m:
                            extracted['price'] = m.group(1)
                            break
                except Exception:
                    pass

            browser.close()

        # ── Validate ─────────────────────────────────────────────────────────
        if not extracted.get('title'):
            print('[scraper] ❌ No title — aborting')
            return None

        # ── Summary ───────────────────────────────────────────────────────────
        core = ['brand', 'color', 'dimensions', 'weight', 'material',
                'certifications', 'country_of_origin', 'warranty', 'product_type']
        extra = ['capacity', 'freezer_capacity', 'voltage', 'model_number',
                 'battery', 'display', 'os']
        print(f'\n[scraper] ✅ Extraction complete')
        print(f'[scraper]    Title     : {extracted.get("title", "")[:70]}')
        print(f'[scraper]    Desc      : {len(extracted.get("description", ""))} chars')
        print(f'[scraper]    Core specs: {[k for k in core if extracted.get(k)]}')
        print(f'[scraper]    Extra     : {[k for k in extra if extracted.get(k)]}')
        print(f'[scraper]    Images    : {sum(1 for i in range(1,21) if extracted.get(f"image_{i}"))}')

        # ── Apply defaults ────────────────────────────────────────────────────
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
        }
        for key, default in defaults.items():
            if key not in extracted or not extracted[key]:
                extracted[key] = default

        for i in range(1, 21):
            extracted.setdefault(f'image_{i}', '')

        return extracted

    except Exception as e:
        print(f'[scraper] ❌ ERROR: {e}')
        import traceback
        traceback.print_exc()
        return None


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    test_url = 'https://www.aliexpress.com/item/1005010716013669.html'
    result = get_product_info(test_url)
    if result:
        print('\n' + '='*80)
        print('=== RESULT ===')
        for k, v in result.items():
            if v and not k.startswith('image_'):
                print(f'  {k:25s}: {str(v)[:100]}')
        imgs = sum(1 for i in range(1, 21) if result.get(f'image_{i}'))
        print(f'  {"images":25s}: {imgs}')
    else:
        print('FAILED')
