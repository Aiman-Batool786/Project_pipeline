"""
scraper.py — AliExpress Network Interception Approach
──────────────────────────────────────────────────────
AliExpress loads ALL product data (including specs) via a background
JavaScript/MTOP API call. The DOM spec section is rendered from this
JSON data, but by the time Playwright tries to read the DOM the data
may not have rendered yet.

Solution: Intercept the network response directly using Playwright's
route/response interception. This captures the raw JSON which contains
ALL specifications, images, title, description, etc.

API endpoints intercepted:
  - mtop.aliexpress.pdp.pc.query        (main product detail)
  - mtop.aliexpress.itemdetail.pc.*     (alternative layout)
  - Any JSON response containing "skuModule" or "pageModule"
"""

from playwright.sync_api import sync_playwright
import re
import json
import time


# ─────────────────────────────────────────────────────────────────────────────
# SPECIFICATION MAPPING RULES
# ─────────────────────────────────────────────────────────────────────────────

SPEC_MAPPING_RULES = {
    'brand':             ['brand name', 'brand', 'marque'],
    'color':             ['main color', 'color', 'colour', 'couleur'],
    'dimensions':        ['dimensions (l x w x h', 'dimensions (cm)', 'dimensions',
                          'size', 'taille', 'product size', 'item size'],
    'weight':            ['net weight', 'weight (kg)', 'weight', 'poids',
                          'gross weight', 'item weight'],
    'material':          ['material composition', 'material', 'matière', 'materials'],
    'certifications':    ['certification', 'certifications', 'normes', 'standards',
                          'energy efficiency rating', 'energy consumption grade'],
    'country_of_origin': ['place of origin', 'country of origin', 'origin',
                          'country', 'pays', 'made in'],
    'warranty':          ['warranty', 'garantie', 'guarantee'],
    'product_type':      ['refrigeration type', 'cooling method', 'defrost type',
                          'product type', 'type de produit', 'item type', 'type',
                          'application', 'design', 'operation system'],
    'age_from':          ['age from', 'recommended age from', 'age (from)'],
    'age_to':            ['age to',   'recommended age to',   'age (to)'],
    'gender':            ['gender', 'suitable for', 'sexe'],
    'capacity':          ['capacity', 'fridge capacity', 'net capacity', 'volume'],
    'freezer_capacity':  ['freezer capacity'],
    'voltage':           ['voltage', 'rated voltage'],
    'model_number':      ['model number', 'model no', 'model', 'item model'],
    'power_source':      ['power source', 'power supply'],
    'installation':      ['installation', 'mounting type'],
    'style':             ['style'],
    'features':          ['feature', 'features', 'key features'],
    'battery':           ['battery capacity', 'battery capacity(mah)'],
    'display':           ['display size', 'screen size', 'display resolution',
                          'screen material', 'screen type'],
    'camera':            ['rear camera pixel', 'front camera pixel', 'camera'],
    'connectivity':      ['cellular', 'wifi', 'nfc', 'bluetooth version'],
    'os':                ['operation system', 'android version'],
}


def map_spec_list_to_fields(spec_list: list) -> dict:
    """
    Map a list of {attrName, attrValue} dicts to internal field names.
    """
    mapped = {}
    raw = {}

    for item in spec_list:
        name  = str(item.get('attrName', '')  or item.get('name',  '') or '').strip()
        value = str(item.get('attrValue', '') or item.get('value', '') or '').strip()
        if name and value:
            raw[name.lower()] = value

    for field, keywords in SPEC_MAPPING_RULES.items():
        for raw_key, raw_val in raw.items():
            if any(kw in raw_key for kw in keywords):
                if field not in mapped:
                    mapped[field] = raw_val
                    break

    # Merge features into bullet_points
    features = mapped.pop('features', '')
    if features:
        mapped['bullet_points'] = [features]

    return mapped, raw


# ─────────────────────────────────────────────────────────────────────────────
# JSON RESPONSE PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def _find_specs_in_json(data: dict) -> list:
    """
    Recursively search a JSON blob for spec arrays.
    AliExpress nests them under various keys.
    """
    candidates = []

    def search(obj, depth=0):
        if depth > 10:
            return
        if isinstance(obj, dict):
            # Known spec container keys
            for key in ('props', 'specifications', 'specificationModule',
                        'properties', 'productProps', 'skuPropSalePrice',
                        'attrList', 'attributes'):
                if key in obj:
                    val = obj[key]
                    if isinstance(val, list) and val:
                        # Check if items look like spec pairs
                        first = val[0] if val else {}
                        if isinstance(first, dict) and any(
                            k in first for k in ('attrName', 'name', 'attrValue', 'value')
                        ):
                            candidates.append(val)
            for v in obj.values():
                search(v, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                search(item, depth + 1)

    search(data)
    return candidates


def parse_aliexpress_api_response(response_text: str) -> dict | None:
    """
    Parse AliExpress MTOP/JSON API response and extract product data.
    Returns a flat dict with all extracted fields, or None if parsing fails.
    """
    try:
        # The response may be JSONP: callback({...}) or plain JSON
        text = response_text.strip()
        # Strip JSONP wrapper
        jsonp_match = re.match(r'^[a-zA-Z_$][a-zA-Z0-9_$]*\s*\((.*)\)\s*;?\s*$', text, re.DOTALL)
        if jsonp_match:
            text = jsonp_match.group(1)

        data = json.loads(text)

        # Navigate into result
        result = (data.get('data', {}) or
                  data.get('result', {}) or
                  data)

        if not result:
            return None

        extracted = {}

        # ── Title ──────────────────────────────────────────────────────────
        title = ''
        for path in [
            ['pageModule', 'title'],
            ['titleModule', 'subject'],
            ['product', 'title'],
            ['GLOBAL_DATA', 'globalData', 'productTitle'],
        ]:
            node = result
            for key in path:
                if isinstance(node, dict):
                    node = node.get(key)
                else:
                    node = None
                    break
            if node and isinstance(node, str):
                title = node
                break

        if not title:
            # Deep search for title-like keys
            def find_title(obj, depth=0):
                if depth > 8 or not isinstance(obj, dict):
                    return ''
                for k in ('subject', 'title', 'productTitle'):
                    v = obj.get(k)
                    if v and isinstance(v, str) and len(v) > 10:
                        return v
                for v in obj.values():
                    if isinstance(v, dict):
                        t = find_title(v, depth + 1)
                        if t:
                            return t
                return ''
            title = find_title(result)

        if title:
            extracted['title'] = title

        # ── Images ─────────────────────────────────────────────────────────
        images = []
        def find_images(obj, depth=0):
            if depth > 8:
                return
            if isinstance(obj, dict):
                for k in ('imagePathList', 'images', 'imageList', 'imageModuleList'):
                    v = obj.get(k)
                    if isinstance(v, list):
                        for item in v:
                            if isinstance(item, str) and ('alicdn' in item or 'aliexpress' in item):
                                images.append(item)
                            elif isinstance(item, dict):
                                for ik in ('imageUrl', 'url', 'imagePathList'):
                                    iv = item.get(ik)
                                    if iv and isinstance(iv, str):
                                        images.append(iv)
                for v in obj.values():
                    find_images(v, depth + 1)
            elif isinstance(obj, list):
                for item in obj:
                    find_images(item, depth + 1)

        find_images(result)
        seen = set()
        for i, img in enumerate(images[:20], 1):
            clean = re.sub(r'_\d+x\d+', '', img)
            if not clean.startswith('http'):
                clean = 'https:' + clean if clean.startswith('//') else 'https://' + clean
            if clean not in seen:
                seen.add(clean)
                extracted[f'image_{i}'] = clean

        # ── Specifications ──────────────────────────────────────────────────
        spec_candidates = _find_specs_in_json(result)

        all_specs_raw = {}
        all_specs_mapped = {}

        for candidate in spec_candidates:
            mapped, raw = map_spec_list_to_fields(candidate)
            all_specs_raw.update(raw)
            all_specs_mapped.update(mapped)

        extracted.update(all_specs_mapped)

        if all_specs_raw:
            print(f"[scraper]    ✅ {len(all_specs_raw)} raw specs from JSON:")
            for k, v in list(all_specs_raw.items())[:20]:
                print(f"[scraper]       {k}: {v[:60]}")

        # ── Price ───────────────────────────────────────────────────────────
        def find_price(obj, depth=0):
            if depth > 8 or not isinstance(obj, dict):
                return ''
            for k in ('price', 'formatedActivityPrice', 'formatedPrice',
                      'minActivityAmount', 'minAmount'):
                v = obj.get(k)
                if v and isinstance(v, (str, int, float)):
                    s = str(v)
                    if any(c.isdigit() for c in s):
                        return s
            for v in obj.values():
                p = find_price(v, depth + 1)
                if p:
                    return p
            return ''

        price = find_price(result)
        if price:
            extracted['price'] = price

        # ── Description ─────────────────────────────────────────────────────
        def find_description(obj, depth=0):
            if depth > 8 or not isinstance(obj, dict):
                return ''
            for k in ('descriptionModule', 'description', 'detail', 'productDesc'):
                v = obj.get(k)
                if isinstance(v, dict):
                    for dk in ('description', 'content', 'detail', 'html'):
                        dv = v.get(dk)
                        if dv and isinstance(dv, str) and len(dv) > 50:
                            # Strip HTML
                            return re.sub(r'<[^>]+>', ' ', dv).strip()
                elif isinstance(v, str) and len(v) > 50:
                    return re.sub(r'<[^>]+>', ' ', v).strip()
            for v in obj.values():
                d = find_description(v, depth + 1)
                if d:
                    return d
            return ''

        desc = find_description(result)
        if desc:
            extracted['description'] = desc[:2000]

        return extracted if extracted.get('title') or extracted.get('image_1') else None

    except Exception as e:
        print(f"[scraper]    ⚠️  JSON parse error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# DOM FALLBACK EXTRACTORS (used if API interception fails)
# ─────────────────────────────────────────────────────────────────────────────

def extract_specs_from_dom(page) -> dict:
    """Fallback DOM extraction with thorough scrolling."""
    specs = {}

    # Scroll all the way down in steps
    print("[scraper]    🔄 Scrolling to load spec section...")
    try:
        total_height = page.evaluate("document.body.scrollHeight")
        for y in range(0, total_height + 1000, 800):
            page.evaluate(f"window.scrollTo(0, {y})")
            page.wait_for_timeout(200)

        page.evaluate("""
            const el = document.getElementById('nav-specification');
            if (el) el.scrollIntoView({ behavior: 'instant', block: 'center' });
        """)
        page.wait_for_timeout(3000)
    except Exception:
        pass

    # Try reading DOM specs
    try:
        items = page.locator('#nav-specification [class*="specification--prop"]').all()
        if not items:
            items = page.locator('[class*="specification--prop"]').all()

        print(f"[scraper]    DOM spec items found: {len(items)}")

        for item in items:
            try:
                title_el = item.locator('[class*="specification--title"] span').first
                desc_el  = item.locator('[class*="specification--desc"]').first
                title = title_el.inner_text().strip() if title_el.count() > 0 else ""
                desc  = ""
                if desc_el.count() > 0:
                    desc = desc_el.get_attribute('title') or ""
                    if not desc:
                        span = desc_el.locator('span').first
                        desc = span.inner_text().strip() if span.count() > 0 else ""
                if title and desc:
                    specs[title.lower()] = desc
            except Exception:
                pass
    except Exception as e:
        print(f"[scraper]    ⚠️  DOM spec error: {e}")

    return specs


def extract_description_dom(page) -> str:
    for selector in [
        'div.richTextContainer[data-rich-text-render="true"]',
        'div[id="product-description"]',
        'div[id="nav-description"]',
        'div.detailmodule_text',
    ]:
        try:
            el = page.locator(selector)
            if el.count() > 0:
                text = el.first.inner_text().strip()
                text = re.sub(r'^.*?Description\s+report\s+', '', text,
                              flags=re.IGNORECASE | re.DOTALL)
                if len(text) > 50 and "Smarter Shopping" not in text:
                    return text[:2000]
        except Exception:
            pass
    return ""


def extract_images_dom(page) -> dict:
    images = {}
    try:
        for script in page.locator("script").all():
            try:
                txt = script.text_content() or ""
                m = re.search(r'"imagePathList":\s*\[(.*?)\]', txt)
                if m:
                    urls = re.findall(r'"(https://[^"]+\.jpg[^"]*)"', m.group(1))
                    for idx, url in enumerate(urls[:20], 1):
                        images[f"image_{idx}"] = re.sub(r'_\d+x\d+', '', url)
                    if images:
                        return images
            except Exception:
                pass
    except Exception:
        pass
    try:
        og = page.locator('meta[property="og:title"]')
        og_img = page.locator('meta[property="og:image"]').get_attribute('content')
        if og_img:
            images['image_1'] = og_img
    except Exception:
        pass
    return images


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    """
    Scrape AliExpress product page using network interception.
    Captures the MTOP API JSON response which contains all product data.
    Falls back to DOM scraping if API interception fails.
    """
    captured_responses = []

    try:
        with sync_playwright() as p:

            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ]
            )

            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 900},
                locale="en-US",
                timezone_id="Asia/Karachi",
            )

            page = context.new_page()

            # ── Network interception ────────────────────────────────────────
            def handle_response(response):
                url_lower = response.url.lower()
                # Capture AliExpress product API responses
                if any(keyword in url_lower for keyword in [
                    'mtop.aliexpress.pdp.pc.query',
                    'mtop.aliexpress.itemdetail',
                    'pdp.pc.query',
                    'asyncpcdetail',
                    'itemdetail',
                ]):
                    try:
                        body = response.body()
                        if body and len(body) > 500:
                            text = body.decode('utf-8', errors='replace')
                            captured_responses.append({
                                'url': response.url,
                                'text': text
                            })
                            print(f"[scraper]    📡 Captured API response: {response.url[:80]}")
                    except Exception:
                        pass

            page.on('response', handle_response)

            # ── Load page ───────────────────────────────────────────────────
            print(f"\n[scraper] 🌐 Opening: {url}")
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            page.wait_for_timeout(5000)

            # Scroll to trigger lazy API calls
            page.mouse.move(400, 300)
            for y in [500, 1500, 3000, 5000]:
                page.mouse.wheel(0, y)
                page.wait_for_timeout(500)

            page.wait_for_timeout(3000)

            # ── Try parsing captured API responses ──────────────────────────
            extracted = {}
            print(f"[scraper]    📦 Captured {len(captured_responses)} API responses")

            for resp in captured_responses:
                print(f"[scraper]    🔍 Parsing: {resp['url'][:80]}")
                parsed = parse_aliexpress_api_response(resp['text'])
                if parsed:
                    print(f"[scraper]    ✅ Got {len(parsed)} fields from API")
                    # Merge, prefer first successful parse for most fields
                    for k, v in parsed.items():
                        if k not in extracted or not extracted[k]:
                            extracted[k] = v

            # ── DOM fallback for title ──────────────────────────────────────
            if not extracted.get('title'):
                print("[scraper]    🔄 Trying DOM title fallback...")
                try:
                    og_title = page.locator(
                        'meta[property="og:title"]'
                    ).get_attribute("content") or ""
                    if og_title:
                        extracted['title'] = og_title
                    else:
                        h1 = page.locator('h1[data-pl="product-title"]').first
                        if h1.count() > 0:
                            extracted['title'] = h1.inner_text().strip()
                except Exception:
                    pass

            # ── DOM fallback for images ─────────────────────────────────────
            if not extracted.get('image_1'):
                print("[scraper]    🔄 Trying DOM image fallback...")
                imgs = extract_images_dom(page)
                extracted.update(imgs)

            # ── DOM fallback for specs ──────────────────────────────────────
            core_spec_fields = ['brand', 'color', 'dimensions', 'weight',
                                 'material', 'certifications', 'country_of_origin',
                                 'warranty', 'product_type']
            if not any(extracted.get(f) for f in core_spec_fields):
                print("[scraper]    🔄 No specs from API — trying DOM fallback...")
                raw_specs = extract_specs_from_dom(page)
                if raw_specs:
                    print(f"[scraper]    📋 DOM found {len(raw_specs)} raw spec items")
                    mapped, _ = map_spec_list_to_fields([
                        {'attrName': k, 'attrValue': v}
                        for k, v in raw_specs.items()
                    ])
                    extracted.update(mapped)
                else:
                    print("[scraper]    ⚠️  DOM spec fallback also found nothing")

            # ── DOM fallback for description ────────────────────────────────
            if not extracted.get('description'):
                print("[scraper]    🔄 Trying DOM description fallback...")
                desc = extract_description_dom(page)
                if desc:
                    extracted['description'] = desc

            # ── JS price fallback ───────────────────────────────────────────
            if not extracted.get('price'):
                try:
                    for script in page.locator("script").all():
                        txt = script.text_content() or ""
                        m = re.search(r'"price":\s*"([^"]+)"', txt)
                        if m:
                            extracted['price'] = m.group(1)
                            break
                except Exception:
                    pass

            browser.close()

        # ── Validate ────────────────────────────────────────────────────────
        if not extracted.get('title'):
            print("[scraper] ❌ No title extracted — aborting")
            return None

        # ── Summary ─────────────────────────────────────────────────────────
        filled_core  = [k for k in core_spec_fields if extracted.get(k)]
        extra_fields = ['capacity', 'freezer_capacity', 'voltage', 'model_number',
                        'battery', 'display', 'os']
        filled_extra = [k for k in extra_fields if extracted.get(k)]

        print(f"\n[scraper] ✅ Extraction complete")
        print(f"[scraper]    Title      : {extracted.get('title', '')[:70]}")
        print(f"[scraper]    Description: {len(extracted.get('description', ''))} chars")
        print(f"[scraper]    Core specs : {len(filled_core)} → {filled_core}")
        print(f"[scraper]    Extra specs: {len(filled_extra)} → {filled_extra}")
        print(f"[scraper]    Images     : {sum(1 for i in range(1,21) if extracted.get(f'image_{i}'))}")

        # ── Apply defaults ───────────────────────────────────────────────────
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
            extracted.setdefault(f'image_{i}', "")

        return extracted

    except Exception as e:
        print(f"[scraper] ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_url = "https://www.aliexpress.com/item/1005010716013669.html"
    result   = get_product_info(test_url)

    if result:
        print("\n" + "=" * 80)
        print("=== SCRAPED DATA ===")
        print("=" * 80)
        for k, v in result.items():
            if v and not k.startswith('image_'):
                print(f"  {k:25s}: {str(v)[:100]}")
        images = sum(1 for i in range(1, 21) if result.get(f'image_{i}'))
        print(f"  {'images':25s}: {images} found")
    else:
        print("Scraping failed.")
