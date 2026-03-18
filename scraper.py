"""
scraper.py
──────────
APPROACH: Direct DOM + embedded JSON extraction (no API interception dependency).

Strategy order:
  1. Load page, scroll fully to trigger all lazy content
  2. Extract __NEXT_DATA__ / window.__runParams / inline JSON from <script> tags
  3. DOM fallback with aggressive selectors
  4. Description: scroll to bottom, wait, extract from rendered DOM / iframe

RULE: Seller info is NEVER sent to LLM. Always stored as original scraped values.
"""

from playwright.sync_api import sync_playwright
import re
import json
import time


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
    'age_to':            ['age to', 'recommended age to', 'age (to)', 'maximum age'],
    'gender':            ['gender', 'suitable for', 'sexe'],
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


def map_props_to_fields(props: list) -> tuple:
    raw = {}
    for item in props:
        # Handle both dict format and tuple/list format
        if isinstance(item, dict):
            name  = str(item.get('attrName', '') or item.get('name', '') or '').strip()
            value = str(item.get('attrValue', '') or item.get('value', '') or '').strip()
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            name  = str(item[0]).strip()
            value = str(item[1]).strip()
        else:
            continue
        if name and value:
            raw[name.lower()] = value

    mapped = {}
    for field, keywords in SPEC_MAPPING.items():
        for raw_key, raw_val in raw.items():
            if any(kw in raw_key for kw in keywords):
                if field not in mapped:
                    mapped[field] = raw_val
                    break

    # Build dimensions from H/W
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
        'store_name':           '',
        'store_id':             '',
        'store_url':            '',
        'seller_id':            '',
        'seller_positive_rate': '',
        'seller_rating':        '',
        'seller_communication': '',
        'seller_shipping_speed':'',
        'seller_country':       '',
        'store_open_date':      '',
        'seller_level':         '',
        'seller_total_reviews': '',
        'seller_positive_num':  '',
        'is_top_rated':         '',
    }

    try:
        shop = result.get('SHOP_CARD_PC', {}) or {}

        # Try alternate paths
        if not shop:
            shop = result.get('storeModule', {}) or {}
        if not shop:
            shop = result.get('shopCardModule', {}) or {}
        if not shop:
            # Deep search
            shop = _deep_find_key(result, 'storeName', max_depth=5)

        if not shop or not isinstance(shop, dict):
            print("[scraper]    ⚠️  No shop/seller data found")
            return seller

        seller['store_name'] = str(shop.get('storeName', '') or '')
        seller['seller_level'] = str(shop.get('sellerLevel', '') or '')
        seller['seller_positive_rate'] = str(shop.get('sellerPositiveRate', '') or
                                              shop.get('positiveRate', '') or '')
        seller['seller_total_reviews'] = str(shop.get('sellerTotalNum', '') or '')
        seller['seller_positive_num']  = str(shop.get('sellerPositiveNum', '') or '')

        benefit_list = shop.get('benefitInfoList', []) or []
        for item in benefit_list:
            title = str(item.get('title', '') or '').lower().strip()
            value = str(item.get('value', '') or '').strip()
            if 'store rating' in title:
                seller['seller_rating'] = value
            elif 'communication' in title:
                seller['seller_communication'] = value

        seller_info = shop.get('sellerInfo', {}) or {}
        if seller_info:
            seller['seller_id']      = str(seller_info.get('adminSeq', '') or '')
            seller['store_id']       = str(seller_info.get('storeNum', '') or '')
            seller['seller_country'] = str(seller_info.get('countryCompleteName', '') or '')
            seller['store_open_date']= str(seller_info.get('formatOpenTime', '') or '')
            seller['is_top_rated']   = str(seller_info.get('topRatedSeller', '') or '')

            raw_url = str(seller_info.get('storeURL', '') or '')
            if raw_url:
                if raw_url.startswith('//'):
                    raw_url = 'https:' + raw_url
                seller['store_url'] = raw_url

        # Fallbacks
        if not seller['store_name']:
            seller['store_name'] = str(shop.get('title', '') or
                                       shop.get('name', '') or '')
        if not seller['store_id']:
            seller['store_id'] = str(shop.get('storeId', '') or
                                     shop.get('storeNum', '') or '')
        if not seller['store_url'] and seller.get('store_id'):
            seller['store_url'] = f"https://www.aliexpress.com/store/{seller['store_id']}"

        # GLOBAL_DATA fallbacks
        try:
            gd = result.get('GLOBAL_DATA', {}).get('globalData', {}) or {}
            if not seller['store_name']:
                seller['store_name'] = str(gd.get('storeName', '') or '')
            if not seller['store_id']:
                seller['store_id'] = str(gd.get('storeId', '') or '')
        except (KeyError, TypeError):
            pass

        found = {k: v for k, v in seller.items() if v}
        if found:
            print(f"[scraper]    ✅ Seller info: {list(found.keys())}")

    except Exception as e:
        print(f"[scraper]    ⚠️  Seller info error: {e}")

    return seller


def _deep_find_key(obj, key, max_depth=5, _depth=0):
    """Recursively find a dict containing a specific key."""
    if _depth > max_depth or not isinstance(obj, dict):
        return None
    if key in obj:
        return obj
    for v in obj.values():
        if isinstance(v, dict):
            result = _deep_find_key(v, key, max_depth, _depth + 1)
            if result:
                return result
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    result = _deep_find_key(item, key, max_depth, _depth + 1)
                    if result:
                        return result
    return None


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACT ALL DATA FROM EMBEDDED SCRIPT TAGS
# This is the PRIMARY extraction method — works even when API interception fails
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_page_scripts(page) -> dict:
    """
    Extract product data from ALL script tags on the page.
    AliExpress embeds JSON in multiple formats:
      - window.runParams = {...}
      - window.__INIT_DATA__ = {...}
      - __NEXT_DATA__ script tag
      - Inline JSON blobs with known keys
    """
    extracted = {}

    print("[scraper]    🔍 Scanning all script tags for embedded data...")

    try:
        all_script_content = page.evaluate('''() => {
            var scripts = document.querySelectorAll('script');
            var contents = [];
            for (var i = 0; i < scripts.length; i++) {
                var text = scripts[i].textContent || '';
                if (text.length > 200) {
                    contents.push(text);
                }
            }
            return contents;
        }''')

        print(f"[scraper]    📜 Found {len(all_script_content)} script blocks to scan")

        for idx, script_text in enumerate(all_script_content):
            try:
                _extract_from_script_text(script_text, extracted)
            except Exception:
                continue

    except Exception as e:
        print(f"[scraper]    ⚠️  Script scan error: {e}")

    return extracted


def _extract_from_script_text(text: str, extracted: dict):
    """Parse a single script block and extract known data patterns."""

    # ── Pattern 1: window.runParams / window.__runParams ────────────────
    for pattern in [
        r'window\.runParams\s*=\s*(\{.+?\})\s*;',
        r'window\.__runParams\s*=\s*(\{.+?\})\s*;',
        r'runParams\s*=\s*(\{.+?\})\s*;',
    ]:
        m = re.search(pattern, text, re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(1))
                _parse_run_params(data, extracted)
                print(f"[scraper]       ✅ Extracted from runParams")
            except json.JSONDecodeError:
                pass

    # ── Pattern 2: window.__INIT_DATA__ ─────────────────────────────────
    m = re.search(r'window\.__INIT_DATA__\s*=\s*(\{.+?\})\s*;?\s*$', text, re.DOTALL | re.MULTILINE)
    if m:
        try:
            data = json.loads(m.group(1))
            _parse_init_data(data, extracted)
            print(f"[scraper]       ✅ Extracted from __INIT_DATA__")
        except json.JSONDecodeError:
            pass

    # ── Pattern 3: Large JSON blobs with known keys ─────────────────────
    if '"imagePathList"' in text or '"productPropList"' in text or '"PRODUCT_PROP_PC"' in text:
        # Try to find any JSON object in the script
        for json_pattern in [
            r'(\{"data"\s*:\s*\{.+\})\s*;?\s*$',
            r'(\{"result"\s*:\s*\{.+\})\s*;?\s*$',
            r'JSON\.parse\s*\(\s*[\'"](.+?)[\'"]\s*\)',
        ]:
            m = re.search(json_pattern, text, re.DOTALL | re.MULTILINE)
            if m:
                try:
                    raw = m.group(1)
                    # Unescape if from JSON.parse
                    if '\\' in raw and '"data"' not in raw:
                        raw = raw.encode().decode('unicode_escape')
                    data = json.loads(raw)
                    _parse_generic_json(data, extracted)
                    print(f"[scraper]       ✅ Extracted from inline JSON blob")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass

    # ── Pattern 4: Direct key extraction via regex (last resort) ────────
    if not extracted.get('title'):
        for p in [
            r'"subject"\s*:\s*"([^"]{10,200})"',
            r'"title"\s*:\s*"([^"]{10,200})"',
            r'"productTitle"\s*:\s*"([^"]{10,200})"',
        ]:
            m = re.search(p, text)
            if m:
                extracted.setdefault('title', m.group(1))
                break

    # Extract image list via regex
    if not extracted.get('image_1'):
        m = re.search(r'"imagePathList"\s*:\s*\[([^\]]+)\]', text)
        if m:
            urls = re.findall(r'"((?:https?://|//)[^"]+)"', m.group(1))
            for i, url in enumerate(urls[:20], 1):
                if url.startswith('//'):
                    url = 'https:' + url
                extracted.setdefault(f'image_{i}', re.sub(r'_\d+x\d+', '', url))

    # Extract price via regex
    if not extracted.get('price'):
        for p in [
            r'"formatedActivityPrice"\s*:\s*"([^"]+)"',
            r'"formatedPrice"\s*:\s*"([^"]+)"',
            r'"minPrice"\s*:\s*"([^"]+)"',
            r'"discountPrice"\s*:\s*"([^"]+)"',
        ]:
            m = re.search(p, text)
            if m:
                extracted.setdefault('price', m.group(1))
                break

    # Extract specs via regex
    spec_patterns = [
        r'"showedProps"\s*:\s*(\[[^\]]*\])',
        r'"outerProps"\s*:\s*(\[[^\]]*\])',
        r'"productPropList"\s*:\s*(\[[^\]]*\])',
        r'"props"\s*:\s*(\[[^\]]*\])',
    ]
    if not extracted.get('_specs_found'):
        for p in spec_patterns:
            m = re.search(p, text, re.DOTALL)
            if m:
                try:
                    props = json.loads(m.group(1))
                    if props and len(props) > 0:
                        mapped, raw = map_props_to_fields(props)
                        if mapped:
                            for k, v in mapped.items():
                                extracted.setdefault(k, v)
                            extracted['_specs_found'] = True
                            extracted['_raw_specs'] = raw
                            print(f"[scraper]       ✅ Found {len(mapped)} specs via regex")
                            break
                except json.JSONDecodeError:
                    pass

    # Extract description HTML via regex
    if not extracted.get('description'):
        for p in [
            r'"description"\s*:\s*"((?:[^"\\]|\\.){50,})"',
            r'"descriptionContent"\s*:\s*"((?:[^"\\]|\\.){50,})"',
            r'"content"\s*:\s*"((?:[^"\\]|\\.){50,})"',
        ]:
            m = re.search(p, text)
            if m:
                try:
                    desc = m.group(1).encode().decode('unicode_escape')
                    desc = re.sub(r'<[^>]+>', ' ', desc)
                    desc = ' '.join(desc.split()).strip()
                    if len(desc) > 50:
                        extracted['description'] = desc[:2000]
                        print(f"[scraper]       ✅ Description from script regex ({len(desc)} chars)")
                        break
                except Exception:
                    pass

    # Extract seller/store info via regex
    if not extracted.get('store_name'):
        m = re.search(r'"storeName"\s*:\s*"([^"]+)"', text)
        if m:
            extracted.setdefault('store_name', m.group(1))


def _parse_run_params(data: dict, extracted: dict):
    """Parse window.runParams format."""
    # Navigate to actual data
    result = data
    if 'data' in result:
        result = result['data']
    if 'result' in result:
        result = result['result']

    _parse_generic_json(result, extracted)


def _parse_init_data(data: dict, extracted: dict):
    """Parse __INIT_DATA__ format."""
    # This format often wraps data differently
    for key in ['data', 'pageData', 'productData', 'result']:
        if key in data and isinstance(data[key], dict):
            _parse_generic_json(data[key], extracted)
            return

    # Try all top-level values
    for v in data.values():
        if isinstance(v, dict) and len(v) > 5:
            _parse_generic_json(v, extracted)


def _parse_generic_json(data: dict, extracted: dict):
    """Extract known fields from any JSON structure."""
    if not isinstance(data, dict):
        return

    # Title
    if not extracted.get('title'):
        for path in [
            ['GLOBAL_DATA', 'globalData', 'subject'],
            ['titleModule', 'subject'],
            ['pageModule', 'title'],
        ]:
            val = _get_nested(data, path)
            if val and isinstance(val, str) and len(val) > 5:
                extracted['title'] = val.strip()
                break

        # Deep search for subject/title
        if not extracted.get('title'):
            val = _deep_find_value(data, 'subject', max_depth=4)
            if val and isinstance(val, str) and len(val) > 5:
                extracted['title'] = val.strip()

    # Specs
    if not extracted.get('_specs_found'):
        for path in [
            ['PRODUCT_PROP_PC', 'showedProps'],
            ['PRODUCT_PROP_PC', 'outerProps'],
            ['productPropModule', 'props'],
            ['specsModule', 'props'],
        ]:
            props = _get_nested(data, path)
            if props and isinstance(props, list) and len(props) > 0:
                mapped, raw = map_props_to_fields(props)
                if mapped:
                    for k, v in mapped.items():
                        extracted.setdefault(k, v)
                    extracted['_specs_found'] = True
                    extracted['_raw_specs'] = raw
                    print(f"[scraper]       ✅ Specs from JSON path: {'.'.join(path)}")
                    break

    # Images
    if not extracted.get('image_1'):
        images = _get_nested(data, ['imageModule', 'imagePathList'])
        if not images:
            images = _deep_find_value(data, 'imagePathList', max_depth=4)
        if images and isinstance(images, list):
            for i, img in enumerate(images[:20], 1):
                if img:
                    if isinstance(img, str):
                        url = img
                    elif isinstance(img, dict):
                        url = img.get('url', '') or img.get('src', '')
                    else:
                        continue
                    if url:
                        if url.startswith('//'):
                            url = 'https:' + url
                        elif not url.startswith('http'):
                            url = 'https://' + url
                        extracted[f'image_{i}'] = re.sub(r'_\d+x\d+', '', url)

    # Price
    if not extracted.get('price'):
        for path in [
            ['priceModule', 'formatedActivityPrice'],
            ['priceModule', 'formatedPrice'],
            ['PRICE_MODULE', 'formatedActivityPrice'],
            ['PRICE_MODULE', 'formatedPrice'],
        ]:
            val = _get_nested(data, path)
            if val:
                extracted['price'] = str(val)
                break

        if not extracted.get('price'):
            val = _deep_find_value(data, 'formatedActivityPrice', max_depth=4)
            if not val:
                val = _deep_find_value(data, 'formatedPrice', max_depth=4)
            if val:
                extracted['price'] = str(val)

    # Description from JSON
    if not extracted.get('description'):
        for path in [
            ['descriptionModule', 'description'],
            ['descriptionModule', 'content'],
            ['DESCRIPTION_PC', 'descriptionContent'],
            ['DESCRIPTION_PC', 'content'],
        ]:
            val = _get_nested(data, path)
            if val and isinstance(val, str) and len(val) > 50:
                desc = re.sub(r'<[^>]+>', ' ', val)
                desc = ' '.join(desc.split()).strip()
                if len(desc) > 50:
                    extracted['description'] = desc[:2000]
                    break

    # Seller info
    seller = extract_seller_info(data)
    for k, v in seller.items():
        if v and not extracted.get(k):
            extracted[k] = v


def _get_nested(data: dict, keys: list):
    """Safely get a nested value."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
        if current is None:
            return None
    return current


def _deep_find_value(obj, key, max_depth=5, _depth=0):
    """Recursively find the value for a specific key."""
    if _depth > max_depth:
        return None
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            result = _deep_find_value(v, key, max_depth, _depth + 1)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _deep_find_value(item, key, max_depth, _depth + 1)
            if result is not None:
                return result
    return None


# ─────────────────────────────────────────────────────────────────────────────
# DOM-BASED EXTRACTION (runs AFTER script extraction)
# ─────────────────────────────────────────────────────────────────────────────

def get_title_from_dom(page) -> str:
    """Extract title from DOM elements."""
    try:
        title = page.evaluate('''() => {
            // Method 1: og:title
            var og = document.querySelector('meta[property="og:title"]');
            if (og && og.content && og.content.length > 5) return og.content.trim();

            // Method 2: h1 tags
            var h1s = document.querySelectorAll('h1');
            for (var i = 0; i < h1s.length; i++) {
                var text = h1s[i].textContent.trim();
                if (text.length > 10 && text.length < 500) return text;
            }

            // Method 3: title tag
            var titleTag = document.querySelector('title');
            if (titleTag) {
                var t = titleTag.textContent.trim();
                // Remove "| AliExpress" suffix
                t = t.replace(/\\s*[-|]\\s*AliExpress.*$/i, '').trim();
                if (t.length > 5) return t;
            }

            return '';
        }''')
        return title or ''
    except Exception:
        return ''


def get_images_from_dom(page) -> dict:
    """Extract images from DOM."""
    images = {}
    try:
        urls = page.evaluate('''() => {
            var results = [];

            // Method 1: Image carousel / gallery
            var imgs = document.querySelectorAll(
                'img[src*="alicdn"], img[src*="ae01.alicdn"], ' +
                'img[data-src*="alicdn"], img[src*="cbu01.alicdn"]'
            );
            for (var i = 0; i < imgs.length; i++) {
                var src = imgs[i].src || imgs[i].getAttribute('data-src') || '';
                if (src && src.includes('alicdn') && !src.includes('avatar') &&
                    !src.includes('icon') && !src.includes('logo') &&
                    (src.includes('.jpg') || src.includes('.png') || src.includes('.webp'))) {
                    if (results.indexOf(src) === -1) results.push(src);
                }
            }

            // Method 2: og:image
            if (results.length === 0) {
                var og = document.querySelector('meta[property="og:image"]');
                if (og && og.content) results.push(og.content);
            }

            return results.slice(0, 20);
        }''')

        for i, url in enumerate(urls or [], 1):
            if url:
                if url.startswith('//'):
                    url = 'https:' + url
                images[f'image_{i}'] = re.sub(r'_\d+x\d+', '', url)

        if images:
            print(f"[scraper]    ✅ {len(images)} images from DOM")

    except Exception as e:
        print(f"[scraper]    ⚠️  DOM image error: {e}")

    return images


def get_specs_from_dom(page) -> tuple:
    """Extract specifications from rendered DOM table/list."""
    try:
        specs = page.evaluate('''() => {
            var results = [];

            // Method 1: Spec list items (key-value pairs)
            var specItems = document.querySelectorAll(
                '[class*="spec"] li, [class*="prop"] li, ' +
                '[class*="attribute"] li, [class*="Specification"] li, ' +
                '[class*="product-prop"] li, [class*="ProductProp"] li, ' +
                'div[class*="spec-item"], div[class*="prop-item"], ' +
                'ul[class*="spec"] li, ul[class*="prop"] li'
            );
            for (var i = 0; i < specItems.length; i++) {
                var el = specItems[i];
                var text = el.textContent.trim();

                // Try to find key-value via child elements
                var spans = el.querySelectorAll('span, div, dt, dd, th, td');
                if (spans.length >= 2) {
                    var key = spans[0].textContent.trim().replace(/:$/, '');
                    var val = spans[1].textContent.trim();
                    if (key && val && key !== val) {
                        results.push({attrName: key, attrValue: val});
                        continue;
                    }
                }

                // Try colon-separated
                if (text.includes(':')) {
                    var parts = text.split(':');
                    var key = parts[0].trim();
                    var val = parts.slice(1).join(':').trim();
                    if (key && val) {
                        results.push({attrName: key, attrValue: val});
                    }
                }
            }

            // Method 2: Table rows
            if (results.length === 0) {
                var rows = document.querySelectorAll(
                    '[class*="spec"] tr, [class*="prop"] tr, ' +
                    '[class*="detail"] table tr, table[class*="spec"] tr'
                );
                for (var i = 0; i < rows.length; i++) {
                    var cells = rows[i].querySelectorAll('td, th');
                    if (cells.length >= 2) {
                        var key = cells[0].textContent.trim().replace(/:$/, '');
                        var val = cells[1].textContent.trim();
                        if (key && val && key !== val) {
                            results.push({attrName: key, attrValue: val});
                        }
                    }
                }
            }

            // Method 3: Definition lists
            if (results.length === 0) {
                var dts = document.querySelectorAll('dt');
                for (var i = 0; i < dts.length; i++) {
                    var dd = dts[i].nextElementSibling;
                    if (dd && dd.tagName === 'DD') {
                        var key = dts[i].textContent.trim().replace(/:$/, '');
                        var val = dd.textContent.trim();
                        if (key && val) {
                            results.push({attrName: key, attrValue: val});
                        }
                    }
                }
            }

            // Method 4: Any key-value looking pairs in spec-like containers
            if (results.length === 0) {
                var containers = document.querySelectorAll(
                    '[class*="specification"], [class*="Specification"], ' +
                    '[class*="product-prop"], [class*="ProductProp"], ' +
                    '[class*="attr"], [class*="Attr"]'
                );
                for (var c = 0; c < containers.length; c++) {
                    var divs = containers[c].querySelectorAll('div, span, p');
                    for (var i = 0; i < divs.length; i++) {
                        var text = divs[i].textContent.trim();
                        if (text.includes(':') && text.length < 200) {
                            var parts = text.split(':');
                            var key = parts[0].trim();
                            var val = parts.slice(1).join(':').trim();
                            if (key && val && key.length < 50 && val.length < 150) {
                                results.push({attrName: key, attrValue: val});
                            }
                        }
                    }
                }
            }

            return results;
        }''')

        if specs and len(specs) > 0:
            print(f"[scraper]    ✅ {len(specs)} specs from DOM")
            return map_props_to_fields(specs)

    except Exception as e:
        print(f"[scraper]    ⚠️  DOM spec error: {e}")

    return {}, {}


def get_description_from_dom(page) -> str:
    """
    Extract description using multiple strategies:
    1. Recursive leaf-text extraction from known containers
    2. Iframe content
    3. Large text blocks
    """

    # ── STRATEGY 1: JavaScript-based recursive extraction ───────────────
    try:
        desc = page.evaluate('''() => {
            // All possible description selectors
            var selectors = [
                "#product-description",
                '[class*="product-description"]',
                '[class*="detail-content"]',
                '.product-detail__description',
                '[class*="productDescription"]',
                '[class*="desc-content"]',
                '#nav-description',
                '[class*="ItemDescription"]',
                'div[data-pl="product-description"]',
                '.description-content',
                'div.richTextContainer[data-rich-text-render="true"]',
                'div.detailmodule_text',
                '[class*="detail-desc"]',
                '[class*="product-detail-info"]',
                '[class*="product_desc"]',
                '[class*="pdp-description"]',
                '[class*="module_description"]',
                '[class*="DescriptionModule"]',
                '[class*="desc_"]',
                '[class*="Description"]',
                '[data-role="description"]',
                '[data-widget="description"]',
            ];

            function getLeafText(node) {
                if (!node) return '';
                var tag = node.tagName ? node.tagName.toLowerCase() : '';
                if (['script', 'style', 'noscript', 'svg', 'iframe', 'button', 'input'].includes(tag)) {
                    return '';
                }
                if (node.nodeType === Node.TEXT_NODE) {
                    return (node.textContent || '').trim();
                }
                var childElements = node.children;
                if (!childElements || childElements.length === 0) {
                    return (node.textContent || '').trim();
                }
                var texts = [];
                for (var i = 0; i < node.childNodes.length; i++) {
                    var childText = getLeafText(node.childNodes[i]);
                    if (childText) {
                        texts.push(childText);
                    }
                }
                return texts.join(' ');
            }

            for (var s = 0; s < selectors.length; s++) {
                try {
                    var el = document.querySelector(selectors[s]);
                    if (!el) continue;
                    var text = getLeafText(el);
                    text = text.replace(/\\s+/g, ' ').trim();
                    // Remove boilerplate
                    text = text.replace(/^.*?Description\\s+report\\s+/i, '');
                    text = text.replace(/(Smarter Shopping|Better Living).*/i, '');
                    text = text.trim();
                    if (text.length > 50) {
                        return text.substring(0, 2000);
                    }
                } catch(e) {}
            }

            return '';
        }''')

        if desc and len(desc) > 50:
            print(f"[scraper]    ✅ Description from DOM selectors ({len(desc)} chars)")
            return desc
    except Exception as e:
        print(f"[scraper]    ⚠️  DOM desc strategy 1 error: {e}")

    # ── STRATEGY 2: Iframe content ──────────────────────────────────────
    try:
        frames = page.frames
        for frame in frames:
            if frame == page.main_frame:
                continue
            try:
                frame_url = frame.url or ''
                if ('desc' in frame_url.lower() or
                    'description' in frame_url.lower() or
                    'detail' in frame_url.lower()):
                    body_text = frame.evaluate('''() => {
                        return document.body ? document.body.innerText : '';
                    }''')
                    if body_text and len(body_text.strip()) > 50:
                        text = re.sub(r'\s+', ' ', body_text).strip()
                        print(f"[scraper]    ✅ Description from iframe ({len(text)} chars)")
                        return text[:2000]
            except Exception:
                continue

        # Also try all frames regardless of URL
        for frame in frames:
            if frame == page.main_frame:
                continue
            try:
                body_text = frame.evaluate('''() => {
                    var text = document.body ? document.body.innerText : '';
                    return text;
                }''')
                if body_text and len(body_text.strip()) > 100:
                    text = re.sub(r'\s+', ' ', body_text).strip()
                    # Filter out tiny frames / navigation frames
                    if len(text) > 100 and 'sign in' not in text.lower()[:50]:
                        print(f"[scraper]    ✅ Description from sub-frame ({len(text)} chars)")
                        return text[:2000]
            except Exception:
                continue
    except Exception as e:
        print(f"[scraper]    ⚠️  Iframe strategy error: {e}")

    # ── STRATEGY 3: Find the largest text block on the page ─────────────
    try:
        desc = page.evaluate('''() => {
            var allDivs = document.querySelectorAll('div');
            var best = '';
            var bestLen = 0;

            for (var i = 0; i < allDivs.length; i++) {
                var div = allDivs[i];
                var cl = (div.className || '').toLowerCase();
                var id = (div.id || '').toLowerCase();

                // Must be related to description/detail
                if (cl.includes('desc') || cl.includes('detail') ||
                    cl.includes('content') || id.includes('desc') ||
                    id.includes('detail')) {

                    // Get direct text (not from deeply nested children that are other sections)
                    var text = div.innerText || '';
                    text = text.replace(/\\s+/g, ' ').trim();

                    // Skip if too short or contains navigation text
                    if (text.length > 100 && text.length > bestLen &&
                        !text.includes('Add to Cart') &&
                        !text.includes('Buy Now') &&
                        text.indexOf('Sign in') !== 0) {
                        best = text;
                        bestLen = text.length;
                    }
                }
            }

            return best ? best.substring(0, 2000) : '';
        }''')

        if desc and len(desc) > 100:
            desc = re.sub(r'^.*?Description\s+report\s+', '', desc,
                          flags=re.IGNORECASE | re.DOTALL)
            desc = re.sub(r'(Smarter Shopping|Better Living).*$', '', desc,
                          flags=re.IGNORECASE)
            desc = desc.strip()
            if len(desc) > 50:
                print(f"[scraper]    ✅ Description from largest block ({len(desc)} chars)")
                return desc[:2000]
    except Exception as e:
        print(f"[scraper]    ⚠️  Large block strategy error: {e}")

    return ''


def get_price_from_dom(page) -> str:
    """Extract price from DOM."""
    try:
        price = page.evaluate('''() => {
            // Method 1: Price-specific elements
            var priceEls = document.querySelectorAll(
                '[class*="price"] span, [class*="Price"] span, ' +
                '[data-pl="product-price"], [class*="uniform-banner-box-price"]'
            );
            for (var i = 0; i < priceEls.length; i++) {
                var text = priceEls[i].textContent.trim();
                if (/[\\d.,]+/.test(text) && (text.includes('$') || text.includes('€') ||
                    text.includes('£') || text.includes('US') || /^\\d/.test(text))) {
                    return text;
                }
            }

            // Method 2: Meta tag
            var meta = document.querySelector('meta[property="product:price:amount"]');
            if (meta && meta.content) {
                var currency = document.querySelector('meta[property="product:price:currency"]');
                var curr = currency ? currency.content : 'USD';
                return meta.content + ' ' + curr;
            }

            return '';
        }''')
        return price or ''
    except Exception:
        return ''


# ─────────────────────────────────────────────────────────────────────────────
# NETWORK INTERCEPTION (kept as supplementary, not primary)
# ─────────────────────────────────────────────────────────────────────────────

def parse_pdp_response(text: str) -> dict | None:
    try:
        text = text.strip()
        m = re.match(r'^[a-zA-Z_$][a-zA-Z0-9_$]*\s*\((.*)\)\s*;?\s*$', text, re.DOTALL)
        if m:
            text = m.group(1)

        data   = json.loads(text)
        result = (data.get('data', {}) or {}).get('result', {}) or {}

        if not result:
            return None

        extracted = {}
        _parse_generic_json(result, extracted)
        return extracted if (extracted.get('title') or extracted.get('image_1')) else None

    except (json.JSONDecodeError, Exception):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    captured_pdp = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                ]
            )
            context = browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/125.0.0.0 Safari/537.36'
                ),
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='Asia/Karachi',
            )

            # Block unnecessary resources for speed
            def route_handler(route):
                resource_type = route.request.resource_type
                url_r = route.request.url
                if resource_type in ['font', 'media']:
                    route.abort()
                elif resource_type == 'image' and 'alicdn' not in url_r:
                    route.abort()
                else:
                    route.continue_()

            context.route("**/*", route_handler)

            page = context.new_page()

            # Stealth: remove webdriver flag
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                window.chrome = { runtime: {} };
            """)

            def handle_response(response):
                url_r = response.url
                if ('mtop.aliexpress.pdp.pc.query' in url_r or
                        'mtop.aliexpress.itemdetail.pc' in url_r):
                    try:
                        body = response.body()
                        if len(body) > 1000:
                            text = body.decode('utf-8', errors='replace')
                            captured_pdp.append(text)
                            print(f"[scraper]    📡 Captured PDP API: {url_r[:80]}")
                    except Exception:
                        pass

            page.on('response', handle_response)

            print(f'\n[scraper] 🌐 Opening: {url}')
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(4000)

            # ── SCROLL THE ENTIRE PAGE to trigger all lazy loading ──────
            print("[scraper]    📜 Scrolling page to load all content...")
            for i in range(8):
                page.mouse.wheel(0, 600)
                page.wait_for_timeout(600)

            # Scroll back up and down once more
            page.evaluate('window.scrollTo(0, 0)')
            page.wait_for_timeout(500)
            for i in range(10):
                page.mouse.wheel(0, 500)
                page.wait_for_timeout(400)

            # Wait for content to settle
            page.wait_for_timeout(2000)

            extracted = {}

            # ── SOURCE 1: Network-intercepted API responses ─────────────
            print(f'[scraper]    📦 Captured {len(captured_pdp)} API responses')
            for resp_text in captured_pdp:
                parsed = parse_pdp_response(resp_text)
                if parsed:
                    print(f'[scraper]    ✅ Parsed {len(parsed)} fields from API')
                    for k, v in parsed.items():
                        if k not in extracted or not extracted[k]:
                            extracted[k] = v

            # ── SOURCE 2: Embedded script tag JSON ──────────────────────
            print("[scraper]    🔍 Extracting from embedded scripts...")
            script_data = extract_from_page_scripts(page)
            for k, v in script_data.items():
                if k.startswith('_'):
                    continue
                if k not in extracted or not extracted[k]:
                    extracted[k] = v

            # ── SOURCE 3: DOM fallbacks ─────────────────────────────────
            if not extracted.get('title'):
                print("[scraper]    🔍 Title from DOM...")
                extracted['title'] = get_title_from_dom(page)

            if not extracted.get('image_1'):
                print("[scraper]    🔍 Images from DOM...")
                extracted.update(get_images_from_dom(page))

            if not extracted.get('_specs_found'):
                print("[scraper]    🔍 Specs from DOM...")
                mapped, raw = get_specs_from_dom(page)
                if mapped:
                    for k, v in mapped.items():
                        if k not in extracted or not extracted[k]:
                            extracted[k] = v
                    extracted['_raw_specs'] = raw

            if not extracted.get('description'):
                print("[scraper]    🔍 Description from DOM...")
                desc = get_description_from_dom(page)
                if desc:
                    extracted['description'] = desc

            if not extracted.get('price'):
                print("[scraper]    🔍 Price from DOM...")
                price = get_price_from_dom(page)
                if price:
                    extracted['price'] = price

            # ── LAST RESORT: Scroll more and try description again ──────
            if not extracted.get('description'):
                print("[scraper]    📜 Extra scroll for description...")
                page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                page.wait_for_timeout(3000)

                # Try clicking "View More" / "See full description" buttons
                try:
                    for btn_text in ['View More', 'See More', 'Show More',
                                     'View Full', 'Read More', 'Description']:
                        btns = page.locator(f'button:has-text("{btn_text}"), '
                                            f'a:has-text("{btn_text}"), '
                                            f'span:has-text("{btn_text}"), '
                                            f'div:has-text("{btn_text}")')
                        if btns.count() > 0:
                            try:
                                btns.first.click(timeout=2000)
                                page.wait_for_timeout(2000)
                                print(f"[scraper]       Clicked '{btn_text}' button")
                                break
                            except Exception:
                                continue
                except Exception:
                    pass

                desc = get_description_from_dom(page)
                if desc:
                    extracted['description'] = desc

            browser.close()

        # Clean up internal keys
        extracted.pop('_specs_found', None)
        extracted.pop('_raw_specs', None)

        if not extracted.get('title'):
            print('[scraper] ❌ No title — aborting')
            return None

        # Summary
        core = ['brand', 'color', 'dimensions', 'weight', 'material',
                'certifications', 'country_of_origin', 'warranty', 'product_type']
        seller_fields = ['store_name', 'store_id', 'seller_positive_rate',
                         'seller_rating', 'seller_country', 'store_open_date']

        print(f'\n[scraper] ✅ Extraction complete')
        print(f'[scraper]    Title      : {extracted.get("title", "")[:70]}')
        print(f'[scraper]    Desc       : {len(extracted.get("description", ""))} chars')
        print(f'[scraper]    Core specs : {[k for k in core if extracted.get(k)]}')
        print(f'[scraper]    Seller     : {[k for k in seller_fields if extracted.get(k)]}')
        print(f'[scraper]    Images     : {sum(1 for i in range(1,21) if extracted.get(f"image_{i}"))}')

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
            # Seller defaults
            'store_name': '', 'store_id': '', 'store_url': '',
            'seller_id': '', 'seller_positive_rate': '', 'seller_rating': '',
            'seller_communication': '', 'seller_shipping_speed': '',
            'seller_country': '', 'store_open_date': '', 'seller_level': '',
            'seller_total_reviews': '', 'seller_positive_num': '',
            'is_top_rated': '',
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


if __name__ == '__main__':
    test_url = 'https://www.aliexpress.com/item/1005010388288135.html'
    result   = get_product_info(test_url)
    if result:
        print('\n' + '='*80)
        for k, v in result.items():
            if v and not k.startswith('image_'):
                print(f'  {k:30s}: {str(v)[:100]}')
        print(f'  {"images":30s}: {sum(1 for i in range(1,21) if result.get(f"image_{i}"))}')
    else:
        print('FAILED')
