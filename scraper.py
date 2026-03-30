"""
scraper.py
──────────
Captures the mtop.aliexpress.pdp.pc.query API response.
Extracts: specs, images, title, price, description, AND seller info.

Seller info comes from: result.SHOP_CARD_PC (confirmed from debug output)
  - store_name          : SHOP_CARD_PC.storeName
  - store_id            : SHOP_CARD_PC.sellerInfo.storeNum
  - store_url           : SHOP_CARD_PC.sellerInfo.storeURL
  - seller_id           : SHOP_CARD_PC.sellerInfo.adminSeq
  - seller_positive_rate: SHOP_CARD_PC.sellerPositiveRate
  - seller_rating       : benefitInfoList[title=store rating].value
  - seller_country      : SHOP_CARD_PC.sellerInfo.countryCompleteName
  - store_open_date     : SHOP_CARD_PC.sellerInfo.formatOpenTime
  - seller_level        : SHOP_CARD_PC.sellerLevel
  - seller_total_reviews: SHOP_CARD_PC.sellerTotalNum

RULE: Seller info is NEVER sent to LLM. Always stored as original scraped values.
"""

from playwright.sync_api import sync_playwright
import re
import json
import urllib.request


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
# Path confirmed from debug: result.SHOP_CARD_PC
# ─────────────────────────────────────────────────────────────────────────────

def extract_seller_info(result: dict) -> dict:
    """
    Extract seller/store information from SHOP_CARD_PC.
    Returns original values — never modified by LLM.
    """
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
        if not shop:
            print("[scraper]    ⚠️  SHOP_CARD_PC not found in API response")
            return seller

        # Store name
        seller['store_name'] = str(shop.get('storeName', '') or '')

        # Seller level
        seller['seller_level'] = str(shop.get('sellerLevel', '') or '')

        # Positive rate & review counts
        seller['seller_positive_rate'] = str(shop.get('sellerPositiveRate', '') or '')
        seller['seller_total_reviews'] = str(shop.get('sellerTotalNum', '') or '')
        seller['seller_positive_num']  = str(shop.get('sellerPositiveNum', '') or '')

        # Detailed ratings from benefitInfoList
        benefit_list = shop.get('benefitInfoList', []) or []
        for item in benefit_list:
            title = str(item.get('title', '') or '').lower().strip()
            value = str(item.get('value', '') or '').strip()
            if 'store rating' in title:
                seller['seller_rating'] = value
            elif 'communication' in title:
                seller['seller_communication'] = value
            elif 'positive' in title:
                # Catch variants like "positive feedback rate"
                if not seller['seller_positive_rate']:
                    seller['seller_positive_rate'] = value

        # sellerInfo sub-object
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

        # Also try GLOBAL_DATA for store name fallback
        if not seller['store_name']:
            try:
                seller['store_name'] = str(
                    result['GLOBAL_DATA']['globalData']['storeName'] or ''
                )
            except (KeyError, TypeError):
                pass

        # Store ID fallback from GLOBAL_DATA
        if not seller['store_id']:
            try:
                seller['store_id'] = str(
                    result['GLOBAL_DATA']['globalData']['storeId'] or ''
                )
            except (KeyError, TypeError):
                pass

        print(f"[scraper]    ✅ Seller info extracted:")
        for k, v in seller.items():
            if v:
                print(f"[scraper]       {k}: {v}")

    except Exception as e:
        print(f"[scraper]    ⚠️  Seller info extraction error: {e}")

    return seller


# ─────────────────────────────────────────────────────────────────────────────
# PRICE EXTRACTOR
# Primary path: result['PRICE'] with SKU map logic
# Fallback: result['priceModule'] / result['PRICE_MODULE']
# ─────────────────────────────────────────────────────────────────────────────

def extract_price(result: dict) -> str:
    """
    Extract price using the PRICE.skuIdStrPriceInfoMap first (v2 API),
    then fall back to legacy priceModule fields.
    """
    try:
        price_module = result.get('PRICE', {}) or {}

        # Try skuIdStrPriceInfoMap for actual price (confirmed path from aliexpress.us)
        sku_map = price_module.get('skuIdStrPriceInfoMap', {})
        if sku_map:
            first_sku = next(iter(sku_map.values()), {})
            act = first_sku.get('actSkuCalPrice') or first_sku.get('actSkuMultiCurrencyCalPrice', '')
            reg = first_sku.get('skuCalPrice') or ''
            price = act or reg
            if price:
                return f"${price}"

    except Exception as e:
        print(f"[scraper]    ⚠️  PRICE SKU map extraction error: {e}")

    # Fallback: legacy priceModule / PRICE_MODULE
    try:
        pm = result.get('priceModule', {}) or result.get('PRICE_MODULE', {}) or {}
        price = (pm.get('formatedActivityPrice') or
                 pm.get('formatedPrice') or
                 pm.get('minActivityAmount', {}).get('formatedAmount', '') or '')
        if price:
            return str(price)
    except Exception as e:
        print(f"[scraper]    ⚠️  Legacy price extraction error: {e}")

    return ''


# ─────────────────────────────────────────────────────────────────────────────
# DESCRIPTION FETCHER
# Primary: DESC['nativeDescUrl'] / DESC['pcDescUrl'] — fetch the URL directly
# Fallback: descriptionModule / DESCRIPTION_PC in API JSON, then DOM strategies
# ─────────────────────────────────────────────────────────────────────────────

def fetch_description(result: dict) -> str:
    """
    Fetch description by requesting the URL found in DESC.nativeDescUrl or
    DESC.pcDescUrl. Falls back to inline JSON fields if the URL is absent.
    """
    try:
        desc_module = result.get('DESC', {}) or {}
        desc_url = (desc_module.get('nativeDescUrl') or
                    desc_module.get('pcDescUrl') or '')

        if not desc_url:
            return ''

        print(f"[scraper]    📥 Fetching description from: {desc_url[:80]}")

        req = urllib.request.Request(
            desc_url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        # nativeDescUrl → JSON payload
        if 'desc.json' in desc_url:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode('utf-8', errors='replace')
                data = json.loads(raw)
                desc_text = _parse_desc_json(data)
                if desc_text:
                    print(f"[scraper]    ✅ Description fetched from JSON URL: {len(desc_text)} chars")
                    return desc_text[:2000]

        # pcDescUrl → HTML
        elif 'desc.htm' in desc_url:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode('utf-8', errors='replace')
                clean = re.sub(r'<[^>]+>', ' ', raw)
                clean = ' '.join(clean.split()).strip()
                if len(clean) > 50:
                    print(f"[scraper]    ✅ Description fetched from HTML URL: {len(clean)} chars")
                    return clean[:2000]

    except Exception as e:
        print(f"[scraper]    ⚠️  Description URL fetch error: {e}")

    return ''


def _parse_desc_json(data) -> str:
    """Recursively extract text from AliExpress description JSON."""
    if isinstance(data, str):
        clean = re.sub(r'<[^>]+>', ' ', data)
        clean = ' '.join(clean.split()).strip()
        return clean if len(clean) > 10 else ''

    if isinstance(data, dict):
        for key in ['text', 'content', 'value', 'description', 'html']:
            val = data.get(key)
            if isinstance(val, str) and len(val) > 10:
                clean = re.sub(r'<[^>]+>', ' ', val)
                clean = ' '.join(clean.split()).strip()
                if len(clean) > 10:
                    return clean
        texts = []
        for v in data.values():
            t = _parse_desc_json(v)
            if t:
                texts.append(t)
        return ' '.join(texts)[:2000] if texts else ''

    if isinstance(data, list):
        texts = []
        for item in data:
            t = _parse_desc_json(item)
            if t:
                texts.append(t)
        return ' '.join(texts)[:2000] if texts else ''

    return ''


# ─────────────────────────────────────────────────────────────────────────────
# MAIN API RESPONSE PARSER
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
            print("[scraper]    ⚠️  Empty result in API response")
            return None

        extracted = {}

        # ── Title ──────────────────────────────────────────────────────────
        try:
            extracted['title'] = result['GLOBAL_DATA']['globalData']['subject']
        except (KeyError, TypeError):
            pass
        if not extracted.get('title'):
            try:
                extracted['title'] = result['titleModule']['subject']
            except (KeyError, TypeError):
                pass
        if extracted.get('title'):
            extracted['title'] = str(extracted['title']).strip()
            print(f"[scraper]    ✅ Title: {extracted['title'][:70]}")

        # ── Specifications ─────────────────────────────────────────────────
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
            print(f"[scraper]    ✅ Found {len(props)} spec props in API JSON")
            mapped, raw = map_props_to_fields(props)
            extracted.update(mapped)
            for k, v in raw.items():
                print(f"[scraper]       {k}: {v[:60]}")
        else:
            print("[scraper]    ⚠️  No specs found in PRODUCT_PROP_PC")

        # ── Seller Info ────────────────────────────────────────────────────
        seller_info = extract_seller_info(result)
        extracted.update(seller_info)

        # ── Images ─────────────────────────────────────────────────────────
        # Primary: HEADER_IMAGE_PC (confirmed path, v2 API)
        images = []
        try:
            images = result['HEADER_IMAGE_PC']['imagePathList'] or []
            if images:
                print(f"[scraper]    ✅ Images from HEADER_IMAGE_PC: {len(images)}")
        except (KeyError, TypeError):
            pass

        # Fallback: imageModule (legacy path)
        if not images:
            try:
                images = result['imageModule']['imagePathList'] or []
                if images:
                    print(f"[scraper]    ✅ Images from imageModule: {len(images)}")
            except (KeyError, TypeError):
                pass

        # Deep search fallback
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
                if img.startswith('//'):
                    img = 'https:' + img
                elif not img.startswith('http'):
                    img = 'https://' + img
                extracted[f'image_{idx}'] = re.sub(r'_\d+x\d+', '', img)

        # ── Price ──────────────────────────────────────────────────────────
        price = extract_price(result)
        if price:
            extracted['price'] = price
            print(f"[scraper]    ✅ Price: {price}")

        # ── Description — try DESC URL first, then inline JSON fields ──────
        desc = fetch_description(result)
        if desc:
            extracted['description'] = desc
        else:
            # Fallback A: descriptionModule in API JSON
            try:
                dm   = result.get('descriptionModule', {}) or {}
                desc = dm.get('description', '') or dm.get('content', '')
                if desc and len(desc) > 50:
                    desc = re.sub(r'<[^>]+>', ' ', desc)
                    extracted['description'] = ' '.join(desc.split())[:2000]
                    print(f"[scraper]    ✅ Description from descriptionModule")
            except Exception:
                pass

            # Fallback B: DESCRIPTION_PC in API JSON
            if not extracted.get('description'):
                try:
                    desc_pc = result.get('DESCRIPTION_PC', {}) or {}
                    desc_html = (desc_pc.get('descriptionContent', '') or
                                 desc_pc.get('content', '') or
                                 desc_pc.get('description', '') or
                                 desc_pc.get('html', ''))
                    if desc_html and len(desc_html) > 50:
                        desc_clean = re.sub(r'<[^>]+>', ' ', desc_html)
                        extracted['description'] = ' '.join(desc_clean.split())[:2000]
                        print(f"[scraper]    ✅ Description from DESCRIPTION_PC in API")
                except Exception:
                    pass

            # Fallback C: deep search in API JSON
            if not extracted.get('description'):
                try:
                    desc = _deep_find_description(result)
                    if desc and len(desc) > 50:
                        extracted['description'] = desc[:2000]
                        print(f"[scraper]    ✅ Description from deep search in API JSON")
                except Exception:
                    pass

        return extracted if (extracted.get('title') or extracted.get('image_1')) else None

    except json.JSONDecodeError as e:
        print(f"[scraper]    ⚠️  JSON parse error: {e}")
        return None
    except Exception as e:
        print(f"[scraper]    ⚠️  Parse error: {e}")
        return None


def _deep_find_description(obj, depth=0):
    """Recursively search for description content in nested JSON."""
    if depth > 6:
        return None
    if isinstance(obj, dict):
        for key in ['description', 'descriptionContent', 'content', 'detailDesc',
                    'productDescription', 'descContent', 'richTextDesc']:
            val = obj.get(key)
            if isinstance(val, str) and len(val) > 50:
                cleaned = re.sub(r'<[^>]+>', ' ', val)
                cleaned = ' '.join(cleaned.split()).strip()
                if len(cleaned) > 50:
                    return cleaned
        for v in obj.values():
            result = _deep_find_description(v, depth + 1)
            if result:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _deep_find_description(item, depth + 1)
            if result:
                return result
    return None


# ─────────────────────────────────────────────────────────────────────────────
# DOM FALLBACKS
# ─────────────────────────────────────────────────────────────────────────────

def get_title_from_dom(page) -> str:
    for selector in ['meta[property="og:title"]', 'h1[data-pl="product-title"]']:
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
    images = {}
    try:
        for script in page.locator('script').all():
            try:
                txt = script.text_content() or ''
                m   = re.search(r'"imagePathList"\s*:\s*\[(.*?)\]', txt, re.DOTALL)
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
    try:
        og = page.locator('meta[property="og:image"]').get_attribute('content')
        if og:
            images['image_1'] = og
    except Exception:
        pass
    return images


# ─────────────────────────────────────────────────────────────────────────────
# DESCRIPTION EXTRACTION — MULTI-STRATEGY DOM FALLBACK
# ─────────────────────────────────────────────────────────────────────────────

def _clean_description(text: str) -> str:
    """Clean extracted description text."""
    if not text:
        return ''
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^.*?Description\s+report\s+', '', text,
                  flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'(Smarter Shopping|Better Living).*$', '', text,
                  flags=re.IGNORECASE)
    text = text.strip()
    return text


def get_description_from_dom(page) -> str:
    """
    5-strategy description extraction:
      1. Broad CSS selectors + recursive leaf-text via JS
      2. Iframe content (AliExpress loads desc in iframes)
      3. All sub-frames regardless of URL
      4. Script tag regex for description HTML strings
      5. Largest desc/detail block on page
    """

    # ── STRATEGY 1: CSS selectors + recursive leaf-text extraction ──────
    print("[scraper]    🔍 Desc Strategy 1: CSS selectors + leaf-text...")
    try:
        desc = page.evaluate('''() => {
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
                '[class*="product_desc"]',
                '[class*="pdp-description"]',
                '[class*="module_description"]',
                '[class*="DescriptionModule"]',
                '[class*="desc_"]',
                '[class*="Description"]',
                '[data-role="description"]',
                '[data-widget="description"]',
                '[class*="product-detail-info"]',
            ];

            function getLeafText(node) {
                if (!node) return '';
                var tag = node.tagName ? node.tagName.toLowerCase() : '';
                if (['script','style','noscript','svg','iframe','button','input','select','textarea'].indexOf(tag) !== -1) {
                    return '';
                }
                if (node.nodeType === 3) {
                    return (node.textContent || '').trim();
                }
                if (!node.children || node.children.length === 0) {
                    return (node.textContent || '').trim();
                }
                var texts = [];
                for (var i = 0; i < node.childNodes.length; i++) {
                    var t = getLeafText(node.childNodes[i]);
                    if (t) texts.push(t);
                }
                return texts.join(' ');
            }

            for (var s = 0; s < selectors.length; s++) {
                try {
                    var els = document.querySelectorAll(selectors[s]);
                    for (var e = 0; e < els.length; e++) {
                        var text = getLeafText(els[e]);
                        if (text && text.length > 50) {
                            return JSON.stringify({text: text.substring(0, 2500), selector: selectors[s]});
                        }
                    }
                } catch(err) {}
            }
            return '';
        }''')

        if desc:
            try:
                parsed = json.loads(desc)
                cleaned = _clean_description(parsed.get('text', ''))
                if len(cleaned) > 50 and 'Smarter Shopping' not in cleaned:
                    print(f"[scraper]    ✅ Description via selector: {parsed.get('selector','')} ({len(cleaned)} chars)")
                    return cleaned[:2000]
            except json.JSONDecodeError:
                cleaned = _clean_description(desc)
                if len(cleaned) > 50:
                    return cleaned[:2000]
    except Exception as e:
        print(f"[scraper]       Strategy 1 error: {e}")

    # ── STRATEGY 2: Iframe with desc-related URL ────────────────────────
    print("[scraper]    🔍 Desc Strategy 2: Iframes (desc URL)...")
    try:
        for frame in page.frames:
            if frame == page.main_frame:
                continue
            try:
                frame_url = frame.url or ''
                if any(kw in frame_url.lower() for kw in ['desc', 'description', 'detail']):
                    body_text = frame.evaluate('''() => {
                        if (!document.body) return '';
                        var text = document.body.innerText || '';
                        return text.substring(0, 2500);
                    }''')
                    cleaned = _clean_description(body_text)
                    if len(cleaned) > 50:
                        print(f"[scraper]    ✅ Description from iframe: {frame_url[:60]} ({len(cleaned)} chars)")
                        return cleaned[:2000]
            except Exception:
                continue
    except Exception as e:
        print(f"[scraper]       Strategy 2 error: {e}")

    # ── STRATEGY 3: ALL sub-frames (some iframes have no desc in URL) ───
    print("[scraper]    🔍 Desc Strategy 3: All sub-frames...")
    try:
        for frame in page.frames:
            if frame == page.main_frame:
                continue
            try:
                body_text = frame.evaluate('''() => {
                    if (!document.body) return '';
                    var text = document.body.innerText || '';
                    return text.substring(0, 2500);
                }''')
                cleaned = _clean_description(body_text)
                if (len(cleaned) > 100 and
                    'sign in' not in cleaned.lower()[:50] and
                    'Smarter Shopping' not in cleaned and
                    'cookie' not in cleaned.lower()[:50]):
                    print(f"[scraper]    ✅ Description from sub-frame ({len(cleaned)} chars)")
                    return cleaned[:2000]
            except Exception:
                continue
    except Exception as e:
        print(f"[scraper]       Strategy 3 error: {e}")

    # ── STRATEGY 4: Regex description from script tags ──────────────────
    print("[scraper]    🔍 Desc Strategy 4: Script tag regex...")
    try:
        desc_from_script = page.evaluate('''() => {
            var scripts = document.querySelectorAll('script');
            var patterns = [
                /"description"\s*:\s*"((?:[^"\\\\]|\\\\.){50,})"/,
                /"descriptionContent"\s*:\s*"((?:[^"\\\\]|\\\\.){50,})"/,
                /"content"\s*:\s*"((?:[^"\\\\]|\\\\.){50,})"/,
                /"detailDesc"\s*:\s*"((?:[^"\\\\]|\\\\.){50,})"/,
                /"richTextDesc"\s*:\s*"((?:[^"\\\\]|\\\\.){50,})"/,
            ];
            for (var i = 0; i < scripts.length; i++) {
                var text = scripts[i].textContent || '';
                if (text.length < 200) continue;
                for (var p = 0; p < patterns.length; p++) {
                    var match = text.match(patterns[p]);
                    if (match && match[1]) {
                        try {
                            var decoded = JSON.parse('"' + match[1] + '"');
                            var div = document.createElement('div');
                            div.innerHTML = decoded;
                            var clean = div.innerText || div.textContent || '';
                            if (clean.length > 50) {
                                return clean.substring(0, 2500);
                            }
                        } catch(e) {
                            var stripped = match[1].replace(/<[^>]+>/g, ' ').replace(/\\s+/g, ' ').trim();
                            if (stripped.length > 50) {
                                return stripped.substring(0, 2500);
                            }
                        }
                    }
                }
            }
            return '';
        }''')

        cleaned = _clean_description(desc_from_script)
        if len(cleaned) > 50:
            print(f"[scraper]    ✅ Description from script regex ({len(cleaned)} chars)")
            return cleaned[:2000]
    except Exception as e:
        print(f"[scraper]       Strategy 4 error: {e}")

    # ── STRATEGY 5: Largest text block with desc/detail class/id ────────
    print("[scraper]    🔍 Desc Strategy 5: Largest desc/detail block...")
    try:
        desc = page.evaluate('''() => {
            var allDivs = document.querySelectorAll('div, section, article');
            var best = '';
            var bestLen = 0;

            for (var i = 0; i < allDivs.length; i++) {
                var el = allDivs[i];
                var cl = (el.className || '').toString().toLowerCase();
                var id = (el.id || '').toLowerCase();

                var isDescRelated = (
                    cl.indexOf('desc') !== -1 || cl.indexOf('detail') !== -1 ||
                    cl.indexOf('content') !== -1 || cl.indexOf('product-info') !== -1 ||
                    id.indexOf('desc') !== -1 || id.indexOf('detail') !== -1 ||
                    id.indexOf('product-description') !== -1
                );

                if (!isDescRelated) continue;

                var text = (el.innerText || '').replace(/\\s+/g, ' ').trim();

                if (text.length > 100 && text.length > bestLen &&
                    text.indexOf('Add to Cart') === -1 &&
                    text.indexOf('Buy Now') === -1 &&
                    text.indexOf('Smarter Shopping') === -1 &&
                    text.indexOf('Sign in') !== 0) {
                    best = text;
                    bestLen = text.length;
                }
            }
            return best ? best.substring(0, 2500) : '';
        }''')

        cleaned = _clean_description(desc)
        if len(cleaned) > 50:
            print(f"[scraper]    ✅ Description from largest block ({len(cleaned)} chars)")
            return cleaned[:2000]
    except Exception as e:
        print(f"[scraper]       Strategy 5 error: {e}")

    print("[scraper]    ⚠️  All 5 description strategies failed")
    return ''


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    captured_pdp = []
    captured_desc_api = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage',
                      '--disable-blink-features=AutomationControlled']
            )
            context = browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/124.0.0.0 Safari/537.36'
                ),
                viewport={'width': 1366, 'height': 900},
                locale='en-GB',
                timezone_id='Europe/London',
                extra_http_headers={'Accept-Language': 'en-GB,en;q=0.9'},
            )

            # Mask automation signals
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            """)

            page = context.new_page()

            def handle_response(response):
                url_r = response.url

                # PDP API interception — only keep real data responses
                if ('mtop.aliexpress.pdp.pc.query' in url_r or
                        'mtop.aliexpress.itemdetail.pc' in url_r):
                    try:
                        body = response.body()
                        if len(body) > 1000:
                            text = body.decode('utf-8', errors='replace')
                            # Guard: skip token-error / empty responses
                            if ('GLOBAL_DATA' in text or
                                    'HEADER_IMAGE_PC' in text or
                                    'PRODUCT_PROP_PC' in text or
                                    'SHOP_CARD_PC' in text):
                                captured_pdp.append(text)
                                print(f"[scraper]    📡 Captured PDP: {url_r[:80]}")
                    except Exception:
                        pass

                # Description API interception (lazy-loaded iframe content)
                if any(kw in url_r.lower() for kw in [
                    'description', 'desc', 'detail.html',
                    'pdp/description', 'product/description',
                    'aeglobal/product', 'descriptionmodule',
                ]):
                    try:
                        body = response.body()
                        if len(body) > 100:
                            text = body.decode('utf-8', errors='replace')
                            if ('<' in text and len(text) > 200) or '"desc' in text.lower():
                                captured_desc_api.append(text)
                                print(f"[scraper]    📡 Captured desc API: {url_r[:80]}")
                    except Exception:
                        pass

            # page.on('response', handle_response)
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(8000)
            page.mouse.wheel(0, 300)
            page.wait_for_timeout(2000)
            page.mouse.wheel(0, 600)
            page.wait_for_timeout(2000)
            page.mouse.wheel(0, 800)
            page.wait_for_timeout(3000)

            extracted = {}
            print(f'[scraper]    📦 Captured {len(captured_pdp)} PDP responses')

            for resp_text in captured_pdp:
                parsed = parse_pdp_response(resp_text)
                if parsed:
                    print(f'[scraper]    ✅ Parsed {len(parsed)} fields from API')
                    for k, v in parsed.items():
                        if k not in extracted or not extracted[k]:
                            extracted[k] = v

            # DOM fallbacks for title and images
            if not extracted.get('title'):
                extracted['title'] = get_title_from_dom(page)
            if not extracted.get('image_1'):
                extracted.update(get_images_from_dom(page))

            # Price DOM fallback
            if not extracted.get('price'):
                try:
                    for script in page.locator('script').all():
                        txt = script.text_content() or ''
                        m   = re.search(r'"price"\s*:\s*"([^"]+)"', txt)
                        if m:
                            extracted['price'] = m.group(1)
                            break
                except Exception:
                    pass

            # ─────────────────────────────────────────────────────────────
            # DESCRIPTION — MULTI-STRATEGY PIPELINE
            # ─────────────────────────────────────────────────────────────

            if not extracted.get('description'):
                print("\n[scraper]    📝 Starting description extraction pipeline...")

                # Step A: Try captured description API responses
                if captured_desc_api:
                    print(f"[scraper]    📡 Processing {len(captured_desc_api)} desc API responses...")
                    for desc_text in captured_desc_api:
                        cleaned = re.sub(r'<[^>]+>', ' ', desc_text)
                        cleaned = ' '.join(cleaned.split()).strip()
                        cleaned = _clean_description(cleaned)
                        if len(cleaned) > 50:
                            extracted['description'] = cleaned[:2000]
                            print(f"[scraper]    ✅ Description from desc API interception ({len(cleaned)} chars)")
                            break

                # Step B: Try DOM extraction (first attempt, before extra scroll)
                if not extracted.get('description'):
                    d = get_description_from_dom(page)
                    if d:
                        extracted['description'] = d

                # Step C: Scroll to bottom to trigger lazy-loaded description
                if not extracted.get('description'):
                    print("[scraper]    📜 Scrolling page to trigger lazy description...")
                    for _ in range(8):
                        page.mouse.wheel(0, 700)
                        page.wait_for_timeout(500)

                    try:
                        page.wait_for_selector(
                            '#product-description, [class*="product-description"], '
                            '[class*="detail-content"], [class*="productDescription"], '
                            '[class*="desc-content"], [class*="Description"], '
                            'iframe[src*="desc"]',
                            timeout=5000
                        )
                        page.wait_for_timeout(2000)
                    except Exception:
                        page.wait_for_timeout(2000)

                    # Check for new desc API captures after scrolling
                    if captured_desc_api:
                        for desc_text in captured_desc_api:
                            cleaned = re.sub(r'<[^>]+>', ' ', desc_text)
                            cleaned = ' '.join(cleaned.split()).strip()
                            cleaned = _clean_description(cleaned)
                            if len(cleaned) > 50 and not extracted.get('description'):
                                extracted['description'] = cleaned[:2000]
                                print(f"[scraper]    ✅ Description from desc API (after scroll) ({len(cleaned)} chars)")
                                break

                    if not extracted.get('description'):
                        d = get_description_from_dom(page)
                        if d:
                            extracted['description'] = d

                # Step D: Click "View More" / expand buttons and try again
                if not extracted.get('description'):
                    print("[scraper]    🖱️  Trying to click expand/view-more buttons...")
                    try:
                        clicked = page.evaluate('''() => {
                            var keywords = ['view more', 'see more', 'show more', 'read more',
                                            'view full', 'description', 'expand', 'show all'];
                            var elements = document.querySelectorAll('button, a, span, div');
                            for (var i = 0; i < elements.length; i++) {
                                var text = (elements[i].textContent || '').toLowerCase().trim();
                                if (text.length > 2 && text.length < 30) {
                                    for (var k = 0; k < keywords.length; k++) {
                                        if (text.indexOf(keywords[k]) !== -1) {
                                            elements[i].click();
                                            return text;
                                        }
                                    }
                                }
                            }
                            return '';
                        }''')
                        if clicked:
                            print(f"[scraper]       Clicked: '{clicked}'")
                            page.wait_for_timeout(2000)
                            d = get_description_from_dom(page)
                            if d:
                                extracted['description'] = d
                    except Exception:
                        pass

                # Step E: Scroll to absolute bottom + wait + final attempt
                if not extracted.get('description'):
                    print("[scraper]    📜 Final scroll to absolute bottom...")
                    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    page.wait_for_timeout(3000)

                    if captured_desc_api:
                        best = max(captured_desc_api, key=len)
                        cleaned = re.sub(r'<[^>]+>', ' ', best)
                        cleaned = ' '.join(cleaned.split()).strip()
                        cleaned = _clean_description(cleaned)
                        if len(cleaned) > 50:
                            extracted['description'] = cleaned[:2000]
                            print(f"[scraper]    ✅ Description from desc API (final) ({len(cleaned)} chars)")

                    if not extracted.get('description'):
                        d = get_description_from_dom(page)
                        if d:
                            extracted['description'] = d

                if extracted.get('description'):
                    print(f"[scraper]    ✅ Description pipeline succeeded: {len(extracted['description'])} chars")
                else:
                    print("[scraper]    ⚠️  Description pipeline: no description found")

            browser.close()

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
        print(f'[scraper]    Price      : {extracted.get("price", "")}')
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
            'style': '', 'season': '', 'fit': '',
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
