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
        images = []
        try:
            images = result['imageModule']['imagePathList'] or []
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
                if img.startswith('//'):
                    img = 'https:' + img
                elif not img.startswith('http'):
                    img = 'https://' + img
                extracted[f'image_{idx}'] = re.sub(r'_\d+x\d+', '', img)

        if images:
            print(f"[scraper]    ✅ Found {len(images)} images")

        # ── Price ──────────────────────────────────────────────────────────
        try:
            pm = result.get('priceModule', {}) or result.get('PRICE_MODULE', {}) or {}
            price = (pm.get('formatedActivityPrice') or
                     pm.get('formatedPrice') or
                     pm.get('minActivityAmount', {}).get('formatedAmount', '') or '')
            if price:
                extracted['price'] = str(price)
        except Exception:
            pass

        # ── Description from API ───────────────────────────────────────────
        try:
            dm   = result.get('descriptionModule', {}) or {}
            desc = dm.get('description', '') or dm.get('content', '')
            if desc and len(desc) > 50:
                desc = re.sub(r'<[^>]+>', ' ', desc)
                extracted['description'] = ' '.join(desc.split())[:2000]
        except Exception:
            pass

        # ── Description fallback: check DESCRIPTION_PC ─────────────────────
        if not extracted.get('description'):
            try:
                desc_pc = result.get('DESCRIPTION_PC', {}) or {}
                desc_html = (desc_pc.get('descriptionContent', '') or
                             desc_pc.get('content', '') or
                             desc_pc.get('description', ''))
                if desc_html and len(desc_html) > 50:
                    desc_clean = re.sub(r'<[^>]+>', ' ', desc_html)
                    extracted['description'] = ' '.join(desc_clean.split())[:2000]
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


def get_description_from_dom(page) -> str:
    """
    Enhanced description extraction using broad selectors
    + recursive child traversal until leaf text is found.
    """

    # ── STRATEGY 1: Broad selectors + recursive leaf text extraction ────
    desc_selectors = [
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
        'div[id="product-description"]',
        'div[id="nav-description"]',
        'div.detailmodule_text',
        '[class*="detail-desc"]',
        '[class*="product-detail-info"]',
        '[class*="product_desc"]',
        '[class*="pdp-description"]',
        '[class*="module_description"]',
        '[class*="DescriptionModule"]',
    ]

    for selector in desc_selectors:
        try:
            el = page.locator(selector).first
            if el.count() == 0:
                continue

            # Recursive leaf text extraction via JavaScript
            text = page.evaluate('''(element) => {
                function getLeafText(node) {
                    if (!node) return '';

                    var tag = node.tagName ? node.tagName.toLowerCase() : '';
                    if (['script', 'style', 'noscript', 'svg', 'iframe'].includes(tag)) {
                        return '';
                    }

                    // Text node — return its content
                    if (node.nodeType === Node.TEXT_NODE) {
                        return (node.textContent || '').trim();
                    }

                    // Get child elements
                    var childElements = node.children;

                    // LEAF NODE: no child elements — return all text content
                    if (!childElements || childElements.length === 0) {
                        return (node.textContent || '').trim();
                    }

                    // HAS CHILDREN: recurse into each child node
                    var texts = [];
                    for (var i = 0; i < node.childNodes.length; i++) {
                        var childText = getLeafText(node.childNodes[i]);
                        if (childText) {
                            texts.push(childText);
                        }
                    }
                    return texts.join(' ');
                }
                return getLeafText(element);
            }''', el.element_handle())

            if text:
                # Clean up the extracted text
                text = re.sub(r'\s+', ' ', text).strip()
                # Remove common AliExpress boilerplate
                text = re.sub(
                    r'^.*?Description\s+report\s+', '', text,
                    flags=re.IGNORECASE | re.DOTALL
                )
                text = re.sub(
                    r'(Smarter Shopping|Better Living).*$', '', text,
                    flags=re.IGNORECASE
                )
                text = text.strip()

                if len(text) > 50:
                    print(f"[scraper]    ✅ Description from DOM selector: "
                          f"{selector} ({len(text)} chars)")
                    return text[:2000]
        except Exception:
            continue

    # ── STRATEGY 2: Description loaded in iframe ────────────────────────
    try:
        iframe_selectors = [
            'iframe[src*="desc"]',
            'iframe[id*="desc"]',
            'iframe[class*="desc"]',
        ]
        for iframe_sel in iframe_selectors:
            try:
                iframe_el = page.locator(iframe_sel).first
                if iframe_el.count() > 0:
                    frame = iframe_el.content_frame()
                    if frame:
                        body_text = frame.locator('body').inner_text()
                        if body_text and len(body_text.strip()) > 50:
                            text = re.sub(r'\s+', ' ', body_text).strip()
                            print(f"[scraper]    ✅ Description from iframe ({len(text)} chars)")
                            return text[:2000]
            except Exception:
                continue
    except Exception:
        pass

    # ── STRATEGY 3: Grab all text from any large content block ──────────
    try:
        large_blocks = page.locator(
            'div[class*="desc"], div[class*="description"], '
            'div[class*="Detail"], div[class*="detail"]'
        ).all()
        for block in large_blocks:
            try:
                text = block.inner_text().strip()
                text = re.sub(r'\s+', ' ', text)
                text = re.sub(
                    r'^.*?Description\s+report\s+', '', text,
                    flags=re.IGNORECASE | re.DOTALL
                )
                text = re.sub(
                    r'(Smarter Shopping|Better Living).*$', '', text,
                    flags=re.IGNORECASE
                )
                text = text.strip()
                if len(text) > 100 and 'Smarter Shopping' not in text:
                    print(f"[scraper]    ✅ Description from large block ({len(text)} chars)")
                    return text[:2000]
            except Exception:
                continue
    except Exception:
        pass

    return ''


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    captured_pdp = []
    captured_descriptions = []

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
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
                viewport={'width': 1366, 'height': 900},
                locale='en-US',
                timezone_id='Asia/Karachi',
            )
            page = context.new_page()

            def handle_response(response):
                url_r = response.url

                # Capture PDP API
                if ('mtop.aliexpress.pdp.pc.query' in url_r or
                        'mtop.aliexpress.itemdetail.pc' in url_r):
                    try:
                        body = response.body()
                        if len(body) > 1000:
                            text = body.decode('utf-8', errors='replace')
                            if ('PRODUCT_PROP_PC' in text or
                                    'imagePathList' in text or
                                    'SHOP_CARD_PC' in text):
                                captured_pdp.append(text)
                                print(f"[scraper]    📡 Captured PDP: {url_r[:80]}")
                    except Exception:
                        pass

                # Capture description API (separate endpoint)
                if ('mtop.aliexpress.pdp.pc.description' in url_r or
                        'aeglobal/product/description' in url_r or
                        'pdp/description' in url_r.lower()):
                    try:
                        body = response.body()
                        if len(body) > 100:
                            text = body.decode('utf-8', errors='replace')
                            desc_clean = re.sub(r'<[^>]+>', ' ', text)
                            desc_clean = ' '.join(desc_clean.split()).strip()
                            if len(desc_clean) > 50:
                                captured_descriptions.append(desc_clean[:2000])
                                print(f"[scraper]    📡 Captured description API: "
                                      f"{url_r[:80]}")
                    except Exception:
                        pass

            page.on('response', handle_response)

            print(f'\n[scraper] 🌐 Opening: {url}')
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.wait_for_timeout(5000)

            # Initial scroll
            page.mouse.wheel(0, 800)
            page.wait_for_timeout(1000)

            extracted = {}
            print(f'[scraper]    📦 Captured {len(captured_pdp)} PDP responses')

            for resp_text in captured_pdp:
                parsed = parse_pdp_response(resp_text)
                if parsed:
                    print(f'[scraper]    ✅ Parsed {len(parsed)} fields from API')
                    for k, v in parsed.items():
                        if k not in extracted or not extracted[k]:
                            extracted[k] = v

            # Use captured description API if available
            if not extracted.get('description') and captured_descriptions:
                best_desc = max(captured_descriptions, key=len)
                if len(best_desc) > 50:
                    extracted['description'] = best_desc
                    print(f"[scraper]    ✅ Description from API interception "
                          f"({len(best_desc)} chars)")

            # DOM fallbacks
            if not extracted.get('title'):
                extracted['title'] = get_title_from_dom(page)
            if not extracted.get('image_1'):
                extracted.update(get_images_from_dom(page))

            # ── SCROLL DOWN to trigger lazy-loaded description ──────────
            if not extracted.get('description'):
                print("[scraper]    📜 Scrolling to load description...")

                for scroll_y in [800, 1600, 2400, 3200, 4000, 5000]:
                    page.mouse.wheel(0, 800)
                    page.wait_for_timeout(500)

                # Wait for description container to appear
                try:
                    page.wait_for_selector(
                        '#product-description, [class*="product-description"], '
                        '[class*="detail-content"], .product-detail__description, '
                        '[class*="productDescription"], [class*="desc-content"], '
                        'div[data-pl="product-description"]',
                        timeout=5000
                    )
                    page.wait_for_timeout(1500)  # Let content fully render
                except Exception:
                    pass  # Description might not exist or uses different selector

                # Check if description API was captured during scrolling
                if captured_descriptions:
                    best_desc = max(captured_descriptions, key=len)
                    if len(best_desc) > 50:
                        extracted['description'] = best_desc
                        print(f"[scraper]    ✅ Description from API (after scroll) "
                              f"({len(best_desc)} chars)")

                # Try DOM extraction after scrolling
                if not extracted.get('description'):
                    d = get_description_from_dom(page)
                    if d:
                        extracted['description'] = d

            # ── Price fallback from DOM scripts ─────────────────────────
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
