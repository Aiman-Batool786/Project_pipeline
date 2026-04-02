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
def _scrape_in_thread(url: str) -> dict:
    """Runs Camoufox in a separate thread to avoid asyncio conflict."""
    captured = []
    html     = ""
    seller   = {}

    try:
        with Camoufox(headless=True, os='windows') as browser:
            context = browser.new_context(
                viewport={'width': 1366, 'height': 900},
                locale='en-US',
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
                        if any(x in text for x in ['titleModule', 'storeModule', 'skuModule',
                                                    'PRODUCT_PROP_PC', 'SHOP_CARD_PC']):
                            captured.append(text)
                            print(f"[scraper] 📡 Captured rich PDP ({len(text)} bytes)")
                except Exception:
                    pass

            page.on('response', handle_response)
            page.goto(url, timeout=90000, wait_until='domcontentloaded')

            # Scroll to trigger lazy loading
            for _ in range(6):
                page.wait_for_timeout(800)
                page.mouse.wheel(0, 600)
            page.wait_for_timeout(2000)

            # ✅ Extract seller info from DOM
            print("[scraper]    🏪 Extracting seller info from DOM...")
            try:
                # ── Click the store/trader link to open popup ──
                store_selectors = [
                    'a[href*="/store/"]',
                    'span:has-text("Trader")',
                    '.store-detail--storeName',
                    '[class*="store-header"]',
                    '[class*="storeName"]',
                    'a[class*="store"]',
                ]
                clicked = False
                for sel in store_selectors:
                    try:
                        el = page.locator(sel).first
                        if el.count() > 0:
                            el.click(timeout=3000)
                            page.wait_for_timeout(2000)
                            clicked = True
                            print(f"[scraper]    ✅ Clicked store link: {sel}")
                            break
                    except Exception:
                        continue

                # ── Extract from popup or page ──
                # Store name
                for sel in [
                    '.store-detail--storeInfo--BMDFsTB td:nth-child(2)',
                    '[class*="storeInfo"] td:nth-child(2)',
                    '[class*="store-name"]',
                    '[class*="storeName"]',
                ]:
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

                # ── Extract all table rows from store popup ──
                try:
                    rows = page.locator('.store-detail--storeInfo--BMDFsTB table tr').all()
                    for row in rows:
                        cells = row.locator('td').all()
                        if len(cells) >= 2:
                            key   = cells[0].inner_text().strip().lower().rstrip(':')
                            value = cells[1].inner_text().strip()
                            if 'name' in key:
                                seller['store_name'] = value
                            elif 'store no' in key or 'no.' in key:
                                seller['store_id'] = value
                                seller['store_url'] = f"https://www.aliexpress.com/store/{value}"
                            elif 'location' in key or 'country' in key:
                                seller['seller_country'] = value
                            elif 'open' in key or 'since' in key:
                                seller['store_open_date'] = value
                    if seller:
                        print(f"[scraper]    ✅ Store table extracted: {seller}")
                except Exception as e:
                    print(f"[scraper]    ⚠️  Store table error: {e}")

                # ── Extract seller ratings ──
                try:
                    rating_rows = page.locator('.store-detail--storeRating--Z2j7q9u table tr').all()
                    for row in rating_rows:
                        cells = row.locator('td').all()
                        if len(cells) >= 2:
                            key   = cells[0].inner_text().strip().lower()
                            value = cells[1].locator('b').first.inner_text().strip()
                            if 'item' in key or 'described' in key:
                                seller['seller_rating'] = value
                            elif 'communication' in key:
                                seller['seller_communication'] = value
                            elif 'shipping' in key:
                                seller['seller_shipping_speed'] = value
                    if seller.get('seller_rating'):
                        print(f"[scraper]    ✅ Ratings extracted: {seller.get('seller_rating')}")
                except Exception as e:
                    print(f"[scraper]    ⚠️  Ratings error: {e}")

                # ── Fallback: extract store name from page header ──
                if not seller.get('store_name'):
                    for sel in [
                        '[class*="store-header--storeName"]',
                        '[class*="shop-name"]',
                        'a[href*="/store/"] span',
                        '[data-pl="store-name"]',
                    ]:
                        try:
                            el = page.locator(sel).first
                            if el.count() > 0:
                                text = el.inner_text().strip()
                                if text and len(text) > 1:
                                    seller['store_name'] = text
                                    break
                        except Exception:
                            continue

                # ── Extract store URL from page ──
                if not seller.get('store_url'):
                    try:
                        store_link = page.locator('a[href*="/store/"]').first
                        if store_link.count() > 0:
                            href = store_link.get_attribute('href') or ''
                            if '/store/' in href:
                                if href.startswith('//'):
                                    href = 'https:' + href
                                seller['store_url'] = href
                                # Extract store ID from URL
                                import re as re_mod
                                m = re_mod.search(r'/store/(\d+)', href)
                                if m and not seller.get('store_id'):
                                    seller['store_id'] = m.group(1)
                    except Exception:
                        pass

            except Exception as e:
                print(f"[scraper]    ⚠️  Seller DOM extraction error: {e}")

            html = page.content()
            page.close()
            context.close()

    except Exception as e:
        print(f'[scraper] ❌ Browser error: {e}')
        import traceback
        traceback.print_exc()

    return {'captured': captured, 'html': html, 'seller': seller}

def get_product_info(url: str) -> dict | None:
    print(f"[scraper] Starting: {url}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_scrape_in_thread, url)
        data   = future.result(timeout=180)

    extracted = {}

    # Parse API responses
    for text in sorted(data['captured'], key=len, reverse=True):
        parsed = parse_pdp_response(text)
        if parsed:
            for k, v in parsed.items():
                if v and (k not in extracted or not extracted[k]):
                    extracted[k] = v
            break

    # ✅ Merge DOM seller info
    dom_seller = data.get('seller', {})
    for k, v in dom_seller.items():
        if v and (k not in extracted or not extracted[k]):
            extracted[k] = v
            print(f"[scraper]    ✅ Seller from DOM: {k} = {v}")

    # Title fallback
    if not extracted.get('title') and data['html']:
        import re
        m = re.search(r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)["\']',
                      data['html'])
        if m:
            extracted['title'] = m.group(1).strip()

    # Image fallback
    if not extracted.get('image_1') and data['html']:
        import re, json
        m = re.search(r'"imagePathList"\s*:\s*(\[[^\]]+\])', data['html'])
        if m:
            try:
                urls = json.loads(m.group(1))
                for idx, img_url in enumerate(urls[:20], 1):
                    if img_url:
                        extracted[f'image_{idx}'] = img_url
            except Exception:
                pass

    print(f"[scraper] Final extracted fields: {len(extracted)}")
    print(f"[scraper] Seller fields found: {[k for k in ['store_name','store_id','seller_rating','seller_country'] if extracted.get(k)]}")

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
    result = get_product_info(test_url)
    
    if result:
        print("\n" + "="*80)
        for k, v in sorted(result.items()):
            if not k.startswith('image_'):
                print(f"{k:20}: {str(v)[:120]}")
