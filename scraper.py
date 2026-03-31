import re
import json
import time
import requests
import urllib.request
import os
from playwright.sync_api import sync_playwright

# ─────────────────────────────────────────────────────────────────────────────
# ZENROWS CONFIG
# ─────────────────────────────────────────────────────────────────────────────

ZENROWS_API_KEY = os.environ.get("ZENROWS_API_KEY", "")

def fetch_rendered_html(url: str, retries: int = 3) -> str:
    """
    Fetch fully rendered page HTML via ZenRows residential proxy.
    Uses JS rendering + wait_for selector to ensure async product data is loaded.
    """
    for attempt in range(1, retries + 1):
        try:
            print(f"[scraper]    🌐 ZenRows fetch (attempt {attempt})...")
            resp = requests.get(
                "https://api.zenrows.com/v1/",
                params={
                    "apikey":          ZENROWS_API_KEY,
                    "url":             url,
                    "js_render":       "true",
                    "premium_proxy":   "true",
                    "proxy_country":   "gb",
                    "wait_for":        "[data-pl='product-title'],h1.product-title-text",
                    "wait":            "6000",
                    "response_type":   "html",
                    "original_status": "true",
                },
                timeout=120,
            )
            if resp.status_code == 200 and len(resp.text) > 5000:
                print(f"[scraper]    ✅ ZenRows returned {len(resp.text)} chars")
                # Check if product data is actually present
                has_specs  = "PRODUCT_PROP_PC" in resp.text
                has_seller = "SHOP_CARD_PC"     in resp.text
                print(f"[scraper]    📦 Has specs: {has_specs} | Has seller: {has_seller}")
                if not has_specs and attempt < retries:
                    print(f"[scraper]    ⚠️  Product data missing, retrying with longer wait...")
                    time.sleep(5 * attempt)
                    continue
                return resp.text
            else:
                print(f"[scraper]    ⚠️  ZenRows attempt {attempt}: "
                      f"status={resp.status_code} len={len(resp.text)}")
                deny = resp.headers.get("x-deny-reason", "")
                if deny:
                    print(f"[scraper]    ⚠️  Deny reason: {deny}")
        except Exception as e:
            print(f"[scraper]    ⚠️  ZenRows attempt {attempt} error: {e}")

        if attempt < retries:
            time.sleep(5 * attempt)

    print("[scraper]    ❌ ZenRows failed after all retries")
    return ""

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
# EMBEDDED JSON EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────

def extract_page_json(page) -> dict:
    """Try all known locations for embedded product JSON."""

    # Strategy 0: Scan all scripts for PRODUCT_PROP_PC / SHOP_CARD_PC blobs
    print("[scraper]    🔍 Strategy 0: direct script scan for product keys...")
    try:
        scripts = page.evaluate('''() => {
            return Array.from(document.querySelectorAll("script"))
                .map(s => s.textContent || "")
                .filter(t => t.length > 500 && (
                    t.includes("PRODUCT_PROP_PC") ||
                    t.includes("SHOP_CARD_PC")
                ));
        }''')
        for script_text in scripts:
            # Try to find a large JSON object containing the product keys
            for pattern in [
                r'window\.runParams\s*=\s*(\{.+\})\s*;',
                r'window\.__aer_data__\s*=\s*(\{.+\})\s*;',
                r'window\._dida_config_\s*=\s*(\{.+\})\s*;',
                r'var\s+_init_data_\s*=\s*(\{.+\})\s*;',
                r'"result"\s*:\s*(\{"GLOBAL_DATA".+?\})\s*(?:,|\})',
            ]:
                m = re.search(pattern, script_text, re.DOTALL)
                if m:
                    try:
                        data   = json.loads(m.group(1))
                        result = _find_product_result(data)
                        if result and _result_has_product_data(result):
                            print("[scraper]    ✅ Strategy 0: data from script pattern")
                            return result
                    except Exception:
                        pass

            # Try extracting the full JSON blob if script text IS valid JSON
            try:
                data   = json.loads(script_text)
                result = _find_product_result(data)
                if result and _result_has_product_data(result):
                    print("[scraper]    ✅ Strategy 0: script text is direct JSON")
                    return result
            except Exception:
                pass

    except Exception as e:
        print(f"[scraper]    ⚠️  Strategy 0 error: {e}")

    # Strategy A: __NEXT_DATA__
    try:
        raw = page.evaluate('''() => {
            var el = document.getElementById("__NEXT_DATA__");
            return el ? el.textContent : "";
        }''')
        if raw and len(raw) > 100:
            data   = json.loads(raw)
            result = _find_product_result(data)
            if result:
                print("[scraper]    ✅ Data from __NEXT_DATA__")
                return result
    except Exception as e:
        print(f"[scraper]    ⚠️  __NEXT_DATA__ error: {e}")

    # Strategy B: window.runParams
    try:
        raw = page.evaluate('() => JSON.stringify(window.runParams || null)')
        if raw and raw != 'null':
            data   = json.loads(raw)
            result = _find_product_result(data)
            if result:
                print("[scraper]    ✅ Data from window.runParams")
                return result
    except Exception as e:
        print(f"[scraper]    ⚠️  window.runParams error: {e}")

    # Strategy C: window.__aer_data__
    try:
        raw = page.evaluate('() => JSON.stringify(window.__aer_data__ || null)')
        if raw and raw != 'null':
            data   = json.loads(raw)
            result = _find_product_result(data)
            if result:
                print("[scraper]    ✅ Data from window.__aer_data__")
                return result
    except Exception as e:
        print(f"[scraper]    ⚠️  window.__aer_data__ error: {e}")

    # Strategy D: window._dida_config_
    try:
        raw = page.evaluate('() => JSON.stringify(window._dida_config_ || null)')
        if raw and raw != 'null':
            data   = json.loads(raw)
            result = _find_product_result(data)
            if result:
                print("[scraper]    ✅ Data from window._dida_config_")
                return result
    except Exception as e:
        print(f"[scraper]    ⚠️  window._dida_config_ error: {e}")

    # Strategy E: full script tag scan (broader)
    print("[scraper]    🔍 Strategy E: broad script tag scan...")
    try:
        scripts = page.evaluate('''() => {
            return Array.from(document.querySelectorAll("script"))
                .map(s => s.textContent || "")
                .filter(t => t.length > 200 && (
                    t.includes("PRODUCT_PROP_PC") ||
                    t.includes("showedProps") ||
                    t.includes("SHOP_CARD_PC") ||
                    t.includes("storeName") ||
                    t.includes("imagePathList") ||
                    t.includes("attrName") ||
                    t.includes("runParams")
                ));
        }''')

        for script_text in scripts:
            for pattern in [
                r'window\.runParams\s*=\s*(\{.+\})\s*;',
                r'window\.__aer_data__\s*=\s*(\{.+\})\s*;',
                r'window\._dida_config_\s*=\s*(\{.+\})\s*;',
                r'var\s+_init_data_\s*=\s*(\{.+\})\s*;',
            ]:
                m = re.search(pattern, script_text, re.DOTALL)
                if m:
                    try:
                        data   = json.loads(m.group(1))
                        result = _find_product_result(data)
                        if result:
                            print("[scraper]    ✅ Data from script tag pattern")
                            return result
                    except Exception:
                        pass

            try:
                for m in re.finditer(r'(\{[^{}]{20,}\})', script_text):
                    try:
                        data = json.loads(m.group(1))
                        if any(k in data for k in ['PRODUCT_PROP_PC', 'SHOP_CARD_PC',
                                                    'imagePathList', 'showedProps']):
                            print("[scraper]    ✅ Data from inline JSON object")
                            return data
                    except Exception:
                        pass
            except Exception:
                pass

    except Exception as e:
        print(f"[scraper]    ⚠️  Strategy E error: {e}")

    # Strategy F: evaluate window object directly for DCData with full product info
    print("[scraper]    🔍 Strategy F: window DCData deep scan...")
    try:
        raw = page.evaluate('''() => {
            try {
                var dc = window._d_c_ && window._d_c_.DCData;
                if (dc) return JSON.stringify(dc);
            } catch(e) {}
            return null;
        }''')
        if raw and raw != 'null' and len(raw) > 200:
            data = json.loads(raw)
            result = _find_product_result(data)
            if result:
                print("[scraper]    ✅ Data from window._d_c_.DCData")
                return result
    except Exception as e:
        print(f"[scraper]    ⚠️  Strategy F error: {e}")

    print("[scraper]    ⚠️  No embedded product JSON found")
    return {}


def _result_has_product_data(result: dict) -> bool:
    """Quick check that a result dict actually contains product spec/seller data."""
    return bool(
        result.get('PRODUCT_PROP_PC') or
        result.get('SHOP_CARD_PC') or
        result.get('titleModule') or
        (result.get('GLOBAL_DATA', {}) or {}).get('globalData', {})
    )


def _find_product_result(data, depth: int = 0) -> dict:
    """Recursively find the dict containing AliExpress product data keys."""
    if depth > 8 or not isinstance(data, dict):
        return {}

    if any(k in data for k in ['PRODUCT_PROP_PC', 'SHOP_CARD_PC', 'HEADER_IMAGE_PC',
                                'imageModule', 'titleModule', 'GLOBAL_DATA']):
        return data

    for path in [
        ['data', 'result'],
        ['props', 'pageProps', 'componentData'],
        ['pageData', 'result'],
        ['result'],
        ['data'],
        ['_init_data_', 'data', 'result'],
    ]:
        node = data
        for key in path:
            node = node.get(key) if isinstance(node, dict) else None
            if node is None:
                break
        if node and isinstance(node, dict):
            found = _find_product_result(node, depth + 1)
            if found:
                return found

    for v in data.values():
        if isinstance(v, dict):
            found = _find_product_result(v, depth + 1)
            if found:
                return found

    return {}

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
                  f"Rating: {seller.get('seller_rating', '')} | "
                  f"Country: {seller.get('seller_country', '')}")
        else:
            print("[scraper]    ⚠️  Seller info empty")

    except Exception as e:
        print(f"[scraper]    ⚠️  Seller extraction error: {e}")

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
        print(f"[scraper]    📥 Fetching desc URL: {url[:80]}")
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        if 'desc.json' in url:
            with urllib.request.urlopen(req, timeout=10) as r:
                return _parse_desc_json(json.loads(r.read().decode('utf-8', errors='replace')))
        elif 'desc.htm' in url:
            with urllib.request.urlopen(req, timeout=10) as r:
                raw   = r.read().decode('utf-8', errors='replace')
                clean = ' '.join(re.sub(r'<[^>]+>', ' ', raw).split()).strip()
                return clean if len(clean) > 50 else ''
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
# DESCRIPTION — DOM FALLBACKS
# ─────────────────────────────────────────────────────────────────────────────

DESC_SELECTORS = [
    "#product-description",
    '[class*="product-description"]',
    '[class*="detail-content"]',
    '.product-detail__description',
    '[class*="productDescription"]',
    '[class*="desc-content"]',
    'div[data-pl="product-description"]',
    '[class*="DescriptionModule"]',
    '[class*="detail-desc"]',
    '[class*="pdp-description"]',
    'div.detailmodule_text',
    'div.richTextContainer',
]

LEAF_WALKER_JS = '''(selectors) => {
    function getLeafTexts(node, texts) {
        if (!node) return;
        var tag = (node.tagName || "").toLowerCase();
        if (["script","style","noscript","svg","iframe","button",
             "input","select","textarea","nav","header","footer"].indexOf(tag) !== -1) return;
        if (node.nodeType === 3) {
            var t = (node.textContent || "").trim();
            if (t.length > 0) texts.push(t);
            return;
        }
        if (!node.hasChildNodes()) {
            var t = (node.textContent || "").trim();
            if (t.length > 0) texts.push(t);
            return;
        }
        for (var i = 0; i < node.childNodes.length; i++) {
            getLeafTexts(node.childNodes[i], texts);
        }
    }
    for (var s = 0; s < selectors.length; s++) {
        var els = document.querySelectorAll(selectors[s]);
        for (var e = 0; e < els.length; e++) {
            var texts = [];
            getLeafTexts(els[e], texts);
            var joined = texts.join(" ").trim();
            if (joined.length > 80) {
                return JSON.stringify({
                    text: joined.substring(0, 3000),
                    selector: selectors[s]
                });
            }
        }
    }
    return "";
}'''

def get_description_from_dom(page) -> str:
    print("[scraper]    🔍 Desc: recursive leaf walker...")
    try:
        raw = page.evaluate(LEAF_WALKER_JS, DESC_SELECTORS)
        if raw:
            parsed  = json.loads(raw)
            cleaned = _clean_description(parsed.get('text', ''))
            if len(cleaned) > 80:
                print(f"[scraper]    ✅ Desc via {parsed.get('selector','')}: {len(cleaned)} chars")
                return cleaned[:2000]
    except Exception as e:
        print(f"[scraper]       Leaf walker error: {e}")

    print("[scraper]    🔍 Desc: iframe scan...")
    try:
        for frame in page.frames:
            if frame == page.main_frame:
                continue
            try:
                body = frame.evaluate('''() => {
                    if (!document.body) return "";
                    function getLeafTexts(node, texts) {
                        if (!node) return;
                        var tag = (node.tagName || "").toLowerCase();
                        if (["script","style","noscript","nav"].indexOf(tag) !== -1) return;
                        if (node.nodeType === 3) {
                            var t = (node.textContent || "").trim();
                            if (t) texts.push(t);
                            return;
                        }
                        for (var i = 0; i < node.childNodes.length; i++)
                            getLeafTexts(node.childNodes[i], texts);
                    }
                    var texts = [];
                    getLeafTexts(document.body, texts);
                    return texts.join(" ").substring(0, 3000);
                }''')
                cleaned = _clean_description(body)
                if (len(cleaned) > 100 and
                        'sign in' not in cleaned.lower()[:50] and
                        'cookie' not in cleaned.lower()[:80] and
                        'Smarter Shopping' not in cleaned):
                    print(f"[scraper]    ✅ Desc from iframe: {len(cleaned)} chars")
                    return cleaned[:2000]
            except Exception:
                continue
    except Exception as e:
        print(f"[scraper]       Iframe scan error: {e}")

    print("[scraper]    🔍 Desc: script tag scan...")
    try:
        desc = page.evaluate('''() => {
            var scripts = document.querySelectorAll("script");
            var patterns = [
                /"description"\s*:\s*"((?:[^"\\\\]|\\\\.){80,})"/,
                /"descriptionContent"\s*:\s*"((?:[^"\\\\]|\\\\.){80,})"/,
                /"detailDesc"\s*:\s*"((?:[^"\\\\]|\\\\.){80,})"/,
                /"richTextDesc"\s*:\s*"((?:[^"\\\\]|\\\\.){80,})"/,
            ];
            for (var i = 0; i < scripts.length; i++) {
                var text = scripts[i].textContent || "";
                if (text.length < 500) continue;
                for (var p = 0; p < patterns.length; p++) {
                    var match = text.match(patterns[p]);
                    if (match && match[1]) {
                        try {
                            var decoded = JSON.parse('"' + match[1] + '"');
                            var div = document.createElement("div");
                            div.innerHTML = decoded;
                            var clean = div.innerText || div.textContent || "";
                            if (clean.length > 80) return clean.substring(0, 3000);
                        } catch(e) {
                            var s = match[1].replace(/<[^>]+>/g," ").replace(/\\s+/g," ").trim();
                            if (s.length > 80) return s.substring(0, 3000);
                        }
                    }
                }
            }
            return "";
        }''')
        cleaned = _clean_description(desc)
        if len(cleaned) > 80:
            print(f"[scraper]    ✅ Desc from script tag: {len(cleaned)} chars")
            return cleaned[:2000]
    except Exception as e:
        print(f"[scraper]       Script scan error: {e}")

    print("[scraper]    🔍 Desc: largest content block...")
    try:
        desc = page.evaluate('''() => {
            var els = document.querySelectorAll("div, section, article");
            var best = "", bestLen = 0;
            for (var i = 0; i < els.length; i++) {
                var el = els[i];
                var cl = (el.className || "").toString().toLowerCase();
                var id = (el.id || "").toLowerCase();
                if (cl.indexOf("desc") === -1 && cl.indexOf("detail") === -1 &&
                    id.indexOf("desc") === -1 && id.indexOf("detail") === -1) continue;
                var text = (el.innerText || "").replace(/\\s+/g," ").trim();
                if (text.length > 100 && text.length > bestLen &&
                        text.indexOf("Add to Cart") === -1 &&
                        text.indexOf("Smarter Shopping") === -1 &&
                        text.indexOf("Sign in") !== 0) {
                    best = text; bestLen = text.length;
                }
            }
            return best ? best.substring(0, 3000) : "";
        }''')
        cleaned = _clean_description(desc)
        if len(cleaned) > 80:
            print(f"[scraper]    ✅ Desc from largest block: {len(cleaned)} chars")
            return cleaned[:2000]
    except Exception as e:
        print(f"[scraper]       Largest block error: {e}")

    return ''

def _clean_description(text: str) -> str:
    if not text:
        return ''
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^.*?Description\s+report\s+', '', text,
                  flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'(Smarter Shopping|Better Living).*$', '',
                  text, flags=re.IGNORECASE)
    return text.strip()

# ─────────────────────────────────────────────────────────────────────────────
# DOM FALLBACKS — title and images
# ─────────────────────────────────────────────────────────────────────────────

def get_title_from_dom(page) -> str:
    for selector in [
        'h1[data-pl="product-title"]',
        'meta[property="og:title"]',
        'h1.product-title-text',
        'h1',
    ]:
        try:
            el = page.locator(selector).first
            if el.count() > 0:
                val = (el.get_attribute('content') or el.inner_text() or '').strip()
                if val and len(val) > 5:
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
                        print(f"[scraper]    ✅ {len(images)} images from script")
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
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    print(f'\n[scraper] 🚀 Starting: {url}')

    # ── Step 1: Fetch rendered HTML via ZenRows ────────────────────────────
    html = fetch_rendered_html(url)
    if not html:
        print('[scraper] ❌ Could not fetch HTML via ZenRows')
        return None

    if '_____tmd_____/punish' in html and 'PRODUCT_PROP_PC' not in html:
        print('[scraper] ❌ Still getting CAPTCHA page')
        return None

    extracted = {}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            context = browser.new_context(
                viewport={'width': 1366, 'height': 900},
                locale='en-GB',
            )
            page = context.new_page()

            # Load the ZenRows-fetched HTML into Playwright for DOM parsing
            page.set_content(html, wait_until='domcontentloaded')
            page.wait_for_timeout(2000)

            # ── Step 2: Extract from embedded JSON ────────────────────────
            print("[scraper]    📦 Extracting from embedded JSON...")
            result = extract_page_json(page)

            if result:
                print(f"[scraper]    🔑 Result keys: {list(result.keys())[:15]}")

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
                    print(f"[scraper]    ✅ {len(props)} spec props")
                    mapped, raw = map_props_to_fields(props)
                    extracted.update(mapped)
                    for k, v in raw.items():
                        print(f"[scraper]       {k}: {v[:60]}")
                else:
                    print("[scraper]    ⚠️  No specs in PRODUCT_PROP_PC")

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
                    print(f"[scraper]    ✅ Price: {price}")

                # Description from embedded URL
                desc = fetch_description_url(result)
                if desc:
                    extracted['description'] = desc
                    print(f"[scraper]    ✅ Desc from URL: {len(desc)} chars")

            # ── Step 3: DOM fallbacks ──────────────────────────────────────
            if not extracted.get('title'):
                extracted['title'] = get_title_from_dom(page)

            if not extracted.get('image_1'):
                extracted.update(get_images_from_dom(page))

            if not extracted.get('price'):
                try:
                    for script in page.locator('script').all():
                        txt = script.text_content() or ''
                        m   = re.search(
                            r'"(?:actSkuCalPrice|formatedActivityPrice|formatedPrice)"\s*:\s*"([^"]+)"',
                            txt
                        )
                        if m:
                            extracted['price'] = m.group(1)
                            break
                except Exception:
                    pass

            # ── Step 4: Description pipeline ──────────────────────────────
            if not extracted.get('description'):
                print("\n[scraper]    📝 Description pipeline...")
                d = get_description_from_dom(page)
                if d:
                    extracted['description'] = d

            browser.close()

    except Exception as e:
        print(f'[scraper] ❌ Playwright error: {e}')
        import traceback
        traceback.print_exc()

    # ── Validate ───────────────────────────────────────────────────────────
    if not extracted.get('title'):
        print('[scraper] ❌ No title — aborting')
        return None

    # ── Summary ────────────────────────────────────────────────────────────
    core          = ['brand', 'color', 'dimensions', 'weight', 'material',
                     'certifications', 'country_of_origin', 'warranty', 'product_type']
    seller_fields = ['store_name', 'store_id', 'seller_positive_rate',
                     'seller_rating', 'seller_country', 'store_open_date']

    print(f'\n[scraper] ✅ Extraction complete')
    print(f'[scraper]    Title  : {extracted.get("title", "")[:70]}')
    print(f'[scraper]    Price  : {extracted.get("price", "")}')
    print(f'[scraper]    Desc   : {len(extracted.get("description", ""))} chars')
    print(f'[scraper]    Specs  : {[k for k in core if extracted.get(k)]}')
    print(f'[scraper]    Seller : {[k for k in seller_fields if extracted.get(k)]}')
    print(f'[scraper]    Images : {sum(1 for i in range(1, 21) if extracted.get(f"image_{i}"))}')

    # ── Apply defaults ─────────────────────────────────────────────────────
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
    test_url = 'https://www.aliexpress.com/item/1005011863842560.html'
    result   = get_product_info(test_url)
    if result:
        print('\n' + '='*80)
        for k, v in result.items():
            if v and not k.startswith('image_'):
                print(f'  {k:30s}: {str(v)[:100]}')
        print(f'  {"images":30s}: {sum(1 for i in range(1, 21) if result.get(f"image_{i}"))}')
    else:
        print('FAILED')
