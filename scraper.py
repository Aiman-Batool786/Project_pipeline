"""
scraper.py v6 - COMPLETE
─────────────────────────
- Product page scraping (existing functionality)
- Search results scraping with pagination (NEW)
"""

import re
import json
import random
import time
import urllib.request
import concurrent.futures
from typing import List, Dict, Optional
from camoufox.sync_api import Camoufox
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

SPEC_MAPPING = {
    'brand':             ['brand', 'brand name', 'marque', 'manufacturer', 'marka'],
    'color':             ['color', 'colour', 'main color', 'couleur', 'kolor'],
    'dimensions':        ['dimensions', 'size', 'product size', 'package size',
                          'item size', 'product dimensions', 'wymiary'],
    'weight':            ['weight', 'net weight', 'gross weight', 'poids', 'waga'],
    'material':          ['material', 'materials', 'composition', 'matiere', 'materiał'],
    'country_of_origin': ['origin', 'country of origin', 'made in',
                          'country/region of manufacture', 'kraj pochodzenia'],
    'warranty':          ['warranty', 'garantie', 'warranty period',
                          'warranty type', 'gwarancja'],
    'certifications':    ['certification', 'certifications', 'certificate',
                          'compliance', 'standard', 'ce', 'rohs', 'certyfikat'],
    'product_type':      ['product type', 'type', 'item type', 'style',
                          'category', 'typ produktu'],
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

MAX_RETRIES  = 3

CATEGORY_ID_MAP = {
    '5090301':   'Cell Phones',
    '509':       'Phones & Telecommunications',
    '202238810': 'Cell Phones (Refurbished)',
    '202238004': 'Consumer Electronics',
    '200000345': 'Women\'s Clothing',
    '200000346': 'Men\'s Clothing',
    '200003655': 'Tablets',
    '100006654': 'Smart Watches',
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _s(v) -> str:
    if v is None:
        return ''
    s = str(v).strip()
    return '' if s in ('None', 'null', '0', 'false', 'undefined') else s


def _safe(d, *keys, default=None):
    cur = d
    for k in keys:
        if isinstance(cur, dict):
            cur = cur.get(k, default)
        elif isinstance(cur, list) and isinstance(k, int):
            cur = cur[k] if k < len(cur) else default
        else:
            return default
        if cur is None:
            return default
    return cur


# ─────────────────────────────────────────────────────────────────────────────
# SEARCH RESULTS SCRAPING (NEW)
# ─────────────────────────────────────────────────────────────────────────────

def extract_product_id_from_url(url: str) -> Optional[str]:
    """Extract product ID from URL - supports /item/ format only"""
    patterns = [
        r'/item/(\d+)\.html',  # Matches: /item/1005009980609725.html
        r'/item/(\d+)',         # Matches: /item/1005009980609725
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def extract_products_from_search_html(html: str) -> List[Dict]:
    """
    Extract products from search result HTML
    Returns list of dicts with product_id, product_url, title
    """
    products = []
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find all product cards - based on your HTML structure
    product_cards = soup.select('[data-tticheck="true"] .lw_b, .search-card-item')
    
    for card in product_cards:
        # Find the product link
        link = card.get('href') or (card.find('a') and card.find('a').get('href'))
        if not link:
            continue
        
        # Ensure full URL
        if link.startswith('//'):
            link = 'https:' + link
        
        # Extract product ID from URL - ONLY accept /item/ format
        product_id = extract_product_id_from_url(link)
        
        # Skip if not a valid /item/ URL (reject /srs/ URLs)
        if not product_id or '/item/' not in link:
            continue
        
        # Extract title from your HTML structure
        title = ''
        title_elem = card.select_one('.lw_k4, .item-title, [class*="title"]')
        if title_elem:
            title = title_elem.get_text(strip=True)
            title = re.sub(r'<font[^>]*>.*?</font>', '', title, flags=re.DOTALL)
            title = re.sub(r'\s+', ' ', title).strip()
        else:
            title_elem = card.select_one('h3, [role="heading"]')
            if title_elem:
                title = title_elem.get_text(strip=True)
                title = re.sub(r'\s+', ' ', title).strip()
        
        if not title:
            continue
        
        products.append({
            'product_id': product_id,
            'product_url': f"https://pl.aliexpress.com/item/{product_id}.html",
            'title': title
        })
    
    # Alternative approach if the above didn't find anything
    if not products:
        product_links = soup.select('a[href*="/item/"]')
        for link in product_links:
            href = link.get('href', '')
            if href.startswith('//'):
                href = 'https:' + href
            
            product_id = extract_product_id_from_url(href)
            if product_id and '/item/' in href:
                title = ''
                parent = link.find_parent()
                if parent:
                    title_elem = parent.select_one('h3, [class*="title"], [role="heading"]')
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        title = re.sub(r'\s+', ' ', title).strip()
                
                if title:
                    products.append({
                        'product_id': product_id,
                        'product_url': f"https://pl.aliexpress.com/item/{product_id}.html",
                        'title': title
                    })
    
    return products


def find_next_page_url(html: str, current_url: str) -> Optional[str]:
    """Find next page URL from pagination"""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Look for next page button
    next_selectors = [
        '.comet-pagination-next a',
        '.comet-pagination-next button',
        'a[rel="next"]',
        '.pagination-next a',
        '[aria-label="Next page"]',
        '.next-page a',
    ]
    
    for selector in next_selectors:
        next_elem = soup.select_one(selector)
        if next_elem:
            href = next_elem.get('href')
            if href:
                if href.startswith('//'):
                    href = 'https:' + href
                return href
    
    # Check if pagination shows "Next" button is disabled
    disabled_next = soup.select_one('.comet-pagination-next.comet-pagination-disabled')
    if disabled_next:
        return None
    
    # Try to extract page number from URL and increment
    page_match = re.search(r'[?&]page=(\d+)', current_url)
    if page_match:
        current_page = int(page_match.group(1))
        next_page = current_page + 1
        
        if '?' in current_url:
            next_url = re.sub(r'page=\d+', f'page={next_page}', current_url)
        else:
            next_url = current_url + f'?page={next_page}'
        
        return next_url
    
    return None


def scrape_search_page(url: str, delay: float = 1.0) -> Dict:
    """Scrape a single search page and return products and HTML"""
    result = {
        'products': [],
        'html': '',
        'next_url': None,
        'has_more': False,
        'url': url,
        'success': False,
        'error': None
    }
    
    ua = random.choice(USER_AGENTS)
    
    try:
        with Camoufox(headless=True, os='windows') as browser:
            context = browser.new_context(
                viewport={'width': 1440, 'height': 900},
                locale='en-US',
                user_agent=ua,
                extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'}
            )
            page = context.new_page()
            
            print(f"[scraper] 📄 Loading search page: {url}")
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            
            time.sleep(delay)
            
            # Scroll to load lazy-loaded content
            for _ in range(3):
                page.mouse.wheel(0, random.randint(400, 800))
                time.sleep(random.uniform(0.5, 1.0))
            
            html = page.content()
            result['html'] = html
            
            products = extract_products_from_search_html(html)
            result['products'] = products
            
            next_url = find_next_page_url(html, url)
            result['next_url'] = next_url
            result['has_more'] = next_url is not None
            
            result['success'] = True
            print(f"[scraper] ✅ Found {len(products)} products from search")
            
            page.close()
            context.close()
            
    except Exception as e:
        print(f"[scraper] ❌ Search page error: {e}")
        result['error'] = str(e)
    
    return result


def scrape_search_results(search_url: str, max_pages: Optional[int] = None, delay: float = 1.0) -> List[Dict]:
    """
    Scrape all pages of search results
    Returns list of unique products (deduplicated by product_id)
    """
    all_products = []
    seen_ids = set()
    current_url = search_url
    page_num = 1
    
    print(f"\n{'='*60}")
    print(f"🔍 Starting search scrape")
    print(f"   URL: {search_url}")
    print(f"   Max pages: {max_pages if max_pages else 'unlimited'}")
    print(f"{'='*60}\n")
    
    while current_url:
        print(f"📄 Scraping page {page_num}...")
        
        result = scrape_search_page(current_url, delay)
        
        if not result['success']:
            print(f"   ❌ Failed: {result['error']}")
            break
        
        new_count = 0
        for product in result['products']:
            pid = product['product_id']
            if pid not in seen_ids:
                seen_ids.add(pid)
                all_products.append(product)
                new_count += 1
        
        print(f"   ✅ Found {len(result['products'])} products ({new_count} new)")
        print(f"   📊 Total unique: {len(all_products)}")
        
        if max_pages and page_num >= max_pages:
            print(f"\n🏁 Reached max pages ({max_pages})")
            break
        
        if not result['has_more']:
            print(f"\n🏁 No more pages")
            break
        
        current_url = result['next_url']
        page_num += 1
        
        if current_url:
            time.sleep(delay + random.uniform(0.5, 1.5))
    
    print(f"\n{'='*60}")
    print(f"✅ Search scrape complete!")
    print(f"   Pages scraped: {page_num}")
    print(f"   Total products: {len(all_products)}")
    print(f"{'='*60}\n")
    
    return all_products


# ─────────────────────────────────────────────────────────────────────────────
# SELLER PARSER (Existing)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_seller_block(store: dict) -> dict:
    if not store or not isinstance(store, dict):
        return {}
    sid = _s(
        store.get('storeNum') or store.get('sellerId') or
        store.get('storeId') or store.get('shopId') or
        store.get('userId') or store.get('memberId')
    )
    result = {
        'store_name':            _s(store.get('storeName') or store.get('sellerName') or
                                    store.get('shopName') or store.get('name')),
        'store_id':              sid,
        'seller_id':             _s(store.get('sellerId') or store.get('userId') or
                                    store.get('memberId')),
        'store_url':             _s(store.get('storeUrl') or store.get('shopUrl') or
                                    (f"https://www.aliexpress.com/store/{sid}" if sid else '')),
        'seller_country':        _s(store.get('country') or store.get('countryCompleteName') or
                                    store.get('shopCountry')),
        'seller_rating':         _s(store.get('positiveRate') or store.get('itemAs') or
                                    store.get('sellerRating')),
        'seller_positive_rate':  _s(store.get('positiveRate') or
                                    store.get('positiveFeedbackRate')),
        'seller_communication':  _s(store.get('communicationRating') or
                                    store.get('serviceAs') or store.get('communicationScore')),
        'seller_shipping_speed': _s(store.get('shippingRating') or store.get('shippingAs') or
                                    store.get('shippingScore')),
        'store_open_date':       _s(store.get('openTime') or store.get('openDate') or
                                    store.get('establishedDate')),
        'seller_level':          _s(store.get('sellerLevel') or store.get('shopLevel') or
                                    store.get('level')),
        'seller_total_reviews':  _s(store.get('totalEvaluationNum') or store.get('reviewNum') or
                                    store.get('feedbackCount')),
        'seller_positive_num':   _s(store.get('positiveNum') or
                                    store.get('positiveFeedbackNum')),
        'is_top_rated':          _s(store.get('isTopRatedSeller') or
                                    store.get('topRatedSeller')),
    }
    return {k: v for k, v in result.items() if v}


# ─────────────────────────────────────────────────────────────────────────────
# MTOP API RESPONSE PARSER (Existing)
# ─────────────────────────────────────────────────────────────────────────────

def parse_mtop_response(text: str) -> dict | None:
    try:
        m = re.match(r'^[a-zA-Z0-9_$]+\((.*)\);?\s*$', text.strip(), re.DOTALL)
        if m:
            text = m.group(1)

        outer  = json.loads(text)
        result = (
            _safe(outer, 'data', 'result') or
            _safe(outer, 'data', 'data') or
            _safe(outer, 'data') or {}
        )
        if not result or not isinstance(result, dict):
            return None

        print(f"[scraper] 📦 mtop result keys: {list(result.keys())[:15]}")
        extracted = {}

        # Title
        for tk in ['titleModule', 'TITLE', 'GLOBAL_DATA']:
            tb = result.get(tk, {})
            if isinstance(tb, dict):
                t = (_s(tb.get('subject')) or _s(tb.get('title')) or
                     _s(_safe(tb, 'globalData', 'subject')))
                if t:
                    extracted['title'] = t
                    break

        # Price
        pm = result.get('priceModule') or result.get('PRICE') or {}
        if isinstance(pm, dict):
            extracted['price'] = _s(
                pm.get('formatedActivityPrice') or pm.get('formatedPrice') or
                pm.get('formattedPrice') or
                _safe(pm, 'minActivityAmount', 'formattedAmount')
            )

        # Images
        im = result.get('imageModule') or result.get('IMAGE') or {}
        if isinstance(im, dict):
            for idx, url in enumerate((im.get('imagePathList') or [])[:20], 1):
                if url:
                    url = ('https:' + url) if str(url).startswith('//') else url
                    extracted[f'image_{idx}'] = url

        if not extracted.get('image_1'):
            tm = result.get('titleModule') or {}
            for idx, url in enumerate((tm.get('images') or [])[:20], 1):
                if url:
                    extracted[f'image_{idx}'] = url

        # Specs
        props = []
        for sk in ['PRODUCT_PROP_PC', 'specsModule', 'productPropComponent', 'skuModule']:
            sb = result.get(sk, {})
            if isinstance(sb, dict):
                raw = (sb.get('props') or sb.get('showedProps') or
                       sb.get('specsList') or sb.get('productSKUPropertyList') or [])
                if raw:
                    props = raw
                    print(f"[scraper] 📋 {len(props)} specs from '{sk}'")
                    break

        if props:
            extracted['specs_raw'] = {}
            for prop in props:
                if not isinstance(prop, dict):
                    continue
                name  = _s(prop.get('attrName') or prop.get('name')).lower()
                value = _s(prop.get('attrValue') or prop.get('value'))
                if not name or not value:
                    continue
                for field, keywords in SPEC_MAPPING.items():
                    if any(kw in name for kw in keywords):
                        extracted.setdefault(field, value)
                        break
                extracted['specs_raw'][name] = value

        # Seller from API
        for sk in ['storeModule', 'SHOP_CARD_PC', 'sellerModule', 'shopInfo']:
            sb = result.get(sk, {})
            if isinstance(sb, dict) and sb:
                seller = _parse_seller_block(sb)
                if seller.get('store_name') or seller.get('store_id'):
                    extracted.update(seller)
                    print(f"[scraper] 🏪 Seller from mtop '{sk}': {list(seller.keys())}")
                    break

        # Category
        cat_mod = result.get('categoryModule') or result.get('CATEGORY') or {}
        if isinstance(cat_mod, dict):
            cat_list = (cat_mod.get('categories') or cat_mod.get('breadcrumbList') or
                        cat_mod.get('list') or [])
            if cat_list and isinstance(cat_list, list):
                leaf = cat_list[-1]
                if isinstance(leaf, dict):
                    extracted['category_id']   = _s(leaf.get('categoryId') or leaf.get('id'))
                    extracted['category_name'] = _s(leaf.get('name') or leaf.get('title'))
                    extracted['category_path'] = ' > '.join(
                        _s(c.get('name') or c.get('title'))
                        for c in cat_list if isinstance(c, dict)
                    )

        # Rating/Reviews
        fb = result.get('feedbackModule') or result.get('FEEDBACK') or {}
        if isinstance(fb, dict):
            extracted['rating']  = _s(fb.get('trialRating') or fb.get('averageStar'))
            extracted['reviews'] = _s(fb.get('trialNum') or fb.get('totalCount'))

        # Description URL
        dm = result.get('descriptionModule') or result.get('DESCRIPTION') or {}
        if isinstance(dm, dict):
            du = _s(dm.get('descriptionUrl'))
            if du:
                extracted['_description_url'] = du

        # Bullet points
        for path_fn in [
            lambda r: r.get('highlights') or _safe(r, 'highlightModule', 'highlightList'),
            lambda r: _safe(r, 'tradeModule', 'highlights'),
        ]:
            try:
                items = path_fn(result)
                if items and isinstance(items, list):
                    bullets = [_s(h.get('title') or h.get('text') or h) for h in items if h]
                    bullets = [b for b in bullets if b]
                    if bullets:
                        extracted['bullet_points'] = bullets[:10]
                        break
            except Exception:
                pass

        return extracted if extracted.get('title') else None

    except Exception as e:
        print(f"[scraper] ⚠️ mtop parse error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# HTML INIT_DATA PARSER (Existing fallback)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_from_html_init_data(html: str) -> dict:
    if not html:
        return {}
    try:
        start_marker = '/*!-->init-data-start--*/'
        end_marker   = '/*!-->init-data-end--*/'
        s = html.find(start_marker)
        e = html.find(end_marker)
        if s != -1 and e != -1:
            block    = html[s + len(start_marker):e]
            assign   = block.find('window._dida_config_._init_data_=')
            if assign != -1:
                json_start = block.index('{', assign)
                depth = 0
                json_end = json_start
                for i, ch in enumerate(block[json_start:], json_start):
                    if ch == '{':
                        depth += 1
                    elif ch == '}':
                        depth -= 1
                        if depth == 0:
                            json_end = i + 1
                            break
                outer = json.loads(block[json_start:json_end])
                inner = _safe(outer, 'data', 'data') or _safe(outer, 'data') or outer
                print(f"[scraper] 📄 init_data keys: {list(inner.keys())[:10]}")
                return parse_mtop_response(
                    json.dumps({'data': {'result': inner}})
                ) or {}
    except Exception as e:
        print(f"[scraper] ⚠️ init_data parse error: {e}")
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# COMPLIANCE MODAL EXTRACTOR (Existing)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_compliance_text(text: str) -> dict:
    result = {}
    text   = text.replace('\r\n', '\n').replace('\r', '\n')

    def extract_field(label_patterns: list, src: str, stop_patterns: list = None) -> str:
        for pat in label_patterns:
            m = re.search(
                r'(?:' + re.escape(pat) + r')\s*[:\n]\s*([^\n]+)',
                src, re.IGNORECASE
            )
            if m:
                val = m.group(1).strip()
                if stop_patterns:
                    for sp in stop_patterns:
                        pos = re.search(re.escape(sp), val, re.IGNORECASE)
                        if pos:
                            val = val[:pos.start()].strip()
                return val
        return ''

    result['manufacturer_name']    = extract_field(
        ['Name', 'Imię i nazwisko', 'Manufacturer Name'], text)
    result['manufacturer_address'] = extract_field(
        ['Address', 'Adres'], text,
        stop_patterns=['Email', 'Telephone', 'Phone', 'Numer', 'Adres e-mail'])
    result['manufacturer_email']   = extract_field(
        ['Email address', 'Adres e-mail', 'Email'], text)
    result['manufacturer_phone']   = extract_field(
        ['Telephone number', 'Numer telefonu', 'Phone', 'Tel'], text)

    eu_match = re.search(
        r'(Details of the person responsible|person responsible for compliance in the EU|'
        r'Osoba odpowiedzialna|EU Representative|EU Responsible)',
        text, re.IGNORECASE
    )
    if eu_match:
        eu_text = text[eu_match.start():]
        result['eu_responsible_name']    = extract_field(
            ['Name', 'Imię i nazwisko', 'Nazwa'], eu_text)
        result['eu_responsible_address'] = extract_field(
            ['Address', 'Adres'], eu_text,
            stop_patterns=['Email', 'Telephone', 'Numer', 'Adres e-mail'])
        result['eu_responsible_email']   = extract_field(
            ['Email address', 'Adres e-mail', 'Email'], eu_text)
        result['eu_responsible_phone']   = extract_field(
            ['Telephone number', 'Numer telefonu', 'Phone'], eu_text)

    pid_m = re.search(r'Product ID[^\n]*\n\s*([0-9][-0-9]+)', text, re.IGNORECASE)
    if pid_m:
        result['compliance_product_id'] = pid_m.group(1).strip()
    else:
        m = re.search(r'(\d{13,20}(?:-\d{13,20})?)', text)
        if m:
            result['compliance_product_id'] = m.group(1)

    return {k: v for k, v in result.items() if v}


def _extract_compliance_info(page) -> dict:
    compliance = {}

    selectors = [
        'text="Product Compliance Information"',
        'span:has-text("Product Compliance Information")',
        'a:has-text("Product Compliance Information")',
        'text="Informacje o zgodności produktu"',
        'span:has-text("Informacje o zgodności")',
        '[data-spm-anchor-id*="i7"]',
        '[class*="compliance"]',
        '[class*="Compliance"]',
    ]

    clicked = False
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=2000):
                el.click(timeout=3000)
                page.wait_for_timeout(2000)
                print(f"[scraper] 🔒 Clicked compliance: {sel}")
                clicked = True
                break
        except Exception:
            continue

    if not clicked:
        print("[scraper] ℹ️ No compliance link found")
        return {}

    try:
        page.wait_for_selector('.comet-v2-modal, .comet-modal', timeout=6000)
        page.wait_for_timeout(1000)
        modal = page.locator('.comet-v2-modal, .comet-modal').first
        if modal.count() == 0:
            return {}
        modal_text = modal.inner_text()
        print(f"[scraper] 🔒 Compliance modal ({len(modal_text)} chars)")
        compliance = _parse_compliance_text(modal_text)
        print(f"[scraper] ✅ Compliance fields: {list(compliance.keys())}")
    except Exception as e:
        print(f"[scraper] ⚠️ Compliance modal error: {e}")

    try:
        page.locator(
            '.comet-v2-modal-close, .comet-modal-close, button[aria-label="Close"]'
        ).first.click(timeout=2000)
    except Exception:
        pass

    return compliance


def _extract_seller_from_dom(page) -> dict:
    seller = {}

    try:
        for link in page.locator('a[href*="/store/"]').all()[:5]:
            href = link.get_attribute('href') or ''
            if '/store/' in href:
                if href.startswith('//'):
                    href = 'https:' + href
                m = re.search(r'/store/(\d+)', href)
                if m:
                    seller['store_id']  = m.group(1)
                    seller['store_url'] = f"https://www.aliexpress.com/store/{m.group(1)}"
                    print(f"[scraper] ✅ DOM store ID: {m.group(1)}")
                    break
    except Exception:
        pass

    for sel in [
        '[class*="store-header--storeName"]', '[class*="shopName"]',
        '[class*="shop-name"]', '[class*="sellerName"]',
        'a[href*="/store/"] span', '[class*="store-info"] h3',
    ]:
        try:
            el = page.locator(sel).first
            if el.count() > 0:
                text = el.inner_text().strip()
                if text and 2 < len(text) < 100:
                    seller['store_name'] = text
                    print(f"[scraper] ✅ DOM store name: {text}")
                    break
        except Exception:
            continue

    info_patterns = {
        'store_open_date': [
            r'(?:Opening date|Opened|Since|Data otwarcia)[:\s]+([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})',
        ],
        'seller_country': [
            r'(?:Location|Country|Kraj)[:\s]+([A-Za-z\s]+)',
        ],
    }

    try:
        page_text = page.locator('body').inner_text()
        for field, patterns in info_patterns.items():
            if field in seller:
                continue
            for pat in patterns:
                m = re.search(pat, page_text, re.IGNORECASE)
                if m:
                    val = m.group(1).strip()
                    if val:
                        seller[field] = val
                        print(f"[scraper] ✅ DOM {field}: {val}")
                        break
    except Exception:
        pass

    return {k: v for k, v in seller.items() if v}


def _dismiss_gdpr_banner(page) -> bool:
    selectors = [
        'button:has-text("Accept All")', 'button:has-text("Accept all cookies")',
        'button:has-text("Agree")', 'button:has-text("I Accept")',
        'button:has-text("Akceptuj")', 'button:has-text("Akceptuję")',
        '#accept-all', '.accept-all', '[data-testid="accept-all"]',
        '[class*="gdpr"] button', '[class*="cookie"] button',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=1500):
                el.click(timeout=2000)
                page.wait_for_timeout(800)
                print(f"[scraper] 🍪 GDPR dismissed: {sel}")
                return True
        except Exception:
            continue
    return False


def _is_eu_url(url: str) -> bool:
    eu_domains = ['pl.aliexpress.com', 'de.aliexpress.com', 'fr.aliexpress.com',
                  'it.aliexpress.com', 'es.aliexpress.com', 'nl.aliexpress.com']
    return any(d in url for d in eu_domains)


def fetch_description(url: str) -> str:
    try:
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.aliexpress.com/',
            }
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode('utf-8', errors='replace')
        clean = re.sub(r'<style[^>]*>.*?</style>', ' ', raw, flags=re.DOTALL)
        clean = re.sub(r'<script[^>]*>.*?</script>', ' ', clean, flags=re.DOTALL)
        clean = re.sub(r'<[^>]+>', ' ', clean)
        clean = re.sub(r'\s+', ' ', clean).strip()
        return clean[:3000]
    except Exception as e:
        print(f"[scraper] ⚠️ Description fetch failed: {e}")
        return ''


def _scrape_in_thread(url: str, try_compliance: bool = False) -> dict:
    captured   = []
    html       = ''
    dom_seller = {}
    compliance = {}

    ua    = random.choice(USER_AGENTS)
    is_eu = _is_eu_url(url)
    print(f"[scraper] 🌐 EU={is_eu} | {url[:80]}")

    try:
        with Camoufox(headless=True, os='windows') as browser:
            context = browser.new_context(
                viewport={'width': 1440, 'height': 900},
                locale='en-US',
                user_agent=ua,
                extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'}
            )
            page = context.new_page()

            def handle_response(response):
                try:
                    resp_url = response.url
                    if (('mtop.aliexpress.pdp.pc.query' in resp_url or
                         'mtop.aliexpress.itemdetail' in resp_url or
                         'pdp.pc.query' in resp_url) and
                            response.status == 200):
                        body = response.body()
                        if len(body) < 1000:
                            return
                        text = body.decode('utf-8', errors='replace')
                        if any(x in text for x in [
                            'titleModule', 'storeModule', 'SHOP_CARD_PC',
                            'imageModule', 'priceModule', '"subject"', '"storeName"'
                        ]):
                            captured.append(text)
                            print(f"[scraper] 📡 Captured API ({len(text):,} bytes)")
                except Exception:
                    pass

            page.on('response', handle_response)
            page.goto(url, timeout=90_000, wait_until='domcontentloaded')

            if is_eu:
                page.wait_for_timeout(2000)
                _dismiss_gdpr_banner(page)
                page.wait_for_timeout(1000)

            page.wait_for_timeout(random.randint(5000, 7000))

            for _ in range(4):
                page.mouse.wheel(0, random.randint(400, 800))
                page.wait_for_timeout(random.randint(500, 900))
            page.wait_for_timeout(2000)

            dom_seller = _extract_seller_from_dom(page)

            if try_compliance:
                print("[scraper] 🔒 Extracting compliance info...")
                compliance = _extract_compliance_info(page)

            html = page.content()
            page.close()
            context.close()

    except Exception as e:
        print(f"[scraper] ❌ Browser error: {e}")
       
