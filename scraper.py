from playwright.sync_api import sync_playwright
import re
import json


def extract_from_meta_tags(page):
    """Extract data from meta tags (most reliable)"""
    data = {}
    
    # Title from og:title
    try:
        title_meta = page.locator('meta[property="og:title"]').get_attribute('content')
        data['title'] = title_meta or ""
    except:
        data['title'] = ""
    
    # Main image from og:image
    try:
        image_meta = page.locator('meta[property="og:image"]').get_attribute('content')
        data['image_1'] = image_meta or ""
    except:
        data['image_1'] = ""
    
    # Description from og:description (FALLBACK ONLY)
    try:
        desc_meta = page.locator('meta[property="og:description"]').get_attribute('content')
        data['description'] = desc_meta or ""
    except:
        data['description'] = ""
    
    return data


def extract_from_javascript(page):
    """Extract data from embedded JavaScript"""
    data = {}
    
    try:
        # Get all script content
        scripts = page.locator('script').all()
        
        for script in scripts:
            try:
                script_text = script.text_content()
                
                # Look for imagePathList
                image_match = re.search(r'"imagePathList":\s*\[(.*?)\]', script_text)
                if image_match:
                    images_str = image_match.group(1)
                    images = re.findall(r'"(https://[^"]+\.jpg[^"]*)"', images_str)
                    
                    for i, img in enumerate(images[:6], 1):
                        key = f'image_{i}'
                        data[key] = img.split('_')[0] + '.jpg' if '_' in img else img
                
                # Look for product price
                price_match = re.search(r'"price":\s*"([^"]+)"', script_text)
                if price_match:
                    data['price'] = price_match.group(1)
                
                # Look for trade quantity
                trade_match = re.search(r'"tradeCount":\s*"([^"]+)"', script_text)
                if trade_match:
                    data['sales_count'] = trade_match.group(1)
                
                # Look for shipping info
                ship_match = re.search(r'"shipmentWay":\s*"([^"]+)"', script_text)
                if ship_match:
                    data['shipping'] = ship_match.group(1)
                
            except:
                pass
    
    except Exception as e:
        print(f"[scraper] Warning extracting JS data: {e}")
    
    return data


def extract_from_dom(page):
    """Extract data from DOM elements"""
    data = {}
    
    # ========================
    # TITLE
    # ========================
    title_selectors = [
        '[data-pl="product-title"]',
        '.title--line-one',
        'h1',
        '.product-title'
    ]
    
    for selector in title_selectors:
        try:
            if page.locator(selector).count() > 0:
                title = page.locator(selector).first.inner_text().strip()
                if title and len(title) > 5:
                    data['title'] = title
                    break
        except:
            pass
    
    # ========================
    # DESCRIPTION (FIXED - From detailmodule_text)
    # ========================
    
    page.mouse.wheel(0, 3000)
    page.wait_for_timeout(1000)
    
    description = ""
    
    # PRIMARY: Extract from detailmodule_text section (most comprehensive)
    try:
        detail_modules = page.locator('div.detailmodule_text').all()
        if detail_modules:
            detail_texts = []
            
            for module in detail_modules:
                try:
                    # Try to get span.detail-desc-decorate-content inside this module
                    spans = module.locator('span.detail-desc-decorate-content').all()
                    if spans:
                        for span in spans:
                            text = span.inner_text().strip()
                            if text and len(text) > 5:
                                detail_texts.append(text)
                    
                    # Also try direct text from module
                    if not spans:
                        module_text = module.inner_text().strip()
                        if module_text and len(module_text) > 20:
                            detail_texts.append(module_text)
                except:
                    pass
            
            if detail_texts:
                # Join all descriptions with pipe separator
                description = " | ".join(detail_texts)
    except:
        pass
    
    # SECONDARY: Extract from any span with detail-desc-decorate-content (orphaned spans)
    if not description or len(description) < 50:
        try:
            desc_spans = page.locator('span.detail-desc-decorate-content').all()
            if desc_spans:
                span_texts = []
                
                for span in desc_spans:
                    text = span.inner_text().strip()
                    if text and len(text) > 5:
                        span_texts.append(text)
                
                if span_texts:
                    description = " | ".join(span_texts)
        except:
            pass
    
    # TERTIARY: Generic description selectors (fallback)
    if not description or len(description) < 50:
        desc_selectors = [
            '[data-pl="product-detail-description"]',
            '.product-description',
            'div[class*="description"]',
            '.detail-desc-text'
        ]
        
        for selector in desc_selectors:
            try:
                if page.locator(selector).count() > 0:
                    desc = page.locator(selector).first.inner_text().strip()
                    if desc and len(desc) > 10:
                        description = desc
                        break
            except:
                pass
    
    # Clean and normalize description
    if description:
        # Replace multiple newlines with pipe separator
        description = re.sub(r'\n+', ' | ', description)
        # Remove excessive whitespace
        description = re.sub(r'\s+', ' ', description).strip()
        # Limit to 2000 characters
        description = description[:2000]
    
    data['description'] = description
    
    # ========================
    # PRICE
    # ========================
    price_selectors = [
        '[data-pl="product-price"]',
        '.price-main',
        '.product-price',
        '[class*="price"]'
    ]
    
    for selector in price_selectors:
        try:
            if page.locator(selector).count() > 0:
                price = page.locator(selector).first.inner_text().strip()
                if price and ('$' in price or any(c.isdigit() for c in price)):
                    data['price'] = price
                    break
        except:
            pass
    
    # ========================
    # BRAND
    # ========================
    brand_selectors = [
        '[data-pl="product-brand"]',
        '.shop-name',
        'a[class*="store"]',
        '.brand-name'
    ]
    
    for selector in brand_selectors:
        try:
            if page.locator(selector).count() > 0:
                brand = page.locator(selector).first.inner_text().strip()
                if brand and len(brand) < 100:
                    data['brand'] = brand
                    break
        except:
            pass
    
    # ========================
    # SPECIFICATIONS
    # ========================
    spec_data = {}
    
    # Try to find specification tables
    try:
        spec_rows = page.locator('[class*="spec"]').all()
        for row in spec_rows[:20]:
            try:
                text = row.inner_text().strip()
                if ':' in text:
                    key, value = text.split(':', 1)
                    spec_data[key.lower().strip()] = value.strip()
            except:
                pass
    except:
        pass
    
    # Map specifications to attributes
    if 'color' in spec_data or 'colour' in spec_data:
        data['color'] = spec_data.get('color') or spec_data.get('colour')
    
    if 'size' in spec_data or 'dimensions' in spec_data:
        data['dimensions'] = spec_data.get('size') or spec_data.get('dimensions')
    
    if 'weight' in spec_data:
        data['weight'] = spec_data.get('weight')
    
    if 'material' in spec_data:
        data['material'] = spec_data.get('material')
    
    # ========================
    # BULLET POINTS / FEATURES
    # ========================
    bullets = []
    
    try:
        # Try common selectors for bullet points
        bullet_selectors = [
            'ul li',
            '[class*="feature"] li',
            '[class*="highlight"] li'
        ]
        
        for selector in bullet_selectors:
            if page.locator(selector).count() > 0:
                items = page.locator(selector).all_text_contents()
                bullets = [b.strip() for b in items[:8] if b.strip() and len(b.strip()) > 5]
                if bullets:
                    break
    except:
        pass
    
    data['bullet_points'] = bullets
    
    # ========================
    # RATINGS & REVIEWS
    # ========================
    try:
        rating = page.locator('[class*="rating"]').first.inner_text().strip()
        if rating:
            data['rating'] = rating
    except:
        pass
    
    try:
        reviews = page.locator('[class*="review"]').first.inner_text().strip()
        if reviews:
            data['reviews'] = reviews
    except:
        pass
    
    # ========================
    # SHIPPING INFO
    # ========================
    try:
        shipping = page.locator('[class*="ship"]').first.inner_text().strip()
        if shipping:
            data['shipping'] = shipping
    except:
        pass
    
    return data


def get_product_info(url):
    """
    Main scraper function - optimized for AliExpress
    
    Priority:
    1. Meta tags (most reliable)
    2. JavaScript embedded data
    3. DOM elements
    """
    
    try:
        with sync_playwright() as p:
            
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            )
            
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )
            
            page = context.new_page()
            
            print(f"[scraper] Opening: {url}")
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            
            # Simulate human behavior
            page.wait_for_timeout(3000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(2000)
            
            # =====================================================
            # EXTRACT DATA - Priority Order
            # =====================================================
            
            print("[scraper] Extracting from meta tags...")
            data = extract_from_meta_tags(page)
            
            print("[scraper] Extracting from JavaScript...")
            js_data = extract_from_javascript(page)
            data.update({k: v for k, v in js_data.items() if v and k not in data})
            
            print("[scraper] Extracting from DOM...")
            dom_data = extract_from_dom(page)
            data.update({k: v for k, v in dom_data.items() if v and k not in data})
            
            browser.close()
            
            # =====================================================
            # VALIDATION
            # =====================================================
            
            if not data.get('title'):
                print("[scraper] ERROR: No title extracted")
                return None
            
            print(f"[scraper] ✅ Successfully extracted {len(data)} attributes")
            
            # Add defaults for missing fields
            for key in ['description', 'brand', 'color', 'dimensions', 'weight', 
                       'material', 'shipping', 'price', 'rating', 'reviews']:
                if key not in data or not data[key]:
                    data[key] = ""
            
            # Ensure images list
            for i in range(1, 7):
                if f'image_{i}' not in data:
                    data[f'image_{i}'] = ""
            
            return data
    
    except Exception as e:
        print(f"[scraper] ERROR: {e}")
        return None


# ============================================================
# TEST
# ============================================================

if __name__ == "__main__":
    # Test URL
    test_url = "https://www.aliexpress.com/item/1005010738806664.html"
    
    result = get_product_info(test_url)
    
    if result:
        print("\n=== SCRAPED DATA ===")
        for key, value in result.items():
            if isinstance(value, list):
                print(f"{key}: {len(value)} items")
            else:
                print(f"{key}: {str(value)[:80]}")
    else:
        print("Failed to scrape")
