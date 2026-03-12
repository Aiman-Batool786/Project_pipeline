from playwright.sync_api import sync_playwright
import re
import json


def extract_description_from_detailmodule(page):
    """
    Extract CORRECT description from AliExpress product detail sections
    
    Priority:
    1. div.detailmodule_text - main product description section
    2. Fallback to other generic selectors only if detailmodule_text is empty
    3. NEVER use og:description from meta tags (contains generic Aliexpress text)
    """
    
    description = ""
    
    print("[scraper] 📝 Extracting description from div.detailmodule_text...")
    
    # PRIMARY: Extract ALL content from div.detailmodule_text
    try:
        detail_modules = page.locator('div.detailmodule_text').all()
        if detail_modules:
            print(f"[scraper]    Found {len(detail_modules)} detail module(s)")
            detail_texts = []
            
            for idx, module in enumerate(detail_modules):
                try:
                    # Get all text content from this module (including nested elements)
                    module_text = module.inner_text().strip()
                    if module_text and len(module_text) > 5:
                        detail_texts.append(module_text)
                        print(f"[scraper]    Module {idx+1}: {len(module_text)} chars")
                except Exception as e:
                    print(f"[scraper]    ⚠️  Error reading module {idx+1}: {e}")
            
            if detail_texts:
                # Join all detail modules with pipe separator
                description = " | ".join(detail_texts)
                print(f"[scraper]    ✅ Extracted {len(detail_texts)} sections ({len(description)} total chars)")
    except Exception as e:
        print(f"[scraper]    ⚠️  Error extracting from detailmodule_text: {e}")
    
    # SECONDARY: Try to extract from description section header if primary failed
    if not description or len(description) < 50:
        print("[scraper]    Trying description section header...")
        try:
            # Look for divs with Description title
            desc_sections = page.locator('div.title--text--Otu0bLr').all()
            if desc_sections:
                print(f"[scraper]    Found {len(desc_sections)} description header(s)")
                for section in desc_sections:
                    section_text = section.inner_text().strip()
                    if "description" in section_text.lower():
                        # Get the text following this header
                        next_elem = section.locator('..').inner_text().strip()
                        if next_elem and len(next_elem) > 50:
                            description = next_elem
                            print(f"[scraper]    ✅ Found from header section ({len(description)} chars)")
                            break
        except Exception as e:
            print(f"[scraper]    ⚠️  Error with header section: {e}")
    
    # TERTIARY: Fallback to generic selectors (ONLY if detailmodule_text was empty)
    if not description or len(description) < 50:
        print("[scraper]    Fallback to generic selectors...")
        desc_selectors = [
            '[data-pl="product-detail-description"]',
            '.product-description',
            'div[class*="detail-desc"]',
            '.detail-desc-text'
        ]
        
        for selector in desc_selectors:
            try:
                if page.locator(selector).count() > 0:
                    desc = page.locator(selector).first.inner_text().strip()
                    if desc and len(desc) > 50:
                        description = desc
                        print(f"[scraper]    ✅ Found using selector: {selector} ({len(description)} chars)")
                        break
            except Exception as e:
                print(f"[scraper]    ⚠️  Error with {selector}: {e}")
    
    # Clean and normalize description
    if description:
        # Normalize whitespace and newlines
        description = re.sub(r'\n+', ' | ', description)  # Replace multiple newlines with separator
        description = re.sub(r'\s+', ' ', description)    # Collapse multiple spaces
        description = description.strip()
        
        # Limit to 3000 characters for detailed content
        if len(description) > 3000:
            description = description[:3000]
            print(f"[scraper]    Trimmed to 3000 chars")
    
    if not description:
        print("[scraper]    ⚠️  WARNING: No detailed description found!")
    
    return description


def extract_from_meta_tags(page):
    """
    Extract data from meta tags
    
    ⚠️ NOTE: og:description is SKIPPED - it contains generic Aliexpress text
    Only use og:title and og:image which are reliable
    """
    data = {}
    
    # Title from og:title (RELIABLE)
    try:
        title_meta = page.locator('meta[property="og:title"]').get_attribute('content')
        data['title'] = title_meta or ""
    except:
        data['title'] = ""
    
    # Main image from og:image (RELIABLE)
    try:
        image_meta = page.locator('meta[property="og:image"]').get_attribute('content')
        data['image_1'] = image_meta or ""
    except:
        data['image_1'] = ""
    
    # ⚠️ SKIP og:description - it's generic "Smarter Shopping, Better Living" text
    # Description will be extracted from div.detailmodule_text instead
    
    return data


def extract_from_javascript(page):
    """Extract data from embedded JavaScript"""
    data = {}
    
    try:
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
    # DESCRIPTION (FIXED - Using detailmodule_text)
    # ========================
    page.mouse.wheel(0, 3000)
    page.wait_for_timeout(1500)
    
    description = extract_description_from_detailmodule(page)
    if description:
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
    1. Meta tags (for title and image ONLY)
    2. JavaScript embedded data (images, price)
    3. DOM elements (description from div.detailmodule_text, specs, bullets)
    
    IMPORTANT: Description is extracted from div.detailmodule_text, NOT from og:description meta tag
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
            
            print(f"\n[scraper] 🌐 Opening: {url}")
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            
            # Simulate human behavior
            page.wait_for_timeout(3000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(2000)
            
            # =====================================================
            # EXTRACT DATA - Priority Order
            # =====================================================
            
            print("[scraper] 🔍 Extracting from meta tags (title, image)...")
            data = extract_from_meta_tags(page)
            
            print("[scraper] 🔍 Extracting from JavaScript...")
            js_data = extract_from_javascript(page)
            data.update({k: v for k, v in js_data.items() if v and k not in data})
            
            print("[scraper] 🔍 Extracting from DOM (including description)...")
            dom_data = extract_from_dom(page)
            data.update({k: v for k, v in dom_data.items() if v and k not in data})
            
            browser.close()
            
            # =====================================================
            # VALIDATION
            # =====================================================
            
            if not data.get('title'):
                print("[scraper] ❌ ERROR: No title extracted")
                return None
            
            if not data.get('description'):
                print("[scraper] ⚠️  WARNING: No description extracted")
            
            print(f"\n[scraper] ✅ Successfully extracted {len(data)} attributes")
            print(f"[scraper]    Title: {data.get('title', '')[:50]}...")
            print(f"[scraper]    Description: {len(data.get('description', ''))} chars")
            print(f"[scraper]    Images: {sum(1 for i in range(1, 7) if data.get(f'image_{i}'))}")
            print(f"[scraper]    Bullets: {len(data.get('bullet_points', []))}")
            
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
        print(f"[scraper] ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================
# TEST
# ============================================================

if __name__ == "__main__":
    # Test URL
    test_url = "https://www.aliexpress.com/item/1005010738806664.html"
    
    result = get_product_info(test_url)
    
    if result:
        print("\n" + "="*80)
        print("=== SCRAPED DATA ===")
        print("="*80)
        for key, value in sorted(result.items()):
            if isinstance(value, list):
                print(f"\n{key}: {len(value)} items")
                for item in value[:3]:
                    print(f"  - {item[:70]}")
            else:
                val_str = str(value)[:100] if len(str(value)) > 100 else str(value)
                print(f"{key}: {val_str}")
    else:
        print("❌ Failed to scrape")
