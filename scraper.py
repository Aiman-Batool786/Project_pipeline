from playwright.sync_api import sync_playwright
import re
import json


def extract_description_correct(page):
    """
    Extract description using CORRECT selectors from inspected HTML
    
    Primary selectors (in priority order):
    1. div.richTextContainer[data-rich-text-render="true"] - BEST (full description)
    2. div[id="product-description"] - Good fallback
    3. div[id="nav-description"] - Section wrapper
    4. div.detailmodule_text - Fallback
    5. p.detail-desc-decorate-content - Individual paragraphs
    """
    
    description = ""
    print("[scraper] 📝 Extracting description (corrected selectors)...")
    
    # METHOD 1: richTextContainer (PRIMARY - has most complete description)
    try:
        rich_container = page.locator('div.richTextContainer[data-rich-text-render="true"]')
        if rich_container.count() > 0:
            text = rich_container.first.inner_text().strip()
            if text and len(text) > 50:
                description = text
                print(f"[scraper]    ✅ Method 1: richTextContainer ({len(description)} chars)")
                return description
    except Exception as e:
        print(f"[scraper]    ⚠️  Method 1 error: {e}")
    
    # METHOD 2: product-description div
    try:
        prod_desc = page.locator('div[id="product-description"]')
        if prod_desc.count() > 0:
            text = prod_desc.first.inner_text().strip()
            if text and len(text) > 50:
                description = text
                print(f"[scraper]    ✅ Method 2: product-description ({len(description)} chars)")
                return description
    except Exception as e:
        print(f"[scraper]    ⚠️  Method 2 error: {e}")
    
    # METHOD 3: nav-description section
    try:
        nav_desc = page.locator('div[id="nav-description"]')
        if nav_desc.count() > 0:
            text = nav_desc.first.inner_text().strip()
            # Remove header
            text = re.sub(r'^.*?Description\s+report\s+', '', text, flags=re.IGNORECASE | re.DOTALL)
            if text and len(text) > 50 and "Smarter Shopping" not in text:
                description = text
                print(f"[scraper]    ✅ Method 3: nav-description ({len(description)} chars)")
                return description
    except Exception as e:
        print(f"[scraper]    ⚠️  Method 3 error: {e}")
    
    # METHOD 4: detailmodule_text divs
    try:
        detail_modules = page.locator('div.detailmodule_text')
        if detail_modules.count() > 0:
            print(f"[scraper]    Found {detail_modules.count()} detailmodule_text elements")
            for idx in range(detail_modules.count()):
                try:
                    text = detail_modules.nth(idx).inner_text().strip()
                    if text and len(text) > 50:
                        description = text
                        print(f"[scraper]    ✅ Method 4: detailmodule_text[{idx}] ({len(description)} chars)")
                        return description
                except:
                    pass
    except Exception as e:
        print(f"[scraper]    ⚠️  Method 4 error: {e}")
    
    # METHOD 5: detail-desc-decorate-content paragraphs
    try:
        content_para = page.locator('p.detail-desc-decorate-content')
        if content_para.count() > 0:
            print(f"[scraper]    Found {content_para.count()} detail-desc-decorate-content elements")
            description_parts = []
            for idx in range(content_para.count()):
                try:
                    text = content_para.nth(idx).inner_text().strip()
                    if text:
                        description_parts.append(text)
                except:
                    pass
            
            if description_parts:
                description = " | ".join(description_parts)
                print(f"[scraper]    ✅ Method 5: detail-desc-decorate-content ({len(description)} chars)")
                return description
    except Exception as e:
        print(f"[scraper]    ⚠️  Method 5 error: {e}")
    
    print("[scraper]    ⚠️  WARNING: No description extracted!")
    return ""


def extract_all_images(page):
    """Extract ALL images from product page"""
    images = {}
    
    print("[scraper] 🖼️  Extracting images...")
    
    try:
        scripts = page.locator("script")
        script_count = scripts.count()
        
        for idx in range(script_count):
            try:
                script_text = scripts.nth(idx).text_content()
                
                # Look for imagePathList
                image_match = re.search(r'"imagePathList":\s*\[(.*?)\]', script_text)
                
                if image_match:
                    images_str = image_match.group(1)
                    images_list = re.findall(r'"(https://[^"]+\.jpg[^"]*)"', images_str)
                    
                    for img_idx, img in enumerate(images_list[:20], 1):
                        key = f"image_{img_idx}"
                        clean_url = re.sub(r'_\d+x\d+', '', img)
                        images[key] = clean_url
                    
                    if images:
                        print(f"[scraper]    ✅ Found {len(images)} images from script")
                        return images
            except:
                pass
    
    except Exception as e:
        print(f"[scraper]    ⚠️  Script search error: {e}")
    
    # Fallback: og:image
    try:
        og_image = page.locator('meta[property="og:image"]').get_attribute('content')
        if og_image:
            images['image_1'] = og_image
            print(f"[scraper]    ✅ Found 1 image from meta tag (fallback)")
    except:
        pass
    
    return images


def extract_from_meta_tags(page):
    """Extract title from meta tags"""
    data = {}
    
    try:
        title_meta = page.locator('meta[property="og:title"]').get_attribute("content")
        data["title"] = title_meta or ""
    except:
        data["title"] = ""
    
    return data


def extract_from_javascript(page):
    """Extract price and shipping from JavaScript"""
    data = {}
    
    try:
        scripts = page.locator("script")
        
        for idx in range(scripts.count()):
            try:
                script_text = scripts.nth(idx).text_content()
                
                # Look for product price
                price_match = re.search(r'"price":\s*"([^"]+)"', script_text)
                if price_match:
                    data["price"] = price_match.group(1)
                
                # Look for shipping info
                ship_match = re.search(r'"shipmentWay":\s*"([^"]+)"', script_text)
                if ship_match:
                    data["shipping"] = ship_match.group(1)
            except:
                pass
    
    except Exception as e:
        print(f"[scraper]    ⚠️  JS extraction error: {e}")
    
    return data


def extract_from_dom(page):
    """Extract data from DOM elements"""
    data = {}
    
    # Scroll down to ensure all content is loaded
    page.mouse.wheel(0, 3000)
    page.wait_for_timeout(2000)
    
    # Extract description with corrected selectors
    description = extract_description_correct(page)
    if description:
        data["description"] = description
    
    # Extract images
    images_dict = extract_all_images(page)
    data.update(images_dict)
    
    return data


def get_product_info(url):
    """
    Main scraper function with CORRECTED description extraction
    using selectors from HTML inspection
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
            
            print("[scraper] 🔍 Extracting data...")
            
            # Extract all data
            data = extract_from_meta_tags(page)
            data.update(extract_from_javascript(page))
            data.update(extract_from_dom(page))
            
            browser.close()
            
            # =====================================================
            # VALIDATION
            # =====================================================
            
            if not data.get('title'):
                print("[scraper] ❌ ERROR: No title extracted")
                return None
            
            print(f"\n[scraper] ✅ Successfully extracted product data")
            print(f"[scraper]    Title: {data.get('title', '')[:60]}...")
            print(f"[scraper]    Description: {len(data.get('description', ''))} chars")
            
            image_count = sum(1 for i in range(1, 21) if data.get(f'image_{i}'))
            print(f"[scraper]    Images: {image_count}")
            
            # Add defaults for missing fields
            defaults = {
                'description': '',
                'brand': '',
                'color': '',
                'dimensions': '',
                'weight': '',
                'material': '',
                'shipping': '',
                'price': '',
                'rating': '',
                'reviews': '',
                'bullet_points': [],
                'age_from': '',
                'age_to': '',
                'gender': '',
                'safety_warning': '',
                'certifications': '',
                'country_of_origin': '',
                'warranty': '',
                'product_type': ''
            }
            
            for key, default_val in defaults.items():
                if key not in data or not data[key]:
                    data[key] = default_val
            
            # Ensure image keys exist
            for i in range(1, 21):
                if f'image_{i}' not in data:
                    data[f'image_{i}'] = ""
            
            return data
    
    except Exception as e:
        print(f"[scraper] ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    test_url = "https://www.aliexpress.com/item/1005006156465150.html"
    
    result = get_product_info(test_url)
    
    if result:
        print("\n" + "="*80)
        print("=== SCRAPED DATA ===")
        print("="*80)
        print(f"Title: {result['title']}")
        print(f"\nDescription ({len(result['description'])} chars):")
        print(result['description'][:300] + "..." if len(result['description']) > 300 else result['description'])
        
        image_count = sum(1 for i in range(1, 21) if result.get(f'image_{i}'))
        print(f"\nImages: {image_count}")
    else:
        print("Failed to scrape")
