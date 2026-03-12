from playwright.sync_api import sync_playwright
import re
import json


def extract_description_from_nav_section(page):
    """
    Extract description from the nav-description section
    
    HTML Structure:
    <a href="#nav-description" ... title="Description">Description</a>
    
    Then find the section with id="nav-description" and extract all content
    """
    
    description = ""
    
    print("[scraper] 📝 Extracting description from nav-description section...")
    
    try:
        # Find the nav-description section
        nav_desc_sections = page.locator('*[id*="nav-description"]').all()
        
        if nav_desc_sections:
            print(f"[scraper]    Found {len(nav_desc_sections)} nav-description section(s)")
            
            for idx, section in enumerate(nav_desc_sections):
                try:
                    section_text = section.inner_text().strip()
                    
                    if section_text and len(section_text) > 50:
                        # Get all text from this section
                        description = section_text
                        print(f"[scraper]    ✅ Found section {idx+1} with {len(description)} chars")
                        break
                except Exception as e:
                    print(f"[scraper]    ⚠️  Error reading section {idx+1}: {e}")
        
        # Alternative: Find by class pattern
        if not description or len(description) < 50:
            print("[scraper]    Trying alternative selectors...")
            
            # Look for elements with "description" in their data attributes
            alt_selectors = [
                '[data-spm-anchor-id*="description"]',
                'div[class*="description"]',
                '[id*="description"]',
                'section[id*="description"]',
                'div[id*="nav-description"]'
            ]
            
            for selector in alt_selectors:
                try:
                    if page.locator(selector).count() > 0:
                        elements = page.locator(selector).all()
                        for elem in elements:
                            elem_text = elem.inner_text().strip()
                            if elem_text and len(elem_text) > 50:
                                description = elem_text
                                print(f"[scraper]    ✅ Found using selector: {selector}")
                                break
                        
                        if description:
                            break
                except Exception as e:
                    print(f"[scraper]    ⚠️  Error with {selector}: {e}")
    
    except Exception as e:
        print(f"[scraper]    ⚠️  Error: {e}")
    
    # Fallback: Get all divs and find one with description-like content
    if not description or len(description) < 50:
        print("[scraper]    Fallback: Searching all divs for description content...")
        try:
            all_divs = page.locator('div').all()
            for div in all_divs:
                try:
                    div_text = div.inner_text().strip()
                    # Check if this div contains description-like keywords
                    if (len(div_text) > 100 and 
                        any(kw in div_text.lower() for kw in ['features', 'content:', 'size:', 'package', 'material'])):
                        description = div_text
                        print(f"[scraper]    ✅ Found div with description keywords ({len(description)} chars)")
                        break
                except:
                    pass
        except Exception as e:
            print(f"[scraper]    ⚠️  Error in fallback: {e}")
    
    # Clean and normalize description
    if description:
        # Remove "Description" header if present at start
        description = re.sub(r'^description\s*\n?', '', description, flags=re.IGNORECASE).strip()
        
        # Normalize whitespace
        description = re.sub(r'\n+', ' | ', description)
        description = re.sub(r'\s+', ' ', description).strip()
        
        # Limit to 3000 chars
        if len(description) > 3000:
            description = description[:3000]
    
    if not description:
        print("[scraper]    ⚠️  WARNING: No description found!")
    
    return description


def extract_all_images(page):
    """
    Extract ALL images from the product page (not just 6)
    
    Look for:
    1. imagePathList in scripts
    2. img tags with product images
    3. picture tags with srcset
    """
    
    images = {}
    
    print("[scraper] 🖼️  Extracting images...")
    
    # PRIMARY: Extract from JavaScript imagePathList (most reliable)
    try:
        scripts = page.locator('script').all()
        
        for script in scripts:
            try:
                script_text = script.text_content()
                
                # Look for imagePathList
                image_match = re.search(r'"imagePathList":\s*\[(.*?)\]', script_text)
                if image_match:
                    images_str = image_match.group(1)
                    # Extract all image URLs (don't limit to 6!)
                    images_list = re.findall(r'"(https://[^"]+\.jpg[^"]*)"', images_str)
                    
                    for idx, img in enumerate(images_list[:20], 1):  # Allow up to 20 images
                        key = f'image_{idx}'
                        # Clean up image URL (remove size parameters)
                        clean_url = re.sub(r'_\d+x\d+', '', img)
                        images[key] = clean_url
                    
                    if images:
                        print(f"[scraper]    ✅ Found {len(images)} images in imagePathList")
                        return images
            except Exception as e:
                print(f"[scraper]    ⚠️  Error parsing script: {e}")
    
    except Exception as e:
        print(f"[scraper]    ⚠️  Error searching scripts: {e}")
    
    # SECONDARY: Extract from img tags
    if not images or len(images) < 5:
        print("[scraper]    Trying img tags...")
        try:
            img_tags = page.locator('img[src*="alicdn"]').all()
            
            if img_tags:
                print(f"[scraper]    Found {len(img_tags)} img tags")
                
                for idx, img_tag in enumerate(img_tags[:20], 1):
                    try:
                        src = img_tag.get_attribute('src')
                        if src and 'alicdn' in src and '.jpg' in src:
                            key = f'image_{idx}'
                            images[key] = src
                    except:
                        pass
                
                if images:
                    print(f"[scraper]    ✅ Found {len(images)} images from img tags")
                    return images
        except Exception as e:
            print(f"[scraper]    ⚠️  Error with img tags: {e}")
    
    # TERTIARY: Extract from picture tags with srcset
    if not images or len(images) < 5:
        print("[scraper]    Trying picture/source tags...")
        try:
            sources = page.locator('source[srcset*="alicdn"]').all()
            
            if sources:
                print(f"[scraper]    Found {len(sources)} source tags")
                
                for idx, source in enumerate(sources[:20], 1):
                    try:
                        srcset = source.get_attribute('srcset')
                        if srcset:
                            # Extract first image from srcset
                            img_url = srcset.split()[0].strip()
                            if img_url.startswith('http'):
                                key = f'image_{idx}'
                                images[key] = img_url
                    except:
                        pass
                
                if images:
                    print(f"[scraper]    ✅ Found {len(images)} images from source tags")
        except Exception as e:
            print(f"[scraper]    ⚠️  Error with source tags: {e}")
    
    # Default: Use meta og:image if no other images found
    if not images:
        print("[scraper]    Using og:image as fallback...")
        try:
            og_image = page.locator('meta[property="og:image"]').get_attribute('content')
            if og_image:
                images['image_1'] = og_image
                print(f"[scraper]    ✅ Found 1 image from meta tag")
        except:
            pass
    
    print(f"[scraper]    Total images found: {len(images)}")
    return images


def extract_from_meta_tags(page):
    """Extract title from meta tags"""
    data = {}
    
    try:
        title_meta = page.locator('meta[property="og:title"]').get_attribute('content')
        data['title'] = title_meta or ""
    except:
        data['title'] = ""
    
    return data


def extract_from_javascript(page):
    """Extract price and shipping from JavaScript"""
    data = {}
    
    try:
        scripts = page.locator('script').all()
        
        for script in scripts:
            try:
                script_text = script.text_content()
                
                # Look for product price
                price_match = re.search(r'"price":\s*"([^"]+)"', script_text)
                if price_match:
                    data['price'] = price_match.group(1)
                
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
    # DESCRIPTION (FIXED)
    # ========================
    # Scroll to description section
    page.mouse.wheel(0, 3000)
    page.wait_for_timeout(2000)
    
    description = extract_description_from_nav_section(page)
    if description:
        data['description'] = description
    
    # ========================
    # IMAGES (FIXED - GET ALL)
    # ========================
    images_dict = extract_all_images(page)
    data.update(images_dict)
    
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
    
    # Map specifications
    if 'color' in spec_data or 'colour' in spec_data:
        data['color'] = spec_data.get('color') or spec_data.get('colour')
    
    if 'size' in spec_data or 'dimensions' in spec_data:
        data['dimensions'] = spec_data.get('size') or spec_data.get('dimensions')
    
    if 'weight' in spec_data:
        data['weight'] = spec_data.get('weight')
    
    if 'material' in spec_data:
        data['material'] = spec_data.get('material')
    
    # ========================
    # BULLET POINTS
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
    
    return data


def get_product_info(url):
    """
    Main scraper function - COMPLETELY REWRITTEN
    
    Fixes:
    1. Extracts description from nav-description section
    2. Extracts ALL images (not just 6)
    3. Better image extraction from multiple sources
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
            # EXTRACT DATA
            # =====================================================
            
            print("[scraper] 🔍 Extracting from meta tags...")
            data = extract_from_meta_tags(page)
            
            print("[scraper] 🔍 Extracting from JavaScript...")
            js_data = extract_from_javascript(page)
            data.update({k: v for k, v in js_data.items() if v})
            
            print("[scraper] 🔍 Extracting from DOM...")
            dom_data = extract_from_dom(page)
            data.update({k: v for k, v in dom_data.items() if v and k not in data})
            
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
            
            # Count images
            image_count = sum(1 for k in data.keys() if k.startswith('image_'))
            print(f"[scraper]    Images: {image_count}")
            print(f"[scraper]    Bullets: {len(data.get('bullet_points', []))}")
            
            # Add defaults for missing fields
            for key in ['description', 'brand', 'color', 'dimensions', 'weight', 
                       'material', 'shipping', 'price', 'rating', 'reviews']:
                if key not in data or not data[key]:
                    data[key] = ""
            
            # Ensure at least image_1
            if 'image_1' not in data:
                data['image_1'] = ""
            
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
    test_url = "https://www.aliexpress.com/item/1005009666264769.html"
    
    result = get_product_info(test_url)
    
    if result:
        print("\n" + "="*80)
        print("=== SCRAPED DATA ===")
        print("="*80)
        
        image_count = sum(1 for k in result.keys() if k.startswith('image_'))
        print(f"Images found: {image_count}")
        
        for key, value in sorted(result.items()):
            if isinstance(value, list):
                print(f"{key}: {len(value)} items")
            elif key.startswith('image_'):
                if value:
                    print(f"{key}: {value[:80]}...")
            else:
                val_str = str(value)[:100] if len(str(value)) > 100 else str(value)
                if val_str:
                    print(f"{key}: {val_str}")
    else:
        print("❌ Failed to scrape")
