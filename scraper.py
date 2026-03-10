from playwright.sync_api import sync_playwright
import re
import json
import time


def extract_specifications(page):
    """
    Extract all product specifications from the specification section
    Handles the new AliExpress layout with specification--wrap--lxVQ2tj
    """
    specs = {}
    
    try:
        # Find all specification items
        spec_items = page.locator('div.specification--prop--Jh28bKu').all()
        
        for item in spec_items:
            try:
                # Get title
                title_elem = item.locator('div.specification--title--SfH3sA8 span').first
                title = title_elem.inner_text().strip() if title_elem.count() > 0 else ""
                
                # Get description
                desc_elem = item.locator('div.specification--desc--Dxx6W0W span').first
                desc = desc_elem.inner_text().strip() if desc_elem.count() > 0 else ""
                
                if title and desc:
                    specs[title.lower()] = desc
            except:
                pass
    except:
        pass
    
    return specs


def map_specifications_to_fields(specs):
    """
    Map extracted specifications to template fields
    """
    mapped = {}
    
    # Mapping of specification keys to template fields
    spec_mapping = {
        'brand name': 'brand',
        'main color': 'color',
        'color': 'color',
        'dimensions': 'dimensions',
        'width': 'dimensions',
        'material': 'material',
        'certification': 'certifications',
        'place of origin': 'country_of_origin',
        'origin': 'country_of_origin',
        'product weight': 'weight',
        'warranty': 'warranty',
        'gender': 'gender',
        'recommend age': 'age',
        'power source': 'power_source',
        'style': 'product_type',
        'capacity': 'capacity',
        'warning': 'safety_warning',
    }
    
    for spec_key, spec_value in specs.items():
        for template_key, field_name in spec_mapping.items():
            if template_key in spec_key:
                if field_name not in mapped:
                    mapped[field_name] = spec_value
    
    return mapped


def extract_description_from_dom(page):
    """
    Extract description from AliExpress product detail sections
    Priority:
    1. detailmodule_text div with detail-desc-decorate-content spans
    2. All divs with detailmodule_text class
    3. Generic description selectors
    """
    
    description = ""
    
    print("[scraper] 📝 Extracting description from DOM...")
    
    # PRIMARY: Extract from detailmodule_text section with detail-desc-decorate-content
    try:
        detail_modules = page.locator('div.detailmodule_text').all()
        if detail_modules:
            print(f"[scraper]    Found {len(detail_modules)} detail modules")
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
                print(f"[scraper]    ✅ Extracted {len(detail_texts)} sections ({len(description)} chars)")
    except Exception as e:
        print(f"[scraper]    ⚠️  Error: {e}")
    
    # SECONDARY: Extract from any span with detail-desc-decorate-content (orphaned spans)
    if not description or len(description) < 50:
        try:
            desc_spans = page.locator('span.detail-desc-decorate-content').all()
            if desc_spans:
                print(f"[scraper]    Found {len(desc_spans)} orphaned description spans")
                span_texts = []
                
                for span in desc_spans:
                    text = span.inner_text().strip()
                    if text and len(text) > 5:
                        span_texts.append(text)
                
                if span_texts:
                    description = " | ".join(span_texts)
                    print(f"[scraper]    ✅ Extracted from {len(span_texts)} spans ({len(description)} chars)")
        except Exception as e:
            print(f"[scraper]    ⚠️  Error: {e}")
    
    # TERTIARY: Generic description selectors
    if not description or len(description) < 50:
        print(f"[scraper]    Trying generic description selectors...")
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
                    if desc and len(desc) > 50:
                        description = desc
                        print(f"[scraper]    ✅ Found using selector: {selector}")
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
    
    return description


def get_product_info(url):
    """
    Main scraper function - HYBRID approach
    - Title: Uses meta + h1 (from old scraper - PROVEN WORKING)
    - Description: Uses detailmodule_text + detail-desc-decorate-content (from new scraper - FIXED)
    - Everything else: From old scraper (proven working)
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
            # 1. TITLE (BEST FROM OLD SCRAPER)
            # =====================================================
            print("[scraper] Extracting title...")
            title = ""
            
            # Try meta tag first
            try:
                title_meta = page.locator('meta[property="og:title"]').get_attribute('content')
                if title_meta:
                    title = title_meta.split(" - AliExpress")[0].strip()
            except:
                pass
            
            # Fallback to h1
            if not title:
                try:
                    h1_elem = page.locator('h1[data-pl="product-title"]')
                    if h1_elem.count() > 0:
                        title = h1_elem.first.inner_text().strip()
                except:
                    pass
            
            # =====================================================
            # 2. PRICE (from price-default--current class)
            # =====================================================
            print("[scraper] Extracting price...")
            price = ""
            
            try:
                price_elem = page.locator('span.price-default--current--F8OlYIo')
                if price_elem.count() > 0:
                    price = price_elem.first.inner_text().strip()
            except:
                pass
            
            # =====================================================
            # 3. IMAGES (from meta or JavaScript)
            # =====================================================
            print("[scraper] Extracting images...")
            images = {}
            
            try:
                image_meta = page.locator('meta[property="og:image"]').get_attribute('content')
                if image_meta:
                    images['image_1'] = image_meta
            except:
                pass
            
            # Try to get more images from scripts
            try:
                scripts = page.locator('script').all()
                for script in scripts:
                    try:
                        script_text = script.text_content()
                        
                        # Look for image URLs in imagePathList
                        img_matches = re.findall(r'"(https://[^"]+\.jpg[^"]*)"', script_text)
                        for idx, img in enumerate(img_matches[:6], 1):
                            if f'image_{idx}' not in images:
                                # Clean URL
                                clean_url = img.split('_')[0] + '.jpg' if '_' in img else img
                                images[f'image_{idx}'] = clean_url
                    except:
                        pass
            except:
                pass
            
            # =====================================================
            # 4. DESCRIPTION (BEST FROM NEW SCRAPER - FIXED)
            # =====================================================
            # Scroll to ensure description elements are loaded
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(2000)
            
            description = extract_description_from_dom(page)
            
            # =====================================================
            # 5. SPECIFICATIONS
            # =====================================================
            print("[scraper] Extracting specifications...")
            specs = extract_specifications(page)
            spec_fields = map_specifications_to_fields(specs)
            
            # =====================================================
            # 6. EXTRACT BRAND
            # =====================================================
            brand = spec_fields.get('brand', '')
            
            # =====================================================
            # 7. EXTRACT COLOR (from SKU selector FIRST, then specifications)
            # =====================================================
            color = ""
            
            # PRIORITY 1: Get from SKU/Variant selector (the actual selected color option)
            try:
                # SKU color is in: <div class="sku--wrap--xgoW06M">
                sku_wrap = page.locator('div.sku--wrap--xgoW06M')
                
                if sku_wrap.count() > 0:
                    # Get all spans with data-spm-anchor-id (these are the selected values)
                    color_spans = sku_wrap.locator('span[data-spm-anchor-id]').all()
                    
                    if color_spans:
                        # The first span with data-spm-anchor-id after SKU title is usually the color
                        for span in color_spans[:2]:
                            text = span.inner_text().strip()
                            # Make sure it's not empty and not a link text
                            if text and len(text) > 0 and 'shop' not in text.lower():
                                color = text
                                break
            except:
                pass
            
            # PRIORITY 2: Fallback to specifications if no color found from SKU
            if not color:
                color = spec_fields.get('color', '')
            
            # =====================================================
            # 8. OTHER SPECIFICATIONS
            # =====================================================
            dimensions = spec_fields.get('dimensions', '')
            weight = spec_fields.get('weight', '')
            material = spec_fields.get('material', '')
            certifications = spec_fields.get('certifications', '')
            country_of_origin = spec_fields.get('country_of_origin', '')
            warranty = spec_fields.get('warranty', '')
            product_type = spec_fields.get('product_type', '')
            
            # =====================================================
            # 9. SHIPPING INFO
            # =====================================================
            shipping = ""
            try:
                ship_spans = page.locator('[class*="ship"]').all()
                for span in ship_spans[:1]:
                    shipping = span.inner_text().strip()
                    break
            except:
                pass
            
            # =====================================================
            # 10. BULLET POINTS / FEATURES
            # =====================================================
            print("[scraper] Extracting bullet points...")
            bullets = []
            
            try:
                # Look for seo-sellpoints
                bullet_items = page.locator('.seo-sellpoints--sellerPoint--RcmFO_y li').all()
                if bullet_items:
                    for item in bullet_items[:8]:
                        text = item.inner_text().strip()
                        if text:
                            bullets.append(text)
                
                # Fallback: look for ul > li patterns
                if not bullets:
                    ul_items = page.locator('ul li').all()
                    for item in ul_items[:8]:
                        text = item.inner_text().strip()
                        if text and len(text) > 10:
                            bullets.append(text)
            except:
                pass
            
            # =====================================================
            # COMPILE RESULTS
            # =====================================================
            result = {
                'title': title,
                'description': description,
                'brand': brand,
                'color': color,
                'weight': weight,
                'material': material,
                'certifications': certifications,
                'country_of_origin': country_of_origin,
                'warranty': warranty,
                'product_type': product_type,
                'dimensions': dimensions,
                'price': price,
                'shipping': shipping,
                'bullet_points': bullets,
            }
            
            # Add images
            for i in range(1, 7):
                result[f'image_{i}'] = images.get(f'image_{i}', '')
            
            browser.close()
            
            # =====================================================
            # VALIDATION
            # =====================================================
            
            if not result.get('title'):
                print("[scraper] ❌ ERROR: No title extracted")
                return None
            
            print(f"[scraper] ✅ Successfully extracted all attributes")
            print(f"[scraper]    Title: {result['title'][:50]}...")
            print(f"[scraper]    Description: {len(result['description'])} chars")
            print(f"[scraper]    Images: {sum(1 for i in range(1, 7) if result.get(f'image_{i}'))}")
            print(f"[scraper]    Bullet points: {len(result['bullet_points'])}")
            
            return result
    
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
        print("\n" + "="*70)
        print("=== SCRAPED DATA ===")
        print("="*70)
        for key, value in sorted(result.items()):
            if isinstance(value, list):
                print(f"{key:25s}: {len(value)} items")
                for item in value[:3]:
                    print(f"  - {item[:60]}")
            else:
                val_str = str(value)[:100] if len(str(value)) > 100 else str(value)
                print(f"{key:25s}: {val_str}")
    else:
        print("❌ Failed to scrape")
