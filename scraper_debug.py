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


def get_product_info(url):
    """
    Main scraper function - optimized for new AliExpress layout
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
            # 1. TITLE (from meta tag or h1)
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
            # 4. DESCRIPTION (from og:description or page content)
            # =====================================================
            print("[scraper] Extracting description...")
            description = ""
            
            try:
                desc_meta = page.locator('meta[property="og:description"]').get_attribute('content')
                if desc_meta:
                    description = desc_meta
            except:
                pass
            
            # =====================================================
            # 5. SPECIFICATIONS (from specification--prop--Jh28bKu)
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
                # Look for the span with data-spm-anchor-id that contains the selected color
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
            # 8. EXTRACT DIMENSIONS
            # =====================================================
            dimensions = spec_fields.get('dimensions', '')
            
            # =====================================================
            # 9. EXTRACT WEIGHT
            # =====================================================
            weight = spec_fields.get('weight', '')
            
            # =====================================================
            # 10. EXTRACT MATERIAL
            # =====================================================
            material = spec_fields.get('material', '')
            
            # =====================================================
            # 11. EXTRACT CERTIFICATIONS
            # =====================================================
            certifications = spec_fields.get('certifications', '')
            
            # =====================================================
            # 12. EXTRACT COUNTRY OF ORIGIN
            # =====================================================
            country_of_origin = spec_fields.get('country_of_origin', '')
            
            # =====================================================
            # 13. EXTRACT WARRANTY
            # =====================================================
            warranty = spec_fields.get('warranty', '')
            
            # =====================================================
            # 14. EXTRACT PRODUCT TYPE
            # =====================================================
            product_type = spec_fields.get('product_type', '')
            
            # =====================================================
            # 15. EXTRACT SHIPPING & OTHER INFO
            # =====================================================
            shipping = ""
            try:
                # Look for shipping info in page
                ship_spans = page.locator('[class*="ship"]').all()
                for span in ship_spans[:1]:
                    shipping = span.inner_text().strip()
                    break
            except:
                pass
            
            # =====================================================
            # 16. BULLET POINTS / FEATURES
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
            
            browser.close()
            
            # =====================================================
            # VALIDATION
            # =====================================================
            
            if not title:
                print("[scraper] ❌ ERROR: No title extracted")
                return None
            
            print(f"[scraper] ✅ Successfully extracted data")
            
            # =====================================================
            # BUILD RESULT OBJECT
            # =====================================================
            
            data = {
                'title': title,
                'description': description,
                'brand': brand,
                'color': color,
                'dimensions': dimensions,
                'weight': weight,
                'material': material,
                'certifications': certifications,
                'country_of_origin': country_of_origin,
                'warranty': warranty,
                'product_type': product_type,
                'price': price,
                'shipping': shipping,
                'bullet_points': bullets,
            }
            
            # Add all extracted images
            for idx in range(1, 7):
                data[f'image_{idx}'] = images.get(f'image_{idx}', '')
            
            # Add defaults for missing fields
            for key in ['age_from', 'age_to', 'gender', 'safety_warning']:
                if key not in data:
                    data[key] = ""
            
            print(f"[scraper] Extracted {len([v for v in data.values() if v])} attributes")
            return data
    
    except Exception as e:
        print(f"[scraper] ❌ ERROR: {e}")
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
