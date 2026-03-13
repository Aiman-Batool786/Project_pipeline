from playwright.sync_api import sync_playwright
import re
import json


def extract_specifications(page):
    """Extract specifications from nav-specification section"""
    
    specs = {}
    print("[scraper] 📋 Extracting specifications...")
    
    try:
        spec_items = page.locator('[id*="nav-specification"] [class*="specification--prop"]').all()
        
        if len(spec_items) > 0:
            print(f"[scraper]    Found {len(spec_items)} specification items")
        
        for idx, item in enumerate(spec_items):
            try:
                title_elem = item.locator('[class*="specification--title"] span').first
                title = title_elem.inner_text().strip() if title_elem.count() > 0 else ""
                
                desc_elem = item.locator('[class*="specification--desc"] span').first
                desc = desc_elem.inner_text().strip() if desc_elem.count() > 0 else ""
                
                if title and desc:
                    specs[title.lower()] = desc
                    print(f"[scraper]      {title}: {desc[:60]}")
            except:
                pass
        
        if specs:
            print(f"[scraper]    ✅ Extracted {len(specs)} specifications")
        
        return specs
    
    except Exception as e:
        print(f"[scraper]    ⚠️  Error: {e}")
        return specs


def map_specifications_to_fields(specs):
    """Map specifications to template fields"""
    
    mapped = {}
    
    mapping_rules = {
        'brand': ['brand name', 'brand', 'marque'],
        'color': ['main color', 'color', 'colour', 'couleur'],
        'dimensions': ['dimensions', 'size', 'taille', 'dimensions (cm)'],
        'weight': ['weight', 'poids', 'net weight', 'weight (kg)'],
        'material': ['material', 'matière', 'materials', 'material composition'],
        'certifications': ['certification', 'certifications', 'normes', 'standards'],
        'country_of_origin': ['country of origin', 'origin', 'country', 'pays'],
        'warranty': ['warranty', 'garantie', 'guarantee'],
        'product_type': ['product type', 'type', 'type de produit', 'product category'],
        'age_from': ['age from', 'recommended age from', 'age (from)'],
        'age_to': ['age to', 'recommended age to', 'age (to)'],
        'gender': ['gender', 'suitable for'],
    }
    
    for template_field, keywords in mapping_rules.items():
        for spec_key, spec_value in specs.items():
            if any(keyword in spec_key for keyword in keywords):
                if template_field not in mapped:
                    mapped[template_field] = spec_value
                break
    
    return mapped


def extract_description_correct(page):
    """Extract description with corrected selectors and multiple fallbacks"""
    
    description = ""
    print("[scraper] 📝 Extracting description...")
    
    # METHOD 1: richTextContainer
    try:
        rich_container = page.locator('div.richTextContainer[data-rich-text-render="true"]')
        if rich_container.count() > 0:
            text = rich_container.first.inner_text().strip()
            if text and len(text) > 50:
                description = text
                print(f"[scraper]    ✅ richTextContainer ({len(description)} chars)")
                return description
    except:
        pass
    
    # METHOD 2: product-description
    try:
        prod_desc = page.locator('div[id="product-description"]')
        if prod_desc.count() > 0:
            text = prod_desc.first.inner_text().strip()
            if text and len(text) > 50:
                description = text
                print(f"[scraper]    ✅ product-description ({len(description)} chars)")
                return description
    except:
        pass
    
    # METHOD 3: nav-description
    try:
        nav_desc = page.locator('div[id="nav-description"]')
        if nav_desc.count() > 0:
            text = nav_desc.first.inner_text().strip()
            text = re.sub(r'^.*?Description\s+report\s+', '', text, flags=re.IGNORECASE | re.DOTALL)
            if text and len(text) > 50 and "Smarter Shopping" not in text:
                description = text
                print(f"[scraper]    ✅ nav-description ({len(description)} chars)")
                return description
    except:
        pass
    
    # METHOD 4: detailmodule_text
    try:
        detail_modules = page.locator('div.detailmodule_text')
        if detail_modules.count() > 0:
            for idx in range(detail_modules.count()):
                try:
                    text = detail_modules.nth(idx).inner_text().strip()
                    if text and len(text) > 50:
                        description = text
                        print(f"[scraper]    ✅ detailmodule_text ({len(description)} chars)")
                        return description
                except:
                    pass
    except:
        pass
    
    # METHOD 5: detail-desc-decorate-content
    try:
        content_para = page.locator('p.detail-desc-decorate-content')
        if content_para.count() > 0:
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
                print(f"[scraper]    ✅ detail-desc-decorate-content ({len(description)} chars)")
                return description
    except:
        pass
    
    print("[scraper]    ⚠️  No description found")
    return ""


def extract_all_images(page):
    """Extract all images from product page"""
    images = {}
    
    print("[scraper] 🖼️  Extracting images...")
    
    try:
        scripts = page.locator("script")
        
        for idx in range(scripts.count()):
            try:
                script_text = scripts.nth(idx).text_content()
                image_match = re.search(r'"imagePathList":\s*\[(.*?)\]', script_text)
                
                if image_match:
                    images_str = image_match.group(1)
                    images_list = re.findall(r'"(https://[^"]+\.jpg[^"]*)"', images_str)
                    
                    for img_idx, img in enumerate(images_list[:20], 1):
                        key = f"image_{img_idx}"
                        clean_url = re.sub(r'_\d+x\d+', '', img)
                        images[key] = clean_url
                    
                    if images:
                        print(f"[scraper]    ✅ Found {len(images)} images")
                        return images
            except:
                pass
    except:
        pass
    
    # Fallback
    try:
        og_image = page.locator('meta[property="og:image"]').get_attribute('content')
        if og_image:
            images['image_1'] = og_image
            print(f"[scraper]    ✅ 1 image from meta tag")
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
                price_match = re.search(r'"price":\s*"([^"]+)"', script_text)
                if price_match:
                    data["price"] = price_match.group(1)
                ship_match = re.search(r'"shipmentWay":\s*"([^"]+)"', script_text)
                if ship_match:
                    data["shipping"] = ship_match.group(1)
            except:
                pass
    except:
        pass
    return data


def extract_from_dom(page):
    """Extract data from DOM elements"""
    data = {}
    
    # Scroll
    page.mouse.wheel(0, 3000)
    page.wait_for_timeout(2000)
    
    # Specifications
    specs = extract_specifications(page)
    spec_fields = map_specifications_to_fields(specs)
    data.update(spec_fields)
    
    # Description
    description = extract_description_correct(page)
    if description:
        data["description"] = description
    
    # Images
    images_dict = extract_all_images(page)
    data.update(images_dict)
    
    return data


def get_product_info(url):
    """Main scraper with all improvements"""
    
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
            
            page.wait_for_timeout(3000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(2000)
            
            print("[scraper] 🔍 Extracting data...")
            
            data = extract_from_meta_tags(page)
            data.update(extract_from_javascript(page))
            data.update(extract_from_dom(page))
            
            browser.close()
            
            if not data.get('title'):
                print("[scraper] ❌ ERROR: No title extracted")
                return None
            
            print(f"\n[scraper] ✅ Successfully extracted product data")
            print(f"[scraper]    Title: {data.get('title', '')[:60]}...")
            print(f"[scraper]    Description: {len(data.get('description', ''))} chars")
            
            specs_count = len([k for k in data.keys() if k in ['brand', 'color', 'dimensions', 'weight', 'material', 'certifications', 'country_of_origin', 'warranty', 'product_type']])
            print(f"[scraper]    Specifications: {specs_count} fields")
            
            image_count = sum(1 for i in range(1, 21) if data.get(f'image_{i}'))
            print(f"[scraper]    Images: {image_count}")
            
            # Add defaults
            defaults = {
                'description': '',
                'brand': '',
                'color': '',
                'dimensions': '',
                'weight': '',
                'material': '',
                'certifications': '',
                'country_of_origin': '',
                'warranty': '',
                'product_type': '',
                'shipping': '',
                'price': '',
                'rating': '',
                'reviews': '',
                'bullet_points': [],
                'age_from': '',
                'age_to': '',
                'gender': '',
                'safety_warning': '',
            }
            
            for key, default_val in defaults.items():
                if key not in data or not data[key]:
                    data[key] = default_val
            
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
        print(f"Brand: {result.get('brand', 'N/A')}")
        print(f"Color: {result.get('color', 'N/A')}")
        print(f"Dimensions: {result.get('dimensions', 'N/A')}")
        print(f"Description ({len(result['description'])} chars): {result['description'][:200]}...")
    else:
        print("Failed to scrape")
