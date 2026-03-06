from playwright.sync_api import sync_playwright
import time
import json

def get_product_info(url):
    """
    Improved AliExpress scraper with proper selectors and dynamic content handling
    """
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu"
                ]
            )
            
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )
            
            page = context.new_page()
            
            print(f"Opening URL: {url}")
            
            # Navigate with better timeout handling
            try:
                page.goto(url, timeout=60000, wait_until="domcontentloaded")
            except Exception as e:
                print(f"Navigation timeout, continuing anyway: {e}")
            
            # Wait for key elements to load
            time.sleep(3)
            
            # Try multiple methods to get title
            title = ""
            title_selectors = [
                ".pc-main h1",  # AliExpress product title
                "h1.pdp-title-h1",
                ".TitleWithLogo h1",
                "h1[class*='title']",
                "span.pdp-mod-product-title-text",
                "[class*='ProductName']",
                "h1"
            ]
            
            for selector in title_selectors:
                try:
                    if page.locator(selector).count() > 0:
                        text = page.locator(selector).first.inner_text()
                        if text and len(text) > 5:  # Ensure it's meaningful
                            title = text.strip()
                            print(f"✓ Title found with selector: {selector}")
                            break
                except:
                    continue
            
            # Get price
            price = ""
            price_selectors = [
                "[class*='ProductPrice']",
                "span.search-card-e-price-main",
                "[class*='priceText']",
                ".search-card-e-price-main-inner"
            ]
            
            for selector in price_selectors:
                try:
                    if page.locator(selector).count() > 0:
                        text = page.locator(selector).first.inner_text()
                        if text and any(char.isdigit() for char in text):
                            price = text.strip()
                            print(f"✓ Price found with selector: {selector}")
                            break
                except:
                    continue
            
            # Get description - AliExpress specific
            description = ""
            description_selectors = [
                "[class*='Description']",
                ".product-description",
                ".pdp-desc",
                "div[class*='Description'] p",
                "[class*='content'] p",
            ]
            
            desc_paragraphs = []
            for selector in description_selectors:
                try:
                    elements = page.locator(selector).all()
                    for elem in elements:
                        text = elem.inner_text()
                        if text and len(text) > 10:
                            desc_paragraphs.append(text.strip())
                    if desc_paragraphs:
                        print(f"✓ Description found with selector: {selector}")
                        break
                except:
                    continue
            
            # Fallback: get all paragraphs if specific selectors failed
            if not desc_paragraphs:
                try:
                    all_p = page.locator("p").all()
                    for p_elem in all_p[:10]:  # Limit to first 10
                        text = p_elem.inner_text()
                        if text and len(text) > 10 and "AliExpress" not in text:
                            desc_paragraphs.append(text.strip())
                except:
                    pass
            
            description = " ".join(desc_paragraphs[:500]) if desc_paragraphs else ""
            
            # Get product features/specifications
            bullet_points = []
            feature_selectors = [
                "[class*='Feature'] li",
                ".product-features li",
                "[class*='Specification'] li",
                "li[class*='feature']"
            ]
            
            for selector in feature_selectors:
                try:
                    items = page.locator(selector).all()
                    for item in items[:10]:
                        text = item.inner_text()
                        if text and len(text) > 5:
                            bullet_points.append(text.strip())
                    if bullet_points:
                        print(f"✓ Bullet points found with selector: {selector}")
                        break
                except:
                    continue
            
            # Fallback: extract from description if contains numbered items
            if not bullet_points and description:
                lines = description.split(".")
                bullet_points = [line.strip() for line in lines if len(line.strip()) > 10][:5]
            
            # Get main product image
            image_url = ""
            image_selectors = [
                "img[class*='productImage']",
                "[class*='ImageViewer'] img",
                ".product-main-image img",
                "img[alt*='Product']",
                "img[src*='oss']",  # AliExpress uses oss for images
            ]
            
            for selector in image_selectors:
                try:
                    if page.locator(selector).count() > 0:
                        img = page.locator(selector).first
                        src = img.get_attribute("src") or img.get_attribute("data-src")
                        if src and ("http" in src or "data:" in src):
                            image_url = src
                            print(f"✓ Image found with selector: {selector}")
                            break
                except:
                    continue
            
            # Get ratings and reviews
            rating = ""
            reviews_count = ""
            
            try:
                rating_elem = page.locator("[class*='Rating']").first
                if rating_elem:
                    rating = rating_elem.inner_text()
            except:
                pass
            
            try:
                reviews_elem = page.locator("[class*='Review']").first
                if reviews_elem:
                    reviews_count = reviews_elem.inner_text()
            except:
                pass
            
            # Get shop/seller information
            seller_name = ""
            try:
                seller = page.locator("[class*='Seller'] a").first
                if seller:
                    seller_name = seller.inner_text()
            except:
                pass
            
            browser.close()
            
            # Validation: if title is still empty or too generic, return None
            if not title or title.lower() in ["aliexpress", "search by image", "welcome"]:
                print("❌ Failed to extract product title - page may not have loaded properly")
                return None
            
            print("\n✓ Scraping successful!")
            print(f"Title: {title[:50]}...")
            print(f"Description length: {len(description)} chars")
            print(f"Bullet points: {len(bullet_points)}")
            
            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image_url,
                "price": price,
                "rating": rating,
                "reviews_count": reviews_count,
                "seller_name": seller_name
            }
            
    except Exception as e:
        print(f"❌ Scraping failed for URL: {url}")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


# Test the scraper
if __name__ == "__main__":
    # Test with your AliExpress URL
    test_url = "https://www.aliexpress.com/item/1005006246885476.html"  # Example
    
    result = get_product_info(test_url)
    
    if result:
        print("\n" + "="*60)
        print("EXTRACTED PRODUCT INFORMATION")
        print("="*60)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("\nFailed to extract product information")
