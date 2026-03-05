from playwright.sync_api import sync_playwright
from stem import Signal
from stem.control import Controller
import time
import re


def renew_tor_ip():
    try:
        with Controller.from_port(port=9051) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
            time.sleep(5)
            print("Got new Tor IP")
    except Exception as e:
        print("Could not rotate IP:", e)


def scrape(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                proxy={"server": "socks5://127.0.0.1:9050"},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {} };
            """)
            page = context.new_page()
            print("Opening URL:", url)
            page.goto(url, timeout=90000, wait_until="domcontentloaded")
            
            # Simulate human behaviour
            page.wait_for_timeout(6000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(4000)
            print("Page title:", page.title())
            print("Current URL:", page.url)

            # ────────────────────────────────────
            # TITLE EXTRACTION (FIXED SELECTORS)
            # ────────────────────────────────────
            title = ""
            title_selectors = [
                "h1[data-pl='product-title']",  # Primary selector for AliExpress
                "h1[class*='product-title']",
                "h1[class*='title']",
                ".product-name",
                "h1",
            ]
            
            for selector in title_selectors:
                try:
                    elements = page.locator(selector)
                    if elements.count() > 0:
                        title = elements.first.inner_text().strip()
                        # Remove common prefixes
                        title = re.sub(r'^(Buy|Shop|Get|Order)\s+', '', title)
                        if title and len(title) > 5:
                            print(f"✅ Title found via: {selector}")
                            break
                except:
                    pass

            # ────────────────────────────────────
            # DESCRIPTION EXTRACTION (FIXED)
            # ────────────────────────────────────
            description = ""
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(2000)
            
            # Try shadow DOM first (common on AliExpress)
            desc_selectors = [
                "div[data-pl='product-description']",
                "div#product-description",
                "div[class*='description--product-description']",
                "div.detailmodule_text",
                "div[class*='description-content']",
                "div[id*='description']",
                "div[class*='product-description']",
                "div[class*='description']",
            ]
            
            for selector in desc_selectors:
                try:
                    el = page.locator(selector)
                    if el.count() > 0:
                        text = el.first.inner_text().strip()
                        # Filter out non-descriptive content
                        if text and len(text) > 20 and text.lower() not in ["description", "report"]:
                            description = text[:1000]  # Increased to 1000 chars
                            print(f"✅ Description found via: {selector}")
                            break
                except:
                    pass
            
            # Fallback: Try to get description from product specs or details
            if not description:
                try:
                    # Look for specification sections
                    spec_text = ""
                    specs = page.locator("div[class*='specification']").all_text_contents()
                    if specs:
                        spec_text = " ".join(specs[:3])[:500]
                    
                    if spec_text and len(spec_text) > 20:
                        description = spec_text
                        print("✅ Description extracted from specifications")
                except:
                    pass

            # ────────────────────────────────────
            # BULLET POINTS EXTRACTION (IMPROVED)
            # ────────────────────────────────────
            bullet_points = []
            try:
                # Method 1: Direct <li> elements
                li_elements = page.locator("li").all_text_contents()
                if li_elements:
                    bullet_points = [
                        point.strip() 
                        for point in li_elements[:8] 
                        if point.strip() and len(point.strip()) > 5
                    ]
                    print(f"✅ Found {len(bullet_points)} bullet points from <li>")
                
                # Method 2: Div-based bullet points (common on AliExpress)
                if len(bullet_points) < 3:
                    divs = page.locator("div[class*='bullet']").all_text_contents()
                    if divs:
                        bullet_points = [
                            point.strip() 
                            for point in divs[:8] 
                            if point.strip() and len(point.strip()) > 5
                        ]
                        print(f"✅ Found {len(bullet_points)} bullet points from div")
                
                # Method 3: Generate from description if bullets are missing
                if len(bullet_points) < 3 and description:
                    sentences = [s.strip() for s in description.split('.') if len(s.strip()) > 10]
                    bullet_points = sentences[:5]
                    print(f"✅ Generated {len(bullet_points)} bullet points from description")
            
            except Exception as e:
                print(f"⚠️ Error extracting bullet points: {e}")

            # ────────────────────────────────────
            # IMAGE EXTRACTION (IMPROVED)
            # ────────────────────────────────────
            image = ""
            try:
                # Try main product image
                img_selectors = [
                    "img[alt*='product']",
                    "img[class*='product-image']",
                    "img[data-pl='image']",
                    "img.slide-image",
                    "img",
                ]
                
                for selector in img_selectors:
                    img_elements = page.locator(selector)
                    if img_elements.count() > 0:
                        for i in range(min(3, img_elements.count())):  # Check first 3 images
                            src = img_elements.nth(i).get_attribute("src")
                            # Skip small/placeholder images
                            if src and len(src) > 20 and "placeholder" not in src.lower():
                                image = src
                                print(f"✅ Image found: {image[:80]}...")
                                break
                        if image:
                            break
            except Exception as e:
                print(f"⚠️ Error extracting image: {e}")

            browser.close()
            
            # ────────────────────────────────────
            # VALIDATION & RETURN
            # ────────────────────────────────────
            if not title or len(title) < 5:
                print("❌ Login page detected or scraping blocked - No valid title found")
                return None
            
            print(f"\n✅ SCRAPE SUCCESSFUL:")
            print(f"   Title: {title[:60]}...")
            print(f"   Description: {description[:60]}..." if description else "   Description: NOT FOUND")
            print(f"   Bullet Points: {len(bullet_points)}")
            print(f"   Image: {'FOUND' if image else 'NOT FOUND'}")
            
            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image
            }
    
    except Exception as e:
        print("❌ Scraping failed for URL:", url)
        print("Error:", e)
        return None


def get_product_info(url, max_retries=3):
    """
    Get product info with retry logic
    
    Args:
        url: Product URL to scrape
        max_retries: Number of retry attempts (default: 3)
    
    Returns:
        Dictionary with product data or None if failed
    """
    for attempt in range(max_retries):
        print(f"\n{'='*60}")
        print(f"--- Attempt {attempt + 1} of {max_retries} ---")
        print(f"{'='*60}")
        result = scrape(url)
        
        if result:
            return result
        
        if attempt < max_retries - 1:
            print("⚠️ Blocked! Rotating Tor IP and retrying...")
            renew_tor_ip()
            time.sleep(3)
    
    print(f"❌ All {max_retries} attempts failed for URL: {url}")
    return None
