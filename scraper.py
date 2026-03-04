from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from stem import Signal
from stem.control import Controller
import time
import random
from urllib.parse import urlparse


def renew_tor_ip():
    """Rotate Tor IP address"""
    try:
        with Controller.from_port(port=9051) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
            time.sleep(5)
            print("✅ Got new Tor IP")
            return True
    except Exception as e:
        print(f"❌ Could not rotate IP: {e}")
        return False


def is_blocked_page(page):
    """Check if current page is a block/punishment page"""
    try:
        current_url = page.url
        page_title = page.title()
        
        # Check for common block indicators
        block_indicators = [
            "punish",
            "x5secdata",
            "x5step",
            "tmd_____",
            "login",
            "verify",
            "challenge",
            "access_denied"
        ]
        
        if any(indicator in current_url.lower() for indicator in block_indicators):
            print(f"🚫 Blocked page detected in URL: {current_url}")
            return True
        
        # Check page content
        body_text = page.locator("body").inner_text().lower()
        if "verify" in body_text or "challenge" in body_text or "security" in body_text:
            if page_title and page_title.strip() == "":
                print(f"🚫 Blocked page detected: empty title with security keywords")
                return True
        
        return False
    except Exception as e:
        print(f"⚠️ Error checking if page is blocked: {e}")
        return False


def get_product_title(page):
    """Extract product title with multiple selector strategies"""
    title_selectors = [
        "h1[data-pl='product-title']",
        "h1.product-title-text",
        ".product-title-text",
        "h1.title",
        "h1[class*='title']",
        "h1"
    ]
    
    for selector in title_selectors:
        try:
            if page.locator(selector).count() > 0:
                title = page.locator(selector).first.inner_text().strip()
                if title and len(title) > 5:  # Minimum title length
                    print(f"✅ Title found: {title[:80]}...")
                    return title
        except Exception as e:
            continue
    
    print("❌ Could not extract title")
    return ""


def get_product_description(page):
    """Extract product description with multiple selector strategies"""
    # Scroll to load description
    page.mouse.wheel(0, 3000)
    page.wait_for_timeout(2000)
    
    desc_selectors = [
        "div[class*='description--description']",
        "div[class*='detailmodule_text']",
        "div[class*='description-content']",
        "div[id*='description']",
        "div[class*='product-description']",
        "div[class*='detail-desc']",
        "section[class*='description']"
    ]
    
    for selector in desc_selectors:
        try:
            elements = page.locator(selector).all()
            for element in elements:
                text = element.inner_text().strip()
                if text and len(text) > 20:  # Minimum description length
                    if text.lower() not in ["description", "report", "details"]:
                        desc = text[:1000]  # Increased from 500 to 1000
                        print(f"✅ Description found via {selector}")
                        return desc
        except Exception as e:
            continue
    
    print("⚠️ No description found")
    return ""


def get_bullet_points(page):
    """Extract bullet points"""
    try:
        # Try to get from list items
        bullets = []
        
        # Strategy 1: Get from <li> elements
        li_elements = page.locator("li").all()
        for li in li_elements[:10]:  # Get up to 10
            text = li.inner_text().strip()
            if text and len(text) > 5:
                bullets.append(text)
        
        if bullets:
            print(f"✅ Found {len(bullets)} bullet points")
            return bullets[:5]
        
        # Strategy 2: Try to get key features from description area
        features = page.locator("div[class*='feature']").all()
        if features:
            for feature in features[:5]:
                text = feature.inner_text().strip()
                if text and len(text) > 5:
                    bullets.append(text)
        
        return bullets[:5] if bullets else []
    except Exception as e:
        print(f"⚠️ Error extracting bullets: {e}")
        return []


def get_product_image(page):
    """Extract product image URL"""
    try:
        image_selectors = [
            "img[class*='product-image']",
            "img[class*='main-image']",
            "img[data-original]",
            "img[src*='alidfs']",
            "img[src*='ae01']",
            "img"
        ]
        
        for selector in image_selectors:
            if page.locator(selector).count() > 0:
                img = page.locator(selector).first
                # Try different attributes
                src = img.get_attribute("src") or img.get_attribute("data-src") or img.get_attribute("data-original")
                if src and ("http" in src or src.startswith("/")):
                    if not src.startswith("data:"):  # Skip data URIs
                        print(f"✅ Image found: {src[:100]}...")
                        return src
        
        print("⚠️ No image found")
        return ""
    except Exception as e:
        print(f"⚠️ Error extracting image: {e}")
        return ""


def scrape(url, attempt_num=1):
    """
    Scrape product information from AliExpress URL
    
    Args:
        url: Product URL to scrape
        attempt_num: Current attempt number (for logging)
    
    Returns:
        dict with product data or None if failed
    """
    browser = None
    try:
        print(f"\n{'='*60}")
        print(f"🔍 Scraping: {url}")
        print(f"{'='*60}")
        
        # Launch browser with Tor proxy
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                proxy={"server": "socks5://127.0.0.1:9050"},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-web-security"
                ]
            )
            
            # Create context with anti-detection measures
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )
            
            # Add stealth scripts
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'permissions', { get: () => ({
                    query: () => Promise.resolve({ state: 'denied' })
                })});
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 4 });
                Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
            """)
            
            page = context.new_page()
            
            # Add request interception for faster loading
            def handle_route(route):
                if route.request.resource_type in ["image", "stylesheet", "font"]:
                    route.abort()
                else:
                    route.continue_()
            
            # page.route("**/*", handle_route)  # Uncomment to block images for faster loading
            
            # Navigate to URL with longer timeout
            print(f"🌐 Navigating to URL...")
            try:
                page.goto(url, timeout=120000, wait_until="domcontentloaded")
            except PlaywrightTimeoutError:
                print(f"⚠️ Page load timeout, continuing anyway...")
            
            print(f"⏳ Waiting for page to stabilize...")
            page.wait_for_timeout(8000)
            
            # Simulate human behavior
            page.mouse.move(
                random.randint(100, 800),
                random.randint(100, 600)
            )
            page.wait_for_timeout(random.randint(1000, 3000))
            page.mouse.wheel(0, random.randint(1500, 3000))
            page.wait_for_timeout(random.randint(2000, 4000))
            
            # Get final URL and page info
            final_url = page.url
            page_title = page.title()
            
            print(f"📄 Final URL: {final_url}")
            print(f"📋 Page Title: {page_title if page_title else '(empty)'}")
            
            # Check if we hit a block page
            if is_blocked_page(page):
                browser.close()
                return None
            
            # Extract product information
            print(f"\n📦 Extracting product data...")
            
            title = get_product_title(page)
            if not title:
                print(f"❌ Failed to extract title - likely blocked or wrong page")
                browser.close()
                return None
            
            description = get_product_description(page)
            bullet_points = get_bullet_points(page)
            image_url = get_product_image(page)
            
            browser.close()
            
            print(f"\n✅ Scraping successful!")
            print(f"   - Title: {title[:60]}...")
            print(f"   - Description: {len(description)} chars")
            print(f"   - Bullet points: {len(bullet_points)} items")
            print(f"   - Image: {image_url[:50] if image_url else 'None'}...")
            
            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image_url
            }
    
    except PlaywrightTimeoutError as e:
        print(f"⏱️ Timeout error: {e}")
        if browser:
            browser.close()
        return None
    
    except Exception as e:
        print(f"❌ Error during scraping: {type(e).__name__}: {e}")
        if browser:
            browser.close()
        return None


def get_product_info(url, max_retries=4):
    """
    Get product info with retry logic and IP rotation
    
    Args:
        url: Product URL
        max_retries: Maximum number of retry attempts
    
    Returns:
        dict with product data or None if all attempts fail
    """
    print(f"\n{'='*60}")
    print(f"🚀 Starting scrape process for: {url}")
    print(f"{'='*60}")
    
    for attempt in range(max_retries):
        print(f"\n📍 Attempt {attempt + 1} of {max_retries}")
        
        result = scrape(url, attempt + 1)
        
        if result:
            print(f"\n🎉 SUCCESS on attempt {attempt + 1}!")
            return result
        
        # If not last attempt, rotate IP and retry
        if attempt < max_retries - 1:
            print(f"\n🔄 Failed, rotating Tor IP and retrying...")
            if renew_tor_ip():
                wait_time = random.randint(5, 10)
                print(f"⏳ Waiting {wait_time}s before next attempt...")
                time.sleep(wait_time)
            else:
                print(f"⚠️ Could not rotate IP, waiting before retry...")
                time.sleep(10)
    
    print(f"\n❌ All {max_retries} attempts failed for URL: {url}")
    return None
