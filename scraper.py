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
        
        # === UPDATED: Catch fake "Aliexpress" title (this was your exact problem) ===
        if page_title and page_title.strip().lower() in ["aliexpress", "ali express", "ali-express"]:
            print(f"🚫 Blocked/generic page detected: page title is '{page_title}'")
            return True
       
        # Check page content
        try:
            body_text = page.locator("body").inner_text().lower()
            if "verify" in body_text or "challenge" in body_text or "security" in body_text:
                if page_title and page_title.strip() == "":
                    print(f"🚫 Blocked page detected: empty title with security keywords")
                    return True
        except:
            pass
       
        print(f"✅ Page appears to be normal (not blocked)")
        return False
    except Exception as e:
        print(f"⚠️ Error checking if page is blocked: {e}")
        return False

def get_product_title(page):
    """Extract product title with multiple selector strategies"""
    # Wait for title to be visible first
    try:
        page.locator("h1[data-pl='product-title']").wait_for(timeout=10000)
        print(f"✅ Title element detected on page")
    except Exception as e:
        print(f"⚠️ Title wait failed: {e}")
   
    title_selectors = [
        "h1[data-pl='product-title']",
        "h1[data-spm-anchor-id]",
        "h1.product-title-text",
        ".product-title-text",
        "h1.title",
        "h1[class*='title']",
        "h1"
    ]
   
    for selector in title_selectors:
        try:
            count = page.locator(selector).count()
            if count > 0:
                title = page.locator(selector).first.inner_text().strip()
                # === UPDATED: stricter length to prevent "Aliexpress" ===
                if title and len(title) > 20: 
                    print(f"✅ Title found via '{selector}': {title[:80]}...")
                    return title
                else:
                    print(f"⚠️ Selector '{selector}' found but text too short: '{title}'")
        except Exception as e:
            print(f"⚠️ Error with selector '{selector}': {e}")
            continue
   
    print("❌ Could not extract title from any selector")
    return ""

def get_product_description(page):
    """Extract product description with multiple selector strategies"""
    print(f"📄 Extracting description...")
   
    # Scroll to load description
    try:
        page.mouse.wheel(0, 3000)
        page.wait_for_timeout(2000)
    except:
        pass
   
    # === UPDATED: exact selector from the HTML you pasted ===
    desc_selectors = [
        "div[style*='font-family:arial, helvetica, sans-serif'][style*='font-size:13px']",  # ← YOUR EXACT CONTAINER
        "div[style*='font-family:arial'][style*='font-size:13px']",
        "div[class*='description--description']",
        "div[class*='detailmodule_text']",
        "div[class*='description-content']",
        "div[id*='description']",
        "div[class*='product-description']",
        "div[class*='detail-desc']",
        "section[class*='description']"
    ]
   
    collected_text = []
   
    for selector in desc_selectors:
        try:
            elements = page.locator(selector).all()
            print(f" Trying selector '{selector}': found {len(elements)} elements")
           
            for element in elements:
                try:
                    text = element.inner_text().strip()          # ← full block, not tiny span
                    if text and len(text) > 30: 
                        if text.lower() not in ["features:", "description", "report", "details", "specifications"]:
                            if text not in collected_text:
                                collected_text.append(text)
                except:
                    continue
        except Exception as e:
            print(f" ⚠️ Error with selector '{selector}': {e}")
            continue
   
    if collected_text:
        full_desc = "\n".join(collected_text)
        desc = full_desc[:1000]
        print(f"✅ Description found: {len(collected_text)} text blocks, {len(desc)} chars")
        return desc
   
    print("⚠️ No description found")
    return ""

def get_bullet_points(page):
    """Extract bullet points"""
    try:
        bullets = []
       
        # Strategy 1: Get from <li> elements
        li_elements = page.locator("li").all()
        for li in li_elements[:10]:
            # === UPDATED: skip the bad Q&A elements you showed ===
            class_attr = (li.get_attribute("class") or "").lower()
            if "ask-item" in class_attr or "answer-box" in class_attr:
                continue
                
            text = li.inner_text().strip()
            if text and len(text) > 5:
                if text not in bullets:
                    bullets.append(text)
       
        if bullets:
            print(f"✅ Found {len(bullets)} bullet points")
            return bullets[:5]
       
        # Strategy 2: fallback (unchanged)
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
                src = img.get_attribute("src") or img.get_attribute("data-src") or img.get_attribute("data-original")
                if src and ("http" in src or src.startswith("/")):
                    if not src.startswith("data:"):
                        print(f"✅ Image found: {src[:100]}...")
                        return src
       
        print("⚠️ No image found")
        return ""
    except Exception as e:
        print(f"⚠️ Error extracting image: {e}")
        return ""

# ====================== scrape() and get_product_info() UNCHANGED ======================
def scrape(url, attempt_num=1):
    """
    Scrape product information from AliExpress URL
    """
    browser = None
    try:
        print(f"\n{'='*60}")
        print(f"🔍 Scraping: {url}")
        print(f"{'='*60}")
       
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
           
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )
           
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
           
            def handle_route(route):
                if route.request.resource_type in ["image", "stylesheet", "font"]:
                    route.abort()
                else:
                    route.continue_()
           
            # page.route("**/*", handle_route)
           
            print(f"🌐 Navigating to URL...")
            try:
                page.goto(url, timeout=120000, wait_until="domcontentloaded")
                print(f"✅ Page loaded (domcontentloaded)")
            except PlaywrightTimeoutError:
                print(f"⚠️ Page load timeout, continuing anyway...")
           
            print(f"⏳ Waiting for product elements to load...")
            try:
                page.wait_for_selector("h1[data-pl='product-title']", timeout=15000)
                print(f"✅ Title element loaded")
            except:
                print(f"⚠️ Title element timeout")
           
            page.wait_for_timeout(5000)
           
            print(f"👤 Simulating human behavior...")
            page.mouse.move(
                random.randint(100, 800),
                random.randint(100, 600)
            )
            page.wait_for_timeout(random.randint(1000, 3000))
            page.mouse.wheel(0, random.randint(1500, 3000))
            page.wait_for_timeout(random.randint(2000, 4000))
           
            final_url = page.url
            page_title = page.title()
           
            print(f"📄 Final URL: {final_url}")
            print(f"📋 Page Title: {page_title if page_title else '(empty)'}")
           
            if is_blocked_page(page):
                browser.close()
                return None
           
            print(f"\n📦 Extracting product data...")
            print(f"{'='*60}")
           
            title = get_product_title(page)
            if not title:
                print(f"❌ Failed to extract title - likely blocked or wrong page")
                browser.close()
                return None
           
            print(f"\n✅ Got title, now extracting description...")
            description = get_product_description(page)
           
            print(f"\n✅ Got description, now extracting bullet points...")
            bullet_points = get_bullet_points(page)
           
            print(f"\n✅ Got bullet points, now extracting image...")
            image_url = get_product_image(page)
           
            browser.close()
           
            print(f"\n{'='*60}")
            print(f"✅ SCRAPING SUCCESSFUL!")
            print(f"{'='*60}")
            print(f" ✓ Title: {title[:60]}...")
            print(f" ✓ Description: {len(description)} chars")
            print(f" ✓ Bullet points: {len(bullet_points)} items")
            print(f" ✓ Image: {image_url[:50] if image_url else 'None'}...")
           
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
