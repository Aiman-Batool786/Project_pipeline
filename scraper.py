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
            time.sleep(10)  # longer wait for new IP to settle
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
       
        block_indicators = [
            "punish", "x5secdata", "x5step", "tmd_____", "login", "verify",
            "challenge", "access_denied"
        ]
       
        if any(indicator in current_url.lower() for indicator in block_indicators):
            print(f"🚫 Blocked page detected in URL: {current_url}")
            return True
        
        page_title_clean = (page_title or "").strip().lower()
        if page_title_clean in ["", "aliexpress", "ali express", "ali-express"]:
            if "aliexpress" in current_url.lower():
                # Debug: show first 300 chars of body
                try:
                    body_snippet = page.locator("body").inner_text()[:300]
                    print(f"🚫 BLOCKED / Challenge page detected! Title = '{page_title}'")
                    print(f"   URL: {current_url}")
                    print(f"   Body snippet: {body_snippet}...")
                except:
                    pass
                return True
       
        try:
            body_text = page.locator("body").inner_text().lower()
            if "verify" in body_text or "challenge" in body_text or "security" in body_text:
                if page_title_clean == "":
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
    try:
        page.locator("h1[data-pl='product-title']").wait_for(timeout=15000)
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
                if title and len(title) > 25:
                    print(f"✅ Title found via '{selector}': {title[:80]}...")
                    return title
                else:
                    print(f"⚠️ Selector '{selector}' found but text too short/generic: '{title}'")
        except Exception as e:
            print(f"⚠️ Error with selector '{selector}': {e}")
            continue
   
    print("❌ Could not extract title from any selector")
    return ""

def get_product_description(page):
    """Extract product description with multiple selector strategies"""
    print(f"📄 Extracting description...")
   
    try:
        page.mouse.wheel(0, 3000)
        page.wait_for_timeout(2000)
    except:
        pass
   
    desc_selectors = [
        "div[style*='font-family:arial'][style*='font-size:13px'] > div > span > span",
        "div[style*='font-family:arial'] div span span",
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
                    text = element.inner_text().strip()
                    if text and len(text) > 10:
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
        li_elements = page.locator("li").all()
        for li in li_elements[:10]:
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

def scrape(url, attempt_num=1):
    """
    Scrape product information from AliExpress URL
    """
    browser = None
    try:
        print(f"\n{'='*70}")
        print(f"🔍 Scraping attempt {attempt_num}: {url}")
        print(f"{'='*70}")
       
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                proxy={"server": "socks5://127.0.0.1:9050"},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-web-security",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-features=IsolateOrigins,site-per-process"
                ]
            )
           
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )
           
            # Stronger stealth script
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
                Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
                window.chrome = { runtime: {} };
                Object.defineProperty(screen, 'width', { get: () => 1920 });
                Object.defineProperty(screen, 'height', { get: () => 1080 });
            """)
           
            page = context.new_page()
           
            # Navigate with networkidle
            print(f"🌐 Navigating to URL...")
            try:
                page.goto(url, timeout=120000, wait_until="domcontentloaded")
                page.wait_for_load_state("networkidle", timeout=30000)
                print(f"✅ Page loaded (networkidle)")
            except PlaywrightTimeoutError:
                print(f"⚠️ Page load timeout, continuing anyway...")
           
            # Force .com domain if redirected to .us
            if ".us/" in page.url:
                original_url = url.replace("aliexpress.us", "aliexpress.com")
                print(f"🔄 Redirected to .us → forcing back to .com")
                page.goto(original_url, timeout=60000, wait_until="domcontentloaded")
           
            # Extra long wait + heavy human simulation
            page.wait_for_timeout(8000)
            
            print(f"👤 Simulating heavy human behavior...")
            for _ in range(3):
                page.mouse.move(random.randint(100, 1200), random.randint(100, 700))
                page.wait_for_timeout(random.randint(800, 2000))
                page.mouse.wheel(0, random.randint(800, 2000))
                page.wait_for_timeout(random.randint(1500, 3000))
            
            # Click random element to simulate real user
            try:
                page.locator("body").click(position={"x": random.randint(100, 800), "y": random.randint(100, 500)})
            except:
                pass
            
            final_url = page.url
            page_title = page.title()
           
            print(f"📄 Final URL: {final_url}")
            print(f"📋 Page Title: {page_title if page_title else '(empty)'}")
           
            if is_blocked_page(page):
                browser.close()
                return None
           
            print(f"\n📦 Extracting product data...")
            print(f"{'='*70}")
           
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
           
            print(f"\n{'='*70}")
            print(f"✅ SCRAPING SUCCESSFUL!")
            print(f"{'='*70}")
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

def get_product_info(url, max_retries=6):  # increased to 6 attempts
    """Get product info with retry logic and IP rotation"""
    print(f"\n{'='*70}")
    print(f"🚀 Starting scrape process for: {url}")
    print(f"{'='*70}")
   
    for attempt in range(max_retries):
        print(f"\n📍 Attempt {attempt + 1} of {max_retries}")
       
        result = scrape(url, attempt + 1)
       
        if result:
            print(f"\n🎉 SUCCESS on attempt {attempt + 1}!")
            return result
       
        if attempt < max_retries - 1:
            print(f"\n🔄 Failed, rotating Tor IP and retrying...")
            if renew_tor_ip():
                wait_time = random.randint(12, 25)  # longer wait
                print(f"⏳ Waiting {wait_time}s before next attempt...")
                time.sleep(wait_time)
            else:
                time.sleep(15)
   
    print(f"\n❌ All {max_retries} attempts failed for URL: {url}")
    return None
