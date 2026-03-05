from playwright.sync_api import sync_playwright
from stem import Signal
from stem.control import Controller
import time
import random

def renew_tor_ip():
    try:
        with Controller.from_port(port=9051) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
            time.sleep(6)
            print("✓ Got new Tor IP")
    except Exception as e:
        print(f"Could not rotate IP: {e}")

def scrape(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                proxy={"server": "socks5://127.0.0.1:9050"},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu"
                ]
            )
            
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={
                    "width": random.randint(1200, 1400),
                    "height": random.randint(700, 900)
                },
                locale="en-US",
                timezone_id="Asia/Karachi",
                extra_http_headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1"
                }
            )
            
            # Advanced stealth
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
                Object.defineProperty(navigator, 'permissions', {get: () => ({query: () => ({state: 'granted'})})});
                window.chrome = {runtime:{}};
                Object.defineProperty(document, 'webdriver', {get: () => undefined});
                delete navigator.__proto__.webdriver;
                window.navigator.vendor = 'Google Inc.';
            """)
            
            page = context.new_page()
            print(f"Opening: {url}")
            
            # Random delay before goto
            time.sleep(random.uniform(2, 5))
            
            page.goto(url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(random.uniform(4000, 8000))
            
            current_url = page.url
            print(f"Current URL: {current_url}")
            
            # Check if blocked/redirected
            if "/item/" not in current_url or "punish" in current_url or "x5secdata" in current_url:
                print("❌ Blocked or wrong page")
                browser.close()
                return None
            
            # Simulate human behavior
            page.mouse.move(random.randint(200, 400), random.randint(300, 500))
            page.mouse.wheel(0, random.randint(1500, 2500))
            page.wait_for_timeout(random.uniform(3000, 6000))
            
            # Try to wait for title
            try:
                page.wait_for_selector("h1[data-pl='product-title']", timeout=15000)
            except:
                pass
            
            # Get title
            title = ""
            title_selectors = [
                "h1[data-pl='product-title']",
                "h1[class*='product-title']",
                "h1"
            ]
            
            for selector in title_selectors:
                try:
                    el = page.locator(selector).first
                    title = el.inner_text().strip()
                    if title and len(title) > 5:
                        print(f"✓ Title found: {title[:60]}...")
                        break
                except:
                    pass
            
            # Block check
            if not title or title.lower() in ["aliexpress", "just a moment", "attention required"]:
                print("❌ No title or blocked page")
                browser.close()
                return None
            
            # Scroll for description
            page.mouse.wheel(0, random.randint(3000, 4000))
            page.wait_for_timeout(random.uniform(2000, 4000))
            
            # Get description
            description = ""
            desc_selectors = [
                "div#product-description",
                "div[class*='description--product-description']",
                "div[class*='detailmodule_text']",
                "div[id*='description']"
            ]
            
            for selector in desc_selectors:
                try:
                    el = page.locator(selector).first
                    text = el.inner_text().strip()
                    if text and len(text) > 20 and text.lower() not in ["description", "report"]:
                        description = text[:1200]
                        print(f"✓ Description found: {len(description)} chars")
                        break
                except:
                    pass
            
            # Get bullet points
            bullet_points = []
            try:
                li_elements = page.locator("li").all()
                bullet_points = [
                    li.inner_text().strip() 
                    for li in li_elements 
                    if len(li.inner_text().strip()) > 10
                ][:8]
                print(f"✓ Bullets found: {len(bullet_points)}")
            except:
                pass
            
            # Get image
            image = ""
            img_selectors = ["img[class*='magnifier']", "img[src*='alicdn']", "img"]
            
            for selector in img_selectors:
                try:
                    img = page.locator(selector).first
                    src = img.get_attribute("src")
                    if src and len(src) > 20:
                        image = src
                        print(f"✓ Image found")
                        break
                except:
                    pass
            
            browser.close()
            
            if not title:
                print("❌ Failed to extract title")
                return None
            
            print("✅ Scrape successful")
            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image
            }
    
    except Exception as e:
        print(f"❌ Scraping error: {e}")
        return None


def get_product_info(url, max_retries=3):
    for attempt in range(max_retries):
        print(f"\n--- Attempt {attempt + 1} of {max_retries} ---")
        result = scrape(url)
        if result:
            return result
        
        if attempt < max_retries - 1:
            print("Blocked! Rotating Tor IP and waiting...")
            renew_tor_ip()
            time.sleep(random.uniform(8, 15))
    
    print("❌ All attempts failed")
    return None
