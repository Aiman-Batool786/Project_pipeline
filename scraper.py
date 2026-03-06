from playwright.sync_api import sync_playwright
from stem import Signal
from stem.control import Controller
import time
import logging
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def renew_tor_ip():
    """Rotate to new Tor circuit"""
    try:
        with Controller.from_port(port=9051) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
            time.sleep(5)
            logger.info("✓ Got new Tor IP")
    except Exception as e:
        logger.error(f"✗ Could not rotate IP: {e}")


def scrape(url):
    """Scrape AliExpress product"""
    try:
        with sync_playwright() as p:
            # Launch browser (NO proxy here!)
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            )
            
            # Add proxy in context (CORRECT location!)
            context = browser.new_context(
                proxy={"server": "socks5://127.0.0.1:9050"},  # ← PROXY HERE
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )
            
            # Anti-detection
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {} };
            """)
            
            page = context.new_page()
            logger.info(f"Opening URL: {url}")
            
            # Navigate
            page.goto(url, timeout=90000, wait_until="domcontentloaded")
            time.sleep(3)
            
            # Simulate human behavior
            page.mouse.move(200, 300)
            time.sleep(1)
            page.mouse.wheel(0, 2000)
            time.sleep(2)
            
            # ===== TITLE =====
            title = ""
            for selector in [
                "h1[data-pl='product-title']",
                "h1[class*='product-title']",
                ".pc-main h1",
                "h1"
            ]:
                if page.locator(selector).count() > 0:
                    text = page.locator(selector).first.inner_text().strip()
                    if text and len(text) > 5:
                        title = text
                        break
            
            logger.info(f"Title: {title[:50] if title else 'NOT FOUND'}...")
            
            # ===== DESCRIPTION =====
            description = ""
            page.mouse.wheel(0, 3000)
            time.sleep(2)
            
            for selector in [
                "div[class*='description']",
                "div[id*='description']",
                "div[class*='detail']",
                "p"
            ]:
                elements = page.locator(selector).all()
                for elem in elements[:5]:
                    text = elem.inner_text().strip()
                    if text and len(text) > 20 and "description" not in text.lower():
                        description = text[:500]
                        break
                if description:
                    break
            
            logger.info(f"Description: {len(description)} chars")
            
            # ===== BULLET POINTS =====
            try:
                bullets = page.locator("li").all_text_contents()
                bullet_points = [b.strip() for b in bullets[:8] if b.strip() and len(b.strip()) > 5]
            except:
                bullet_points = []
            
            logger.info(f"Bullets: {len(bullet_points)} found")
            
            # ===== IMAGE =====
            image = ""
            selectors = [
                "img[src*='oss']",
                "img[class*='product']",
                "img[class*='main']",
                "img[data-src*='oss']"
            ]
            
            for selector in selectors:
                if page.locator(selector).count() > 0:
                    img = page.locator(selector).first
                    src = img.get_attribute("src") or img.get_attribute("data-src")
                    if src and ("http" in src or "data:" in src):
                        image = src
                        break
            
            logger.info(f"Image: {'FOUND' if image else 'NOT FOUND'}")
            
            browser.close()
            
            # Validate
            if not title:
                logger.warning("⚠ No title extracted!")
                return None
            
            logger.info("✓ Scraping successful!")
            
            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image
            }
            
    except Exception as e:
        logger.error(f"✗ Scraping failed: {e}")
        logger.error(traceback.format_exc())
        return None


def get_product_info(url, max_retries=3):
    """Get product info with retries"""
    for attempt in range(max_retries):
        logger.info(f"\n--- Attempt {attempt + 1}/{max_retries} ---")
        result = scrape(url)
        
        if result:
            logger.info("✓ SUCCESS!")
            return result
        
        if attempt < max_retries - 1:
            logger.warning("Blocked! Rotating IP...")
            renew_tor_ip()
    
    logger.error("✗ All attempts failed!")
    return None


# TEST
if __name__ == "__main__":
    url = "https://www.aliexpress.com/item/1005006246885476.html"
    result = get_product_info(url)
    
    if result:
        print("\n" + "="*50)
        print(f"Title: {result['title']}")
        print(f"Price: {result.get('price', 'N/A')}")
        print(f"Description: {result['description'][:100]}...")
        print(f"Image: {result['image_url'][:50]}...")
        print("="*50)
    else:
        print("Failed to scrape")
