from playwright.sync_api import sync_playwright
from stem import Signal
from stem.control import Controller
import time


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
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                }
            )
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'permissions', { get: () => ({ query: () => ({state: 'granted'}) }) });
                window.chrome = { runtime: {} };
                Object.defineProperty(document, 'webdriver', { get: () => undefined });
            """)
            page = context.new_page()
            print("Opening URL:", url)
            page.goto(url, timeout=90000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
            
            # Check if redirected
            current_url = page.url
            if current_url != url and ("redirect" in current_url or "punish" in current_url or current_url.split('/')[2] != url.split('/')[2]):
                print(f"❌ BLOCKED - Redirected to: {current_url[:100]}")
                browser.close()
                return None
            
            page.wait_for_timeout(6000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(4000)

            # TITLE - FIXED SELECTOR
            title = ""
            for selector in ["h1[data-pl='product-title']", "h1[class*='product-title']", "h1"]:
                if page.locator(selector).count() > 0:
                    title = page.locator(selector).first.inner_text().strip()
                    if title:
                        break

            # DESCRIPTION - FIXED SELECTOR
            description = ""
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(2000)
            for selector in ["div#product-description", "div[class*='description--product-description']", "div[class*='detailmodule_text']", "div[id*='description']"]:
                if page.locator(selector).count() > 0:
                    text = page.locator(selector).first.inner_text().strip()
                    if text and text.lower() not in ["description", "report"]:
                        description = text[:1000]
                        break

            # BULLET POINTS
            bullets = page.locator("li").all_text_contents()
            bullet_points = [b.strip() for b in bullets[:8] if b.strip()]

            # IMAGE
            image = ""
            if page.locator("img").count() > 0:
                src = page.locator("img").first.get_attribute("src")
                if src:
                    image = src

            browser.close()
            
            if not title:
                return None
            
            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image
            }
    except Exception as e:
        print("Scraping failed:", e)
        return None


def get_product_info(url, max_retries=3):
    for attempt in range(max_retries):
        print(f"\n--- Attempt {attempt + 1} of {max_retries} ---")
        result = scrape(url)
        if result:
            return result
        if attempt < max_retries - 1:
            print("Blocked! Retrying...")
            time.sleep(5)
    return None
