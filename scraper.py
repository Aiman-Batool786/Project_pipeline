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
            print("✅ Got new Tor IP")
    except Exception as e:
        print("❌ Could not rotate IP:", e)

def scrape(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                proxy={"server": "socks5://127.0.0.1:9050"},
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            
            # Anti-detection
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            page = context.new_page()
            print("🌐 Opening URL:", url)
            
            # Use 'networkidle' to ensure data-heavy elements load
            page.goto(url, timeout=90000, wait_until="networkidle")
            page.wait_for_timeout(5000)

            # --- 🏷️ TITLE CORRECTION ---
            title = ""
            # AliExpress often uses very specific class names that start with 'title--'
            title_selectors = [
                "h1[data-pl='product-title']", 
                "h1.title--titleText--v7_m0_b", 
                ".product-title-text",
                "div#j-product-detail-name h1"
            ]
            
            for selector in title_selectors:
                locator = page.locator(selector).first
                if locator.count() > 0:
                    raw_title = locator.inner_text().strip()
                    # Verify we didn't just grab the "AliExpress" logo text
                    if raw_title and "AliExpress" not in raw_title:
                        title = raw_title
                        break

            # --- 📝 DESCRIPTION CORRECTION ---
            description = ""
            # 1. Scroll down to trigger lazy-loading of the description area
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(3000)

            # 2. Check for Iframe (Most common for Ali descriptions)
            # Ali often puts descriptions in an iframe with 'desc' in the URL
            desc_iframe = None
            for frame in page.frames:
                if "desc" in frame.url or "description" in frame.url:
                    desc_iframe = frame
                    break
            
            if desc_iframe:
                try:
                    description = desc_iframe.locator("body").inner_text().strip()
                    print("✅ Description found via Iframe")
                except:
                    pass

            # 3. Fallback: Check standard div selectors if no iframe found
            if not description:
                desc_selectors = [
                    "div.description--content--A6ay_S3",
                    "div#product-description",
                    "div.detailmodule_text",
                    "div.product-description"
                ]
                for selector in desc_selectors:
                    el = page.locator(selector).first
                    if el.count() > 0:
                        text = el.inner_text().strip()
                        if len(text) > 30:
                            description = text
                            print(f"✅ Description found via: {selector}")
                            break

            # --- 🖼️ IMAGE ---
            image = ""
            img_el = page.locator(".magnifier--image--He47f9B, .pdp-main-image img").first
            if img_el.count() > 0:
                image = img_el.get_attribute("src")

            browser.close()

            if not title:
                print("⚠️ No title found. Check if the page is rendering correctly.")
                return None

            return {
                "title": title,
                "description": description[:1000] if description else "N/A",
                "image_url": image
            }

    except Exception as e:
        print("❌ Error:", e)
        return None

def get_product_info(url, max_retries=3):
    for attempt in range(max_retries):
        print(f"\n--- Attempt {attempt + 1} ---")
        result = scrape(url)
        if result:
            return result
        renew_tor_ip()
    return None
