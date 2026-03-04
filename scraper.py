from playwright.sync_api import sync_playwright
from stem import Signal
from stem.control import Controller
import time

# ─────────────────────────────────────────────
# 🔁 Rotate Tor IP
# ─────────────────────────────────────────────
def renew_tor_ip():
    try:
        with Controller.from_port(port=9051) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
            time.sleep(5)
            print("✅ Got new Tor IP")
    except Exception as e:
        print("❌ Could not rotate IP:", e)

# ─────────────────────────────────────────────
# 🔎 Scraper Function
# ─────────────────────────────────────────────
def scrape(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                proxy={"server": "socks5://127.0.0.1:9050"},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox"
                ]
            )

            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )

            # Prevent region-based redirects that trigger blocks
            context.add_cookies([{
                'name': 'aep_usuc_f',
                'value': 'region=US&site=glo&b_locale=en_US&curr=USD',
                'domain': '.aliexpress.com',
                'path': '/'
            }])

            page = context.new_page()
            
            # Anti-bot stealth script
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            print("🌐 Opening URL:", url)
            response = page.goto(url, timeout=90000, wait_until="domcontentloaded")
            
            # Wait for main content to render
            page.wait_for_timeout(5000)

            # 🚨 BLOCK DETECTION
            if "punish" in page.url or "login" in page.url.lower():
                print("🚨 Blocked by Captcha/Punish Page.")
                browser.close()
                return None

            # ─────────────────────────────
            # 🏷️ TITLE (FIXED SELECTOR)
            # ─────────────────────────────
            title = ""
            # Priority selectors that target the product container specifically
            title_selectors = [
                "h1[data-pl='product-title']",
                "div.product-title h1",
                "h1.title--titleText--v7_m0_b",
                ".product-title-text"
            ]

            for selector in title_selectors:
                try:
                    locator = page.locator(selector).first
                    if locator.is_visible():
                        text = locator.inner_text().strip()
                        # CRITICAL: Ignore the logo if it's accidentally caught
                        if text and "AliExpress" not in text:
                            title = text
                            print(f"✅ Title found via: {selector}")
                            break
                except:
                    continue

            # ─────────────────────────────
            # 📝 DESCRIPTION (FIXED SELECTOR)
            # ─────────────────────────────
            description = ""
            # AliExpress requires scrolling to load the description data
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(3000)

            desc_selectors = [
                "#product-description",
                ".product-description",
                "div.description--content--A6ay_S3",
                "#j-product-desc",
                ".detailmodule_text"
            ]

            for selector in desc_selectors:
                try:
                    el = page.locator(selector).first
                    if el.count() > 0:
                        text = el.inner_text().strip()
                        if len(text) > 30:
                            description = text
                            print(f"✅ Description found via: {selector}")
                            break
                except:
                    continue

            # Fallback: Check if description is hidden in an iframe
            if not description:
                for frame in page.frames:
                    if "desc" in frame.url.lower():
                        try:
                            description = frame.locator("body").inner_text().strip()
                            print("✅ Description found inside iframe")
                            break
                        except:
                            continue

            # ─────────────────────────────
            # 🖼️ IMAGE & DATA VALIDATION
            # ─────────────────────────────
            image = ""
            try:
                img_locator = page.locator(".magnifier--image--He47f9B, .pdp-main-image img").first
                image = img_locator.get_attribute("src")
            except:
                pass

            browser.close()

            if not title:
                print("❌ Failed to find product title.")
                return None

            return {
                "title": title,
                "description": description[:1000] if description else "Description missing",
                "image_url": image
            }

    except Exception as e:
        print(f"❌ Scraping error: {e}")
        return None

# ─────────────────────────────────────────────
# 🔁 Main Execution Logic
# ─────────────────────────────────────────────
def get_product_info(url, max_retries=3):
    for attempt in range(max_retries):
        print(f"\n--- Attempt {attempt + 1} of {max_retries} ---")
        result = scrape(url)
        
        if result:
            print("\n🎉 SUCCESS:")
            print(f"Title: {result['title'][:70]}...")
            return result
        
        if attempt < max_retries - 1:
            print("🔁 Rotating IP and retrying...")
            renew_tor_ip()
            
    return None
