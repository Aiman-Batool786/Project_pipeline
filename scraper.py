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
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            )

            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US"
            )

            # Anti-detection tweaks
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            """)

            page = context.new_page()

            print("🌐 Opening URL:", url)
            page.goto(url, timeout=90000, wait_until="domcontentloaded")

            # Wait for the main title element specifically to avoid logo-snatching
            try:
                page.wait_for_selector("h1[data-pl='product-title']", timeout=15000)
            except:
                pass

            print("📌 Current URL:", page.url)
            
            # 🚨 Detect login / captcha / punishment page
            if "punish" in page.url or "login" in page.url.lower():
                print("🚨 Login/Captcha page detected!")
                browser.close()
                return None

            # ─────────────────────────────
            # 🏷️ TITLE (CORRECTED)
            # ─────────────────────────────
            title = ""
            # We use a specific attribute selector to avoid the generic <a> or <h1> in the header
            title_selectors = [
                "h1[data-pl='product-title']",
                ".product-title-text",
                "h1.title--titleText--v7_m0_b"
            ]
            
            for sel in title_selectors:
                locator = page.locator(sel).first
                if locator.count() > 0:
                    text = locator.inner_text().strip()
                    if text and text.lower() != "aliexpress":
                        title = text
                        break

            # ─────────────────────────────
            # 📝 DESCRIPTION (CORRECTED)
            # ─────────────────────────────
            description = ""
            # AliExpress lazy-loads the description. We MUST scroll down.
            page.evaluate("window.scrollBy(0, 1500)")
            page.wait_for_timeout(2000)

            # Targeted description selectors
            desc_selectors = [
                "#product-description",
                ".product-description",
                "#item_description",
                ".detailmodule_text",
                ".description--content--A6ay_S3"
            ]

            for selector in desc_selectors:
                try:
                    el = page.locator(selector).first
                    if el.count() > 0:
                        text = el.inner_text().strip()
                        if len(text) > 20:
                            description = text
                            print(f"✅ Description found via {selector}")
                            break
                except:
                    continue

            # If still not found, it's often in an iframe called 'desc_html_content'
            if not description:
                try:
                    frame = page.frame(name="desc_html_content") or page.frame(url=lambda u: "desc" in u)
                    if frame:
                        description = frame.locator("body").inner_text().strip()
                        print("✅ Description found inside iframe")
                except:
                    pass

            browser.close()

            if not title or title.lower() == "aliexpress":
                print("❌ Correct title not found. Likely blocked or wrong selector.")
                return None

            return {
                "title": title,
                "description": description[:1000] if description else "No description found"
            }

    except Exception as e:
        print("❌ Scraping failed:", e)
        return None

# ─────────────────────────────────────────────
# 🔁 Retry Logic
# ─────────────────────────────────────────────
def get_product_info(url, max_retries=3):
    for attempt in range(max_retries):
        print(f"\n--- Attempt {attempt + 1} of {max_retries} ---")
        result = scrape(url)
        if result:
            return result
        
        if attempt < max_retries - 1:
            print("🔁 Blocked or Failed! Rotating Tor IP...")
            renew_tor_ip()

    return None
