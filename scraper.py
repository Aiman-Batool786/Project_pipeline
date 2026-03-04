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

            # 🔥 REMOVE TOR FOR TESTING
            browser = p.chromium.launch(
                headless=False,  # Important for testing
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            )

            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/124.0.0.0 Safari/537.36",
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

            # Step 1: Open homepage first
            print("Opening homepage...")
            page.goto("https://www.aliexpress.com", wait_until="domcontentloaded")
            page.wait_for_timeout(5000)

            # Step 2: Open product page
            print("Opening product URL:", url)
            page.goto(url, timeout=90000, wait_until="domcontentloaded")
            page.wait_for_timeout(6000)

            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(4000)

            print("Page title:", page.title())
            print("Current URL:", page.url)

            # ── TITLE ─────────────────────────────
            title = ""
            try:
                page.wait_for_selector("h1[data-pl='product-title']", timeout=30000)
                title = page.locator("h1[data-pl='product-title']").inner_text().strip()
                print("✅ Title found")
            except Exception as e:
                print("❌ Error fetching title:", e)

            # ── DESCRIPTION ───────────────────────
            description = ""
            try:
                page.wait_for_selector("p.detail-desc-decorate-content", timeout=30000)

                paragraphs = page.locator("p.detail-desc-decorate-content")
                texts = []

                for i in range(paragraphs.count()):
                    txt = paragraphs.nth(i).inner_text().strip()
                    if txt:
                        texts.append(txt)

                description = "\n".join(texts)
                print("✅ Description found")

            except Exception as e:
                print("❌ Error fetching description:", e)

            browser.close()

            if not title:
                print("⚠️ Possibly blocked or login page")
                return None

            return {
                "title": title,
                "description": description
            }

    except Exception as e:
        print("Scraping failed for URL:", url)
        print("Error:", e)
        return None


def get_product_info(url, max_retries=3):
    for attempt in range(max_retries):
        print(f"\n--- Attempt {attempt + 1} of {max_retries} ---")
        result = scrape(url)

        if result:
            return result

        if attempt < max_retries - 1:
            print("Blocked! Rotating Tor IP and retrying...")
            renew_tor_ip()

    print("All attempts failed for URL:", url)
    return None
