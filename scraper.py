from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from stem import Signal
from stem.control import Controller
import time


# ────────────────────────────────────────────────
# TOR IP ROTATION
# ────────────────────────────────────────────────
def renew_tor_ip():
    try:
        with Controller.from_port(port=9051) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
            time.sleep(5)
            print("Got new Tor IP")
    except Exception as e:
        print("Could not rotate IP:", e)


# ────────────────────────────────────────────────
# MAIN SCRAPE FUNCTION
# ────────────────────────────────────────────────
def scrape(url):
    try:
        with sync_playwright() as p:

            # ── Browser & Context Setup ───────────────────────────────
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

            # ── Navigation & Human Simulation ──────────────────────────
            print("Opening URL:", url)
            page.goto(url, timeout=90000, wait_until="domcontentloaded")

            page.wait_for_timeout(6000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(4000)

            print("Page title:", page.title())
            print("Current URL:", page.url)

            # ── Block Detection ────────────────────────────────────────
            current_url = page.url
            page_title = page.title().strip().lower()
            if (
                "/item/" not in current_url
                or page_title in ["aliexpress", "aliexpress.com", ""]
                or "login" in current_url
                or "passport" in current_url
            ):
                print("❌ Blocked or redirected — not a product page")
                browser.close()
                return None

            # ── Title ──────────────────────────────────────────────────
            title = ""
            for selector in ["h1[data-pl='product-title']", ".product-title-text", "h1"]:
                if page.locator(selector).count() > 0:
                    title = page.locator(selector).first.inner_text().strip()
                    if title:
                        break

            # ── Description ────────────────────────────────────────────
            description = ""
            page.mouse.wheel(0, 5000)
            page.wait_for_timeout(3000)

            try:
                page.wait_for_selector("p.detail-desc-decorate-content", timeout=60000)
                paragraphs = page.locator("p.detail-desc-decorate-content")
                all_text = []
                for i in range(paragraphs.count()):
                    text = paragraphs.nth(i).inner_text().strip()
                    if text:
                        all_text.append(text)
                description = "\n".join(all_text)
                if description:
                    print("✅ Description extracted successfully")
            except Exception as e:
                print("❌ Description not found:", e)

            # ── Bullet Points ──────────────────────────────────────────
            bullets = page.locator("li").all_text_contents()
            bullet_points = bullets[:5] if bullets else []

            # ── Image ──────────────────────────────────────────────────
            image = ""
            if page.locator("img").count() > 0:
                image = page.locator("img").first.get_attribute("src")

            browser.close()

            if not title:
                print("Login page detected or scraping blocked")
                return None

            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image
            }

    except Exception as e:
        print("Scraping failed for URL:", url)
        print("Error:", e)
        return None


# ────────────────────────────────────────────────
# RETRY WRAPPER WITH TOR ROTATION
# ────────────────────────────────────────────────
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
