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

            # Simulate human behaviour
            page.wait_for_timeout(6000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(4000)

            print("Page title:", page.title())
            print("Current URL:", page.url)

            # ── TITLE EXTRACTION ───────────────────────────────────────
            title = ""

            try:
                page.wait_for_selector('h1[data-pl="product-title"]',
                                       state="visible",
                                       timeout=30000)
                title_elem = page.locator('h1[data-pl="product-title"]').first
                title = title_elem.inner_text().strip()
                print("Found via data-pl →", title)
            except PlaywrightTimeoutError:
                print("Timeout waiting for h1[data-pl='product-title'] – page likely blocked or not loaded")

            if not title or len(title) < 20:
                print("Primary selector failed → trying fallbacks")

                fallback_selectors = [
                    'h1[data-pl="product-title"]',
                    'h1[class*="title"], h1[class*="name"]',
                    '[data-pl*="title"], [data-pl*="product"]',
                    'div[class*="product-title"], span[class*="title"]',
                    'h1'
                ]

                candidates = []
                for sel in fallback_selectors:
                    elements = page.locator(sel)
                    for el in elements.all():
                        txt = el.inner_text().strip()
                        if txt and len(txt) > 30 and "aliexpress" not in txt.lower() and "http" not in txt:
                            candidates.append(txt)

                if candidates:
                    title = max(candidates, key=len)
                    print("Fallback picked →", title)
                else:
                    all_h1 = [el.inner_text().strip() for el in page.locator("h1").all() if el.inner_text().strip()]
                    print("All visible <h1> texts:", all_h1)

            if title and ("aliexpress" in title.lower() or len(title) < 30 or "content saved" in title.lower()):
                title = ""
                print("Detected possible block/junk title – discarding")

            print("Final Title:", title or "NOT FOUND – check if blocked")

            # ── DESCRIPTION EXTRACTION ─────────────────────────────────
            description = ""
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(2000)

            desc_selectors = [
                "div[class*='description--description']",
                "div[class*='detailmodule_text']",
                "div[class*='description-content']",
                "div[id*='description']",
                "div[class*='product-description']",
            ]

            for selector in desc_selectors:
                el = page.locator(selector)
                if el.count() > 0:
                    text = el.first.inner_text().strip()
                    if text and text.lower() not in ["description", "report"]:
                        description = text[:500]
                        print(f"✅ Description found via: {selector}")
                        break

            # ── BULLET POINTS ──────────────────────────────────────────
            bullets = page.locator("li").all_text_contents()
            bullet_points = bullets[:5] if bullets else []

            # ── IMAGE ──────────────────────────────────────────────────
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
