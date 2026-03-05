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
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
                window.chrome = { runtime: {} };
            """)

            page = context.new_page()

            print("Opening URL:", url)

            page.goto(url, timeout=90000, wait_until="domcontentloaded")

            page.wait_for_timeout(5000)

            page.mouse.move(200, 300)
            page.mouse.wheel(0, 1500)

            page.wait_for_timeout(3000)

            print("Page title:", page.title())

            # -------------------------------------------------
            # TITLE
            # -------------------------------------------------

            title = ""

            if page.locator('h1[data-pl="product-title"]').count() > 0:
                title = page.locator('h1[data-pl="product-title"]').first.inner_text().strip()

            if not title and page.locator("h1").count() > 0:
                title = page.locator("h1").first.inner_text().strip()

            print("TITLE:", title)

            # -------------------------------------------------
            # DESCRIPTION
            # -------------------------------------------------

            description = ""

            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(2000)

            if page.locator("#product-description").count() > 0:
                description = page.locator("#product-description").inner_text()

            elif page.locator('div[data-pl="product-description"]').count() > 0:
                description = page.locator('div[data-pl="product-description"]').inner_text()

            description = description.strip()[:800]

            print("DESCRIPTION FOUND:", len(description))

            # -------------------------------------------------
            # BULLET POINTS
            # -------------------------------------------------
            bullet_points = []

            bullets = page.locator("#product-description li").all_text_contents()

            if not bullets:
                bullets = page.locator("li").all_text_contents()

            bullet_points = [b.strip() for b in bullets if len(b.strip()) > 10][:5]

            print("BULLETS:", bullet_points)

            # -------------------------------------------------
            # IMAGE
            # -------------------------------------------------

            image = ""

            if page.locator('img[class*="magnifier"]').count() > 0:
                image = page.locator('img[class*="magnifier"]').first.get_attribute("src")

            elif page.locator('img[src*="alicdn"]').count() > 0:
                image = page.locator('img[src*="alicdn"]').first.get_attribute("src")

            print("IMAGE:", image)

            browser.close()

            if not title:
                print("Blocked or login page detected")
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


def get_product_info(url, max_retries=3):

    for attempt in range(max_retries):

        print(f"\n--- Attempt {attempt + 1} of {max_retries} ---")

        result = scrape(url)

        if result:
            return result

        if attempt < max_retries - 1:
            print("Blocked! Rotating Tor IP...")
            renew_tor_ip()

    print("All attempts failed")

    return None
