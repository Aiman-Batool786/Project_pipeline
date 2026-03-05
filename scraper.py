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
            print("Got new Tor IP")
    except Exception as e:
        print("Could not rotate IP:", e)


def scrape(url):

    try:
        with sync_playwright() as p:

            browser = p.chromium.launch(
                headless=False,   # change to True later if needed
                proxy={"server": "socks5://127.0.0.1:9050"},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            )

            context = browser.new_context(

                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",

                viewport={
                    "width": random.randint(1200, 1400),
                    "height": random.randint(700, 900)
                },

                locale="en-US",
                timezone_id="Asia/Karachi",

                extra_http_headers={
                    "accept-language": "en-US,en;q=0.9",
                    "upgrade-insecure-requests": "1",
                    "referer": "https://www.google.com/"
                }
            )

            # stealth script
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
                window.chrome = {runtime:{}};
            """)

            page = context.new_page()

            print("Opening URL:", url)

            time.sleep(random.uniform(3,7))

            page.goto(url, timeout=90000)

            page.wait_for_load_state("networkidle")

            page.wait_for_timeout(5000)

            print("Current URL:", page.url)

            # FIXED redirect detection
            if "/item/" not in page.url:
                print("Redirect detected")
                browser.close()
                return None

            # simulate human behaviour
            page.mouse.move(300, 400)
            page.mouse.wheel(0, 2000)

            page.wait_for_timeout(3000)

            # Wait for product title
            try:
                page.wait_for_selector("h1[data-pl='product-title']", timeout=20000)
            except:
                print("Title selector timeout")

            # ------------------------
            # TITLE
            # ------------------------

            title = ""

            selectors = [
                "h1[data-pl='product-title']",
                "h1[class*='product-title']",
                "h1"
            ]

            for selector in selectors:
                if page.locator(selector).count() > 0:
                    title = page.locator(selector).first.inner_text().strip()
                    if title:
                        break

            print("TITLE:", title)

            # Blocked page detection
            blocked_titles = ["aliexpress", "just a moment", "attention required"]

            if title.lower() in blocked_titles:
                print("Blocked page detected")
                browser.close()
                return None

            # ------------------------
            # DESCRIPTION
            # ------------------------

            description = ""

            page.mouse.wheel(0, 4000)

            page.wait_for_timeout(3000)

            description_selectors = [
                "div#product-description",
                "div[class*='description--product-description']",
                "div[class*='detailmodule_text']",
                "div[id*='description']"
            ]

            for selector in description_selectors:

                if page.locator(selector).count() > 0:

                    text = page.locator(selector).first.inner_text().strip()

                    if text and text.lower() not in ["description", "report"]:
                        description = text[:1200]
                        break

            print("DESCRIPTION LENGTH:", len(description))

            # ------------------------
            # BULLET POINTS
            # ------------------------

            bullets = []

            if page.locator("li").count() > 0:
                bullets = page.locator("li").all_text_contents()

            bullet_points = [
                b.strip()
                for b in bullets
                if len(b.strip()) > 10
            ][:8]

            print("BULLETS:", bullet_points)

            # ------------------------
            # IMAGE
            # ------------------------

            image = ""

            image_selectors = [
                "img[class*='magnifier']",
                "img[src*='alicdn']",
                "img"
            ]

            for selector in image_selectors:

                if page.locator(selector).count() > 0:

                    src = page.locator(selector).first.get_attribute("src")

                    if src:
                        image = src
                        break

            print("IMAGE:", image)

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

        print(f"\n--- Attempt {attempt+1} of {max_retries} ---")

        result = scrape(url)

        if result:
            return result

        if attempt < max_retries - 1:
            print("Blocked! Rotating Tor IP...")
            renew_tor_ip()

    print("All attempts failed")

    return None
