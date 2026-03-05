from playwright.sync_api import sync_playwright
from stem import Signal
from stem.control import Controller
import time


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
                headless=False,
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

            # Stealth script
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined})
                Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]})
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']})
                window.chrome = {runtime:{}}
            """)

            page = context.new_page()

            print("Opening URL:", url)

            page.goto(url, timeout=90000)

            # wait for full network idle
            page.wait_for_load_state("networkidle")

            page.wait_for_timeout(5000)

            print("Current URL:", page.url)

            # Detect redirect
            if "aliexpress.com/item" not in page.url:
                print("Redirect detected!")
                browser.close()
                return None

            # simulate human behaviour
            page.mouse.move(300, 400)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(3000)

            # Wait for product title
            try:
                page.wait_for_selector("h1", timeout=20000)
            except:
                print("Title not found")
                browser.close()
                return None

            # TITLE
            title = ""
            for selector in [
                "h1[data-pl='product-title']",
                "h1[class*='product-title']",
                "h1"
            ]:
                if page.locator(selector).count() > 0:
                    title = page.locator(selector).first.inner_text().strip()
                    if title:
                        break

            # DESCRIPTION
            description = ""
            page.mouse.wheel(0, 4000)
            page.wait_for_timeout(3000)

            for selector in [
                "div#product-description",
                "div[class*='description--product-description']",
                "div[class*='detailmodule_text']",
                "div[id*='description']"
            ]:
                if page.locator(selector).count() > 0:
                    text = page.locator(selector).first.inner_text().strip()
                    if text and text.lower() not in ["description", "report"]:
                        description = text[:1200]
                        break

            # BULLET POINTS
            bullets = []
            if page.locator("li").count() > 0:
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

        print(f"\n--- Attempt {attempt+1} of {max_retries} ---")

        result = scrape(url)

        if result:
            return result

        if attempt < max_retries - 1:
            print("Blocked! Rotating IP...")
            renew_tor_ip()

    return None
