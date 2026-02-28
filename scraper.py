
from playwright.sync_api import sync_playwright
import random


def get_product_info(url):

    try:

        with sync_playwright() as p:

            browser = p.chromium.launch(

                headless=True,

                proxy={
                    "server": "socks5://127.0.0.1:9050"
                },

                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process"
                ]

            )

            context = browser.new_context(

                user_agent=random.choice([
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119 Safari/537.36"
                ]),

                viewport={"width": 1366, "height": 768},

                locale="en-US",

                timezone_id="Asia/Karachi",

                java_script_enabled=True

            )

            page = context.new_page()

            print("Opening URL:", url)

            page.goto(

                url,

                timeout=90000,

                wait_until="networkidle"

            )


            # wait for page fully load
            page.wait_for_timeout(7000)


            # scroll to load everything
            page.mouse.wheel(0, 3000)

            page.wait_for_timeout(3000)


            # SAVE DEBUG SCREENSHOT
            page.screenshot(path="debug.png")


            # CHECK CAPTCHA
            if "captcha" in page.url.lower():

                print("CAPTCHA detected")

                browser.close()

                return None


            # TITLE
            title = ""

            if page.locator('h1[data-pl="product-title"]').count() > 0:

                title = page.locator(
                    'h1[data-pl="product-title"]'
                ).first.inner_text()


            # IMAGE
            image = ""

            if page.locator('img[src*="alicdn"]').count() > 0:

                image = page.locator(
                    'img[src*="alicdn"]'
                ).first.get_attribute("src")


            # DESCRIPTION FROM IFRAME
            description = ""

            for frame in page.frames:

                if "description" in frame.url:

                    try:

                        description = frame.locator("body").inner_text()

                        break

                    except:
                        pass


            # BULLET POINTS
            bullet_points = []

            bullets = page.locator("ul li").all_text_contents()

            if bullets:

                bullet_points = bullets[:5]


            browser.close()


            if title == "":

                print("Scraping blocked")

                return None


            return {

                "title": title,

                "description": description,

                "bullet_points": bullet_points,

                "image_url": image

            }


    except Exception as e:

        print("SCRAPER ERROR:", e)

        return None
