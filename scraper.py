from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
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
                    "--disable-dev-shm-usage"
                ]

            )

            context = browser.new_context(

                user_agent=random.choice([
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119 Safari/537.36"
                ]),

                viewport={"width": 1366, "height": 768},

                locale="en-US",

                timezone_id="Asia/Karachi"

            )

            page = context.new_page()


            # ✅ APPLY STEALTH HERE
            stealth_sync(page)


            print("Opening URL:", url)


            page.goto(

                url,

                timeout=90000

            )


            page.wait_for_timeout(8000)


            page.mouse.wheel(0, 3000)

            page.wait_for_timeout(4000)


            # DEBUG
            page.screenshot(path="debug.png")


            # CAPTCHA CHECK
            if "captcha" in page.url.lower():

                print("CAPTCHA detected")

                browser.close()

                return None


            # TITLE
            title = ""

            if page.locator("h1").count() > 0:

                title = page.locator("h1").first.inner_text()


            # IMAGE
            image = ""

            if page.locator("img").count() > 0:

                image = page.locator("img").first.get_attribute("src")


            # DESCRIPTION
            description = ""

            for frame in page.frames:

                try:

                    text = frame.locator("body").inner_text()

                    if len(text) > 200:

                        description = text

                        break

                except:

                    pass


            # BULLETS
            bullet_points = page.locator("ul li").all_text_contents()[:5]


            browser.close()


            if title == "":

                print("Blocked by AliExpress")

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
