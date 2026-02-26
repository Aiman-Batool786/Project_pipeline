from playwright.sync_api import sync_playwright


def get_product_info(url):

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

                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",

                viewport={"width": 1366, "height": 768},

                locale="en-US",

                timezone_id="Asia/Karachi"

            )

            page = context.new_page()

            print("Opening URL:", url)

            page.goto(

                url,

                timeout=60000,

                wait_until="domcontentloaded"

            )

            # simulate human behaviour
            page.wait_for_timeout(3000)

            page.mouse.move(200, 300)

            page.mouse.wheel(0, 2000)

            page.wait_for_timeout(3000)


            # safer title extraction
            title = ""

            if page.locator("h1").count() > 0:

                title = page.locator("h1").first.inner_text()


            # description extraction
            paragraphs = page.locator("p").all_text_contents()

            description = " ".join(paragraphs[:5]) if paragraphs else ""


            # bullet points
            bullets = page.locator("li").all_text_contents()

            bullet_points = bullets[:5] if bullets else []


            # image
            image = ""

            if page.locator("img").count() > 0:

                image = page.locator("img").first.get_attribute("src")


            browser.close()


            if title == "":
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
