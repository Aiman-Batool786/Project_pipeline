from playwright.sync_api import sync_playwright


def get_product_info(url):

    try:

        with sync_playwright() as p:

            browser = p.chromium.launch(

                headless=True,
                proxy={"server": "socks5://127.0.0.1:9050"},  # Tor proxy (unchanged)

                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]

            )

            context = browser.new_context(

                # FIX 1: Updated Chrome version (115 is 2 years old, gets flagged)
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",

                viewport={"width": 1366, "height": 768},

                locale="en-US",

                # FIX 2: Timezone now matches en-US locale (Karachi + en-US = bot signal)
                timezone_id="America/New_York",

                # FIX 3: Added real browser headers (missing headers = bot signal)
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Referer": "https://www.google.com/",
                }

            )

            # FIX 4: Mask navigator.webdriver (the #1 thing bot detectors check)
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            """)

            page = context.new_page()

            print("Opening URL:", url)

            page.goto(

                url,

                timeout=60000,

                # FIX 5: networkidle instead of domcontentloaded
                # AliExpress is JS-rendered — domcontentloaded fires before
                # product data loads. networkidle waits for JS to finish.
                wait_until="networkidle"

            )

            # simulate human behaviour (unchanged)
            page.wait_for_timeout(3000)

            page.mouse.move(200, 300)

            page.mouse.wheel(0, 2000)

            page.wait_for_timeout(3000)


            # safer title extraction (unchanged)
            title = ""

            if page.locator("h1").count() > 0:

                title = page.locator("h1").first.inner_text()


            # description extraction (unchanged)
            paragraphs = page.locator("p").all_text_contents()

            description = " ".join(paragraphs[:5]) if paragraphs else ""


            # bullet points (unchanged)
            bullets = page.locator("li").all_text_contents()

            bullet_points = bullets[:5] if bullets else []


            # image (unchanged)
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
