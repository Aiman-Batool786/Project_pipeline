from playwright.sync_api import sync_playwright


def get_product_info(url):

    try:

        with sync_playwright() as p:

            print("[scraper] Launching browser...")

            browser = p.chromium.launch(

                headless=True,
                proxy={"server": "socks5://127.0.0.1:9050"},

                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]

            )

            print("[scraper] Browser launched OK")

            context = browser.new_context(

                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",

                viewport={"width": 1366, "height": 768},

                locale="en-US",

                timezone_id="America/New_York",

                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Referer": "https://www.google.com/",
                }

            )

            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            """)

            page = context.new_page()

            print("[scraper] Navigating to:", url)

            page.goto(

                url,

                timeout=60000,

                wait_until="networkidle"

            )

            print("[scraper] Page loaded. Current URL:", page.url)
            print("[scraper] Page title tag:", page.title())

            # simulate human behaviour
            page.wait_for_timeout(3000)

            page.mouse.move(200, 300)

            page.mouse.wheel(0, 2000)

            page.wait_for_timeout(3000)

            # DEBUG: Print first 500 chars of page body to see what actually loaded
            try:
                body_text = page.locator("body").inner_text()
                print("[scraper] Body preview (first 500 chars):")
                print(body_text[:500])
                print("[scraper] ---")
            except Exception as body_err:
                print("[scraper] Could not read body:", body_err)

            # title extraction
            title = ""

            h1_count = page.locator("h1").count()
            print(f"[scraper] h1 elements found: {h1_count}")

            if h1_count > 0:

                title = page.locator("h1").first.inner_text()
                print("[scraper] Title extracted:", title[:100])

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
                print("[scraper] BLOCKED: title is empty - likely CAPTCHA or login wall")
                return None

            return {

                "title": title,

                "description": description,

                "bullet_points": bullet_points,

                "image_url": image

            }


    except Exception as e:

        print(f"[scraper] EXCEPTION TYPE: {type(e).__name__}")
        print(f"[scraper] EXCEPTION MESSAGE: {e}")
        return None
