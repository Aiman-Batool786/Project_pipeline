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

            context.add_cookies([
                {"name": "aep_usuc_f", "value": "site=glo&c_tp=USD&region=US&b_locale=en_US", "domain": ".aliexpress.com", "path": "/"},
                {"name": "intl_locale", "value": "en_US", "domain": ".aliexpress.com", "path": "/"},
            ])

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

                timeout=120000,

                wait_until="domcontentloaded"

            )

            # simulate human behaviour
            page.wait_for_timeout(3000)

            page.mouse.move(200, 300)

            page.mouse.wheel(0, 2000)

            page.wait_for_timeout(3000)


            # CHANGE 1: Title from data-pl="product-title" attribute
            title = ""

            if page.locator("[data-pl='product-title']").count() > 0:
                title = page.locator("[data-pl='product-title']").first.inner_text().strip()

            # Fallback to h1 if data-pl not found
            if not title and page.locator("h1").count() > 0:
                title = page.locator("h1").first.inner_text().strip()


            # CHANGE 2: Description by clicking nav Description link then fetching content
            description = ""

            try:
                # Click the Description link in the nav bar
                desc_link = page.locator("a[href='#nav-description']")
                if desc_link.count() > 0:
                    desc_link.first.click()
                    page.wait_for_timeout(2000)

                # Fetch content from the description section
                desc_section = page.locator("#nav-description")
                if desc_section.count() > 0:
                    description = desc_section.first.inner_text(timeout=5000).strip()[:2000]

            except Exception as e:
                print("Description section error:", e)

            # Fallback to paragraphs if description section not found
            if not description:
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
