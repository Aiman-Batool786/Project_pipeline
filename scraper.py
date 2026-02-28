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

            # Go to page
            page.goto(url, timeout=60000)

            # Wait for title to appear (IMPORTANT FIX)
            page.wait_for_selector('h1[data-pl="product-title"]', timeout=20000)

            # Simulate human behavior
            page.wait_for_timeout(2000)
            page.mouse.move(300, 400)
            page.mouse.wheel(0, 1500)
            page.wait_for_timeout(2000)

            # =========================
            # TITLE (FIXED SELECTOR)
            # =========================
            title = page.locator('h1[data-pl="product-title"]').inner_text().strip()

            # =========================
            # DESCRIPTION
            # =========================
            description = ""

            # Try product description section first
            if page.locator("#product-description").count() > 0:
                description = page.locator("#product-description").inner_text().strip()
            else:
                # fallback to strong tag (from nav area you showed)
                if page.locator("strong").count() > 0:
                    description = page.locator("strong").first.inner_text().strip()

            # =========================
            # BULLET POINTS (cleaner)
            # =========================
            bullet_points = []
            if page.locator("ul li").count() > 0:
                bullet_points = page.locator("ul li").all_text_contents()[:5]

            # =========================
            # MAIN IMAGE
            # =========================
            image_url = ""
            if page.locator("img").count() > 0:
                image_url = page.locator("img").first.get_attribute("src")

            browser.close()

            # =========================
            # BLOCK DETECTION
            # =========================
            if not title:
                print("Possible login page / CAPTCHA / blocked request")
                return None

            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image_url
            }

    except Exception as e:
        print("Scraping failed for URL:", url)
        print("Error:", e)
        return None
