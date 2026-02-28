from playwright.sync_api import sync_playwright

def get_product_info(url):   # ✅ fixed missing bracket
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

            # ✅ Wait properly for dynamic load (AliExpress loads via JS)
            page.wait_for_load_state("networkidle")

            # ✅ Safer wait (try multiple selectors)
            try:
                page.wait_for_selector('h1[data-pl="product-title"]', timeout=15000)
            except:
                page.wait_for_selector("h1", timeout=15000)

            # Simulate human behavior
            page.wait_for_timeout(2000)
            page.mouse.move(300, 400)
            page.mouse.wheel(0, 1500)
            page.wait_for_timeout(2000)

            # =========================
            # TITLE (MORE STABLE)
            # =========================
            title = ""

            if page.locator('h1[data-pl="product-title"]').count() > 0:
                title = page.locator('h1[data-pl="product-title"]').first.inner_text().strip()
            elif page.locator("h1").count() > 0:
                title = page.locator("h1").first.inner_text().strip()

            # =========================
            # DESCRIPTION
            # =========================
            description = ""

            if page.locator("#product-description").count() > 0:
                description = page.locator("#product-description").first.inner_text().strip()
            elif page.locator("strong").count() > 0:
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

    except Exception as e:
        print("Scraping failed for URL:", url)
        print("Error:", e)
        return None
