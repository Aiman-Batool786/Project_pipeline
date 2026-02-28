from playwright.sync_api import sync_playwright

def get_product_info(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                # Remove Tor proxy — try direct or use a residential proxy instead
                # proxy={"server": "socks5://127.0.0.1:9050"},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process",
                ]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi",
                # Mask automation signals
                java_script_enabled=True,
                has_touch=False,
                is_mobile=False,
            )

            # Inject stealth JS to hide navigator.webdriver
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {} };
            """)

            page = context.new_page()
            print("Opening URL:", url)

            # Wait for full network idle — critical for SPAs like AliExpress
            page.goto(url, timeout=60000, wait_until="networkidle")

            page.wait_for_timeout(3000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(3000)

            # Debug: check what page we actually landed on
            page_title = page.title()
            print("Page title:", page_title)

            # Detect block/login pages
            blocked_keywords = ["sign in", "login", "verify", "captcha", "robot", "blocked"]
            if any(k in page_title.lower() for k in blocked_keywords):
                print("Blocked or redirected to login/captcha page")
                # Optional: save screenshot for debugging
                page.screenshot(path="debug_screenshot.png")
                browser.close()
                return None

            # AliExpress-specific selectors (more reliable than generic h1)
            title = ""
            title_selectors = [
                "h1[data-pl='product-title']",
                ".product-title-text",
                "h1.title--wrap--UUHae_g",  # class names change, check DevTools
                "h1",
            ]
            for selector in title_selectors:
                if page.locator(selector).count() > 0:
                    title = page.locator(selector).first.inner_text().strip()
                    if title:
                        break

            paragraphs = page.locator("p").all_text_contents()
            description = " ".join(paragraphs[:5]) if paragraphs else ""

            bullets = page.locator("li").all_text_contents()
            bullet_points = bullets[:5] if bullets else []

            image = ""
            if page.locator("img").count() > 0:
                image = page.locator("img").first.get_attribute("src")

            browser.close()

            if not title:
                print("Could not extract title — likely blocked")
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
