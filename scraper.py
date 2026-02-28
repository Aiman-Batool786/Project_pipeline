from playwright.sync_api import sync_playwright
import random

def get_product_info(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                proxy={"server": "socks5://127.0.0.1:9050"},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                timezone_id="Asia/Karachi",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                }
            )

            # ✅ Mask webdriver
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            """)

            page = context.new_page()
            print("Opening URL:", url)

            # ✅ Use domcontentloaded (faster), then wait for JS manually
            page.goto(url, timeout=60000, wait_until="domcontentloaded")

            # ✅ Give JS time to boot up
            page.wait_for_timeout(3000)

            # ✅ Scroll to trigger lazy loading
            page.mouse.wheel(0, 500)
            page.wait_for_timeout(2000)

            # ✅ Wait for title with longer timeout
            try:
                page.wait_for_selector(
                    'h1[data-pl="product-title"]',
                    timeout=30000,   # 30 seconds
                    state="visible"
                )
            except Exception:
                # ✅ DEBUG: print page title and URL to understand what loaded
                print("Page title:", page.title())
                print("Current URL:", page.url)
                # ✅ Check if redirected to login or captcha
                current_url = page.url
                if "login" in current_url or "passport" in current_url:
                    print("❌ Redirected to login page!")
                elif "robot" in page.title().lower() or "verify" in page.title().lower():
                    print("❌ CAPTCHA page detected!")
                else:
                    print("❌ Product title not found - page may not have loaded correctly")
                    # Save full HTML for inspection
                    with open("/tmp/debug_page.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    print("💾 Full HTML saved to /tmp/debug_page.html")
                browser.close()
                return None

            print("✅ Page loaded successfully!")
            print("Page title:", page.title())

            # =========================
            # TITLE
            # =========================
            title = ""
            el = page.locator('h1[data-pl="product-title"]')
            if el.count() > 0:
                title = el.first.inner_text().strip()
            print("✅ Title:", title)

            # =========================
            # DESCRIPTION
            # =========================
            description = ""
            desc_selectors = [
                '#nav-description strong',
                'div[class*="description"] strong',
                '[id*="description"] strong',
                '.product-description strong',
            ]
            for selector in desc_selectors:
                el = page.locator(selector)
                if el.count() > 0:
                    text = el.first.inner_text().strip()
                    if text:
                        description = text
                        print(f"✅ Description found via: {selector}")
                        break

            # =========================
            # MAIN IMAGE
            # =========================
            image_url = ""
            img_selectors = [
                'img[src*="ae01.alicdn.com"]',
                'img[src*="alicdn.com"]',
                '.image-view--previewImage--tnpEVgJ img',
            ]
            for selector in img_selectors:
                el = page.locator(selector)
                if el.count() > 0:
                    src = el.first.get_attribute("src") or ""
                    if src:
                        image_url = src
                        break

            browser.close()

            if not title:
                print("❌ Title empty after extraction")
                return None

            return {
                "title": title,
                "description": description,
                "image_url": image_url
            }

    except Exception as e:
        print("Scraping failed for URL:", url)
        print("Error:", e)
        return None
