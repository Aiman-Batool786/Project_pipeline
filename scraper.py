from playwright.sync_api import sync_playwright
import random

def get_product_info(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,  # ⚠️ Try headful first to debug; set True in production
                proxy={"server": "socks5://127.0.0.1:9050"},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process",
                ]
            )
            context = browser.new_context(
                # ✅ Updated to recent Chrome version
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                timezone_id="Asia/Karachi",
                # ✅ These help bypass bot detection
                java_script_enabled=True,
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                }
            )

            # ✅ Mask webdriver property
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            """)

            page = context.new_page()
            print("Opening URL:", url)

            page.goto(url, timeout=60000, wait_until="domcontentloaded")

            # ✅ Check if we got blocked before waiting for selectors
            page_title = page.title()
            print("Page title:", page_title)
            if "verify" in page_title.lower() or "captcha" in page_title.lower() or "robot" in page_title.lower():
                print("❌ CAPTCHA or verification page detected!")
                browser.close()
                return None

            # ✅ Human-like random delay
            page.wait_for_timeout(random.randint(2000, 4000))
            page.mouse.move(random.randint(200, 500), random.randint(300, 600))
            page.wait_for_timeout(random.randint(500, 1500))
            page.mouse.wheel(0, random.randint(300, 800))
            page.wait_for_timeout(random.randint(1000, 2000))

            #  Wait for title with longer timeout
            try:
                page.wait_for_selector('h1[data-pl="product-title"]', timeout=20000)
            except Exception:
                # Dump HTML to debug what page we actually got
                html_snippet = page.content()[:2000]
                print("⚠️ Title selector not found. Page snippet:")
                print(html_snippet)
                browser.close()
                return None

            # =========================
            # TITLE — correct selector
            # =========================
            title = ""
            title_el = page.locator('h1[data-pl="product-title"]')
            if title_el.count() > 0:
                title = title_el.first.inner_text().strip()

            # =========================
            # DESCRIPTION — fixed selector
            # The description <strong> tag lives inside the description
            # section div, not just any <strong> on the page
            # =========================
            description = ""
            # Try the specific description section first
            desc_selectors = [
                '#nav-description strong',           # <strong> inside description nav section
                '.product-description strong',        # class-based fallback
                '[id*="description"] strong',         # any id containing "description"
                'div[class*="description"] strong',   # any div with description in class
            ]
            for selector in desc_selectors:
                el = page.locator(selector)
                if el.count() > 0:
                    description = el.first.inner_text().strip()
                    print(f"Description found with selector: {selector}")
                    break

            # If still empty, scroll to description section and try again
            if not description:
                try:
                    page.locator('a[href="#nav-description"]').click()
                    page.wait_for_timeout(2000)
                    for selector in desc_selectors:
                        el = page.locator(selector)
                        if el.count() > 0:
                            description = el.first.inner_text().strip()
                            break
                except Exception:
                    pass

            # =========================
            # MAIN IMAGE — more specific
            # =========================
            image_url = ""
            img_selectors = [
                '.image-view--previewImage--tnpEVgJ img',   # AliExpress product image class
                '.slider-item img',
                '.product-image img',
                'img[src*="ae01.alicdn.com"]',              # AliExpress CDN images
            ]
            for selector in img_selectors:
                el = page.locator(selector)
                if el.count() > 0:
                    image_url = el.first.get_attribute("src") or ""
                    if image_url:
                        break

            browser.close()

            # =========================
            # BLOCK DETECTION
            # =========================
            if not title:
                print("No title found — likely blocked or wrong page")
                return None

            print("Title:", title)
            print("Description:", description[:100] if description else "Not found")

            return {
                "title": title,
                "description": description,
                "image_url": image_url
            }

    except Exception as e:
        print("Scraping failed for URL:", url)
        print("Error:", e)
        return None
