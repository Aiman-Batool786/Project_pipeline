from playwright.sync_api import sync_playwright
import re


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

            # FIX 1: Force English US region via cookies (stops Dutch/Italian redirects)
            context.add_cookies([
                {"name": "aep_usuc_f", "value": "site=glo&c_tp=USD&region=US&b_locale=en_US", "domain": ".aliexpress.com", "path": "/"},
                {"name": "intl_locale", "value": "en_US", "domain": ".aliexpress.com", "path": "/"},
            ])

            # Stealth JS tweaks (unchanged)
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
                timeout=120000,
                wait_until="domcontentloaded"
            )
            page.wait_for_timeout(5000)

            print("[scraper] Page loaded. Current URL:", page.url)
            print("[scraper] Page title tag:", page.title())

            # FIX 2: If Tor redirected to regional site, force back to English
            current_url = page.url
            if "www.aliexpress.com" not in current_url:
                match = re.search(r'item/(\d+)', current_url)
                if match:
                    english_url = f"https://www.aliexpress.com/item/{match.group(1)}.html"
                    print("[scraper] Regional redirect detected! Switching to:", english_url)
                    page.goto(english_url, timeout=120000, wait_until="domcontentloaded")
                    page.wait_for_timeout(5000)
                    print("[scraper] Now on:", page.url)

            # Simulate human behavior (unchanged)
            page.wait_for_timeout(3000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(3000)

            # DEBUG: Check body content (unchanged)
            try:
                body_text = page.locator("body").inner_text()
                print("[scraper] Body preview (first 500 chars):")
                print(body_text[:500])
                print("[scraper] ---")
            except Exception as body_err:
                print("[scraper] Could not read body:", body_err)

            # Extract title (unchanged)
            title = ""
            h1_count = page.locator("h1").count()
            print(f"[scraper] h1 elements found: {h1_count}")
            if h1_count > 0:
                title = page.locator("h1").first.inner_text()
                print("[scraper] Title extracted:", title[:100])

            # Extract description (unchanged)
            paragraphs = page.locator("p").all_text_contents()
            description = " ".join(paragraphs[:5]) if paragraphs else ""

            # Extract bullet points (unchanged)
            bullets = page.locator("li").all_text_contents()
            bullet_points = bullets[:5] if bullets else []

            # Extract first image (unchanged)
            image = ""
            if page.locator("img").count() > 0:
                image = page.locator("img").first.get_attribute("src")

            browser.close()

            # Detect block (unchanged)
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
