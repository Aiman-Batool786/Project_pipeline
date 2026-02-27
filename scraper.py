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

            # Force English US via cookies
            context.add_cookies([
                {"name": "aep_usuc_f", "value": "site=glo&c_tp=USD&region=US&b_locale=en_US", "domain": ".aliexpress.com", "path": "/"},
                {"name": "intl_locale", "value": "en_US", "domain": ".aliexpress.com", "path": "/"},
            ])

            # Mask webdriver
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            """)

            page = context.new_page()

            print("[scraper] Navigating to:", url)
            page.goto(url, timeout=90000, wait_until="domcontentloaded")
            page.wait_for_timeout(4000)

            # Fix regional redirect
            current_url = page.url
            if "www.aliexpress.com" not in current_url:
                match = re.search(r'item/(\d+)', current_url)
                if match:
                    english_url = f"https://www.aliexpress.com/item/{match.group(1)}.html"
                    print("[scraper] Redirect detected, switching to:", english_url)
                    page.goto(english_url, timeout=90000, wait_until="domcontentloaded")
                    page.wait_for_timeout(4000)

            print("[scraper] Current URL:", page.url)

            # Simulate human
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 1000)
            page.wait_for_timeout(2000)

            # Block detection
            body_text = page.locator("body").inner_text()
            if "Sign in / Register" in body_text and "Browse by Category" in body_text:
                print("[scraper] BLOCKED: homepage detected")
                browser.close()
                return None

            # ── TITLE ─────────────────────────────────────────────────────
            title = ""
            title_selectors = [
                "h1.product-title-text",
                "[class*='title--wrap'] h1",
                "[class*='product-title']",
                "h1",
            ]
            for sel in title_selectors:
                try:
                    el = page.locator(sel).first
                    if el.count() > 0:
                        text = el.inner_text(timeout=2000).strip()
                        if text and text.lower() not in ["aliexpress", "aliexpress.com"]:
                            title = text
                            print("[scraper] Title:", title[:80])
                            break
                except Exception:
                    continue

            if not title:
                print("[scraper] BLOCKED: no product title found")
                browser.close()
                return None

            # ── DESCRIPTION (from #nav-description section) ───────────────
            description = ""

            # Scroll to description section
            try:
                page.evaluate("document.querySelector('#nav-description')?.scrollIntoView()")
                page.wait_for_timeout(2000)
            except Exception:
                pass

            desc_selectors = [
                "#nav-description",
                "[class*='description--wrap--']",
                "[class*='description-content']",
                ".detail-desc-decorate-richtext",
                "[id*='description']",
            ]
            for sel in desc_selectors:
                try:
                    el = page.locator(sel).first
                    if el.count() > 0:
                        text = el.inner_text(timeout=3000).strip()
                        # Must be real description, not nav links
                        if len(text) > 50 and "Customer Reviews" not in text[:30]:
                            description = text[:2000]
                            print(f"[scraper] Description via {sel!r} ({len(description)} chars)")
                            break
                except Exception:
                    continue

            # Fallback: meaningful paragraphs
            if not description:
                try:
                    paragraphs = page.locator("p").all_text_contents()
                    meaningful = [p.strip() for p in paragraphs if len(p.strip()) > 40]
                    description = " ".join(meaningful[:5])[:2000]
                    print(f"[scraper] Description from <p> tags ({len(description)} chars)")
                except Exception:
                    pass

            # ── BULLET POINTS ─────────────────────────────────────────────
            bullet_points = []
            try:
                bullets = page.locator("li").all_text_contents()
                bullet_points = [b.strip() for b in bullets if len(b.strip()) > 20][:5]
            except Exception:
                pass

            # ── IMAGE ─────────────────────────────────────────────────────
            image = ""
            img_selectors = [
                ".magnifier-image",
                "[class*='product-image'] img",
                "img[src*='alicdn']",
                "img",
            ]
            for sel in img_selectors:
                try:
                    src = page.locator(sel).first.get_attribute("src", timeout=2000)
                    if src and src.startswith("http"):
                        image = src
                        break
                except Exception:
                    continue

            browser.close()

            print("[scraper] Done!")
            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image
            }

    except Exception as e:
        print(f"[scraper] EXCEPTION: {type(e).__name__}: {e}")
        return None
