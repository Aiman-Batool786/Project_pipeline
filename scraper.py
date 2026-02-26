from playwright.sync_api import sync_playwright
import random
import time

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

def get_product_info(url):
    try:
        with sync_playwright() as p:

            browser_args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-gpu",
                "--window-size=1366,768",
                "--disable-notifications",
                "--disable-popup-blocking",
            ]

            # Launch with Tor proxy
            try:
                browser = p.chromium.launch(
                    headless=True,
                    proxy={"server": "socks5://127.0.0.1:9050"},
                    args=browser_args
                )
                print("[INFO] Browser launched with Tor proxy")
            except Exception as proxy_err:
                print(f"[WARN] Tor failed ({proxy_err}), launching without proxy")
                browser = p.chromium.launch(
                    headless=True,
                    args=browser_args
                )

            context = browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi",
                java_script_enabled=True,
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                }
            )

            # Mask webdriver detection
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
                window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){}, app: {} };
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
                );
            """)

            page = context.new_page()

            # Block unnecessary resources to speed up loading
            def block_resources(route):
                if route.request.resource_type in ["font", "media"]:
                    route.abort()
                else:
                    route.continue_()

            page.route("**/*", block_resources)

            print(f"[INFO] Navigating to: {url}")

            # Use domcontentloaded — AliExpress never reaches networkidle
            try:
                page.goto(url, timeout=60000, wait_until="domcontentloaded")
            except Exception as nav_err:
                print(f"[WARN] Navigation timeout (continuing): {nav_err}")

            # Let JS render
            page.wait_for_timeout(5000)

            # Human-like scrolling
            page.mouse.wheel(0, 1000)
            page.wait_for_timeout(1500)
            page.mouse.wheel(0, 1500)
            page.wait_for_timeout(1500)
            page.mouse.wheel(0, 1500)
            page.wait_for_timeout(2000)

            # Save screenshot for debugging
            try:
                page.screenshot(path="debug.png", full_page=False)
                print("[INFO] Screenshot saved: debug.png")
            except Exception as ss_err:
                print(f"[WARN] Screenshot failed: {ss_err}")

            current_url = page.url.lower()
            page_content = page.content().lower()
            print(f"[INFO] Current URL: {page.url}")

            # Detect blocks
            if "captcha" in current_url or "captcha" in page_content:
                print("[ERROR] CAPTCHA detected")
                browser.close()
                return None

            if "robot" in page_content or "are you a human" in page_content:
                print("[ERROR] Bot detection triggered")
                browser.close()
                return None

            if "403" in page.title() or "access denied" in page_content:
                print("[ERROR] Access denied / 403")
                browser.close()
                return None

            # ── TITLE ──
            title = ""
            title_selectors = [
                'h1[data-pl="product-title"]',
                'h1.product-title-text',
                'h1[class*="title"]',
                '.product-title h1',
                '[class*="product-title"]',
                '[class*="ProductTitle"]',
                'h1',
            ]
            for selector in title_selectors:
                try:
                    el = page.locator(selector).first
                    if el.count() > 0:
                        text = el.inner_text(timeout=3000).strip()
                        if text and len(text) > 5:
                            title = text
                            print(f"[INFO] Title found with: {selector}")
                            break
                except Exception:
                    continue

            # ── IMAGE URL ──
            image = ""
            image_selectors = [
                'img[src*="alicdn"]',
                '.image-view--preview-item--3FBSmPh img',
                '.slider-item img',
                '.product-image img',
                '[class*="gallery"] img',
                '[class*="image"] img',
                'img[alt*="product"]',
            ]
            for selector in image_selectors:
                try:
                    el = page.locator(selector).first
                    if el.count() > 0:
                        src = el.get_attribute("src", timeout=3000)
                        if src:
                            if src.startswith("//"):
                                src = "https:" + src
                            if src.startswith("http"):
                                image = src
                                print(f"[INFO] Image found with: {selector}")
                                break
                except Exception:
                    continue

            # ── DESCRIPTION ──
            description = ""

            # Try description iframe first (AliExpress puts desc in iframe)
            try:
                page.wait_for_timeout(2000)
                for frame in page.frames:
                    frame_url = frame.url.lower()
                    if "description" in frame_url or "desc" in frame_url:
                        try:
                            desc_text = frame.locator("body").inner_text(timeout=5000)
                            if desc_text and desc_text.strip():
                                description = desc_text.strip()
                                print("[INFO] Description found in iframe")
                                break
                        except Exception:
                            pass
            except Exception as frame_err:
                print(f"[WARN] Frame search error: {frame_err}")

            # Fallback: look for description div on main page
            if not description:
                desc_selectors = [
                    '[id="product-description"]',
                    '[class*="product-description"]',
                    '[class*="description-content"]',
                    '[class*="ProductDescription"]',
                    '[data-pl="product-description"]',
                ]
                for selector in desc_selectors:
                    try:
                        el = page.locator(selector).first
                        if el.count() > 0:
                            desc_text = el.inner_text(timeout=3000).strip()
                            if desc_text:
                                description = desc_text
                                print(f"[INFO] Description found with: {selector}")
                                break
                    except Exception:
                        continue

            # ── BULLET POINTS ──
            bullet_points = []
            bullet_selectors = [
                '.product-prop-list li',
                '.specification-list li',
                '[class*="spec"] li',
                '[class*="property"] li',
                '[class*="feature"] li',
                'ul li',
            ]
            for selector in bullet_selectors:
                try:
                    items = page.locator(selector).all_text_contents()
                    cleaned = [
                        b.strip() for b in items
                        if b.strip() and len(b.strip()) > 3 and len(b.strip()) < 300
                    ]
                    if len(cleaned) >= 2:
                        bullet_points = cleaned[:5]
                        print(f"[INFO] Bullets found with: {selector}")
                        break
                except Exception:
                    continue

            browser.close()

            print(f"[RESULT] title='{title[:60]}' | image={'YES' if image else 'NO'} | desc_len={len(description)} | bullets={len(bullet_points)}")

            if not title:
                print("[ERROR] Title empty — page likely blocked or DOM changed")
                return None

            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image
            }

    except Exception as e:
        print(f"[SCRAPER ERROR] {e}")
        return None
