from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import random
import time

# ─── Config ───────────────────────────────────────────────────────────────────

MAX_RETRIES = 3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _random_delay(min_ms=1500, max_ms=4000):
    """Random human-like delay."""
    time.sleep(random.randint(min_ms, max_ms) / 1000)


def _simulate_human(page):
    """Simulate mouse movement and scrolling."""
    page.mouse.move(random.randint(100, 400), random.randint(100, 400))
    page.mouse.wheel(0, random.randint(400, 800))
    _random_delay(1000, 2500)
    page.mouse.wheel(0, random.randint(400, 800))
    _random_delay(1000, 2000)


def _extract_title(page) -> str:
    """Try multiple selectors for the product title."""
    selectors = [
        "h1.product-title-text",        # AliExpress-specific
        "[class*='product-title']",
        "[data-pl='product-title']",
        "h1",
    ]
    for sel in selectors:
        try:
            locator = page.locator(sel).first
            if locator.count() > 0:
                text = locator.inner_text(timeout=3000).strip()
                if text:
                    print(f"  [title] Found via selector: {sel!r}")
                    return text
        except Exception:
            continue
    return ""


def _extract_description(page) -> str:
    """Try AliExpress-specific description selectors before falling back to <p>."""
    selectors = [
        "[class*='product-description']",
        "[class*='description-content']",
        "[id*='description']",
        ".detail-desc-decorate-richtext",
    ]
    for sel in selectors:
        try:
            locator = page.locator(sel).first
            if locator.count() > 0:
                text = locator.inner_text(timeout=3000).strip()
                if len(text) > 30:
                    print(f"  [description] Found via selector: {sel!r}")
                    return text[:1500]
        except Exception:
            continue

    # Fallback: grab meaningful <p> tags (skip nav/footer noise)
    try:
        paragraphs = page.locator("p").all_text_contents()
        meaningful = [p.strip() for p in paragraphs if len(p.strip()) > 40]
        if meaningful:
            return " ".join(meaningful[:5])[:1500]
    except Exception:
        pass

    return ""


def _extract_image(page) -> str:
    """Get the main product image."""
    selectors = [
        ".magnifier-image",
        "[class*='product-image'] img",
        ".slider-item img",
        "img[src*='alicdn']",
    ]
    for sel in selectors:
        try:
            src = page.locator(sel).first.get_attribute("src", timeout=2000)
            if src and src.startswith("http"):
                return src
        except Exception:
            continue
    return ""

# ─── Main ─────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    """
    Scrape product info from AliExpress with anti-bot measures.
    Retries up to MAX_RETRIES times on failure.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n[scraper] Attempt {attempt}/{MAX_RETRIES} → {url}")
        result = _scrape_once(url)
        if result:
            return result
        if attempt < MAX_RETRIES:
            wait = random.randint(3, 7)
            print(f"[scraper] Retrying in {wait}s...")
            time.sleep(wait)

    print("[scraper] All attempts failed.")
    return None


def _scrape_once(url: str) -> dict | None:
    try:
        with sync_playwright() as p:

            browser = p.chromium.launch(
                headless=False,          # Less detectable than headless=True
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-infobars",
                    "--window-size=1366,768",
                ]
            )

            context = browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="America/New_York",   # More neutral than Asia/Karachi
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Referer": "https://www.google.com/",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "cross-site",
                    "Upgrade-Insecure-Requests": "1",
                }
            )

            # Mask navigator.webdriver
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            """)

            page = context.new_page()

            print("[scraper] Navigating to URL...")
            page.goto(url, timeout=60000, wait_until="networkidle")  # wait for JS to settle

            _simulate_human(page)

            # ── Extract fields ──────────────────────────────────────────────
            title = _extract_title(page)

            if not title:
                # Likely hit a CAPTCHA or login wall
                page_text = page.locator("body").inner_text()[:300]
                print(f"[scraper] No title found. Page snippet:\n{page_text}")
                browser.close()
                return None

            description = _extract_description(page)
            image = _extract_image(page)

            # Bullet points from <li> — filter short/nav items
            try:
                raw_bullets = page.locator("li").all_text_contents()
                bullet_points = [b.strip() for b in raw_bullets if len(b.strip()) > 20][:7]
            except Exception:
                bullet_points = []

            browser.close()

            print(f"[scraper] Scraped: {title[:60]}...")
            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image,
            }

    except PlaywrightTimeoutError as e:
        print(f"[scraper] Timeout: {e}")
        return None
    except Exception as e:
        print(f"[scraper] Error: {type(e).__name__}: {e}")
        return None
