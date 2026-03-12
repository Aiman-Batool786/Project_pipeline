"""
scraper.py
──────────
AliExpress product scraper using Playwright.

Key fixes vs old version:
  1. Strong stealth script (navigator.webdriver, plugins, chrome object)
  2. Newer Chrome UA (124 not 115 — 115 is flagged)
  3. Waits for networkidle before extracting — ensures JS renders
  4. Correct title selectors (h1[data-pl] first)
  5. Description extracted ONLY from div.detailmodule_text
  6. Iframe detection for description
  7. Blocklist filter rejects "Smarter Shopping" and similar noise
  8. Returns price + extra_images in addition to original fields
"""

import re
import time
import random

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

BLOCKED_SIGNALS = [
    "smarter shopping",
    "better living",
    "aliexpress.com",
    "just a moment",
    "attention required",
    "access denied",
    "captcha",
    "enable javascript",
    "robot",
    "cf-browser-verification",
]

STEALTH_SCRIPT = """
// Hide webdriver flag
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

// Fake plugins
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const arr = [
            { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
            { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
            { name: 'Native Client', filename: 'internal-nacl-plugin' },
        ];
        arr.__proto__ = PluginArray.prototype;
        return arr;
    }
});

// Fake languages
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });

// Fake chrome object
window.chrome = {
    app: { isInstalled: false },
    runtime: {
        onConnect: null,
        onMessage: null
    }
};

// Fake permissions
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) =>
    parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : originalQuery(parameters);

// Remove headless traces
Object.defineProperty(navigator, 'maxTouchPoints', { get: () => 1 });
Object.defineProperty(screen, 'colorDepth', { get: () => 24 });
"""


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def _is_blocked(text: str) -> bool:
    low = (text or "").lower().strip()
    if len(low) < 10:
        return True
    return any(sig in low for sig in BLOCKED_SIGNALS)


def _html_to_text(html: str) -> str:
    """Convert inner HTML to clean readable text."""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    lines = [l.strip() for l in text.splitlines()]
    return "\n".join(l for l in lines if l).strip()


def _extract_description_from_context(context_obj) -> str:
    """
    Try all known description selectors on a page or frame object.
    Returns clean text or empty string.
    """
    selectors = [
        "div.detailmodule_text",
        "div[class*='detailmodule_text']",
        "div[class*='detail-desc-decorate-richtext']",
        "div[class*='description--product-description']",
        "div#product-description",
        "div[id*='description']",
        "div[class*='product-description']",
    ]
    for sel in selectors:
        try:
            loc = context_obj.locator(sel)
            if loc.count() > 0:
                inner = loc.first.inner_html()
                text = _html_to_text(inner)
                if text and not _is_blocked(text) and len(text) > 30:
                    print(f"[scraper] Description found: {sel} ({len(text)} chars)")
                    return text
        except Exception:
            continue
    return ""


def _extract_description(page) -> str:
    """Check iframes first, then main page."""
    iframe_selectors = [
        "iframe[id*='desc']",
        "iframe[src*='description']",
        "iframe[id*='description']",
        "iframe[class*='description']",
        "iframe[src*='desc']",
    ]
    for sel in iframe_selectors:
        try:
            iframe_loc = page.locator(sel)
            if iframe_loc.count() > 0:
                print(f"[scraper] Iframe detected: {sel}")
                frame = iframe_loc.first.content_frame()
                if frame:
                    try:
                        frame.wait_for_load_state("domcontentloaded", timeout=8000)
                    except Exception:
                        pass
                    text = _extract_description_from_context(frame)
                    if text:
                        return text
        except Exception:
            continue

    return _extract_description_from_context(page)


# ─────────────────────────────────────────
# CORE SCRAPE FUNCTION
# ─────────────────────────────────────────

def _scrape_once(url: str) -> dict | None:
    with sync_playwright() as p:

        ua = random.choice(USER_AGENTS)

        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-infobars",
                "--window-size=1366,768",
                "--disable-extensions",
                "--disable-setuid-sandbox",
                "--ignore-certificate-errors",
                "--disable-web-security",
            ],
        )

        context = browser.new_context(
            user_agent=ua,
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Karachi",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
            },
        )

        context.add_init_script(STEALTH_SCRIPT)

        page = context.new_page()

        # Block ads/tracking to speed up load
        page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}",
                   lambda r: r.abort() if any(x in r.request.url for x in
                       ["ads", "track", "analytics", "pixel", "beacon"]) else r.continue_())

        print(f"[scraper] Opening: {url}")

        try:
            page.goto(url, timeout=90000, wait_until="domcontentloaded")
        except PWTimeout:
            print("[scraper] Page load timeout — trying networkidle fallback")
            try:
                page.goto(url, timeout=60000, wait_until="commit")
            except Exception as e:
                print(f"[scraper] Navigation failed: {e}")
                browser.close()
                return None

        # Human-like delays + scroll
        page.wait_for_timeout(random.randint(3000, 5000))
        page.mouse.move(random.randint(100, 400), random.randint(200, 500))
        page.mouse.wheel(0, random.randint(1500, 2500))
        page.wait_for_timeout(random.randint(2000, 3000))

        # ── Check if blocked ────────────────────────────────
        page_title = page.title()
        if _is_blocked(page_title):
            print(f"[scraper] Blocked — page title: '{page_title}'")
            browser.close()
            return None

        # ── TITLE ────────────────────────────────────────────
        title = ""
        title_selectors = [
            "h1[data-pl='product-title']",
            "h1[class*='product-title']",
            "h1[class*='title--wrap']",
            "h1",
        ]
        for sel in title_selectors:
            try:
                loc = page.locator(sel)
                if loc.count() > 0:
                    t = loc.first.inner_text().strip()
                    if t and not _is_blocked(t):
                        title = t
                        print(f"[scraper] Title ({sel}): {title[:60]}")
                        break
            except Exception:
                continue

        if not title:
            print("[scraper] No title found — page may be blocked")
            # Debug: show what's actually on the page
            try:
                body_text = page.locator("body").inner_text()[:200]
                print(f"[scraper] Page body preview: {body_text}")
            except Exception:
                pass
            browser.close()
            return None

        # ── PRICE ────────────────────────────────────────────
        price = ""
        for sel in [
            "div[class*='price--currentPriceText']",
            "span[class*='price--current']",
            "div[class*='product-price-value']",
            "span[class*='uniform-banner-box-price']",
            "div[class*='price']",
        ]:
            try:
                loc = page.locator(sel)
                if loc.count() > 0:
                    p_text = loc.first.inner_text().strip()
                    if p_text and any(c.isdigit() for c in p_text):
                        price = p_text
                        break
            except Exception:
                continue

        # ── IMAGES ───────────────────────────────────────────
        images = []
        img_selectors = [
            "img[class*='magnifier--image']",
            "img[class*='product-image']",
            "img[class*='main-image']",
            "div[class*='image-viewer'] img",
            "div[class*='slider'] img",
        ]
        for sel in img_selectors:
            try:
                locs = page.locator(sel)
                count = locs.count()
                if count > 0:
                    for i in range(min(count, 6)):
                        src = locs.nth(i).get_attribute("src") or ""
                        if src.startswith("http") and src not in images:
                            images.append(src)
                    if images:
                        break
            except Exception:
                continue

        # Fallback: first img
        if not images:
            try:
                src = page.locator("img").first.get_attribute("src") or ""
                if src.startswith("http"):
                    images.append(src)
            except Exception:
                pass

        # ── DESCRIPTION ──────────────────────────────────────
        # Scroll deep so lazy content renders
        page.mouse.wheel(0, random.randint(3000, 5000))
        page.wait_for_timeout(random.randint(2500, 4000))

        description = _extract_description(page)
        if not description:
            print("[scraper] WARNING: Description not found")

        # ── BRAND ─────────────────────────────────────────────
        brand = ""
        for sel in [
            "a[class*='store-name']",
            "span[class*='store-name']",
            "div[class*='store-header'] a",
        ]:
            try:
                loc = page.locator(sel)
                if loc.count() > 0:
                    b = loc.first.inner_text().strip()
                    if b:
                        brand = b
                        break
            except Exception:
                continue

        browser.close()

        return {
            "title": title,
            "description": description,
            "price": price,
            "brand": brand,
            "image_url": images[0] if images else "",
            "extra_images": images[1:] if len(images) > 1 else [],
            # Also store as image_1..6 for db.py compatibility
            "image_1": images[0] if len(images) > 0 else "",
            "image_2": images[1] if len(images) > 1 else "",
            "image_3": images[2] if len(images) > 2 else "",
            "image_4": images[3] if len(images) > 3 else "",
            "image_5": images[4] if len(images) > 4 else "",
            "image_6": images[5] if len(images) > 5 else "",
            "bullet_points": [],
            "color": "",
            "dimensions": "",
            "weight": "",
            "material": "",
            "certifications": "",
            "country_of_origin": "",
            "warranty": "",
            "shipping": "",
            "product_type": "",
            "store_name": brand,
        }


# ─────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────

def get_product_info(url: str, max_retries: int = 3) -> dict | None:
    """
    Scrape an AliExpress product page.
    Retries up to max_retries times with increasing delays.

    Returns dict with product data or None if all attempts fail.
    """
    for attempt in range(1, max_retries + 1):
        print(f"\n[scraper] Attempt {attempt}/{max_retries}: {url}")
        try:
            result = _scrape_once(url)
            if result:
                print(f"[scraper] ✅ Success on attempt {attempt}")
                return result
        except Exception as e:
            print(f"[scraper] Attempt {attempt} exception: {e}")

        if attempt < max_retries:
            wait = attempt * random.randint(4, 8)
            print(f"[scraper] Waiting {wait}s before retry...")
            time.sleep(wait)

    print(f"[scraper] ❌ All {max_retries} attempts failed for: {url}")
    return None
