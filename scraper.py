from playwright.sync_api import sync_playwright
import re


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

BLOCKED_DESCRIPTIONS = [
    "smarter shopping, better living",
    "aliexpress.com",
    "just a moment",
    "attention required",
    "access denied",
    "captcha",
    "enable javascript",
    "please turn javascript on",
]


def _is_blocked_text(text: str) -> bool:
    low = text.lower().strip()
    return any(b in low for b in BLOCKED_DESCRIPTIONS) or len(low) < 20


def _html_to_text(html: str) -> str:
    """Convert inner HTML to clean readable text."""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(lines).strip()


# ─────────────────────────────────────────
# DESCRIPTION EXTRACTION
# ─────────────────────────────────────────

def _extract_description_from_frame(frame) -> str:
    """Extract description from div.detailmodule_text inside a frame/page."""
    selectors = [
        "div.detailmodule_text",
        "div[class*='detailmodule_text']",
        "div[class*='detail-desc-decorate-richtext']",
        "div[class*='description--product-description']",
        "div#product-description",
        "div[id*='description']",
    ]

    for sel in selectors:
        try:
            loc = frame.locator(sel)
            if loc.count() > 0:
                inner_html = loc.first.inner_html()
                text = _html_to_text(inner_html)
                if text and not _is_blocked_text(text):
                    print(f"[scraper] Description found via selector: {sel}")
                    return text
        except Exception:
            continue

    return ""


def _extract_description(page) -> str:
    """
    1. Try to find a description iframe and switch context.
    2. If no iframe, extract directly from main page.
    """
    iframe_selectors = [
        "iframe[id*='desc']",
        "iframe[src*='description']",
        "iframe[id*='description']",
        "iframe[class*='description']",
    ]

    for iframe_sel in iframe_selectors:
        try:
            iframe_loc = page.locator(iframe_sel)
            if iframe_loc.count() > 0:
                print(f"[scraper] Description iframe detected: {iframe_sel}")
                frame = iframe_loc.first.content_frame()
                if frame:
                    try:
                        frame.wait_for_load_state("domcontentloaded", timeout=10000)
                    except Exception:
                        pass
                    text = _extract_description_from_frame(frame)
                    if text:
                        return text
        except Exception:
            continue

    print("[scraper] No description iframe found, scraping main page")
    return _extract_description_from_frame(page)


# ─────────────────────────────────────────
# MAIN SCRAPER
# ─────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    try:
        with sync_playwright() as p:

            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )

            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi",
            )

            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {} };
            """)

            page = context.new_page()

            print(f"[scraper] Opening URL: {url}")
            page.goto(url, timeout=90000, wait_until="domcontentloaded")

            page.wait_for_timeout(4000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(3000)

            # ── TITLE ──────────────────────────────────────────
            title = ""
            for sel in [
                "h1[data-pl='product-title']",
                "h1[class*='product-title']",
                "h1",
            ]:
                if page.locator(sel).count() > 0:
                    t = page.locator(sel).first.inner_text().strip()
                    if t:
                        title = t
                        break

            if not title or _is_blocked_text(title):
                print("[scraper] Blocked page or empty title — aborting")
                browser.close()
                return None

            # ── PRICE ──────────────────────────────────────────
            price = ""
            for sel in [
                "div[class*='price--currentPriceText']",
                "span[class*='price--current']",
                "div[class*='product-price-value']",
                "span[class*='uniform-banner-box-price']",
            ]:
                if page.locator(sel).count() > 0:
                    p_text = page.locator(sel).first.inner_text().strip()
                    if p_text:
                        price = p_text
                        break

            # ── IMAGES ─────────────────────────────────────────
            images = []
            for sel in [
                "img[class*='magnifier--image']",
                "img[class*='product-image']",
                "img[class*='main-image']",
            ]:
                locs = page.locator(sel)
                if locs.count() > 0:
                    for i in range(min(locs.count(), 6)):
                        src = locs.nth(i).get_attribute("src") or ""
                        if src and src.startswith("http"):
                            images.append(src)
                    if images:
                        break

            if not images and page.locator("img").count() > 0:
                src = page.locator("img").first.get_attribute("src") or ""
                if src:
                    images.append(src)

            # ── DESCRIPTION ────────────────────────────────────
            # Scroll down so lazy-loaded description content renders
            page.mouse.wheel(0, 4000)
            page.wait_for_timeout(3000)

            description = _extract_description(page)

            if not description:
                print("[scraper] WARNING: Description could not be extracted")

            browser.close()

            return {
                "title": title,
                "description": description,
                "price": price,
                "image_url": images[0] if images else "",
                "extra_images": images[1:] if len(images) > 1 else [],
            }

    except Exception as e:
        print(f"[scraper] Scraping failed for URL: {url}")
        print(f"[scraper] Error: {e}")
        return None
