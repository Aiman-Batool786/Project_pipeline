from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from stem import Signal
from stem.control import Controller
import time
import random


# ─────────────────────────────────────────
# TOR IP ROTATION
# ─────────────────────────────────────────
def renew_tor_ip():
    try:
        with Controller.from_port(port=9051) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
            time.sleep(8)
            print("✅ New Tor IP acquired")
            return True
    except Exception as e:
        print(f"❌ Tor IP rotation failed: {e}")
        return False


# ─────────────────────────────────────────
# TITLE EXTRACTION (Stable Method)
# ─────────────────────────────────────────
def get_product_title(page):
    try:
        page.wait_for_selector("h1", timeout=60000)

        h1_tags = page.locator("h1").all()
        for h1 in h1_tags:
            text = h1.inner_text().strip()
            if text and len(text) > 25:
                print(f"✅ Product title found")
                return text

        print("❌ No valid title found")
        return ""

    except Exception as e:
        print(f"❌ Title extraction error: {e}")
        return ""


# ─────────────────────────────────────────
# DESCRIPTION EXTRACTION (Iframe + Main DOM)
# ─────────────────────────────────────────
def get_product_description(page):
    print("📄 Extracting description...")

    # Scroll slowly to trigger lazy loading
    for _ in range(3):
        page.mouse.wheel(0, 1500)
        page.wait_for_timeout(2000)

    # 1️⃣ Try main DOM
    selectors = [
        "div[class*='description']",
        "div[class*='detail-desc']",
        "section[class*='description']",
        "div[id*='description']"
    ]

    for selector in selectors:
        try:
            elements = page.locator(selector).all()
            for el in elements:
                text = el.inner_text().strip()
                if text and len(text) > 100:
                    print("✅ Description found in main DOM")
                    return text[:1500]
        except:
            continue

    # 2️⃣ Try iframe (AliExpress often uses iframe)
    for frame in page.frames:
        try:
            body = frame.locator("body")
            if body.count() > 0:
                text = body.inner_text()
                if text and len(text) > 200:
                    print("✅ Description found in iframe")
                    return text[:1500]
        except:
            continue

    print("⚠️ Description not found")
    return ""


# ─────────────────────────────────────────
# BULLET POINTS
# ─────────────────────────────────────────
def get_bullet_points(page):
    bullets = []
    try:
        li_elements = page.locator("li").all()
        for li in li_elements:
            text = li.inner_text().strip()
            if text and 10 < len(text) < 200:
                if text not in bullets:
                    bullets.append(text)
        print(f"✅ {len(bullets[:5])} bullet points found")
        return bullets[:5]
    except:
        return []


# ─────────────────────────────────────────
# IMAGE EXTRACTION
# ─────────────────────────────────────────
def get_product_image(page):
    try:
        images = page.locator("img").all()
        for img in images:
            src = img.get_attribute("src")
            if src and "http" in src and not src.startswith("data:"):
                if "ae01" in src or "alidfs" in src:
                    print("✅ Product image found")
                    return src
        return ""
    except:
        return ""


# ─────────────────────────────────────────
# MAIN SCRAPE FUNCTION
# ─────────────────────────────────────────
def scrape(url, attempt=1):
    print(f"\n{'='*60}")
    print(f"🔍 Attempt {attempt}: {url}")
    print(f"{'='*60}")

    browser = None

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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )

            # Stealth patch
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
                window.chrome = { runtime: {} };
            """)

            page = context.new_page()

            # Better navigation strategy
            page.goto(url, timeout=120000, wait_until="commit")

            # Wait for real product content
            page.wait_for_selector("h1", timeout=60000)

            page.wait_for_timeout(5000)

            # Simulate light human behavior
            page.mouse.move(random.randint(100, 800), random.randint(100, 500))
            page.wait_for_timeout(2000)

            print(f"📄 Page title: {page.title()}")

            title = get_product_title(page)

            if not title:
                print("❌ No title found. Likely blocked.")
                browser.close()
                return None

            description = get_product_description(page)
            bullet_points = get_bullet_points(page)
            image_url = get_product_image(page)

            browser.close()

            print("✅ SCRAPE SUCCESS")

            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image_url
            }

    except PlaywrightTimeoutError:
        print("⏱️ Timeout occurred")
        if browser:
            browser.close()
        return None

    except Exception as e:
        print(f"❌ Scrape error: {e}")
        if browser:
            browser.close()
        return None


# ─────────────────────────────────────────
# RETRY WRAPPER
# ─────────────────────────────────────────
def get_product_info(url, max_retries=5):

    for attempt in range(max_retries):

        result = scrape(url, attempt + 1)

        if result:
            return result

        print("🔄 Rotating Tor IP and retrying...")
        renew_tor_ip()
        time.sleep(random.randint(10, 20))

    print("❌ All attempts failed")
    return None
