from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import time
import random
import os


PROFILE_PATH = "/home/aimanbatool114/aliexpress_profile"


# ─────────────────────────────────────────
# TITLE EXTRACTION
# ─────────────────────────────────────────
def get_product_title(page):
    try:
        page.wait_for_selector("h1", timeout=60000)

        h1_tags = page.locator("h1").all()
        for h1 in h1_tags:
            text = h1.inner_text().strip()
            if text and len(text) > 20:
                print("✅ Product title found")
                return text

        return ""

    except Exception as e:
        print(f"❌ Title extraction error: {e}")
        return ""


# ─────────────────────────────────────────
# DESCRIPTION EXTRACTION
# ─────────────────────────────────────────
def get_product_description(page):
    print("📄 Extracting description...")

    # scroll to trigger lazy load
    for _ in range(3):
        page.mouse.wheel(0, 1500)
        page.wait_for_timeout(2000)

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
                    print("✅ Description found")
                    return text[:1500]
        except:
            continue

    # try iframe
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
                    return src
        return ""
    except:
        return ""


# ─────────────────────────────────────────
# MAIN SCRAPER
# ─────────────────────────────────────────
def scrape(url, attempt=1):

    print("\n" + "=" * 60)
    print(f"🔍 Attempt {attempt}: {url}")
    print("=" * 60)

    try:
        with sync_playwright() as p:

            # create profile folder if not exists
            os.makedirs(PROFILE_PATH, exist_ok=True)

            context = p.chromium.launch_persistent_context(
                user_data_dir=PROFILE_PATH,
                headless=False,  # IMPORTANT
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            )

            page = context.pages[0] if context.pages else context.new_page()

            # stealth patch
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
                window.chrome = { runtime: {} };
            """)

            # 🔥 IMPORTANT: warm up homepage first
            page.goto("https://www.aliexpress.com", wait_until="domcontentloaded")
            page.wait_for_timeout(10000)

            # open product
            page.goto(url, timeout=120000, wait_until="domcontentloaded")
            page.wait_for_timeout(8000)

            print("📄 Page title:", page.title())

            # if first time and captcha appears, allow manual solve
            if "captcha" in page.title().lower():
                input("⚠️ Solve CAPTCHA manually, then press ENTER...")

            title = get_product_title(page)

            if not title:
                print("❌ No title found. Possibly blocked.")
                context.close()
                return None

            description = get_product_description(page)
            bullet_points = get_bullet_points(page)
            image_url = get_product_image(page)

            context.close()

            print("✅ SCRAPE SUCCESS")

            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image_url
            }

    except PlaywrightTimeoutError:
        print("⏱️ Timeout occurred")
        return None

    except Exception as e:
        print(f"❌ Scrape error: {e}")
        return None


# ─────────────────────────────────────────
# RETRY WRAPPER
# ─────────────────────────────────────────
def get_product_info(url, max_retries=3):

    for attempt in range(max_retries):
        result = scrape(url, attempt + 1)
        if result:
            return result

        print("⏳ Waiting before retry...")
        time.sleep(random.randint(15, 30))

    print("❌ All attempts failed")
    return None
