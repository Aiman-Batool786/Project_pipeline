from playwright.sync_api import sync_playwright
from stem import Signal
from stem.control import Controller
import time


# ─────────────────────────────────────────────
# 🔁 Rotate Tor IP
# ─────────────────────────────────────────────
def renew_tor_ip():
    try:
        with Controller.from_port(port=9051) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
            time.sleep(5)
            print("✅ Got new Tor IP")
    except Exception as e:
        print("❌ Could not rotate IP:", e)


# ─────────────────────────────────────────────
# 🔎 Scraper Function
# ─────────────────────────────────────────────
def scrape(url):
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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi",
                extra_http_headers={
                    "accept-language": "en-US,en;q=0.9",
                }
            )

            # Anti-detection tweaks
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {} };
            """)

            page = context.new_page()

            print("🌐 Opening URL:", url)
            page.goto(url, timeout=90000, wait_until="networkidle")

            page.wait_for_timeout(5000)

            print("📌 Current URL:", page.url)
            print("📌 Page Title:", page.title())

            # Save screenshot for debugging
            page.screenshot(path="debug.png", full_page=True)

            # 🚨 Detect login / captcha
            if "login" in page.url.lower() or "captcha" in page.content().lower():
                print("🚨 Login/Captcha page detected!")
                browser.close()
                return None

            # ─────────────────────────────
            # 🏷️ TITLE
            # ─────────────────────────────
            title = ""
            try:
                title = page.locator("h1").first.inner_text(timeout=10000).strip()
            except:
                pass

            # ─────────────────────────────
            # 📝 DESCRIPTION
            # ─────────────────────────────
            description = ""

            # Scroll to trigger lazy load
            page.mouse.wheel(0, 4000)
            page.wait_for_timeout(3000)

            # Try normal page first
            desc_selectors = [
                "div[class*='description']",
                "div[class*='detailmodule']",
                "div[id*='description']",
            ]

            for selector in desc_selectors:
                try:
                    el = page.locator(selector)
                    if el.count() > 0:
                        text = el.first.inner_text().strip()
                        if text and len(text) > 20:
                            description = text[:800]
                            print(f"✅ Description found via {selector}")
                            break
                except:
                    continue

            # If not found → check iframes
            if not description:
                for frame in page.frames:
                    if "description" in frame.url:
                        try:
                            description = frame.locator("body").inner_text()[:800]
                            print("✅ Description found inside iframe")
                            break
                        except:
                            continue

            # ─────────────────────────────
            # 📌 BULLET POINTS
            # ─────────────────────────────
            bullet_points = []
            try:
                bullets = page.locator("ul li").all_text_contents()
                bullet_points = [b.strip() for b in bullets if len(b.strip()) > 10][:5]
            except:
                pass

            # ─────────────────────────────
            # 🖼️ IMAGE
            # ─────────────────────────────
            image = ""
            try:
                img = page.locator("img").first
                image = img.get_attribute("src")
            except:
                pass

            browser.close()

            if not title:
                print("❌ No title found. Likely blocked.")
                return None

            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image
            }

    except Exception as e:
        print("❌ Scraping failed for URL:", url)
        print("Error:", e)
        return None


# ─────────────────────────────────────────────
# 🔁 Retry Logic
# ─────────────────────────────────────────────
def get_product_info(url, max_retries=3):
    for attempt in range(max_retries):
        print(f"\n--- Attempt {attempt + 1} of {max_retries} ---")

        result = scrape(url)

        if result:
            return result

        if attempt < max_retries - 1:
            print("🔁 Blocked! Rotating Tor IP and retrying...")
            renew_tor_ip()

    print("❌ All attempts failed for URL:", url)
    return None
