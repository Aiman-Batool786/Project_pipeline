from playwright.sync_api import sync_playwright
import time

def scrape(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            )
            
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768}
            )
            
            # Stealth scripts
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
                Object.defineProperty(navigator, 'permissions', {get: () => ({query: () => ({state: 'granted'})})});
                window.chrome = {runtime:{}};
                Object.defineProperty(document, 'webdriver', {get: () => undefined});
            """)
            
            page = context.new_page()
            print(f"Opening: {url}")
            
            page.goto(url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(5000)
            
            print(f"Current URL: {page.url}")
            
            # Check if on product page
            if "/item/" not in page.url:
                print("Not on product page")
                browser.close()
                return None
            
            # Scroll to simulate human
            page.mouse.move(300, 400)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(3000)
            
            # Get title
            title = ""
            try:
                title = page.locator("h1[data-pl='product-title']").first.inner_text().strip()
            except:
                try:
                    title = page.locator("h1").first.inner_text().strip()
                except:
                    pass
            
            print(f"Title: {title}")
            
            # Blocked page check
            if not title or title.lower() in ["aliexpress", "just a moment"]:
                print("Blocked or no title found")
                browser.close()
                return None
            
            # Scroll for description
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(2000)
            
            # Get description
            description = ""
            selectors = [
                "div#product-description",
                "div[class*='description--product-description']",
                "div[class*='detailmodule_text']",
                "div[id*='description']"
            ]
            
            for sel in selectors:
                try:
                    el = page.locator(sel).first
                    if el:
                        text = el.inner_text().strip()
                        if text and len(text) > 20:
                            description = text[:1000]
                            break
                except:
                    pass
            
            print(f"Description length: {len(description)}")
            
            # Get bullet points
            bullet_points = []
            try:
                li_elements = page.locator("li").all()
                bullet_points = [li.inner_text().strip() for li in li_elements if len(li.inner_text().strip()) > 10][:8]
            except:
                pass
            
            print(f"Bullets: {len(bullet_points)}")
            
            # Get image
            image = ""
            try:
                img = page.locator("img").first
                image = img.get_attribute("src")
            except:
                pass
            
            print(f"Image: {image[:50] if image else 'None'}")
            
            browser.close()
            
            if not title:
                return None
            
            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image
            }
    
    except Exception as e:
        print(f"Scraping error: {e}")
        return None


def get_product_info(url, max_retries=3):
    for attempt in range(max_retries):
        print(f"\n--- Attempt {attempt + 1} of {max_retries} ---")
        result = scrape(url)
        if result:
            return result
        if attempt < max_retries - 1:
            print("Retrying...")
            time.sleep(10)
    
    print("All attempts failed")
    return None
