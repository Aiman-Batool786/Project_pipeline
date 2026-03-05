from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

def scrape(url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            
            # Apply stealth
            stealth_sync(context)
            
            page = context.new_page()
            page.goto(url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(5000)
            
            # Get title
            title = ""
            try:
                title = page.locator("h1[data-pl='product-title']").first.inner_text().strip()
            except:
                pass
            
            # Get description
            description = ""
            try:
                description = page.locator("div#product-description").first.inner_text().strip()[:1000]
            except:
                pass
            
            # Get bullets
            bullets = []
            try:
                li_elements = page.locator("li").all()
                bullets = [li.inner_text().strip() for li in li_elements[:8] if len(li.inner_text().strip()) > 10]
            except:
                pass
            
            # Get image
            image = ""
            try:
                image = page.locator("img").first.get_attribute("src")
            except:
                pass
            
            browser.close()
            
            if not title:
                return None
            
            return {
                "title": title,
                "description": description,
                "bullet_points": bullets,
                "image_url": image
            }
    except Exception as e:
        print(f"Error: {e}")
        return None

def get_product_info(url, max_retries=3):
    for attempt in range(max_retries):
        print(f"Attempt {attempt + 1}/{max_retries}")
        result = scrape(url)
        if result:
            return result
        time.sleep(10)
    return None
