from playwright.sync_api import sync_playwright

def get_product_info(url):
    # Clean the URL
    url = url.split("?gatewayAdapt")[0]
    url = url.replace("de.aliexpress.com", "www.aliexpress.com")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                # ✅ NO PROXY - test if the VM's own IP works
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                timezone_id="America/New_York",   # ✅ US timezone
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                }
            )

            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            """)

            page = context.new_page()
            print("Opening URL:", url)
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            page.wait_for_timeout(4000)

            print("Page title:", page.title())
            print("Current URL:", page.url)

            # Check if still redirected to non-English site
            if "de.aliexpress" in page.url or "gatewayAdapt" in page.url:
                print("❌ Still being redirected to regional site")
                browser.close()
                return None

            try:
                page.wait_for_selector('h1[data-pl="product-title"]', timeout=20000, state="visible")
            except Exception:
                print("❌ Title not found. Saving debug HTML...")
                with open("/tmp/debug_page.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                browser.close()
                return None

            title = page.locator('h1[data-pl="product-title"]').first.inner_text().strip()
            print("✅ Title:", title)

            description = ""
            for selector in ['#nav-description strong', 'div[class*="description"] strong']:
                el = page.locator(selector)
                if el.count() > 0:
                    text = el.first.inner_text().strip()
                    if text:
                        description = text
                        break

            image_url = ""
            for selector in ['img[src*="ae01.alicdn.com"]', 'img[src*="alicdn.com"]']:
                el = page.locator(selector)
                if el.count() > 0:
                    src = el.first.get_attribute("src") or ""
                    if src:
                        image_url = src
                        break

            browser.close()
            return {"title": title, "description": description, "image_url": image_url}

    except Exception as e:
        print("Scraping failed:", e)
        return None
```

---

## Decision Tree
```
Test without proxy
       │
       ├── ✅ Works → Your VM's IP is fine, Tor was the problem
       │              → Use VM's IP directly, no proxy needed
       │
       └── ❌ Still fails → VM IP is also flagged or redirecting
                           → You need a paid residential proxy
                              (Webshare.io has free tier - 10 proxies)
