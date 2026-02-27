from playwright.sync_api import sync_playwright


def get_product_info(url):

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

                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",

                viewport={"width": 1366, "height": 768},

                locale="en-US",

                timezone_id="Asia/Karachi"

            )

            page = context.new_page()

            print("Opening URL:", url)

            page.goto(

                url,

                timeout=60000,

                wait_until="domcontentloaded"

            )

            # simulate human behaviour
            page.wait_for_timeout(3000)

            page.mouse.move(200, 300)

            page.mouse.wheel(0, 2000)

            page.wait_for_timeout(3000)

im fetching data through scraper but scraper fetches wrong selectors i just want it fetch description from nav bar it go to nave bar see descritopn and fetch description content.
see i get this from developers tools exact ali express description.
 
 <div class="comet-v2-anchor navigation--wrap--dDqVSSY notranslate" data-spm-anchor-id="a2g0o.detail.0.i10.2f8cAlv9Alv9Yk"><div class="comet-v2-anchor-scroll-wrapper"><div class="comet-v2-anchor-scroller"><a href="#nav-review" class="comet-v2-anchor-link comet-v2-anchor-link-active" title="Customer Reviews" data-spm-anchor-id="a2g0o.detail.0.0"><span class="comet-icon comet-icon-locationfilled " style="color: var(--color-grey-10, rgba(25,25,25,1)); width: 14px; margin: 0px 4px 0px 0px;"><svg viewBox="0 0 1024 1024" width="1em" height="1em" fill="currentColor" aria-hidden="false" focusable="false"><path d="M712.533333 138.666667C646.4 85.333333 567.466667 64 512 64c-55.466667 0-134.4 21.333333-200.533333 74.666667C245.333333 194.133333 192 279.466667 192 403.2c0 100.266667 51.2 209.066667 106.666667 296.533333 57.6 89.6 123.733333 166.4 160 206.933334 27.733333 32 76.8 32 104.533333 0 36.266667-40.533333 102.4-117.333333 160-206.933334 55.466667-89.6 106.666667-196.266667 106.666667-296.533333 2.133333-123.733333-51.2-209.066667-117.333334-264.533333z m-200.533333 341.333333c-53.333333 0-96-42.666667-96-96s42.666667-96 96-96 96 42.666667 96 96-42.666667 96-96 96z"></path></svg></span>Customer Reviews<span style="display: inline-block; width: 14px; height: 8px;"></span></a><a href="#nav-specification" class="comet-v2-anchor-link" title="Specifications"><span style="display: inline-block; width: 14px; height: 8px;"></span>Specifications<span style="display: inline-block; width: 14px; height: 8px;"></span></a><a href="#nav-description" class="comet-v2-anchor-link" title="Description" data-spm-anchor-id="a2g0o.detail.0.0"><span style="display: inline-block; width: 14px; height: 8px;"></span>Description<span style="display: inline-block; width: 14px; height: 8px;"></span></a><a href="#nav-store" class="comet-v2-anchor-link" title="Store"><span style="display: inline-block; width: 14px; height: 8px;"></span>Store<span style="display: inline-block; width: 14px; height: 8px;"></span></a><a href="#nav-moretolove" class="comet-v2-anchor-link" title="More to love "><span style="display: inline-block; width: 14px; height: 8px;"></span>More to love <span style="display: inline-block; width: 14px; height: 8px;"></span></a></div></div></div>
 
title should be taken by from this section:
<h1 data-pl="product-title" data-tticheck="true" data-spm-anchor-id="a2g0o.detail.0.i8.19b2j2GXj2GXuD">1/2pcs Autumn Winter Men's High Neck T-shirt Slim Fit Fashion High Elastic Long Sleeve Casual Breathable Apparel Pullover</h1>
 
 
 
in my code i fetch description and title like this :
 
&nbsp;&nbsp;&nbsp;title = ""
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;if page.locator("h1").count() > 0:
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;title = page.locator("h1").first.inner_text()
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# description extraction
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;paragraphs = page.locator("p").all_text_contents()
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;description = " ".join(paragraphs[:5]) if paragraphs else ""
 
so now i want my description and ttile should be fateched correctlyThe issue with your current code is that you're using very generic selectors:

h1 works sometimes for the title, but AliExpress often uses just one main h1 with the product title, so your code is actually okay for the title in many cases (as shown in your example HTML).
For description, grabbing the first 5 <p> tags from the whole page is unreliable — the page has tons of <p> elements in headers, footers, reviews, specs, sidebars, etc. That's why you're getting wrong/waste content.

From the HTML snippet you shared (the navigation bar), the "Description" tab/link exists, but the actual content is not inside that navbar div — it's loaded in a separate section lower on the page, usually inside a container that becomes visible when you "click" the Description tab (or it's already there if the page loads with description active).
AliExpress product pages are heavily JavaScript-rendered and dynamic, so using Playwright is perfect because it waits for elements and can simulate scrolling/clicking if needed.
Recommended Fixes (Playwright + Python)
Pythonfrom playwright.sync_api import sync_playwright  # or async if you prefer

def extract_aliexpress_data(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # headless=False for debugging
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=60000)

        # Wait for main content to load (helps avoid empty results)
        page.wait_for_selector("h1", timeout=30000)

        # === TITLE ===
        title = ""
        # Your current approach is usually fine — there's normally only one meaningful h1
        if page.locator("h1").count() > 0:
            title = page.locator("h1").first.inner_text().strip()
        # More reliable fallback (using data attribute from your example)
        elif page.locator('h1[data-pl="product-title"]').count() > 0:
            title = page.locator('h1[data-pl="product-title"]').inner_text().strip()

        print("Title:", title)

        # === DESCRIPTION ===
        description = ""

        # Option 1: Most reliable in recent AliExpress layout — click the "Description" tab first
        try:
            # Find and click the Description tab/link in the anchor navigation
            description_tab = page.locator('a[href="#nav-description"], a[title="Description"]')
            if description_tab.count() > 0:
                description_tab.first.click(delay=300)  # small delay helps
                page.wait_for_timeout(1500)  # give time for content to swap/load

            # Now try to get the description content
            # Common modern selectors for the description block (2025–2026 layouts)
            desc_selectors = [
                'div#product-description',                     # sometimes used
                'div[class*="description--wrap"]',             # common class prefix
                'div[class*="detail"][class*="description"]',  # another frequent one
                'div[class*="comet-v2-container"] >> div.rich-text',  # rich text container
                'div[class*="product-detail"] >> div[class*="content"]',
                '#nav-description ~ div',                      # sibling after anchor
            ]

            for sel in desc_selectors:
                locator = page.locator(sel)
                if locator.count() > 0 and locator.inner_text().strip():
                    description = locator.inner_text().strip()
                    if len(description) > 100:  # avoid tiny fragments
                        break

            # If still empty → fallback: get all rich-text / prose content after specs/reviews
            if not description or len(description) < 200:
                description_parts = page.locator(
                    'div[class*="description"] p, '
                    'div[class*="detail"] p, '
                    'div.rich-text p, '
                    '[id="nav-description"] ~ * p'
                ).all_text_contents()
                description = " ".join([p.strip() for p in description_parts if p.strip()])[:2000]

        except Exception as e:
            print("Description tab/selector issue:", str(e))

        # Clean up description
        description = " ".join(description.split())  # remove extra spaces

        print("Description preview:", description[:300] + "..." if description else "(not found)")

        browser.close()

        return {"title": title, "description": description}


# Usage
url = "https://www.aliexpress.com/item/1005001234567890.html"  # ← replace with real url
result = extract_aliexpress_data(url)
Quick Improvements / Alternatives

Title — your h1 code is fine in 90% of cases. Add the data attribute fallback only if needed.
Description — never use plain p on the whole page. Always:
Click the Description tab first (most reliable way to force-load the right content)
Then use a scoped locator inside the detail/description container

If description is still wrong/empty:
Run in headless=False → watch what happens when the script clicks the tab
Open DevTools → search for words you see in the description → right-click → Copy selector
Update the desc_selectors list with what you find (class names change often on AliExpress)

Extra robustness:Pythonpage.wait_for_load_state("domcontentloaded")
page.evaluate("window.scrollTo(0, document.body.scrollHeight)")  # sometimes helps lazy-load
If you don't want to click (faster but less reliable):
Just use the selectors without .click() — sometimes description is already loaded.

Try this updated logic on a few products. AliExpress changes classes often, but the tab-click + rich-text / description container approach survives most updates.
Let me know what selectors you see in DevTools for the actual description block (the big text area) — I can refine it more.2.1sFastthis is my original code:
 
i just update 2 things so give me output of two things
 
from playwright.sync_api import sync_playwright
def get_product_info(url):
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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )
            page = context.new_page()
            print("Opening URL:", url)
            page.goto(
                url,
                timeout=60000,
                wait_until="domcontentloaded"
            )
            # simulate human behaviour
            page.wait_for_timeout(3000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(3000)
            # safer title extraction
            title = ""
            if page.locator("h1").count() > 0:
                title = page.locator("h1").first.inner_text()
            # description extraction
            paragraphs = page.locator("p").all_text_contents()
            description = " ".join(paragraphs[:5]) if paragraphs else ""
            # bullet points
            bullets = page.locator("li").all_text_contents()
            bullet_points = bullets[:5] if bullets else []
            # image
            image = ""
            if page.locator("img").count() > 0:
                image = page.locator("img").first.get_attribute("src")
            browser.close()
            if title == "":
                print("Login page detected or scraping blocked")
                return None
            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image
            }
    except Exception as e:
        print("Scraping failed for URL:", url)
        print("Error:", e)
        return NoneHere are the two main updates I recommend for your code to fetch the title and description much more reliably on current AliExpress product pages (as of 2026). The biggest problems in your original code are:

h1 usually works for title → keep it, but add a more specific fallback.
p from the whole page grabs random paragraphs (footer, reviews, specs sidebar, etc.) → this is why description is wrong.

Two Key Changes

Better title (small improvement, more robust)
Proper description (big fix: click the Description tab → then extract from the right container)

Updated code with only these two parts changed (rest stays exactly the same):
Pythonfrom playwright.sync_api import sync_playwright

def get_product_info(url):
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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )
            page = context.new_page()
            print("Opening URL:", url)
            page.goto(
                url,
                timeout=60000,
                wait_until="domcontentloaded"
            )
            # simulate human behaviour
            page.wait_for_timeout(3000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(3000)

            # === CHANGE 1: Safer & more specific title extraction ===
            title = ""
            # Prefer the data attribute when present (very common on AliExpress)
            title_locator = page.locator('h1[data-pl="product-title"], h1[data-tticheck="true"]')
            if title_locator.count() > 0:
                title = title_locator.first.inner_text().strip()
            # Fallback to any h1 if the above not found
            elif page.locator("h1").count() > 0:
                title = page.locator("h1").first.inner_text().strip()

            # === CHANGE 2: Proper description extraction ===
            description = ""

            try:
                # Step 1: Click the "Description" tab to make sure content is loaded/visible
                desc_tab = page.locator(
                    'a[href="#nav-description"], '
                    'a[title="Description"], '
                    'div.comet-v2-anchor a[href="#nav-description"]'
                )
                if desc_tab.count() > 0:
                    desc_tab.first.scroll_into_view_if_needed()
                    desc_tab.first.click(delay=200)   # small human-like delay
                    page.wait_for_timeout(1800)       # wait for tab content to appear

                # Step 2: Try modern/reliable description containers
                desc_candidates = [
                    'div#product-description',
                    'div[class*="description--content"]',
                    'div[class*="detail"][class*="description"]',
                    'div[class*="comet-v2-container"] >> div[class*="rich-text"]',
                    'div[class*="product-detail"] >> div[class*="content"]',
                    '#nav-description ~ div',                       # sibling after the anchor
                    'div[class*="description"]',                    # broad but last resort
                ]

                for selector in desc_candidates:
                    loc = page.locator(selector)
                    if loc.count() > 0:
                        text = loc.first.inner_text().strip()
                        if len(text) > 150:  # avoid tiny useless fragments
                            description = text
                            break

                # Fallback: collect paragraphs inside description area only
                if not description or len(description) < 300:
                    desc_paras = page.locator(
                        'div[class*="description"] p, '
                        'div[class*="detail"] p, '
                        '#nav-description ~ * p, '
                        'div.rich-text p'
                    ).all_text_contents()
                    cleaned = [p.strip() for p in desc_paras if p.strip() and len(p.strip()) > 10]
                    description = " ".join(cleaned[:12])  # limit to ~ first 10-12 paragraphs

                description = " ".join(description.split())  # normalize spaces

            except Exception as tab_err:
                print("Description tab/selector issue:", str(tab_err))

            # bullet points (unchanged, but you can improve later similarly)
            bullets = page.locator("li").all_text_contents()
            bullet_points = bullets[:5] if bullets else []

            # image (unchanged)
            image = ""
            if page.locator("img").count() > 0:
                image = page.locator("img").first.get_attribute("src")

            browser.close()

            if title == "":
                print("Login page detected or scraping blocked")
                return None

            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image
            }
    except Exception as e:
        print("Scraping failed for URL:", url)
        print("Error:", e)
        return None
Why These Changes Work Better

Title: Uses data-pl="product-title" (from your original HTML example) → survives when AliExpress uses multiple headings.
Description:
Clicks the Description tab → forces lazy-loaded content to appear (common on AliExpress).
Uses targeted selectors instead of global p.
Falls back to collecting <p>only near the description area.
Adds small waits and scroll → mimics human + helps dynamic loading.


Tips for Debugging

Run with headless=False once → watch if the tab click works.
After click, in DevTools → right-click the main description text block → Copy → Copy selector → add it to desc_candidates if none match.
AliExpress classes change → the tab link (a[href="#nav-description"]) and broad div[class*="description"] usually last longest.

Test on 3–5 different products. If description is still wrong on some, share the product URL + what you see in DevTools for the description block — I can give a more precise selector. Good luck!3.5sFastdont chnge my other code just update title and descriptionHere is your original code with only the title and description extraction parts updated — nothing else is changed (proxy, args, user-agent, mouse simulation, bullet points, image, timeouts, error handling, etc. all stay exactly the same).
Pythonfrom playwright.sync_api import sync_playwright

def get_product_info(url):
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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )
            page = context.new_page()
            print("Opening URL:", url)
            page.goto(
                url,
                timeout=60000,
                wait_until="domcontentloaded"
            )
            # simulate human behaviour
            page.wait_for_timeout(3000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(3000)

            # ────────────────────────────────────────────────
            # UPDATED TITLE EXTRACTION (more reliable)
            # ────────────────────────────────────────────────
            title = ""
            # First try the specific attribute from your example HTML
            title_loc = page.locator('h1[data-pl="product-title"], h1[data-tticheck="true"]')
            if title_loc.count() > 0:
                title = title_loc.first.inner_text().strip()
            # Fallback to first h1 (usually still correct)
            elif page.locator("h1").count() > 0:
                title = page.locator("h1").first.inner_text().strip()

            # ────────────────────────────────────────────────
            # UPDATED DESCRIPTION EXTRACTION (click tab + better target)
            # ────────────────────────────────────────────────
            description = ""

            try:
                # Try to click the Description tab to load/activate the content
                desc_tab = page.locator(
                    'a[href="#nav-description"], '
                    'a[title="Description"], '
                    '.comet-v2-anchor a[href="#nav-description"]'
                )
                if desc_tab.count() > 0:
                    desc_tab.first.scroll_into_view_if_needed(timeout=10000)
                    desc_tab.first.click(delay=250)
                    page.wait_for_timeout(1800)  # give time for content to appear

                # Try to find the description container
                desc_loc = page.locator(
                    'div[class*="description--content"], '
                    'div[class*="product-description"], '
                    'div[class*="detail"][class*="description"], '
                    '#nav-description ~ div, '
                    'div[class*="description"]'
                )

                if desc_loc.count() > 0:
                    description = desc_loc.first.inner_text().strip()
                
                # If still weak/empty → collect paragraphs only from description area
                if not description or len(description) < 200:
                    desc_paragraphs = page.locator(
                        'div[class*="description"] p, '
                        'div[class*="detail"] p, '
                        '#nav-description ~ * p'
                    ).all_text_contents()
                    
                    cleaned = [p.strip() for p in desc_paragraphs if p.strip() and len(p.strip()) > 15]
                    description = " ".join(cleaned[:15])  # take up to ~15 meaningful paragraphs

                description = " ".join(description.split())  # clean extra spaces

            except Exception as e:
                print("Description extraction had minor issue:", str(e))
                
                # keep going, don't fail the whole function

            # bullet points
            bullets = page.locator("li").all_text_contents()

            bullet_points = bullets[:5] if bullets else []


            # image
            image = ""

            if page.locator("img").count() > 0:

                image = page.locator("img").first.get_attribute("src")


            browser.close()


            if title == "":
                print("Login page detected or scraping blocked")
                return None


            return {

                "title": title,

                "description": description,

                "bullet_points": bullet_points,

                "image_url": image

            }


    except Exception as e:

        print("Scraping failed for URL:", url)

        print("Error:", e)

        return None
