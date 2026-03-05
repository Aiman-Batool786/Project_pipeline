from playwright.sync_api import sync_playwright
import time
import random


def scrape(url):

    try:

        with sync_playwright() as p:

            # Persistent browser (saves cookies)
            browser = p.chromium.launch_persistent_context(
                user_data_dir="browser_profile",
                headless=True,
                args=[
                    "--start-maximized",
                    "--disable-blink-features=AutomationControlled"
                ]
            )

            page = browser.new_page()

            print("Opening URL:", url)

            page.goto(url, timeout=120000)

            # wait like a human
            time.sleep(random.uniform(5, 8))

            print("Current URL:", page.url)

            # Detect redirect or blocked page
            if "/item/" not in page.url:
                print("Redirect detected or blocked")
                browser.close()
                return None

            # Human behavior
            page.mouse.move(random.randint(200, 400), random.randint(300, 500))
            page.mouse.wheel(0, random.randint(1500, 2500))
            time.sleep(random.uniform(3, 5))

            # -------------------
            # GET TITLE
            # -------------------

            title = ""

            title_selectors = [
                "h1[data-pl='product-title']",
                "h1[class*='product-title']",
                "h1"
            ]

            for selector in title_selectors:

                try:
                    if page.locator(selector).count() > 0:

                        title = page.locator(selector).first.inner_text().strip()

                        if title and len(title) > 5:
                            print("Title:", title)
                            break

                except:
                    pass

            if not title:
                print("Title not found")
                browser.close()
                return None

            # -------------------
            # SCROLL
            # -------------------

            page.mouse.wheel(0, random.randint(3000, 4000))
            time.sleep(random.uniform(3, 5))

            # -------------------
            # DESCRIPTION
            # -------------------

            description = ""

            desc_selectors = [
                "div#product-description",
                "div[class*='description--product-description']",
                "div[class*='detailmodule_text']",
                "div[id*='description']"
            ]

            for selector in desc_selectors:

                try:

                    if page.locator(selector).count() > 0:

                        text = page.locator(selector).first.inner_text().strip()

                        if text and len(text) > 20:

                            description = text[:1200]

                            print("Description found")

                            break

                except:
                    pass

            # -------------------
            # BULLET POINTS
            # -------------------

            bullet_points = []

            try:

                li_elements = page.locator("li").all()

                for li in li_elements:

                    text = li.inner_text().strip()

                    if len(text) > 10:
                        bullet_points.append(text)

                    if len(bullet_points) == 8:
                        break

                print("Bullets:", len(bullet_points))

            except:
                pass

            # -------------------
            # IMAGE
            # -------------------

            image_url = ""

            img_selectors = [
                "img[class*='magnifier']",
                "img[src*='alicdn']",
                "img"
            ]

            for selector in img_selectors:

                try:

                    if page.locator(selector).count() > 0:

                        src = page.locator(selector).first.get_attribute("src")

                        if src and len(src) > 20:

                            image_url = src

                            print("Image found")

                            break

                except:
                    pass

            browser.close()

            return {
                "title": title,
                "description": description,
                "bullet_points": bullet_points,
                "image_url": image_url
            }

    except Exception as e:

        print("Scraping failed:", e)

        return None


# -------------------
# MAIN FUNCTION
# -------------------

def get_product_info(url, max_retries=3):

    for attempt in range(max_retries):

        print(f"\n--- Attempt {attempt+1} of {max_retries} ---")

        result = scrape(url)

        if result:
            print("Scraping successful")
            return result

        print("Retrying...")

        time.sleep(random.uniform(5, 10))

    print("All attempts failed")

    return None
