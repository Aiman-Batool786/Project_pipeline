from playwright.sync_api import sync_playwright
import re


def extract_description(page):
    """
    Extract the real product description from AliExpress.

    Correct container:
    div.detailmodule_text
    """

    print("[scraper] Extracting description from div.detailmodule_text...")

    try:
        page.wait_for_selector("div.detailmodule_text", timeout=10000)

        desc_block = page.locator("div.detailmodule_text").first

        if desc_block.count() == 0:
            print("[scraper] Description container not found")
            return ""

        html = desc_block.inner_html()

        # Convert <br> to newline
        html = re.sub(r"<br\s*/?>", "\n", html)

        # Remove remaining HTML tags
        text = re.sub(r"<.*?>", "", html)

        # Clean whitespace
        text = re.sub(r"\n+", "\n", text).strip()

        if "Smarter Shopping, Better Living" in text:
            return ""

        print(f"[scraper] Description length: {len(text)} characters")

        return text

    except Exception as e:
        print("[scraper] Description extraction failed:", e)
        return ""


def extract_images(page):
    """Extract product images"""

    images = {}

    try:
        scripts = page.locator("script").all()

        for script in scripts:
            content = script.text_content()

            match = re.search(r'"imagePathList":\[(.*?)\]', content)

            if match:
                urls = re.findall(r'"(https://[^"]+\.jpg[^"]*)"', match.group(1))

                for i, img in enumerate(urls[:20], start=1):
                    key = f"image_{i}"
                    images[key] = re.sub(r'_\d+x\d+', '', img)

                break

    except Exception as e:
        print("[scraper] Image extraction error:", e)

    return images


def extract_meta(page):
    """Extract title from meta"""

    data = {}

    try:
        title = page.locator('meta[property="og:title"]').get_attribute("content")
        data["title"] = title or ""
    except:
        data["title"] = ""

    return data


def extract_price(page):
    """Extract price from JS"""

    price = ""
    shipping = ""

    try:
        scripts = page.locator("script").all()

        for script in scripts:
            content = script.text_content()

            price_match = re.search(r'"price":"([^"]+)"', content)
            ship_match = re.search(r'"shipmentWay":"([^"]+)"', content)

            if price_match:
                price = price_match.group(1)

            if ship_match:
                shipping = ship_match.group(1)

    except:
        pass

    return price, shipping


def get_product_info(url):

    try:
        with sync_playwright() as p:

            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ]
            )

            context = browser.new_context(
                user_agent="Mozilla/5.0",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )

            page = context.new_page()

            print("[scraper] Opening:", url)

            page.goto(url, timeout=60000, wait_until="domcontentloaded")

            page.wait_for_timeout(4000)

            data = {}

            # TITLE
            data.update(extract_meta(page))

            # DESCRIPTION
            description = extract_description(page)
            data["description"] = description

            # IMAGES
            images = extract_images(page)
            data.update(images)

            # PRICE
            price, shipping = extract_price(page)

            data["price"] = price
            data["shipping"] = shipping

            browser.close()

            return data

    except Exception as e:
        print("Scraper error:", e)
        return None


if __name__ == "__main__":

    url = "https://www.aliexpress.com/item/1005009666264769.html"

    result = get_product_info(url)

    print(result)
