from playwright.sync_api import sync_playwright
import re
import json


def extract_description_from_nav_section(page):
    """
    Extract description ONLY from the correct AliExpress container

    HTML structure:
    <h2 class="title--title--O6xcB1q">Description</h2>

    <div class="detailmodule_text">
        <p>.... actual description ....</p>
    </div>
    """

    description = ""

    print("[scraper] 📝 Extracting description from detailmodule_text...")

    try:
        page.wait_for_selector("div.detailmodule_text", timeout=10000)

        container = page.locator("div.detailmodule_text").first

        html = container.inner_html()

        html = html.replace("<br>", "\n").replace("<br/>", "\n")

        text = re.sub("<.*?>", "", html)

        text = re.sub(r"\n+", "\n", text)
        text = re.sub(r"[ \t]+", " ", text)

        description = text.strip()

        if "Smarter Shopping, Better Living!" in description:
            description = ""

        print(f"[scraper]    ✅ Description extracted ({len(description)} chars)")

    except Exception as e:
        print("[scraper] ⚠️ Description extraction failed:", e)

    return description


def extract_all_images(page):

    images = {}

    print("[scraper] 🖼️ Extracting images...")

    try:

        scripts = page.locator("script").all()

        for script in scripts:

            script_text = script.text_content()

            image_match = re.search(r'"imagePathList":\s*\[(.*?)\]', script_text)

            if image_match:

                images_str = image_match.group(1)

                images_list = re.findall(r'"(https://[^"]+\.jpg[^"]*)"', images_str)

                for idx, img in enumerate(images_list[:20], 1):

                    key = f"image_{idx}"

                    clean_url = re.sub(r'_\d+x\d+', '', img)

                    images[key] = clean_url

                if images:
                    return images

    except:
        pass

    return images


def extract_from_meta_tags(page):

    data = {}

    try:

        title_meta = page.locator('meta[property="og:title"]').get_attribute("content")

        data["title"] = title_meta or ""

    except:

        data["title"] = ""

    return data


def extract_from_javascript(page):

    data = {}

    try:

        scripts = page.locator("script").all()

        for script in scripts:

            script_text = script.text_content()

            price_match = re.search(r'"price":\s*"([^"]+)"', script_text)

            if price_match:
                data["price"] = price_match.group(1)

            ship_match = re.search(r'"shipmentWay":\s*"([^"]+)"', script_text)

            if ship_match:
                data["shipping"] = ship_match.group(1)

    except:
        pass

    return data


def extract_from_dom(page):

    data = {}

    page.mouse.wheel(0, 3000)

    page.wait_for_timeout(2000)

    description = extract_description_from_nav_section(page)

    if description:
        data["description"] = description

    images_dict = extract_all_images(page)

    data.update(images_dict)

    return data


def get_product_info(url):

    try:

        with sync_playwright() as p:

            browser = p.chromium.launch(headless=True)

            context = browser.new_context(
                user_agent="Mozilla/5.0",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi",
            )

            page = context.new_page()

            print("Opening:", url)

            page.goto(url, timeout=60000)

            page.wait_for_timeout(3000)

            data = extract_from_meta_tags(page)

            js_data = extract_from_javascript(page)

            data.update(js_data)

            dom_data = extract_from_dom(page)

            data.update(dom_data)

            browser.close()

            return data

    except Exception as e:

        print("Scraper error:", e)

        return None
