"""
scraper.py
──────────
Playwright-based scraper for AliExpress product pages.

Extracts:
  • title (meta og:title)
  • price + shipping (JavaScript)
  • description (multiple fallback selectors)
  • specifications (nav-specification section)
  • images (imagePathList JSON → og:image fallback)

All spec keys are normalised to lowercase before the
mapping_rules lookup so casing differences don't matter.
"""

from playwright.sync_api import sync_playwright
import re
import json


# ─────────────────────────────────────────────────────────────────────────────
# SPECIFICATION EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

# Maps template field name → list of spec labels to match (lowercase substring)
SPEC_MAPPING_RULES = {
    'brand':             ['brand name', 'brand', 'marque'],
    'color':             ['main color', 'color', 'colour', 'couleur'],
    'dimensions':        ['dimensions', 'size', 'taille', 'dimensions (cm)'],
    'weight':            ['weight', 'poids', 'net weight', 'weight (kg)'],
    'material':          ['material', 'matière', 'materials', 'material composition'],
    'certifications':    ['certification', 'certifications', 'normes', 'standards'],
    'country_of_origin': ['country of origin', 'origin', 'country', 'pays'],
    'warranty':          ['warranty', 'garantie', 'guarantee'],
    'product_type':      ['product type', 'type', 'type de produit', 'product category'],
    'age_from':          ['age from', 'recommended age from', 'age (from)'],
    'age_to':            ['age to', 'recommended age to', 'age (to)'],
    'gender':            ['gender', 'suitable for', 'sexe'],
}


def extract_specifications(page) -> dict:
    """Extract key-value pairs from the nav-specification section."""
    specs = {}
    print("[scraper] 📋 Extracting specifications...")

    try:
        items = page.locator(
            '[id*="nav-specification"] [class*="specification--prop"]'
        ).all()

        if items:
            print(f"[scraper]    Found {len(items)} specification items")

        for item in items:
            try:
                title_el = item.locator('[class*="specification--title"] span').first
                desc_el  = item.locator('[class*="specification--desc"] span').first
                title = title_el.inner_text().strip() if title_el.count() > 0 else ""
                desc  = desc_el.inner_text().strip()  if desc_el.count() > 0  else ""
                if title and desc:
                    specs[title.lower()] = desc
                    print(f"[scraper]      {title}: {desc[:60]}")
            except Exception:
                pass

        if specs:
            print(f"[scraper]    ✅ {len(specs)} specifications extracted")

    except Exception as e:
        print(f"[scraper]    ⚠️  Spec extraction error: {e}")

    return specs


def map_specifications_to_fields(specs: dict) -> dict:
    """
    Map raw spec dict (lowercase keys) to template field names
    using SPEC_MAPPING_RULES.
    """
    mapped = {}

    for template_field, keywords in SPEC_MAPPING_RULES.items():
        for spec_key, spec_value in specs.items():
            if any(kw in spec_key for kw in keywords):
                if template_field not in mapped:
                    mapped[template_field] = spec_value
                break

    return mapped


# ─────────────────────────────────────────────────────────────────────────────
# DESCRIPTION EXTRACTION  (multiple fallback selectors)
# ─────────────────────────────────────────────────────────────────────────────

def extract_description_correct(page) -> str:
    """Try several selectors in order; return the first useful result."""
    print("[scraper] 📝 Extracting description...")

    # 1. richTextContainer
    try:
        el = page.locator('div.richTextContainer[data-rich-text-render="true"]')
        if el.count() > 0:
            text = el.first.inner_text().strip()
            if len(text) > 50:
                print(f"[scraper]    ✅ richTextContainer ({len(text)} chars)")
                return text
    except Exception:
        pass

    # 2. product-description
    try:
        el = page.locator('div[id="product-description"]')
        if el.count() > 0:
            text = el.first.inner_text().strip()
            if len(text) > 50:
                print(f"[scraper]    ✅ product-description ({len(text)} chars)")
                return text
    except Exception:
        pass

    # 3. nav-description
    try:
        el = page.locator('div[id="nav-description"]')
        if el.count() > 0:
            text = el.first.inner_text().strip()
            text = re.sub(r'^.*?Description\s+report\s+', '', text,
                          flags=re.IGNORECASE | re.DOTALL)
            if len(text) > 50 and "Smarter Shopping" not in text:
                print(f"[scraper]    ✅ nav-description ({len(text)} chars)")
                return text
    except Exception:
        pass

    # 4. detailmodule_text
    try:
        modules = page.locator('div.detailmodule_text')
        for i in range(modules.count()):
            try:
                text = modules.nth(i).inner_text().strip()
                if len(text) > 50:
                    print(f"[scraper]    ✅ detailmodule_text ({len(text)} chars)")
                    return text
            except Exception:
                pass
    except Exception:
        pass

    # 5. detail-desc-decorate-content
    try:
        paras = page.locator('p.detail-desc-decorate-content')
        parts = []
        for i in range(paras.count()):
            try:
                t = paras.nth(i).inner_text().strip()
                if t:
                    parts.append(t)
            except Exception:
                pass
        if parts:
            text = " | ".join(parts)
            print(f"[scraper]    ✅ detail-desc-decorate-content ({len(text)} chars)")
            return text
    except Exception:
        pass

    print("[scraper]    ⚠️  No description found")
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# IMAGE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_all_images(page) -> dict:
    """Extract up to 20 product images from JS or meta fallback."""
    images = {}
    print("[scraper] 🖼️  Extracting images...")

    # Primary: imagePathList in page scripts
    try:
        for script in page.locator("script").all():
            try:
                txt = script.text_content() or ""
                m   = re.search(r'"imagePathList":\s*\[(.*?)\]', txt)
                if m:
                    urls = re.findall(r'"(https://[^"]+\.jpg[^"]*)"', m.group(1))
                    for idx, url in enumerate(urls[:20], 1):
                        images[f"image_{idx}"] = re.sub(r'_\d+x\d+', '', url)
                    if images:
                        print(f"[scraper]    ✅ {len(images)} images from imagePathList")
                        return images
            except Exception:
                pass
    except Exception:
        pass

    # Fallback: og:image meta tag
    try:
        og = page.locator('meta[property="og:image"]').get_attribute('content')
        if og:
            images['image_1'] = og
            print("[scraper]    ✅ 1 image from og:image meta tag")
    except Exception:
        pass

    return images


# ─────────────────────────────────────────────────────────────────────────────
# META / JS EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_meta_tags(page) -> dict:
    data = {}
    try:
        data["title"] = page.locator(
            'meta[property="og:title"]'
        ).get_attribute("content") or ""
    except Exception:
        data["title"] = ""
    return data


def extract_from_javascript(page) -> dict:
    data = {}
    try:
        for script in page.locator("script").all():
            try:
                txt = script.text_content() or ""
                m   = re.search(r'"price":\s*"([^"]+)"', txt)
                if m:
                    data["price"] = m.group(1)
                m = re.search(r'"shipmentWay":\s*"([^"]+)"', txt)
                if m:
                    data["shipping"] = m.group(1)
            except Exception:
                pass
    except Exception:
        pass
    return data


# ─────────────────────────────────────────────────────────────────────────────
# DOM EXTRACTION  (scroll + specs + description + images)
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_dom(page) -> dict:
    data = {}

    # Scroll to trigger lazy-loaded content
    page.mouse.wheel(0, 3000)
    page.wait_for_timeout(2000)

    # Specifications
    raw_specs = extract_specifications(page)
    data.update(map_specifications_to_fields(raw_specs))

    # Description
    desc = extract_description_correct(page)
    if desc:
        data["description"] = desc

    # Images
    data.update(extract_all_images(page))

    return data


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def get_product_info(url: str) -> dict | None:
    """
    Launch a headless Chromium browser, navigate to url, and extract
    all available product data.  Returns a flat dict or None on failure.
    """
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
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Karachi"
            )

            page = context.new_page()

            print(f"\n[scraper] 🌐 Opening: {url}")
            page.goto(url, timeout=60000, wait_until="domcontentloaded")

            page.wait_for_timeout(3000)
            page.mouse.move(200, 300)
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(2000)

            print("[scraper] 🔍 Extracting data...")

            data = extract_from_meta_tags(page)
            data.update(extract_from_javascript(page))
            data.update(extract_from_dom(page))

            browser.close()

        # ── Validate ──────────────────────────────────────────────────────────
        if not data.get('title'):
            print("[scraper] ❌ No title extracted — aborting")
            return None

        print(f"\n[scraper] ✅ Extraction complete")
        print(f"[scraper]    Title      : {data.get('title', '')[:60]}…")
        print(f"[scraper]    Description: {len(data.get('description', ''))} chars")

        spec_count = len([k for k in [
            'brand', 'color', 'dimensions', 'weight', 'material',
            'certifications', 'country_of_origin', 'warranty', 'product_type'
        ] if data.get(k)])
        print(f"[scraper]    Spec fields: {spec_count}")
        print(f"[scraper]    Images     : "
              f"{sum(1 for i in range(1, 21) if data.get(f'image_{i}'))}")

        # ── Apply defaults ────────────────────────────────────────────────────
        defaults = {
            'description': '', 'brand': '', 'color': '', 'dimensions': '',
            'weight': '', 'material': '', 'certifications': '',
            'country_of_origin': '', 'warranty': '', 'product_type': '',
            'shipping': '', 'price': '', 'rating': '', 'reviews': '',
            'bullet_points': [], 'age_from': '', 'age_to': '',
            'gender': '', 'safety_warning': '',
        }
        for key, default in defaults.items():
            if key not in data or not data[key]:
                data[key] = default

        for i in range(1, 21):
            data.setdefault(f'image_{i}', "")

        return data

    except Exception as e:
        print(f"[scraper] ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_url = "https://www.aliexpress.com/item/1005006156465150.html"
    result   = get_product_info(test_url)

    if result:
        print("\n" + "=" * 80)
        print("=== SCRAPED DATA ===")
        print("=" * 80)
        print(f"Title       : {result['title']}")
        print(f"Brand       : {result.get('brand', 'N/A')}")
        print(f"Color       : {result.get('color', 'N/A')}")
        print(f"Dimensions  : {result.get('dimensions', 'N/A')}")
        print(f"Description : {result['description'][:200]}…")
    else:
        print("Scraping failed.")
