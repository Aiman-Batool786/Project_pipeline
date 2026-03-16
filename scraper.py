"""
scraper.py
──────────
Playwright-based scraper for AliExpress product pages.

ROOT CAUSE FIX:
  The spec section (id="nav-specification") is far down the page and only
  renders after the viewport reaches it. The old code scrolled to 3000px
  which was not enough. The fix is to:
    1. Scroll incrementally to the bottom
    2. Use page.evaluate() to scroll the spec section into view
    3. Wait for spec items to be visible before extracting

HTML structure confirmed from real page:
  <div id="nav-specification">
    <ul class="specification--list--...">
      <li class="specification--line--...">
        <div class="specification--prop--...">         ← each spec item
          <div class="specification--title--..."><span>Brand Name</span></div>
          <div class="specification--desc--..." title="XMSJ"><span>XMSJ</span></div>
        </div>
        <div class="specification--prop--...">         ← two per line
          ...
        </div>
      </li>
    </ul>
  </div>
"""

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import re
import json


# ─────────────────────────────────────────────────────────────────────────────
# SPECIFICATION MAPPING RULES
# ─────────────────────────────────────────────────────────────────────────────

SPEC_MAPPING_RULES = {
    'brand': [
        'brand name', 'brand', 'marque', 'manufacturer brand'
    ],
    'color': [
        'main color', 'color', 'colour', 'couleur', 'item color'
    ],
    'dimensions': [
        'dimensions (l x w x h',
        'dimensions (cm)',
        'dimensions',
        'size',
        'taille',
        'product size',
        'item size',
        'package size',
        'product dimensions',
    ],
    'weight': [
        'net weight',
        'weight (kg)',
        'weight',
        'poids',
        'gross weight',
        'item weight',
        'package weight',
    ],
    'material': [
        'material composition',
        'material',
        'matière',
        'materials',
        'body material',
        'housing material',
    ],
    'certifications': [
        'certification',
        'certifications',
        'normes',
        'standards',
        'energy efficiency rating',
        'energy consumption grade',
        'energy rating',
        'energy class',
    ],
    'country_of_origin': [
        'place of origin',
        'country of origin',
        'origin',
        'country',
        'pays',
        'made in',
        'manufactured in',
    ],
    'warranty': [
        'warranty',
        'garantie',
        'guarantee',
        'warranty period',
    ],
    'product_type': [
        'refrigeration type',
        'cooling method',
        'defrost type',
        'product type',
        'type de produit',
        'product category',
        'item type',
        'type',
        'application',
        'use',
        'design',
        'operation system',
        'os',
    ],
    'age_from': ['age from', 'recommended age from', 'age (from)', 'minimum age'],
    'age_to':   ['age to',   'recommended age to',   'age (to)',   'maximum age'],
    'gender':   ['gender', 'suitable for', 'sexe', 'for whom'],

    # Extra fields
    'capacity':         ['capacity', 'fridge capacity', 'net capacity', 'total capacity', 'volume'],
    'freezer_capacity': ['freezer capacity'],
    'voltage':          ['voltage', 'rated voltage', 'operating voltage'],
    'model_number':     ['model number', 'model no', 'model', 'item model', 'numéro de modèle'],
    'power_source':     ['power source', 'power supply'],
    'installation':     ['installation', 'mounting type'],
    'style':            ['style'],
    'features':         ['feature', 'features', 'key features', 'highlights'],

    # Phone-specific
    'battery':          ['battery capacity', 'battery capacity(mah)', 'battery capacity range'],
    'display':          ['display size', 'screen size', 'display resolution', 'screen material', 'screen type'],
    'camera':           ['rear camera pixel', 'front camera pixel', 'camera'],
    'connectivity':     ['cellular', 'wifi', 'nfc', 'bluetooth'],
    'memory':           ['storage', 'memory', 'rom', 'ram'],
    'os':               ['operation system', 'os', 'android version'],
}


def _build_dimensions_from_hwl(specs: dict) -> str:
    height = width = depth = ""
    for key, val in specs.items():
        k = key.lower()
        if 'height' in k and not height:
            height = val
        elif 'width' in k and not width:
            width = val
        elif ('depth' in k or 'length' in k) and not depth:
            depth = val
    parts = [v for v in [height, width, depth] if v]
    return " x ".join(parts) if len(parts) >= 2 else ""


# ─────────────────────────────────────────────────────────────────────────────
# CORE FIX: scroll spec section into view and wait for it
# ─────────────────────────────────────────────────────────────────────────────

def scroll_to_specifications(page) -> bool:
    """
    Scroll the page so the #nav-specification section enters the viewport
    and its contents are rendered.
    Returns True if the section was found and scrolled to.
    """
    print("[scraper]    📜 Scrolling to specification section...")

    # Step 1: Incremental scroll to bottom to trigger lazy-load
    for scroll_y in [1000, 2000, 3000, 4000, 5000, 6000, 8000, 10000]:
        page.mouse.wheel(0, scroll_y)
        page.wait_for_timeout(300)

    page.wait_for_timeout(1500)

    # Step 2: Use JS to scroll #nav-specification into view
    try:
        page.evaluate("""
            const el = document.getElementById('nav-specification');
            if (el) {
                el.scrollIntoView({ behavior: 'instant', block: 'start' });
            }
        """)
        page.wait_for_timeout(2000)
        print("[scraper]    ✅ Scrolled to #nav-specification")
    except Exception as e:
        print(f"[scraper]    ⚠️  JS scroll failed: {e}")

    # Step 3: Wait for spec items to appear (up to 8 seconds)
    try:
        page.wait_for_selector(
            '#nav-specification [class*="specification--prop"]',
            timeout=8000,
            state="visible"
        )
        print("[scraper]    ✅ Spec items are visible")
        return True
    except PWTimeout:
        print("[scraper]    ⚠️  Spec items did not appear within timeout")
        return False
    except Exception as e:
        print(f"[scraper]    ⚠️  Waiting for specs failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# SPECIFICATION EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_specifications(page) -> dict:
    """
    Extract all key-value pairs from the nav-specification section.
    Returns raw dict with lowercased keys.
    """
    specs = {}
    print("[scraper] 📋 Extracting specifications...")

    # First scroll to the section
    scroll_to_specifications(page)

    # Primary selector — confirmed from real HTML
    try:
        items = page.locator(
            '#nav-specification [class*="specification--prop"]'
        ).all()

        if not items:
            # Fallback without id scope
            items = page.locator('[class*="specification--prop"]').all()

        print(f"[scraper]    Found {len(items)} specification items")

        for item in items:
            try:
                # Title: <div class="specification--title--..."><span>Label</span></div>
                title_el = item.locator('[class*="specification--title"] span').first
                title = title_el.inner_text().strip() if title_el.count() > 0 else ""

                # Desc: <div class="specification--desc--..." title="Value"><span>Value</span></div>
                # Try span text first, fall back to title attribute
                desc_el = item.locator('[class*="specification--desc"]').first
                if desc_el.count() > 0:
                    # Prefer title attribute (more reliable, no formatting)
                    desc = desc_el.get_attribute('title') or ""
                    if not desc:
                        span = desc_el.locator('span').first
                        desc = span.inner_text().strip() if span.count() > 0 else ""
                else:
                    desc = ""

                if title and desc:
                    specs[title.lower()] = desc.strip()
                    print(f"[scraper]      ✓ {title}: {desc[:70]}")

            except Exception:
                pass

        if specs:
            print(f"[scraper]    ✅ {len(specs)} raw specifications extracted")
        else:
            print("[scraper]    ⚠️  No specifications found")

    except Exception as e:
        print(f"[scraper]    ⚠️  Spec extraction error: {e}")

    return specs


def map_specifications_to_fields(specs: dict) -> dict:
    """Map raw spec labels → internal field names."""
    if not specs:
        return {}

    mapped = {}

    for field_name, keywords in SPEC_MAPPING_RULES.items():
        for spec_key, spec_value in specs.items():
            if any(kw in spec_key for kw in keywords):
                if field_name not in mapped and spec_value.strip():
                    mapped[field_name] = spec_value.strip()
                    break

    # Build dimensions from H/W if not directly matched
    if not mapped.get('dimensions'):
        combined = _build_dimensions_from_hwl(specs)
        if combined:
            mapped['dimensions'] = combined
            print(f"[scraper]    📐 Dimensions built from H/W: {combined}")

    # Append capacity to dimensions
    capacity = mapped.get('capacity', '')
    if capacity:
        if mapped.get('dimensions'):
            mapped['dimensions'] = f"{mapped['dimensions']} | Capacity: {capacity}"
        else:
            mapped['dimensions'] = f"Capacity: {capacity}"

    # Merge features into bullet_points
    features = mapped.pop('features', '')
    if features:
        bullets = mapped.get('bullet_points', [])
        if isinstance(bullets, list):
            bullets.append(features)
        else:
            bullets = [bullets, features]
        mapped['bullet_points'] = bullets

    print(f"[scraper]    ✅ {len(mapped)} fields mapped from specifications:")
    for k, v in mapped.items():
        print(f"[scraper]       {k}: {str(v)[:70]}")

    return mapped


# ─────────────────────────────────────────────────────────────────────────────
# DESCRIPTION EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_description_correct(page) -> str:
    """Try several selectors in order; return the first useful result."""
    print("[scraper] 📝 Extracting description...")

    # Scroll to description section
    try:
        page.evaluate("""
            const el = document.getElementById('nav-description');
            if (el) el.scrollIntoView({ behavior: 'instant', block: 'start' });
        """)
        page.wait_for_timeout(1500)
    except Exception:
        pass

    selectors = [
        ('richTextContainer',          'div.richTextContainer[data-rich-text-render="true"]'),
        ('product-description',        'div[id="product-description"]'),
        ('nav-description',            'div[id="nav-description"]'),
        ('detailmodule_text',          'div.detailmodule_text'),
        ('detail-desc-decorate-content','p.detail-desc-decorate-content'),
    ]

    for name, selector in selectors:
        try:
            el = page.locator(selector)
            if el.count() > 0:
                text = el.first.inner_text().strip()
                # Clean up common noise
                text = re.sub(r'^.*?Description\s+report\s+', '', text,
                              flags=re.IGNORECASE | re.DOTALL)
                if len(text) > 50 and "Smarter Shopping" not in text:
                    print(f"[scraper]    ✅ Description via {name} ({len(text)} chars)")
                    return text
        except Exception:
            pass

    # Paragraph fallback
    try:
        paras = page.locator('p.detail-desc-decorate-content')
        parts = [paras.nth(i).inner_text().strip()
                 for i in range(paras.count())
                 if paras.nth(i).inner_text().strip()]
        if parts:
            text = " | ".join(parts)
            print(f"[scraper]    ✅ Description via paragraphs ({len(text)} chars)")
            return text
    except Exception:
        pass

    print("[scraper]    ⚠️  No description found")
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# IMAGE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_all_images(page) -> dict:
    images = {}
    print("[scraper] 🖼️  Extracting images...")

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

    try:
        og = page.locator('meta[property="og:image"]').get_attribute('content')
        if og:
            images['image_1'] = og
            print("[scraper]    ✅ 1 image from og:image")
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
                m = re.search(r'"price":\s*"([^"]+)"', txt)
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
# DOM EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_dom(page) -> dict:
    data = {}

    # Extract specs (includes scroll logic)
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
    try:
        with sync_playwright() as p:

            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-web-security",
                ]
            )

            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 900},
                locale="en-US",
                timezone_id="Asia/Karachi",
                java_script_enabled=True,
            )

            page = context.new_page()

            print(f"\n[scraper] 🌐 Opening: {url}")
            page.goto(url, timeout=60000, wait_until="domcontentloaded")

            # Wait for initial render
            page.wait_for_timeout(4000)

            # Human-like behaviour
            page.mouse.move(400, 300)
            page.mouse.wheel(0, 500)
            page.wait_for_timeout(1000)

            print("[scraper] 🔍 Extracting data...")

            data = extract_from_meta_tags(page)
            data.update(extract_from_javascript(page))
            data.update(extract_from_dom(page))

            browser.close()

        # Validate
        if not data.get('title'):
            print("[scraper] ❌ No title extracted — aborting")
            return None

        # Summary
        core_spec_fields = [
            'brand', 'color', 'dimensions', 'weight', 'material',
            'certifications', 'country_of_origin', 'warranty', 'product_type'
        ]
        extra_spec_fields = [
            'capacity', 'freezer_capacity', 'voltage', 'model_number',
            'power_source', 'installation', 'battery', 'display',
            'camera', 'connectivity', 'memory', 'os'
        ]
        filled_core  = [k for k in core_spec_fields  if data.get(k)]
        filled_extra = [k for k in extra_spec_fields if data.get(k)]

        print(f"\n[scraper] ✅ Extraction complete")
        print(f"[scraper]    Title      : {data.get('title', '')[:70]}")
        print(f"[scraper]    Description: {len(data.get('description', ''))} chars")
        print(f"[scraper]    Core specs : {len(filled_core)} → {filled_core}")
        print(f"[scraper]    Extra specs: {len(filled_extra)} → {filled_extra}")
        print(f"[scraper]    Images     : {sum(1 for i in range(1, 21) if data.get(f'image_{i}'))}")

        # Apply defaults
        defaults = {
            'description': '', 'brand': '', 'color': '', 'dimensions': '',
            'weight': '', 'material': '', 'certifications': '',
            'country_of_origin': '', 'warranty': '', 'product_type': '',
            'shipping': '', 'price': '', 'rating': '', 'reviews': '',
            'bullet_points': [], 'age_from': '', 'age_to': '',
            'gender': '', 'safety_warning': '',
            'capacity': '', 'freezer_capacity': '', 'voltage': '',
            'model_number': '', 'power_source': '', 'installation': '',
            'battery': '', 'display': '', 'camera': '',
            'connectivity': '', 'memory': '', 'os': '',
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
    test_url = "https://www.aliexpress.com/item/1005010716013669.html"
    result   = get_product_info(test_url)

    if result:
        print("\n" + "=" * 80)
        print("=== SCRAPED DATA ===")
        print("=" * 80)
        for k, v in result.items():
            if v and not k.startswith('image_'):
                print(f"  {k:25s}: {str(v)[:100]}")
        images = sum(1 for i in range(1, 21) if result.get(f'image_{i}'))
        print(f"  {'images':25s}: {images} found")
    else:
        print("Scraping failed.")
