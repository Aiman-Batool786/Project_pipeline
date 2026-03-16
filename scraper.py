"""
scraper.py
──────────
Playwright-based scraper for AliExpress product pages.

Extracts:
  • title (meta og:title)
  • price + shipping (JavaScript)
  • description (multiple fallback selectors)
  • specifications (nav-specification section) — comprehensive label mapping
  • images (imagePathList JSON → og:image fallback)

SPEC_MAPPING_RULES covers real AliExpress label names (lowercase substring match).
Extra fields (model_number, voltage, capacity, etc.) are also captured and
stored so data_mapper.py can route them to the correct template columns.
"""

from playwright.sync_api import sync_playwright
import re
import json


# ─────────────────────────────────────────────────────────────────────────────
# SPECIFICATION MAPPING RULES
# key   = your internal field name
# value = list of lowercase substrings to match against AliExpress spec labels
#
# REAL AliExpress labels seen in the wild (all lowercased):
#   "brand name", "brand"
#   "origin", "place of origin", "country of origin", "country"
#   "dimensions (l x w x h (inches)", "dimensions", "size", "taille"
#   "material", "matière", "materials"
#   "energy efficiency rating", "energy consumption grade"
#   "certification", "certifications", "normes", "standards"
#   "warranty", "garantie", "guarantee"
#   "refrigeration type", "product type", "type", "type de produit"
#   "color", "colour", "main color", "couleur"
#   "height", "width"                    ← used to build dimensions if no direct match
#   "weight", "net weight", "poids"
#   "voltage"
#   "capacity", "freezer capacity", "fridge capacity"
#   "model number", "model", "numéro de modèle"
#   "feature", "features"               ← mapped to bullet_points
#   "application"                       ← mapped to product_type
#   "age from", "recommended age from"
#   "age to",   "recommended age to"
#   "gender", "suitable for", "sexe"
#   "defrost type", "cooling method"    ← mapped to product_type (extra detail)
#   "power source"                      ← stored in product_type extras
#   "installation"                      ← stored as extra
#   "packaging types"                   ← ignored (not useful)
# ─────────────────────────────────────────────────────────────────────────────

SPEC_MAPPING_RULES = {
    # ── Core spec fields (map to template columns) ────────────────────────────
    'brand': [
        'brand name', 'brand', 'marque', 'manufacturer brand'
    ],
    'color': [
        'main color', 'color', 'colour', 'couleur', 'item color'
    ],
    'dimensions': [
        'dimensions (l x w x h',    # AliExpress fridge format
        'dimensions (l x w x h (cm',
        'dimensions (l x w x h (inches',
        'dimensions (cm)',
        'dimensions',
        'size',
        'taille',
        'product size',
        'item size',
        'package size',
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
        'energy efficiency rating',  # AliExpress appliance field
        'energy consumption grade',  # AliExpress appliance field
        'energy rating',
        'energy class',
    ],
    'country_of_origin': [
        'place of origin',           # AliExpress common label
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
        'refrigeration type',        # AliExpress appliance
        'cooling method',            # AliExpress appliance
        'defrost type',              # AliExpress appliance
        'product type',
        'type de produit',
        'product category',
        'item type',
        'type',
        'application',               # AliExpress appliance (Hotel, Household…)
        'use',
    ],

    # ── Extra fields stored for enrichment / mapping ──────────────────────────
    'age_from': [
        'age from',
        'recommended age from',
        'age (from)',
        'minimum age',
    ],
    'age_to': [
        'age to',
        'recommended age to',
        'age (to)',
        'maximum age',
    ],
    'gender': [
        'gender',
        'suitable for',
        'sexe',
        'for whom',
    ],

    # ── Extra appliance / electronics fields (stored, mapper can use them) ────
    'capacity': [
        'capacity',
        'fridge capacity',
        'net capacity',
        'total capacity',
        'volume',
    ],
    'freezer_capacity': [
        'freezer capacity',
    ],
    'voltage': [
        'voltage',
        'rated voltage',
        'operating voltage',
    ],
    'model_number': [
        'model number',
        'model no',
        'model',
        'item model',
        'numéro de modèle',
    ],
    'power_source': [
        'power source',
        'power supply',
    ],
    'installation': [
        'installation',
        'mounting type',
    ],
    'style': [
        'style',
    ],
    'features': [
        'feature',
        'features',
        'key features',
        'highlights',
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: try to build a combined dimensions string from H/W/D labels
# ─────────────────────────────────────────────────────────────────────────────

def _build_dimensions_from_hwl(specs: dict) -> str:
    """
    If no direct 'dimensions' key was found, try to combine
    Height + Width + Depth/Length into one string.
    """
    height = ""
    width  = ""
    depth  = ""

    for key, val in specs.items():
        k = key.lower()
        if 'height' in k and not height:
            height = val
        elif 'width' in k and not width:
            width = val
        elif ('depth' in k or 'length' in k) and not depth:
            depth = val

    parts = [v for v in [height, width, depth] if v]
    if len(parts) >= 2:
        return " x ".join(parts)
    return ""


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

    try:
        items = page.locator(
            '[id*="nav-specification"] [class*="specification--prop"]'
        ).all()

        if not items:
            # Fallback: try alternate selectors
            items = page.locator('[class*="specification-item"]').all()

        if items:
            print(f"[scraper]    Found {len(items)} specification items")

        for item in items:
            try:
                # Primary selectors
                title_el = item.locator('[class*="specification--title"] span').first
                desc_el  = item.locator('[class*="specification--desc"] span').first

                title = title_el.inner_text().strip() if title_el.count() > 0 else ""
                desc  = desc_el.inner_text().strip()  if desc_el.count() > 0  else ""

                # Fallback selectors if primary failed
                if not title:
                    title_el = item.locator('dt, .title, .key').first
                    title = title_el.inner_text().strip() if title_el.count() > 0 else ""
                if not desc:
                    desc_el = item.locator('dd, .value, .val').first
                    desc = desc_el.inner_text().strip() if desc_el.count() > 0 else ""

                if title and desc:
                    specs[title.lower()] = desc
                    print(f"[scraper]      {title}: {desc[:70]}")

            except Exception:
                pass

        if specs:
            print(f"[scraper]    ✅ {len(specs)} raw specifications extracted")
        else:
            print("[scraper]    ⚠️  No specifications found in nav-specification")

    except Exception as e:
        print(f"[scraper]    ⚠️  Spec extraction error: {e}")

    return specs


def map_specifications_to_fields(specs: dict) -> dict:
    """
    Map raw spec dict (lowercase keys) → template field names.

    Uses substring matching so minor label variations are handled.
    Multiple spec labels can populate the same field (first match wins).
    """
    if not specs:
        return {}

    mapped = {}

    for field_name, keywords in SPEC_MAPPING_RULES.items():
        for spec_key, spec_value in specs.items():
            if any(kw in spec_key for kw in keywords):
                if field_name not in mapped and spec_value.strip():
                    mapped[field_name] = spec_value.strip()
                    break   # first match wins for this field

    # ── Special: build dimensions from H/W if not directly matched ────────────
    if not mapped.get('dimensions'):
        combined = _build_dimensions_from_hwl(specs)
        if combined:
            mapped['dimensions'] = combined
            print(f"[scraper]    📐 Dimensions built from H/W/D: {combined}")

    # ── Special: append capacity to dimensions string if dimensions exist ─────
    capacity = mapped.get('capacity', '')
    if capacity and mapped.get('dimensions'):
        mapped['dimensions'] = f"{mapped['dimensions']} | Capacity: {capacity}"
    elif capacity:
        mapped['dimensions'] = f"Capacity: {capacity}"

    # ── Special: merge features into bullet_points ────────────────────────────
    features = mapped.pop('features', '')
    if features:
        existing_bullets = mapped.get('bullet_points', [])
        if isinstance(existing_bullets, list):
            existing_bullets.append(features)
        else:
            existing_bullets = [existing_bullets, features]
        mapped['bullet_points'] = existing_bullets

    print(f"[scraper]    ✅ {len(mapped)} fields mapped from specifications")
    for k, v in mapped.items():
        val_preview = str(v)[:60] + "…" if len(str(v)) > 60 else str(v)
        print(f"[scraper]       {k}: {val_preview}")

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

    # 5. detail-desc-decorate-content paragraphs
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
# DOM EXTRACTION  (scroll + specs + description + images)
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_dom(page) -> dict:
    data = {}

    # Scroll down to trigger lazy-loaded specification section
    page.mouse.wheel(0, 3000)
    page.wait_for_timeout(2000)

    # Click the "Specifications" tab if present (some AliExpress layouts)
    try:
        spec_tab = page.locator('a[href*="nav-specification"], [data-tab="nav-specification"]').first
        if spec_tab.count() > 0:
            spec_tab.click()
            page.wait_for_timeout(1500)
            print("[scraper]    📌 Clicked Specifications tab")
    except Exception:
        pass

    # Extract specifications
    raw_specs = extract_specifications(page)
    spec_fields = map_specifications_to_fields(raw_specs)
    data.update(spec_fields)

    # Store raw specs as well for debugging
    data['_raw_specs'] = raw_specs

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
    Launch headless Chromium, navigate to url, extract all product data.
    Returns a flat dict or None on failure.
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

        # ── Summary ───────────────────────────────────────────────────────────
        print(f"\n[scraper] ✅ Extraction complete")
        print(f"[scraper]    Title      : {data.get('title', '')[:70]}")
        print(f"[scraper]    Description: {len(data.get('description', ''))} chars")

        core_spec_fields = [
            'brand', 'color', 'dimensions', 'weight', 'material',
            'certifications', 'country_of_origin', 'warranty', 'product_type'
        ]
        extra_spec_fields = [
            'capacity', 'freezer_capacity', 'voltage', 'model_number',
            'power_source', 'installation', 'style'
        ]

        filled_core  = [k for k in core_spec_fields  if data.get(k)]
        filled_extra = [k for k in extra_spec_fields if data.get(k)]

        print(f"[scraper]    Core specs : {len(filled_core)} → {filled_core}")
        print(f"[scraper]    Extra specs: {len(filled_extra)} → {filled_extra}")
        print(f"[scraper]    Images     : {sum(1 for i in range(1, 21) if data.get(f'image_{i}'))}")

        # ── Apply defaults ────────────────────────────────────────────────────
        defaults = {
            'description': '',
            'brand': '',
            'color': '',
            'dimensions': '',
            'weight': '',
            'material': '',
            'certifications': '',
            'country_of_origin': '',
            'warranty': '',
            'product_type': '',
            'shipping': '',
            'price': '',
            'rating': '',
            'reviews': '',
            'bullet_points': [],
            'age_from': '',
            'age_to': '',
            'gender': '',
            'safety_warning': '',
            # Extra appliance fields
            'capacity': '',
            'freezer_capacity': '',
            'voltage': '',
            'model_number': '',
            'power_source': '',
            'installation': '',
            'style': '',
        }
        for key, default in defaults.items():
            if key not in data or not data[key]:
                data[key] = default

        for i in range(1, 21):
            data.setdefault(f'image_{i}', "")

        # Remove internal debug key before returning
        data.pop('_raw_specs', None)

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
