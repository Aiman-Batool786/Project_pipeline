"""
variant_scraper.py
──────────────────
Extracts structured product variant data from AliExpress product pages.

Handles:
  • Color variants  — image-based swatches (sku-item--image)
  • Size variants   — text tiles (sku-item--text)
  • Country dropdown — Size(EU), Size(US), etc.
      ✅ Opens dropdown using correct selector
      ✅ Reads ALL country options from .comet-v2-menu-item
      ✅ Clicks each country, waits for DOM refresh
      ✅ Extracts sizes after each selection
      ✅ Avoids stale element issues by re-querying each loop
  • Plain sizes — "S", "M", "L", "XL"

Output JSON contract:
{
  "product_id": "1005012117886583",
  "variants": {
    "color": [
      {"name": "Navy Blue", "image_url": "https://...", "sku_col_id": "14-193", "selected": false}
    ],
    "size": {
      "type": "country_mapped" | "plain",
      "systems": [
        {"country": "EU", "options": ["S(EU 36)", "M(EU 38)", "L(EU 40/42)"]},
        {"country": "US", "options": ["S(US 4)",  "M(US 6)",  "L(US 08/10)"]}
      ],
      "plain_options": []
    }
  }
}
"""

import re
import json
import random
import time
from typing import Optional
from bs4 import BeautifulSoup


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

COUNTRY_SIZE_CODES = {
    "US", "EU", "AU", "UK", "CA", "FR", "DE", "IT", "ES",
    "JP", "CN", "RU", "BR", "IN", "KR", "MX", "SG"
}

# Matches country code inside a size label: "S(EU 36)", "M(US 6)", "XL(SG SG-3XL)"
_COUNTRY_IN_SIZE_RE = re.compile(
    r'\(('
    + '|'.join(re.escape(c) for c in COUNTRY_SIZE_CODES)
    + r')\b',
    re.IGNORECASE
)

# Matches the size button label: "Size(FR)", "Size(US)", "Size(EU)"
_SIZE_BTN_COUNTRY_RE = re.compile(r'Size\s*\(([A-Z]{2})\)', re.IGNORECASE)

# Strip AliExpress thumbnail/avif suffixes
_AVIF_THUMB_RE = re.compile(r'(_\d+x\d+q\d+\.jpg_\.avif|_\.avif)$', re.IGNORECASE)


def _normalise_image_url(url: str) -> str:
    if not url:
        return url
    url = _AVIF_THUMB_RE.sub('', url)
    if url.startswith('//'):
        url = 'https:' + url
    return url


# ─────────────────────────────────────────────────────────────────────────────
# HTML PARSERS  (BeautifulSoup — no browser interaction)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_color_variants(soup: BeautifulSoup) -> list:
    """Extract color swatches from sku-item--image divs."""
    colors = []
    seen   = set()

    sku_rows = soup.find_all(
        'div',
        attrs={'data-sku-row': True, 'class': re.compile(r'sku-item--skus')}
    )
    for row in sku_rows:
        items = row.find_all(
            'div',
            attrs={'data-sku-col': True, 'class': re.compile(r'sku-item--image')}
        )
        if not items:
            continue
        for item in items:
            img = item.find('img')
            if not img:
                continue
            alt      = (img.get('alt') or '').strip()
            src      = _normalise_image_url(img.get('src', ''))
            sku_col  = item.get('data-sku-col', '')
            selected = 'sku-item--selected' in ' '.join(item.get('class', []))
            key = alt.lower() or src
            if key in seen:
                continue
            seen.add(key)
            colors.append({
                'name':       alt or sku_col,
                'image_url':  src,
                'sku_col_id': sku_col,
                'selected':   selected,
            })
    return colors


def _scrape_size_labels_from_soup(soup: BeautifulSoup) -> list[str]:
    """
    Read every visible size label from sku-item--text divs.
    Returns a flat list, e.g. ["S(EU 36)", "M(EU 38)", ...].
    """
    labels    = []
    size_rows = soup.find_all(
        'div',
        attrs={'data-sku-row': True, 'class': re.compile(r'sku-item--skus')}
    )
    for row in size_rows:
        items = row.find_all(
            'div',
            attrs={'data-sku-col': True, 'class': re.compile(r'sku-item--text')}
        )
        if not items:
            continue
        for item in items:
            label = (item.get('title') or '').strip()
            if not label:
                span  = item.find('span')
                label = span.get_text(strip=True) if span else item.get_text(strip=True)
            if label:
                labels.append(label)
    return labels


def _detect_country_from_labels(labels: list[str]) -> Optional[str]:
    for label in labels:
        m = _COUNTRY_IN_SIZE_RE.search(label)
        if m:
            return m.group(1).upper()
    return None


def _parse_plain_size_variants(html: str) -> dict:
    """Parse size variants from a static HTML snapshot (no dropdown)."""
    soup   = BeautifulSoup(html, 'html.parser')
    labels = _scrape_size_labels_from_soup(soup)

    if not labels:
        return {'type': 'plain', 'systems': [], 'plain_options': []}

    country = _detect_country_from_labels(labels)
    if country:
        return {
            'type':          'country_mapped',
            'systems':       [{'country': country, 'options': labels}],
            'plain_options': [],
        }
    return {'type': 'plain', 'systems': [], 'plain_options': labels}


def extract_variants_from_html(html: str, product_id: str) -> dict:
    """
    Parse a static HTML snapshot — use only when no country dropdown is present.
    For country-dropdown products call scrape_product_variants() instead.
    """
    soup           = BeautifulSoup(html, 'html.parser')
    color_variants = _parse_color_variants(soup)
    size_variants  = _parse_plain_size_variants(html)
    return {
        'product_id': product_id,
        'variants':   {'color': color_variants, 'size': size_variants},
    }


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER HELPERS — country dropdown interaction
# ─────────────────────────────────────────────────────────────────────────────

# CSS selectors based on the confirmed AliExpress HTML:
#   button  → button.comet-v2-btn-important  (contains "Size(XX)" text)
#   menu    → .comet-v2-dropdown-body .comet-v2-menu-item  (each country <li>)
#   label   → .comet-v2-menu-item-content  (text inside each <li>)

_BTN_SELECTOR      = 'button.comet-v2-btn-important'
_MENU_ITEM_SEL     = '.comet-v2-dropdown-body .comet-v2-menu-item'
_MENU_CONTENT_SEL  = '.comet-v2-menu-item-content'
_SKU_ROW_SEL       = '[data-sku-row]'


def _has_country_size_dropdown(page) -> bool:
    """
    Return True if any visible button has text matching 'Size(XX)'.
    Uses the confirmed button selector.
    """
    try:
        btns = page.locator(_BTN_SELECTOR).all()
        for btn in btns:
            try:
                txt = (btn.inner_text(timeout=1_000) or '').strip()
                if _SIZE_BTN_COUNTRY_RE.search(txt):
                    return True
            except Exception:
                pass
    except Exception:
        pass
    return False


def _open_dropdown(page) -> bool:
    """
    Click the Size(XX) button to open the country dropdown.
    Returns True if the menu appears within 3 s.
    """
    try:
        btns = page.locator(_BTN_SELECTOR).all()
        for btn in btns:
            try:
                txt = (btn.inner_text(timeout=1_000) or '').strip()
                if _SIZE_BTN_COUNTRY_RE.search(txt):
                    btn.scroll_into_view_if_needed()
                    btn.click()
                    # Wait until the dropdown body is visible
                    page.wait_for_selector(
                        '.comet-v2-dropdown-body',
                        state='visible',
                        timeout=3_000
                    )
                    return True
            except Exception:
                pass
    except Exception as e:
        print(f'[variant_scraper] _open_dropdown error: {e}')
    return False


def _read_country_list(page) -> list[str]:
    """
    Read all country names from the open dropdown menu.
    Dropdown must already be open when this is called.
    Returns e.g. ["Default", "EU", "US", "ES", "FR", "UK", "DE", ...]
    """
    countries = []
    try:
        items = page.locator(_MENU_ITEM_SEL).all()
        for item in items:
            try:
                # text is inside .comet-v2-menu-item-content <span>
                span = item.locator(_MENU_CONTENT_SEL).first
                txt  = (span.inner_text(timeout=800) or '').strip()
                if txt:
                    countries.append(txt)
            except Exception:
                pass
    except Exception as e:
        print(f'[variant_scraper] _read_country_list error: {e}')
    return countries


def _click_country_in_open_dropdown(page, country: str) -> bool:
    """
    Click a specific country option.  The dropdown must be open.
    Re-queries the DOM to avoid stale handles.
    """
    try:
        items = page.locator(_MENU_ITEM_SEL).all()
        for item in items:
            try:
                span = item.locator(_MENU_CONTENT_SEL).first
                txt  = (span.inner_text(timeout=800) or '').strip()
                if txt.upper() == country.upper():
                    item.scroll_into_view_if_needed()
                    item.click()
                    return True
            except Exception:
                pass
    except Exception as e:
        print(f'[variant_scraper] _click_country error "{country}": {e}')
    return False


def _wait_for_sizes_to_refresh(page, previous_labels: list[str], timeout_ms: int = 4_000):
    """
    Poll until the visible size labels differ from previous_labels,
    or until timeout_ms elapses.  This handles AliExpress's async DOM update.
    """
    deadline = time.time() + timeout_ms / 1_000
    while time.time() < deadline:
        html    = page.content()
        soup    = BeautifulSoup(html, 'html.parser')
        current = _scrape_size_labels_from_soup(soup)
        if current and current != previous_labels:
            return current
        time.sleep(0.3)
    # Return whatever we have even if unchanged
    soup = BeautifulSoup(page.content(), 'html.parser')
    return _scrape_size_labels_from_soup(soup)


def _scrape_all_country_sizes(page) -> dict:
    """
    Main orchestrator for country-dropdown products.

    Algorithm:
      1. Open dropdown → read full country list (done once)
      2. For each country:
           a. Re-open dropdown  (re-query to avoid stale handles)
           b. Click the country option
           c. Wait for size tiles to refresh
           d. Scrape the new labels
      3. Assemble & return a country_mapped size block
    """
    # ── Step 1: open once, read the full country list ──────────────────────
    if not _open_dropdown(page):
        print('[variant_scraper] Could not open dropdown — falling back to plain parse')
        return _parse_plain_size_variants(page.content())

    countries = _read_country_list(page)
    print(f'[variant_scraper] Dropdown countries: {countries}')

    if not countries:
        # Close and fall back
        try:
            page.keyboard.press('Escape')
        except Exception:
            pass
        return _parse_plain_size_variants(page.content())

    # Close dropdown before the loop starts
    try:
        page.keyboard.press('Escape')
        page.wait_for_timeout(400)
    except Exception:
        pass

    systems         = []
    seen_countries  = set()

    # Get baseline labels (whatever is shown before we touch anything)
    baseline_soup   = BeautifulSoup(page.content(), 'html.parser')
    previous_labels = _scrape_size_labels_from_soup(baseline_soup)

    # ── Step 2: iterate every country ─────────────────────────────────────
    for country in countries:
        norm = country.strip()
        if not norm or norm in seen_countries:
            continue
        seen_countries.add(norm)

        print(f'[variant_scraper] Selecting: {norm}')

        # a. Re-open dropdown
        if not _open_dropdown(page):
            print(f'[variant_scraper]   Could not reopen dropdown for {norm}, skipping')
            continue

        page.wait_for_timeout(400)   # brief pause after open

        # b. Click country
        clicked = _click_country_in_open_dropdown(page, norm)
        if not clicked:
            print(f'[variant_scraper]   Could not click "{norm}", skipping')
            try:
                page.keyboard.press('Escape')
            except Exception:
                pass
            continue

        # c. Wait for DOM update
        page.wait_for_timeout(300)                          # initial settle
        labels = _wait_for_sizes_to_refresh(page, previous_labels, timeout_ms=4_000)

        # d. Store result
        print(f'[variant_scraper]   {norm} → {labels}')

        if labels:
            detected_country = _detect_country_from_labels(labels)
            systems.append({
                'country': detected_country or norm,
                'options': labels,
            })
            previous_labels = labels  # update baseline for next iteration

    if not systems:
        return {'type': 'plain', 'systems': [], 'plain_options': []}

    # If only "Default" with no country code in labels → treat as plain
    if len(systems) == 1 and systems[0]['country'] == 'Default':
        return {
            'type':          'plain',
            'systems':       [],
            'plain_options': systems[0]['options'],
        }

    return {
        'type':          'country_mapped',
        'systems':       systems,
        'plain_options': [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER-BASED SCRAPER  (Camoufox)
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

REGIONS_SAFE = ["AE", "US", "AU", "CA", "PK", "SA", "TR"]


def _build_product_url(product_id: str) -> str:
    return f"https://www.aliexpress.com/item/{product_id}.html"


def _get_rotated_url(url: str) -> str:
    region = random.choice(REGIONS_SAFE)
    sep    = '&' if '?' in url else '?'
    return f"{url}{sep}shipFromCountry={region}"


def scrape_product_variants(product_id: str, max_retries: int = 2) -> dict:
    """
    Full browser scrape of an AliExpress product page.

    Flow:
      1. Load the page with Camoufox
      2. Parse color variants from HTML (no clicks needed)
      3. Detect whether a country-size dropdown is present
         • YES → _scrape_all_country_sizes() clicks every country
         • NO  → _parse_plain_size_variants() reads the static tiles
      4. Return the combined variant dict

    Args:
        product_id:  AliExpress numeric product ID.
        max_retries: Retry attempts on failure.

    Returns:
        Variant dict per the module-level JSON contract.
        On complete failure returns empty structure + 'error' key.
    """
    from camoufox.sync_api import Camoufox

    base_url = _build_product_url(product_id)
    empty    = {
        'product_id': product_id,
        'variants':   {
            'color': [],
            'size':  {'type': 'plain', 'systems': [], 'plain_options': []},
        },
    }

    for attempt in range(1, max_retries + 1):
        print(f'[variant_scraper] Attempt {attempt}/{max_retries} — product {product_id}')

        url = _get_rotated_url(base_url)
        ua  = random.choice(USER_AGENTS)

        try:
            with Camoufox(headless=True, os='windows') as browser:
                ctx = browser.new_context(
                    viewport={'width': 1440, 'height': 900},
                    locale='en-US',
                    user_agent=ua,
                    extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'},
                )
                page = ctx.new_page()
                page.goto(url, timeout=90_000, wait_until='domcontentloaded')

                # Wait for SKU tiles to appear
                try:
                    page.wait_for_selector(_SKU_ROW_SEL, timeout=15_000)
                except Exception:
                    print(f'[variant_scraper] SKU selector not found on attempt {attempt}')

                # Gentle scroll to trigger lazy-loaded content
                for _ in range(3):
                    page.mouse.wheel(0, random.randint(300, 600))
                    page.wait_for_timeout(random.randint(400, 700))
                page.wait_for_timeout(1_500)

                # ── Colors (pure HTML parse, no interaction) ───────────────
                soup           = BeautifulSoup(page.content(), 'html.parser')
                color_variants = _parse_color_variants(soup)

                # ── Sizes ──────────────────────────────────────────────────
                if _has_country_size_dropdown(page):
                    print('[variant_scraper] Country dropdown detected → iterating all countries')
                    size_variants = _scrape_all_country_sizes(page)
                else:
                    print('[variant_scraper] No country dropdown → plain size parse')
                    size_variants = _parse_plain_size_variants(page.content())

                page.close()
                ctx.close()

            result = {
                'product_id': product_id,
                'variants':   {'color': color_variants, 'size': size_variants},
            }

            has_colors = bool(result['variants']['color'])
            has_sizes  = bool(
                result['variants']['size']['plain_options'] or
                result['variants']['size']['systems']
            )

            if has_colors or has_sizes:
                n_systems = len(result['variants']['size']['systems'])
                countries = [s['country'] for s in result['variants']['size']['systems']]
                print(
                    f'[variant_scraper] ✓ {len(color_variants)} colors | '
                    f'size type={result["variants"]["size"]["type"]} | '
                    f'{n_systems} country system(s): {countries}'
                )
                return result

            print(f'[variant_scraper] No variants on attempt {attempt}')

        except Exception as e:
            print(f'[variant_scraper] Browser error attempt {attempt}: {e}')
            import traceback
            traceback.print_exc()

        if attempt < max_retries:
            time.sleep(random.uniform(3, 6))

    empty['error'] = 'No variants extracted after all retries'
    return empty


# ─────────────────────────────────────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────────────────────────────────────

DB_NAME = "products.db"


def save_variants_to_db(product_id_int: int, variants_data: dict) -> bool:
    """
    Persist variant data to the product_variants table.

    Schema
    ──────
    product_variants
      id             INTEGER PK AUTOINCREMENT
      product_id     INTEGER  FK → scraped_products.product_id
      aliexpress_id  TEXT
      variant_type   TEXT     'color' | 'size'
      variant_name   TEXT     color name OR full size label
      variant_value  TEXT     image_url for colors; '' for sizes
      image_url      TEXT
      sku_col_id     TEXT     colors only
      system_country TEXT     country code for mapped sizes; '' for plain
      is_selected    INTEGER  1 if selected at scrape time
      created_at     TIMESTAMP
    """
    import sqlite3

    conn   = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS product_variants (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id     INTEGER NOT NULL,
        aliexpress_id  TEXT,
        variant_type   TEXT NOT NULL,
        variant_name   TEXT,
        variant_value  TEXT,
        image_url      TEXT,
        sku_col_id     TEXT,
        system_country TEXT,
        is_selected    INTEGER DEFAULT 0,
        created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES scraped_products(product_id)
    )""")

    aliexpress_id = str(variants_data.get('product_id', ''))
    color_list    = variants_data.get('variants', {}).get('color', [])
    size_data     = variants_data.get('variants', {}).get('size', {})

    rows = []

    for c in color_list:
        rows.append((
            product_id_int, aliexpress_id, 'color',
            c.get('name', ''), c.get('image_url', ''), c.get('image_url', ''),
            c.get('sku_col_id', ''), '', 1 if c.get('selected') else 0,
        ))

    if size_data.get('type') == 'country_mapped':
        for system in size_data.get('systems', []):
            country = system.get('country', '')
            for opt in system.get('options', []):
                rows.append((
                    product_id_int, aliexpress_id, 'size',
                    opt, '', '', '', country, 0,
                ))
    else:
        for opt in size_data.get('plain_options', []):
            rows.append((
                product_id_int, aliexpress_id, 'size',
                opt, '', '', '', '', 0,
            ))

    if rows:
        cursor.execute(
            "DELETE FROM product_variants WHERE product_id = ?",
            (product_id_int,)
        )
        cursor.executemany("""
            INSERT INTO product_variants
              (product_id, aliexpress_id, variant_type, variant_name,
               variant_value, image_url, sku_col_id, system_country, is_selected)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        print(f'[variant_scraper] Saved {len(rows)} rows for product_id={product_id_int}')

    conn.close()
    return len(rows) > 0


def get_variants_from_db(product_id_int: int) -> Optional[dict]:
    """Re-assemble the variant JSON from stored rows. Returns None if empty."""
    import sqlite3

    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT * FROM product_variants
            WHERE product_id = ?
            ORDER BY variant_type, id
        """, (product_id_int,))
        rows = cursor.fetchall()
    except Exception:
        conn.close()
        return None
    finally:
        conn.close()

    if not rows:
        return None

    aliexpress_id = rows[0]['aliexpress_id']
    colors        = []
    sizes_plain   = []
    sizes_mapped  = {}

    for row in rows:
        if row['variant_type'] == 'color':
            colors.append({
                'name':       row['variant_name'],
                'image_url':  row['image_url'],
                'sku_col_id': row['sku_col_id'],
                'selected':   bool(row['is_selected']),
            })
        elif row['variant_type'] == 'size':
            country = row['system_country'] or ''
            if country:
                sizes_mapped.setdefault(country, []).append(row['variant_name'])
            else:
                sizes_plain.append(row['variant_name'])

    if sizes_mapped:
        size_block = {
            'type':          'country_mapped',
            'systems':       [{'country': c, 'options': o} for c, o in sizes_mapped.items()],
            'plain_options': [],
        }
    else:
        size_block = {
            'type':          'plain',
            'systems':       [],
            'plain_options': sizes_plain,
        }

    return {
        'product_id': aliexpress_id,
        'variants':   {'color': colors, 'size': size_block},
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    pid    = sys.argv[1] if len(sys.argv) > 1 else '1005010435033239'
    result = scrape_product_variants(pid)
    print(json.dumps(result, indent=2, ensure_ascii=False))
