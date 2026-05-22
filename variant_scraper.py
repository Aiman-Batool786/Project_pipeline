"""
variant_scraper.py
──────────────────
Extracts structured product variant data from AliExpress product pages.

Handles:
  • Color variants  — image-based swatches (sku-item--image)
  • Size variants   — text tiles (sku-item--text) with optional country-based labels
  • Country dropdown — Size(EU), Size(US), etc. → clicks each option and scrapes
  • Plain sizes     — e.g. "S", "M", "L", "XL" with no country prefix

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
        {"country": "US", "options": ["S(US 4)", "M(US 6)", "L(US 08/10)"]}
      ],
      "plain_options": []   // only when type == "plain"
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

# Matches: "S(EU 36)", "M(US 6)", "L(UK 12/14)", "XL(SG SG-3XL)", "M(MX M)"
_COUNTRY_IN_SIZE_RE = re.compile(
    r'\(('
    + '|'.join(re.escape(c) for c in COUNTRY_SIZE_CODES)
    + r')\b',
    re.IGNORECASE
)

# AliExpress CDN thumbnail suffix stripper
_AVIF_THUMB_RE = re.compile(r'(_\d+x\d+q\d+\.jpg_\.avif|_\.avif)$', re.IGNORECASE)

# Matches the current country shown in the size button: Size(FR), Size(EU), etc.
_SIZE_BTN_COUNTRY_RE = re.compile(r'Size\(([A-Z]{2})\)', re.IGNORECASE)


def _normalise_image_url(url: str) -> str:
    if not url:
        return url
    url = _AVIF_THUMB_RE.sub('', url)
    if url.startswith('//'):
        url = 'https:' + url
    return url


# ─────────────────────────────────────────────────────────────────────────────
# HTML PARSERS (BeautifulSoup)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_color_variants(soup: BeautifulSoup) -> list:
    """Extract color variants from sku-item--image divs."""
    colors = []
    seen   = set()

    sku_rows = soup.find_all('div', attrs={'data-sku-row': True,
                                           'class': re.compile(r'sku-item--skus')})
    for row in sku_rows:
        items = row.find_all('div', attrs={'data-sku-col': True,
                                            'class': re.compile(r'sku-item--image')})
        if not items:
            continue
        for item in items:
            img = item.find('img')
            if not img:
                continue
            alt     = (img.get('alt') or '').strip()
            src     = _normalise_image_url(img.get('src', ''))
            sku_col = item.get('data-sku-col', '')
            cls     = ' '.join(item.get('class', []))
            selected = 'sku-item--selected' in cls
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


def _scrape_sizes_from_soup(soup: BeautifulSoup) -> list[str]:
    """
    Read all size labels currently visible in the DOM.
    Returns a flat list of title strings, e.g. ["S(EU 36)", "M(EU 38)", ...].
    """
    labels = []
    size_rows = soup.find_all('div', attrs={'data-sku-row': True,
                                             'class': re.compile(r'sku-item--skus')})
    for row in size_rows:
        items = row.find_all('div', attrs={'data-sku-col': True,
                                            'class': re.compile(r'sku-item--text')})
        if not items:
            continue
        for item in items:
            label = (item.get('title') or '').strip()
            if not label:
                span  = item.find('span')
                label = (span.get_text(strip=True) if span else item.get_text(strip=True))
            if label:
                labels.append(label)
    return labels


def _detect_country_from_labels(labels: list[str]) -> Optional[str]:
    """Return the country code found in the first matching label, or None."""
    for label in labels:
        m = _COUNTRY_IN_SIZE_RE.search(label)
        if m:
            return m.group(1).upper()
    return None


def _parse_size_variants_from_html(html: str) -> dict:
    """
    Parse size variants from a single HTML snapshot (no browser interaction).
    Used when there is no country dropdown — i.e. plain sizes only.
    """
    soup   = BeautifulSoup(html, 'html.parser')
    labels = _scrape_sizes_from_soup(soup)

    if not labels:
        return {'type': 'plain', 'systems': [], 'plain_options': []}

    country = _detect_country_from_labels(labels)
    if country:
        return {
            'type':          'country_mapped',
            'systems':       [{'country': country, 'options': labels}],
            'plain_options': [],
        }
    return {
        'type':          'plain',
        'systems':       [],
        'plain_options': labels,
    }


def extract_variants_from_html(html: str, product_id: str) -> dict:
    """
    Parse rendered HTML (no browser) and return full variant structure.
    NOTE: For country-dropdown products use scrape_product_variants() instead,
    which clicks through each country option in the browser.
    """
    soup           = BeautifulSoup(html, 'html.parser')
    color_variants = _parse_color_variants(soup)
    size_variants  = _parse_size_variants_from_html(html)

    return {
        'product_id': product_id,
        'variants': {
            'color': color_variants,
            'size':  size_variants,
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER HELPERS — country dropdown interaction
# ─────────────────────────────────────────────────────────────────────────────

def _get_size_button_text(page) -> str:
    """Return the visible text of the size-system button, e.g. 'Size(FR)'."""
    try:
        btn = page.query_selector(
            'button.comet-v2-btn-important span[data-spm-anchor-id]'
        )
        if btn:
            return (btn.inner_text() or '').strip()
    except Exception:
        pass
    return ''


def _has_country_size_dropdown(page) -> bool:
    """
    Return True if the page has a country-based size-system dropdown button
    (e.g. 'Size(FR)', 'Size(EU)').
    """
    text = _get_size_button_text(page)
    return bool(_SIZE_BTN_COUNTRY_RE.search(text))


def _get_dropdown_country_options(page) -> list[str]:
    """
    Open the size-system dropdown and return all available country codes.
    Closes the dropdown afterward (presses Escape).

    Returns e.g. ["EU", "US", "ES", "FR", "UK", "DE", "IT", "MX", "BR", "AU", "SG", "JP", "KR"]
    """
    countries = []
    try:
        # Click the button to open the dropdown
        btn = page.query_selector(
            'button.comet-v2-btn-important'
        )
        if not btn:
            return countries

        btn.click()
        page.wait_for_timeout(800)

        # The dropdown items — AliExpress renders them in a popup/overlay list
        # Selector covers both comet-v2-select-option and comet-dropdown-menu-item patterns
        option_els = page.query_selector_all(
            '.comet-v2-select-dropdown li, '
            '.comet-v2-select-item, '
            '.comet-dropdown-menu-item, '
            '[class*="select-dropdown"] li, '
            '[class*="dropdown-menu"] li'
        )

        for el in option_els:
            text = (el.inner_text() or '').strip().upper()
            if text in COUNTRY_SIZE_CODES or text == 'DEFAULT':
                countries.append(text)

        # Close the dropdown
        page.keyboard.press('Escape')
        page.wait_for_timeout(400)

    except Exception as e:
        print(f'[variant_scraper] Dropdown read error: {e}')
        try:
            page.keyboard.press('Escape')
        except Exception:
            pass

    return countries


def _select_country_option(page, country: str) -> bool:
    """
    Open the size-system dropdown and click the given country option.
    Returns True on success.
    """
    try:
        btn = page.query_selector('button.comet-v2-btn-important')
        if not btn:
            return False

        btn.click()
        page.wait_for_timeout(800)

        # Find and click the matching li/option
        option_els = page.query_selector_all(
            '.comet-v2-select-dropdown li, '
            '.comet-v2-select-item, '
            '.comet-dropdown-menu-item, '
            '[class*="select-dropdown"] li, '
            '[class*="dropdown-menu"] li'
        )

        for el in option_els:
            text = (el.inner_text() or '').strip().upper()
            if text == country.upper():
                el.click()
                page.wait_for_timeout(900)
                return True

        # Not found — close and return False
        page.keyboard.press('Escape')
        page.wait_for_timeout(400)
        return False

    except Exception as e:
        print(f'[variant_scraper] Select country "{country}" error: {e}')
        try:
            page.keyboard.press('Escape')
        except Exception:
            pass
        return False


def _scrape_all_country_sizes(page) -> dict:
    """
    Iterate through every country option in the size dropdown,
    scrape the size labels for each, and return a complete size block.

    Returns a size dict with type='country_mapped'.
    """
    systems = []

    countries = _get_dropdown_country_options(page)
    print(f'[variant_scraper] Found country options: {countries}')

    if not countries:
        # Fallback: read whatever is currently shown
        soup   = BeautifulSoup(page.content(), 'html.parser')
        labels = _scrape_sizes_from_soup(soup)
        country = _detect_country_from_labels(labels) or 'DEFAULT'
        return {
            'type':          'country_mapped' if country != 'DEFAULT' else 'plain',
            'systems':       [{'country': country, 'options': labels}] if labels else [],
            'plain_options': labels if country == 'DEFAULT' else [],
        }

    seen_countries = set()

    for country in countries:
        if country in seen_countries:
            continue
        seen_countries.add(country)

        if country == 'DEFAULT':
            # Read current state without selecting
            soup   = BeautifulSoup(page.content(), 'html.parser')
            labels = _scrape_sizes_from_soup(soup)
            if labels:
                systems.append({'country': 'DEFAULT', 'options': labels})
            continue

        ok = _select_country_option(page, country)
        if not ok:
            print(f'[variant_scraper] Could not select country: {country}')
            continue

        # Wait for the size tiles to update
        page.wait_for_timeout(600)

        soup   = BeautifulSoup(page.content(), 'html.parser')
        labels = _scrape_sizes_from_soup(soup)

        print(f'[variant_scraper]   {country}: {labels}')

        if labels:
            # Verify the labels actually contain this country code
            detected = _detect_country_from_labels(labels)
            systems.append({
                'country': detected or country,
                'options': labels,
            })

    if not systems:
        return {'type': 'plain', 'systems': [], 'plain_options': []}

    # If all options are DEFAULT/plain, treat as plain
    if len(systems) == 1 and systems[0]['country'] == 'DEFAULT':
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
# BROWSER-BASED SCRAPER (Camoufox)
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
    Full browser scrape: load the AliExpress product page, detect whether a
    country-size dropdown is present, iterate through all country options if so,
    and return the complete variant structure.

    Args:
        product_id:  AliExpress numeric product ID string.
        max_retries: Number of browser retry attempts.

    Returns:
        Variant dict as described in the module docstring.
        On failure returns the empty-structure with an 'error' key.
    """
    from camoufox.sync_api import Camoufox

    base_url = _build_product_url(product_id)
    empty    = {
        'product_id': product_id,
        'variants':   {'color': [], 'size': {'type': 'plain', 'systems': [], 'plain_options': []}},
    }

    for attempt in range(1, max_retries + 1):
        print(f'[variant_scraper] Attempt {attempt}/{max_retries} for product {product_id}')

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

                # Wait for SKU rows
                try:
                    page.wait_for_selector('[data-sku-row]', timeout=15_000)
                except Exception:
                    print(f'[variant_scraper] SKU selector not found on attempt {attempt}')

                # Gentle scroll to trigger lazy loads
                for _ in range(3):
                    page.mouse.wheel(0, random.randint(300, 600))
                    page.wait_for_timeout(random.randint(400, 700))
                page.wait_for_timeout(1_500)

                # ── Color variants (HTML parse, no interaction needed) ──────
                html           = page.content()
                soup           = BeautifulSoup(html, 'html.parser')
                color_variants = _parse_color_variants(soup)

                # ── Size variants ────────────────────────────────────────────
                if _has_country_size_dropdown(page):
                    print('[variant_scraper] Country-size dropdown detected — scraping all options')
                    size_variants = _scrape_all_country_sizes(page)
                else:
                    print('[variant_scraper] No country dropdown — scraping plain sizes')
                    size_variants = _parse_size_variants_from_html(page.content())

                page.close()
                ctx.close()

            result = {
                'product_id': product_id,
                'variants': {
                    'color': color_variants,
                    'size':  size_variants,
                }
            }

            has_colors = bool(result['variants']['color'])
            has_sizes  = bool(
                result['variants']['size']['plain_options'] or
                result['variants']['size']['systems']
            )

            if has_colors or has_sizes:
                print(
                    f"[variant_scraper] OK — "
                    f"{len(result['variants']['color'])} colors, "
                    f"size type={result['variants']['size']['type']}, "
                    f"systems={[s['country'] for s in result['variants']['size']['systems']]}"
                )
                return result

            print(f'[variant_scraper] No variants found on attempt {attempt}')

        except Exception as e:
            print(f'[variant_scraper] Browser error on attempt {attempt}: {e}')
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
    Persist extracted variant data to the product_variants table.

    Schema
    ──────
    product_variants
      id             INTEGER PK AUTOINCREMENT
      product_id     INTEGER  (FK → scraped_products.product_id)
      aliexpress_id  TEXT
      variant_type   TEXT     ('color' | 'size')
      variant_name   TEXT     (color name OR full size label)
      variant_value  TEXT     (image_url for colors; '' for sizes)
      image_url      TEXT
      sku_col_id     TEXT     (colors only)
      system_country TEXT     (country code for country-mapped sizes; '' otherwise)
      is_selected    INTEGER  (1 if selected at scrape time)
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

    # Color rows
    for c in color_list:
        rows.append((
            product_id_int,
            aliexpress_id,
            'color',
            c.get('name', ''),
            c.get('image_url', ''),
            c.get('image_url', ''),
            c.get('sku_col_id', ''),
            '',
            1 if c.get('selected') else 0,
        ))

    # Size rows
    if size_data.get('type') == 'country_mapped':
        for system in size_data.get('systems', []):
            country = system.get('country', '')
            for opt in system.get('options', []):
                rows.append((
                    product_id_int,
                    aliexpress_id,
                    'size',
                    opt,
                    '',
                    '',
                    '',
                    country,
                    0,
                ))
    else:
        for opt in size_data.get('plain_options', []):
            rows.append((
                product_id_int,
                aliexpress_id,
                'size',
                opt,
                '',
                '',
                '',
                '',
                0,
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
        print(f'[variant_scraper] Saved {len(rows)} variant rows for product_id={product_id_int}')

    conn.close()
    return len(rows) > 0


def get_variants_from_db(product_id_int: int) -> Optional[dict]:
    """Re-assemble the variant JSON from stored rows. Returns None if nothing stored."""
    import sqlite3

    conn   = sqlite3.connect(DB_NAME)
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
        'variants': {
            'color': colors,
            'size':  size_block,
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    pid    = sys.argv[1] if len(sys.argv) > 1 else '1005012117886583'
    result = scrape_product_variants(pid)
    print(json.dumps(result, indent=2, ensure_ascii=False))
