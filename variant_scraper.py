"""
variant_scraper.py
──────────────────
Extracts structured product variant data from AliExpress product pages.

Handles:
  • Color variants  — image-based swatches (sku-item--image)
  • Size variants   — text tiles (sku-item--text) with optional country-based labels
  • Country-mapped  — e.g. "M (US 38)", "M (EU 48)", etc.
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
        {"country": "US", "options": ["M (US 38)", "L (US 40)"]}
      ],
      "plain_options": ["S", "M", "L", "XL"]   // only when type == "plain"
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

# Known country codes that appear inside size labels
COUNTRY_SIZE_CODES = {
    "US", "EU", "AU", "UK", "CA", "FR", "DE", "IT", "ES",
    "JP", "CN", "RU", "BR", "IN", "KR"
}

# Regex: detect country in size label like "M (US 38)" or "L (EU 44)"
_COUNTRY_IN_SIZE_RE = re.compile(
    r'\(('
    + '|'.join(re.escape(c) for c in COUNTRY_SIZE_CODES)
    + r')\s+[\d\-]+\)',
    re.IGNORECASE
)

# AliExpress CDN — strip the thumbnail+avif suffix to get the base .jpg URL
# Input:  ...S134b54c50b9f4fbfb64d346cbf11b1352.jpg_220x220q75.jpg_.avif
# Output: ...S134b54c50b9f4fbfb64d346cbf11b1352.jpg
_AVIF_THUMB_RE = re.compile(r'(_\d+x\d+q\d+\.jpg_\.avif|_\.avif)$', re.IGNORECASE)


def _normalise_image_url(url: str) -> str:
    """Strip the thumbnail size/avif suffix to get the base image URL."""
    if not url:
        return url
    # Remove avif+thumbnail suffix (the base already ends in .jpg)
    url = _AVIF_THUMB_RE.sub('', url)
    if url.startswith('//'):
        url = 'https:' + url
    return url


# ─────────────────────────────────────────────────────────────────────────────
# HTML PARSERS (BeautifulSoup)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_color_variants(soup: BeautifulSoup) -> list:
    """
    Extract color variants from sku-item--image divs.

    HTML pattern:
      <div class="sku-item--skus--..." data-sku-row="14">
        <div data-sku-col="14-193" class="sku-item--image--...">
          <img src="..." alt="black">
        </div>
        ...
      </div>
    """
    colors = []
    seen   = set()

    # Find the container div for image-based variants (data-sku-row present)
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

            alt = (img.get('alt') or '').strip()
            src = _normalise_image_url(img.get('src', ''))
            sku_col = item.get('data-sku-col', '')

            # is this item currently selected?
            cls = ' '.join(item.get('class', []))
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


def _parse_size_variants(soup: BeautifulSoup) -> dict:
    """
    Extract size variants from sku-item--text divs.

    Two modes:
      1. Country-mapped: "M (US 38)", "L (US 40)"  → grouped by country code
      2. Plain:          "S", "M", "L", "XL"

    HTML pattern:
      <div class="sku-item--skus--..." data-sku-row="5">
        <div data-sku-col="5-100014064" class="sku-item--text--..." title="S"><span>S</span></div>
        ...
      </div>
    """
    size_rows = soup.find_all('div', attrs={'data-sku-row': True,
                                             'class': re.compile(r'sku-item--skus')})

    raw_labels = []  # all size text labels in DOM order

    for row in size_rows:
        items = row.find_all('div', attrs={'data-sku-col': True,
                                            'class': re.compile(r'sku-item--text')})
        if not items:
            continue

        for item in items:
            # prefer title attribute, fall back to inner text
            label = (item.get('title') or '').strip()
            if not label:
                span = item.find('span')
                label = (span.get_text(strip=True) if span else item.get_text(strip=True))
            if label:
                raw_labels.append(label)

    if not raw_labels:
        return {'type': 'plain', 'systems': [], 'plain_options': []}

    # Determine if labels contain country codes
    country_hits = {}
    for label in raw_labels:
        m = _COUNTRY_IN_SIZE_RE.search(label)
        if m:
            code = m.group(1).upper()
            country_hits.setdefault(code, []).append(label)

    if country_hits:
        # Country-mapped mode: group by country code
        systems = [
            {'country': code, 'options': opts}
            for code, opts in country_hits.items()
        ]
        return {
            'type':          'country_mapped',
            'systems':       systems,
            'plain_options': [],
        }
    else:
        # Plain mode: return as-is
        return {
            'type':          'plain',
            'systems':       [],
            'plain_options': raw_labels,
        }


def extract_variants_from_html(html: str, product_id: str) -> dict:
    """
    Master entry: parse rendered HTML and return full variant structure.

    Args:
        html:       Full rendered page HTML string.
        product_id: AliExpress product ID string.

    Returns:
        Structured variant dict matching the JSON contract above.
    """
    soup = BeautifulSoup(html, 'html.parser')

    color_variants = _parse_color_variants(soup)
    size_variants  = _parse_size_variants(soup)

    return {
        'product_id': product_id,
        'variants': {
            'color': color_variants,
            'size':  size_variants,
        }
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
    Full browser scrape: load the AliExpress product page and extract variants.

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
        print(f"[variant_scraper] Attempt {attempt}/{max_retries} for product {product_id}")

        url = _get_rotated_url(base_url)
        ua  = random.choice(USER_AGENTS)

        try:
            with Camoufox(headless=True, os='windows') as browser:
                ctx  = browser.new_context(
                    viewport={'width': 1440, 'height': 900},
                    locale='en-US',
                    user_agent=ua,
                    extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'},
                )
                page = ctx.new_page()

                page.goto(url, timeout=90_000, wait_until='domcontentloaded')

                # Wait for SKU rows to appear
                try:
                    page.wait_for_selector(
                        '[data-sku-row]',
                        timeout=15_000
                    )
                except Exception:
                    print(f"[variant_scraper] SKU selector not found on attempt {attempt}")

                # Scroll slightly to trigger lazy loads
                for _ in range(3):
                    page.mouse.wheel(0, random.randint(300, 600))
                    page.wait_for_timeout(random.randint(400, 700))

                page.wait_for_timeout(1500)

                html = page.content()
                page.close()
                ctx.close()

            result = extract_variants_from_html(html, product_id)

            # Validate: if we got at least some variants, return
            has_colors = bool(result['variants']['color'])
            has_sizes  = bool(
                result['variants']['size']['plain_options'] or
                result['variants']['size']['systems']
            )

            if has_colors or has_sizes:
                print(
                    f"[variant_scraper] OK — "
                    f"{len(result['variants']['color'])} colors, "
                    f"size type={result['variants']['size']['type']}"
                )
                return result

            print(f"[variant_scraper] No variants found on attempt {attempt}")

        except Exception as e:
            print(f"[variant_scraper] Browser error on attempt {attempt}: {e}")
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

    Each color and each size option is stored as a separate row:

    product_variants
    ─────────────────────────────────────────────────────────────────
    id              INTEGER PK
    product_id      INTEGER  (FK → scraped_products.product_id)
    aliexpress_id   TEXT     (raw AliExpress numeric ID)
    variant_type    TEXT     ('color' | 'size')
    variant_name    TEXT     (color name OR size label)
    variant_value   TEXT     (image_url for colors; empty for sizes)
    image_url       TEXT     (color image or empty)
    sku_col_id      TEXT     (raw data-sku-col attribute, colors only)
    system_country  TEXT     (US/EU/AU for country-mapped sizes, else '')
    is_selected     INTEGER  (1 if swatch was selected at scrape time)
    created_at      TIMESTAMP
    """
    import sqlite3

    conn   = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Ensure table exists
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

    # ── Color rows ──────────────────────────────────────────────────────────
    for c in color_list:
        rows.append((
            product_id_int,
            aliexpress_id,
            'color',
            c.get('name', ''),
            c.get('image_url', ''),       # variant_value = image URL for colors
            c.get('image_url', ''),
            c.get('sku_col_id', ''),
            '',                           # no country for colors
            1 if c.get('selected') else 0,
        ))

    # ── Size rows ────────────────────────────────────────────────────────────
    if size_data.get('type') == 'country_mapped':
        for system in size_data.get('systems', []):
            country = system.get('country', '')
            for opt in system.get('options', []):
                rows.append((
                    product_id_int,
                    aliexpress_id,
                    'size',
                    opt,
                    '',         # no image for sizes
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
        # Remove old rows for this product before re-inserting
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
        print(f"[variant_scraper] Saved {len(rows)} variant rows for product_id={product_id_int}")

    conn.close()
    return len(rows) > 0


def get_variants_from_db(product_id_int: int) -> Optional[dict]:
    """
    Re-assemble the variant JSON from stored rows.
    Returns None if nothing is stored.
    """
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
    sizes_mapped  = {}   # country → [labels]

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
# CLI (quick test)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    pid = sys.argv[1] if len(sys.argv) > 1 else '1005012117886583'
    result = scrape_product_variants(pid)
    print(json.dumps(result, indent=2, ensure_ascii=False))
