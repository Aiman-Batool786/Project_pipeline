"""
variant_scraper.py  (anti-block edition v2)
─────────────────────────────────────────────
Extracts structured product variant data from AliExpress product pages.

Anti-blocking techniques:
  ✅ Camoufox with OS-matched User-Agent (windows UA ↔ os='windows', etc.)
  ✅ Random User-Agent rotation per OS bucket
  ✅ Region URL rotation (REGIONS_SAFE / REGIONS_EU + SERVER_IS_EU flag)
  ✅ Optional residential proxy support (PROXY_LIST)
  ✅ GDPR / cookie-banner auto-dismissal (_dismiss_gdpr_banner)
  ✅ EU-page detection → auto-triggers consent flow
  ✅ ThreadPoolExecutor with hard timeout → no zombie browser hangs
  ✅ page.on('response') mtop interceptor → confirms real page load vs bot wall
  ✅ Early bot-wall detection via page title check after navigation
  ✅ Scroll-to-top retry when SKU tiles not found on first pass
  ✅ max_retries=3 with exponential + jitter sleeps between attempts
  ✅ Extra HTTP headers (Accept-Language, Accept, Sec-Fetch-*) for naturalness
  ✅ Randomised viewport size to avoid fingerprint uniformity
  ✅ Human-like mouse movement before scraping

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
import concurrent.futures
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

# Bot-wall page title keywords
_BOT_WALL_TITLES = ['verify', 'captcha', 'robot', 'security check', 'blocked',
                    'access denied', 'unusual traffic', 'are you human']


# ── Anti-block config ─────────────────────────────────────────────────────────

# Set True when running on an EU server so region rotation favours EU IPs
SERVER_IS_EU = False

REGIONS_SAFE = ["AE", "US", "AU", "CA", "PK", "SA", "TR"]
REGIONS_EU   = ["DE", "FR", "NL", "IT", "ES"]

# ── OS-matched User-Agent buckets (IMPORTANT: must match Camoufox os= param) ──
WINDOWS_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
]

MACOS_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/16.6 Safari/605.1.15",
]

# ── Proxy list (optional) ─────────────────────────────────────────────────────
# Format: "http://user:pass@host:port"  or  "http://host:port"
# Leave empty to run without proxies (expect higher block rate from servers).
PROXY_LIST: list[str] = [
    # "http://user:pass@residential-proxy-host:port",
]

MAX_RETRIES   = 3
PAGE_TIMEOUT  = 90_000   # ms — full page load budget
SKU_TIMEOUT   = 15_000   # ms — wait for SKU tiles to appear

# CSS selectors based on the confirmed AliExpress HTML
_BTN_SELECTOR     = 'button.comet-v2-btn-important'
_MENU_ITEM_SEL    = '.comet-v2-dropdown-body .comet-v2-menu-item'
_MENU_CONTENT_SEL = '.comet-v2-menu-item-content'
_SKU_ROW_SEL      = '[data-sku-row]'


# ─────────────────────────────────────────────────────────────────────────────
# FINGERPRINT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _pick_os_and_ua() -> tuple[str, str]:
    """
    Pick a random OS and a matching User-Agent string.
    Camoufox os= param must match the UA OS to avoid fingerprint mismatch.
    """
    os_choice = random.choice(['windows', 'macos'])
    ua = random.choice(
        WINDOWS_USER_AGENTS if os_choice == 'windows' else MACOS_USER_AGENTS
    )
    return os_choice, ua


def _pick_proxy() -> Optional[dict]:
    """Return a random proxy dict for Playwright context, or None."""
    if not PROXY_LIST:
        return None
    raw = random.choice(PROXY_LIST)
    return {"server": raw}


def _random_viewport() -> dict:
    """Randomise viewport slightly to avoid a uniform fingerprint."""
    width  = random.randint(1366, 1920)
    height = random.randint(768, 1080)
    return {"width": width, "height": height}


# ─────────────────────────────────────────────────────────────────────────────
# URL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _build_product_url(product_id: str) -> str:
    return f"https://www.aliexpress.com/item/{product_id}.html"


def _get_rotated_url(url: str) -> str:
    """Append a random shipFromCountry param to vary the CDN/cache path."""
    region = random.choice(REGIONS_EU if SERVER_IS_EU else REGIONS_SAFE)
    sep    = '&' if '?' in url else '?'
    print(f"[variant_scraper] Region → {region}")
    return f"{url}{sep}shipFromCountry={region}"


def _is_eu_url(url: str) -> bool:
    eu_domains = [
        'pl.aliexpress.com', 'de.aliexpress.com', 'fr.aliexpress.com',
        'it.aliexpress.com', 'es.aliexpress.com', 'nl.aliexpress.com',
    ]
    return any(d in url for d in eu_domains)


def _detect_eu_page(url: str, html_snippet: str) -> bool:
    indicators = ['gdpr', 'cookie-consent', 'Trader', 'DSA',
                  'de.aliexpress.com', 'fr.aliexpress.com']
    return any(ind in (url + html_snippet[:5000]) for ind in indicators)


def _normalise_image_url(url: str) -> str:
    if not url:
        return url
    url = _AVIF_THUMB_RE.sub('', url)
    if url.startswith('//'):
        url = 'https:' + url
    return url


# ─────────────────────────────────────────────────────────────────────────────
# ANTI-BLOCK: BOT-WALL DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def _is_bot_wall_page(page) -> bool:
    """
    Return True if the current page looks like a CAPTCHA / bot-wall.
    Checks page title and URL for known challenge indicators.
    """
    try:
        title = (page.title() or '').lower()
        url   = (page.url or '').lower()
        combined = title + ' ' + url
        if any(kw in combined for kw in _BOT_WALL_TITLES):
            print(f'[variant_scraper] ⚠ Bot wall detected — title="{page.title()}"')
            return True
    except Exception:
        pass
    return False


# ─────────────────────────────────────────────────────────────────────────────
# ANTI-BLOCK: BANNER / CONSENT DISMISSAL
# ─────────────────────────────────────────────────────────────────────────────

def _dismiss_gdpr_banner(page) -> bool:
    """Click the cookie-consent / GDPR accept button if present."""
    selectors = [
        'button:has-text("Accept All")',
        'button:has-text("Accept all cookies")',
        'button:has-text("Agree")',
        'button:has-text("I Accept")',
        '#accept-all',
        '.accept-all',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible(timeout=1_500):
                el.click(timeout=2_000)
                page.wait_for_timeout(800)
                print(f"[variant_scraper] GDPR banner dismissed via: {sel}")
                return True
        except Exception:
            continue
    return False


def _dismiss_banners(page) -> None:
    _dismiss_gdpr_banner(page)


# ─────────────────────────────────────────────────────────────────────────────
# ANTI-BLOCK: HUMAN-LIKE MOUSE MOVEMENT
# ─────────────────────────────────────────────────────────────────────────────

def _human_mouse_warmup(page) -> None:
    """
    Perform a few small random mouse movements to simulate a real user.
    Runs before interacting with any elements.
    """
    try:
        vp     = page.viewport_size or {'width': 1440, 'height': 900}
        width  = vp.get('width',  1440)
        height = vp.get('height', 900)
        for _ in range(random.randint(3, 6)):
            x = random.randint(100, width  - 100)
            y = random.randint(100, height - 100)
            page.mouse.move(x, y)
            page.wait_for_timeout(random.randint(80, 220))
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# ANTI-BLOCK: mtop RESPONSE INTERCEPTOR
# ─────────────────────────────────────────────────────────────────────────────

def _attach_mtop_interceptor(page) -> list:
    """
    Attach a response listener that captures the AliExpress mtop PDP API call.
    Returns a shared list; non-empty means the page loaded real product data
    (not a bot-wall / CAPTCHA page).
    """
    captured = []

    def _handle_response(response):
        try:
            resp_url = response.url
            if (('mtop.aliexpress.pdp.pc.query' in resp_url or
                 'pdp.pc.query' in resp_url) and response.status == 200):
                body = response.body()
                if len(body) < 500:
                    return
                text = body.decode('utf-8', errors='replace')
                if any(x in text for x in ['titleModule', 'imageModule', '"subject"',
                                            'skuModule', 'SKU_PROPERTY']):
                    captured.append(text)
                    print(f"[variant_scraper] mtop API captured ({len(text):,} bytes) ✓")
        except Exception:
            pass

    page.on('response', _handle_response)
    return captured


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
            key      = alt.lower() or src
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

def _has_country_size_dropdown(page) -> bool:
    """Return True if any visible button has text matching 'Size(XX)'."""
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
    Returns e.g. ["Default", "EU", "US", "ES", "FR", "UK", "DE", ...]
    """
    countries = []
    try:
        items = page.locator(_MENU_ITEM_SEL).all()
        for item in items:
            try:
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
    or until timeout_ms elapses.
    """
    deadline = time.time() + timeout_ms / 1_000
    while time.time() < deadline:
        html    = page.content()
        soup    = BeautifulSoup(html, 'html.parser')
        current = _scrape_size_labels_from_soup(soup)
        if current and current != previous_labels:
            return current
        time.sleep(0.3)
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
    if not _open_dropdown(page):
        print('[variant_scraper] Could not open dropdown — falling back to plain parse')
        return _parse_plain_size_variants(page.content())

    countries = _read_country_list(page)
    print(f'[variant_scraper] Dropdown countries: {countries}')

    if not countries:
        try:
            page.keyboard.press('Escape')
        except Exception:
            pass
        return _parse_plain_size_variants(page.content())

    try:
        page.keyboard.press('Escape')
        page.wait_for_timeout(400)
    except Exception:
        pass

    systems        = []
    seen_countries = set()

    baseline_soup   = BeautifulSoup(page.content(), 'html.parser')
    previous_labels = _scrape_size_labels_from_soup(baseline_soup)

    for country in countries:
        norm = country.strip()
        if not norm or norm in seen_countries:
            continue
        seen_countries.add(norm)

        print(f'[variant_scraper] Selecting: {norm}')

        if not _open_dropdown(page):
            print(f'[variant_scraper]   Could not reopen dropdown for {norm}, skipping')
            continue

        page.wait_for_timeout(400)

        clicked = _click_country_in_open_dropdown(page, norm)
        if not clicked:
            print(f'[variant_scraper]   Could not click "{norm}", skipping')
            try:
                page.keyboard.press('Escape')
            except Exception:
                pass
            continue

        page.wait_for_timeout(300)
        labels = _wait_for_sizes_to_refresh(page, previous_labels, timeout_ms=4_000)

        print(f'[variant_scraper]   {norm} → {labels}')

        if labels:
            detected_country = _detect_country_from_labels(labels)
            systems.append({
                'country': detected_country or norm,
                'options': labels,
            })
            previous_labels = labels

    if not systems:
        return {'type': 'plain', 'systems': [], 'plain_options': []}

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
# ANTI-BLOCK: CORE BROWSER SESSION  (runs inside ThreadPoolExecutor)
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_in_thread(product_id: str, url: str) -> dict:
    """
    Single browser session wrapped so it can be killed by a ThreadPoolExecutor
    timeout.  Returns dict with keys: color_variants, size_variants,
    mtop_captured (bool), bot_wall (bool).
    """
    from camoufox.sync_api import Camoufox

    os_choice, ua = _pick_os_and_ua()
    proxy         = _pick_proxy()
    is_eu         = _is_eu_url(url)

    color_variants: list = []
    size_variants:  dict = {'type': 'plain', 'systems': [], 'plain_options': []}
    mtop_captured         = False
    bot_wall              = False

    try:
        with Camoufox(headless=True, os=os_choice) as browser:
            ctx_kwargs = dict(
                viewport=_random_viewport(),
                locale='en-US',
                user_agent=ua,
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept':          'text/html,application/xhtml+xml,application/xml;'
                                       'q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Sec-Fetch-Dest':  'document',
                    'Sec-Fetch-Mode':  'navigate',
                    'Sec-Fetch-Site':  'none',
                    'Sec-Fetch-User':  '?1',
                },
            )
            if proxy:
                ctx_kwargs['proxy'] = proxy
                print(f'[variant_scraper] Using proxy: {proxy["server"]}')

            ctx  = browser.new_context(**ctx_kwargs)
            page = ctx.new_page()

            # ── Anti-block: attach mtop interceptor ───────────────────────
            captured_api = _attach_mtop_interceptor(page)

            # ── Navigate ──────────────────────────────────────────────────
            page.goto(url, timeout=PAGE_TIMEOUT, wait_until='domcontentloaded')

            # ── Anti-block: early bot-wall check ──────────────────────────
            if _is_bot_wall_page(page):
                bot_wall = True
                page.close()
                ctx.close()
                return {
                    'color_variants': color_variants,
                    'size_variants':  size_variants,
                    'mtop_captured':  False,
                    'bot_wall':       True,
                }

            # ── Anti-block: handle EU / GDPR consent ─────────────────────
            detected_eu = _detect_eu_page(page.url, page.content()[:5000]) or is_eu
            if detected_eu:
                page.wait_for_timeout(2_000)
                _dismiss_banners(page)
                page.wait_for_timeout(1_000)

            # ── Human-like mouse warmup ───────────────────────────────────
            _human_mouse_warmup(page)

            # ── Wait for SKU tiles ────────────────────────────────────────
            sku_found = False
            try:
                page.wait_for_selector(_SKU_ROW_SEL, timeout=SKU_TIMEOUT)
                sku_found = True
            except Exception:
                print(f'[variant_scraper] SKU selector not found on first pass')

            # ── Gentle scroll to trigger lazy-loaded content ──────────────
            for _ in range(3):
                page.mouse.wheel(0, random.randint(300, 600))
                page.wait_for_timeout(random.randint(400, 700))
            page.wait_for_timeout(1_500)

            # ── Anti-block: scroll-to-top retry ──────────────────────────
            if not sku_found or not captured_api:
                print('[variant_scraper] Scroll-to-top retry...')
                page.mouse.wheel(0, -9_999)
                page.wait_for_timeout(3_000)
                page.mouse.wheel(0, 600)
                page.wait_for_timeout(2_000)
                if not sku_found:
                    try:
                        page.wait_for_selector(_SKU_ROW_SEL, timeout=8_000)
                        sku_found = True
                    except Exception:
                        print('[variant_scraper] SKU tiles still absent after retry')

            # ── Second bot-wall check after full load ─────────────────────
            if _is_bot_wall_page(page):
                bot_wall = True
                page.close()
                ctx.close()
                return {
                    'color_variants': color_variants,
                    'size_variants':  size_variants,
                    'mtop_captured':  False,
                    'bot_wall':       True,
                }

            mtop_captured = bool(captured_api)
            print(f'[variant_scraper] mtop captured: {mtop_captured} | '
                  f'SKU found: {sku_found}')

            # ── Colors (pure HTML parse, no interaction) ──────────────────
            soup           = BeautifulSoup(page.content(), 'html.parser')
            color_variants = _parse_color_variants(soup)

            # ── Sizes ─────────────────────────────────────────────────────
            if _has_country_size_dropdown(page):
                print('[variant_scraper] Country dropdown detected → iterating all countries')
                size_variants = _scrape_all_country_sizes(page)
            else:
                print('[variant_scraper] No country dropdown → plain size parse')
                size_variants = _parse_plain_size_variants(page.content())

            page.close()
            ctx.close()

    except Exception as e:
        print(f'[variant_scraper] Browser error: {e}')
        import traceback
        traceback.print_exc()

    return {
        'color_variants': color_variants,
        'size_variants':  size_variants,
        'mtop_captured':  mtop_captured,
        'bot_wall':       bot_wall,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RETRY SLEEP  (exponential backoff + jitter)
# ─────────────────────────────────────────────────────────────────────────────

def _retry_sleep(attempt: int, bot_wall: bool) -> None:
    """
    Sleep between retries.
    - Normal failure:   8–20 s (short — may just be a transient load issue)
    - Bot-wall hit:     45–90 s (long — give AliExpress rate-limiter time to cool)
    Exponential multiplier doubles each attempt.
    """
    if bot_wall:
        base  = random.uniform(45, 90)
    else:
        base  = random.uniform(8, 20)
    sleep = base * (1.5 ** (attempt - 1))
    sleep = min(sleep, 180)   # hard cap at 3 min
    print(f'[variant_scraper] Sleeping {sleep:.1f}s before retry '
          f'({"bot-wall" if bot_wall else "soft-fail"} back-off)...')
    time.sleep(sleep)


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER-BASED SCRAPER  (public API)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_product_variants(product_id: str, max_retries: int = MAX_RETRIES) -> dict:
    """
    Full browser scrape of an AliExpress product page with anti-blocking.

    Anti-blocking flow per attempt:
      1. Rotate shipFromCountry param on URL
      2. OS-matched random User-Agent (no fingerprint mismatch)
      3. Camoufox os= matches chosen UA OS
      4. Randomised viewport dimensions
      5. Optional residential proxy (configure PROXY_LIST above)
      6. Rich extra HTTP headers (Sec-Fetch-*, Accept)
      7. Attach mtop interceptor (detect bot wall vs real page)
      8. Early bot-wall detection via page title after navigation
      9. Human-like mouse movement before interacting
     10. Dismiss GDPR / cookie banners (EU pages)
     11. Scroll-to-top retry when SKU tiles absent on first pass
     12. Second bot-wall check after full page load
     13. ThreadPoolExecutor timeout = 200 s to prevent zombie hangs
     14. Exponential + jitter sleep between retries
         (short for soft fails, long for confirmed bot-walls)

    Args:
        product_id:  AliExpress numeric product ID.
        max_retries: Retry attempts on failure (default 3).

    Returns:
        Variant dict per the module-level JSON contract.
        On complete failure returns empty structure + 'error' key.
    """
    base_url = _build_product_url(product_id)
    empty    = {
        'product_id': product_id,
        'variants':   {
            'color': [],
            'size':  {'type': 'plain', 'systems': [], 'plain_options': []},
        },
    }

    for attempt in range(1, max_retries + 1):
        print(f'\n[variant_scraper] ══ Attempt {attempt}/{max_retries} — product {product_id} ══')

        url = _get_rotated_url(base_url)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future        = executor.submit(_scrape_in_thread, product_id, url)
                thread_result = future.result(timeout=200)

        except concurrent.futures.TimeoutError:
            print(f'[variant_scraper] Attempt {attempt} timed out (200 s)')
            if attempt < max_retries:
                _retry_sleep(attempt, bot_wall=False)
            continue
        except Exception as e:
            print(f'[variant_scraper] Attempt {attempt} executor error: {e}')
            if attempt < max_retries:
                _retry_sleep(attempt, bot_wall=False)
            continue

        color_variants = thread_result['color_variants']
        size_variants  = thread_result['size_variants']
        mtop_captured  = thread_result['mtop_captured']
        is_bot_wall    = thread_result.get('bot_wall', False)

        result = {
            'product_id': product_id,
            'variants':   {'color': color_variants, 'size': size_variants},
        }

        has_colors = bool(color_variants)
        has_sizes  = bool(
            size_variants.get('plain_options') or
            size_variants.get('systems')
        )

        # Confirmed bot-wall → long sleep before retry
        if is_bot_wall:
            print(f'[variant_scraper] ✗ Confirmed bot-wall on attempt {attempt}')
            if attempt < max_retries:
                _retry_sleep(attempt, bot_wall=True)
            continue

        # mtop not captured + no variants → likely soft bot-wall
        if not mtop_captured and not has_colors and not has_sizes:
            print(f'[variant_scraper] Bot wall suspected (no mtop + no variants) '
                  f'on attempt {attempt}')
            if attempt < max_retries:
                _retry_sleep(attempt, bot_wall=True)
            continue

        if has_colors or has_sizes:
            n_systems = len(size_variants.get('systems', []))
            countries = [s['country'] for s in size_variants.get('systems', [])]
            print(
                f'[variant_scraper] ✓ {len(color_variants)} colors | '
                f'size type={size_variants["type"]} | '
                f'{n_systems} country system(s): {countries}'
            )
            return result

        print(f'[variant_scraper] No variants on attempt {attempt}')
        if attempt < max_retries:
            _retry_sleep(attempt, bot_wall=False)

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
