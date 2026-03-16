"""
debug_scraper.py
Run this directly on your server:
  python3 debug_scraper.py

It will print ALL network requests and the first 2000 chars of each response
so we can see exactly what AliExpress sends and find the specs.
"""

from playwright.sync_api import sync_playwright
import re
import json

TEST_URL = "https://www.aliexpress.com/item/1005010716013669.html"

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 900},
            locale="en-US",
        )
        page = context.new_page()

        all_responses = []

        def on_response(response):
            url = response.url
            # Skip images, fonts, css
            if any(x in url for x in ['.png', '.jpg', '.gif', '.css', '.woff', '.svg']):
                return
            try:
                body = response.body()
                if len(body) > 200:
                    text = body.decode('utf-8', errors='replace')
                    all_responses.append({
                        'url': url,
                        'status': response.status,
                        'size': len(body),
                        'text': text,
                    })
            except Exception:
                pass

        page.on('response', on_response)

        print(f"Loading: {TEST_URL}")
        page.goto(TEST_URL, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_timeout(5000)

        # Scroll to trigger lazy loads
        for y in [1000, 3000, 6000]:
            page.mouse.wheel(0, y)
            page.wait_for_timeout(1000)

        page.wait_for_timeout(3000)
        browser.close()

    print(f"\n{'='*80}")
    print(f"TOTAL RESPONSES CAPTURED: {len(all_responses)}")
    print(f"{'='*80}\n")

    # Show all JSON responses sorted by size
    json_responses = []
    for r in all_responses:
        try:
            json.loads(r['text'].split('(', 1)[-1].rstrip(');'))
            json_responses.append(r)
        except Exception:
            pass

    print(f"JSON responses: {len(json_responses)}")
    print()

    # Sort by size descending — biggest likely has most data
    json_responses.sort(key=lambda x: x['size'], reverse=True)

    for i, r in enumerate(json_responses[:15]):
        print(f"--- Response #{i+1} ---")
        print(f"  URL    : {r['url'][:120]}")
        print(f"  Status : {r['status']}")
        print(f"  Size   : {r['size']:,} bytes")

        # Check if it contains spec-like content
        text_lower = r['text'].lower()
        has_specs = any(kw in text_lower for kw in [
            'attrname', 'attrvalue', 'specifications', 'specification',
            'brand name', 'origin', 'material', 'dimensions', 'weight',
            'prop', 'property', 'attribute'
        ])
        print(f"  HasSpec: {has_specs}")

        # Show first 500 chars
        print(f"  Preview: {r['text'][:500]}")
        print()

    # Find the response most likely containing specs
    print(f"\n{'='*80}")
    print("SEARCHING FOR SPEC DATA IN ALL RESPONSES...")
    print(f"{'='*80}\n")

    for r in all_responses:
        text_lower = r['text'].lower()
        if 'attrname' in text_lower or ('brand name' in text_lower and 'origin' in text_lower):
            print(f"✅ FOUND SPEC DATA IN:")
            print(f"   URL: {r['url']}")
            print(f"   Size: {r['size']:,} bytes")
            # Find the section with specs
            idx = r['text'].lower().find('attrname')
            if idx == -1:
                idx = r['text'].lower().find('brand name')
            start = max(0, idx - 200)
            print(f"   Context around specs:")
            print(r['text'][start:start+1000])
            print()

    # Also dump the page HTML to check spec section
    print(f"\n{'='*80}")
    print("CHECKING PAGE DOM FOR SPEC SECTION...")
    print(f"{'='*80}")
    print("(Re-run needed for DOM check — see below)")

if __name__ == "__main__":
    run()
