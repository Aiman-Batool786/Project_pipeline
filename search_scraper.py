"""
search_scraper.py
AliExpress Search Results Scraper
Extracts product URLs, IDs, and titles with pagination support
"""

import re
import time
import random
from typing import List, Dict, Optional
from camoufox.sync_api import Camoufox
from bs4 import BeautifulSoup

# Configuration
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


def extract_product_id_from_url(url: str) -> Optional[str]:
    """Extract product ID from URL - supports /item/ format only"""
    patterns = [
        r'/item/(\d+)\.html',  # Matches: /item/1005009980609725.html
        r'/item/(\d+)',         # Matches: /item/1005009980609725
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def extract_products_from_html(html: str) -> List[Dict]:
    """
    Extract products from search result HTML
    Returns list of dicts with product_id, product_url, title
    """
    products = []
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find all product cards - based on your HTML structure
    product_cards = soup.select('[data-tticheck="true"] .lw_b, .search-card-item')
    
    for card in product_cards:
        # Find the product link
        link = card.get('href') or (card.find('a') and card.find('a').get('href'))
        if not link:
            continue
        
        # Ensure full URL
        if link.startswith('//'):
            link = 'https:' + link
        
        # Extract product ID from URL - ONLY accept /item/ format
        product_id = extract_product_id_from_url(link)
        
        # Skip if not a valid /item/ URL
        if not product_id:
            continue
        
        # Verify URL has correct format (reject /srs/ URLs)
        if '/item/' not in link:
            continue
        
        # Extract title from your HTML structure
        title = ''
        title_elem = card.select_one('.lw_k4, .item-title, [class*="title"]')
        if title_elem:
            title = title_elem.get_text(strip=True)
            # Clean up font tags and extra spaces
            title = re.sub(r'<font[^>]*>.*?</font>', '', title, flags=re.DOTALL)
            title = re.sub(r'\s+', ' ', title).strip()
        else:
            # Alternative title selector from your HTML
            title_elem = card.select_one('h3, [role="heading"]')
            if title_elem:
                title = title_elem.get_text(strip=True)
                title = re.sub(r'\s+', ' ', title).strip()
        
        # Skip if no title
        if not title:
            continue
        
        products.append({
            'product_id': product_id,
            'product_url': f"https://pl.aliexpress.com/item/{product_id}.html",
            'title': title
        })
    
    # Alternative approach if the above didn't find anything
    if not products:
        product_links = soup.select('a[href*="/item/"]')
        for link in product_links:
            href = link.get('href', '')
            if href.startswith('//'):
                href = 'https:' + href
            
            product_id = extract_product_id_from_url(href)
            if product_id and '/item/' in href:
                # Find title by looking at parent elements
                title = ''
                parent = link.find_parent()
                if parent:
                    title_elem = parent.select_one('h3, [class*="title"], [role="heading"]')
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        title = re.sub(r'\s+', ' ', title).strip()
                
                if title:
                    products.append({
                        'product_id': product_id,
                        'product_url': f"https://pl.aliexpress.com/item/{product_id}.html",
                        'title': title
                    })
    
    return products


def find_next_page_url(html: str, current_url: str) -> Optional[str]:
    """Find next page URL from pagination"""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Look for next page button
    next_selectors = [
        '.comet-pagination-next a',
        '.comet-pagination-next button',
        'a[rel="next"]',
        '.pagination-next a',
        '[aria-label="Next page"]',
        '.next-page a',
    ]
    
    for selector in next_selectors:
        next_elem = soup.select_one(selector)
        if next_elem:
            href = next_elem.get('href')
            if href:
                if href.startswith('//'):
                    href = 'https:' + href
                return href
    
    # Check if pagination shows "Next" button is disabled
    disabled_next = soup.select_one('.comet-pagination-next.comet-pagination-disabled')
    if disabled_next:
        return None
    
    # Try to extract page number from URL and increment
    page_match = re.search(r'[?&]page=(\d+)', current_url)
    if page_match:
        current_page = int(page_match.group(1))
        next_page = current_page + 1
        
        if '?' in current_url:
            next_url = re.sub(r'page=\d+', f'page={next_page}', current_url)
        else:
            next_url = current_url + f'?page={next_page}'
        
        return next_url
    
    return None


def scrape_search_page(url: str, delay: float = 1.0) -> Dict:
    """Scrape a single search page and return products and HTML"""
    result = {
        'products': [],
        'html': '',
        'next_url': None,
        'has_more': False,
        'url': url,
        'success': False,
        'error': None
    }
    
    ua = random.choice(USER_AGENTS)
    
    try:
        with Camoufox(headless=True, os='windows') as browser:
            context = browser.new_context(
                viewport={'width': 1440, 'height': 900},
                locale='en-US',
                user_agent=ua,
                extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'}
            )
            page = context.new_page()
            
            print(f"[search_scraper] 📄 Loading: {url}")
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            
            # Wait for product cards to load
            time.sleep(delay)
            
            # Scroll to load lazy-loaded content
            for _ in range(3):
                page.mouse.wheel(0, random.randint(400, 800))
                time.sleep(random.uniform(0.5, 1.0))
            
            # Get page HTML
            html = page.content()
            result['html'] = html
            
            # Extract products
            products = extract_products_from_html(html)
            result['products'] = products
            
            # Find next page
            next_url = find_next_page_url(html, url)
            result['next_url'] = next_url
            result['has_more'] = next_url is not None
            
            result['success'] = True
            print(f"[search_scraper] ✅ Found {len(products)} products")
            
            page.close()
            context.close()
            
    except Exception as e:
        print(f"[search_scraper] ❌ Error: {e}")
        result['error'] = str(e)
    
    return result


def scrape_search_results(search_url: str, max_pages: Optional[int] = None, delay: float = 1.0) -> List[Dict]:
    """
    Scrape all pages of search results
    Returns list of unique products (deduplicated by product_id)
    """
    all_products = []
    seen_ids = set()
    current_url = search_url
    page_num = 1
    
    print(f"\n{'='*60}")
    print(f"🔍 Starting search scrape")
    print(f"   URL: {search_url}")
    print(f"   Max pages: {max_pages if max_pages else 'unlimited'}")
    print(f"{'='*60}\n")
    
    while current_url:
        print(f"📄 Scraping page {page_num}...")
        
        result = scrape_search_page(current_url, delay)
        
        if not result['success']:
            print(f"   ❌ Failed: {result['error']}")
            break
        
        # Add new products
        new_count = 0
        for product in result['products']:
            pid = product['product_id']
            if pid not in seen_ids:
                seen_ids.add(pid)
                all_products.append(product)
