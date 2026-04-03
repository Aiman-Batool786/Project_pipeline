#!/usr/bin/env python3
"""
AliExpress Product Detail Page (PDP) Scraper with Multi-Region Support
Supports Pakistan (PK) and Poland (PL/EU) regions with fallback parsing
"""

import json
import re
import time
import random
import logging
import requests
from typing import Dict, Optional, List, Any
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# Try to import optional dependencies
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 2
TIMEOUT = 30

# Supported regions
REGIONS = {
    "US": "United States", "GB": "United Kingdom", "DE": "Germany", "FR": "France",
    "AE": "UAE", "AU": "Australia", "CA": "Canada", "PK": "Pakistan", "PL": "Poland"
}

# User agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

# API endpoints
REGIONAL_API_ENDPOINTS = {
    "default": ["mtop.aliexpress.pdp.detail", "mtop.aliexpress.product.detail", "mtop.aliexpress.pdp.v2"],
    "EU": ["mtop.aliexpress.gdpr.product.detail", "mtop.aliexpress.eu.pdp", "mtop.aliexpress.product.eu.detail"]
}

# EU field mappings
EU_FIELD_MAPPINGS = {
    "title": ["subject", "title", "productTitle", "euProductName", "gdprProductName"],
    "price": ["price", "salePrice", "euPrice", "gdprPrice"],
    "original_price": ["originalPrice", "marketPrice", "euOriginalPrice"],
    "discount": ["discount", "discountRate", "euDiscount"],
    "seller": ["storeName", "sellerName", "euStoreInfo", "euSellerName"],
    "seller_id": ["storeId", "sellerId", "euStoreId", "euSellerId"],
    "rating": ["averageStarRate", "rating", "euRating"],
    "reviews": ["reviewCount", "reviews", "euReviewCount"],
    "orders": ["orders", "tradeCount", "euOrders"],
    "specifications": ["productPropDtos", "specifications", "euProductSpecs", "gdprSpecifications"],
    "images": ["images", "imagePathList", "euImagePathList", "gdprImages"],
    "description_url": ["descriptionUrl", "description", "euDescriptionUrl", "gdprDescriptionUrl"],
    "bullet_points": ["bulletPoints", "highlights", "euBulletPoints"]
}

def get_region_from_url(url: str) -> str:
    """Extract region from URL or return default"""
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    
    if 'shipFromCountry' in query_params:
        region = query_params['shipFromCountry'][0].upper()
        if region in REGIONS:
            return region
    
    return "US"

def add_region_to_url(url: str, region: str) -> str:
    """Add or update region parameter in URL"""
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    query_params['shipFromCountry'] = [region.upper()]
    new_query = urlencode(query_params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))

def get_random_user_agent() -> str:
    """Get random user agent"""
    return random.choice(USER_AGENTS)

def clean_text(text: str) -> str:
    """Clean and normalize text"""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text.strip())

def extract_price(text: str) -> Dict[str, Any]:
    """Extract price information from text"""
    if not text:
        return {"price": 0.0, "currency": "USD"}
    
    price_match = re.search(r'(\d+(?:\.\d{1,2})?)', text.replace(',', ''))
    currency_match = re.search(r'([A-Z]{3}|\$|€|£|₹|PKR|PLN)', text)
    
    price = float(price_match.group(1)) if price_match else 0.0
    currency = currency_match.group(1) if currency_match else "USD"
    
    currency_map = {'$': 'USD', '€': 'EUR', '£': 'GBP', '₹': 'INR', 'PKR': 'PKR', 'PLN': 'PLN'}
    currency = currency_map.get(currency, currency)
    
    return {"price": price, "currency": currency}

def _extract_field_with_fallback(data: Dict, field_names: List[str]) -> Any:
    """Extract field value trying multiple possible field names"""
    for field_name in field_names:
        if field_name in data:
            return data[field_name]
        if '.' in field_name:
            parts = field_name.split('.')
            current = data
            try:
                for part in parts:
                    current = current[part]
                return current
            except (KeyError, TypeError):
                continue
    return None

def _extract_price_with_fallback(data: Dict, price_fields: List[str], original_price_fields: List[str]) -> Dict[str, float]:
    """Extract price information with fallback logic"""
    result = {"price": 0.0, "original_price": 0.0, "discount": 0, "currency": "USD"}
    
    for field in price_fields:
        if field in data and data[field]:
            price_data = extract_price(str(data[field]))
            result.update(price_data)
            break
    
    for field in original_price_fields:
        if field in data and data[field]:
            orig_data = extract_price(str(data[field]))
            result["original_price"] = orig_data["price"]
            break
    
    if result["price"] and result["original_price"] and result["original_price"] > result["price"]:
        result["discount"] = round((1 - result["price"] / result["original_price"]) * 100)
    
    return result

def _extract_seller_info(data: Dict, region: str) -> Dict[str, Any]:
    """Extract seller information with region-specific handling"""
    result = {}
    
    seller_fields = EU_FIELD_MAPPINGS['seller']
    seller_id_fields = EU_FIELD_MAPPINGS['seller_id']
    
    # For EU regions, check for euStoreInfo structure
    if region in ["PL", "DE", "FR"] or 'euStoreInfo' in data:
        eu_store = data.get('euStoreInfo', {})
        if eu_store:
            result['seller'] = eu_store.get('storeName', '')
            result['seller_id'] = eu_store.get('storeId', '')
            result['seller_rating'] = eu_store.get('rating', 0)
            result['seller_reviews'] = eu_store.get('reviewCount', 0)
            return result
    
    # Standard seller extraction
    for field in seller_fields:
        if field in data and data[field]:
            result['seller'] = str(data[field])
            break
    
    for field in seller_id_fields:
        if field in data and data[field]:
            result['seller_id'] = str(data[field])
            break
    
    return result

def _extract_specifications(data: Dict, region: str) -> List[Dict[str, str]]:
    """Extract product specifications with region-specific handling"""
    specs = []
    
    # Try EU-specific specification extraction
    if region in ["PL", "DE", "FR"]:
        eu_specs = data.get('gdprModule', {}).get('specifications', [])
        if eu_specs:
            for spec in eu_specs:
                if isinstance(spec, dict):
                    specs.append({'name': spec.get('name', ''), 'value': spec.get('value', '')})
            return specs
        
        # Try euProductSpecs
        eu_product_specs = data.get('euProductSpecs', [])
        if eu_product_specs:
            for spec in eu_product_specs:
                if isinstance(spec, dict):
                    specs.append({'name': spec.get('name', ''), 'value': spec.get('value', '')})
            return specs
    
    # Standard specification extraction
    spec_fields = EU_FIELD_MAPPINGS['specifications']
    for field in spec_fields:
        if field in data and data[field]:
            spec_data = data[field]
            if isinstance(spec_data, list):
                specs = [{'name': str(item.get('name', '')), 'value': str(item.get('value', ''))} 
                        for item in spec_data if isinstance(item, dict)]
            break
    
    return specs

def _extract_images(data: Dict, region: str) -> List[str]:
    """Extract product images with region-specific handling"""
    images = []
    
    # Try EU-specific image extraction
    if region in ["PL", "DE", "FR"]:
        # Try euImagePathList
        eu_images = data.get('euImagePathList', [])
        if eu_images:
            return [str(img) for img in eu_images if img]
        
        # Try gdprImages
        gdpr_images = data.get('gdprImages', [])
        if gdpr_images:
            return [str(img) for img in gdpr_images if img]
    
    # Standard image extraction
    image_fields = EU_FIELD_MAPPINGS['images']
    for field in image_fields:
        if field in data and data[field]:
            img_data = data[field]
            if isinstance(img_data, list):
                images = [str(img) for img in img_data if img]
            elif isinstance(img_data, str):
                images = [img_data]
            break
    
    return images

def _extract_bullet_points(data: Dict, region: str) -> List[str]:
    """Extract bullet points with region-specific handling"""
    bullets = []
    
    # Try EU-specific bullet extraction
    if region in ["PL", "DE", "FR"]:
        eu_bullets = data.get('gdprModule', {}).get('bulletPoints', [])
        if eu_bullets:
            return [str(bullet) for bullet in eu_bullets if bullet]
        
        # Try euBulletPoints
        eu_bullets = data.get('euBulletPoints', [])
        if eu_bullets:
            return [str(bullet) for bullet in eu_bullets if bullet]
    
    # Standard bullet extraction
    bullet_fields = EU_FIELD_MAPPINGS['bullet_points']
    for field in bullet_fields:
        if field in data and data[field]:
            bullet_data = data[field]
            if isinstance(bullet_data, list):
                bullets = [str(bullet) for bullet in bullet_data if bullet]
            break
    
    return bullets

def _parse_init_data(html: str) -> Dict[str, Any]:
    """Parse _init_data_ from HTML as fallback"""
    result = {}
    
    # Look for _init_data_ in script tags
    init_data_match = re.search(r'window\._init_data_\s*=\s*({.+?});', html, re.DOTALL)
    if init_data_match:
        try:
            init_data = json.loads(init_data_match.group(1))
            
            # Extract basic info
            if 'title' in init_data:
                result['title'] = init_data['title']
            if 'price' in init_data:
                result['price'] = init_data['price']
            if 'images' in init_data:
                result['images'] = init_data['images']
            
            logger.info("Parsed _init_data_ successfully")
        except json.JSONDecodeError:
            logger.error("Failed to parse _init_data_ JSON")
    
    return result

def _parse_eu_fallback(html: str, url: str) -> Dict[str, Any]:
    """Parse EU-specific page structure as fallback"""
    result = {}
    
    if not BS4_AVAILABLE:
        logger.warning("BeautifulSoup4 not available, using regex fallback")
        # Simple regex-based parsing without BeautifulSoup
        title_match = re.search(r'<title>([^<]+)</title>', html, re.IGNORECASE)
        if title_match:
            result['title'] = title_match.group(1).split(' - ')[0].strip()
        
        price_match = re.search(r'\$(\d+(?:\.\d{1,2})?)', html)
        if price_match:
            result['price'] = {"price": float(price_match.group(1)), "currency": "USD"}
        
        return result
    
    # Use BeautifulSoup if available
    soup = BeautifulSoup(html, 'html.parser')
    
    # EU pages have different selectors
    try:
        # Title - EU pages often use different title selectors
        title_selectors = [
            'h1[data-e2e="product-title"]',
            '.product-title',
            'h1[class*="title"]',
            'title'
        ]
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                result['title'] = clean_text(title_elem.get_text())
                break
        
        # Price - EU pages have GDPR-compliant price display
        price_selectors = [
            '[data-e2e="price"]',
            '.price-current',
            '[class*="price"][class*="current"]',
            '.notranslate'
        ]
        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                price_text = clean_text(price_elem.get_text())
                result['price'] = extract_price(price_text)
                break
        
        # Seller info - EU pages have GDPR-compliant seller info
        seller_selectors = [
            '[data-e2e="store-name"]',
            '.store-name',
            '[class*="seller"][class*="name"]'
        ]
        for selector in seller_selectors:
            seller_elem = soup.select_one(selector)
            if seller_elem:
                result['seller'] = clean_text(seller_elem.get_text())
                break
        
        # Images - EU pages may have different image galleries
        image_selectors = [
            'img[data-e2e="product-image"]',
            '.product-image img',
            'img[class*="gallery"]'
        ]
        images = []
        for selector in image_selectors:
            img_elems = soup.select(selector)
            for img in img_elems:
                src = img.get('src') or img.get('data-src')
                if src and 'placeholder' not in src:
                    images.append(src)
        if images:
            result['images'] = images[:10]  # Limit to first 10
        
        logger.info(f"Parsed EU fallback for {url}: {len(result)} fields")
        
    except Exception as e:
        logger.error(f"EU fallback parsing failed for {url}: {e}")
    
    return result

def fetch_description(url: str) -> str:
    """Fetch and clean product description"""
    if not url:
        return ""
    
    try:
        headers = {'User-Agent': get_random_user_agent()}
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        
        if not BS4_AVAILABLE:
            # Simple text extraction without BeautifulSoup
            text = re.sub(r'<[^>]+>', '', response.text)
            return text[:3000] if len(text) > 3000 else text
        
        # Clean HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Remove script and style elements
        for elem in soup(['script', 'style', 'noscript']):
            elem.decompose()
        
        # Get text and clean
        text = soup.get_text()
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        description = '\n'.join(lines)
        
        # Limit length
        if len(description) > 3000:
            description = description[:3000] + "..."
        
        return description
        
    except Exception as e:
        logger.error(f"Failed to fetch description from {url}: {e}")
        return ""

def _scrape_with_playwright(url: str) -> Dict[str, Any]:
    """Scrape using Playwright with region detection"""
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright not available, using requests fallback")
        return _scrape_with_requests(url)
    
    region = get_region_from_url(url)
    logger.info(f"Scraping URL for region {region}: {url}")
    
    captured_data = {}
    html_content = ""
    
    try:
        from playwright.sync_api import sync_playwright
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=get_random_user_agent(),
                viewport={'width': 1920, 'height': 1080}
            )
            
            page = context.new_page()
            
            # Intercept network requests to capture API responses
            def handle_response(response):
                try:
                    # Check for regional API endpoints
                    all_endpoints = REGIONAL_API_ENDPOINTS["default"] + REGIONAL_API_ENDPOINTS["EU"]
                    
                    for endpoint in all_endpoints:
                        if endpoint in response.url:
                            if response.status == 200:
                                text = response.text()
                                if len(text) > 100:  # Only capture substantial responses
                                    parsed = parse_pdp_response(text, region)
                                    if parsed and 'title' in parsed:
                                        captured_data.update(parsed)
                                        logger.info(f"Captured API response from {endpoint}")
                            break
                except Exception as e:
                    logger.error(f"Error handling response: {e}")
            
            page.on("response", handle_response)
            
            try:
                page.goto(url, wait_until='networkidle', timeout=TIMEOUT * 1000)
                
                # Scroll to trigger lazy loading
                page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                page.wait_for_timeout(2000)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(2000)
                
                html_content = page.content()
                
            except Exception as e:
                logger.error(f"Playwright navigation error: {e}")
            
            browser.close()
    except ImportError:
        logger.error("Playwright not available")
        return _scrape_with_requests(url)
    
    return {
        'captured': captured_data,
        'html': html_content
    }

def _scrape_with_requests(url: str) -> Dict[str, Any]:
    """Fallback scraping using requests library"""
    logger.info(f"Using requests fallback for: {url}")
    
    try:
        headers = {'User-Agent': get_random_user_agent()}
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        
        return {
            'captured': {},
            'html': response.text
        }
    except Exception as e:
        logger.error(f"Requests fallback failed: {e}")
        return {'captured': {}, 'html': ''}

def get_product_info(url: str) -> Optional[Dict[str, Any]]:
    """Get comprehensive product information with region support"""
    
    # Ensure region is in URL
    if 'shipFromCountry=' not in url:
        region = get_region_from_url(url)
        url = add_region_to_url(url, region)
    
    region = get_region_from_url(url)
    logger.info(f"Getting product info for region {region}")
    
    result = {}
    
    # Try scraping with Playwright or requests
    scrape_result = _scrape_with_playwright(url)
    
    # Use captured API data if available
    if scrape_result['captured']:
        result.update(scrape_result['captured'])
        logger.info("Using captured API data")
    
    # If no title from API, try fallback parsing
    if not result.get('title'):
        logger.info("No title from API, trying fallbacks...")
        
        # Try _init_data_ parsing
        init_data = _parse_init_data(scrape_result['html'])
        if init_data.get('title'):
            result.update(init_data)
            logger.info("Got title from _init_data_")
        
        # If still no title, try EU fallback
        if not result.get('title') and region in ["PL", "DE", "FR"]:
            logger.info("Trying EU fallback parsing...")
            eu_fallback = _parse_eu_fallback(scrape_result['html'], url)
            if eu_fallback.get('title'):
                result.update(eu_fallback)
                logger.info("Got title from EU fallback")
        
        # Final fallback to meta tags
        if not result.get('title'):
            title_match = re.search(r'<title>([^<]+)</title>', scrape_result['html'], re.IGNORECASE)
            if title_match:
                result['title'] = title_match.group(1).split(' - ')[0]
                logger.info("Got title from og:meta")
    
    # Fetch description if we have URL
    if result.get('description_url'):
        logger.info("Fetching description...")
        result['description'] = fetch_description(result['description_url'])
    
    # Ensure all expected fields exist
    defaults = {
        'title': '',
        'price': {'price': 0.0, 'currency': 'USD'},
        'original_price': 0.0,
        'discount': 0,
        'seller': '',
        'seller_id': '',
        'seller_rating': 0,
        'seller_reviews': 0,
        'rating': 0,
        'reviews': 0,
        'orders': 0,
        'specifications': [],
        'images': [],
        'bullet_points': [],
        'description': '',
        'description_url': '',
        'region': region
    }
    
    # Apply defaults
    for key, default_value in defaults.items():
        if key not in result:
            result[key] = default_value
    
    # Ensure image fields exist
    for i in range(1, 21):
        key = f'image_{i}'
        if key not in result:
            result[key] = ''
    
    # Fill image fields from images list
    if result['images'] and isinstance(result['images'], list):
        for i, img in enumerate(result['images'][:20], 1):
            result[f'image_{i}'] = str(img)
    
    logger.info(f"Final result for region {region}: {result.get('title', 'No title')}")
    return result

# Test function
def test_regions():
    """Test scraper with different regions"""
    test_urls = [
        "https://www.aliexpress.com/item/1005010089125608.html?shipFromCountry=PK",
        "https://www.aliexpress.com/item/1005010089125608.html?shipFromCountry=PL"
    ]
    
    results = {}
    
    for url in test_urls:
        region = get_region_from_url(url)
        logger.info(f"Testing region: {region}")
        
        try:
            result = get_product_info(url)
            if result:
                results[region] = {
                    'success': True,
                    'title': result.get('title', 'No title'),
                    'price': result.get('price', {}),
                    'seller': result.get('seller', 'No seller'),
                    'images_count': len(result.get('images', [])),
                    'specs_count': len(result.get('specifications', [])),
                    'error': None
                }
                logger.info(f"✓ {region}: {result.get('title', 'No title')}")
            else:
                results[region] = {
                    'success': False,
                    'error': 'No result returned'
                }
                logger.error(f"✗ {region}: No result")
                
        except Exception as e:
            results[region] = {
                'success': False,
                'error': str(e)
            }
            logger.error(f"✗ {region}: {e}")
    
    return results

if __name__ == "__main__":
    # Test with both regions
    logger.info("Testing AliExpress scraper with Pakistan and Poland regions...")
    
    test_results = test_regions()
    
    print("\n=== Test Results ===")
    for region, result in test_results.items():
        print(f"\n{region} ({REGIONS.get(region, 'Unknown')}):")
        if result['success']:
            print(f"  Title: {result['title']}")
            print(f"  Price: {result['price']}")
            print(f"  Seller: {result['seller']}")
            print(f"  Images: {result['images_count']}")
            print(f"  Specs: {result['specs_count']}")
        else:
            print(f"  Error: {result['error']}")
    
    # Test individual URL
    test_url = "https://www.aliexpress.com/item/1005010089125608.html"
    logger.info(f"\nTesting individual URL: {test_url}")
    
    # Test Pakistan
    pk_url = add_region_to_url(test_url, "PK")
    pk_result = get_product_info(pk_url)
    
    if pk_result:
        print(f"\nPakistan Result:")
        print(f"Title: {pk_result.get('title', 'N/A')}")
        print(f"Price: {pk_result.get('price', {}).get('price', 'N/A')}")
        print(f"Seller: {pk_result.get('seller', 'N/A')}")
        print(f"Images: {len(pk_result.get('images', []))}")
    
    # Test Poland
    pl_url = add_region_to_url(test_url, "PL")
    pl_result = get_product_info(pl_url)
    
    if pl_result:
        print(f"\nPoland Result:")
        print(f"Title: {pl_result.get('title', 'N/A')}")
        print(f"Price: {pl_result.get('price', {}).get('price', 'N/A')}")
        print(f"Seller: {pl_result.get('seller', 'N/A')}")
        print(f"Images: {len(pl_result.get('images', []))}")
    
    print("\nScraping complete!")
