"""
AliExpress Scraper with Tor Integration using Stem
Handles Tor connections, circuit management, and safe scraping
"""

import time
import json
import socket
from stem import Signal
from stem.control import Controller
import logging
from typing import Optional, Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TorStemManager:
    """Manages Tor connections using Stem library"""
    
    def __init__(self, control_port: int = 9051, socks_port: int = 9050):
        """
        Initialize Tor manager
        
        Args:
            control_port: Tor control port (default 9051)
            socks_port: Tor SOCKS port (default 9050)
        """
        self.control_port = control_port
        self.socks_port = socks_port
        self.controller = None
        self.current_ip = None
        
    def connect(self) -> bool:
        """
        Connect to running Tor instance
        
        Returns:
            True if connection successful
        """
        try:
            logger.info(f"Connecting to Tor control port {self.control_port}...")
            self.controller = Controller.from_port(port=self.control_port)
            self.controller.authenticate()
            logger.info("✓ Connected to Tor successfully")
            
            # Get initial IP
            self.current_ip = self.get_exit_ip()
            logger.info(f"Current exit IP: {self.current_ip}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to connect to Tor: {e}")
            logger.error("Make sure Tor is running:")
            logger.error("  Linux/Mac: tor --SocksPort 9050 --ControlPort 9051")
            logger.error("  Or use: apt-get install tor && tor &")
            return False
    
    def get_exit_ip(self) -> str:
        """Get current Tor exit IP"""
        try:
            import requests
            from requests.adapters import HTTPAdapter
            from requests.packages.urllib3.util.retry import Retry
            
            proxies = {
                'http': f'socks5://127.0.0.1:{self.socks_port}',
                'https': f'socks5://127.0.0.1:{self.socks_port}'
            }
            
            session = requests.Session()
            session.proxies.update(proxies)
            
            response = session.get('http://icanhazip.com', timeout=10)
            ip = response.text.strip()
            return ip
        except Exception as e:
            logger.warning(f"Could not fetch exit IP: {e}")
            return "Unknown"
    
    def renew_circuit(self) -> bool:
        """
        Get a new Tor circuit (new exit IP)
        
        Returns:
            True if renewal successful
        """
        if not self.controller:
            logger.error("Not connected to Tor")
            return False
        
        try:
            old_ip = self.current_ip
            logger.info("Renewing Tor circuit...")
            
            # Signal new identity
            self.controller.signal(Signal.NEWNYM)
            
            # Wait for circuit to establish
            time.sleep(3)
            
            # Get new IP
            self.current_ip = self.get_exit_ip()
            
            if old_ip != self.current_ip:
                logger.info(f"✓ Circuit renewed: {old_ip} → {self.current_ip}")
                return True
            else:
                logger.warning(f"⚠ IP didn't change (same circuit): {self.current_ip}")
                return True
                
        except Exception as e:
            logger.error(f"✗ Failed to renew circuit: {e}")
            return False
    
    def disconnect(self):
        """Close Tor connection"""
        if self.controller:
            self.controller.close()
            logger.info("Disconnected from Tor")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


def setup_playwright_with_tor(socks_port: int = 9050):
    """
    Set up Playwright to use Tor via SOCKS5 proxy
    
    Args:
        socks_port: Tor SOCKS port
    
    Returns:
        Playwright context options dict
    """
    return {
        "proxy": {
            "server": f"socks5://127.0.0.1:{socks_port}"
        },
        "ignore_https_errors": True,
    }


def get_product_info_with_tor(
    url: str,
    tor_manager: Optional[TorStemManager] = None,
    max_retries: int = 3,
    rotate_circuit: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Scrape AliExpress product using Tor
    
    Args:
        url: Product URL
        tor_manager: TorStemManager instance (optional)
        max_retries: Number of retry attempts
        rotate_circuit: Rotate Tor circuit between retries
    
    Returns:
        Product information dict or None
    """
    from playwright.sync_api import sync_playwright
    
    for attempt in range(max_retries):
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Attempt {attempt + 1}/{max_retries}")
            logger.info(f"URL: {url}")
            if tor_manager:
                logger.info(f"Tor IP: {tor_manager.current_ip}")
            logger.info(f"{'='*60}")
            
            with sync_playwright() as p:
                # Set up proxy options if Tor manager provided
                context_options = {
                    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "viewport": {"width": 1920, "height": 1080},
                    "locale": "en-US",
                    "timezone_id": "Asia/Karachi",
                }
                
                # Add Tor proxy if manager provided
                if tor_manager:
                    context_options.update(setup_playwright_with_tor())
                
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu"
                    ]
                )
                
                context = browser.new_context(**context_options)
                page = context.new_page()
                
                # Add timeout for page navigation
                page.set_default_timeout(60000)
                
                logger.info("Opening URL...")
                
                try:
                    page.goto(url, timeout=60000, wait_until="domcontentloaded")
                except Exception as e:
                    logger.warning(f"Navigation timeout: {e}")
                    # Continue anyway - page might have partial content
                
                # Wait for JS to render
                logger.info("Waiting for page to render...")
                time.sleep(3)
                
                # Simulate human behavior
                page.mouse.move(200, 300)
                time.sleep(1)
                page.mouse.wheel(0, 2000)
                time.sleep(2)
                
                # EXTRACT TITLE
                title = extract_field(page, [
                    ".pc-main h1",
                    "h1.pdp-title-h1",
                    ".TitleWithLogo h1",
                    "span.pdp-mod-product-title-text",
                    "h1[class*='title']",
                    "h1"
                ], min_length=5)
                
                if not title:
                    logger.warning("⚠ Could not extract title")
                    if attempt < max_retries - 1:
                        if rotate_circuit and tor_manager:
                            logger.info("Rotating Tor circuit before retry...")
                            tor_manager.renew_circuit()
                            time.sleep(2)
                        else:
                            time.sleep(5)
                        continue
                    else:
                        return None
                
                logger.info(f"✓ Title: {title[:50]}...")
                
                # EXTRACT PRICE
                price = extract_field(page, [
                    ".search-card-e-price-main",
                    "span[class*='Price']",
                    ".pricing-section",
                    "[data-price]"
                ])
                if price:
                    logger.info(f"✓ Price: {price}")
                
                # EXTRACT DESCRIPTION
                description = extract_description(page)
                if description:
                    logger.info(f"✓ Description: {len(description)} characters")
                
                # EXTRACT FEATURES
                bullet_points = extract_features(page)
                if bullet_points:
                    logger.info(f"✓ Features: {len(bullet_points)} bullet points")
                
                # EXTRACT IMAGE
                image_url = extract_image(page)
                if image_url:
                    logger.info(f"✓ Image: {image_url[:60]}...")
                
                # EXTRACT ADDITIONAL INFO
                rating = extract_field(page, ["[class*='Rating']"], max_length=10)
                reviews = extract_field(page, ["[class*='Review']"], max_length=20)
                seller = extract_field(page, ["[class*='Seller'] a"])
                
                browser.close()
                
                logger.info("\n✓ Scraping successful!\n")
                
                return {
                    "title": title,
                    "description": description,
                    "bullet_points": bullet_points,
                    "image_url": image_url,
                    "price": price,
                    "rating": rating,
                    "reviews_count": reviews,
                    "seller_name": seller,
                    "url": url
                }
        
        except Exception as e:
            logger.error(f"✗ Attempt {attempt + 1} failed: {e}")
            import traceback
            traceback.print_exc()
            
            if attempt < max_retries - 1:
                if rotate_circuit and tor_manager:
                    logger.info("Rotating Tor circuit before retry...")
                    tor_manager.renew_circuit()
                    time.sleep(3)
                else:
                    time.sleep(5 * (attempt + 1))  # Exponential backoff
    
    logger.error("✗ All scraping attempts failed")
    return None


def extract_field(page, selectors: list, min_length: int = 0, max_length: int = None) -> str:
    """
    Safely extract text from page using multiple selector fallbacks
    
    Args:
        page: Playwright page object
        selectors: List of CSS selectors to try
        min_length: Minimum text length (default 0)
        max_length: Maximum text length (default None)
    
    Returns:
        Extracted text or empty string
    """
    for selector in selectors:
        try:
            if page.locator(selector).count() > 0:
                text = page.locator(selector).first.inner_text()
                
                if text and len(text.strip()) >= min_length:
                    text = text.strip()
                    if max_length and len(text) > max_length:
                        text = text[:max_length] + "..."
                    return text
        except Exception as e:
            logger.debug(f"Selector failed {selector}: {e}")
            continue
    
    return ""


def extract_description(page) -> str:
    """Extract product description"""
    selectors = [
        "[class*='Description']",
        ".product-description",
        ".pdp-desc",
        "div[class*='Description'] p",
        "[class*='content'] p",
    ]
    
    paragraphs = []
    
    for selector in selectors:
        try:
            elements = page.locator(selector).all()
            for elem in elements[:10]:
                text = elem.inner_text()
                if text and len(text) > 10 and "AliExpress" not in text:
                    paragraphs.append(text.strip())
            
            if paragraphs:
                break
        except:
            continue
    
    # Fallback to all paragraphs
    if not paragraphs:
        try:
            all_p = page.locator("p").all()
            for p_elem in all_p[:10]:
                text = p_elem.inner_text()
                if text and len(text) > 10 and "AliExpress" not in text:
                    paragraphs.append(text.strip())
        except:
            pass
    
    return " ".join(paragraphs[:500])


def extract_features(page) -> list:
    """Extract product features/bullet points"""
    selectors = [
        "[class*='Feature'] li",
        ".product-features li",
        "[class*='Specification'] li",
        "li[class*='feature']"
    ]
    
    features = []
    
    for selector in selectors:
        try:
            items = page.locator(selector).all()
            for item in items[:10]:
                text = item.inner_text()
                if text and len(text) > 5:
                    features.append(text.strip())
            
            if features:
                break
        except:
            continue
    
    return features


def extract_image(page) -> str:
    """Extract product image URL"""
    selectors = [
        "img[class*='productImage']",
        "[class*='ImageViewer'] img",
        ".product-main-image img",
        "img[alt*='Product']",
        "img[src*='oss']",
    ]
    
    for selector in selectors:
        try:
            if page.locator(selector).count() > 0:
                img = page.locator(selector).first
                src = img.get_attribute("src") or img.get_attribute("data-src")
                if src and ("http" in src or "data:" in src):
                    return src
        except:
            continue
    
    return ""


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

if __name__ == "__main__":
    
    # Example 1: Using Tor with circuit rotation
    print("\n" + "="*60)
    print("EXAMPLE 1: Scraping with Tor and circuit rotation")
    print("="*60 + "\n")
    
    with TorStemManager(control_port=9051, socks_port=9050) as tor:
        if tor.controller:
            url = "https://www.aliexpress.com/item/1005006246885476.html"
            
            result = get_product_info_with_tor(
                url,
                tor_manager=tor,
                max_retries=3,
                rotate_circuit=True
            )
            
            if result:
                print(json.dumps(result, indent=2, ensure_ascii=False))
    
    
    # Example 2: Using Tor without circuit rotation
    print("\n" + "="*60)
    print("EXAMPLE 2: Simple Tor scraping")
    print("="*60 + "\n")
    
    with TorStemManager() as tor:
        if tor.controller:
            url = "https://www.aliexpress.com/item/1005006246885476.html"
            result = get_product_info_with_tor(url, tor_manager=tor, max_retries=2)
            
            if result:
                print(f"Title: {result['title']}")
                print(f"Price: {result['price']}")
    
    
    # Example 3: Just circuit management
    print("\n" + "="*60)
    print("EXAMPLE 3: Tor circuit management only")
    print("="*60 + "\n")
    
    tor_manager = TorStemManager(control_port=9051, socks_port=9050)
    if tor_manager.connect():
        print(f"Initial IP: {tor_manager.current_ip}")
        
        time.sleep(2)
        tor_manager.renew_circuit()
        print(f"New IP: {tor_manager.current_ip}")
        
        time.sleep(2)
        tor_manager.renew_circuit()
        print(f"Newest IP: {tor_manager.current_ip}")
        
        tor_manager.disconnect()
