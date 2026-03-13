"""
Data Mapper: Maps scraped product data to 71 Octopia template columns
UPDATED: Includes all specification fields extracted from nav-specification
"""

import re
import json


TEMPLATE_MAPPING = {
    # REQUIRED FIELDS
    "gtin": None,
    "sellerProductReference": None,
    "title": "title",                          # Titre* ✅
    "description": "description",              # Description* ✅
    "sellerPictureUrls_1": "image_1",         # URL image 1* ✅
    "gtinReference": None,
    
    # BRAND & MARKETING
    "brand": "brand",                          # Marque ✅
    "richMarketingDescription": "html_description",
    
    # ADDITIONAL IMAGES
    "sellerPictureUrls_2": "image_2",
    "sellerPictureUrls_3": "image_3",
    "sellerPictureUrls_4": "image_4",
    "sellerPictureUrls_5": "image_5",
    "sellerPictureUrls_6": "image_6",
    
    # COLORS & PHYSICAL ATTRIBUTES (From Specifications)
    "3264": "color",                           # Couleur principale ✅
    "3263": "color",                           # Couleur(s) ✅
    "24069": "dimensions",                     # Dimensions ✅
    "24072": "weight",                         # Poids ✅
    "24061": "material",                       # Matières ✅
    
    # CERTIFICATIONS & ORIGIN (From Specifications)
    "6720": "certifications",                  # Certifications et normes ✅
    "11429": "country_of_origin",             # Pays d'origine ✅
    
    # WARRANTY (From Specifications)
    "37937": "warranty",                       # Garantie ✅
    
    # PRODUCT INFO (From Specifications)
    "23346": "product_type",                   # Type de Produit ✅
    
    # BULLET POINTS & FEATURES
    "24612": "bullet_points",                  # Fonction du produit ✅
    
    # PRICE & SHIPPING
    "6587": "price",                           # Notes ✅
    
    # AGE INFORMATION (From Specifications)
    "11335": "age_from",                       # Age (A partir de) ✅
    "11338": "age_to",                         # Age (Jusqu'à) ✅
    
    # OPTIONAL FIELDS
    "3485": None,
    "11347": None,
    "36517": None,
    "11384": None,
    "36519": None,
    "36516": None,
    "24097": None,
    "24947": None,
    "24077": None,
    "3487": None,
    "36520": None,
    "26158": None,
    "38412": None,
    "28003": None,
    "5403": None,
    "26127": None,
    "36497": None,
    "34025": None,
    "35581": None,
    "45244": None,
    "36522": None,
    "31210": None,
    "7426": None,
    "38414": None,
    "37045": None,
    "38413": None,
    "47288": None,
    "47443": None,
    "47444": None,
    "47456": "brand",
    "47457": None,
    "47458": None,
    "45465": "brand",
    "26117": "description",
}


def strip_html(text):
    """Remove HTML tags from text"""
    if not text:
        return ""
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    text = ' '.join(text.split())
    return text


def map_scraped_data_to_template(scraped_data):
    """
    Maps scraped product data to 71 Octopia template columns
    
    Uses ROW 5 technical field names
    Includes specifications from nav-specification section
    """
    
    mapped = {}
    
    print("[mapper] 🔄 Mapping scraped data to template...")
    
    for field_key, mapping_key in TEMPLATE_MAPPING.items():
        value = ""
        
        if mapping_key is None:
            continue
        
        elif mapping_key == "bullet_points":
            bullet_list = scraped_data.get("bullet_points", [])
            if isinstance(bullet_list, list) and bullet_list:
                value = " | ".join(str(b).strip() for b in bullet_list if b)
        
        elif mapping_key == "price":
            price = scraped_data.get("price", "")
            shipping = scraped_data.get("shipping", "")
            if price or shipping:
                value = f"Price: {price} | Shipping: {shipping}".strip()
        
        elif mapping_key == "html_description":
            value = scraped_data.get("html_description", "")
        
        else:
            raw_value = scraped_data.get(mapping_key, "")
            if raw_value is None or raw_value == "":
                continue
            value = raw_value
        
        # FIELD-SPECIFIC FORMATTING
        if mapping_key == "title" and value:
            value = str(value)[:132]
        
        elif mapping_key == "description" and value and mapping_key != "html_description":
            value = strip_html(str(value))
            value = value[:2000]
        
        elif mapping_key and "image" in mapping_key:
            if value and not str(value).startswith(("http://", "https://", "//")):
                continue
        
        if value:
            mapped[field_key] = value
    
    print(f"[mapper] ✅ Mapped {len(mapped)} fields to template")
    return mapped


def validate_mapped_data(mapped_data):
    """Validates that required fields are filled"""
    
    required_fields = [
        "title",
        "description",
        "sellerPictureUrls_1",
    ]
    
    missing = []
    
    for field in required_fields:
        value = str(mapped_data.get(field, "")).strip()
        if not value:
            missing.append(field)
    
    is_valid = len(missing) == 0
    
    if is_valid:
        print("[mapper] ✅ All required fields present")
    else:
        print(f"[mapper] ⚠️  Missing: {missing}")
    
    return is_valid, missing


def create_template_row(mapped_data, product_id, category_id, category_name):
    """Creates a complete row for the template"""
    row = {
        "product_id": product_id,
        "category_id": category_id,
        "category_name": category_name,
    }
    row.update(mapped_data)
    return row
