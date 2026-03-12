"""
Data Mapper: Maps scraped product data to 71 Octopia template columns
CORRECTED: Uses ROW 5 technical field names (title, description, etc.)
Works with template_filler.py which reads from ROW 5
"""

import re
import json


# ============================================================
# TEMPLATE MAPPING - Uses ROW 5 technical field names
# ============================================================
# Key = ROW 5 technical field name (what template_filler.py expects)
# Value = scraper output key name

TEMPLATE_MAPPING = {
    # ============================================================
    # REQUIRED FIELDS (marked with * in row 4)
    # ============================================================
    "gtin": None,                              # GTIN (EAN, ISBN, UPC…)*
    "sellerProductReference": None,            # Référence vendeur*
    "title": "title",                          # Titre* ✅
    "description": "description",              # Description* ✅
    "sellerPictureUrls_1": "image_1",         # URL image 1* ✅
    "gtinReference": None,                     # Référence de Regroupement des Variants*
    
    # ============================================================
    # BRAND & MARKETING
    # ============================================================
    "brand": "brand",                          # Marque ✅
    "richMarketingDescription": "html_description",  # Description marketing ✅
    
    # ============================================================
    # ADDITIONAL IMAGES
    # ============================================================
    "sellerPictureUrls_2": "image_2",         # URL image 2 ✅
    "sellerPictureUrls_3": "image_3",         # URL image 3 ✅
    "sellerPictureUrls_4": "image_4",         # URL image 4 ✅
    "sellerPictureUrls_5": "image_5",         # URL image 5 ✅
    "sellerPictureUrls_6": "image_6",         # URL image 6 ✅
    
    # ============================================================
    # COLORS & PHYSICAL ATTRIBUTES
    # ============================================================
    "3264": "color",                           # Couleur principale (code 3264) ✅
    "3263": "color",                           # Couleur(s) (code 3263) ✅
    "24069": "dimensions",                     # Dimensions (code 24069) ✅
    "999999": "dimensions",                    # Dimension maximum (cm) ✅
    "999998": "dimensions",                    # Dimension medium (cm) ✅
    "999997": "dimensions",                    # Dimension minimum (cm) ✅
    "36453": "dimensions",                     # Dimensions plié ✅
    "24072": "weight",                         # Poids (code 24072) ✅
    "999996": "weight",                        # Poids emballé (kg) ✅
    "38410": "material",                       # Matière - Puériculture ✅
    "24061": "material",                       # Matières ✅
    
    # ============================================================
    # CERTIFICATIONS & ORIGIN
    # ============================================================
    "6720": "certifications",                  # Certifications et normes ✅
    "11429": "country_of_origin",             # Pays d'origine ✅
    
    # ============================================================
    # WARRANTY
    # ============================================================
    "37937": "warranty",                       # Garantie (²) ✅
    "37938": "warranty",                       # Garantie additionnelle ✅
    
    # ============================================================
    # PRODUCT INFO
    # ============================================================
    "23346": "product_type",                   # Type de Produit ✅
    "11348": "product_type",                   # Gamme ✅
    
    # ============================================================
    # BULLET POINTS & FEATURES
    # ============================================================
    "24612": "bullet_points",                  # Fonction du produit ✅
    "24630": "bullet_points",                  # Informations complémentaires ✅
    
    # ============================================================
    # PRICE & SHIPPING (stored in Notes)
    # ============================================================
    "6587": "price",                           # Notes ✅
    
    # ============================================================
    # MANUFACTURER
    # ============================================================
    "47456": "brand",                          # Fabricant - Nom et raison sociale ✅
    "47457": None,                             # Fabricant - Adresse postale
    "47458": None,                             # Fabricant - Adresse électronique
    "45465": "brand",                          # Découvrir la marque ✅
    "26117": "description",                    # Description du produit ✅
    
    # ============================================================
    # OPTIONAL FIELDS (set to None - not available from AliExpress)
    # ============================================================
    "3485": None,                              # Mode économie d'énergie
    "11347": None,                             # Accessoires livrés
    "36517": None,                             # Technologie utilisée - Puériculture
    "11384": None,                             # Fonction vidéo
    "36519": None,                             # Ondes zéro émission
    "36516": None,                             # Type de transmission - Puériculture
    "24097": None,                             # Type de public
    "11335": None,                             # Age (A partir de)
    "24947": None,                             # Type d'écran
    "24077": None,                             # Type d'alimentation - Maison
    "3487": None,                              # Modèles
    "36520": None,                             # Objet connecté
    "26158": None,                             # Avertissements de sécurité
    "38412": None,                             # Fixation - Puériculture
    "11338": None,                             # Age (Jusqu'à)
    "28003": None,                             # Genre
    "5403": None,                              # Compatibilité
    "26127": None,                             # Nombre de canaux
    "36497": None,                             # Autonomie
    "34025": None,                             # Sous-état - Niveau d'usure...
    "35581": None,                             # durée de disponibilité...
    "45244": None,                             # Label
    "36522": None,                             # Universel
    "31210": None,                             # Licences
    "7426": None,                              # Réglable - Sécurité bébé
    "38414": None,                             # Options - Puériculture
    "37045": None,                             # Label - French Tech
    "38413": None,                             # Type d'ouverture - Puériculture
    "47288": None,                             # N° certification Standard 100 by Oeko-Tex ®
    "47443": None,                             # N° certification Longtime ®
    "47444": None,                             # Données relatives au produit connecté
    "999992": None,                            # Données douanières
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
    
    Uses ROW 5 technical field names (e.g., 'title', 'description', 'brand')
    Template filler reads these keys from ROW 5
    
    Args:
        scraped_data: Dict with scraped product attributes from scraper.py
                      Can include 'html_description' from OpenAI enrichment
        
    Returns:
        Dict with ROW 5 field keys and values ready for template
    """
    
    mapped = {}
    
    print("[mapper] 🔄 Mapping scraped data to template...")
    
    for field_key, mapping_key in TEMPLATE_MAPPING.items():
        value = ""
        
        if mapping_key is None:
            # Field not available from scraper
            continue
        
        elif mapping_key == "bullet_points":
            # Handle bullet points list from scraper
            bullet_list = scraped_data.get("bullet_points", [])
            if isinstance(bullet_list, list) and bullet_list:
                # Join with pipe separator
                value = " | ".join(str(b).strip() for b in bullet_list if b)
        
        elif mapping_key == "price":
            # Store price info in Notes field
            price = scraped_data.get("price", "")
            shipping = scraped_data.get("shipping", "")
            if price or shipping:
                value = f"Price: {price} | Shipping: {shipping}".strip()
        
        elif mapping_key == "html_description":
            # Use LLM-generated HTML description (from openai_client.py)
            # Keep HTML as-is, don't strip tags!
            value = scraped_data.get("html_description", "")
        
        else:
            # Get value directly from scraped data
            raw_value = scraped_data.get(mapping_key, "")
            if raw_value is None or raw_value == "":
                continue
            
            value = raw_value
        
        # ============================================================
        # FIELD-SPECIFIC FORMATTING
        # ============================================================
        
        if mapping_key == "title" and value:
            # Max 132 characters for title
            value = str(value)[:132]
        
        elif mapping_key == "description" and value and mapping_key != "html_description":
            # Strip HTML and limit to 2000 characters (for description field only)
            value = strip_html(str(value))
            value = value[:2000]
        
        elif mapping_key and "image" in mapping_key:
            # Validate image URLs
            if value and not str(value).startswith(("http://", "https://", "//")):
                continue
        
        # Only add non-empty values to keep mapping clean
        if value:
            mapped[field_key] = value
    
    print(f"[mapper] ✅ Mapped {len(mapped)} fields to template")
    return mapped


def validate_mapped_data(mapped_data):
    """
    Validates that required fields are filled
    
    Required fields:
    - title (required by Octopia)
    - description (required by Octopia)
    - sellerPictureUrls_1 (required by Octopia)
    
    Args:
        mapped_data: Dict with mapped template data
        
    Returns:
        Tuple (is_valid: bool, missing_fields: list)
    """
    
    required_fields = [
        "title",                    # Titre* is required
        "description",              # Description* is required
        "sellerPictureUrls_1",     # URL image 1* is required
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
        print(f"[mapper] ❌ Missing required fields: {missing}")
    
    return is_valid, missing


def create_template_row(mapped_data, product_id, category_id, category_name):
    """
    Creates a complete row for the template with all information
    
    Args:
        mapped_data: Dict with mapped template fields
        product_id: Product ID from database
        category_id: Octopia category ID
        category_name: Octopia category name
        
    Returns:
        Dict with complete row data
    """
    row = {
        "product_id": product_id,
        "category_id": category_id,
        "category_name": category_name,
    }
    row.update(mapped_data)
    return row
