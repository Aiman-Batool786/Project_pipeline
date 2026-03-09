"""
Data Mapper: Maps scraped product attributes to 71 Octopia template columns
FIXED: Uses EXACT keys from scraper.py output
"""

TEMPLATE_MAPPING = {
    # ============================================================
    # REQUIRED FIELDS (marked with *)
    # ============================================================
    "GTIN (EAN, ISBN, UPC…)*": None,  # Not available from AliExpress
    "Référence vendeur*": None,  # Not available from AliExpress
    "Titre*": "title",  # ✅ from scraper
    "Description*": "description",  # ✅ from scraper
    "URL image 1*": "image_1",  # ✅ from scraper
    "Référence de Regroupement des Variants*": None,  # Not available
    
    # ============================================================
    # BRAND & MARKETING
    # ============================================================
    "Marque": "brand",  # ✅ from scraper
    "Description marketing": "description",  # Use description
    
    # ============================================================
    # ADDITIONAL IMAGES (6 images extracted)
    # ============================================================
    "URL image 2": "image_2",  # ✅ from scraper
    "URL image 3": "image_3",  # ✅ from scraper
    "URL image 4": "image_4",  # ✅ from scraper
    "URL image 5": "image_5",  # ✅ from scraper
    "URL image 6": "image_6",  # ✅ from scraper
    
    # ============================================================
    # COLORS & PHYSICAL ATTRIBUTES
    # ============================================================
    "Couleur principale": "color",  # ✅ from scraper
    "Couleur(s)": "color",  # ✅ from scraper
    "Dimensions": "dimensions",  # ✅ from scraper
    "Dimension maximum (cm)": "dimensions",  # ✅ from scraper
    "Dimension medium (cm)": "dimensions",  # ✅ from scraper
    "Dimension minimum (cm)": "dimensions",  # ✅ from scraper
    "Dimensions plié": "dimensions",  # ✅ from scraper
    "Poids": "weight",  # ✅ from scraper
    "Poids emballé (kg)": "weight",  # ✅ from scraper
    "Matière - Puériculture": "material",  # ✅ from scraper
    "Matières": "material",  # ✅ from scraper
    
    # ============================================================
    # CERTIFICATIONS & ORIGIN
    # ============================================================
    "Certifications et normes": "certifications",  # ✅ from scraper
    "Pays d'origine": "country_of_origin",  # ✅ from scraper
    
    # ============================================================
    # WARRANTY & SHIPPING
    # ============================================================
    "Garantie (²)": "warranty",  # ✅ from scraper
    "Garantie additionnelle": "warranty",  # ✅ from scraper
    
    # ============================================================
    # PRODUCT INFO
    # ============================================================
    "Type de Produit": "product_type",  # ✅ from scraper
    "Gamme": "product_type",  # ✅ from scraper
    
    # ============================================================
    # BULLET POINTS & FEATURES
    # ============================================================
    "Fonction du produit": "bullet_points",  # ✅ from scraper (list)
    "Informations complémentaires": "bullet_points",  # ✅ from scraper (list)
    
    # ============================================================
    # PRICE & SHIPPING INFO (stored in Notes)
    # ============================================================
    "Notes": "price",  # ✅ Price: stored in Notes field
    
    # ============================================================
    # MANUFACTURER
    # ============================================================
    "Fabricant - Nom et raison sociale": "brand",  # Use brand as manufacturer
    "Découvrir la marque": "brand",  # Use brand
    "Description du produit": "description",  # ✅ from scraper
    
    # ============================================================
    # BABY PHONE SPECIFIC (Optional - may be empty)
    # ============================================================
    "Mode économie d'énergie": None,
    "Accessoires livrés": None,
    "Technologie utilisée - Puériculture": None,
    "Fonction vidéo": None,
    "Ondes zéro émission": None,
    "Type de transmission - Puériculture": None,
    "Type de public": None,
    "Age (A partir de)": None,
    "Type d'écran": None,
    "Type d'alimentation - Maison": None,
    "Modèles": None,
    "Objet connecté": None,
    "Avertissements de sécurité": None,
    "Fabricant - Adresse postale": None,
    "Fabricant - Adresse électronique": None,
    "Fixation - Puériculture": None,
    "Age (Jusqu'à)": None,
    "Genre": None,
    "Compatibilité": None,
    "Nombre de canaux": None,
    "Autonomie": None,
    "Sous-état - Niveau d'usure des produits d'occasion": None,
    "durée de disponibilité des pièces détachées essentielles à l'utilisation du produit": None,
    "Label": None,
    "Universel": None,
    "Licences": None,
    "Réglable - Sécurité bébé": None,
    "Options - Puériculture": None,
    "Label - French Tech": None,
    "Type d'ouverture - Puériculture": None,
    "N° certification Standard 100 by Oeko-Tex ®": None,
    "N° certification Longtime ®": None,
    "Données relatives au produit connecté": None,
    "Données douanières": None,
    "Groupe de variation": None,
}


def map_scraped_data_to_template(scraped_data):
    """
    Maps scraped product data to 71 Octopia template columns
    
    Args:
        scraped_data: Dict with scraped product attributes from scraper.py
        
    Returns:
        Dict with template fields as keys and scraped values as values
    """
    
    mapped = {}
    
    for template_field, mapping_key in TEMPLATE_MAPPING.items():
        value = ""
        
        if mapping_key is None:
            # Field not available from scraper
            value = ""
        
        elif mapping_key == "bullet_points":
            # Handle bullet points list from scraper
            bullet_list = scraped_data.get("bullet_points", [])
            if isinstance(bullet_list, list) and bullet_list:
                value = " | ".join(str(b).strip() for b in bullet_list if b)
            else:
                value = ""
        
        elif mapping_key == "price":
            # Store price info in Notes field
            price = scraped_data.get("price", "")
            shipping = scraped_data.get("shipping", "")
            if price or shipping:
                value = f"Price: {price} | Shipping: {shipping}".strip()
            else:
                value = ""
        
        else:
            # Get value directly from scraped data
            value = scraped_data.get(mapping_key, "")
            if value is None:
                value = ""
        
        # ============================================================
        # FIELD-SPECIFIC FORMATTING
        # ============================================================
        
        if mapping_key == "title" and value:
            # Max 132 characters for title
            value = value[:132]
        
        elif mapping_key == "description" and value:
            # Max 2000 characters for description
            value = value[:2000]
        
        elif mapping_key and "image" in mapping_key:
            # Validate image URLs
            if value and not value.startswith(("http://", "https://", "//")):
                value = ""
        
        # Only add non-empty values to keep mapping clean
        if value:
            mapped[template_field] = value
    
    return mapped


def validate_mapped_data(mapped_data):
    """
    Validates that required fields are filled
    
    Args:
        mapped_data: Dict with mapped template data
        
    Returns:
        Tuple (is_valid: bool, missing_fields: list)
    """
    
    required_fields = [
        "Titre*",           # Title is required
        "Description*",     # Description is required
        "URL image 1*",     # First image is required
    ]
    
    missing = []
    
    for field in required_fields:
        value = mapped_data.get(field, "").strip()
        if not value:
            missing.append(field)
    
    is_valid = len(missing) == 0
    
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
