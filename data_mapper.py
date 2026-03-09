"""
Data Mapper: Maps scraped product attributes to template columns
FIXED: Uses correct keys from scraper output
"""

TEMPLATE_MAPPING = {
    # Required fields (marked with *)
    "GTIN (EAN, ISBN, UPC…)*": None,  # Not available from AliExpress
    "Référence vendeur*": None,  # Not available from AliExpress
    "Titre*": "title",  # ✅ Available
    "Description*": "description",  # ✅ Available
    "URL image 1*": "image_1",  # ✅ Available
    "Référence de Regroupement des Variants*": None,  # Not available
    
    # Brand & Marketing
    "Marque": "brand",  # ✅ Available
    "Description marketing": "description",  # Use description
    
    # Additional Images
    "URL image 2": "image_2",  # ✅ Available
    "URL image 3": "image_3",  # ✅ Available
    "URL image 4": "image_4",  # ✅ Available
    "URL image 5": "image_5",  # ✅ Available
    "URL image 6": "image_6",  # ✅ Available
    
    # Colors & Physical Attributes
    "Couleur principale": "color",  # ✅ Available
    "Dimensions": "dimensions",  # ✅ Available
    "Poids": "weight",  # ✅ Available
    "Poids emballé (kg)": "weight",  # ✅ Available
    "Matière - Puériculture": "material",  # ✅ Available
    "Matières": "material",  # ✅ Available
    
    # Certifications & Origin
    "Certifications et normes": "certifications",  # ✅ Available
    "Pays d'origine": "country_of_origin",  # ✅ Available
    
    # Warranty & Features
    "Garantie (²)": "warranty",  # ✅ Available
    "Fonction du produit": "bullet_points",  # Use bullet points
    "Type de Produit": "product_type",  # ✅ Available
    
    # Additional Info
    "Informations complémentaires": "bullet_points",  # ✅ Use bullet points
    "Notes": "price",  # Store price in notes
    
    # Baby Phone specific (empty if not available)
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
    "Fabricant - Nom et raison sociale": "brand",  # Use brand as manufacturer
    "Fabricant - Adresse postale": None,
    "Fabricant - Adresse électronique": None,
    "Dimension maximum (cm)": "dimensions",
    "Dimension medium (cm)": "dimensions",
    "Dimension minimum (cm)": "dimensions",
    "Garantie additionnelle": "warranty",
    "Fixation - Puériculture": None,
    "Age (Jusqu'à)": None,
    "Genre": None,
    "Compatibilité": None,
    "Nombre de canaux": None,
    "Autonomie": None,
    "Gamme": "product_type",
    "Dimensions plié": "dimensions",
    "Sous-état - Niveau d'usure des produits d'occasion": None,
    "durée de disponibilité des pièces détachées essentielles à l'utilisation du produit": None,
    "Label": None,
    "Universel": None,
    "Découvrir la marque": "brand",
    "Description du produit": "description",
    "Licences": None,
    "Couleur(s)": "color",
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
    Maps scraped product data to template columns
    Uses actual keys from scraper output
    
    Args:
        scraped_data: Dict with scraped product attributes
        
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
            # Handle bullet points list
            bullet_list = scraped_data.get("bullet_points", [])
            if isinstance(bullet_list, list):
                value = " | ".join(str(b) for b in bullet_list)
            else:
                value = str(bullet_list)
        
        elif mapping_key == "price":
            # Store price in Notes field
            value = f"Price: {scraped_data.get('price', '')}"
        
        else:
            # Get value from scraped data
            value = scraped_data.get(mapping_key, "")
        
        # Apply field-specific formatting
        if mapping_key == "title" and value:
            value = value[:132]  # Max 132 chars
        
        elif mapping_key == "description" and value:
            value = value[:2000]  # Max 2000 chars
        
        elif mapping_key and "image" in mapping_key:
            # Validate image URLs
            if value and not value.startswith(("http://", "https://", "//")):
                value = ""
        
        mapped[template_field] = value
    
    return mapped


def validate_mapped_data(mapped_data):
    """
    Validates that required fields are filled
    
    Args:
        mapped_data: Dict with mapped template data
        
    Returns:
        (is_valid: bool, missing_fields: list)
    """
    
    required_fields = [
        "Titre*",
        "Description*",
        "URL image 1*",
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
    """
    row = {
        "product_id": product_id,
        "category_id": category_id,
        "category_name": category_name,
    }
    row.update(mapped_data)
    return row
