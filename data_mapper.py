"""
Data Mapper: Maps scraped product attributes to template columns
Template: Baby Phone / Écoute Bébé
"""

# ============================================
# TEMPLATE COLUMN MAPPING
# ============================================
# Maps template fields to scraped attributes

TEMPLATE_MAPPING = {
    # Required fields (marked with *)
    "GTIN (EAN, ISBN, UPC…)*": "ean",  # Not available from AliExpress
    "Référence vendeur*": "seller_reference",  # Not available from AliExpress
    "Titre*": "title",
    "Description*": "description",
    "URL image 1*": "image_1",
    "Référence de Regroupement des Variants*": "variant_reference",
    
    # Variation
    "Groupe de variation": "variation_group",
    
    # Brand & Marketing
    "Marque": "brand",
    "Description marketing": "marketing_description",
    
    # Additional Images
    "URL image 2": "image_2",
    "URL image 3": "image_3",
    "URL image 4": "image_4",
    "URL image 5": "image_5",
    "URL image 6": "image_6",
    
    # Energy & Technology (Baby Phone specific)
    "Mode économie d'énergie": "energy_mode",
    "Accessoires livrés": "accessories",
    "Technologie utilisée - Puériculture": "technology",
    "Fonction vidéo": "video_function",
    "Ondes zéro émission": "zero_emissions",
    "Type de transmission - Puériculture": "transmission_type",
    
    # General Product Info
    "Fonction du produit": "product_function",
    "Type de public": "target_audience",
    "Age (A partir de)": "age_from",
    "Age (Jusqu'à)": "age_to",
    "Type d'écran": "screen_type",
    "Type d'alimentation - Maison": "power_type",
    "Couleur principale": "color",
    "Modèles": "models",
    "Objet connecté": "smart_device",
    "Avertissements de sécurité": "safety_warnings",
    
    # Manufacturer Information
    "Fabricant - Nom et raison sociale": "manufacturer_name",
    "Fabricant - Adresse postale": "manufacturer_address",
    "Fabricant - Adresse électronique": "manufacturer_email",
    
    # Dimensions & Weight
    "Dimension maximum (cm)": "dimension_max",
    "Dimension medium (cm)": "dimension_medium",
    "Dimension minimum (cm)": "dimension_min",
    "Poids emballé (kg)": "weight_packaged",
    "Dimensions": "dimensions",
    "Poids": "weight",
    
    # Materials & Features
    "Matière - Puériculture": "material_baby",
    "Matières": "material",
    "Genre": "gender",
    
    # Warranty & Support
    "Garantie additionnelle": "warranty_additional",
    "Garantie (²)": "warranty",
    
    # Additional Info
    "Informations complémentaires": "additional_info",
    "Compatibilité": "compatibility",
    "Nombre de canaux": "channels",
    "Autonomie": "battery_life",
    "Certifications et normes": "certifications",
    "Gamme": "range",
    "Dimensions plié": "folded_dimensions",
    "Label": "label",
    "Type de Produit": "product_type",
    "Découvrir la marque": "discover_brand",
    "Description du produit": "product_description",
    "Licences": "licenses",
    "Couleur(s)": "colors",
    "Réglable - Sécurité bébé": "adjustable",
    "Notes": "notes",
    "Options - Puériculture": "options",
    "Label - French Tech": "label_french_tech",
    "Type d'ouverture - Puériculture": "opening_type",
    "N° certification Standard 100 by Oeko-Tex ®": "certification_oeko_tex",
    "N° certification Longtime ®": "certification_longtime",
    "Données relatives au produit connecté": "smart_data",
    "Pays d'origine": "country_of_origin",
    "Données douanières": "customs_data",
    
    # Other
    "Fixation - Puériculture": "mounting",
    "Sous-état - Niveau d'usure des produits d'occasion": "condition",
    "durée de disponibilité des pièces détachées essentielles à l'utilisation du produit": "spare_parts_availability",
    "Universel": "universal",
}


def map_scraped_data_to_template(scraped_data):
    """
    Maps scraped product data to template columns
    
    Args:
        scraped_data: Dict with scraped product attributes
        
    Returns:
        Dict with template fields as keys and scraped values as values
    """
    
    mapped = {}
    
    for template_field, mapping_key in TEMPLATE_MAPPING.items():
        # Get value from scraped data or leave empty
        value = scraped_data.get(mapping_key, "")
        
        # Handle special cases
        if mapping_key == "title" and value:
            # Limit title to 132 characters per template requirement
            value = value[:132]
        
        elif mapping_key == "description" and value:
            # Limit description to 2000 characters per template requirement
            value = value[:2000]
        
        elif mapping_key in ["image_1", "image_2", "image_3", "image_4", "image_5", "image_6"]:
            # Ensure images are valid URLs
            if value and not value.startswith(("http://", "https://", "//")):
                value = ""
        
        elif mapping_key == "product_function" and value:
            # Join bullet points if available
            if isinstance(scraped_data.get("bullet_points"), list):
                value = " | ".join(scraped_data.get("bullet_points", []))
        
        mapped[template_field] = value
    
    return mapped


def get_required_fields():
    """Returns list of required fields for the template"""
    required = [
        "GTIN (EAN, ISBN, UPC…)*",
        "Référence vendeur*",
        "Titre*",
        "Description*",
        "URL image 1*",
        "Référence de Regroupement des Variants*"
    ]
    return required


def validate_mapped_data(mapped_data):
    """
    Validates that required fields are filled
    
    Args:
        mapped_data: Dict with mapped template data
        
    Returns:
        (is_valid, missing_fields)
    """
    
    required = get_required_fields()
    missing = []
    
    for field in required:
        value = mapped_data.get(field, "").strip()
        if not value:
            missing.append(field)
    
    return len(missing) == 0, missing


def create_template_row(mapped_data, product_id, category_id, category_name):
    """
    Creates a complete row for the template with all information
    
    Args:
        mapped_data: Dict with mapped template data
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
        "timestamp": "",
    }
    
    # Add all mapped fields
    row.update(mapped_data)
    
    return row


# ============================================
# FIELD DESCRIPTIONS FOR REFERENCE
# ============================================

FIELD_DESCRIPTIONS = {
    "GTIN (EAN, ISBN, UPC…)*": "13 caractères max - Product barcode",
    "Référence vendeur*": "50 caractères max - Seller reference",
    "Titre*": "132 caractères max - Product title",
    "Description*": "2000 caractères max - Product description",
    "URL image 1*": "Main product image (>=500x500px, https, <5MB)",
    "Marque": "Brand name (max 30 characters)",
    "Couleur principale": "Main color",
    "Poids emballé (kg)": "Packaged weight in kg",
    "Dimensions": "Product dimensions",
    "Certifications et normes": "Certifications and standards",
}
