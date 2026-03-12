"""
Data Mapper: Maps scraped product attributes to 71 Octopia template columns
UPDATED: Handles RAW HTML description in "Description marketing" field
"""

TEMPLATE_MAPPING = {
    # ============================================================
    # REQUIRED FIELDS (marked with *)
    # ============================================================
    "GTIN (EAN, ISBN, UPC…)*": None,  # Not available from AliExpress
    "Référence vendeur*": None,  # Not available from AliExpress
    "Titre*": "title",  # ✅ from scraper
    "Description*": "description",  # ✅ HTML description from detailmodule_text
    "URL image 1*": "image_1",  # ✅ from scraper
    "Référence de Regroupement des Variants*": None,  # Not available
    
    # ============================================================
    # BRAND & MARKETING
    # ============================================================
    "Marque": "brand",  # ✅ from scraper
    "Description marketing": "description",  # 🔥 RAW HTML from detailmodule_text
    
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
    "Couleur principale": "color",  # ✅ from scraper specs
    "Couleur(s)": "color",  # ✅ from scraper specs
    "Dimensions": "dimensions",  # ✅ from scraper specs
    "Dimension maximum (cm)": "dimensions",  # ✅ from scraper specs
    "Dimension medium (cm)": "dimensions",  # ✅ from scraper specs
    "Dimension minimum (cm)": "dimensions",  # ✅ from scraper specs
    "Dimensions plié": "dimensions",  # ✅ from scraper specs
    "Poids": "weight",  # ✅ from scraper specs
    "Poids emballé (kg)": "weight",  # ✅ from scraper specs
    "Matière - Puériculture": "material",  # ✅ from scraper specs
    "Matières": "material",  # ✅ from scraper specs
    
    # ============================================================
    # CERTIFICATIONS & ORIGIN
    # ============================================================
    "Certifications et normes": "certifications",  # ✅ from scraper specs
    "Pays d'origine": "country_of_origin",  # ✅ from scraper specs
    
    # ============================================================
    # WARRANTY & SHIPPING
    # ============================================================
    "Garantie (²)": "warranty",  # ✅ from scraper specs
    "Garantie additionnelle": "warranty",  # ✅ from scraper specs
    
    # ============================================================
    # PRODUCT INFO
    # ============================================================
    "Type de Produit": "product_type",  # ✅ from scraper specs
    "Gamme": "product_type",  # ✅ from scraper specs
    
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
    "Description du produit": "description",  # ✅ HTML description
    
    # ============================================================
    # OPTIONAL FIELDS (may be empty)
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
    
    IMPORTANT:
    - Description field contains RAW HTML from detailmodule_text
    - HTML is stored as-is (not converted to text)
    - Maps to both "Description*" and "Description marketing" fields
    
    Args:
        scraped_data: Dict with scraped product attributes from scraper.py
        
    Returns:
        Dict with template fields as keys and scraped values as values
    """
    
    mapped = {}
    
    print("\n" + "="*70)
    print(f"[mapper] 🔄 MAPPING {len(scraped_data)} scraped fields to template")
    print("="*70 + "\n")
    
    for template_field, mapping_key in TEMPLATE_MAPPING.items():
        value = ""
        
        if mapping_key is None:
            # Field not available from scraper
            continue
        
        elif mapping_key == "bullet_points":
            # Handle bullet points list from scraper
            bullet_list = scraped_data.get("bullet_points", [])
            if isinstance(bullet_list, list) and bullet_list:
                # Filter out empty bullets
                filtered = [str(b).strip() for b in bullet_list if b and str(b).strip()]
                if filtered:
                    value = " | ".join(filtered)
                    print(f"[mapper] ✅ {template_field:40s} = {len(filtered)} bullets")
        
        elif mapping_key == "price":
            # Store price info in Notes field
            price = scraped_data.get("price", "")
            shipping = scraped_data.get("shipping", "")
            
            parts = []
            if price:
                parts.append(f"Price: {price}")
            if shipping:
                parts.append(f"Shipping: {shipping}")
            
            if parts:
                value = " | ".join(parts)
                print(f"[mapper] ✅ {template_field:40s} = {value}")
        
        else:
            # Get value directly from scraped data
            raw_value = scraped_data.get(mapping_key, "")
            
            if not raw_value:
                continue
            
            # ============================================================
            # FIELD-SPECIFIC FORMATTING
            # ============================================================
            
            if mapping_key == "title":
                # Clean and limit title to 132 chars
                value = str(raw_value).strip()
                if len(value) > 132:
                    # Cut at word boundary
                    value = value[:132].rsplit(' ', 1)[0]
                print(f"[mapper] ✅ {template_field:40s} = {len(value)} chars")
            
            elif mapping_key == "description":
                # 🔥 IMPORTANT: Keep as RAW HTML - do NOT convert to text
                # The description field contains HTML from detailmodule_text
                value = str(raw_value)
                
                # Check if it's HTML (contains tags)
                if '<' in value and '>' in value:
                    # It's HTML - keep as-is, limit to 5000 chars
                    value = value[:5000]
                    print(f"[mapper] ✅ {template_field:40s} = {len(value)} chars (RAW HTML)")
                else:
                    # It's plain text - limit to 2000 chars
                    value = value[:2000]
                    print(f"[mapper] ✅ {template_field:40s} = {len(value)} chars (TEXT)")
            
            elif "image" in mapping_key:
                # Validate image URLs
                url = str(raw_value).strip()
                if url and url.startswith(("http://", "https://", "//")):
                    value = url
                    print(f"[mapper] ✅ {template_field:40s} = {url[:50]}...")
                continue
            
            else:
                # Generic string fields
                value = str(raw_value).strip()
                if value:
                    if len(value) > 50:
                        print(f"[mapper] ✅ {template_field:40s} = {value[:50]}...")
                    else:
                        print(f"[mapper] ✅ {template_field:40s} = {value}")
        
        # Only add non-empty values to mapped data
        if value:
            mapped[template_field] = value
    
    print(f"\n[mapper] ✅ MAPPING COMPLETE: {len(mapped)} fields ready for template\n")
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
    
    print("\n[mapper] 📋 Validating required fields...")
    
    for field in required_fields:
        value = mapped_data.get(field, "").strip()
        if not value:
            missing.append(field)
            print(f"[mapper] ❌ MISSING: {field}")
        else:
            # Show preview
            preview = value[:50] if len(value) > 50 else value
            print(f"[mapper] ✅ PRESENT: {field} ({len(value)} chars)")
    
    is_valid = len(missing) == 0
    
    if is_valid:
        print(f"[mapper] ✅ All required fields present!\n")
    else:
        print(f"[mapper] ❌ {len(missing)} required fields missing!\n")
    
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
