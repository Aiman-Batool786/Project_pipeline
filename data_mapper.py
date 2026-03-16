"""
data_mapper.py
──────────────
Maps enriched product data to Octopia template columns.

Keys in TEMPLATE_MAPPING = exact string in ROW 5 of the .xlsm
(confirmed by scanning the real template file).

ROW 5 column scan result:
  Col 1  gtin | Col 2  sellerProductReference | Col 3  title
  Col 4  description | Col 5  sellerPictureUrls_1 | Col 7  gtinReference
  Col 8  brand | Col 9  richMarketingDescription
  Col 10-14  sellerPictureUrls_2 … sellerPictureUrls_6
  Col 15 3485  | Col 16 11347 | Col 17 36517 | Col 18 11384
  Col 19 36519 | Col 20 36516 | Col 21 24612 | Col 22 24097
  Col 23 11335 | Col 24 24947 | Col 25 24077 | Col 26 3264
  Col 27 3487  | Col 28 36520 | Col 29 26158 | Col 31 47456
  Col 32 47457 | Col 33 47458 | Col 34-38 999999-37938
  Col 39 38412 | Col 40 11338 | Col 41 38410 | Col 42 28003
  Col 43 24630 | Col 44 24069 | Col 45 5403  | Col 46 26127
  Col 47 24061 | Col 48 36497 | Col 49 6720  | Col 50 37937
  Col 51 11348 | Col 52 36453 | Col 53 34025 | Col 54 35581
  Col 55 45244 | Col 56 36522 | Col 57 23346 | Col 58 45465
  Col 59 26117 | Col 60 24072 | Col 61 31210 | Col 62 3263
  Col 63 7426  | Col 64 6587  | Col 65 38414 | Col 66 37045
  Col 67 38413 | Col 68 47288 | Col 69 47443 | Col 70 47444
  Col 72 11429 | Col 73 999992
"""

import re
import json


# ─────────────────────────────────────────────────────────────────────────────
# TEMPLATE COLUMN MAPPING
# Keys   = exact ROW 5 cell value (verified from real template)
# Values = key in enriched product data dict  |  None = skip
# ─────────────────────────────────────────────────────────────────────────────
TEMPLATE_MAPPING = {

    # ── REQUIRED ─────────────────────────────────────────────────────────────
    "title":                    "title",
    "description":              "description",
    "sellerPictureUrls_1":      "image_1",

    # ── IDENTIFIERS ──────────────────────────────────────────────────────────
    "gtin":                     None,
    "sellerProductReference":   None,
    "gtinReference":            None,

    # ── BRAND & RICH CONTENT ─────────────────────────────────────────────────
    "brand":                    "brand",
    "richMarketingDescription": "html_description",

    # ── ADDITIONAL IMAGES ────────────────────────────────────────────────────
    "sellerPictureUrls_2":      "image_2",
    "sellerPictureUrls_3":      "image_3",
    "sellerPictureUrls_4":      "image_4",
    "sellerPictureUrls_5":      "image_5",
    "sellerPictureUrls_6":      "image_6",

    # ── COLOUR ───────────────────────────────────────────────────────────────
    "3264":     "color",            # Couleur(s)
    "3263":     "color",            # Couleur principale 2
    "3485":     "color",            # Couleur principale

    # ── DIMENSIONS & WEIGHT ──────────────────────────────────────────────────
    "24069":    "dimensions",       # Dimensions (L x l x H)
    "24630":    "dimensions",       # Dimensions produit
    "5403":     "weight",           # Poids net
    "24072":    "weight",           # Poids (kg)
    "26127":    "weight",           # Poids brut

    # ── MATERIAL ─────────────────────────────────────────────────────────────
    "24061":    "material",         # Matières
    "36517":    "material",         # Matière principale

    # ── CERTIFICATIONS ───────────────────────────────────────────────────────
    "6720":     "certifications",   # Certifications
    "38412":    "certifications",   # Certifications et normes

    # ── COUNTRY OF ORIGIN ────────────────────────────────────────────────────
    "37045":    "country_of_origin",    # Pays d'origine
    "11429":    "country_of_origin",    # Pays de fabrication
    "38414":    "country_of_origin",    # Origine géographique

    # ── WARRANTY ─────────────────────────────────────────────────────────────
    "37937":    "warranty",         # Garantie (années)
    "37938":    "warranty",         # Garantie durée

    # ── PRODUCT TYPE ─────────────────────────────────────────────────────────
    "23346":    "product_type",     # Type de Produit

    # ── BULLET POINTS ────────────────────────────────────────────────────────
    "24612":    "bullet_points",    # Fonction du produit

    # ── PRICE / NOTES ────────────────────────────────────────────────────────
    "6587":     "price",            # Notes

    # ── AGE ──────────────────────────────────────────────────────────────────
    "11335":    "age_from",         # Age (A partir de)
    "24947":    "age_to",           # Age (Jusqu'à)
    "11338":    "age_to",           # Age (Jusqu'à) 2

    # ── GENDER ───────────────────────────────────────────────────────────────
    "24077":    "gender",           # Sexe / Genre

    # ── MANUFACTURER ─────────────────────────────────────────────────────────
    "47456":    "brand",            # Fabricant - Nom
    "45465":    "brand",            # Marque fabricant

    # ── SECONDARY DESCRIPTION ────────────────────────────────────────────────
    "26117":    "description",      # Description secondaire

    # ── STYLE ────────────────────────────────────────────────────────────────
    "3487":     "style",            # Style

    # ── VOLUME / CAPACITY → mapped to col 31210 ──────────────────────────────
    "31210":    "capacity",         # Volume / Capacité

    # ── MODEL NUMBER → col 38413 ─────────────────────────────────────────────
    "38413":    "model_number",     # Numéro de modèle

    # ── POWER / VOLTAGE → col 45244 ──────────────────────────────────────────
    "45244":    "voltage",          # Puissance / Voltage

    # ── INSTALLATION → col 36453 ─────────────────────────────────────────────
    "36453":    "installation",     # Type d'installation

    # ── INTENTIONALLY BLANK ──────────────────────────────────────────────────
    "11347":    None,   # Couleur secondaire
    "11384":    None,   # Composition
    "36519":    None,   # Contenance / Capacité (different from 31210)
    "36516":    None,   # Nombre de pièces
    "24097":    None,   # Langue
    "36520":    None,   # Format
    "26158":    None,   # Thème
    "47457":    None,   # Fabricant - Adresse
    "47458":    None,   # Fabricant - Contact
    "999999":   None,
    "999998":   None,
    "999997":   None,
    "999996":   None,
    "999992":   None,
    "38410":    None,   # Avertissement sécurité
    "28003":    None,   # Instructions entretien
    "36497":    None,   # Classe énergétique
    "11348":    None,   # Tranche d'âge
    "34025":    None,   # Type de pile
    "35581":    None,   # Nombre de piles
    "36522":    None,   # Collection
    "7426":     None,   # Sous-thème
    "47288":    None,   # Référence fournisseur
    "47443":    None,   # EAN
    "47444":    None,   # ASIN
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def strip_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    return ' '.join(text.split())


def _format_bullet_points(raw) -> str:
    if isinstance(raw, list):
        return " | ".join(str(b).strip() for b in raw if b)
    if isinstance(raw, str):
        return raw.strip()
    return ""


def _format_price_notes(scraped_data: dict) -> str:
    price    = scraped_data.get("price", "")
    shipping = scraped_data.get("shipping", "")
    parts = []
    if price:
        parts.append(f"Price: {price}")
    if shipping:
        parts.append(f"Shipping: {shipping}")
    return " | ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN MAPPER
# ─────────────────────────────────────────────────────────────────────────────

def map_scraped_data_to_template(scraped_data: dict) -> dict:
    """
    Maps product data dict → {template_column_key: value}.
    Passed to TemplateFiller.fill_product_data() which does a
    case-insensitive lookup against ROW 5 of the .xlsm.
    """
    print("[mapper] 🔄 Mapping product data to template columns...")
    mapped = {}

    for col_key, data_key in TEMPLATE_MAPPING.items():

        if data_key is None:
            continue

        # ── Special cases ─────────────────────────────────────────────────
        if data_key == "bullet_points":
            value = _format_bullet_points(scraped_data.get("bullet_points", []))

        elif data_key == "price":
            value = _format_price_notes(scraped_data)

        elif data_key == "html_description":
            value = scraped_data.get("html_description", "")

        else:
            raw = scraped_data.get(data_key, "")
            if raw is None:
                continue
            value = raw

        # ── Field-level formatting ────────────────────────────────────────
        if data_key == "title" and value:
            value = str(value)[:132]

        elif data_key == "description" and data_key != "html_description" and value:
            value = strip_html(str(value))[:2000]

        elif "image" in data_key and value:
            if not str(value).startswith(("http://", "https://", "//")):
                continue

        # ── Store non-empty values only ───────────────────────────────────
        if value:
            mapped[col_key] = str(value) if not isinstance(value, str) else value

    print(f"[mapper] ✅ {len(mapped)} template columns populated")
    for k, v in mapped.items():
        preview = str(v)[:70] + "…" if len(str(v)) > 70 else str(v)
        print(f"[mapper]    {k:30s} → {preview}")

    return mapped


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

def validate_mapped_data(mapped_data: dict):
    required = ["title", "description", "sellerPictureUrls_1"]
    missing  = [f for f in required if not str(mapped_data.get(f, "")).strip()]

    if missing:
        print(f"[mapper] ⚠️  Missing required fields: {missing}")
    else:
        print("[mapper] ✅ All required fields present")

    return len(missing) == 0, missing


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY
# ─────────────────────────────────────────────────────────────────────────────

def create_template_row(mapped_data: dict, product_id, category_id, category_name) -> dict:
    row = {"product_id": product_id, "category_id": category_id,
           "category_name": category_name}
    row.update(mapped_data)
    return row
