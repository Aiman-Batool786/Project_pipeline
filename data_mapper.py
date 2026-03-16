"""
data_mapper.py
──────────────
Maps enriched product data to Octopia template columns.

IMPORTANT: Keys in TEMPLATE_MAPPING must match EXACTLY the cell values
in ROW 5 of the .xlsm file (as read by TemplateFiller._build_field_map).

Confirmed from actual template scan:
  Col 1  → gtin
  Col 2  → sellerProductReference
  Col 3  → title
  Col 4  → description
  Col 5  → sellerPictureUrls_1
  Col 7  → gtinReference
  Col 8  → brand
  Col 9  → richMarketingDescription
  Col 10 → sellerPictureUrls_2  ... Col 14 → sellerPictureUrls_6
  Col 21 → 24612  (bullet points / product function)
  Col 23 → 11335  (age from)
  Col 25 → 24077  (gender)
  Col 26 → 3264   (color)
  Col 38 → 37938  (warranty duration)
  Col 39 → 38412  (certifications)
  Col 40 → 11338  (age to)
  Col 44 → 24069  (dimensions L x l x H)
  Col 45 → 5403   (net weight)
  Col 47 → 24061  (materials)
  Col 49 → 6720   (certifications 2)
  Col 50 → 37937  (warranty years)
  Col 57 → 23346  (product type)
  Col 58 → 45465  (brand manufacturer)
  Col 59 → 26117  (secondary description)
  Col 60 → 24072  (weight kg)
  Col 62 → 3263   (main color 2)
  Col 64 → 6587   (notes / price)
  Col 66 → 37045  (country of origin)
  Col 72 → 11429  (country of manufacture)
"""

import re
import json


# ─────────────────────────────────────────────────────────────────────────────
# TEMPLATE COLUMN MAPPING
# Keys   = exact string in ROW 5 of the .xlsm  (confirmed from real template)
# Values = key to look up in the enriched product data dict
#          None → skip this column
# ─────────────────────────────────────────────────────────────────────────────
TEMPLATE_MAPPING = {

    # ── REQUIRED TEXT FIELDS ─────────────────────────────────────────────────
    "title":                    "title",
    "description":              "description",
    "sellerPictureUrls_1":      "image_1",

    # ── IDENTIFIERS (leave blank) ─────────────────────────────────────────────
    "gtin":                     None,
    "sellerProductReference":   None,
    "gtinReference":            None,

    # ── BRAND & RICH CONTENT ──────────────────────────────────────────────────
    "brand":                    "brand",
    "richMarketingDescription": "html_description",

    # ── ADDITIONAL IMAGES ────────────────────────────────────────────────────
    "sellerPictureUrls_2":      "image_2",
    "sellerPictureUrls_3":      "image_3",
    "sellerPictureUrls_4":      "image_4",
    "sellerPictureUrls_5":      "image_5",
    "sellerPictureUrls_6":      "image_6",

    # ── COLOUR ───────────────────────────────────────────────────────────────
    "3264":     "color",        # Couleur(s)
    "3263":     "color",        # Couleur principale 2
    "3485":     "color",        # Couleur principale

    # ── DIMENSIONS & WEIGHT ──────────────────────────────────────────────────
    "24069":    "dimensions",   # Dimensions (L x l x H)
    "24630":    "dimensions",   # Dimensions produit
    "5403":     "weight",       # Poids net
    "24072":    "weight",       # Poids (kg)
    "26127":    "weight",       # Poids brut

    # ── MATERIAL ─────────────────────────────────────────────────────────────
    "24061":    "material",     # Matières
    "36517":    "material",     # Matière principale

    # ── CERTIFICATIONS ───────────────────────────────────────────────────────
    "6720":     "certifications",   # Certifications
    "38412":    "certifications",   # Certifications et normes

    # ── COUNTRY OF ORIGIN ────────────────────────────────────────────────────
    "37045":    "country_of_origin",    # Pays d'origine
    "11429":    "country_of_origin",    # Pays de fabrication
    "38414":    "country_of_origin",    # Origine géographique

    # ── WARRANTY ─────────────────────────────────────────────────────────────
    "37937":    "warranty",     # Garantie (années)
    "37938":    "warranty",     # Garantie durée

    # ── PRODUCT TYPE ─────────────────────────────────────────────────────────
    "23346":    "product_type", # Type de Produit

    # ── BULLET POINTS / FEATURES ─────────────────────────────────────────────
    "24612":    "bullet_points",    # Fonction du produit

    # ── PRICE / NOTES ────────────────────────────────────────────────────────
    "6587":     "price",        # Notes

    # ── AGE ──────────────────────────────────────────────────────────────────
    "11335":    "age_from",     # Age (A partir de)
    "24947":    "age_to",       # Age (Jusqu'à)
    "11338":    "age_to",       # Age (Jusqu'à) 2

    # ── GENDER ───────────────────────────────────────────────────────────────
    "24077":    "gender",       # Sexe / Genre

    # ── MANUFACTURER (maps to brand) ─────────────────────────────────────────
    "47456":    "brand",        # Fabricant - Nom
    "45465":    "brand",        # Marque fabricant

    # ── SECONDARY DESCRIPTION ────────────────────────────────────────────────
    "26117":    "description",  # Description secondaire

    # ── INTENTIONALLY BLANK ──────────────────────────────────────────────────
    "11347":    None,   # Couleur secondaire
    "11384":    None,   # Composition
    "36519":    None,   # Contenance / Capacité
    "36516":    None,   # Nombre de pièces
    "24097":    None,   # Langue
    "3487":     None,   # Style
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
    "36453":    None,   # Niveau de compétence
    "34025":    None,   # Type de pile
    "35581":    None,   # Nombre de piles
    "45244":    None,   # Puissance
    "36522":    None,   # Collection
    "31210":    None,   # Volume
    "7426":     None,   # Sous-thème
    "38413":    None,   # Numéro de modèle
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

    The returned dict is passed to TemplateFiller.fill_product_data(),
    which looks up each key against ROW 5 of the .xlsm file
    (case-insensitive, whitespace-stripped).
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

        # ── Only store non-empty values ───────────────────────────────────
        if value:
            mapped[col_key] = str(value) if not isinstance(value, str) else value

    print(f"[mapper] ✅ {len(mapped)} template columns populated")
    for k, v in mapped.items():
        preview = str(v)[:60] + "…" if len(str(v)) > 60 else str(v)
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
