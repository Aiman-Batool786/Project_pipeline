"""
data_mapper.py
──────────────
Maps scraped/enriched product data to Octopia template columns.

IMPORTANT: the keys in TEMPLATE_MAPPING must match EXACTLY the cell values
in ROW 5 of the .xlsm template (as read by TemplateFiller._build_field_map).
Update these keys if your template uses different column headers.
"""

import re
import json


# ─────────────────────────────────────────────────────────────────────────────
# TEMPLATE COLUMN MAPPING
# Keys  = exact header text from ROW 5 of the .xlsm file
# Values= key to look up in the enriched product data dict
#         None  → field intentionally left blank
# ─────────────────────────────────────────────────────────────────────────────
TEMPLATE_MAPPING = {

    # ── REQUIRED ─────────────────────────────────────────────────────────────
    "Titre*":               "title",
    "Description*":         "description",
    "URL image 1*":         "image_1",

    # ── IDENTIFIERS ──────────────────────────────────────────────────────────
    "GTIN":                 None,
    "Référence vendeur":    None,
    "Référence GTIN":       None,

    # ── BRAND & RICH CONTENT ─────────────────────────────────────────────────
    "Marque":               "brand",
    "Description marketing riche": "html_description",

    # ── ADDITIONAL IMAGES ────────────────────────────────────────────────────
    "URL image 2":          "image_2",
    "URL image 3":          "image_3",
    "URL image 4":          "image_4",
    "URL image 5":          "image_5",
    "URL image 6":          "image_6",

    # ── PHYSICAL / COLOUR ────────────────────────────────────────────────────
    "Couleur principale":   "color",
    "Couleur(s)":           "color",
    "Dimensions":           "dimensions",
    "Poids":                "weight",
    "Matières":             "material",

    # ── CERTIFICATIONS & ORIGIN ──────────────────────────────────────────────
    "Certifications et normes": "certifications",
    "Pays d'origine":           "country_of_origin",

    # ── WARRANTY ─────────────────────────────────────────────────────────────
    "Garantie (²)":         "warranty",

    # ── PRODUCT TYPE ─────────────────────────────────────────────────────────
    "Type de Produit":      "product_type",

    # ── BULLET POINTS / FEATURES ─────────────────────────────────────────────
    "Fonction du produit":  "bullet_points",

    # ── PRICE / NOTES ────────────────────────────────────────────────────────
    "Notes":                "price",

    # ── AGE ──────────────────────────────────────────────────────────────────
    "Age (A partir de)":    "age_from",
    "Age (Jusqu'à)":        "age_to",

    # ── MANUFACTURER ─────────────────────────────────────────────────────────
    "Fabricant - Nom et raison sociale": "brand",

    # ── OPTIONAL / UNMAPPED ──────────────────────────────────────────────────
    "Sexe":                 None,
    "Matière principale":   None,
    "Composition":          None,
    "Contenance":           None,
    "Nombre de pièces":     None,
    "Langue":               None,
    "Thème":                None,
    "Collection":           None,
    "Sous-thème":           None,
    "Style":                None,
    "Format":               None,
    "Type de fermeture":    None,
    "Longueur des manches": None,
    "Type de col":          None,
    "Coupe":                None,
    "Motif":                None,
    "Saison":               None,
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def strip_html(text: str) -> str:
    """Remove HTML tags and collapse whitespace."""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    return ' '.join(text.split())


def _format_bullet_points(raw) -> str:
    """Convert a list (or pipe-delimited string) of bullets to a single string."""
    if isinstance(raw, list):
        return " | ".join(str(b).strip() for b in raw if b)
    if isinstance(raw, str):
        return raw.strip()
    return ""


def _format_price_notes(scraped_data: dict) -> str:
    """Combine price + shipping into a notes string."""
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
    Maps product data dict → {template_column_header: value}.

    The returned dict is passed directly to TemplateFiller.fill_product_data(),
    which looks up each key against ROW 5 of the .xlsm file.
    """
    print("[mapper] 🔄 Mapping product data to template columns...")
    mapped = {}

    for col_header, data_key in TEMPLATE_MAPPING.items():

        if data_key is None:
            continue

        # ── Special cases ──────────────────────────────────────────────────

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

        # ── Field-level formatting ─────────────────────────────────────────

        if data_key == "title" and value:
            value = str(value)[:132]

        elif data_key == "description" and value and data_key != "html_description":
            value = strip_html(str(value))[:2000]

        elif "image" in data_key and value:
            # Keep only valid absolute URLs
            if not str(value).startswith(("http://", "https://", "//")):
                continue

        # ── Store ─────────────────────────────────────────────────────────
        if value:
            mapped[col_header] = str(value) if not isinstance(value, str) else value

    print(f"[mapper] ✅ {len(mapped)} template columns populated")
    return mapped


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

def validate_mapped_data(mapped_data: dict):
    """Check that required template columns are populated."""
    required = ["Titre*", "Description*", "URL image 1*"]
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
    """Attach metadata to a mapped row (convenience helper)."""
    row = {
        "product_id":    product_id,
        "category_id":   category_id,
        "category_name": category_name,
    }
    row.update(mapped_data)
    return row
