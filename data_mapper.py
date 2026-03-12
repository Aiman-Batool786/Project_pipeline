"""
data_mapper.py
──────────────
Maps enriched scraped product data to Octopia template field keys.

main.py calls:
    mapped_data = map_scraped_data_to_template(enriched_data)
    is_valid, missing = validate_mapped_data(mapped_data)

mapped_data is a flat dict keyed by Octopia field names (row 5 of template):
    title, description, richMarketingDescription, brand,
    sellerPictureUrls_1..6, gtin, sellerProductReference,
    plus numeric attribute IDs for optional fields.
"""

import re


# ─────────────────────────────────────────
# REQUIRED FIELDS (Octopia mandatory)
# ─────────────────────────────────────────
REQUIRED_FIELDS = [
    "title",
    "description",
    "sellerPictureUrls_1",
    "sellerProductReference",
    "gtinReference",
]


# ─────────────────────────────────────────
# CATEGORY HELPERS
# ─────────────────────────────────────────

def parse_category(category_name: str) -> tuple:
    """
    Given:  "ADULT - EROTIC/ARTICLES WITH SEXUAL CONNOTATIONS/DISHWASHER"
    Return: ("", "DISHWASHER")

    Given:  "0V0702 | DISHWASHER"
    Return: ("0V0702", "DISHWASHER")
    """
    if not category_name:
        return ("", "")
    if "|" in category_name:
        parts = [p.strip() for p in category_name.split("|")]
        return (parts[0], parts[-1])
    leaf = category_name.split("/")[-1].strip()
    return ("", leaf)


def format_octopia_category(category_code: str, leaf_category: str) -> str:
    """Returns:  OCTOPIA | Catégorie | code | leaf"""
    return f"OCTOPIA | Catégorie | {category_code} | {leaf_category}"


# ─────────────────────────────────────────
# MAIN MAPPER
# ─────────────────────────────────────────

def map_scraped_data_to_template(enriched_data: dict) -> dict:
    """
    Convert enriched_data (from scraper + OpenAI) into a flat dict
    using Octopia template field keys.

    enriched_data keys expected:
        title, description, html_description, bullet_points,
        brand, price, color, dimensions, weight, material,
        certifications, country_of_origin, warranty, shipping,
        image_1..image_6  OR  image_url + extra_images,
        gtin, seller_ref
    """

    # ── Images ────────────────────────────────────────────────
    # Support both image_1/image_2... and image_url/extra_images formats
    images = []
    if enriched_data.get("image_url"):
        images.append(enriched_data["image_url"])
        for img in enriched_data.get("extra_images", []):
            images.append(img)
    for i in range(1, 7):
        val = enriched_data.get(f"image_{i}", "")
        if val and val not in images:
            images.append(val)

    image_fields = {}
    for i, img in enumerate(images[:6], 1):
        key = "sellerPictureUrls_1" if i == 1 else f"sellerPictureUrls_{i}"
        image_fields[key] = img

    # ── Title (max 132 chars) ─────────────────────────────────
    title = (enriched_data.get("title") or "").strip()[:132]

    # ── Raw description (plain text, max 2000) ────────────────
    description = (enriched_data.get("description") or "").strip()[:2000]

    # ── HTML marketing description (max 5000) ─────────────────
    html_desc = (enriched_data.get("html_description") or "").strip()[:5000]

    # ── Seller reference: derive from title slug ──────────────
    seller_ref = enriched_data.get("seller_ref") or enriched_data.get("sellerProductReference") or ""
    if not seller_ref and title:
        slug = re.sub(r"[^a-zA-Z0-9]", "-", title[:40]).strip("-").upper()
        seller_ref = f"AE-{slug}"

    # ── Build mapped dict ─────────────────────────────────────
    mapped = {
        # Core required fields
        "gtin":                    enriched_data.get("gtin", ""),
        "sellerProductReference":  seller_ref[:50],
        "gtinReference":           enriched_data.get("gtin", seller_ref)[:50],
        "title":                   title,
        "description":             description,               # Description* (plain text)
        "richMarketingDescription": html_desc,               # Description marketing (HTML)

        # Images
        **image_fields,

        # Brand / manufacturer
        "brand":                   (enriched_data.get("brand") or "").strip()[:30],

        # Optional product attributes mapped to Octopia numeric IDs
        # Col 26 / ID 3264  → Couleur principale
        "3264":  (enriched_data.get("color") or enriched_data.get("colour") or "").strip(),
        # Col 34 / ID 999999 → Dimension maximum (cm)
        "999999": (enriched_data.get("dimension_max") or enriched_data.get("dimensions") or "").strip(),
        # Col 35 / ID 999998 → Dimension medium (cm)
        "999998": (enriched_data.get("dimension_med") or "").strip(),
        # Col 36 / ID 999997 → Dimension minimum (cm)
        "999997": (enriched_data.get("dimension_min") or "").strip(),
        # Col 37 / ID 999996 → Poids emballé (kg)
        "999996": (enriched_data.get("weight") or enriched_data.get("poids") or "").strip(),
        # Col 47 / ID 24061  → Matières
        "24061":  (enriched_data.get("material") or enriched_data.get("matiere") or "").strip(),
        # Col 49 / ID 6720   → Certifications et normes
        "6720":   (enriched_data.get("certifications") or "").strip(),
        # Col 50 / ID 37937  → Garantie
        "37937":  (enriched_data.get("warranty") or enriched_data.get("garantie") or "").strip(),
        # Col 72 / ID 11429  → Pays d'origine
        "11429":  (enriched_data.get("country_of_origin") or enriched_data.get("pays_origine") or "").strip(),
        # Col 44 / ID 24069  → Dimensions
        "24069":  (enriched_data.get("dimensions") or "").strip(),
        # Col 60 / ID 24072  → Poids
        "24072":  (enriched_data.get("weight") or "").strip(),
    }

    # Remove empty optional fields to keep dict clean
    mapped = {k: v for k, v in mapped.items() if v != "" or k in REQUIRED_FIELDS}

    return mapped


# ─────────────────────────────────────────
# VALIDATOR
# ─────────────────────────────────────────

def validate_mapped_data(mapped_data: dict) -> tuple:
    """
    Check all required Octopia fields are present and non-empty.

    Returns:
        (is_valid: bool, missing_fields: list[str])
    """
    missing = [f for f in REQUIRED_FIELDS if not mapped_data.get(f)]
    return (len(missing) == 0, missing)


# ─────────────────────────────────────────
# PIPELINE CONVENIENCE (kept for backward compat)
# ─────────────────────────────────────────

def map_pipeline_result_to_template(
    template_path: str,
    scraped: dict,
    enhanced: dict,
    category_info: dict,
    output_path: str = None,
) -> str:
    """
    Legacy convenience wrapper used by older main.py versions.
    Merges scraped + enhanced into enriched_data, then writes the template.
    """
    from template_filler import fill_template_for_product
    import os
    from datetime import datetime

    enriched = scraped.copy()
    enriched["title"] = enhanced.get("title", scraped.get("title", ""))
    enriched["description"] = scraped.get("description", "")
    enriched["html_description"] = enhanced.get("html_description", "")
    enriched["bullet_points"] = enhanced.get("bullet_points", [])

    mapped = map_scraped_data_to_template(enriched)

    # Inject category into mapped data
    cat_code = str(category_info.get("category_id", ""))
    cat_name = category_info.get("category_name", "")
    _, leaf = parse_category(cat_name)
    mapped["_category_code"] = cat_code
    mapped["_category_leaf"] = leaf

    if output_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = os.path.splitext(template_path)[0]
        output_path = f"{base}_output_{ts}.xlsm"

    product_id = 0
    out_dir = os.path.dirname(output_path) or "."
    return fill_template_for_product(template_path, mapped, product_id, out_dir)
