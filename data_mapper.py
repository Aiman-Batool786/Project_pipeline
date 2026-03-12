import re


TEMPLATE_MAPPING = {

    "Titre*": "title",
    "Description*": "description",

    "URL image 1*": "image_1",
    "URL image 2": "image_2",
    "URL image 3": "image_3",
    "URL image 4": "image_4",
    "URL image 5": "image_5",
    "URL image 6": "image_6",

    "Marque": "brand",

    "Description marketing": "description_html",

    "Couleur principale": "color",

    "Dimensions": "dimensions",

    "Poids": "weight",

    "Matières": "material",

    "Fonction du produit": "bullet_points",

    "Informations complémentaires": "bullet_points",

    "Notes": "price",
}


def strip_html(text):

    if not text:
        return ""

    clean = re.compile("<.*?>")
    text = re.sub(clean, "", text)

    return " ".join(text.split())


def map_scraped_data_to_template(scraped_data):

    mapped = {}

    for template_field, mapping_key in TEMPLATE_MAPPING.items():

        value = ""

        if mapping_key == "bullet_points":

            bullets = scraped_data.get("bullet_points", [])

            if isinstance(bullets, list):
                value = " | ".join(bullets)

        elif mapping_key == "description_html":

            value = scraped_data.get("description_html", "")

        else:

            value = scraped_data.get(mapping_key, "")

        if mapping_key == "title" and value:
            value = value[:132]

        if mapping_key == "description" and value:
            value = strip_html(value)[:2000]

        if value:
            mapped[template_field] = value

    return mapped


def validate_mapped_data(mapped):

    required = [
        "Titre*",
        "Description*",
        "URL image 1*",
    ]

    missing = []

    for r in required:
        if not mapped.get(r):
            missing.append(r)

    return len(missing) == 0, missing


def create_template_row(mapped, product_id, category_code, leaf_category):

    row = {
        "product_id": product_id,
        "OCTOPIA | Catégorie": f"OCTOPIA | Catégorie | {category_code} | {leaf_category}",
    }

    row.update(mapped)

    return row
