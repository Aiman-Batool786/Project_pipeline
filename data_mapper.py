import re


def strip_html(text):

    if not text:
        return ""

    clean = re.compile("<.*?>")

    text = re.sub(clean, "", text)

    text = " ".join(text.split())

    return text


def map_scraped_data_to_template(scraped_data):

    mapped = {}

    mapped["Titre*"] = scraped_data.get("title", "")[:132]

    mapped["Description*"] = strip_html(scraped_data.get("description", ""))[:2000]

    mapped["URL image 1*"] = scraped_data.get("image_1", "")

    mapped["Marque"] = scraped_data.get("brand", "")

    mapped["Description marketing"] = scraped_data.get(
        "description_html_enriched", ""
    )

    for i in range(2, 7):

        key = f"image_{i}"

        mapped[f"URL image {i}"] = scraped_data.get(key, "")

    return mapped
