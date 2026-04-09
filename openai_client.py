"""
openai_client.py — HYBRID APPROACH
────────────────────────────────────
Sends title + description + all scraped specifications to GPT-4o-mini.

NEW: Applies restricted keyword filtering after enhancement.
     Restricted keywords are loaded from the DB (restricted_keywords table)
     and any matches are removed from description, bullet_points, and specs.

Returns:
    title                   – improved marketing title (max 200 chars)
    description             – plain-text 2-3 sentence description
    bullet_points           – list of 5 marketing bullet points
    html_description        – full structured HTML description
    specifications_enhanced – dict of enhanced spec fields
"""

import os
import re
import json
import logging
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
logger = logging.getLogger(__name__)

CORE_SPEC_FIELDS = [
    'brand', 'color', 'dimensions', 'weight', 'material',
    'certifications', 'country_of_origin', 'warranty', 'product_type'
]

EXTRA_SPEC_FIELDS = [
    'capacity', 'freezer_capacity', 'voltage', 'model_number',
    'power_source', 'installation', 'style'
]

# ── Restricted keywords cache ─────────────────────────────────────────────────
# Loaded once at first call to improve_product_content().
# If the DB has no keywords yet, filtering is skipped silently.
_RESTRICTED_KEYWORDS: list | None = None


def _get_restricted_keywords() -> list:
    """Lazy-load restricted keywords from DB. Returns [] if DB unavailable."""
    global _RESTRICTED_KEYWORDS
    if _RESTRICTED_KEYWORDS is not None:
        return _RESTRICTED_KEYWORDS
    try:
        from db import get_restricted_keywords
        _RESTRICTED_KEYWORDS = get_restricted_keywords()
        if _RESTRICTED_KEYWORDS:
            print(f"[openai] Loaded {len(_RESTRICTED_KEYWORDS)} restricted keywords")
        else:
            print("[openai] No restricted keywords in DB (run: python db.py load-keywords)")
    except Exception as e:
        print(f"[openai] Could not load restricted keywords: {e}")
        _RESTRICTED_KEYWORDS = []
    return _RESTRICTED_KEYWORDS


def _apply_keyword_filter(result: dict) -> dict:
    """
    Remove restricted keywords from description, bullet_points, and
    specifications_enhanced values.  Logs what was removed.
    """
    keywords = _get_restricted_keywords()
    if not keywords:
        return result

    try:
        from db import filter_restricted_keywords
    except ImportError:
        return result

    flagged_all = []

    # Filter description
    if result.get('description'):
        clean, flagged = filter_restricted_keywords(result['description'], keywords)
        if flagged:
            result['description'] = clean
            flagged_all.extend(flagged)

    # Filter bullet points
    cleaned_bullets = []
    for bullet in result.get('bullet_points', []):
        clean, flagged = filter_restricted_keywords(bullet, keywords)
        if flagged:
            flagged_all.extend(flagged)
        cleaned_bullets.append(clean)
    if cleaned_bullets:
        result['bullet_points'] = cleaned_bullets

    # Filter html_description
    if result.get('html_description'):
        clean, flagged = filter_restricted_keywords(result['html_description'], keywords)
        if flagged:
            result['html_description'] = clean
            flagged_all.extend(flagged)

    # Filter spec values
    specs = result.get('specifications_enhanced', {})
    for field, value in specs.items():
        if value:
            clean, flagged = filter_restricted_keywords(str(value), keywords)
            if flagged:
                specs[field] = clean
                flagged_all.extend(flagged)
    result['specifications_enhanced'] = specs

    if flagged_all:
        unique_flagged = list(set(flagged_all))
        print(f"[openai] Restricted keywords removed: {unique_flagged}")
        result['_restricted_keywords_removed'] = unique_flagged

    return result


def _build_spec_text(specifications: dict) -> str:
    if not specifications or not isinstance(specifications, dict):
        return ""
    lines = []
    all_keys = CORE_SPEC_FIELDS + EXTRA_SPEC_FIELDS
    for key in all_keys:
        value = specifications.get(key)
        if value and str(value).strip():
            display_key = key.replace('_', ' ').title()
            lines.append(f"  {display_key}: {value}")
    return ("\n\nProduct Specifications (scraped):\n" + "\n".join(lines)) if lines else ""


def _empty_spec_dict() -> dict:
    return {field: "" for field in CORE_SPEC_FIELDS}


def improve_product_content(
    title: str,
    description: str,
    specifications: dict = None,
    category: str = None
) -> dict | None:
    """
    Enhance product content via OpenAI, then filter restricted keywords.

    Rules:
    - specifications_enhanced must contain ALL CORE_SPEC_FIELDS keys
    - Fields that cannot be enhanced must be returned as ""
    - LLM must NOT invent specs not in original data
    - Restricted keywords are stripped from all text output after LLM response
    """

    spec_text     = _build_spec_text(specifications)
    category_line = f"\nCategory: {category}" if category else ""
    spec_skeleton = json.dumps(_empty_spec_dict(), indent=4)

    prompt = f"""You are a professional eCommerce copywriter for marketplace listings
(Amazon, Cdiscount, Octopia).

Raw product data:
Title: {title}
Description:
{description}{spec_text}{category_line}

Return ONLY valid JSON — no markdown, no code fences, no extra text:
{{
    "title": "compelling keyword-rich title, max 200 characters",
    "description": "2-3 sentence plain-text benefit-focused description",
    "bullet_points": [
        "Benefit 1 – clear and specific",
        "Benefit 2 – clear and specific",
        "Benefit 3 – clear and specific",
        "Benefit 4 – clear and specific",
        "Benefit 5 – clear and specific"
    ],
    "html_description": "<structured HTML — rules below>",
    "specifications_enhanced": {spec_skeleton}
}}

RULES for specifications_enhanced:
1. ALL keys listed above MUST be present in your response.
2. Only enhance / clarify a field if its value was present in the scraped data.
3. If a field was not in the scraped data, return exactly "" (empty string).
4. Do NOT invent or assume values that were not provided.
5. Use the extra specs (voltage, capacity, model, etc.) to write richer
   bullet points and html_description — but do not add them to
   specifications_enhanced.

html_description format rules:
- <p><strong>Label:</strong> value</p> for individual product specs
- <h3>Section Title</h3> then <ul><li>...</li></ul> for Features, Package Includes
- Include relevant technical specs (capacity, voltage, dimensions) in the HTML
- No <html>, <body>, <head>, or wrapper tags
- Valid, clean, semantic HTML only

IMPORTANT — DO NOT include any of the following in your output:
shipping info, delivery times, handling times, country of origin (China),
AliExpress branding, platform-specific promotions, or return policies.
"""

    content = ""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a professional eCommerce copywriter. "
                        "Always respond with valid JSON only — no markdown, "
                        "no code fences, no preamble. "
                        "Return empty string \"\" for any spec you cannot enhance."
                    )
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=2000,
        )

        content = response.choices[0].message.content.strip()

        if content.startswith("```"):
            parts = content.split("```")
            content = parts[1] if len(parts) > 1 else content
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()

        result = json.loads(content)

        for key in ("title", "description", "bullet_points", "html_description"):
            if key not in result:
                result[key] = "" if key != "bullet_points" else []

        raw_specs = result.get("specifications_enhanced", {})
        if not isinstance(raw_specs, dict):
            raw_specs = {}

        normalised = {}
        for field in CORE_SPEC_FIELDS:
            val = raw_specs.get(field, "")
            if val is None or not isinstance(val, str) or not val.strip():
                normalised[field] = ""
            else:
                normalised[field] = val.strip()

        result["specifications_enhanced"] = normalised

        # ── Apply restricted keyword filter ────────────────────────────────
        result = _apply_keyword_filter(result)

        enhanced_count = len([v for v in normalised.values() if v])
        print(f"[openai] Content enhanced — specs: {enhanced_count}/{len(CORE_SPEC_FIELDS)}")

        return result

    except json.JSONDecodeError as e:
        print(f"[openai] Invalid JSON from LLM: {e}")
        print(f"[openai] Raw response: {content[:500]}")
        return None
    except Exception as e:
        print(f"[openai] API error: {e}")
        return None
