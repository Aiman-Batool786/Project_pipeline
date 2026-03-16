"""
openai_client.py - HYBRID APPROACH
────────────────────────────────────
Sends title + description + all scraped specifications to GPT-4o-mini.

Returns:
    title                   – improved marketing title (max 200 chars)
    description             – plain-text 2-3 sentence description
    bullet_points           – list of 5 marketing bullet points
    html_description        – full structured HTML description
    specifications_enhanced – dict of enhanced spec fields
                              (empty string "" for any not enhanced)
"""

import os
import json
import logging
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
logger = logging.getLogger(__name__)

# ── Core spec fields sent to and returned from LLM ───────────────────────────
CORE_SPEC_FIELDS = [
    'brand', 'color', 'dimensions', 'weight', 'material',
    'certifications', 'country_of_origin', 'warranty', 'product_type'
]

# ── Extra fields included in prompt but NOT required back ────────────────────
EXTRA_SPEC_FIELDS = [
    'capacity', 'freezer_capacity', 'voltage', 'model_number',
    'power_source', 'installation', 'style'
]


def _build_spec_text(specifications: dict) -> str:
    """Format all spec fields for the prompt."""
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
    """Return the expected output skeleton with all core fields empty."""
    return {field: "" for field in CORE_SPEC_FIELDS}


def improve_product_content(
    title: str,
    description: str,
    specifications: dict = None,
    category: str = None
) -> dict | None:
    """
    Enhance product content via OpenAI.

    Rules enforced:
    - specifications_enhanced must contain ALL CORE_SPEC_FIELDS keys
    - Fields that cannot be enhanced must be returned as ""
    - LLM must NOT invent specs not present in original data
    - Extra fields (voltage, capacity, etc.) are used for context only
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
- <h3>Section Title</h3> then <ul><li>…</li></ul> for Features, Package Includes
- Include relevant technical specs (capacity, voltage, dimensions) in the HTML
- No <html>, <body>, <head>, or wrapper tags
- Valid, clean, semantic HTML only

Example:
<p><strong>Capacity:</strong> 118L (84L fridge + 34L freezer)</p>
<p><strong>Dimensions:</strong> 453×525×1181mm</p>
<p><strong>Voltage:</strong> 220V</p>
<h3>Features</h3>
<ul>
<li>Energy-efficient Grade 3 compressor cooling</li>
<li>Frost-free operation for easy maintenance</li>
<li>Freestanding or built-in installation options</li>
</ul>
<h3>Package Includes</h3>
<ul>
<li>1 × Mini Refrigerator</li>
</ul>
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

        # Strip accidental markdown fences
        if content.startswith("```"):
            parts = content.split("```")
            content = parts[1] if len(parts) > 1 else content
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()

        result = json.loads(content)

        # ── Validate required text keys ───────────────────────────────────────
        for key in ("title", "description", "bullet_points", "html_description"):
            if key not in result:
                logger.warning(f"[openai] Missing key '{key}' — using fallback")
                result[key] = "" if key != "bullet_points" else []

        # ── Normalise specifications_enhanced ─────────────────────────────────
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

        enhanced_count = len([v for v in normalised.values() if v])
        print(f"[openai] ✅ Content enhanced")
        print(f"[openai]    Text fields (title, desc, bullets, HTML) — OK")
        print(f"[openai]    Specs enhanced: {enhanced_count} / {len(CORE_SPEC_FIELDS)}")
        for k, v in normalised.items():
            if v:
                print(f"[openai]       {k}: {v[:60]}")

        return result

    except json.JSONDecodeError as e:
        print(f"[openai] ❌ Invalid JSON from LLM: {e}")
        print(f"[openai]    Raw response: {content[:500]}")
        return None
    except Exception as e:
        print(f"[openai] ❌ API error: {e}")
        return None
