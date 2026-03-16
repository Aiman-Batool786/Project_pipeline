"""
openai_client.py - HYBRID APPROACH
────────────────────────────────────
Sends title + description + scraped specifications to GPT-4o-mini.

Returns:
    title                  – improved marketing title (max 200 chars)
    description            – plain-text refined description (2-3 sentences)
    bullet_points          – list of 5 marketing bullet points
    html_description       – full structured HTML description
    specifications_enhanced – dict of ONLY the specs that were enhanced
                              (empty string "" for any field not enhanced)
"""

import os
import json
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Spec fields that the LLM is asked to enhance
SPEC_FIELDS = [
    'brand', 'color', 'dimensions', 'weight', 'material',
    'certifications', 'country_of_origin', 'warranty', 'product_type'
]


def _build_spec_text(specifications: dict) -> str:
    """Format scraped spec fields for the prompt."""
    if not specifications or not isinstance(specifications, dict):
        return ""

    lines = []
    extra_keys = ['age_from', 'age_to', 'gender']
    all_keys = SPEC_FIELDS + extra_keys

    for key in all_keys:
        value = specifications.get(key)
        if value and str(value).strip():
            display_key = key.replace('_', ' ').title()
            lines.append(f"  {display_key}: {value}")

    return ("\n\nProduct Specifications (scraped):\n" + "\n".join(lines)) if lines else ""


def _empty_spec_dict() -> dict:
    """Return the expected spec dict with all fields empty."""
    return {field: "" for field in SPEC_FIELDS}


def improve_product_content(
    title: str,
    description: str,
    specifications: dict = None,
    category: str = None
) -> dict | None:
    """
    Enhance product content via OpenAI.

    Rules enforced on the LLM response:
    - specifications_enhanced must contain ALL SPEC_FIELDS keys
    - Fields the LLM couldn't enhance must be returned as ""
    - The LLM must NOT invent specs that weren't in the original data
    """

    spec_text     = _build_spec_text(specifications)
    category_line = f"\nCategory: {category}" if category else ""

    # Build the expected JSON skeleton so the LLM knows the exact output shape
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
2. Only enhance / clarify a field if its value was present in the scraped data above.
3. If a field was not in the scraped data, return exactly "" (empty string).
4. Do NOT invent or assume values that were not provided.
5. If a value exists but needs no improvement, return the original value unchanged.

html_description rules:
- <p><strong>Label:</strong> value</p> for individual product specs
- <h3>Section Title</h3> then <ul><li>…</li></ul> for Features, Package Includes, Notes
- No <html>, <body>, <head>, or wrapper tags
- Valid, clean, semantic HTML only

Example html_description structure:
<p><strong>Content:</strong> 15ml</p>
<p><strong>Size:</strong> 10×7cm</p>
<h3>Features</h3>
<ul>
<li>High-purity formula promotes collagen production</li>
<li>Gentle formula for sensitive skin</li>
</ul>
<h3>Package Includes</h3>
<ul>
<li>1 × Product Name</li>
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

        # ── Validate required text keys ─────────────────────────────────────
        for key in ("title", "description", "bullet_points", "html_description"):
            if key not in result:
                logger.warning(f"[openai] WARNING: Missing key '{key}' — using fallback")
                result[key] = "" if key != "bullet_points" else []

        # ── Validate / normalise specifications_enhanced ────────────────────
        raw_specs = result.get("specifications_enhanced", {})
        if not isinstance(raw_specs, dict):
            raw_specs = {}

        # Ensure every expected key is present; unknown keys are discarded
        normalised_specs = {}
        for field in SPEC_FIELDS:
            val = raw_specs.get(field, "")
            # Treat None, non-strings, or whitespace-only as empty
            if val is None or not isinstance(val, str) or not val.strip():
                normalised_specs[field] = ""
            else:
                normalised_specs[field] = val.strip()

        result["specifications_enhanced"] = normalised_specs

        enhanced_count = len([v for v in normalised_specs.values() if v])
        print(f"[openai] ✅ Content enhanced")
        print(f"[openai]    Title, description, bullets, HTML — OK")
        print(f"[openai]    Specifications enhanced: {enhanced_count} / {len(SPEC_FIELDS)} fields")

        return result

    except json.JSONDecodeError as e:
        print(f"[openai] ❌ Invalid JSON from LLM: {e}")
        print(f"[openai]    Raw response: {content[:500]}")
        return None
    except Exception as e:
        print(f"[openai] ❌ API error: {e}")
        return None


# Expose logger so the validate block above can use it
import logging
logger = logging.getLogger(__name__)
