"""
openai_client.py - HYBRID APPROACH
Enhanced to accept and process specifications
Returns both enhanced text and specifications
"""

import os
import json
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def improve_product_content(
    title: str, 
    description: str,
    specifications: dict = None,  # ✅ NEW PARAMETER
    category: str = None
) -> dict | None:
    """
    Send raw scraped title + description + specifications to the LLM.
    
    Returns a dict with:
        title           – improved marketing title (max 200 chars)
        description     – plain-text refined description (2-3 sentences)
        bullet_points   – list of 5 marketing bullet points
        html_description – full structured HTML description
        specifications_enhanced – dict of enhanced specifications ✅ NEW
    """
    
    # ✅ Format specifications for the prompt
    spec_text = ""
    if specifications and isinstance(specifications, dict):
        spec_lines = []
        
        # Include only actual specification fields
        spec_keys = [
            'brand', 'color', 'dimensions', 'weight', 'material',
            'certifications', 'country_of_origin', 'warranty', 'product_type',
            'age_from', 'age_to', 'gender'
        ]
        
        for key in spec_keys:
            value = specifications.get(key)
            if value and value != "":
                # Format the key nicely
                display_key = key.replace('_', ' ').title()
                spec_lines.append(f"{display_key}: {value}")
        
        if spec_lines:
            spec_text = "\n\nProduct Specifications:\n" + "\n".join(spec_lines)
    
    category_line = f"\nCategory: {category}" if category else ""
    
    prompt = f"""
You are a professional eCommerce copywriter specialising in marketplace listings (Amazon, Cdiscount, Octopia).

Raw product data:
Title: {title}
Description:
{description}{spec_text}{category_line}

Your task — return ONLY valid JSON (no markdown, no code fences, no extra text):
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
    "html_description": "<full structured HTML — see format below>",
    "specifications_enhanced": {{
        "brand": "Brand name if mentioned (or empty string if not)",
        "color": "Any color enhancements or clarifications (or empty string)",
        "dimensions": "Dimension clarifications (or empty string)",
        "material": "Material quality highlights (or empty string)",
        "certifications": "Certification details (or empty string)",
        "warranty": "Warranty information (or empty string)",
        "country_of_origin": "Manufacturing origin (or empty string)",
        "product_type": "Product category refinement (or empty string)",
        "weight": "Weight information (or empty string)"
    }}
}}

IMPORTANT for specifications_enhanced:
- ONLY include fields that were in the original data
- If a spec wasn't provided, return empty string ""
- If a spec can't be enhanced, return empty string ""
- Do NOT create specifications that don't exist
- Focus on enhancing/clarifying what was provided

html_description format rules:
- Use <p><strong>Label:</strong> value</p> for product specs (Content, Size, etc.)
- Use <h3>Section Title</h3> followed by <ul><li>...</li></ul> for Features, Package Includes, Notes
- Do NOT include <html>, <body>, <head>, or any wrapper tags
- Output MUST be valid, clean, semantic HTML only
- Example structure:
<p><strong>Content:</strong> 15ml</p>
<p><strong>Size:</strong> 10×7cm</p>
<h3>Features</h3>
<ul>
<li>High-purity formula promotes collagen production and improves skin firmness</li>
<li>Gentle formula designed for sensitive skin</li>
<li>Deep penetration technology for active ingredients</li>
<li>Portable design convenient for travel</li>
<li>Dermatologist recommended and safe</li>
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
                        "Always respond with valid JSON only. "
                        "Never use markdown code fences. "
                        "Only enhance specifications that exist in original data."
                    ),
                },
                {"role": "user", "content": prompt},
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
        
        # Validate required keys
        for key in ("title", "description", "bullet_points", "html_description"):
            if key not in result:
                print(f"[openai] WARNING: Missing key '{key}' in LLM response")
                result[key] = "" if key != "bullet_points" else []
        
        # ✅ Ensure specifications_enhanced exists
        if "specifications_enhanced" not in result:
            result["specifications_enhanced"] = {}
        
        print(f"[openai] ✅ Enhanced content generated")
        print(f"[openai]    - Title, description, bullets, HTML")
        print(f"[openai]    - Specifications enhanced: {len([k for k, v in result.get('specifications_enhanced', {}).items() if v])}")
        
        return result
    
    except json.JSONDecodeError as e:
        print(f"[openai] Invalid JSON from LLM: {e}")
        print(f"[openai] Raw response: {content[:500]}")
        return None
    except Exception as e:
        print(f"[openai] API error: {e}")
        return None
