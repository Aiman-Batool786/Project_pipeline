import os
import json
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def improve_product_content(title, description, category=None):

    category_line = f"Category: {category}" if category else ""

    prompt = f"""
You are a professional eCommerce copywriter.

Improve the product listing below.

Title: {title}
Description: {description}
{category_line}

Instructions:

1. Improve the title (max 200 characters)
2. Create 5 bullet points highlighting key benefits
3. Convert the description into CLEAN HTML

Use this structure:

<p><strong>Content:</strong> ...</p>

<h3>Features</h3>
<ul>
<li>Feature 1</li>
<li>Feature 2</li>
<li>Feature 3</li>
<li>Feature 4</li>
<li>Feature 5</li>
</ul>

<h3>Package Includes</h3>
<ul>
<li>Item</li>
</ul>

Return ONLY JSON:

{{
"title": "...",
"description_html": "...",
"bullet_points": ["...", "...", "...", "...", "..."]
}}
"""

    try:

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return valid JSON only"},
                {"role": "user", "content": prompt},
            ],
            temperature=0.7,
            max_tokens=900,
        )

        content = response.choices[0].message.content.strip()

        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]

        return json.loads(content)

    except Exception as e:
        print("OpenAI error:", e)
        return None
