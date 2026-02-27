import os
import json
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def improve_product_content(title, description):
    prompt = f"""
Rewrite this product professionally.

Title: {title}
Description: {description}

Return ONLY valid JSON. No extra text.

Format:
{{
"title": "...",
"description": "...",
"bullet_points": ["...", "..."]
}}
"""

    content = None  # ✅ always defined

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )

        content = response.choices[0].message.content.strip()
        return json.loads(content)

    except Exception as e:
        print("OpenAI error:", e)
        if content:
            print("Raw response:", content)
        return None
