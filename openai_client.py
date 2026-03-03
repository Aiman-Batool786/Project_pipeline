import os
import json
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def improve_product_content(title, description, category=None):
    category_line = f"Category: {category}" if category else ""

    prompt = f"""
You are a professional Amazon product copywriter.
Rewrite the following product content to be engaging, clear, and optimized for eCommerce listings.

Product Details:
Title: {title}
Description: {description}
{category_line}

Instructions:
- Write a compelling, keyword-rich title (max 200 characters)
- Write a clear, benefit-focused description (2-3 sentences)
- Write 5 bullet points highlighting key features and benefits
- If category is provided, tailor the tone and keywords for that category

Return ONLY valid JSON. No extra text, no markdown, no code blocks.
Format:
{{
    "title": "...",
    "description": "...",
    "bullet_points": ["...", "...", "...", "...", "..."]
}}
"""
    content = ""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a professional eCommerce copywriter. Always respond with valid JSON only."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.7,
            max_tokens=1000,
        )
        content = response.choices[0].message.content.strip()

        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()

        return json.loads(content)

    except json.JSONDecodeError as e:
        print(" OpenAI returned invalid JSON:", e)
        print("Raw response:", content)
        return None
    except Exception as e:
        print(" OpenAI API error:", e)
        return None
