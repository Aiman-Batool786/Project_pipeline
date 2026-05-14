"""
translation.py — Self-contained product-field translation helper.

Translates three product fields (title, description, specification) into
one or more target languages using either OpenAI or Anthropic as the LLM
backend.

Priority order (automatic, based on available env vars):
  1. OpenAI  — requires OPENAI_API_KEY
  2. Anthropic — requires ANTHROPIC_API_KEY

No project-specific imports.  Drop this file anywhere and it works.

Public API
----------
translate_product_fields(
    title:         str,
    description:   str,
    specification: str,
    languages:     list[str],
) -> dict[str, dict]

Returns a dict keyed by language name.  Each value is a dict with keys
that match the `translation` DB table column names exactly:

    {
        "title":         "<translated title>",
        "description":   "<translated description>",
        "specification": "<translated specification>",
    }

On per-language failure the dict contains empty strings so the caller
always gets a result for every requested language.

Constants
---------
OPENAI_MODEL    — model used for OpenAI calls   (override via env var)
ANTHROPIC_MODEL — model used for Anthropic calls (override via env var)
REQUEST_TIMEOUT — seconds before an API call is abandoned
MAX_RETRIES     — how many times to retry a failed API call (with backoff)
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — override any of these via environment variables
# ---------------------------------------------------------------------------

OPENAI_MODEL: str = os.environ.get("TRANSLATION_OPENAI_MODEL", "gpt-4o-mini")
ANTHROPIC_MODEL: str = os.environ.get(
    "TRANSLATION_ANTHROPIC_MODEL", "claude-sonnet-4-20250514"
)
REQUEST_TIMEOUT: int = int(os.environ.get("TRANSLATION_TIMEOUT_SEC", "60"))
MAX_RETRIES: int = int(os.environ.get("TRANSLATION_MAX_RETRIES", "2"))

# Supported languages — must stay in sync with db.SUPPORTED_LANGUAGES
SUPPORTED_LANGUAGES: List[str] = [
    "Romanian",
    "German",
    "Portuguese",
    "Spanish",
    "French",
]

# Regex patterns used to strip markdown code fences that some models add
_FENCE_START = re.compile(r"^```(?:json)?\s*", re.MULTILINE)
_FENCE_END = re.compile(r"\s*```$", re.MULTILINE)


# ===========================================================================
# Prompt builder
# ===========================================================================


def build_translation_prompt(
    language: str,
    title: str,
    description: str,
    specification: str,
) -> str:
    """
    Build the LLM prompt for translating the three product fields into
    *language*.

    The model is instructed to return raw JSON only with exactly three keys
    whose names match the translation table column names: title, description,
    specification.
    """
    return f"""You are a professional product translator for an e-commerce platform.

Translate the following product data into {language}.
Return ONLY a valid JSON object with exactly these three keys:
  "title"         — translated product title
  "description"   — translated product description
  "specification" — translated product specification

Rules:
- Keep brand names, model numbers, and measurements unchanged.
- Do not add or remove information.
- Return raw JSON only — no markdown fences, no extra text.
- If a field is empty, return an empty string for that key.

Product data to translate:

TITLE:
{title or ""}

DESCRIPTION:
{description or ""}

SPECIFICATION:
{specification or ""}
"""


# ===========================================================================
# Low-level API callers  (pure urllib — no third-party SDK required)
# ===========================================================================


def _post_json(url: str, headers: Dict[str, str], body: dict) -> dict:
    """
    POST *body* as JSON to *url* with *headers*.  Returns the parsed
    response dict.  Raises urllib.error.HTTPError / ValueError on failure.
    """
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url, data=payload, headers=headers, method="POST"
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _call_openai(prompt: str, api_key: str) -> str:
    """
    Send *prompt* to the OpenAI chat-completions endpoint and return the
    model's text reply.

    Raises on HTTP error or missing content.
    """
    body = {
        "model": OPENAI_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        "max_tokens": 2000,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    data = _post_json(
        "https://api.openai.com/v1/chat/completions", headers, body
    )
    return data["choices"][0]["message"]["content"].strip()


def _call_anthropic(prompt: str, api_key: str) -> str:
    """
    Send *prompt* to the Anthropic messages endpoint and return the
    model's text reply.

    Raises on HTTP error or missing content.
    """
    body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 2000,
        "messages": [{"role": "user", "content": prompt}],
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    data = _post_json(
        "https://api.anthropic.com/v1/messages", headers, body
    )
    # Anthropic returns content as a list of blocks
    text_blocks = [
        block.get("text", "")
        for block in data.get("content", [])
        if block.get("type") == "text"
    ]
    return "".join(text_blocks).strip()


def _strip_fences(raw: str) -> str:
    """Remove ```json ... ``` fences that some models add despite instructions."""
    raw = _FENCE_START.sub("", raw)
    raw = _FENCE_END.sub("", raw)
    return raw.strip()


def _llm_translate(prompt: str) -> str:
    """
    Call the best available LLM backend with retry + exponential back-off.

    Priority:
      1. OpenAI  (OPENAI_API_KEY env var)
      2. Anthropic (ANTHROPIC_API_KEY env var)

    Raises RuntimeError if neither key is available or all retries fail.
    """
    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()

    if not openai_key and not anthropic_key:
        raise RuntimeError(
            "No LLM API key found.  Set OPENAI_API_KEY or ANTHROPIC_API_KEY."
        )

    last_exc: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 2):  # +2 so attempt 1 is the first try
        try:
            if openai_key:
                logger.debug("[translation] Using OpenAI backend (attempt %d)", attempt)
                return _call_openai(prompt, openai_key)
            else:
                logger.debug("[translation] Using Anthropic backend (attempt %d)", attempt)
                return _call_anthropic(prompt, anthropic_key)

        except urllib.error.HTTPError as exc:
            # 429 = rate-limited; 5xx = transient server error — both worth retrying
            if exc.code in (429, 500, 502, 503, 504):
                wait = 2 ** attempt  # 2s, 4s, 8s …
                logger.warning(
                    "[translation] HTTP %d on attempt %d — retrying in %ds",
                    exc.code, attempt, wait,
                )
                last_exc = exc
                time.sleep(wait)
                continue
            raise  # 4xx (other than 429) → caller handles it

        except Exception as exc:
            last_exc = exc
            wait = 2 ** attempt
            logger.warning(
                "[translation] Attempt %d failed (%s) — retrying in %ds",
                attempt, exc, wait,
            )
            time.sleep(wait)

    raise RuntimeError(
        f"All {MAX_RETRIES + 1} translation attempts failed"
    ) from last_exc


# ===========================================================================
# Public API
# ===========================================================================


def translate_product_fields(
    title: str,
    description: str,
    specification: str,
    languages: Optional[List[str]] = None,
) -> Dict[str, Dict[str, str]]:
    """
    Translate product fields into each language in *languages*.

    Parameters
    ----------
    title:         Product title (source language, typically English).
    description:   Product description.
    specification: Product specification string (plain text or JSON).
    languages:     List of target languages.  Defaults to SUPPORTED_LANGUAGES.

    Returns
    -------
    A dict keyed by language name.  Each value is a dict with keys matching
    the `translation` DB table column names:

        {
            "title":         "<translated title>",
            "description":   "<translated description>",
            "specification": "<translated specification>",
        }

    Per-language errors are caught and logged; that language's entry will
    contain empty strings so the caller always receives a full result dict.
    """
    if languages is None:
        languages = SUPPORTED_LANGUAGES

    results: Dict[str, Dict[str, str]] = {}
    _empty = {"title": "", "description": "", "specification": ""}

    for lang in languages:
        try:
            prompt = build_translation_prompt(lang, title, description, specification)
            raw = _llm_translate(prompt)
            raw = _strip_fences(raw)

            parsed = json.loads(raw)

            results[lang] = {
                "title":         str(parsed.get("title", "")),
                "description":   str(parsed.get("description", "")),
                "specification": str(parsed.get("specification", "")),
            }
            logger.info(
                "[translation] ✅ %s — title preview: %.60s",
                lang, results[lang]["title"],
            )

        except json.JSONDecodeError as exc:
            logger.warning(
                "[translation] ⚠️  %s — JSON parse failed (%s).  Raw: %.120s",
                lang, exc, raw if "raw" in dir() else "<no response>",
            )
            results[lang] = dict(_empty)

        except Exception as exc:
            logger.warning("[translation] ⚠️  %s — %s", lang, exc)
            results[lang] = dict(_empty)

    return results
