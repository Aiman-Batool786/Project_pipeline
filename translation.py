"""
translation.py — Self-contained product-field translation helper.

Translates three product fields (title, description, specification) into
one or more target languages using either OpenAI or Anthropic as the LLM
backend.

Priority order (automatic, based on available env vars):
  1. OpenAI    — requires OPENAI_API_KEY
  2. Anthropic — requires ANTHROPIC_API_KEY

No project-specific imports.  Drop this file anywhere and it works.

Public API
----------
translate_product_fields(
    title:         str,
    description:   str,
    specification: str,
    languages:     list[str] | None,   # defaults to SUPPORTED_LANGUAGES
) -> dict[str, dict]

Returns a dict keyed by language name.  Each value is a dict whose keys
match the `translation` DB table column names exactly:

    {
        "title":         "<translated title>",
        "description":   "<translated description>",
        "specification": "<translated specification>",
    }

On per-language failure the dict contains empty strings so the caller
always receives a result for every requested language.

Tuneable constants (override via environment variables)
-------------------------------------------------------
TRANSLATION_OPENAI_MODEL    — default: gpt-4o-mini
TRANSLATION_ANTHROPIC_MODEL — default: claude-sonnet-4-20250514
TRANSLATION_TIMEOUT_SEC     — seconds before an API call is abandoned (default: 60)
TRANSLATION_MAX_RETRIES     — extra retries on 429/5xx (default: 2 -> 3 attempts total)
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
# Configuration — all tuneable via environment variables
# ---------------------------------------------------------------------------

OPENAI_MODEL: str = os.environ.get("TRANSLATION_OPENAI_MODEL", "gpt-4o-mini")
ANTHROPIC_MODEL: str = os.environ.get(
    "TRANSLATION_ANTHROPIC_MODEL", "claude-sonnet-4-20250514"
)
REQUEST_TIMEOUT: int = int(os.environ.get("TRANSLATION_TIMEOUT_SEC", "60"))
MAX_RETRIES: int = int(os.environ.get("TRANSLATION_MAX_RETRIES", "2"))

# Supported languages — must stay in sync with db.py translation table usage
SUPPORTED_LANGUAGES: List[str] = [
    "Romanian",
    "German",
    "Portuguese",
    "Spanish",
    "French",
]

# Regex patterns used to strip markdown code fences that some models add
_FENCE_START = re.compile(r"^```(?:json)?\s*", re.MULTILINE)
_FENCE_END   = re.compile(r"\s*```$",          re.MULTILINE)


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
    *language*.  The model is told to return raw JSON only, with exactly
    three keys matching the translation table column names.
    """
    return (
        f"You are a professional product translator for an e-commerce platform.\n\n"
        f"Translate the following product data into {language}.\n"
        f'Return ONLY a valid JSON object with exactly these three keys:\n'
        f'  "title"         - translated product title\n'
        f'  "description"   - translated product description\n'
        f'  "specification" - translated product specification\n\n'
        f"Rules:\n"
        f"- Keep brand names, model numbers, and measurements unchanged.\n"
        f"- Do not add or remove information.\n"
        f"- Return raw JSON only - no markdown fences, no extra text.\n"
        f"- If a field is empty, return an empty string for that key.\n\n"
        f"Product data to translate:\n\n"
        f"TITLE:\n{title or ''}\n\n"
        f"DESCRIPTION:\n{description or ''}\n\n"
        f"SPECIFICATION:\n{specification or ''}\n"
    )


# ===========================================================================
# Low-level API callers (pure stdlib urllib — no third-party SDK required)
# ===========================================================================

def _post_json(url: str, headers: Dict[str, str], body: dict) -> dict:
    """POST body as JSON to url.  Returns the parsed response dict."""
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _call_openai(prompt: str, api_key: str) -> str:
    """Send prompt to the OpenAI chat-completions endpoint."""
    data = _post_json(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        body={
            "model":       OPENAI_MODEL,
            "messages":    [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "max_tokens":  2000,
        },
    )
    return data["choices"][0]["message"]["content"].strip()


def _call_anthropic(prompt: str, api_key: str) -> str:
    """Send prompt to the Anthropic messages endpoint."""
    data = _post_json(
        "https://api.anthropic.com/v1/messages",
        headers={
            "Content-Type":      "application/json",
            "x-api-key":         api_key,
            "anthropic-version": "2023-06-01",
        },
        body={
            "model":      ANTHROPIC_MODEL,
            "max_tokens": 2000,
            "messages":   [{"role": "user", "content": prompt}],
        },
    )
    # Anthropic returns content as a list of typed blocks
    return "".join(
        block.get("text", "")
        for block in data.get("content", [])
        if block.get("type") == "text"
    ).strip()


def _strip_fences(raw: str) -> str:
    """Remove ```json ... ``` fences that some models add despite instructions."""
    raw = _FENCE_START.sub("", raw)
    raw = _FENCE_END.sub("", raw)
    return raw.strip()


def _llm_translate(prompt: str) -> str:
    """
    Call the best available LLM backend with retry + exponential back-off.

    Priority:
      1. OpenAI    (OPENAI_API_KEY env var is non-empty)
      2. Anthropic (ANTHROPIC_API_KEY env var is non-empty)

    Retries on HTTP 429 / 5xx up to MAX_RETRIES additional times.
    Raises RuntimeError if neither key is available or all attempts fail.
    """
    openai_key    = os.environ.get("OPENAI_API_KEY",    "").strip()
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()

    if not openai_key and not anthropic_key:
        raise RuntimeError(
            "No LLM API key found. Set OPENAI_API_KEY or ANTHROPIC_API_KEY."
        )

    last_exc: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 2):   # +2 so attempt 1 is the first try
        try:
            if openai_key:
                logger.debug("[translation] OpenAI backend (attempt %d)", attempt)
                return _call_openai(prompt, openai_key)
            else:
                logger.debug("[translation] Anthropic backend (attempt %d)", attempt)
                return _call_anthropic(prompt, anthropic_key)

        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503, 504):
                wait = 2 ** attempt          # 2 s, 4 s, 8 s
                logger.warning(
                    "[translation] HTTP %d on attempt %d — retrying in %ds",
                    exc.code, attempt, wait,
                )
                last_exc = exc
                time.sleep(wait)
                continue
            raise  # non-retryable 4xx -> propagate immediately

        except Exception as exc:
            wait = 2 ** attempt
            logger.warning(
                "[translation] Attempt %d failed (%s) — retrying in %ds",
                attempt, exc, wait,
            )
            last_exc = exc
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
    Translate title, description, and specification into each language in
    languages (defaults to SUPPORTED_LANGUAGES).

    Returns a dict keyed by language name.  Each value is a dict with keys
    matching the translation DB table column names:

        {
            "title":         "<translated title>",
            "description":   "<translated description>",
            "specification": "<translated specification>",
        }

    Per-language errors are caught and logged; that language entry will
    contain empty strings so the caller always receives a complete result dict.
    """
    if languages is None:
        languages = SUPPORTED_LANGUAGES

    _empty  = {"title": "", "description": "", "specification": ""}
    results: Dict[str, Dict[str, str]] = {}

    for lang in languages:
        raw = ""
        try:
            prompt = build_translation_prompt(lang, title, description, specification)
            raw    = _llm_translate(prompt)
            raw    = _strip_fences(raw)
            parsed = json.loads(raw)

            results[lang] = {
                "title":         str(parsed.get("title",         "")),
                "description":   str(parsed.get("description",   "")),
                "specification": str(parsed.get("specification", "")),
            }
            logger.info(
                "[translation] OK %s — title: %.60s",
                lang, results[lang]["title"],
            )

        except json.JSONDecodeError as exc:
            logger.warning(
                "[translation] WARN %s — JSON parse failed (%s). Raw: %.120s",
                lang, exc, raw,
            )
            results[lang] = dict(_empty)

        except Exception as exc:
            logger.warning("[translation] WARN %s — %s", lang, exc)
            results[lang] = dict(_empty)

    return results
