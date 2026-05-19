"""
utils.py — Shared text utilities for the Octopia Pipeline.
"""

import re


def clean_text(text: str) -> str:
    """
    Remove HTML tags from *text*, collapse internal whitespace,
    and strip leading / trailing whitespace.

    Uses BeautifulSoup when available (recommended).  If bs4 is not
    installed the function falls back to a regex-only approach so
    existing behaviour is preserved without requiring a new dependency.

    Examples
    --------
    >>> clean_text("<p>Hello  <b>world</b></p>")
    'Hello world'
    >>> clean_text("  multiple   spaces  ")
    'multiple spaces'
    """
    if not text:
        return ""

    # ── BeautifulSoup path (strips tags properly) ─────────────────────────
    try:
        from bs4 import BeautifulSoup  # type: ignore
        text = BeautifulSoup(text, "html.parser").get_text(separator=" ")
    except ImportError:
        # Fallback: strip HTML-like tags with a simple regex
        text = re.sub(r"<[^>]+>", " ", text)

    # ── Normalise whitespace ──────────────────────────────────────────────
    text = re.sub(r"\s+", " ", text)
    return text.strip()
