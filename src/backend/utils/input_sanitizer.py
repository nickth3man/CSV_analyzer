"""Sanitize user-provided questions to reduce prompt injection risk."""

from __future__ import annotations

import re
from typing import Iterable


MAX_QUESTION_LENGTH = 2000

_SUSPICIOUS_PATTERNS: Iterable[re.Pattern[str]] = [
    re.compile(r"ignore\\s+(all|previous|above)\\s+instructions", re.IGNORECASE),
    re.compile(r"disregard\\s+(all|previous|above)\\s+instructions", re.IGNORECASE),
    re.compile(r"system\\s+prompt", re.IGNORECASE),
    re.compile(r"developer\\s+message", re.IGNORECASE),
    re.compile(r"act\\s+as\\s+a\\s+system", re.IGNORECASE),
    re.compile(r"(tool|function)\\s+call", re.IGNORECASE),
    re.compile(r"(api\\s*key|password|secret)", re.IGNORECASE),
]


def sanitize_user_question(question: str) -> tuple[str, list[str]]:
    """Normalize and strip high-risk prompt injection patterns.

    Returns a sanitized string and a list of warning messages.
    """
    warnings: list[str] = []

    if not question:
        return "", warnings

    sanitized = "".join(ch for ch in question if ch.isprintable())
    sanitized = sanitized.replace("\r", "\n")

    if len(sanitized) > MAX_QUESTION_LENGTH:
        sanitized = sanitized[:MAX_QUESTION_LENGTH]
        warnings.append("Question truncated to max length.")

    lines = [line.strip() for line in sanitized.splitlines() if line.strip()]
    filtered_lines: list[str] = []
    for line in lines:
        if any(pattern.search(line) for pattern in _SUSPICIOUS_PATTERNS):
            warnings.append("Removed a potentially unsafe instruction.")
            continue
        filtered_lines.append(line)

    if filtered_lines:
        sanitized = " ".join(filtered_lines)

    sanitized = re.sub(r"\\s+", " ", sanitized).strip()
    return sanitized, warnings
