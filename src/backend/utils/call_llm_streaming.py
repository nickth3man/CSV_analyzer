"""Streaming LLM wrapper for OpenRouter with caching support."""

from __future__ import annotations

import time
from typing import Iterable

from openai import OpenAI

from src.backend.utils.cache import get_cached, set_cached
from src.backend.utils.call_llm import (
    DEFAULT_MAX_TOKENS,
    DEFAULT_MODEL,
    DEFAULT_TEMPERATURE,
    LLMSettings,
    _build_cache_key,
    _cache_enabled,
    _load_llm_settings,
    _log_llm_call,
)


def _chunk_text(text: str, size: int = 64) -> Iterable[str]:
    for idx in range(0, len(text), size):
        yield text[idx : idx + size]


def _resolve_settings(model: str | None) -> LLMSettings:
    settings = _load_llm_settings()
    if model:
        return LLMSettings(
            api_key=settings.api_key,
            model=model,
            temperature=settings.temperature,
            max_tokens=settings.max_tokens,
            timeout=settings.timeout,
            base_url=settings.base_url,
        )
    return settings


def call_llm_streaming(prompt: str, model: str | None = None):
    """Stream response tokens from OpenRouter for the given prompt."""
    settings = _resolve_settings(model)
    cache_key = _build_cache_key(prompt, settings)

    if _cache_enabled():
        cached = get_cached(cache_key)
        if cached is not None:
            _log_llm_call(prompt, cached, 0.0, cached=True, model=settings.model)
            yield from _chunk_text(cached)
            return

    client = OpenAI(
        api_key=settings.api_key,
        base_url=settings.base_url,
        timeout=settings.timeout,
    )

    kwargs = {
        "model": settings.model or DEFAULT_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": True,
        "temperature": settings.temperature or DEFAULT_TEMPERATURE,
        "max_tokens": settings.max_tokens or DEFAULT_MAX_TOKENS,
    }

    start_time = time.time()
    full_response = ""
    stream = client.chat.completions.create(**kwargs)

    for chunk in stream:
        delta = chunk.choices[0].delta.content
        if delta:
            full_response += delta
            yield delta

    if _cache_enabled() and full_response:
        set_cached(cache_key, full_response)
    _log_llm_call(prompt, full_response, start_time, cached=False, model=settings.model)


def call_llm_with_callback(prompt: str, callback=None, model: str | None = None) -> str:
    full_response = ""
    for token in call_llm_streaming(prompt, model):
        full_response += token
        if callback:
            callback(token)
    return full_response
