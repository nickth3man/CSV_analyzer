"""LLM wrapper for OpenRouter API with retries, caching, and async support."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass

import httpx
from openai import OpenAI
from tenacity import (
    AsyncRetrying,
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.backend.utils.cache import get_cached, set_cached
from src.backend.utils.logger import get_logger
from src.backend.utils.resilience import circuit_breaker


logger = logging.getLogger(__name__)

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_MODEL = "meta-llama/llama-3.3-70b-instruct"
DEFAULT_TEMPERATURE = 0.1
DEFAULT_MAX_TOKENS = 2000
DEFAULT_TIMEOUT_SECONDS = 60.0

CACHE_ENV = "LLM_CACHE_ENABLED"


class AuthenticationError(RuntimeError):
    """Raised when OpenRouter authentication fails."""


@dataclass(frozen=True)
class LLMSettings:
    """Resolved settings for a single LLM call."""

    api_key: str
    model: str
    temperature: float
    max_tokens: int
    timeout: float
    base_url: str = OPENROUTER_BASE_URL


def _cache_enabled() -> bool:
    value = os.environ.get(CACHE_ENV, "1")
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _build_cache_key(prompt: str, settings: LLMSettings) -> str:
    return (
        f"[model={settings.model}|temp={settings.temperature}|"
        f"max_tokens={settings.max_tokens}] {prompt}"
    )


def _is_auth_error(error_message: str) -> bool:
    lower = error_message.lower()
    return (
        "401" in lower
        or "user not found" in lower
        or "invalid api key" in lower
        or "authentication" in lower
        or "unauthorized" in lower
        or "forbidden" in lower
    )


def _load_llm_settings() -> LLMSettings:
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "OPENROUTER_API_KEY is required. Set it in the environment or .env.",
        )

    model = os.environ.get("OPENROUTER_MODEL") or os.environ.get("LLM_MODEL")
    model = model or DEFAULT_MODEL

    temperature = DEFAULT_TEMPERATURE
    if env_temp := os.environ.get("LLM_TEMPERATURE"):
        try:
            temperature = float(env_temp)
        except ValueError:
            logger.warning("Invalid LLM_TEMPERATURE=%s; using default", env_temp)

    max_tokens = DEFAULT_MAX_TOKENS
    if env_tokens := os.environ.get("LLM_MAX_TOKENS"):
        try:
            max_tokens = int(env_tokens)
        except ValueError:
            logger.warning("Invalid LLM_MAX_TOKENS=%s; using default", env_tokens)

    return LLMSettings(
        api_key=api_key,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )


def _log_llm_call(
    prompt: str,
    response: str,
    start_time: float,
    *,
    cached: bool,
    model: str,
) -> None:
    latency_ms = int((time.time() - start_time) * 1000) if start_time else 0
    get_logger().log_llm_call(
        prompt=prompt,
        response=response,
        latency_ms=latency_ms,
        cached=cached,
        model=model,
    )


@circuit_breaker(threshold=5, recovery=120)
def call_llm(prompt: str, max_retries: int = 3) -> str:
    """Call OpenRouter synchronously with retries and caching."""
    settings = _load_llm_settings()
    attempts = max(1, max_retries)
    cache_key = _build_cache_key(prompt, settings)

    if _cache_enabled():
        cached = get_cached(cache_key)
        if cached is not None:
            _log_llm_call(prompt, cached, 0.0, cached=True, model=settings.model)
            return cached

    client = OpenAI(
        api_key=settings.api_key,
        base_url=settings.base_url,
        timeout=settings.timeout,
    )
    messages = [{"role": "user", "content": prompt}]
    retryer = Retrying(
        stop=stop_after_attempt(attempts),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type(Exception)
        & retry_if_not_exception_type(AuthenticationError),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )

    start_time = time.time()
    try:
        for attempt in retryer:
            with attempt:
                try:
                    response = client.chat.completions.create(
                        model=settings.model,
                        messages=messages,  # type: ignore[arg-type]
                        temperature=settings.temperature,
                        max_tokens=settings.max_tokens,
                    )
                except Exception as e:
                    if _is_auth_error(str(e)):
                        raise AuthenticationError(str(e)) from e
                    raise

                content = response.choices[0].message.content or ""
                if _cache_enabled() and content:
                    set_cached(cache_key, content)
                _log_llm_call(
                    prompt,
                    content,
                    start_time,
                    cached=False,
                    model=settings.model,
                )
                return content
    except AuthenticationError as e:
        raise RuntimeError(
            "OpenRouter authentication failed. Set OPENROUTER_API_KEY.",
        ) from e
    except Exception as e:
        raise RuntimeError(
            f"LLM call failed after {attempts} attempts: {e!s}",
        ) from e
    return ""


async def call_llm_async(prompt: str, max_retries: int = 3) -> str:
    """Call OpenRouter asynchronously using httpx with retries and caching."""
    settings = _load_llm_settings()
    attempts = max(1, max_retries)
    cache_key = _build_cache_key(prompt, settings)

    if _cache_enabled():
        cached = get_cached(cache_key)
        if cached is not None:
            _log_llm_call(prompt, cached, 0.0, cached=True, model=settings.model)
            return cached

    headers = {"Authorization": f"Bearer {settings.api_key}"}
    payload = {
        "model": settings.model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": settings.temperature,
        "max_tokens": settings.max_tokens,
    }

    retryer = AsyncRetrying(
        stop=stop_after_attempt(attempts),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type(Exception)
        & retry_if_not_exception_type(AuthenticationError),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )

    start_time = time.time()
    try:
        async with httpx.AsyncClient(
            base_url=settings.base_url,
            timeout=settings.timeout,
            headers=headers,
        ) as client:
            async for attempt in retryer:
                with attempt:
                    response = await client.post(
                        "/chat/completions",
                        json=payload,
                    )
                    if response.status_code in {401, 403}:
                        raise AuthenticationError(response.text)
                    response.raise_for_status()
                    data = response.json()
                    content = (
                        data.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content")
                        or ""
                    )
                    if _cache_enabled() and content:
                        set_cached(cache_key, content)
                    _log_llm_call(
                        prompt,
                        content,
                        start_time,
                        cached=False,
                        model=settings.model,
                    )
                    return content
    except AuthenticationError as e:
        raise RuntimeError(
            "OpenRouter authentication failed. Set OPENROUTER_API_KEY.",
        ) from e
    except Exception as e:
        raise RuntimeError(
            f"LLM call failed after {attempts} attempts: {e!s}",
        ) from e
    return ""


if __name__ == "__main__":
    prompt = "What is the meaning of life?"
