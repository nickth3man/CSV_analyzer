"""Semantic cache for LLM responses.

This module provides semantic caching to reduce costs and latency,
as specified in design.md Section 4.6.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent.parent / "data" / "llm_cache"
DEFAULT_SIMILARITY_THRESHOLD = 0.95
DEFAULT_TTL_HOURS = 24
DEFAULT_MAX_ENTRIES = 10000


@dataclass
class CacheEntry:
    """A single cache entry."""

    prompt_hash: str
    prompt: str
    response: str
    timestamp: float
    embedding: list[float] | None = None


class SemanticCache:
    """Semantic cache for LLM responses.

    Caches responses by semantic similarity, not just exact match.
    Uses embeddings to find similar prompts.
    """

    def __init__(
        self,
        similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
        ttl_hours: int = DEFAULT_TTL_HOURS,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        use_embeddings: bool = True,
    ) -> None:
        """Initialize the semantic cache.

        Args:
            similarity_threshold: Minimum similarity for cache hit (0-1).
            ttl_hours: Time-to-live in hours.
            max_entries: Maximum number of cached entries.
            use_embeddings: Whether to use embeddings for semantic matching.
        """
        self.similarity_threshold = similarity_threshold
        self.ttl_seconds = ttl_hours * 3600
        self.max_entries = max_entries
        self.use_embeddings = use_embeddings

        self._cache: dict[str, CacheEntry] = {}
        self._lock = threading.Lock()
        self._embedding_service = None

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._load_cache()

    def _get_embedding_service(self):
        """Lazy-load embedding service."""
        if self._embedding_service is None and self.use_embeddings:
            try:
                from src.backend.utils.embeddings import get_embedding_service

                self._embedding_service = get_embedding_service()
            except ImportError:
                logger.warning("Embedding service not available for cache")
                self.use_embeddings = False
        return self._embedding_service

    def _load_cache(self) -> None:
        """Load cache from disk."""
        cache_file = CACHE_DIR / "cache.json"
        if cache_file.exists():
            try:
                with open(cache_file) as f:
                    data = json.load(f)

                current_time = time.time()
                for entry_data in data.get("entries", []):
                    if current_time - entry_data["timestamp"] < self.ttl_seconds:
                        entry = CacheEntry(
                            prompt_hash=entry_data["prompt_hash"],
                            prompt=entry_data["prompt"],
                            response=entry_data["response"],
                            timestamp=entry_data["timestamp"],
                            embedding=entry_data.get("embedding"),
                        )
                        self._cache[entry.prompt_hash] = entry

                logger.debug(f"Loaded {len(self._cache)} cache entries")
            except (json.JSONDecodeError, KeyError, OSError) as e:
                logger.warning(f"Failed to load cache: {e}")

    def _save_cache(self) -> None:
        """Save cache to disk."""
        cache_file = CACHE_DIR / "cache.json"
        try:
            entries = [
                {
                    "prompt_hash": e.prompt_hash,
                    "prompt": e.prompt,
                    "response": e.response,
                    "timestamp": e.timestamp,
                    "embedding": e.embedding,
                }
                for e in self._cache.values()
            ]

            with open(cache_file, "w") as f:
                json.dump({"entries": entries}, f)
        except OSError as e:
            logger.warning(f"Failed to save cache: {e}")

    def _get_hash(self, prompt: str) -> str:
        """Generate hash for a prompt."""
        normalized = self._normalize_prompt(prompt)
        return hashlib.md5(normalized.encode()).hexdigest()

    def _normalize_prompt(self, prompt: str) -> str:
        """Normalize prompt for comparison.

        Args:
            prompt: Original prompt.

        Returns:
            Normalized prompt.
        """
        text = prompt.lower().strip()
        text = re.sub(r"\s+", " ", text)
        return text

    def get(self, prompt: str) -> str | None:
        """Check cache for a matching response.

        First tries exact match, then semantic similarity if embeddings enabled.

        Args:
            prompt: The prompt to look up.

        Returns:
            Cached response if found, None otherwise.
        """
        with self._lock:
            prompt_hash = self._get_hash(prompt)

            if prompt_hash in self._cache:
                entry = self._cache[prompt_hash]
                if time.time() - entry.timestamp < self.ttl_seconds:
                    logger.debug("Cache hit (exact match)")
                    return entry.response
                else:
                    del self._cache[prompt_hash]

            if not self.use_embeddings:
                return None

            embedding_service = self._get_embedding_service()
            if not embedding_service:
                return None

            try:
                query_embedding = embedding_service.embed_text(prompt)

                best_match: CacheEntry | None = None
                best_similarity = 0.0

                current_time = time.time()
                for entry in list(self._cache.values()):
                    if current_time - entry.timestamp >= self.ttl_seconds:
                        del self._cache[entry.prompt_hash]
                        continue

                    if entry.embedding is None:
                        entry.embedding = embedding_service.embed_text(entry.prompt)

                    similarity = embedding_service.cosine_similarity(
                        query_embedding, entry.embedding
                    )

                    if similarity > best_similarity:
                        best_similarity = similarity
                        best_match = entry

                if best_match and best_similarity >= self.similarity_threshold:
                    logger.debug(
                        f"Cache hit (semantic, similarity={best_similarity:.3f})"
                    )
                    return best_match.response

            except Exception as e:
                logger.warning(f"Semantic cache lookup failed: {e}")

        return None

    def set(self, prompt: str, response: str) -> None:
        """Store a prompt-response pair in cache.

        Args:
            prompt: The prompt.
            response: The response to cache.
        """
        with self._lock:
            if len(self._cache) >= self.max_entries:
                oldest_hash = min(
                    self._cache.keys(),
                    key=lambda k: self._cache[k].timestamp,
                )
                del self._cache[oldest_hash]

            prompt_hash = self._get_hash(prompt)

            embedding = None
            if self.use_embeddings:
                try:
                    embedding_service = self._get_embedding_service()
                    if embedding_service:
                        embedding = embedding_service.embed_text(prompt)
                except Exception as e:
                    logger.warning(f"Failed to compute embedding: {e}")

            entry = CacheEntry(
                prompt_hash=prompt_hash,
                prompt=prompt,
                response=response,
                timestamp=time.time(),
                embedding=embedding,
            )

            self._cache[prompt_hash] = entry
            self._save_cache()

    def invalidate(self, pattern: str) -> int:
        """Invalidate cache entries matching a pattern.

        Args:
            pattern: Regex pattern to match against prompts.

        Returns:
            Number of entries invalidated.
        """
        with self._lock:
            regex = re.compile(pattern, re.IGNORECASE)
            to_remove = [
                hash_key
                for hash_key, entry in self._cache.items()
                if regex.search(entry.prompt)
            ]

            for hash_key in to_remove:
                del self._cache[hash_key]

            if to_remove:
                self._save_cache()

            return len(to_remove)

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._save_cache()

    def stats(self) -> dict:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats.
        """
        with self._lock:
            current_time = time.time()
            valid_entries = [
                e
                for e in self._cache.values()
                if current_time - e.timestamp < self.ttl_seconds
            ]

            return {
                "total_entries": len(self._cache),
                "valid_entries": len(valid_entries),
                "max_entries": self.max_entries,
                "ttl_hours": self.ttl_seconds / 3600,
                "similarity_threshold": self.similarity_threshold,
                "use_embeddings": self.use_embeddings,
            }


_cache_instance: SemanticCache | None = None


def get_cache() -> SemanticCache:
    """Get the global semantic cache instance.

    Returns:
        Semantic cache instance.
    """
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = SemanticCache()
    return _cache_instance


def get_cached(prompt: str) -> str | None:
    """Convenience function to check cache.

    Args:
        prompt: Prompt to look up.

    Returns:
        Cached response or None.
    """
    return get_cache().get(prompt)


def set_cached(prompt: str, response: str) -> None:
    """Convenience function to store in cache.

    Args:
        prompt: The prompt.
        response: The response.
    """
    get_cache().set(prompt, response)


def invalidate_cache(pattern: str) -> int:
    """Convenience function to invalidate cache entries.

    Args:
        pattern: Pattern to match.

    Returns:
        Number of entries invalidated.
    """
    return get_cache().invalidate(pattern)
