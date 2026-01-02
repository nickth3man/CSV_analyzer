"""Embedding service for semantic search.

This module provides vector embeddings for semantic search and caching,
as specified in design.md Section 4.3.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np


logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent.parent / "data" / "embeddings_cache"
EMBEDDING_DIM = 1536  # openai/text-embedding-3-small dimension (OpenRouter)
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_EMBEDDING_MODEL = "openai/text-embedding-3-small"

if TYPE_CHECKING:
    from openai import OpenAI


class EmbeddingService:
    """Service for generating and searching embeddings.

    Supports OpenRouter embeddings with local caching for efficiency.
    Falls back to simple TF-IDF-like embeddings when API is unavailable.
    """

    def __init__(
        self,
        model: str | None = None,
        use_cache: bool = True,
    ) -> None:
        """Initialize the embedding service.

        Args:
            model: OpenRouter embedding model name.
            use_cache: Whether to cache embeddings locally.
        """
        self.model = (
            model
            or os.environ.get("OPENROUTER_EMBEDDING_MODEL")
            or DEFAULT_EMBEDDING_MODEL
        )
        self.use_cache = use_cache
        self._cache: dict[str, list[float]] = {}
        self._openai_client: OpenAI | None = None

        if use_cache:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            self._load_cache()

    def _get_openai_client(self) -> OpenAI | None:
        """Lazy-load OpenRouter OpenAI-compatible client."""
        if self._openai_client is None:
            try:
                from openai import OpenAI

                api_key = os.environ.get("OPENROUTER_API_KEY")
                if api_key:
                    self._openai_client = OpenAI(
                        api_key=api_key,
                        base_url=OPENROUTER_BASE_URL,
                    )
                else:
                    logger.warning(
                        "OPENROUTER_API_KEY not set, using fallback embeddings"
                    )
            except ImportError:
                logger.warning(
                    "OpenAI package not available, using fallback embeddings"
                )
        return self._openai_client

    def _load_cache(self) -> None:
        """Load cached embeddings from disk."""
        cache_file = CACHE_DIR / "embeddings.json"
        if cache_file.exists():
            try:
                with open(cache_file) as f:
                    self._cache = json.load(f)
                logger.debug(f"Loaded {len(self._cache)} cached embeddings")
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Failed to load embedding cache: {e}")

    def _save_cache(self) -> None:
        """Save cached embeddings to disk."""
        if not self.use_cache:
            return

        cache_file = CACHE_DIR / "embeddings.json"
        try:
            with open(cache_file, "w") as f:
                json.dump(self._cache, f)
        except OSError as e:
            logger.warning(f"Failed to save embedding cache: {e}")

    def _get_cache_key(self, text: str) -> str:
        """Generate a cache key for text."""
        return hashlib.sha256(text.encode()).hexdigest()

    @staticmethod
    def _normalize_embedding(embedding: list[float] | Any) -> list[float]:
        """Normalize embeddings into a list of floats."""
        return [float(value) for value in embedding]

    def embed_text(self, text: str) -> list[float]:
        """Generate embedding vector for text.

        Args:
            text: Text to embed.

        Returns:
            Embedding vector as list of floats.
        """
        cache_key = self._get_cache_key(text)

        if self.use_cache and cache_key in self._cache:
            return self._cache[cache_key]

        client = self._get_openai_client()

        if client:
            try:
                response = client.embeddings.create(
                    input=text,
                    model=self.model,
                )
                embedding = self._normalize_embedding(response.data[0].embedding)

                if self.use_cache:
                    self._cache[cache_key] = embedding
                    self._save_cache()

                return embedding
            except Exception as e:
                logger.warning(f"OpenRouter embedding failed, using fallback: {e}")

        return self._fallback_embedding(text)

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for multiple texts.

        Args:
            texts: List of texts to embed.

        Returns:
            List of embedding vectors.
        """
        results = []
        uncached_texts = []
        uncached_indices = []

        for i, text in enumerate(texts):
            cache_key = self._get_cache_key(text)
            if self.use_cache and cache_key in self._cache:
                results.append((i, self._cache[cache_key]))
            else:
                uncached_texts.append(text)
                uncached_indices.append(i)
                results.append((i, None))

        if uncached_texts:
            client = self._get_openai_client()

            if client:
                try:
                    response = client.embeddings.create(
                        input=uncached_texts,
                        model=self.model,
                    )
                    for j, embedding_data in enumerate(response.data):
                        idx = uncached_indices[j]
                        embedding = self._normalize_embedding(embedding_data.embedding)

                        for k, (result_idx, _) in enumerate(results):
                            if result_idx == idx:
                                results[k] = (idx, embedding)
                                break

                        if self.use_cache:
                            cache_key = self._get_cache_key(uncached_texts[j])
                            self._cache[cache_key] = embedding

                    if self.use_cache:
                        self._save_cache()

                except Exception as e:
                    logger.warning(f"OpenRouter batch embedding failed: {e}")
                    for j, text in enumerate(uncached_texts):
                        idx = uncached_indices[j]
                        embedding = self._fallback_embedding(text)
                        for k, (result_idx, _) in enumerate(results):
                            if result_idx == idx:
                                results[k] = (idx, embedding)
                                break
            else:
                for j, text in enumerate(uncached_texts):
                    idx = uncached_indices[j]
                    embedding = self._fallback_embedding(text)
                    for k, (result_idx, _) in enumerate(results):
                        if result_idx == idx:
                            results[k] = (idx, embedding)
                            break

        results.sort(key=lambda x: x[0])
        return [emb for _, emb in results]

    def _fallback_embedding(self, text: str) -> list[float]:
        """Generate a simple fallback embedding using hash-based approach.

        This is a deterministic fallback when the OpenRouter client is unavailable.
        Not suitable for production semantic search but allows basic operation.

        Args:
            text: Text to embed.

        Returns:
            Fallback embedding vector.
        """
        text_lower = text.lower()
        words = text_lower.split()

        embedding = [0.0] * EMBEDDING_DIM

        for i, word in enumerate(words):
            word_hash = int(hashlib.sha256(word.encode()).hexdigest(), 16)
            for j in range(min(10, EMBEDDING_DIM)):
                idx = (word_hash + j * 97) % EMBEDDING_DIM
                embedding[idx] += 1.0 / (i + 1)

        norm = sum(x * x for x in embedding) ** 0.5
        if norm > 0:
            embedding = [x / norm for x in embedding]

        return embedding

    def find_similar(
        self,
        query_vec: list[float],
        candidates: dict[str, list[float]],
        top_k: int = 10,
    ) -> list[str]:
        """Find top-k most similar items by cosine similarity.

        Args:
            query_vec: Query embedding vector.
            candidates: Dictionary mapping item names to their embeddings.
            top_k: Number of top results to return.

        Returns:
            List of item names sorted by similarity (most similar first).
        """
        if not candidates:
            return []

        query_arr = np.array(query_vec)
        query_norm = np.linalg.norm(query_arr)

        if query_norm == 0:
            return list(candidates.keys())[:top_k]

        similarities = []
        for name, vec in candidates.items():
            vec_arr = np.array(vec)
            vec_norm = np.linalg.norm(vec_arr)

            if vec_norm > 0:
                similarity = np.dot(query_arr, vec_arr) / (query_norm * vec_norm)
            else:
                similarity = 0.0

            similarities.append((name, similarity))

        similarities.sort(key=lambda x: x[1], reverse=True)

        return [name for name, _ in similarities[:top_k]]

    def cosine_similarity(self, vec1: list[float], vec2: list[float]) -> float:
        """Calculate cosine similarity between two vectors.

        Args:
            vec1: First embedding vector.
            vec2: Second embedding vector.

        Returns:
            Cosine similarity score between 0 and 1.
        """
        arr1 = np.array(vec1)
        arr2 = np.array(vec2)

        norm1 = np.linalg.norm(arr1)
        norm2 = np.linalg.norm(arr2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return float(np.dot(arr1, arr2) / (norm1 * norm2))


@lru_cache(maxsize=1)
def get_embedding_service() -> EmbeddingService:
    """Get the global embedding service instance.

    Returns:
        Embedding service instance.
    """
    return EmbeddingService()


def embed_text(text: str) -> list[float]:
    """Convenience function to embed text.

    Args:
        text: Text to embed.

    Returns:
        Embedding vector.
    """
    return get_embedding_service().embed_text(text)


def embed_batch(texts: list[str]) -> list[list[float]]:
    """Convenience function to embed multiple texts.

    Args:
        texts: Texts to embed.

    Returns:
        List of embedding vectors.
    """
    return get_embedding_service().embed_batch(texts)


def find_similar(
    query_vec: list[float],
    candidates: dict[str, list[float]],
    top_k: int = 10,
) -> list[str]:
    """Convenience function to find similar items.

    Args:
        query_vec: Query embedding.
        candidates: Dictionary of item names to embeddings.
        top_k: Number of results.

    Returns:
        List of similar item names.
    """
    return get_embedding_service().find_similar(query_vec, candidates, top_k)
