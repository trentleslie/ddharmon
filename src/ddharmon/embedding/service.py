"""Embedding orchestrator and result types.

Wires the provider, cache, and composer into the top-level embed_dictionary()
function. Provides EmbeddedDictionary result type with similarity search.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
from numpy.typing import NDArray

from ddharmon.embedding.cache import EmbeddingCache
from ddharmon.embedding.composer import (
    compose_embedding_text,
    compose_value_content_hash,
    compose_value_text,
    composed_content_hash,
)
from ddharmon.embedding.provider import EmbeddingProvider, SentenceTransformerProvider
from ddharmon.models.data_dictionary import DataDictionary, Field

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Semantic vector includes: variable_name (optional), primary_text (question_text or
# description fallback), category, and parent_context (injected by compose_embedding_text).
# Response options, data_type, and units belong to the value vector, not semantic.
SEMANTIC_INCLUDE: set[str] = {"category"}


@dataclass
class EmbeddedDictionary:
    """Result of embedding a data dictionary.

    Contains the original dictionary, all embeddings keyed by variable name,
    and the model name used. Provides convenience methods for retrieving
    vectors in a consistent order for downstream matching.

    Attributes:
        dictionary: The source DataDictionary that was embedded.
        embeddings: Mapping of variable_name -> embedding vector.
        model_name: Identifier of the embedding model used.
    """

    dictionary: DataDictionary
    embeddings: dict[str, NDArray[np.float32]] = field(default_factory=dict)
    model_name: str = ""
    value_embeddings: dict[str, NDArray[np.float32]] = field(default_factory=dict)

    def get_all_vectors(self) -> NDArray[np.float32]:
        """Return (N, D) matrix of all semantic embeddings ordered by sorted variable names.

        Returns:
            Float32 array of shape (field_count, embedding_dimension).
        """
        names = self.get_variable_names()
        return np.stack([self.embeddings[name] for name in names])

    def get_variable_names(self) -> list[str]:
        """Return sorted variable names matching get_all_vectors() row order.

        Returns:
            Sorted list of variable names with semantic embeddings.
        """
        return sorted(self.embeddings.keys())

    def get_value_vectors(self) -> NDArray[np.float32]:
        """Return (N, D) matrix of value embeddings ordered by sorted variable names.

        Only includes fields that have value embeddings. Callers should check
        get_value_variable_names() first to know which fields are included.

        Returns:
            Float32 array of shape (value_field_count, embedding_dimension).

        Raises:
            ValueError: If no value embeddings exist.
        """
        names = self.get_value_variable_names()
        if not names:
            raise ValueError("no value embeddings exist")
        return np.stack([self.value_embeddings[name] for name in names])

    def get_value_variable_names(self) -> list[str]:
        """Return sorted variable names that have value embeddings.

        Returns:
            Sorted list of variable names with value embeddings.
        """
        return sorted(self.value_embeddings.keys())


def embed_dictionary(
    dictionary: DataDictionary,
    *,
    provider: EmbeddingProvider | None = None,
    cache_dir: Path | None = None,
    text_composer: Callable[[Field, DataDictionary], str] | None = None,
) -> EmbeddedDictionary:
    """Embed all fields in a data dictionary with caching.

    Orchestrates the provider, cache, and composer to produce embeddings for
    every field. Previously embedded fields are retrieved from cache; only
    new/changed fields are sent to the provider.

    Args:
        dictionary: The DataDictionary to embed.
        provider: Embedding provider (defaults to SentenceTransformerProvider).
        cache_dir: Directory for the SQLite embedding cache (defaults to .ddharmon).
        text_composer: Custom text composition function. If None, uses
            compose_embedding_text() with parent context injection.

    Returns:
        EmbeddedDictionary with one embedding per field.
    """
    if provider is None:
        provider = SentenceTransformerProvider()
    if cache_dir is None:
        cache_dir = Path(".ddharmon")

    cache = EmbeddingCache(cache_dir / "embeddings.db", provider.dimension)

    t0 = time.perf_counter()
    try:
        result = _embed_with_cache(dictionary, provider, cache, text_composer)
    finally:
        cache.close()
    elapsed = time.perf_counter() - t0
    logger.info(
        "embed_dictionary(%s): %d semantic, %d value embeddings in %.2fs",
        dictionary.name or "unnamed",
        len(result.embeddings),
        len(result.value_embeddings),
        elapsed,
    )
    return result


def _embed_with_cache(
    dictionary: DataDictionary,
    provider: EmbeddingProvider,
    cache: EmbeddingCache,
    text_composer: Callable[[Field, DataDictionary], str] | None,
) -> EmbeddedDictionary:
    """Internal: embed dictionary fields with cache lookup and store.

    Produces both semantic and value embeddings when using the default composer.
    Custom text_composer only produces semantic vectors (not multivector-aware).
    """
    using_custom_composer = text_composer is not None

    # --- Pass 1: Semantic embeddings ---
    sem_texts: dict[str, str] = {}
    sem_hashes: dict[str, str] = {}

    for var_name, fld in dictionary.fields.items():
        if using_custom_composer:
            text = text_composer(fld, dictionary)
            content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
        else:
            text = compose_embedding_text(fld, dictionary, include=SEMANTIC_INCLUDE)
            content_hash = composed_content_hash(fld, dictionary, include=SEMANTIC_INCLUDE)
        sem_texts[var_name] = text
        sem_hashes[var_name] = content_hash

    short_text_count = sum(1 for t in sem_texts.values() if len(t) < 20)

    embeddings = _embed_pass(
        dictionary, provider, cache, sem_texts, sem_hashes, vector_type="semantic"
    )

    # --- Pass 2: Value embeddings (only with default composer) ---
    value_embeddings: dict[str, NDArray[np.float32]] = {}
    val_skipped = 0

    if not using_custom_composer:
        val_texts: dict[str, str] = {}
        val_hashes: dict[str, str] = {}

        for var_name, fld in dictionary.fields.items():
            vtext = compose_value_text(fld)
            if not vtext:
                val_skipped += 1
                continue
            val_texts[var_name] = vtext
            val_hashes[var_name] = compose_value_content_hash(fld)

        if val_texts:
            value_embeddings = _embed_pass(
                dictionary, provider, cache, val_texts, val_hashes, vector_type="value"
            )

    total = len(dictionary.fields)
    sem_cached = total - sum(1 for v in embeddings.values() if v is not None) + len(embeddings)
    val_total = len(value_embeddings)
    if short_text_count > 0:
        logger.warning(
            "%d fields have short embedding text (<20 chars) — may produce low-quality embeddings",
            short_text_count,
        )

    logger.info(
        "Embedding %d fields: semantic (%d), value (%d produced, %d skipped)",
        total,
        len(embeddings),
        val_total,
        val_skipped,
    )

    return EmbeddedDictionary(
        dictionary=dictionary,
        embeddings=embeddings,
        model_name=provider.model_name,
        value_embeddings=value_embeddings,
    )


def _embed_pass(
    dictionary: DataDictionary,
    provider: EmbeddingProvider,
    cache: EmbeddingCache,
    field_texts: dict[str, str],
    field_hashes: dict[str, str],
    vector_type: str,
) -> dict[str, NDArray[np.float32]]:
    """Embed a set of fields with cache lookup/store for a given vector type.

    Args:
        dictionary: The source DataDictionary.
        provider: Embedding provider.
        cache: Embedding cache.
        field_texts: Mapping of variable_name -> composed text.
        field_hashes: Mapping of variable_name -> content hash.
        vector_type: Cache vector type key ('semantic' or 'value').

    Returns:
        Dict of variable_name -> embedding vector.
    """
    all_hashes = list(field_hashes.values())
    cached = cache.get_many(provider.model_name, all_hashes, vector_type=vector_type)

    result: dict[str, NDArray[np.float32]] = {}
    uncached_varnames: list[str] = []
    uncached_texts: list[str] = []
    uncached_hashes: list[str] = []

    for var_name in sorted(field_texts.keys()):
        h = field_hashes[var_name]
        if h in cached:
            result[var_name] = cached[h]
        else:
            uncached_varnames.append(var_name)
            uncached_texts.append(field_texts[var_name])
            uncached_hashes.append(h)

    cached_count = len(result)
    new_count = len(uncached_varnames)

    logger.info(
        "%s embeddings: %d cached, %d new",
        vector_type,
        cached_count,
        new_count,
    )

    if uncached_texts:
        new_vectors = provider.embed(uncached_texts)
        new_items: list[tuple[str, NDArray[np.float32]]] = []

        for i, var_name in enumerate(uncached_varnames):
            vec = new_vectors[i]
            result[var_name] = vec
            new_items.append((uncached_hashes[i], vec))

        cache.put_many(provider.model_name, new_items, vector_type=vector_type)

    return result


def find_similar(
    query_embedding: NDArray[np.float32],
    candidate_embeddings: NDArray[np.float32],
    top_k: int = 10,
) -> list[tuple[int, float]]:
    """Find the most similar candidates to a query by cosine similarity.

    Assumes L2-normalized vectors (cosine similarity = dot product).

    Args:
        query_embedding: Query vector of shape (D,).
        candidate_embeddings: Candidate matrix of shape (N, D).
        top_k: Number of top results to return.

    Returns:
        List of (index, similarity_score) tuples sorted by descending score.
    """
    similarities = candidate_embeddings @ query_embedding
    top_indices = np.argsort(similarities)[::-1][:top_k]
    return [(int(idx), float(similarities[idx])) for idx in top_indices]
