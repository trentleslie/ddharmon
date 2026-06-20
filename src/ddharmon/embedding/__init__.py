"""Embedding layer for ddharmon data dictionary harmonization.

Provides embedding generation, caching, and text composition for
dictionary fields using local sentence-transformer models.

Public API:
    embed_dictionary() -- Orchestrate embedding with caching
    EmbeddedDictionary -- Result type with vectors and similarity search
    find_similar() -- Top-k cosine similarity search
    EmbeddingProvider -- ABC for embedding backends
    SentenceTransformerProvider -- Default local CPU provider
    EmbeddingCache -- SQLite-backed embedding cache
    compose_embedding_text() -- Text composition with parent context
    composed_content_hash() -- Content hash for cache keying
"""

from ddharmon.embedding.cache import EmbeddingCache
from ddharmon.embedding.composer import compose_embedding_text, composed_content_hash
from ddharmon.embedding.provider import EmbeddingProvider, SentenceTransformerProvider
from ddharmon.embedding.service import EmbeddedDictionary, embed_dictionary, find_similar

__all__ = [
    "EmbeddedDictionary",
    "EmbeddingCache",
    "EmbeddingProvider",
    "SentenceTransformerProvider",
    "compose_embedding_text",
    "composed_content_hash",
    "embed_dictionary",
    "find_similar",
]
