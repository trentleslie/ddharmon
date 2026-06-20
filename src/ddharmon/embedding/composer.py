"""Embedding text composition for semantic and value vectors.

Composes rich text for embedding generation:
- Semantic text: field name/description with parent context injection
- Value text: answer-meaning text from response options, encoding type, data type, units
"""

from __future__ import annotations

import hashlib
import logging

from ddharmon.models.data_dictionary import DataDictionary, Field

logger = logging.getLogger(__name__)

_MIN_TEXT_LENGTH = 20


def compose_embedding_text(
    field: Field, dictionary: DataDictionary, include: set[str] | None = None
) -> str:
    """Compose embedding text with parent context for child fields.

    For root fields, returns field.to_embedding_text() unchanged.
    For child fields with a parent in the dictionary, prepends the
    parent's description to provide context.

    Args:
        field: The field to compose text for.
        dictionary: The containing dictionary (for parent lookups).
        include: Optional set of field names to include in embedding text.
            Passed through to field.to_embedding_text(). If None, all
            populated fields are included.

    Returns:
        Composed text string suitable for embedding generation.
    """
    base_text = field.to_embedding_text(include=include)

    if field.parent_field_id:
        parent = dictionary.fields.get(field.parent_field_id)
        if parent:
            base_text = f"{parent.description}: {base_text}"

    if len(base_text) < _MIN_TEXT_LENGTH:
        logger.debug(
            "Field '%s' has short embedding text (%d chars)",
            field.variable_name,
            len(base_text),
        )

    return base_text


def compose_value_text(field: Field) -> str:
    """Compose answer-meaning text from a Field's value metadata.

    Builds text from response option labels, data type, and units.
    Response options come first (most discriminative signal), then data_type
    and units -- all separated by " | ". Only non-empty parts are included.

    No parent context injection: value text describes answer structure, not semantics.

    TODO: In cosine-only mode (no LLM), value embeddings are not used for retrieval
        or scoring. Investigate whether value vectors could improve cosine-only
        results via blended scoring when both sides have response options.

    Args:
        field: The field to compose value text for.

    Returns:
        Composed value text string, or empty string if no value metadata populated.
    """
    parts: list[str] = []

    # Response option labels (no codes)
    if field.response_options:
        labels = "; ".join(opt.label for opt in field.response_options)
        parts.append(labels)

    # Data type (raw source string)
    if field.data_type:
        parts.append(field.data_type)

    # Units
    if field.units:
        parts.append(field.units)

    return " | ".join(parts)


def compose_value_content_hash(field: Field) -> str:
    """Compute content hash of the composed value text.

    SHA256[:16] of compose_value_text() output. Parallel to
    composed_content_hash() but for value vectors.

    Args:
        field: The field to hash.

    Returns:
        16-character hex SHA256 hash of the value text.
    """
    text = compose_value_text(field)
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def composed_content_hash(field: Field, dictionary: DataDictionary, include: set[str] | None = None) -> str:
    """Compute content hash of the composed embedding text.

    This hash is separate from Field.content_hash() because it includes
    parent context. Used as the cache key for embeddings.

    Args:
        field: The field to hash.
        dictionary: The containing dictionary (for parent lookups).
        include: Optional set of field names to include (passed through
            to compose_embedding_text).

    Returns:
        16-character hex SHA256 hash of the composed text.
    """
    text = compose_embedding_text(field, dictionary, include=include)
    return hashlib.sha256(text.encode()).hexdigest()[:16]
