"""Candidate retrieval and field context building for the matching pipeline.

Provides cosine-similarity-based candidate retrieval using pre-computed embeddings,
and builds rich context dicts for LLM reranking prompts.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np

from ddharmon.embedding.service import EmbeddedDictionary, find_similar
from ddharmon.models.data_dictionary import DataDictionary, Field

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def retrieve_candidates(
    source: EmbeddedDictionary,
    target: EmbeddedDictionary,
    top_k: int = 5,
    cosine_threshold: float = 0.3,
    *,
    semantic_weight: float = 1.0,
    value_weight: float = 0.0,
) -> dict[str, list[tuple[str, float]]]:
    """Retrieve top-k candidate matches for each source field from the target dictionary.

    For each source field, computes cosine similarity against all target embeddings
    using find_similar() for initial retrieval, then re-scores candidates using a
    blended score when both source and target have response_options AND value embeddings:
        blended = semantic_weight * cos_semantic + value_weight * cos_value

    When either field lacks response_options or a value embedding, the score falls
    back to the raw semantic cosine. This prevents thin value vectors (from data_type
    or encoding metadata alone) from penalizing good semantic matches — e.g., a
    free-text field that is semantically correct but has no answer-pattern overlap.

    Args:
        source: Embedded source dictionary.
        target: Embedded target dictionary.
        top_k: Maximum candidates per source field.
        cosine_threshold: Minimum blended cosine similarity to include.
        semantic_weight: Weight for semantic cosine in blended score (default 0.7).
        value_weight: Weight for value cosine in blended score (default 0.3).

    Returns:
        Dict mapping source variable name -> list of (target_variable_name, blended_score)
        tuples sorted by descending score.
    """
    target_vectors = target.get_all_vectors()
    target_names = target.get_variable_names()

    results: dict[str, list[tuple[str, float]]] = {}

    # Pre-compute which fields have response_options (rich value signal).
    # Fields with only data_type/encoding metadata produce weak value vectors
    # that can penalize good semantic matches (e.g., free-text targets).
    src_has_options = {
        name for name, fld in source.dictionary.fields.items() if fld.response_options
    }
    tgt_has_options = {
        name for name, fld in target.dictionary.fields.items() if fld.response_options
    }

    for src_name in sorted(source.embeddings.keys()):
        query_vec = source.embeddings[src_name]
        raw_matches = find_similar(query_vec, target_vectors, top_k=top_k)

        # Map indices to variable names with semantic scores
        candidates: list[tuple[str, float]] = []
        for idx, sem_score in raw_matches:
            tgt_name = target_names[idx]

            # Blended re-scoring: only when both have response_options AND value vectors.
            # Thin value embeddings (just data_type/encoding, no response options) can
            # penalize good semantic matches, so we fall back to semantic-only.
            if (
                src_name in src_has_options
                and tgt_name in tgt_has_options
                and src_name in source.value_embeddings
                and tgt_name in target.value_embeddings
            ):
                val_score = float(np.dot(source.value_embeddings[src_name], target.value_embeddings[tgt_name]))
                blended = semantic_weight * sem_score + value_weight * val_score
            else:
                blended = sem_score

            if blended >= cosine_threshold:
                candidates.append((tgt_name, blended))

        # Re-sort by blended score descending
        candidates.sort(key=lambda c: c[1], reverse=True)

        results[src_name] = candidates

    logger.debug(
        "Retrieved candidates for %d source fields (top_k=%d, threshold=%.2f)",
        len(results),
        top_k,
        cosine_threshold,
    )

    return results


def build_field_context(field: Field, dictionary: DataDictionary) -> dict[str, str]:
    """Build a rich context dict from a Field for LLM reranking prompts.

    The LLM is designing harmonization/transformation rules, so we give it
    every signal that could inform value mapping. Unlike the semantic vector
    (which uses only question_text-or-description as a single primary signal),
    the LLM sees BOTH question_text and description when both are populated
    and distinct — "maximum information for transformation accuracy".

    Args:
        field: The Field to extract context from.
        dictionary: The parent DataDictionary (for parent field lookup).

    Returns:
        Dict with keys: variable, description, options, data_type, units,
        validation, category, codes, parent_context.
    """
    # Description block: show both question_text and description when both are
    # populated and differ. Fall back to whichever one is present otherwise.
    if (
        field.question_text
        and field.description
        and field.question_text.strip() != field.description.strip()
    ):
        description = f"{field.question_text}\n  Definition: {field.description}"
    else:
        description = field.question_text or field.description or ""

    # Response options (comma-joined code=label pairs)
    options = ", ".join(f"{opt.code}={opt.label}" for opt in field.response_options) if field.response_options else ""

    # Standard codes from standard_codes dict
    code_parts: list[str] = []
    for system, codes in sorted(field.standard_codes.items()):
        for code in codes:
            code_parts.append(f"{system}:{code}")
    codes_str = ", ".join(code_parts)

    # Parent context for hierarchy-aware matching (MATCH-07)
    parent_context = ""
    if field.parent_field_id:
        parent = dictionary.fields.get(field.parent_field_id)
        if parent is not None:
            parent_context = f"Parent question: {parent.description}"

    return {
        "variable": field.variable_name,
        "description": description,
        "options": options,
        "data_type": field.data_type or "",
        "units": field.units or "",
        "validation": field.validation or "",
        "category": field.category or "",
        "codes": codes_str,
        "parent_context": parent_context,
    }
