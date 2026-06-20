"""LLM reranker orchestration for the matching pipeline.

Wires candidate fields through the LLM client for semantic judgment,
building rich context for each field before sending to the LLM.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ddharmon.matching.candidate_retrieval import build_field_context

if TYPE_CHECKING:
    from ddharmon.llm.base import BaseLLMClient
    from ddharmon.llm.prompts import CandidateJudgment
    from ddharmon.models.data_dictionary import DataDictionary, Field

logger = logging.getLogger(__name__)


def rerank_candidates(
    client: BaseLLMClient,
    source_field: Field,
    source_dict: DataDictionary,
    candidate_fields: list[tuple[Field, float]],
    target_dict: DataDictionary,
) -> list[tuple[CandidateJudgment, float]]:
    """Rerank candidate matches using LLM semantic judgment.

    Builds rich context for the source field and each candidate, then sends
    a single batch request to the LLM per source field (per locked decision).

    Args:
        client: LLM client implementing BaseLLMClient.
        source_field: The source field being matched.
        source_dict: Source DataDictionary (for parent lookups).
        candidate_fields: List of (Field, cosine_score) tuples for candidates.
        target_dict: Target DataDictionary (for parent lookups).

    Returns:
        List of (CandidateJudgment, cosine_score) tuples, one per candidate
        that the LLM returned a judgment for.
    """
    if not candidate_fields:
        return []

    # Build contexts
    source_context = build_field_context(source_field, source_dict)

    candidate_contexts: list[dict[str, str]] = []
    candidate_names: list[str] = []
    cosine_scores: dict[str, float] = {}

    for cand_field, cosine_score in candidate_fields:
        ctx = build_field_context(cand_field, target_dict)
        candidate_contexts.append(ctx)
        candidate_names.append(cand_field.variable_name)
        cosine_scores[cand_field.variable_name] = cosine_score

    # Call LLM (one batch per source field)
    response = client.rerank_candidates(source_context, candidate_contexts, candidate_names)

    # Defensive: warn if LLM returned fewer judgments than candidates
    if len(response.judgments) < len(candidate_fields):
        logger.warning(
            "LLM returned %d judgments for %d candidates (source: %s)",
            len(response.judgments),
            len(candidate_fields),
            source_field.variable_name,
        )

    # Pair judgments with cosine scores
    results: list[tuple[CandidateJudgment, float]] = []
    for judgment in response.judgments:
        cos_score = cosine_scores.get(judgment.candidate_variable, 0.0)
        results.append((judgment, cos_score))

    return results
