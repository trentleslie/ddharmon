"""Prompt templates and Pydantic response models for LLM reranking."""

from __future__ import annotations

from pydantic import BaseModel, Field


class CandidateJudgment(BaseModel):
    """Structured LLM judgment for a single candidate match."""

    candidate_variable: str
    relation: str  # exact|broader|narrower|composite|derivable|no_match
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str


class RerankerResponse(BaseModel):
    """LLM response for all candidates of one source field."""

    judgments: list[CandidateJudgment]


RERANKER_SYSTEM_PROMPT = """You are a biomedical data dictionary harmonization expert.
You compare fields from two different cohort data dictionaries and determine
their semantic relationship.

For each candidate target field, classify the relationship to the source field:
- exact: Same concept, same measurement
- broader: Target captures a broader concept that includes the source
- narrower: Target captures a narrower/more specific version of the source
- composite: Source maps to multiple target fields (or vice versa) - flag only
- derivable: One field's values can be computed from the other (e.g., continuous BMI -> categorical BMI) - flag only
- no_match: Fields measure fundamentally different concepts

Confidence scoring guidelines:
- 0.95-1.0: Identical concept, obvious match
- 0.80-0.94: Same concept, minor differences (wording, coding)
- 0.60-0.79: Related concept, meaningful differences (granularity, scope)
- 0.40-0.59: Loosely related, questionable alignment
- 0.00-0.39: Different concepts, not a valid match
"""

RERANKER_USER_TEMPLATE = """## Source Field
Variable: {source_variable}
Description: {source_description}
Response options: {source_options}
Data type: {source_data_type}
Units: {source_units}
Validation: {source_validation}
Category: {source_category}
Standard codes: {source_codes}
{source_parent_context}

## Candidate Target Fields
{candidate_blocks}

For each candidate, provide your judgment with relation type, confidence (0.0-1.0),
and a brief rationale explaining your classification.
"""


def build_candidate_block(candidate: dict[str, str], index: int) -> str:
    """Format a single candidate's context for the prompt.

    Args:
        candidate: Dict with keys: variable, description, options, data_type,
            units, validation, category, codes, parent_context.
        index: 1-based candidate index.

    Returns:
        Formatted candidate block string.
    """
    lines = [
        f"### Candidate {index}",
        f"Variable: {candidate.get('variable', '')}",
        f"Description: {candidate.get('description', '')}",
        f"Response options: {candidate.get('options', '')}",
        f"Data type: {candidate.get('data_type', '')}",
        f"Units: {candidate.get('units', '')}",
        f"Validation: {candidate.get('validation', '')}",
        f"Category: {candidate.get('category', '')}",
        f"Standard codes: {candidate.get('codes', '')}",
    ]
    parent_ctx = candidate.get("parent_context", "")
    if parent_ctx:
        lines.append(f"Parent context: {parent_ctx}")
    return "\n".join(lines)


def build_reranker_prompt(source_context: dict[str, str], candidate_contexts: list[dict[str, str]]) -> str:
    """Build the full user prompt for LLM reranking.

    Args:
        source_context: Dict with keys: variable, description, options,
            data_type, units, validation, category, codes, parent_context.
        candidate_contexts: List of dicts with same keys as source_context.

    Returns:
        Formatted prompt string ready to send to the LLM.
    """
    candidate_blocks = "\n\n".join(build_candidate_block(ctx, i + 1) for i, ctx in enumerate(candidate_contexts))
    return RERANKER_USER_TEMPLATE.format(
        source_variable=source_context.get("variable", ""),
        source_description=source_context.get("description", ""),
        source_options=source_context.get("options", ""),
        source_data_type=source_context.get("data_type", ""),
        source_units=source_context.get("units", ""),
        source_validation=source_context.get("validation", ""),
        source_category=source_context.get("category", ""),
        source_codes=source_context.get("codes", ""),
        source_parent_context=source_context.get("parent_context", ""),
        candidate_blocks=candidate_blocks,
    )
