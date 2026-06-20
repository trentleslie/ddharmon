"""Confidence scoring and triage for field mappings.

Computes weighted composite scores from LLM confidence and cosine similarity,
then triages into auto_approved / pending_review / auto_rejected buckets.
"""

from __future__ import annotations

from dataclasses import dataclass

from ddharmon.models.enums import ReviewStatus


@dataclass
class ConfidenceConfig:
    """Configuration for confidence scoring and triage thresholds.

    Weights must sum to 1.0 (within floating-point tolerance).

    Attributes:
        llm_weight: Weight for LLM confidence in composite score.
        cosine_weight: Weight for cosine similarity in composite score.
        auto_approve_threshold: Minimum composite score for auto-approval.
        auto_reject_threshold: Maximum composite score for auto-rejection.
    """

    llm_weight: float = 0.6
    cosine_weight: float = 0.4
    auto_approve_threshold: float = 0.9
    auto_reject_threshold: float = 0.3

    def __post_init__(self) -> None:
        weight_sum = self.llm_weight + self.cosine_weight
        if abs(weight_sum - 1.0) > 1e-6:
            raise ValueError(f"Weights must sum to 1.0, got {weight_sum:.4f}")


_DEFAULT_CONFIG = ConfidenceConfig()


def score_mapping(
    llm_confidence: float,
    cosine_similarity: float,
    config: ConfidenceConfig | None = None,
) -> float:
    """Compute weighted composite confidence score.

    Args:
        llm_confidence: LLM-assigned confidence (0.0-1.0).
        cosine_similarity: Cosine similarity from embedding retrieval.
        config: Optional custom config (uses defaults if None).

    Returns:
        Composite score clamped to [0.0, 1.0].
    """
    if config is None:
        config = _DEFAULT_CONFIG

    raw = config.llm_weight * llm_confidence + config.cosine_weight * cosine_similarity
    return max(0.0, min(1.0, raw))


def triage_mapping(
    confidence: float,
    config: ConfidenceConfig | None = None,
) -> ReviewStatus:
    """Triage a mapping into review status based on confidence thresholds.

    Args:
        confidence: Composite confidence score (0.0-1.0).
        config: Optional custom config (uses defaults if None).

    Returns:
        ReviewStatus: AUTO_APPROVED, PENDING_REVIEW, or AUTO_REJECTED.
    """
    if config is None:
        config = _DEFAULT_CONFIG

    if confidence >= config.auto_approve_threshold:
        return ReviewStatus.AUTO_APPROVED
    elif confidence <= config.auto_reject_threshold:
        return ReviewStatus.AUTO_REJECTED
    else:
        return ReviewStatus.PENDING_REVIEW
