"""Data models for pairwise field mapping results."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from ddharmon.models.enums import Relation, ReviewStatus, UnmappedReason


@dataclass
class FieldMapping:
    """A single field-level mapping between source and target dictionaries."""

    source_variable: str
    target_variable: str
    relation: Relation
    confidence: float  # Composite score (0.0-1.0)
    cosine_similarity: float
    llm_confidence: float
    rationale: str
    review_status: ReviewStatus
    # Provenance
    method: str = "embed_rerank"
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class UnmappedField:
    """A field that could not be mapped, with reason."""

    variable_name: str
    reason: UnmappedReason
    rejected_candidates: list[FieldMapping] = field(default_factory=list)


@dataclass
class MappingResult:
    """Complete result of pairwise matching between two dictionaries."""

    source_name: str
    target_name: str
    mappings: list[FieldMapping]
    source_unmapped: list[UnmappedField]
    target_unmapped: list[UnmappedField]
    # Config used
    top_k: int = 5
    cosine_threshold: float = 0.3
    auto_approve_threshold: float = 0.9
    auto_reject_threshold: float = 0.3
    # Stats
    timestamp: datetime = field(default_factory=datetime.now)
    model_name: str = ""
    llm_provider: str = ""

    @property
    def auto_approved(self) -> list[FieldMapping]:
        """Mappings that were auto-approved (high confidence)."""
        return [m for m in self.mappings if m.review_status == ReviewStatus.AUTO_APPROVED]

    @property
    def pending_review(self) -> list[FieldMapping]:
        """Mappings queued for human review (ambiguous confidence)."""
        return [m for m in self.mappings if m.review_status == ReviewStatus.PENDING_REVIEW]

    @property
    def auto_rejected(self) -> list[FieldMapping]:
        """Mappings that were auto-rejected (low confidence)."""
        return [m for m in self.mappings if m.review_status == ReviewStatus.AUTO_REJECTED]
