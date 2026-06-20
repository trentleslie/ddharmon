"""Data models for sub-cluster-anchored CDE harmonization (v1).

Plain dataclasses (not Pydantic), with __post_init__ validation. The v1 pipeline:

    semantic cluster  ->  value sub-cluster  ->  CDE anchor  ->  adopt/refine/novel

These models carry the result of each stage so the orchestrator
(``harmonization.pipeline``) and downstream consumers (EITL export) can work
without re-deriving anything from the notebook globals the logic used to live in.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ddharmon.models.cluster import FieldReference
from ddharmon.models.data_dictionary import Field

# Verdict vocabularies, by gate mode. ``harmonize`` mode (cluster has response-
# option data) emits the full three-way verdict; ``kg_only`` mode (concept-only,
# no machine-readable encodings) emits a concept-level two-way verdict.
HARMONIZE_VERDICTS = ("adopt", "refine", "novel")
KGONLY_VERDICTS = ("adopt", "unaligned")


@dataclass
class AnchorResult:
    """CDE anchor recommendation for one sub-cluster.

    The anchor is the in-sub-cluster CDE most central to the members (ranked by
    similarity to the medoid, then canonicalness, then metadata richness).
    ``has_cde`` is False when no CDE landed in the sub-cluster — a GenCDE is
    needed. ``alternate_cdes`` are the runner-up CDEs as ``(ref, field, sim)``.
    """

    has_cde: bool
    anchor_ref: FieldReference | None = None
    anchor_field: Field | None = None
    medoid_ref: FieldReference | None = None
    medoid_sim: float | None = None
    alternate_cdes: list[tuple[FieldReference, Field, float]] = field(default_factory=list)


@dataclass
class HarmonizationVerdict:
    """The adopt/refine/novel recommendation for one sub-cluster.

    This is the v1 deliverable per sub-cluster — routed to EITL for human
    verification. No transformation spec is authored (deferred to v1.1+).
    """

    sub_cluster_id: str  # f"{parent_topic_id}:{sub_label}"
    parent_topic_id: int
    sub_label: int
    mode: str  # harmonize | kg_only | single_cohort | cde_only | noise
    verdict: str  # adopt | refine | novel | unaligned | "" (no LLM call)
    parent_cde_id: str | None = None
    confidence: float | None = None
    evidence: str = ""
    label: str = ""  # derived (c-TF-IDF) sub-cluster label
    cohorts: list[str] = field(default_factory=list)
    n_fields: int = 0  # non-CDE cohort fields
    encoded_fraction: float = 0.0
    anchor_designation: str | None = None
    decided_by: str = "llm"  # llm | deterministic
    raw: dict = field(default_factory=dict)
