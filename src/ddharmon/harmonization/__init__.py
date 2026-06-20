"""Sub-cluster-anchored CDE harmonization (v1).

Pipeline: semantic cluster -> value sub-cluster -> CDE anchor -> classify
(adopt/refine/novel) -> EITL. The single LLM call is the classify-only pass;
coherence judging, concept labeling, and spec authoring are out of v1.

Public API:
    harmonize_dictionaries() -- full pipeline (cluster -> sub-cluster -> anchor -> classify)
    prepare_from_clusters()  -- sub-cluster/anchor/gate/prompt from precomputed clusters
    assemble_verdicts()      -- combine deterministic + LLM responses into verdicts
    find_anchor_cde()        -- CDE anchor selection for one sub-cluster
    HarmonizationResult / HarmonizationVerdict / AnchorResult / SubClusterResult
"""

from ddharmon.harmonization.anchor import (
    CDE_COHORT,
    build_field_lookup,
    canonicalness_score,
    field_richness,
    find_anchor_cde,
)
from ddharmon.harmonization.models import (
    AnchorResult,
    HarmonizationVerdict,
)
from ddharmon.harmonization.parse import parse_verdict_payload
from ddharmon.harmonization.pipeline import (
    HarmonizationResult,
    PromptRecord,
    assemble_verdicts,
    export_eitl_queue,
    harmonize_dictionaries,
    prepare_from_clusters,
    write_buckets,
    write_prompts_jsonl,
)
from ddharmon.models.cluster import SubClusterResult

__all__ = [
    "CDE_COHORT",
    "AnchorResult",
    "HarmonizationResult",
    "HarmonizationVerdict",
    "PromptRecord",
    "SubClusterResult",
    "assemble_verdicts",
    "build_field_lookup",
    "canonicalness_score",
    "export_eitl_queue",
    "field_richness",
    "find_anchor_cde",
    "harmonize_dictionaries",
    "parse_verdict_payload",
    "prepare_from_clusters",
    "write_buckets",
    "write_prompts_jsonl",
]
