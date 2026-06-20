"""Top-level pairwise mapper orchestrator.

Wires candidate retrieval, LLM reranking, and confidence scoring into
a single match_dictionaries() call. Produces MappingResult with all
mappings, unmapped source fields (with reasons), and unmapped target fields.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from ddharmon.embedding.service import EmbeddedDictionary
from ddharmon.llm.base import BaseLLMClient
from ddharmon.matching.candidate_retrieval import retrieve_candidates
from ddharmon.matching.confidence import ConfidenceConfig, score_mapping, triage_mapping
from ddharmon.matching.reranker import rerank_candidates
from ddharmon.models.enums import Relation, UnmappedReason
from ddharmon.models.mapping import FieldMapping, MappingResult, UnmappedField

logger = logging.getLogger(__name__)


@dataclass
class MatchingConfig:
    """Configuration for the pairwise matching pipeline.

    Attributes:
        top_k: Maximum candidates per source field from cosine retrieval.
        cosine_threshold: Minimum cosine similarity to include a candidate.
        llm_provider: LLM provider name for reranking (e.g., "anthropic", "openai").
        llm_model: Specific model override (None = use provider default).
        confidence_config: Weights and thresholds for composite scoring and triage.
        semantic_weight: Weight for semantic cosine score in blended scoring.
        value_weight: Weight for value cosine score in blended scoring.
    """

    top_k: int = 5
    cosine_threshold: float = 0.3
    llm_provider: str = "anthropic"
    llm_model: str | None = None
    confidence_config: ConfidenceConfig = field(default_factory=ConfidenceConfig)
    semantic_weight: float = 1.0
    value_weight: float = 0.0

    def __post_init__(self) -> None:
        if abs(self.semantic_weight + self.value_weight - 1.0) >= 1e-6:
            msg = f"semantic_weight ({self.semantic_weight}) + value_weight ({self.value_weight}) must sum to 1.0"
            raise ValueError(msg)


def match_dictionaries(
    source: EmbeddedDictionary,
    target: EmbeddedDictionary,
    client: BaseLLMClient | None = None,
    config: MatchingConfig | None = None,
) -> MappingResult:
    """Match fields between two embedded dictionaries.

    Orchestrates the full pipeline: cosine candidate retrieval, optional LLM
    reranking, composite scoring, and triage into review buckets.

    When ``client`` is None, runs in **cosine-only mode**: the top-1 cosine
    candidate is selected as the match with cosine similarity as the confidence
    score. No LLM calls are made, so no API key is needed. This is useful as
    a fast baseline or when no LLM is available.

    Args:
        source: Embedded source dictionary.
        target: Embedded target dictionary.
        client: LLM client for reranking. If None, runs cosine-only mode.
        config: Optional matching configuration (uses defaults if None).

    Returns:
        MappingResult with all mappings, unmapped source/target fields,
        and pipeline metadata.
    """
    if config is None:
        config = MatchingConfig()

    cosine_only = client is None

    t0_total = time.perf_counter()

    # Step 1: Retrieve cosine candidates (blended scoring when value vectors available)
    t0_retrieval = time.perf_counter()
    candidates = retrieve_candidates(
        source,
        target,
        top_k=config.top_k,
        cosine_threshold=config.cosine_threshold,
        semantic_weight=config.semantic_weight,
        value_weight=config.value_weight,
    )
    logger.info("retrieve_candidates: %d source fields with candidates in %.2fs",
                len(candidates), time.perf_counter() - t0_retrieval)

    mappings: list[FieldMapping] = []
    source_unmapped: list[UnmappedField] = []
    mapped_target_vars: set[str] = set()
    interrupted = False

    # Step 2: Process each source field
    sorted_src_vars = sorted(source.embeddings.keys())

    if not cosine_only:
        try:
            from tqdm.auto import tqdm

            src_iter = tqdm(sorted_src_vars, desc="LLM reranking", unit="field")
        except ImportError:
            src_iter = sorted_src_vars
    else:
        logger.info("Running in cosine-only mode (no LLM reranking)")
        src_iter = sorted_src_vars

    try:
        for src_var in src_iter:
            src_candidates = candidates.get(src_var, [])

            # No candidates above threshold
            if not src_candidates:
                source_unmapped.append(UnmappedField(variable_name=src_var, reason=UnmappedReason.NO_CANDIDATES))
                continue

            if cosine_only:
                # Cosine-only mode: take top-1 candidate directly
                tgt_var, cosine_score = src_candidates[0]
                review_status = triage_mapping(cosine_score, config.confidence_config)
                fm = FieldMapping(
                    source_variable=src_var,
                    target_variable=tgt_var,
                    relation=Relation.EXACT,  # Assumed; no LLM to classify
                    confidence=cosine_score,
                    cosine_similarity=cosine_score,
                    llm_confidence=0.0,
                    rationale="Cosine-only mode: top-1 candidate by embedding similarity",
                    review_status=review_status,
                )
                mappings.append(fm)
                mapped_target_vars.add(tgt_var)
                continue

            # Look up Field objects
            src_field = source.dictionary.fields[src_var]
            candidate_fields_with_scores: list[tuple] = []
            for tgt_var, cosine_score in src_candidates:
                tgt_field = target.dictionary.fields[tgt_var]
                candidate_fields_with_scores.append((tgt_field, cosine_score))

            # Step 3: LLM reranking
            judgments = rerank_candidates(
                client, src_field, source.dictionary, candidate_fields_with_scores, target.dictionary
            )

            # Step 4: Score and find best non-no_match judgment
            best_mapping: FieldMapping | None = None
            best_score: float = -1.0
            rejected: list[FieldMapping] = []

            for judgment, cosine_score in judgments:
                composite = score_mapping(judgment.confidence, cosine_score, config.confidence_config)
                review_status = triage_mapping(composite, config.confidence_config)

                try:
                    relation = Relation(judgment.relation)
                except ValueError:
                    relation = Relation.NO_MATCH

                fm = FieldMapping(
                    source_variable=src_var,
                    target_variable=judgment.candidate_variable,
                    relation=relation,
                    confidence=composite,
                    cosine_similarity=cosine_score,
                    llm_confidence=judgment.confidence,
                    rationale=judgment.rationale,
                    review_status=review_status,
                )

                if relation == Relation.NO_MATCH:
                    rejected.append(fm)
                elif composite > best_score:
                    # Demote previous best to rejected if exists
                    if best_mapping is not None:
                        rejected.append(best_mapping)
                    best_mapping = fm
                    best_score = composite
                else:
                    rejected.append(fm)

            if best_mapping is not None:
                mappings.append(best_mapping)
                mapped_target_vars.add(best_mapping.target_variable)
            else:
                # All judgments were no_match
                source_unmapped.append(
                    UnmappedField(
                        variable_name=src_var,
                        reason=UnmappedReason.LLM_REJECTED_ALL,
                        rejected_candidates=rejected,
                    )
                )
    except KeyboardInterrupt:
        interrupted = True
        logger.warning(
            "Interrupted after %d/%d fields. Returning partial results.",
            len(mappings) + len(source_unmapped),
            len(sorted_src_vars),
        )

    # Step 5: Identify unmapped target fields
    all_target_vars = set(target.dictionary.fields.keys())
    unmapped_target_vars = all_target_vars - mapped_target_vars
    target_unmapped = [
        UnmappedField(variable_name=tgt_var, reason=UnmappedReason.NO_CANDIDATES)
        for tgt_var in sorted(unmapped_target_vars)
    ]

    result = MappingResult(
        source_name=source.dictionary.name,
        target_name=target.dictionary.name,
        mappings=mappings,
        source_unmapped=source_unmapped,
        target_unmapped=target_unmapped,
        top_k=config.top_k,
        cosine_threshold=config.cosine_threshold,
        auto_approve_threshold=config.confidence_config.auto_approve_threshold,
        auto_reject_threshold=config.confidence_config.auto_reject_threshold,
        model_name=client.model_name if client else "cosine_only",
        llm_provider=client.provider_name if client else "none",
    )

    # Step 6: Log summary
    elapsed_total = time.perf_counter() - t0_total
    status = "PARTIAL" if interrupted else "complete"
    logger.info(
        "match_dictionaries(%s -> %s) [%s]: %d/%d matched in %.1fs. "
        "Auto-approved: %d, Pending: %d, Rejected: %d, "
        "Unmapped source: %d, Unmapped target: %d",
        source.dictionary.name or "source",
        target.dictionary.name or "target",
        status,
        len(mappings),
        len(source.embeddings),
        elapsed_total,
        len(result.auto_approved),
        len(result.pending_review),
        len(result.auto_rejected),
        len(source_unmapped),
        len(target_unmapped),
    )

    return result
