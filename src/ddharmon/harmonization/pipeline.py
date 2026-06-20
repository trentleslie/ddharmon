"""v1 sub-cluster-anchored CDE harmonization pipeline.

End-to-end orchestration of the v1 release pipeline::

    ingest -> dual-vector embed -> semantic cluster (BERTopic)
           -> value sub-cluster -> CDE anchor -> classify (adopt/refine/novel) -> EITL

The single LLM call per sub-cluster is the *classify-only* adopt/refine/novel
pass. Coherence judging, concept labeling, and spec authoring are intentionally
out of v1 (publication-pending). Verdicts are routed to EITL for human review.

The clustering step is split out (``prepare_from_clusters``) so the
sub-cluster -> anchor -> gate -> prompt logic is testable without BERTopic, and
so callers can run the LLM inline *or* export prompts for the secure-server
Batch API workflow and assemble verdicts later.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path

import numpy as np
from numpy.typing import NDArray

from ddharmon.clustering.labeling import derive_cluster_label
from ddharmon.clustering.subcluster import build_value_vector_lookup, value_subcluster
from ddharmon.embedding.service import EmbeddedDictionary
from ddharmon.harmonization.anchor import CDE_COHORT, build_field_lookup, find_anchor_cde
from ddharmon.harmonization.models import HarmonizationVerdict
from ddharmon.harmonization.parse import parse_verdict_payload
from ddharmon.harmonization.prompts import (
    anchor_candidates,
    build_classify_user_prompt,
    representative_members,
    system_prompt_for_mode,
)
from ddharmon.models.cluster import FieldCluster, FieldReference
from ddharmon.models.data_dictionary import Field

logger = logging.getLogger(__name__)

DEFAULT_MODEL_TAG = "claude-sonnet-4-6"


@dataclass
class PromptRecord:
    """One classify-only prompt plus the context needed to assemble its verdict."""

    id: str  # f"subcluster:{topic}:{sub_label}"
    system_prompt: str
    user_prompt: str
    schema: str
    model_tag: str
    context: dict = field(default_factory=dict)

    def to_jsonl_record(self) -> dict:
        """Serialize to the ``{id, system_prompt, user_prompt, schema, model_tag}`` shape
        consumed by ``ddharmon.llm.batch.submit_batch`` / ``process_prompts*.sh``."""
        return {
            "id": self.id,
            "system_prompt": self.system_prompt,
            "user_prompt": self.user_prompt,
            "schema": self.schema,
            "model_tag": self.model_tag,
        }


@dataclass
class HarmonizationResult:
    """All sub-cluster verdicts plus the prompts that produced (or will produce) them."""

    verdicts: list[HarmonizationVerdict] = field(default_factory=list)
    prompt_records: list[PromptRecord] = field(default_factory=list)

    def buckets(self) -> dict[str, list[HarmonizationVerdict]]:
        """Group verdicts into output buckets keyed by verdict / mode."""
        out: dict[str, list[HarmonizationVerdict]] = defaultdict(list)
        for v in self.verdicts:
            if v.mode in ("single_cohort", "cde_only", "noise"):
                out[v.mode].append(v)
            else:
                out[v.verdict or "unclassified"].append(v)
        return dict(out)


def _has_encoding(fld: Field | None) -> bool:
    if fld is None:
        return False
    return bool(fld.response_options) or bool(fld.value_encoding_raw and fld.value_encoding_raw.strip())


def _gate_mode(
    non_cde_members: list[FieldReference],
    field_lookup: dict[tuple[str, str], Field],
    *,
    min_cohorts: int,
    min_encoded_fraction: float,
) -> tuple[str, float, int]:
    """Compose the two cluster-level gates -> (mode, encoded_fraction, n_cohorts).

    - cohort-multiplicity: single-cohort sub-clusters skip the LLM (no pooling payoff).
    - encoded-fraction: multi-cohort sub-clusters with response-option data
      ``harmonize`` (3-way verdict); those without get concept-only ``kg_only``.
    """
    n_cohorts = len({m.dictionary_name for m in non_cde_members})
    encoded = sum(1 for m in non_cde_members if _has_encoding(field_lookup.get((m.dictionary_name, m.variable_name))))
    frac = encoded / len(non_cde_members) if non_cde_members else 0.0
    if n_cohorts < min_cohorts:
        return "single_cohort", frac, n_cohorts
    return ("harmonize" if frac >= min_encoded_fraction else "kg_only"), frac, n_cohorts


def prepare_from_clusters(
    clusters: list[FieldCluster],
    embedded_dicts: list[EmbeddedDictionary],
    embeddings: NDArray[np.float32],
    field_refs: list[FieldReference],
    *,
    cde_cohort: str = CDE_COHORT,
    min_value_members: int = 8,
    min_cohorts_for_harmonize: int = 2,
    min_encoded_fraction: float = 0.5,
    max_candidates: int = 5,
    model_tag: str = DEFAULT_MODEL_TAG,
) -> tuple[list[PromptRecord], list[HarmonizationVerdict]]:
    """Sub-cluster, anchor, gate, and build prompts from already-computed clusters.

    Returns ``(prompt_records, deterministic_verdicts)``. Sub-clusters that skip
    the LLM (single-cohort, CDE-only, noise, or GenCDE-needed) are returned as
    deterministic verdicts; the rest become prompt records for the classify call.

    Args:
        clusters: Semantic (topic) clusters with derived labels and members.
        embedded_dicts: Embedded dictionaries (for value vectors + Field metadata).
        embeddings: Full ``(N, D)`` semantic matrix (row order matches ``field_refs``).
        field_refs: Ordered FieldReference list aligned to ``embeddings``.
    """
    field_lookup = build_field_lookup(embedded_dicts)
    value_vecs = build_value_vector_lookup(embedded_dicts)

    prompt_records: list[PromptRecord] = []
    deterministic: list[HarmonizationVerdict] = []

    for cluster in clusters:
        sc = value_subcluster(cluster, value_vecs, min_value_members=min_value_members)

        for sub_label, sub_members in sc.sub_clusters.items():
            sub_id = f"{cluster.cluster_id}:{sub_label}"
            non_cde = [m for m in sub_members if m.dictionary_name != cde_cohort]

            # Noise sub-cluster — flag for review, no harmonization.
            if sub_label == -1:
                deterministic.append(
                    HarmonizationVerdict(
                        sub_cluster_id=sub_id,
                        parent_topic_id=cluster.cluster_id,
                        sub_label=sub_label,
                        mode="noise",
                        verdict="",
                        n_fields=len(non_cde),
                        cohorts=sorted({m.dictionary_name for m in non_cde}),
                        decided_by="deterministic",
                    )
                )
                continue

            # CDE-only sub-cluster — no cohort data to harmonize.
            if not non_cde:
                deterministic.append(
                    HarmonizationVerdict(
                        sub_cluster_id=sub_id,
                        parent_topic_id=cluster.cluster_id,
                        sub_label=sub_label,
                        mode="cde_only",
                        verdict="",
                        n_fields=0,
                        decided_by="deterministic",
                    )
                )
                continue

            anchor = find_anchor_cde(sub_members, embeddings, field_refs, field_lookup, cde_cohort=cde_cohort)
            mode, frac, n_cohorts = _gate_mode(
                non_cde,
                field_lookup,
                min_cohorts=min_cohorts_for_harmonize,
                min_encoded_fraction=min_encoded_fraction,
            )
            label = derive_cluster_label([m.description for m in non_cde])
            cohorts = sorted({m.dictionary_name for m in non_cde})

            base = {
                "sub_cluster_id": sub_id,
                "parent_topic_id": cluster.cluster_id,
                "sub_label": sub_label,
                "label": label,
                "cohorts": cohorts,
                "n_fields": len(non_cde),
                "encoded_fraction": round(frac, 3),
            }

            # Single-cohort — skip LLM, no cross-cohort pooling payoff.
            if mode == "single_cohort":
                deterministic.append(
                    HarmonizationVerdict(mode="single_cohort", verdict="", decided_by="deterministic", **base)
                )
                continue

            # No CDE in the sub-cluster — verdict is forced (can't adopt/refine
            # with no candidate). harmonize -> novel (GenCDE needed); kg_only -> unaligned.
            if not anchor.has_cde:
                forced = "novel" if mode == "harmonize" else "unaligned"
                deterministic.append(
                    HarmonizationVerdict(
                        mode=mode,
                        verdict=forced,
                        evidence="No candidate CDE in sub-cluster.",
                        decided_by="deterministic",
                        **base,
                    )
                )
                continue

            # Build the classify prompt. (has_cde is True here, so anchor_ref is set.)
            candidates = anchor_candidates(anchor, max_candidates=max_candidates)
            member_lines = representative_members(non_cde, field_lookup)
            system_prompt, schema = system_prompt_for_mode(mode)
            user_prompt = build_classify_user_prompt(
                sub_cluster_id=sub_id,
                label=label,
                member_lines=member_lines,
                candidates=candidates,
                mode=mode,
            )
            anchor_designation = anchor.anchor_ref.variable_name if anchor.anchor_ref else None
            prompt_records.append(
                PromptRecord(
                    id=f"subcluster:{sub_id}",
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    schema=schema,
                    model_tag=model_tag,
                    context={**base, "mode": mode, "anchor_designation": anchor_designation},
                )
            )

    logger.info(
        "prepare_from_clusters: %d clusters -> %d LLM prompts, %d deterministic verdicts",
        len(clusters),
        len(prompt_records),
        len(deterministic),
    )
    return prompt_records, deterministic


def assemble_verdicts(
    prompt_records: list[PromptRecord],
    responses: dict[str, object],
    deterministic: list[HarmonizationVerdict],
) -> HarmonizationResult:
    """Combine deterministic verdicts with parsed LLM responses keyed by prompt id."""
    verdicts: list[HarmonizationVerdict] = list(deterministic)

    for rec in prompt_records:
        ctx = rec.context
        resp = responses.get(rec.id)
        payload = parse_verdict_payload(resp) if resp is not None else None
        if payload is None:
            # Parse failure / missing response — surface for human review rather than drop.
            verdicts.append(
                HarmonizationVerdict(
                    sub_cluster_id=ctx["sub_cluster_id"],
                    parent_topic_id=ctx["parent_topic_id"],
                    sub_label=ctx["sub_label"],
                    mode=ctx["mode"],
                    verdict="",
                    label=ctx.get("label", ""),
                    cohorts=ctx.get("cohorts", []),
                    n_fields=ctx.get("n_fields", 0),
                    encoded_fraction=ctx.get("encoded_fraction", 0.0),
                    anchor_designation=ctx.get("anchor_designation"),
                    decided_by="llm",
                    evidence="LLM response missing or unparseable.",
                )
            )
            continue
        verdicts.append(
            HarmonizationVerdict(
                sub_cluster_id=ctx["sub_cluster_id"],
                parent_topic_id=ctx["parent_topic_id"],
                sub_label=ctx["sub_label"],
                mode=ctx["mode"],
                verdict=str(payload.get("verdict", "")),
                parent_cde_id=payload.get("parent_cde_id"),
                confidence=payload.get("confidence"),
                evidence=str(payload.get("evidence", "")),
                label=ctx.get("label", ""),
                cohorts=ctx.get("cohorts", []),
                n_fields=ctx.get("n_fields", 0),
                encoded_fraction=ctx.get("encoded_fraction", 0.0),
                anchor_designation=ctx.get("anchor_designation"),
                decided_by="llm",
                raw=payload,
            )
        )

    return HarmonizationResult(verdicts=verdicts, prompt_records=prompt_records)


def harmonize_dictionaries(
    embedded_dicts: list[EmbeddedDictionary],
    *,
    classify: Callable[[list[PromptRecord]], dict[str, object]] | None = None,
    cde_cohort: str = CDE_COHORT,
    min_cluster_size: int = 15,
    min_value_members: int = 8,
    min_cohorts_for_harmonize: int = 2,
    min_encoded_fraction: float = 0.5,
    max_candidates: int = 5,
    model_tag: str = DEFAULT_MODEL_TAG,
) -> HarmonizationResult:
    """Run the full v1 pipeline: cluster -> sub-cluster -> anchor -> classify.

    Args:
        embedded_dicts: Embedded cohort dictionaries plus the CDE dictionary
            (loaded under ``cde_cohort``), all sharing one embedding space.
        classify: Optional callable mapping prompt records to ``{id: response}``.
            If None, no LLM call is made — the result carries the deterministic
            verdicts and the ``prompt_records`` to export for the Batch API; call
            :func:`assemble_verdicts` once responses are retrieved.
        min_cluster_size: BERTopic/HDBSCAN minimum semantic cluster size.

    Returns:
        A :class:`HarmonizationResult`.
    """
    from ddharmon.clustering.topic_engine import topic_model_dictionaries

    result = topic_model_dictionaries(embedded_dicts, min_cluster_size=min_cluster_size)

    prompt_records, deterministic = prepare_from_clusters(
        result.clusters,
        embedded_dicts,
        result.embeddings,
        result.field_refs,
        cde_cohort=cde_cohort,
        min_value_members=min_value_members,
        min_cohorts_for_harmonize=min_cohorts_for_harmonize,
        min_encoded_fraction=min_encoded_fraction,
        max_candidates=max_candidates,
        model_tag=model_tag,
    )

    if classify is None:
        return HarmonizationResult(verdicts=deterministic, prompt_records=prompt_records)

    responses = classify(prompt_records)
    return assemble_verdicts(prompt_records, responses, deterministic)


# ── export helpers ─────────────────────────────────────────────


def write_prompts_jsonl(prompt_records: list[PromptRecord], path: str | Path) -> int:
    """Write prompt records as JSONL for the Batch API / process_prompts*.sh. Returns count."""
    path = Path(path)
    with open(path, "w") as f:
        for rec in prompt_records:
            f.write(json.dumps(rec.to_jsonl_record()) + "\n")
    return len(prompt_records)


def write_buckets(result: HarmonizationResult, out_dir: str | Path) -> dict[str, int]:
    """Write one JSON file per bucket (adopt/refine/novel/unaligned/...). Returns counts."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for name, verdicts in result.buckets().items():
        with open(out_dir / f"cluster_{name}.json", "w") as f:
            json.dump([asdict(v) for v in verdicts], f, indent=2)
        counts[name] = len(verdicts)
    return counts


def export_eitl_queue(result: HarmonizationResult, path: str | Path) -> int:
    """Write a TSV review queue for expert-in-the-loop verification. Returns row count.

    One row per harmonizable sub-cluster (excludes cde_only / noise), ordered so
    the verdicts most needing review (low confidence, refine/novel) surface first.
    """
    path = Path(path)
    cols = [
        "sub_cluster_id",
        "label",
        "verdict",
        "parent_cde_id",
        "anchor_designation",
        "confidence",
        "mode",
        "n_fields",
        "cohorts",
        "decided_by",
        "evidence",
    ]
    rows = [v for v in result.verdicts if v.mode not in ("cde_only", "noise")]

    def sort_key(v: HarmonizationVerdict) -> tuple:
        verdict_rank = {"refine": 0, "novel": 1, "unaligned": 1, "adopt": 3, "": 2}
        return (verdict_rank.get(v.verdict, 2), v.confidence if v.confidence is not None else 0.0)

    rows.sort(key=sort_key)

    with open(path, "w") as f:
        f.write("\t".join(cols) + "\n")
        for v in rows:
            values = [
                v.sub_cluster_id,
                v.label,
                v.verdict,
                v.parent_cde_id or "",
                v.anchor_designation or "",
                "" if v.confidence is None else f"{v.confidence:.3f}",
                v.mode,
                str(v.n_fields),
                ";".join(v.cohorts),
                v.decided_by,
                v.evidence.replace("\t", " ").replace("\n", " "),
            ]
            f.write("\t".join(values) + "\n")
    return len(rows)
