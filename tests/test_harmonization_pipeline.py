"""Tests for the v1 harmonization pipeline orchestration.

Exercises prepare_from_clusters -> assemble_verdicts and the export helpers on
hand-built clusters (no BERTopic), plus a mock-LLM end-to-end via classify().
"""

from __future__ import annotations

import json

import numpy as np
import pytest

from ddharmon.clustering.topic_engine import collect_inputs
from ddharmon.harmonization.pipeline import (
    HarmonizationResult,
    PromptRecord,
    assemble_verdicts,
    export_eitl_queue,
    prepare_from_clusters,
    write_buckets,
    write_prompts_jsonl,
)
from ddharmon.models.cluster import FieldCluster, FieldReference


@pytest.fixture
def world(hf):
    """Three embedded dicts (CohortA, CohortB, NIH_CDE) with value vectors.

    Returns (embedded_dicts, embeddings, field_refs, refs_by_key).
    """
    a_fields = [
        hf.field("age", "Age in years", encoding="1=0-18|2=19-65|3=65+"),
        hf.field("smoke", "Do you smoke", encoding="1=Yes|2=No"),
    ]
    b_fields = [
        hf.field("age_yrs", "Age in years", encoding="0-120"),
        hf.field("smoke_b", "Current smoker", encoding="1=Yes|0=No"),
    ]
    cde_fields = [
        hf.field("AgeCDE", "Age of the participant in years", field_id="cde_age", encoding="years"),
    ]

    ed_a = hf.embedded_dict(
        "CohortA",
        a_fields,
        sem_vecs=hf.l2(np.array([[1, 0, 0, 0], [0, 1, 0, 0]], float)),
        val_vecs=[np.array([1.0, 0, 0, 0]), np.array([0, 1.0, 0, 0])],
    )
    ed_b = hf.embedded_dict(
        "CohortB",
        b_fields,
        sem_vecs=hf.l2(np.array([[0.98, 0.02, 0, 0], [0.02, 0.98, 0, 0]], float)),
        val_vecs=[np.array([0.95, 0.05, 0, 0]), np.array([0.05, 0.95, 0, 0])],
    )
    ed_cde = hf.embedded_dict(
        "NIH_CDE",
        cde_fields,
        sem_vecs=hf.l2(np.array([[0.99, 0.01, 0, 0]], float)),
        val_vecs=[np.array([0.9, 0.1, 0, 0])],
    )

    embedded = [ed_a, ed_b, ed_cde]
    _docs, embeddings, field_refs, _cohorts = collect_inputs(embedded)
    refs_by_key = {(r.dictionary_name, r.variable_name): r for r in field_refs}
    return embedded, embeddings, field_refs, refs_by_key


def _cluster(cid: int, refs: list[FieldReference]) -> FieldCluster:
    return FieldCluster(cluster_id=cid, label="topic", members=refs)


def test_harmonize_cluster_with_cde_becomes_llm_prompt(world):
    embedded, embeddings, field_refs, by_key = world
    age = _cluster(1, [by_key[("CohortA", "age")], by_key[("CohortB", "age_yrs")], by_key[("NIH_CDE", "AgeCDE")]])

    prompts, deterministic = prepare_from_clusters([age], embedded, embeddings, field_refs)
    assert len(prompts) == 1
    assert deterministic == []
    rec = prompts[0]
    assert rec.context["mode"] == "harmonize"
    assert rec.context["anchor_designation"] == "AgeCDE"
    assert "adopt, refine, or novel" in rec.user_prompt


def test_single_cohort_cluster_is_deterministic_skip(world):
    embedded, embeddings, field_refs, by_key = world
    solo = _cluster(2, [by_key[("CohortA", "age")], by_key[("CohortA", "smoke")]])
    prompts, deterministic = prepare_from_clusters([solo], embedded, embeddings, field_refs)
    assert prompts == []
    assert len(deterministic) == 1
    assert deterministic[0].mode == "single_cohort"
    assert deterministic[0].decided_by == "deterministic"


def test_multi_cohort_no_cde_forced_novel(world):
    embedded, embeddings, field_refs, by_key = world
    smoke = _cluster(3, [by_key[("CohortA", "smoke")], by_key[("CohortB", "smoke_b")]])
    prompts, deterministic = prepare_from_clusters([smoke], embedded, embeddings, field_refs)
    assert prompts == []  # no candidate CDE -> no LLM call
    assert len(deterministic) == 1
    assert deterministic[0].verdict == "novel"
    assert deterministic[0].decided_by == "deterministic"


def test_assemble_verdicts_parses_llm_response(world):
    embedded, embeddings, field_refs, by_key = world
    age = _cluster(1, [by_key[("CohortA", "age")], by_key[("CohortB", "age_yrs")], by_key[("NIH_CDE", "AgeCDE")]])
    prompts, deterministic = prepare_from_clusters([age], embedded, embeddings, field_refs)

    responses = {
        prompts[0].id: {"verdict": "adopt", "parent_cde_id": "cde_age", "confidence": 0.92, "evidence": "Exact match."}
    }
    result = assemble_verdicts(prompts, responses, deterministic)
    assert len(result.verdicts) == 1
    v = result.verdicts[0]
    assert v.verdict == "adopt"
    assert v.parent_cde_id == "cde_age"
    assert v.confidence == 0.92
    assert v.decided_by == "llm"


def test_assemble_verdicts_handles_missing_response(world):
    embedded, embeddings, field_refs, by_key = world
    age = _cluster(1, [by_key[("CohortA", "age")], by_key[("CohortB", "age_yrs")], by_key[("NIH_CDE", "AgeCDE")]])
    prompts, deterministic = prepare_from_clusters([age], embedded, embeddings, field_refs)
    result = assemble_verdicts(prompts, responses={}, deterministic=deterministic)
    # Missing response -> surfaced (empty verdict) rather than dropped.
    assert len(result.verdicts) == 1
    assert result.verdicts[0].verdict == ""
    assert "missing" in result.verdicts[0].evidence.lower()


def test_buckets_group_by_verdict_and_mode(world):
    embedded, embeddings, field_refs, by_key = world
    age = _cluster(1, [by_key[("CohortA", "age")], by_key[("CohortB", "age_yrs")], by_key[("NIH_CDE", "AgeCDE")]])
    smoke = _cluster(3, [by_key[("CohortA", "smoke")], by_key[("CohortB", "smoke_b")]])
    solo = _cluster(2, [by_key[("CohortA", "age")], by_key[("CohortA", "smoke")]])
    prompts, deterministic = prepare_from_clusters([age, smoke, solo], embedded, embeddings, field_refs)
    responses = {prompts[0].id: {"verdict": "refine", "parent_cde_id": "cde_age", "confidence": 0.7, "evidence": "x"}}
    result = assemble_verdicts(prompts, responses, deterministic)
    buckets = result.buckets()
    assert "refine" in buckets
    assert "novel" in buckets
    assert "single_cohort" in buckets


def test_export_helpers_write_files(world, tmp_path):
    embedded, embeddings, field_refs, by_key = world
    age = _cluster(1, [by_key[("CohortA", "age")], by_key[("CohortB", "age_yrs")], by_key[("NIH_CDE", "AgeCDE")]])
    prompts, deterministic = prepare_from_clusters([age], embedded, embeddings, field_refs)
    responses = {prompts[0].id: {"verdict": "adopt", "parent_cde_id": "cde_age", "confidence": 0.9, "evidence": "x"}}
    result = assemble_verdicts(prompts, responses, deterministic)

    n_prompts = write_prompts_jsonl(prompts, tmp_path / "prompts.jsonl")
    assert n_prompts == 1
    line = json.loads((tmp_path / "prompts.jsonl").read_text().splitlines()[0])
    assert {"id", "system_prompt", "user_prompt", "schema", "model_tag"} <= set(line)

    counts = write_buckets(result, tmp_path / "buckets")
    assert counts.get("adopt") == 1
    assert (tmp_path / "buckets" / "cluster_adopt.json").exists()

    n_rows = export_eitl_queue(result, tmp_path / "eitl_queue.tsv")
    assert n_rows == 1
    header = (tmp_path / "eitl_queue.tsv").read_text().splitlines()[0]
    assert "verdict" in header and "parent_cde_id" in header


def test_end_to_end_with_mock_classify(world):
    """harmonize via prepare + a mock classify callable (no BERTopic)."""
    embedded, embeddings, field_refs, by_key = world
    age = _cluster(1, [by_key[("CohortA", "age")], by_key[("CohortB", "age_yrs")], by_key[("NIH_CDE", "AgeCDE")]])
    prompts, deterministic = prepare_from_clusters([age], embedded, embeddings, field_refs)

    def mock_classify(records: list[PromptRecord]) -> dict[str, object]:
        return {
            r.id: {"verdict": "refine", "parent_cde_id": "cde_age", "confidence": 0.8, "evidence": "m"} for r in records
        }

    result = assemble_verdicts(prompts, mock_classify(prompts), deterministic)
    assert isinstance(result, HarmonizationResult)
    assert result.verdicts[0].verdict == "refine"
