"""Tests for prompt construction and response parsing (classify-only A/R/N pass)."""

from __future__ import annotations

from ddharmon.harmonization.models import AnchorResult
from ddharmon.harmonization.parse import extract_json, parse_verdict_payload, payload_from_response
from ddharmon.harmonization.prompts import (
    HARMONIZE_CLASSIFY_SCHEMA,
    HARMONIZE_CLASSIFY_SYSTEM_PROMPT,
    KGONLY_SCHEMA,
    anchor_candidates,
    build_classify_user_prompt,
    representative_members,
    system_prompt_for_mode,
)
from ddharmon.models.cluster import FieldReference

# ── prompts ────────────────────────────────────────────────────


def test_system_prompt_for_mode():
    sys_h, schema_h = system_prompt_for_mode("harmonize")
    assert sys_h == HARMONIZE_CLASSIFY_SYSTEM_PROMPT
    assert schema_h == HARMONIZE_CLASSIFY_SCHEMA
    assert "adopt | refine | novel" in schema_h

    sys_k, schema_k = system_prompt_for_mode("kg_only")
    assert "unaligned" in schema_k
    assert schema_k == KGONLY_SCHEMA


def test_anchor_candidates_prefers_field_id(hf):
    anchor_ref = FieldReference("NIH_CDE", "Age Value", "Age in years")
    anchor_fld = hf.field("Age Value", "Age in years", field_id="tiny123")
    alt_ref = FieldReference("NIH_CDE", "Age Other", "Age, other")
    alt_fld = hf.field("Age Other", "Age, other")  # no field_id -> falls back to designation
    anchor = AnchorResult(
        has_cde=True,
        anchor_ref=anchor_ref,
        anchor_field=anchor_fld,
        medoid_sim=0.91,
        alternate_cdes=[(alt_ref, alt_fld, 0.80)],
    )
    cands = anchor_candidates(anchor)
    assert cands[0]["canonical_id"] == "tiny123"  # field_id preferred
    assert cands[0]["cosine"] == 0.91
    assert cands[1]["canonical_id"] == "Age Other"  # fallback to designation


def test_anchor_candidates_empty_when_no_cde():
    assert anchor_candidates(AnchorResult(has_cde=False)) == []


def test_representative_members_uses_question_or_description(hf):
    members = [FieldReference("CohortA", "q1", "fallback desc")]
    lookup = {("CohortA", "q1"): hf.field("q1", "fallback desc", question_text="In what year were you born?")}
    lines = representative_members(members, lookup)
    assert "In what year were you born?" in lines[0]
    assert "CohortA/q1" in lines[0]


def test_build_classify_user_prompt_harmonize_vs_kgonly():
    cands = [{"canonical_id": "c1", "designation": "Age Value", "text": "Age in years", "cosine": 0.9}]
    prompt_h = build_classify_user_prompt(
        sub_cluster_id="3:0", label="age", member_lines=["CohortA/age: Age"], candidates=cands, mode="harmonize"
    )
    assert "adopt, refine, or novel" in prompt_h
    assert "age" in prompt_h and "Age Value" in prompt_h

    prompt_k = build_classify_user_prompt(
        sub_cluster_id="3:0", label="age", member_lines=["CohortA/age: Age"], candidates=cands, mode="kg_only"
    )
    assert "adopt or unaligned" in prompt_k
    assert "concept-level alignment only" in prompt_k


# ── parsing ────────────────────────────────────────────────────


def test_extract_json_plain():
    assert extract_json('{"verdict": "adopt"}')["verdict"] == "adopt"


def test_extract_json_fenced_with_prose_and_trailing():
    text = 'Here is my answer:\n```json\n{"verdict": "refine", "confidence": 0.7}\n```\nHope that helps!'
    out = extract_json(text)
    assert out["verdict"] == "refine"
    assert out["confidence"] == 0.7


def test_extract_json_trailing_commentary_after_brace():
    text = '{"verdict": "novel", "evidence": "no match"} -- note: low certainty {x}'
    out = extract_json(text)
    assert out["verdict"] == "novel"


def test_payload_from_response_variants():
    assert payload_from_response({"verdict": "adopt"})["verdict"] == "adopt"
    assert payload_from_response({"content": '{"verdict": "refine"}'})["verdict"] == "refine"
    assert payload_from_response('{"verdict": "novel"}')["verdict"] == "novel"


def test_parse_verdict_payload_none_on_bad_or_missing():
    assert parse_verdict_payload("not json at all") is None
    assert parse_verdict_payload({"no_verdict_here": 1}) is None
    assert parse_verdict_payload({"verdict": "adopt", "confidence": 0.8})["verdict"] == "adopt"
