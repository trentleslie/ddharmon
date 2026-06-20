"""Tests for CDE anchor selection (ddharmon.harmonization.anchor)."""

from __future__ import annotations

import numpy as np

from ddharmon.harmonization.anchor import (
    build_field_lookup,
    canonicalness_score,
    field_encoding_type,
    field_richness,
    find_anchor_cde,
)
from ddharmon.models.cluster import FieldReference


def test_field_encoding_type(hf):
    assert field_encoding_type(hf.field("a", "x", encoding="1=Y|2=N")) == "categorical"
    assert field_encoding_type(hf.field("b", "x", data_type="continuous")) == "continuous"
    assert field_encoding_type(None) is None


def test_canonicalness_rewards_collections_and_codes(hf):
    plain = hf.field("c1", "x")
    rich = hf.field("c2", "x", category="NIH > A; NIH > B; NIH > C", standard_codes={"NCI": ["1", "2"]})
    assert canonicalness_score(rich) > canonicalness_score(plain)
    assert canonicalness_score(None) == 0.0


def test_field_richness_monotonic(hf):
    bare = hf.field("c1", "x")
    full = hf.field(
        "c2", "A full definition", question_text="What?", data_type="categorical", units="kg", category="dom"
    )
    assert field_richness(full) > field_richness(bare)


def test_find_anchor_returns_false_with_fewer_than_two_members(hf):
    refs = [FieldReference("C", "v0", "d0")]
    emb = np.eye(1, 4, dtype=np.float32)
    result = find_anchor_cde(refs, emb, refs, {}, cde_cohort="NIH_CDE")
    assert result.has_cde is False


def test_find_anchor_no_cde_returns_medoid_only(hf):
    refs = [FieldReference("C", "v0", "d0"), FieldReference("C", "v1", "d1")]
    emb = np.array([[1.0, 0, 0, 0], [0.9, 0.1, 0, 0]], dtype=np.float32)
    fields = {("C", "v0"): hf.field("v0", "d0"), ("C", "v1"): hf.field("v1", "d1")}
    result = find_anchor_cde(refs, emb, refs, fields, cde_cohort="NIH_CDE")
    assert result.has_cde is False
    assert result.medoid_ref is not None  # medoid still computed for cohort-only sub-clusters


def test_find_anchor_picks_cde_in_subcluster(hf):
    # 2 cohort fields + 1 CDE, all near each other; CDE should be the anchor.
    refs = [
        FieldReference("CohortA", "age", "Age in years"),
        FieldReference("CohortB", "age_yrs", "Age in years"),
        FieldReference("NIH_CDE", "Age Value", "Age of the participant in years"),
    ]
    emb = np.array(
        [[1.0, 0, 0, 0], [0.98, 0.02, 0, 0], [0.99, 0.01, 0, 0]],
        dtype=np.float32,
    )
    fields = {
        ("CohortA", "age"): hf.field("age", "Age in years"),
        ("CohortB", "age_yrs"): hf.field("age_yrs", "Age in years"),
        ("NIH_CDE", "Age Value"): hf.field("Age Value", "Age of the participant in years", field_id="abc123"),
    }
    result = find_anchor_cde(refs, emb, refs, fields, cde_cohort="NIH_CDE")
    assert result.has_cde is True
    assert result.anchor_ref.dictionary_name == "NIH_CDE"
    assert result.anchor_field.field_id == "abc123"


def test_anchor_canonicalness_breaks_similarity_ties(hf):
    # Two CDEs with IDENTICAL embeddings (equal similarity); the more canonical wins.
    refs = [
        FieldReference("CohortA", "smoke", "Do you smoke"),
        FieldReference("NIH_CDE", "PlainCDE", "Smoking status"),
        FieldReference("NIH_CDE", "CanonicalCDE", "Smoking status"),
    ]
    emb = np.array([[1.0, 0, 0, 0], [0.95, 0.05, 0, 0], [0.95, 0.05, 0, 0]], dtype=np.float32)
    fields = {
        ("CohortA", "smoke"): hf.field("smoke", "Do you smoke"),
        ("NIH_CDE", "PlainCDE"): hf.field("PlainCDE", "Smoking status", field_id="plain"),
        ("NIH_CDE", "CanonicalCDE"): hf.field(
            "CanonicalCDE",
            "Smoking status",
            field_id="canon",
            category="NIH > Tier1; NIH > Tier2; NIH > Tier3",
            standard_codes={"NCI": ["1", "2", "3"]},
        ),
    }
    result = find_anchor_cde(refs, emb, refs, fields, cde_cohort="NIH_CDE")
    assert result.has_cde is True
    assert result.anchor_field.field_id == "canon"
    assert len(result.alternate_cdes) == 1  # the plain CDE is an alternate


def test_build_field_lookup(hf):
    ed = hf.embedded_dict("CohortA", [hf.field("a", "Age"), hf.field("b", "BMI")], sem_vecs=np.eye(2, 4))
    lookup = build_field_lookup([ed])
    assert ("CohortA", "a") in lookup
    assert lookup[("CohortA", "b")].description == "BMI"
