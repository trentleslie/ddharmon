"""Tests for value-vector sub-clustering (ddharmon.clustering.subcluster)."""

from __future__ import annotations

import numpy as np

from ddharmon.clustering.subcluster import build_value_vector_lookup, value_subcluster
from ddharmon.models.cluster import FieldCluster, FieldReference


def _refs(cohort: str, n: int) -> list[FieldReference]:
    return [FieldReference(cohort, f"{cohort}_v{i}", f"{cohort} variable {i}") for i in range(n)]


def _cluster(members: list[FieldReference]) -> FieldCluster:
    return FieldCluster(cluster_id=1, label="topic", members=members)


def test_build_value_vector_lookup_keys_by_cohort_and_var(hf):
    f = [hf.field("a", "Age"), hf.field("b", "BMI")]
    ed = hf.embedded_dict(
        "CohortA",
        f,
        sem_vecs=np.eye(2, 4),
        val_vecs=[np.array([1.0, 0, 0, 0]), None],  # only 'a' has a value vector
    )
    lookup = build_value_vector_lookup([ed])
    assert ("CohortA", "a") in lookup
    assert ("CohortA", "b") not in lookup  # no value vector -> absent


def test_too_few_when_no_value_vectors():
    members = _refs("C", 5)
    result = value_subcluster(_cluster(members), value_vecs={})
    assert result.status == "too_few"
    assert result.sub_clusters == {}
    assert len(result.excluded) == 5  # all excluded — none had a value vector


def test_single_group_below_min_value_members():
    members = _refs("C", 5)
    value_vecs = {
        (m.dictionary_name, m.variable_name): np.random.default_rng(i).standard_normal(4) for i, m in enumerate(members)
    }
    result = value_subcluster(_cluster(members), value_vecs, min_value_members=8)
    assert result.status == "single_group"
    assert list(result.sub_clusters.keys()) == [0]
    assert len(result.sub_clusters[0]) == 5


def test_excluded_members_lack_value_vectors():
    members = _refs("C", 10)
    # Only the first 6 have value vectors.
    value_vecs = {
        (m.dictionary_name, m.variable_name): np.random.default_rng(i).standard_normal(4)
        for i, m in enumerate(members[:6])
    }
    result = value_subcluster(_cluster(members), value_vecs, min_value_members=8)
    # 6 with vectors < 8 -> single_group of 6; the other 4 excluded.
    assert result.status == "single_group"
    assert len(result.sub_clusters[0]) == 6
    assert len(result.excluded) == 4


def test_subclustered_splits_two_separated_blobs():
    members = _refs("C", 24)
    rng = np.random.default_rng(0)
    value_vecs = {}
    for i, m in enumerate(members):
        center = np.array([10.0, 0, 0, 0]) if i < 12 else np.array([0, 10.0, 0, 0])
        value_vecs[(m.dictionary_name, m.variable_name)] = center + 0.05 * rng.standard_normal(4)
    result = value_subcluster(_cluster(members), value_vecs)
    assert result.status == "subclustered"
    real = [k for k in result.sub_clusters if k != -1]
    assert len(real) >= 2  # two well-separated value blobs -> >=2 sub-clusters
