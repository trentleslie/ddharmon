"""Tests for BERTopic-based topic modeling.

Unit tests (no bertopic required):
- extract_topic_clusters: synthetic topic IDs -> FieldCluster conversion
- TopicModelResult: dataclass construction and properties
"""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pytest

from ddharmon.clustering.topic_engine import extract_topic_clusters
from ddharmon.models.cluster import FieldCluster, FieldReference, TopicModelResult


# ── helpers ─────────────────────────────────────────────────────


def _make_refs(n: int, cohorts: list[str]) -> list[FieldReference]:
    """Create synthetic FieldReferences cycling through cohorts."""
    return [
        FieldReference(
            dictionary_name=cohorts[i % len(cohorts)],
            variable_name=f"var_{i}",
            description=f"Description for variable {i}",
        )
        for i in range(n)
    ]


# ── extract_topic_clusters ──────────────────────────────────────


def test_extract_topic_clusters_basic():
    """Basic topic extraction with 3 topics and no outliers."""
    refs = _make_refs(9, ["A", "B", "C"])
    topics = [0, 0, 0, 1, 1, 1, 2, 2, 2]

    clusters, outlier = extract_topic_clusters(topics, refs, ["A", "B", "C"])

    assert outlier is None
    assert len(clusters) == 3
    assert all(isinstance(c, FieldCluster) for c in clusters)
    assert all(len(c.members) == 3 for c in clusters)


def test_extract_topic_clusters_with_outliers():
    """Topic -1 should be separated into outlier_cluster."""
    refs = _make_refs(6, ["A", "B"])
    topics = [0, 0, 1, 1, -1, -1]

    clusters, outlier = extract_topic_clusters(topics, refs, ["A", "B"])

    assert len(clusters) == 2
    assert outlier is not None
    assert outlier.cluster_id == -1
    assert len(outlier.members) == 2


def test_extract_topic_clusters_cohort_coverage():
    """Verify cohort coverage and missing_cohorts tracking."""
    refs = [
        FieldReference("A", "v1", "desc1"),
        FieldReference("A", "v2", "desc2"),
        FieldReference("B", "v3", "desc3"),
    ]
    topics = [0, 0, 0]

    clusters, _ = extract_topic_clusters(topics, refs, ["A", "B", "C"])

    assert len(clusters) == 1
    c = clusters[0]
    assert c.cohort_coverage == {"A": 2, "B": 1}
    assert c.missing_cohorts == ["C"]


def test_extract_topic_clusters_all_outliers():
    """All fields assigned to outlier topic."""
    refs = _make_refs(5, ["A"])
    topics = [-1, -1, -1, -1, -1]

    clusters, outlier = extract_topic_clusters(topics, refs, ["A"])

    assert len(clusters) == 0
    assert outlier is not None
    assert len(outlier.members) == 5


def test_extract_topic_clusters_single_topic():
    """Single topic with no outliers."""
    refs = _make_refs(10, ["A", "B"])
    topics = [0] * 10

    clusters, outlier = extract_topic_clusters(topics, refs, ["A", "B"])

    assert len(clusters) == 1
    assert outlier is None
    assert len(clusters[0].members) == 10


# ── TopicModelResult ────────────────────────────────────────────


def test_topic_model_result_n_topics():
    """n_topics property returns cluster count."""
    refs = _make_refs(3, ["A"])
    clusters = [
        FieldCluster(cluster_id=0, label="a", members=refs[:2], cohort_coverage={"A": 2}, missing_cohorts=[]),
        FieldCluster(cluster_id=1, label="b", members=refs[2:], cohort_coverage={"A": 1}, missing_cohorts=[]),
    ]

    result = TopicModelResult(
        model=MagicMock(),
        docs=["d1", "d2", "d3"],
        embeddings=np.zeros((3, 8)),
        field_refs=refs,
        clusters=clusters,
        outlier_cluster=None,
        all_cohort_names=["A"],
    )

    assert result.n_topics == 2
    assert result.outlier_cluster is None


def test_topic_model_result_topic_info_delegates():
    """topic_info property delegates to model.get_topic_info()."""
    mock_model = MagicMock()
    mock_model.get_topic_info.return_value = "fake_df"

    result = TopicModelResult(
        model=mock_model,
        docs=[],
        embeddings=np.zeros((0, 8)),
        field_refs=[],
        clusters=[],
        outlier_cluster=None,
    )

    assert result.topic_info == "fake_df"
    mock_model.get_topic_info.assert_called_once()


def test_topic_model_result_topic_info_graceful_failure():
    """topic_info returns None if model raises."""
    mock_model = MagicMock()
    mock_model.get_topic_info.side_effect = RuntimeError("no model")

    result = TopicModelResult(
        model=mock_model,
        docs=[],
        embeddings=np.zeros((0, 8)),
        field_refs=[],
        clusters=[],
        outlier_cluster=None,
    )

    assert result.topic_info is None
