"""Unit tests for the clustering pipeline.

Uses synthetic embeddings (random L2-normalized vectors) to avoid
needing the sentence-transformers model.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pytest
from numpy.typing import NDArray

from ddharmon.models.cluster import ClusterHierarchy, CutSuggestion, FieldCluster, FieldReference
from ddharmon.models.data_dictionary import DataDictionary, Field


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _random_l2_vectors(n: int, dim: int = 768, seed: int = 42) -> NDArray[np.float32]:
    """Generate n random L2-normalized vectors."""
    rng = np.random.default_rng(seed)
    vecs = rng.standard_normal((n, dim)).astype(np.float32)
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    return vecs / norms


def _make_embedded_dict(
    name: str,
    variables: list[str],
    descriptions: list[str],
    vectors: NDArray[np.float32],
    cohort_name: str | None = None,
):
    """Create a mock EmbeddedDictionary with given fields and vectors."""
    from ddharmon.embedding.service import EmbeddedDictionary

    fields = {
        var: Field(variable_name=var, description=desc)
        for var, desc in zip(variables, descriptions)
    }
    dd = DataDictionary(name=name, fields=fields, cohort_name=cohort_name or name)
    embeddings = {var: vectors[i] for i, var in enumerate(variables)}
    return EmbeddedDictionary(dictionary=dd, embeddings=embeddings, model_name="test-model")


@pytest.fixture
def two_dicts():
    """Two EmbeddedDictionaries with 5 fields each."""
    vecs_a = _random_l2_vectors(5, seed=100)
    vecs_b = _random_l2_vectors(5, seed=200)
    dict_a = _make_embedded_dict(
        name="CohortA",
        variables=["age", "bmi", "height", "weight", "sex"],
        descriptions=[
            "Age at enrollment in years",
            "Body mass index",
            "Body height in centimeters",
            "Body weight in kilograms",
            "Biological sex at birth",
        ],
        vectors=vecs_a,
        cohort_name="CohortA",
    )
    dict_b = _make_embedded_dict(
        name="CohortB",
        variables=["enrol_age", "body_mass", "ht_cm", "wt_kg", "gender"],
        descriptions=[
            "Age when participant enrolled",
            "Body mass index computed from height and weight",
            "Height measured in centimeters",
            "Weight measured in kilograms",
            "Self-reported gender",
        ],
        vectors=vecs_b,
        cohort_name="CohortB",
    )
    return dict_a, dict_b


# ---------------------------------------------------------------------------
# Tests: hierarchical.py
# ---------------------------------------------------------------------------


def test_compute_linkage():
    """compute_linkage returns (N-1, 4) shaped ndarray for N vectors."""
    from ddharmon.clustering.hierarchical import compute_linkage

    vecs = _random_l2_vectors(10, seed=1)
    Z = compute_linkage(vecs)
    assert Z.shape == (9, 4)
    assert Z.dtype == np.float64


def test_compute_linkage_ward():
    """Ward linkage uses euclidean distance internally, returns valid linkage."""
    from ddharmon.clustering.hierarchical import compute_linkage

    vecs = _random_l2_vectors(10, seed=2)
    Z = compute_linkage(vecs, method="ward")
    assert Z.shape == (9, 4)
    # Ward merges should have increasing distances
    assert all(Z[i, 2] <= Z[i + 1, 2] for i in range(len(Z) - 1))


def test_suggest_cuts():
    """suggest_cuts returns CutSuggestion list with valid fields."""
    from ddharmon.clustering.hierarchical import suggest_cuts, compute_linkage

    vecs = _random_l2_vectors(25, seed=3)
    Z = compute_linkage(vecs)
    suggestions = suggest_cuts(Z, vecs)
    assert len(suggestions) > 0
    for s in suggestions:
        assert isinstance(s, CutSuggestion)
        assert s.distance > 0
        assert -1 <= s.silhouette_score <= 1
        assert s.n_clusters >= 2


def test_suggest_cuts_filters_extremes():
    """Thresholds producing <2 or >N/2 clusters are excluded."""
    from ddharmon.clustering.hierarchical import suggest_cuts, compute_linkage

    vecs = _random_l2_vectors(20, seed=4)
    Z = compute_linkage(vecs)
    suggestions = suggest_cuts(Z, vecs)
    for s in suggestions:
        assert s.n_clusters >= 2
        assert s.n_clusters <= 10  # N/2 = 20/2 = 10


def test_extract_clusters():
    """extract_clusters returns FieldCluster list with valid members and coverage."""
    from ddharmon.clustering.hierarchical import extract_clusters, compute_linkage

    vecs = _random_l2_vectors(10, seed=5)
    refs = [
        FieldReference(dictionary_name=f"Dict{i % 2}", variable_name=f"var_{i}", description=f"Field {i}")
        for i in range(10)
    ]
    Z = compute_linkage(vecs)
    # Use a middle distance
    mid_dist = float(Z[:, 2].mean())
    clusters = extract_clusters(Z, refs, mid_dist, all_cohort_names=["Dict0", "Dict1"])
    assert len(clusters) >= 1
    total_members = sum(len(c.members) for c in clusters)
    assert total_members == 10
    for c in clusters:
        assert isinstance(c, FieldCluster)
        assert len(c.members) > 0
        # cohort_coverage keys should be subset of all_cohort_names
        for cohort in c.cohort_coverage:
            assert cohort in ["Dict0", "Dict1"]


# ---------------------------------------------------------------------------
# Tests: labeling.py
# ---------------------------------------------------------------------------


def test_derive_cluster_label():
    """Derived label from body-related descriptions contains 'body'."""
    from ddharmon.clustering.labeling import derive_cluster_label

    descs = ["Body mass index", "Body weight in kg", "Body height"]
    label = derive_cluster_label(descs)
    assert "body" in label.lower()


def test_derive_cluster_label_empty():
    """Empty descriptions list returns 'Unlabeled cluster'."""
    from ddharmon.clustering.labeling import derive_cluster_label

    label = derive_cluster_label([])
    assert label == "Unlabeled cluster"


# ---------------------------------------------------------------------------
# Tests: cluster_engine.py
# ---------------------------------------------------------------------------


def test_cluster_dictionaries(two_dicts):
    """cluster_dictionaries returns ClusterHierarchy with correct field count."""
    from ddharmon.clustering import cluster_dictionaries

    dict_a, dict_b = two_dicts
    result = cluster_dictionaries([dict_a, dict_b])
    assert isinstance(result, ClusterHierarchy)
    assert len(result.field_refs) == 10
    assert result.linkage_matrix.shape == (9, 4)
    assert len(result.cut_suggestions) > 0
    assert len(result.clusters_at_cuts) > 0


def test_cluster_dictionaries_cohort_coverage(two_dicts):
    """Clusters track which cohorts have members and which are missing."""
    from ddharmon.clustering import cluster_dictionaries

    dict_a, dict_b = two_dicts
    result = cluster_dictionaries([dict_a, dict_b])
    assert set(result.all_cohort_names) == {"CohortA", "CohortB"}
    # At least one cut should have clusters
    for distance, clusters in result.clusters_at_cuts.items():
        for c in clusters:
            # Every member's dictionary_name should be in all_cohort_names
            for m in c.members:
                assert m.dictionary_name in result.all_cohort_names
            # cohort_coverage + missing_cohorts should cover all cohort names
            covered = set(c.cohort_coverage.keys())
            missing = set(c.missing_cohorts)
            assert covered | missing == set(result.all_cohort_names)


def test_cluster_dictionaries_no_llm(two_dicts):
    """Without llm_client, labels are derived (not generic 'Cluster N')."""
    from ddharmon.clustering import cluster_dictionaries

    dict_a, dict_b = two_dicts
    result = cluster_dictionaries([dict_a, dict_b])
    for distance, clusters in result.clusters_at_cuts.items():
        for c in clusters:
            # Labels should not be empty or generic "Cluster N"
            assert c.label
            assert not c.label.startswith("Cluster ")


def test_cluster_dictionaries_with_llm_client(two_dicts):
    """When llm_client with complete() is provided, labels come from LLM."""
    from ddharmon.clustering import cluster_dictionaries
    from ddharmon.llm.base import BaseLLMClient
    from ddharmon.llm.prompts import RerankerResponse

    class MockLLMClient(BaseLLMClient):
        @property
        def provider_name(self) -> str:
            return "mock"

        @property
        def model_name(self) -> str:
            return "mock-model"

        def rerank_candidates(self, source_context, candidate_contexts, candidate_names):
            return RerankerResponse(judgments=[])

        def complete(self, prompt: str, *, system: str | None = None, max_tokens: int = 512) -> str:
            return "LLM Generated Label"

    dict_a, dict_b = two_dicts
    result = cluster_dictionaries([dict_a, dict_b], llm_client=MockLLMClient())
    for dist, clusters in result.clusters_at_cuts.items():
        for c in clusters:
            assert c.label == "LLM Generated Label"


def test_cluster_dictionaries_llm_no_complete(two_dicts):
    """When llm_client lacks complete(), derived labels are used (no crash)."""
    from ddharmon.clustering import cluster_dictionaries
    from ddharmon.llm.base import BaseLLMClient
    from ddharmon.llm.prompts import RerankerResponse

    class NoCompleteLLMClient(BaseLLMClient):
        @property
        def provider_name(self) -> str:
            return "mock"

        @property
        def model_name(self) -> str:
            return "mock-model"

        def rerank_candidates(self, source_context, candidate_contexts, candidate_names):
            return RerankerResponse(judgments=[])

        # Does NOT override complete() -- uses default NotImplementedError

    dict_a, dict_b = two_dicts
    result = cluster_dictionaries([dict_a, dict_b], llm_client=NoCompleteLLMClient())
    # Should not crash, should have derived labels
    for dist, clusters in result.clusters_at_cuts.items():
        for c in clusters:
            assert c.label
            assert c.label != "LLM Generated Label"
