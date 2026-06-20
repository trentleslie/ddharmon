"""Unit tests for the visualization module.

Tests Plotly dendrogram, scatter plot (UMAP and t-SNE), per-cluster
value similarity heatmaps, and static PNG export.
"""

from __future__ import annotations

import numpy as np
import pytest
from numpy.typing import NDArray

go = pytest.importorskip("plotly.graph_objects")

from ddharmon.export.visualization import (
    compute_umap_coords,
    create_cluster_umap,
    create_cohort_umap,
    create_combined_umap,
    create_dendrogram,
    create_scatter_plot,
    create_typed_umap,
    create_value_heatmaps,
    export_png,
)
from ddharmon.models.cluster import ClusterHierarchy, CutSuggestion, FieldCluster, FieldReference
from ddharmon.models.data_dictionary import DataDictionary, Field

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _random_l2_vectors(n: int, dim: int = 50, seed: int = 42) -> NDArray[np.float32]:
    """Generate n random L2-normalized vectors."""
    rng = np.random.default_rng(seed)
    vecs = rng.standard_normal((n, dim)).astype(np.float32)
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    return vecs / norms


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_hierarchy():
    """Build a ClusterHierarchy with 10 fields from 2 mock cohorts."""
    from scipy.cluster.hierarchy import linkage

    n = 10
    vecs = _random_l2_vectors(n, dim=50, seed=10)

    # Compute real linkage for dendrogram validity
    linkage_mat = linkage(vecs, method="average", metric="cosine")

    refs = [
        FieldReference(
            dictionary_name=f"Cohort{'A' if i < 5 else 'B'}",
            variable_name=f"var_{i}",
            description=f"Description for variable {i}",
        )
        for i in range(n)
    ]

    cuts = [
        CutSuggestion(distance=0.5, silhouette_score=0.6, n_clusters=3),
        CutSuggestion(distance=0.8, silhouette_score=0.4, n_clusters=2),
    ]

    cluster_a = FieldCluster(
        cluster_id=0,
        label="Body measurements",
        members=refs[:4],
        cohort_coverage={"CohortA": 3, "CohortB": 1},
        missing_cohorts=[],
    )
    cluster_b = FieldCluster(
        cluster_id=1,
        label="Demographics",
        members=refs[4:7],
        cohort_coverage={"CohortA": 1, "CohortB": 2},
        missing_cohorts=[],
    )
    cluster_c = FieldCluster(
        cluster_id=2,
        label="Questionnaire items",
        members=refs[7:],
        cohort_coverage={"CohortA": 1, "CohortB": 2},
        missing_cohorts=[],
    )

    return ClusterHierarchy(
        linkage_matrix=linkage_mat,
        field_refs=refs,
        cut_suggestions=cuts,
        clusters_at_cuts={0.5: [cluster_a, cluster_b, cluster_c]},
        model_name="test-model",
        all_cohort_names=["CohortA", "CohortB"],
    )


@pytest.fixture
def sample_vectors_and_refs():
    """Return (vectors, field_refs) for scatter plot tests."""
    n = 10
    vecs = _random_l2_vectors(n, dim=50, seed=20)
    refs = [
        FieldReference(
            dictionary_name=f"Cohort{'A' if i < 5 else 'B'}",
            variable_name=f"field_{i}",
            description=f"Scatter field {i}",
        )
        for i in range(n)
    ]
    return vecs, refs


@pytest.fixture
def sample_clusters_with_value_data():
    """Return (clusters, embedded_dicts) with value embeddings populated."""
    from ddharmon.embedding.service import EmbeddedDictionary

    dim = 50
    rng = np.random.default_rng(30)

    # Build 2 embedded dicts with value embeddings
    def _make_ed(name: str, vars_: list[str], has_values: list[bool]) -> EmbeddedDictionary:
        fields = {v: Field(variable_name=v, description=f"Desc {v}") for v in vars_}
        dd = DataDictionary(name=name, fields=fields, cohort_name=name)
        embeddings = {v: _random_l2_vectors(1, dim=dim, seed=hash(v) % 10000)[0] for v in vars_}
        value_embs: dict[str, NDArray[np.float32]] = {}
        for v, has_val in zip(vars_, has_values, strict=True):
            if has_val:
                vec = rng.standard_normal(dim).astype(np.float32)
                vec /= np.linalg.norm(vec)
                value_embs[v] = vec
        return EmbeddedDictionary(dictionary=dd, embeddings=embeddings, model_name="test", value_embeddings=value_embs)

    ed_a = _make_ed("CohortA", ["age", "bmi", "height"], [True, True, True])
    ed_b = _make_ed("CohortB", ["enrol_age", "body_mass", "ht_cm"], [True, True, False])

    # Cluster with 3+ members that have value embeddings
    big_cluster = FieldCluster(
        cluster_id=0,
        label="Body measurements",
        members=[
            FieldReference("CohortA", "age", "Age"),
            FieldReference("CohortA", "bmi", "BMI"),
            FieldReference("CohortB", "enrol_age", "Enrollment age"),
            FieldReference("CohortB", "body_mass", "Body mass"),
        ],
        cohort_coverage={"CohortA": 2, "CohortB": 2},
        missing_cohorts=[],
    )

    # Cluster with only 1 member (should be skipped)
    single_cluster = FieldCluster(
        cluster_id=1,
        label="Singleton",
        members=[FieldReference("CohortA", "height", "Height")],
        cohort_coverage={"CohortA": 1},
        missing_cohorts=["CohortB"],
    )

    return [big_cluster, single_cluster], [ed_a, ed_b]


# ---------------------------------------------------------------------------
# Tests: Dendrogram
# ---------------------------------------------------------------------------


def test_create_dendrogram_returns_figure(sample_hierarchy):
    """create_dendrogram returns a plotly Figure with expected title."""
    fig = create_dendrogram(sample_hierarchy)
    assert isinstance(fig, go.Figure)
    assert "Dendrogram" in fig.layout.title.text


def test_dendrogram_cut_lines(sample_hierarchy):
    """Dendrogram has horizontal dashed lines for cut suggestions."""
    fig = create_dendrogram(sample_hierarchy)
    # Shapes include hlines added by add_hline
    shapes = fig.layout.shapes
    assert len(shapes) >= len(sample_hierarchy.cut_suggestions)


# ---------------------------------------------------------------------------
# Tests: Scatter plot
# ---------------------------------------------------------------------------


def test_create_scatter_umap(sample_vectors_and_refs):
    """UMAP scatter plot returns Figure with correct point count."""
    pytest.importorskip("umap")
    vecs, refs = sample_vectors_and_refs
    fig = create_scatter_plot(vecs, refs, method="umap")
    assert isinstance(fig, go.Figure)
    # Count total points across all traces
    total_points = sum(len(trace.x) for trace in fig.data)
    assert total_points == len(refs)


def test_create_scatter_tsne(sample_vectors_and_refs):
    """t-SNE scatter plot returns Figure with correct point count."""
    pytest.importorskip("sklearn")
    vecs, refs = sample_vectors_and_refs
    fig = create_scatter_plot(vecs, refs, method="tsne")
    assert isinstance(fig, go.Figure)
    total_points = sum(len(trace.x) for trace in fig.data)
    assert total_points == len(refs)


def test_scatter_plot_cohort_colors(sample_vectors_and_refs):
    """Scatter traces are colored by cohort (2 cohorts -> 2 traces)."""
    pytest.importorskip("umap")
    vecs, refs = sample_vectors_and_refs
    fig = create_scatter_plot(vecs, refs, method="umap")
    # px.scatter creates one trace per color group
    assert len(fig.data) == 2  # CohortA and CohortB


# ---------------------------------------------------------------------------
# Tests: Value heatmaps
# ---------------------------------------------------------------------------


def test_value_heatmaps_correct_clusters(sample_clusters_with_value_data):
    """Heatmaps returned for clusters with >=2 members having value embeddings."""
    clusters, eds = sample_clusters_with_value_data
    heatmaps = create_value_heatmaps(clusters, eds)
    assert 0 in heatmaps  # big_cluster has 4 members with values
    assert isinstance(heatmaps[0], go.Figure)


def test_value_heatmaps_skips_small(sample_clusters_with_value_data):
    """Single-member clusters produce no heatmap."""
    clusters, eds = sample_clusters_with_value_data
    heatmaps = create_value_heatmaps(clusters, eds)
    assert 1 not in heatmaps  # single_cluster skipped


# ---------------------------------------------------------------------------
# Tests: PNG export
# ---------------------------------------------------------------------------


def test_export_png_creates_file(tmp_path):
    """export_png creates a non-empty PNG file on disk."""
    fig = go.Figure(data=go.Scatter(x=[1, 2, 3], y=[1, 2, 3]))
    out = export_png(fig, tmp_path / "test.png")
    assert out.exists()
    assert out.stat().st_size > 0


# ---------------------------------------------------------------------------
# Tests: Paired UMAP views
# ---------------------------------------------------------------------------


@pytest.fixture
def umap_coords_and_clusters(sample_hierarchy):
    """Pre-compute UMAP coords and extract clusters from sample hierarchy."""
    pytest.importorskip("umap")
    vecs = _random_l2_vectors(10, dim=50, seed=10)
    coords = compute_umap_coords(vecs)
    clusters = sample_hierarchy.clusters_at_cuts[0.5]
    return coords, sample_hierarchy, clusters


def test_compute_umap_coords_shape():
    """compute_umap_coords returns (N, 2) array."""
    pytest.importorskip("umap")
    vecs = _random_l2_vectors(20, dim=50)
    coords = compute_umap_coords(vecs)
    assert coords.shape == (20, 2)


def test_cohort_umap_traces_match_cohorts(umap_coords_and_clusters):
    """Cohort UMAP has one trace per cohort."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    fig = create_cohort_umap(coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names)
    assert isinstance(fig, go.Figure)
    # One trace per cohort
    trace_names = [t.name for t in fig.data]
    assert "CohortA" in trace_names
    assert "CohortB" in trace_names


def test_cohort_umap_has_annotations(umap_coords_and_clusters):
    """Cohort UMAP has cluster label annotations."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    fig = create_cohort_umap(coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names)
    assert len(fig.layout.annotations) > 0


def test_cohort_umap_total_points(umap_coords_and_clusters):
    """All field refs appear as points in the cohort UMAP."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    fig = create_cohort_umap(coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names)
    total_points = sum(len(trace.x) for trace in fig.data)
    assert total_points == len(hierarchy.field_refs)


def test_cluster_umap_traces(umap_coords_and_clusters):
    """Cluster UMAP has one trace per cluster plus 'Other'."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    fig = create_cluster_umap(coords, hierarchy.field_refs, clusters, top_n=30)
    assert isinstance(fig, go.Figure)
    # 3 clusters + possibly "Other" trace
    assert len(fig.data) >= 3


def test_cluster_umap_has_numbered_annotations(umap_coords_and_clusters):
    """Cluster UMAP has numbered centroid annotations."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    fig = create_cluster_umap(coords, hierarchy.field_refs, clusters, top_n=30)
    annotation_texts = [a.text for a in fig.layout.annotations]
    assert "<b>0</b>" in annotation_texts
    assert "<b>1</b>" in annotation_texts
    assert "<b>2</b>" in annotation_texts


def test_paired_umap_axis_ranges_match(umap_coords_and_clusters):
    """Both UMAP views share identical axis ranges."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    fig_cohort = create_cohort_umap(coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names)
    fig_cluster = create_cluster_umap(coords, hierarchy.field_refs, clusters)
    assert fig_cohort.layout.xaxis.range == fig_cluster.layout.xaxis.range
    assert fig_cohort.layout.yaxis.range == fig_cluster.layout.yaxis.range


def test_cluster_umap_legend_format(umap_coords_and_clusters):
    """Cluster UMAP legend entries have 'rank: label (count)' format."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    fig = create_cluster_umap(coords, hierarchy.field_refs, clusters, top_n=30)
    # First trace should be the largest cluster
    assert fig.data[0].name.startswith("0: ")


def test_combined_umap_has_shapes(umap_coords_and_clusters):
    """Combined UMAP uses different marker symbols per cohort."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    fig = create_combined_umap(
        coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names,
    )
    assert isinstance(fig, go.Figure)
    # Should have sub-traces per cohort within clusters, so more traces than clusters
    assert len(fig.data) > len(clusters)
    # Check that different symbols are used
    symbols = {trace.marker.symbol for trace in fig.data if hasattr(trace.marker, "symbol")}
    assert len(symbols) >= 2  # at least 2 cohort shapes


def test_combined_umap_legend_groups(umap_coords_and_clusters):
    """Combined UMAP uses legendgroup so each cluster shows once in legend."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    fig = create_combined_umap(
        coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names,
    )
    shown = [t.name for t in fig.data if t.showlegend is not False]
    # Should have one legend entry per cluster (not per cluster*cohort)
    assert len(shown) <= len(clusters) + 1  # +1 for "Other"


def test_combined_umap_axis_ranges_match(umap_coords_and_clusters):
    """Combined UMAP axis ranges match the other two views."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    fig_cluster = create_cluster_umap(coords, hierarchy.field_refs, clusters)
    fig_combined = create_combined_umap(
        coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names,
    )
    assert fig_cluster.layout.xaxis.range == fig_combined.layout.xaxis.range
    assert fig_cluster.layout.yaxis.range == fig_combined.layout.yaxis.range


def test_combined_umap_shape_key_annotation(umap_coords_and_clusters):
    """Combined UMAP has a shape key annotation."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    fig = create_combined_umap(
        coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names,
    )
    annotation_texts = [a.text for a in fig.layout.annotations]
    shape_annotations = [t for t in annotation_texts if "Shapes" in t]
    assert len(shape_annotations) == 1


def test_combined_umap_data_type_borders(umap_coords_and_clusters):
    """Combined UMAP uses data_type-based border colors when data_type_map provided."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    dt_map = {}
    for ref in hierarchy.field_refs:
        dt_map[(ref.dictionary_name, ref.variable_name)] = "categorical"
    dt_map[(hierarchy.field_refs[0].dictionary_name, hierarchy.field_refs[0].variable_name)] = "continuous"

    fig = create_combined_umap(
        coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names,
        data_type_map=dt_map,
    )
    assert isinstance(fig, go.Figure)
    annotation_texts = [a.text for a in fig.layout.annotations]
    border_annotations = [t for t in annotation_texts if "Borders" in t]
    assert len(border_annotations) == 1


def test_combined_umap_data_type_in_hover(umap_coords_and_clusters):
    """Hover text includes data_type label when data_type_map provided."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    dt_map = {
        (ref.dictionary_name, ref.variable_name): {"type": "ordinal"}
        for ref in hierarchy.field_refs
    }
    fig = create_combined_umap(
        coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names,
        data_type_map=dt_map,
    )
    has_hover = any(
        any("Type: ordinal" in t for t in trace.text)
        for trace in fig.data
        if trace.text is not None and trace.hoverinfo != "skip"
    )
    assert has_hover


def test_combined_umap_data_type_plain_string(umap_coords_and_clusters):
    """Data-type map with plain string values works."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    dt_map = {
        (ref.dictionary_name, ref.variable_name): "continuous"
        for ref in hierarchy.field_refs
    }
    fig = create_combined_umap(
        coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names,
        data_type_map=dt_map,
    )
    has_hover = any(
        any("Type: continuous" in t for t in trace.text)
        for trace in fig.data
        if trace.text is not None and trace.hoverinfo != "skip"
    )
    assert has_hover


# ---------------------------------------------------------------------------
# Tests: Typed UMAP (cohort color + data_type shape + cluster spotlight)
# ---------------------------------------------------------------------------


def test_typed_umap_basic(umap_coords_and_clusters):
    """Typed UMAP creates a valid figure with numbered clusters."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    dt_map = {
        (ref.dictionary_name, ref.variable_name): "categorical"
        for ref in hierarchy.field_refs
    }
    fig = create_typed_umap(
        coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names,
        data_type_map=dt_map,
    )
    assert isinstance(fig, go.Figure)
    annotation_texts = [a.text for a in fig.layout.annotations]
    assert "<b>0</b>" in annotation_texts


def test_typed_umap_legend_is_cluster_based(umap_coords_and_clusters):
    """Typed UMAP legend entries are cluster-based, not cohort-based."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    dt_map = {
        (ref.dictionary_name, ref.variable_name): "ordinal"
        for ref in hierarchy.field_refs
    }
    fig = create_typed_umap(
        coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names,
        data_type_map=dt_map,
    )
    shown = [t.name for t in fig.data if t.showlegend is not False]
    assert any(":" in name for name in shown)
    assert "CohortA" not in shown


def test_typed_umap_subtitle_has_colors_and_shapes(umap_coords_and_clusters):
    """Typed UMAP subtitle shows both color key and shape key."""
    coords, hierarchy, clusters = umap_coords_and_clusters
    dt_map = {
        (ref.dictionary_name, ref.variable_name): "continuous"
        for ref in hierarchy.field_refs
    }
    fig = create_typed_umap(
        coords, hierarchy.field_refs, clusters, hierarchy.all_cohort_names,
        data_type_map=dt_map,
    )
    annotation_texts = [a.text for a in fig.layout.annotations]
    subtitle = [t for t in annotation_texts if "Colors" in t and "Shapes" in t]
    assert len(subtitle) == 1
