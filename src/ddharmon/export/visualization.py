"""Interactive Plotly visualizations for clustering results.

Provides dendrogram, 2D scatter (UMAP/t-SNE), paired UMAP views
(cohort-colored + cluster-colored), per-cluster value similarity
heatmaps, and static PNG export via kaleido.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import plotly.express as px
import plotly.figure_factory as ff
import plotly.graph_objects as go
from numpy.typing import NDArray

from ddharmon.models.cluster import ClusterHierarchy, FieldCluster, FieldReference

if TYPE_CHECKING:
    from ddharmon.embedding.service import EmbeddedDictionary

logger = logging.getLogger(__name__)

# Default qualitative palette for cohorts
_PLOTLY_PALETTE = px.colors.qualitative.Plotly

# Shared layout constants for paired UMAP views
_UMAP_WIDTH = 1400
_UMAP_HEIGHT = 900
_UMAP_MARKER_SIZE = 4
_UMAP_MARKER_OPACITY = 0.7
_UMAP_BG = "white"
_UMAP_FONT_COLOR = "black"


def create_dendrogram(
    hierarchy: ClusterHierarchy,
    cohort_color_map: dict[str, str] | None = None,
) -> go.Figure:
    """Create an interactive Plotly dendrogram from a ClusterHierarchy.

    Args:
        hierarchy: Clustering result with linkage matrix and field references.
        cohort_color_map: Optional mapping of cohort name -> hex color.
            If None, auto-generates from Plotly qualitative palette.

    Returns:
        Plotly Figure with interactive dendrogram, hover labels, and
        horizontal dashed lines for each cut suggestion.
    """
    if cohort_color_map is None:
        cohort_color_map = {
            name: _PLOTLY_PALETTE[i % len(_PLOTLY_PALETTE)] for i, name in enumerate(hierarchy.all_cohort_names)
        }

    labels = [f"{fr.variable_name} ({fr.dictionary_name})" for fr in hierarchy.field_refs]

    fig = ff.create_dendrogram(
        X=np.eye(len(hierarchy.field_refs)),  # Dummy — overridden by linkagefun
        labels=labels,
        linkagefun=lambda _x: hierarchy.linkage_matrix,
    )

    fig.update_layout(
        title="Field Clustering Dendrogram",
        xaxis_title="Fields",
        yaxis_title="Distance",
        width=max(1200, len(hierarchy.field_refs) * 15),
        height=800,
    )

    # Add horizontal lines for each cut suggestion
    for cs in hierarchy.cut_suggestions:
        fig.add_hline(
            y=cs.distance,
            line_dash="dash",
            annotation_text=f"Suggested cut (k={cs.n_clusters}, sil={cs.silhouette_score:.2f})",
        )

    return fig


def create_scatter_plot(
    vectors: NDArray[np.float32],
    field_refs: list[FieldReference],
    *,
    method: str = "umap",
    random_state: int = 42,
) -> go.Figure:
    """Create a 2D scatter plot of field embeddings colored by cohort.

    Performs dimensionality reduction (UMAP or t-SNE) then renders an
    interactive Plotly scatter with hover labels showing variable name,
    cohort, and description.

    Args:
        vectors: (N, D) embedding matrix.
        field_refs: Ordered list of FieldReference matching vector rows.
        method: Reduction method, "umap" or "tsne".
        random_state: Seed for reproducibility.

    Returns:
        Plotly Figure with 2D scatter plot.

    Raises:
        ValueError: If method is not "umap" or "tsne".
    """
    if method == "umap":
        from umap import UMAP

        reducer = UMAP(
            n_components=2, metric="cosine", random_state=random_state, n_neighbors=min(15, len(vectors) - 1)
        )
    elif method == "tsne":
        from sklearn.manifold import TSNE

        reducer = TSNE(
            n_components=2,
            metric="cosine",
            random_state=random_state,
            perplexity=min(30, len(vectors) - 1),
        )
    else:
        raise ValueError(f"Unknown method: {method!r}. Use 'umap' or 'tsne'.")

    coords = reducer.fit_transform(vectors)

    cohorts = [fr.dictionary_name for fr in field_refs]
    hover_names = [fr.variable_name for fr in field_refs]
    descriptions = [fr.description for fr in field_refs]

    fig = px.scatter(
        x=coords[:, 0],
        y=coords[:, 1],
        color=cohorts,
        hover_name=hover_names,
        hover_data={"description": descriptions},
        title=f"Field Embeddings ({method.upper()})",
    )

    fig.update_traces(marker={"size": 6, "opacity": 0.7})

    return fig


def create_value_heatmaps(
    clusters: list[FieldCluster],
    embedded_dicts: list[EmbeddedDictionary],
) -> dict[int, go.Figure]:
    """Create per-cluster value similarity heatmaps.

    For each cluster with >=2 members that have value embeddings,
    computes pairwise cosine similarity of value vectors and renders
    a heatmap.

    Args:
        clusters: List of FieldCluster from a given cut distance.
        embedded_dicts: List of EmbeddedDictionary containing value_embeddings.

    Returns:
        Dict mapping cluster_id -> Plotly heatmap Figure.
        Clusters with <2 members having value embeddings are skipped.
    """
    # Build lookup: (dictionary_name, variable_name) -> value_embedding
    value_lookup: dict[tuple[str, str], NDArray[np.float32]] = {}
    for ed in embedded_dicts:
        dict_name = ed.dictionary.name
        for var_name, vec in ed.value_embeddings.items():
            value_lookup[(dict_name, var_name)] = vec

    result: dict[int, go.Figure] = {}

    for cluster in clusters:
        if len(cluster.members) < 2:
            continue

        # Collect value vectors for members that have them
        member_vecs: list[NDArray[np.float32]] = []
        member_labels: list[str] = []
        for m in cluster.members:
            key = (m.dictionary_name, m.variable_name)
            if key in value_lookup:
                member_vecs.append(value_lookup[key])
                member_labels.append(f"{m.variable_name}\n({m.dictionary_name})")

        if len(member_vecs) < 2:
            continue

        # Compute pairwise cosine similarity (assumes L2-normalized vectors)
        vals = np.stack(member_vecs)
        similarity_matrix = np.dot(vals, vals.T)

        fig = go.Figure(
            data=go.Heatmap(
                z=similarity_matrix,
                x=member_labels,
                y=member_labels,
                colorscale="RdYlGn",
                zmin=0,
                zmax=1,
            )
        )
        fig.update_layout(title=f"Value Similarity: {cluster.label}")

        result[cluster.cluster_id] = fig

    return result


def compute_umap_coords(
    vectors: NDArray[np.float32],
    *,
    random_state: int = 42,
    n_neighbors: int = 15,
) -> NDArray[np.float64]:
    """Compute 2D UMAP coordinates from embedding vectors.

    Separated from plotting so the same coordinates can be reused
    across multiple visualizations (e.g., cohort view + cluster view).

    Args:
        vectors: (N, D) embedding matrix.
        random_state: Seed for reproducibility.
        n_neighbors: UMAP n_neighbors (capped at len(vectors)-1).

    Returns:
        (N, 2) array of UMAP coordinates.
    """
    from umap import UMAP

    reducer = UMAP(
        n_components=2,
        metric="cosine",
        random_state=random_state,
        n_neighbors=min(n_neighbors, len(vectors) - 1),
    )
    return reducer.fit_transform(vectors)


def _shared_umap_layout(
    title: str,
    coords: NDArray[np.float64] | None = None,
    *,
    width: int = _UMAP_WIDTH,
    height: int = _UMAP_HEIGHT,
) -> dict:
    """Return shared layout kwargs for paired UMAP figures.

    If coords are provided, locks axis ranges with 5% padding so both
    views align exactly when displayed side-by-side.
    """
    layout: dict = {
        "title": title,
        "paper_bgcolor": _UMAP_BG,
        "plot_bgcolor": _UMAP_BG,
        "font_color": _UMAP_FONT_COLOR,
        "width": width,
        "height": height,
        "xaxis_title": "UMAP1",
        "yaxis_title": "UMAP2",
        # Fixed margins so plot area is identical regardless of legend size
        "margin": {"l": 60, "r": 300, "t": 60, "b": 60},
    }
    if coords is not None:
        x_pad = (coords[:, 0].max() - coords[:, 0].min()) * 0.05
        y_pad = (coords[:, 1].max() - coords[:, 1].min()) * 0.05
        layout["xaxis_range"] = [float(coords[:, 0].min() - x_pad), float(coords[:, 0].max() + x_pad)]
        layout["yaxis_range"] = [float(coords[:, 1].min() - y_pad), float(coords[:, 1].max() + y_pad)]
    return layout


def create_cohort_umap(
    coords: NDArray[np.float64],
    field_refs: list[FieldReference],
    clusters: list[FieldCluster],
    cohort_names: list[str],
    *,
    grid_cells: int = 6,
    min_label_members: int = 2,
    marker_size: int = _UMAP_MARKER_SIZE,
    marker_opacity: float = _UMAP_MARKER_OPACITY,
    width: int = _UMAP_WIDTH,
    height: int = _UMAP_HEIGHT,
    title: str | None = None,
    cohort_color_map: dict[str, str] | None = None,
) -> go.Figure:
    """Create a UMAP scatter colored by cohort with cluster labels overlaid.

    Labels are placed using a grid-based strategy: the UMAP space is divided
    into cells and the largest cluster per cell is labeled, ensuring spatial
    coverage without overcrowding.

    Args:
        coords: (N, 2) UMAP coordinates (from compute_umap_coords).
        field_refs: Ordered list of FieldReference matching coord rows.
        clusters: Flat cluster list at a given cut distance.
        cohort_names: Ordered cohort names for consistent coloring.
        grid_cells: Grid resolution for label placement (grid_cells x grid_cells).
        min_label_members: Minimum cluster size to be eligible for labeling.
        marker_size: Point size in pixels.
        marker_opacity: Point opacity (0.0–1.0).
        width: Figure width in pixels.
        height: Figure height in pixels.
        title: Custom title. If None, auto-generated.
        cohort_color_map: Optional mapping of cohort name -> hex color.
            If None, auto-generates from Plotly qualitative palette.

    Returns:
        Plotly Figure colored by cohort with cluster label annotations.
    """
    ref_index = {(ref.dictionary_name, ref.variable_name): i for i, ref in enumerate(field_refs)}

    # Cohort colors
    if cohort_color_map is None:
        cohort_palette = px.colors.qualitative.Plotly + px.colors.qualitative.D3
        cohort_color_map = {name: cohort_palette[i % len(cohort_palette)] for i, name in enumerate(cohort_names)}

    fig = go.Figure()

    for cohort in cohort_names:
        mask = [i for i, ref in enumerate(field_refs) if ref.dictionary_name == cohort]
        fig.add_trace(go.Scatter(
            x=coords[mask, 0],
            y=coords[mask, 1],
            mode="markers",
            marker={"size": marker_size, "opacity": marker_opacity, "color": cohort_color_map[cohort]},
            name=cohort,
            text=[
                f"<b>{field_refs[i].variable_name}</b><br>{field_refs[i].description}"
                for i in mask
            ],
            hoverinfo="text+name",
        ))

    # Grid-based cluster label placement
    centroids = []
    for c in clusters:
        if len(c.members) < min_label_members:
            continue
        indices = [ref_index[(m.dictionary_name, m.variable_name)] for m in c.members
                   if (m.dictionary_name, m.variable_name) in ref_index]
        if not indices:
            continue
        cx = float(np.mean(coords[indices, 0]))
        cy = float(np.mean(coords[indices, 1]))
        centroids.append((cx, cy, c, len(c.members)))

    x_min, x_max = float(coords[:, 0].min()), float(coords[:, 0].max())
    y_min, y_max = float(coords[:, 1].min()), float(coords[:, 1].max())
    cell_w = (x_max - x_min) / grid_cells
    cell_h = (y_max - y_min) / grid_cells

    grid: dict[tuple[int, int], tuple[float, float, FieldCluster, int]] = {}
    for cx, cy, c, size in centroids:
        gx = int((cx - x_min) / cell_w) if cell_w > 0 else 0
        gy = int((cy - y_min) / cell_h) if cell_h > 0 else 0
        key = (gx, gy)
        if key not in grid or size > grid[key][3]:
            grid[key] = (cx, cy, c, size)

    for cx, cy, c, size in grid.values():
        font_size = max(8, min(12, size // 3))
        fig.add_annotation(
            x=cx, y=cy,
            text=f"<b>{c.label}</b> ({size})",
            showarrow=True, arrowhead=0, arrowcolor="gray",
            ax=0, ay=-20,
            font={"size": font_size, "color": "black"},
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor="gray", borderwidth=1, borderpad=3,
        )

    n_labels = len(grid)
    auto_title = f"Field Embeddings by Cohort — {len(field_refs)} fields, {n_labels} cluster labels"
    fig.update_layout(
        **_shared_umap_layout(title or auto_title, coords, width=width, height=height),
        legend={"title": "Cohort", "font": {"size": 11}},
    )

    return fig


def create_cluster_umap(
    coords: NDArray[np.float64],
    field_refs: list[FieldReference],
    clusters: list[FieldCluster],
    *,
    top_n: int = 30,
    marker_size: int = _UMAP_MARKER_SIZE,
    marker_opacity: float = _UMAP_MARKER_OPACITY,
    width: int = _UMAP_WIDTH,
    height: int = _UMAP_HEIGHT,
    title: str | None = None,
    other_marker_size: int = 3,
    other_marker_opacity: float = 0.3,
) -> go.Figure:
    """Create a UMAP scatter colored by cluster membership with numbered centroids.

    The top_n largest clusters are each colored distinctly and numbered at their
    centroid. Remaining fields are shown in gray as "Other". The legend shows
    cluster rank, label, and member count.

    Args:
        coords: (N, 2) UMAP coordinates (from compute_umap_coords).
        field_refs: Ordered list of FieldReference matching coord rows.
        clusters: Flat cluster list at a given cut distance.
        top_n: Number of top clusters to color (rest shown as gray).
        marker_size: Point size in pixels for top-N clusters.
        marker_opacity: Point opacity (0.0–1.0) for top-N clusters.
        width: Figure width in pixels.
        height: Figure height in pixels.
        title: Custom title. If None, auto-generated.
        other_marker_size: Point size for "Other" (non-top-N) fields.
        other_marker_opacity: Opacity for "Other" fields.

    Returns:
        Plotly Figure colored by cluster with numbered centroids and legend.
    """
    ref_index = {(ref.dictionary_name, ref.variable_name): i for i, ref in enumerate(field_refs)}

    # Sort clusters by size
    sorted_clusters = sorted(clusters, key=lambda c: -len(c.members))

    # Color palette
    palette = px.colors.qualitative.Dark24 + px.colors.qualitative.Light24

    fig = go.Figure()

    # Track which points are in top-N clusters
    top_n_point_set: set[int] = set()

    for rank, c in enumerate(sorted_clusters[:top_n]):
        indices = [ref_index[(m.dictionary_name, m.variable_name)] for m in c.members
                   if (m.dictionary_name, m.variable_name) in ref_index]
        if not indices:
            continue
        top_n_point_set.update(indices)
        color = palette[rank % len(palette)]

        fig.add_trace(go.Scatter(
            x=coords[indices, 0],
            y=coords[indices, 1],
            mode="markers",
            marker={"size": marker_size, "opacity": marker_opacity, "color": color},
            name=f"{rank}: {c.label} ({len(c.members)})",
            text=[
                f"<b>{field_refs[i].variable_name}</b><br>{field_refs[i].description}"
                for i in indices
            ],
            hoverinfo="text+name",
        ))

        # Number at centroid
        cx = float(np.mean(coords[indices, 0]))
        cy = float(np.mean(coords[indices, 1]))
        fig.add_annotation(
            x=cx, y=cy,
            text=f"<b>{rank}</b>",
            showarrow=False,
            font={"size": 11, "color": "black", "family": "Arial Black"},
            bgcolor="rgba(255,255,255,0.8)",
            borderpad=2,
        )

    # Remaining points in gray
    remaining = [i for i in range(len(field_refs)) if i not in top_n_point_set]
    if remaining:
        fig.add_trace(go.Scatter(
            x=coords[remaining, 0],
            y=coords[remaining, 1],
            mode="markers",
            marker={"size": other_marker_size, "opacity": other_marker_opacity, "color": "gray"},
            name=f"Other ({len(remaining)} fields)",
            text=[
                f"<b>{field_refs[i].variable_name}</b><br>{field_refs[i].description}"
                for i in remaining
            ],
            hoverinfo="text",
        ))

    n_shown = min(top_n, len(sorted_clusters))
    auto_title = f"Semantic Clusters (UMAP) — top {n_shown} of {len(clusters)} clusters"
    fig.update_layout(
        **_shared_umap_layout(title or auto_title, coords, width=width, height=height),
        legend={
            "title": "Clusters (by size)",
            "font": {"size": 9},
            "yanchor": "top", "y": 1, "xanchor": "left", "x": 1.02,
            "bgcolor": "rgba(255,255,255,0.9)",
        },
    )

    return fig


_COHORT_SHAPES = ["circle", "square", "diamond", "triangle-up", "cross", "star", "hexagon", "bowtie"]

# Palette for dynamically assigning colors/shapes to distinct data_type values.
_DATA_TYPE_BORDER_PALETTE = ["#000000", "#2563eb", "#7c3aed", "#059669", "#d97706", "#dc2626", "#0891b2", "#ca8a04"]
_DATA_TYPE_BORDER_DEFAULT = "#9ca3af"  # gray for unknown/missing


def _assign_per_value(values: list[str], palette: list[str], default: str) -> dict[str, str]:
    """Assign a palette entry to each distinct non-empty value in order of appearance."""
    seen: dict[str, str] = {}
    for v in values:
        if v and v not in seen:
            seen[v] = palette[len(seen) % len(palette)]
    seen[""] = default
    return seen


def build_data_type_map(
    embedded_dicts: list[EmbeddedDictionary],
) -> dict[tuple[str, str], str]:
    """Build a (dict_name, var_name) -> raw data_type string lookup.

    Uses the source-declared data_type column directly (no inference).
    Fields without a data_type are omitted.

    Args:
        embedded_dicts: List of EmbeddedDictionary with original DataDictionary.

    Returns:
        Dict mapping (cohort_name, variable_name) to the raw data_type string
        (e.g., "continuous", "Categorical (Single)", "Integer").
    """
    result: dict[tuple[str, str], str] = {}
    for ed in embedded_dicts:
        cohort = ed.dictionary.cohort_name or ed.dictionary.name
        for var_name, field in ed.dictionary.fields.items():
            if field.data_type:
                result[(cohort, var_name)] = field.data_type
    return result


def build_value_text_map(
    embedded_dicts: list[EmbeddedDictionary],
    *,
    max_options: int = 8,
) -> dict[tuple[str, str], dict[str, str]]:
    """Build a (dict_name, var_name) -> field metadata lookup for hover text.

    Args:
        embedded_dicts: List of EmbeddedDictionary with original DataDictionary.
        max_options: Maximum number of response options to include before truncating.

    Returns:
        Dict mapping (cohort_name, variable_name) to a dict with optional keys:
        - "values": response option labels (e.g., "Never, Rarely, Sometimes, Often, Daily")
        - "type": data type (e.g., "ordinal", "Continuous")
        - "units": units of measurement (e.g., "kg/m2", "days/week")
        - "standard_codes": ontology codes (e.g., "SNOMED:60621009")
        Fields without any metadata are omitted.
    """
    result: dict[tuple[str, str], dict[str, str]] = {}
    for ed in embedded_dicts:
        cohort = ed.dictionary.cohort_name or ed.dictionary.name
        for var_name, fld in ed.dictionary.fields.items():
            info: dict[str, str] = {}
            if fld.response_options:
                labels = [o.label for o in fld.response_options[:max_options]]
                text = ", ".join(labels)
                if len(fld.response_options) > max_options:
                    text += f" ... (+{len(fld.response_options) - max_options} more)"
                info["values"] = text
            if fld.data_type:
                info["type"] = fld.data_type
            if fld.units:
                info["units"] = fld.units
            if fld.standard_codes:
                codes = [f"{sys}:{code}" for sys, codes_list in fld.standard_codes.items() for code in codes_list]
                if codes:
                    info["standard_codes"] = ", ".join(codes[:5])
            if info:
                result[(cohort, var_name)] = info
    return result


def create_combined_umap(
    coords: NDArray[np.float64],
    field_refs: list[FieldReference],
    clusters: list[FieldCluster],
    cohort_names: list[str],
    *,
    top_n: int = 30,
    marker_size: int = 5,
    marker_opacity: float = _UMAP_MARKER_OPACITY,
    width: int = _UMAP_WIDTH,
    height: int = _UMAP_HEIGHT,
    title: str | None = None,
    other_marker_size: int = 3,
    other_marker_opacity: float = 0.3,
    cohort_shape_map: dict[str, str] | None = None,
    data_type_map: dict[tuple[str, str], str | dict[str, str]] | None = None,
    data_type_border_colors: dict[str, str] | None = None,
    data_type_border_width: float = 2.0,
    value_text_map: dict[tuple[str, str], dict[str, str]] | None = None,
) -> go.Figure:
    """Create a UMAP scatter combining cluster color and cohort shape.

    Encodes up to three dimensions simultaneously:
    - **Color** = cluster membership (fill color)
    - **Shape** = source cohort (marker symbol)
    - **Border color** = raw data_type (marker outline) — optional

    Top-N clusters are colored distinctly with numbered centroids;
    remaining fields shown in gray.

    Args:
        coords: (N, 2) UMAP coordinates (from compute_umap_coords).
        field_refs: Ordered list of FieldReference matching coord rows.
        clusters: Flat cluster list at a given cut distance.
        cohort_names: Ordered cohort names for consistent shape assignment.
        top_n: Number of top clusters to color (rest shown as gray).
        marker_size: Point size in pixels for top-N clusters.
        marker_opacity: Point opacity (0.0-1.0) for top-N clusters.
        width: Figure width in pixels.
        height: Figure height in pixels.
        title: Custom title. If None, auto-generated.
        other_marker_size: Point size for "Other" (non-top-N) fields.
        other_marker_opacity: Opacity for "Other" fields.
        cohort_shape_map: Optional mapping of cohort name -> Plotly marker symbol.
            If None, auto-assigns from built-in shape list.
        data_type_map: Optional mapping of (dict_name, var_name) -> data_type.
            Values can be a plain string (e.g., "continuous") or a dict with a
            "type" key. Use build_data_type_map() to create. When provided,
            marker borders are colored by data_type and hover text includes
            the data_type label.
        data_type_border_colors: Optional mapping of data_type string -> hex color
            for border. If None, colors are auto-assigned per distinct value.
        data_type_border_width: Border line width in pixels when data_type_map is
            provided. Default 2.0.
        value_text_map: Optional mapping of (dict_name, var_name) -> metadata dict
            (as returned by build_value_text_map()). When provided, hover text
            includes response options, data type, units, and standard codes.

    Returns:
        Plotly Figure with cluster-colored, cohort-shaped markers.
    """
    ref_index = {(ref.dictionary_name, ref.variable_name): i for i, ref in enumerate(field_refs)}

    # Cohort shapes
    if cohort_shape_map is None:
        cohort_shape_map = {
            name: _COHORT_SHAPES[i % len(_COHORT_SHAPES)]
            for i, name in enumerate(cohort_names)
        }

    # Data-type border colors (auto-assign per distinct value if no explicit map)
    use_data_type = data_type_map is not None

    def _get_data_type(idx: int) -> str:
        """Get data_type string for a field index."""
        if not use_data_type or not data_type_map:
            return ""
        key = (field_refs[idx].dictionary_name, field_refs[idx].variable_name)
        val = data_type_map.get(key, "")
        if isinstance(val, dict):
            return val.get("type", "")
        return val

    if use_data_type:
        border_colors = data_type_border_colors or _assign_per_value(
            [_get_data_type(i) for i in range(len(field_refs))],
            _DATA_TYPE_BORDER_PALETTE,
            _DATA_TYPE_BORDER_DEFAULT,
        )
    else:
        border_colors = {}

    def _border_color(idx: int) -> str:
        """Get border color for a field index based on data_type."""
        if not use_data_type:
            return "white"
        return border_colors.get(_get_data_type(idx), _DATA_TYPE_BORDER_DEFAULT)

    def _border_width() -> float:
        return data_type_border_width if use_data_type else 0.5

    def _encoding_label(idx: int) -> str:
        """Get data_type label for hover text."""
        if not use_data_type:
            return ""
        dt = _get_data_type(idx)
        if not dt:
            return ""
        return f"<br>Type: {dt}"

    def _value_label(idx: int) -> str:
        """Get structured value metadata lines for hover text."""
        if not value_text_map:
            return ""
        key = (field_refs[idx].dictionary_name, field_refs[idx].variable_name)
        info = value_text_map.get(key)
        if not info:
            return ""
        lines: list[str] = []
        if "values" in info:
            lines.append(f"<br>Values: {info['values']}")
        type_parts: list[str] = []
        if "type" in info:
            type_parts.append(info["type"])
        if "units" in info:
            type_parts.append(f"Units: {info['units']}")
        if type_parts:
            lines.append(f"<br>Type: {' | '.join(type_parts)}")
        if "standard_codes" in info:
            lines.append(f"<br>Codes: {info['standard_codes']}")
        return "".join(lines)

    sorted_clusters = sorted(clusters, key=lambda c: -len(c.members))
    palette = px.colors.qualitative.Dark24 + px.colors.qualitative.Light24

    # Reverse lookup: field index -> (rank, cluster label, cluster size)
    _cluster_info: dict[int, tuple[int | None, str, int]] = {}
    for rank, c in enumerate(sorted_clusters):
        for m in c.members:
            key = (m.dictionary_name, m.variable_name)
            if key in ref_index:
                is_top = rank < top_n
                _cluster_info[ref_index[key]] = (rank if is_top else None, c.label, len(c.members))

    # Precompute cohort count per cluster for hover text
    _cluster_cohort_counts: dict[int, int] = {}
    for rank, c in enumerate(sorted_clusters):
        _cluster_cohort_counts[rank] = len(c.cohort_coverage)

    n_total_cohorts = len(cohort_names)

    def _cluster_hover(idx: int) -> str:
        """Get cluster membership line for hover text."""
        info = _cluster_info.get(idx)
        if not info:
            return ""
        rank, label, size = info
        if rank is not None:
            n_cohorts = _cluster_cohort_counts.get(rank, 0)
            return f"<br>Cluster {rank}: {label} ({size} members, {n_cohorts}/{n_total_cohorts} cohorts)"
        # For non-top-N clusters, find cohort count from sorted_clusters
        return f"<br>Cluster: {label} ({size} members)"

    fig = go.Figure()
    top_n_point_set: set[int] = set()

    for rank, c in enumerate(sorted_clusters[:top_n]):
        indices = [ref_index[(m.dictionary_name, m.variable_name)] for m in c.members
                   if (m.dictionary_name, m.variable_name) in ref_index]
        if not indices:
            continue
        top_n_point_set.update(indices)
        color = palette[rank % len(palette)]

        # Legend-only trace: single point at centroid with circle symbol.
        # Ensures legend icon is always a filled circle regardless of
        # which cohort shapes appear in the cluster's data traces.
        cx = float(np.mean(coords[indices, 0]))
        cy = float(np.mean(coords[indices, 1]))
        fig.add_trace(go.Scatter(
            x=[cx], y=[cy],
            mode="markers",
            marker={"size": 8, "color": color, "symbol": "circle"},
            name=f"{rank}: {c.label} ({len(c.members)})",
            legendgroup=f"cluster_{rank}",
            showlegend=True,
            hoverinfo="skip",
        ))

        # Data traces per cohort for shape differentiation (hidden from legend)
        for cohort in cohort_names:
            cohort_indices = [i for i in indices if field_refs[i].dictionary_name == cohort]
            if not cohort_indices:
                continue
            shape = cohort_shape_map[cohort]
            border_line = {
                "width": _border_width(),
                "color": [_border_color(i) for i in cohort_indices] if use_data_type else "white",
            }
            fig.add_trace(go.Scatter(
                x=coords[cohort_indices, 0],
                y=coords[cohort_indices, 1],
                mode="markers",
                marker={
                    "size": marker_size, "opacity": marker_opacity, "color": color,
                    "symbol": shape, "line": border_line,
                },
                name=f"{rank}: {c.label} ({len(c.members)})",
                legendgroup=f"cluster_{rank}",
                showlegend=False,
                text=[
                    f"<b>{field_refs[i].variable_name}</b> ({cohort})<br>{field_refs[i].description}{_cluster_hover(i)}{_value_label(i)}{_encoding_label(i)}"
                    for i in cohort_indices
                ],
                hoverinfo="text",
            ))

        # Number at centroid
        cx = float(np.mean(coords[indices, 0]))
        cy = float(np.mean(coords[indices, 1]))
        fig.add_annotation(
            x=cx, y=cy,
            text=f"<b>{rank}</b>",
            showarrow=False,
            font={"size": 11, "color": "black", "family": "Arial Black"},
            bgcolor="rgba(255,255,255,0.8)",
            borderpad=2,
        )

    # Remaining points in gray (per cohort for shape)
    remaining = [i for i in range(len(field_refs)) if i not in top_n_point_set]
    if remaining:
        n_remaining = len(remaining)
        # Legend-only trace for "Other" with circle symbol
        fig.add_trace(go.Scatter(
            x=[float(np.mean(coords[remaining, 0]))],
            y=[float(np.mean(coords[remaining, 1]))],
            mode="markers",
            marker={"size": 8, "color": "gray", "symbol": "circle"},
            name=f"Other ({n_remaining} fields)",
            legendgroup="other",
            showlegend=True,
            hoverinfo="skip",
        ))
        for cohort in cohort_names:
            cohort_remaining = [i for i in remaining if field_refs[i].dictionary_name == cohort]
            if not cohort_remaining:
                continue
            shape = cohort_shape_map[cohort]
            other_border = {
                "width": _border_width(),
                "color": [_border_color(i) for i in cohort_remaining] if use_data_type else "white",
            }
            fig.add_trace(go.Scatter(
                x=coords[cohort_remaining, 0],
                y=coords[cohort_remaining, 1],
                mode="markers",
                marker={
                    "size": other_marker_size, "opacity": other_marker_opacity,
                    "color": "gray", "symbol": shape, "line": other_border,
                },
                name=f"Other ({n_remaining} fields)",
                legendgroup="other",
                showlegend=False,
                text=[
                    f"<b>{field_refs[i].variable_name}</b> ({cohort})<br>{field_refs[i].description}{_cluster_hover(i)}{_value_label(i)}{_encoding_label(i)}"
                    for i in cohort_remaining
                ],
                hoverinfo="text",
            ))

    # Key annotations as subtitle
    shape_text = "    ".join(f"{cohort_shape_map[name]}: {name}" for name in cohort_names)
    subtitle = f"Shapes — {shape_text}"
    if use_data_type:
        enc_text = "    ".join(
            f'<span style="color:{c}">■</span> {t}'
            for t, c in sorted(border_colors.items()) if t
        )
        subtitle += f"        Borders — {enc_text}"
    fig.add_annotation(
        x=0.5, y=1.02, xref="paper", yref="paper",
        text=subtitle,
        showarrow=False,
        font={"size": 10, "color": "gray"},
    )

    n_shown = min(top_n, len(sorted_clusters))
    auto_title = f"Clusters + Cohorts (UMAP) — top {n_shown} of {len(clusters)} clusters"
    fig.update_layout(
        **_shared_umap_layout(title or auto_title, coords, width=width, height=height),
        legend={
            "title": "Clusters (by size)",
            "font": {"size": 9},
            "yanchor": "top", "y": 1, "xanchor": "left", "x": 1.02,
            "bgcolor": "rgba(255,255,255,0.9)",
        },
    )

    return fig


_DATA_TYPE_SHAPE_PALETTE = ["circle", "square", "diamond", "triangle-up", "star", "hexagon", "cross", "bowtie"]
_DATA_TYPE_SHAPE_DEFAULT = "x"


def create_typed_umap(
    coords: NDArray[np.float64],
    field_refs: list[FieldReference],
    clusters: list[FieldCluster],
    cohort_names: list[str],
    data_type_map: dict[tuple[str, str], str | dict[str, str]],
    *,
    top_n: int = 30,
    marker_size: int = 5,
    marker_opacity: float = _UMAP_MARKER_OPACITY,
    width: int = _UMAP_WIDTH,
    height: int = _UMAP_HEIGHT,
    title: str | None = None,
    other_marker_size: int = 3,
    other_marker_opacity: float = 0.3,
    cohort_color_map: dict[str, str] | None = None,
    data_type_shape_map: dict[str, str] | None = None,
    value_text_map: dict[tuple[str, str], dict[str, str]] | None = None,
) -> go.Figure:
    """Create a UMAP scatter with cohort color, data_type shape, and cluster spotlight.

    Visual encoding:
    - **Fill color** = source cohort
    - **Shape** = raw data_type string from source dictionary
    - **Numbers** = cluster centroids (overlaid)
    - **Legend** = cluster list (click to spotlight/isolate a single cluster)

    Each cluster appears as a legendgroup containing sub-traces split by
    cohort x data_type. Clicking a cluster in the legend toggles
    visibility of all its points.

    Args:
        coords: (N, 2) UMAP coordinates (from compute_umap_coords).
        field_refs: Ordered list of FieldReference matching coord rows.
        clusters: Flat cluster list at a given cut distance.
        cohort_names: Ordered cohort names for color assignment.
        data_type_map: Mapping of (dict_name, var_name) -> data_type.
            Values can be a plain string (e.g., "continuous") or a dict with
            a "type" key. Use build_data_type_map() to create.
        top_n: Number of top clusters to show in legend (rest = "Other").
        marker_size: Point size in pixels for top-N clusters.
        marker_opacity: Point opacity (0.0-1.0) for top-N clusters.
        width: Figure width in pixels.
        height: Figure height in pixels.
        title: Custom title. If None, auto-generated.
        other_marker_size: Point size for "Other" fields.
        other_marker_opacity: Opacity for "Other" fields.
        cohort_color_map: Optional mapping of cohort name -> hex color.
            If None, auto-generates from Plotly palette.
        data_type_shape_map: Optional mapping of data_type string -> Plotly symbol.
            If None, shapes are auto-assigned per distinct data_type value.

    Returns:
        Plotly Figure with cohort-colored, data_type-shaped markers and
        cluster legend for spotlight toggling.
    """
    ref_index = {(ref.dictionary_name, ref.variable_name): i for i, ref in enumerate(field_refs)}

    # Cohort colors
    if cohort_color_map is None:
        cohort_palette = px.colors.qualitative.Plotly + px.colors.qualitative.D3
        cohort_color_map = {name: cohort_palette[i % len(cohort_palette)] for i, name in enumerate(cohort_names)}

    def _get_data_type(idx: int) -> str:
        key = (field_refs[idx].dictionary_name, field_refs[idx].variable_name)
        val = data_type_map.get(key, "")
        if isinstance(val, dict):
            return val.get("type", "")
        return val

    # Data-type shapes (auto-assign per distinct value if no explicit map)
    shape_map = data_type_shape_map or _assign_per_value(
        [_get_data_type(i) for i in range(len(field_refs))],
        _DATA_TYPE_SHAPE_PALETTE,
        _DATA_TYPE_SHAPE_DEFAULT,
    )

    def _get_shape(idx: int) -> str:
        return shape_map.get(_get_data_type(idx), _DATA_TYPE_SHAPE_DEFAULT)

    sorted_clusters = sorted(clusters, key=lambda c: -len(c.members))

    # Reverse lookup: field index -> (rank, cluster label, cluster size)
    _typed_cluster_info: dict[int, tuple[int | None, str, int]] = {}
    for rank, c in enumerate(sorted_clusters):
        for m in c.members:
            key = (m.dictionary_name, m.variable_name)
            if key in ref_index:
                is_top = rank < top_n
                _typed_cluster_info[ref_index[key]] = (rank if is_top else None, c.label, len(c.members))

    # Precompute cohort count per cluster for typed UMAP hover
    _typed_cohort_counts: dict[int, int] = {}
    for rank, c in enumerate(sorted_clusters):
        _typed_cohort_counts[rank] = len(c.cohort_coverage)
    n_total_cohorts = len(cohort_names)

    def _hover_text(idx: int) -> str:
        ref = field_refs[idx]
        dt = _get_data_type(idx)
        enc_str = f"<br>Type: {dt}" if dt else ""
        info = _typed_cluster_info.get(idx)
        if info:
            rank, label, size = info
            if rank is not None:
                n_coh = _typed_cohort_counts.get(rank, 0)
                cluster_str = f"<br>Cluster {rank}: {label} ({size} members, {n_coh}/{n_total_cohorts} cohorts)"
            else:
                cluster_str = f"<br>Cluster: {label} ({size} members)"
        else:
            cluster_str = ""
        # Value metadata
        val_str = ""
        if value_text_map:
            val_key = (ref.dictionary_name, ref.variable_name)
            val_info = value_text_map.get(val_key)
            if val_info:
                parts: list[str] = []
                if "values" in val_info:
                    parts.append(f"<br>Values: {val_info['values']}")
                type_parts: list[str] = []
                if "type" in val_info:
                    type_parts.append(val_info["type"])
                if "units" in val_info:
                    type_parts.append(f"Units: {val_info['units']}")
                if type_parts:
                    parts.append(f"<br>Type: {' | '.join(type_parts)}")
                if "standard_codes" in val_info:
                    parts.append(f"<br>Codes: {val_info['standard_codes']}")
                val_str = "".join(parts)
        return f"<b>{ref.variable_name}</b> ({ref.dictionary_name})<br>{ref.description}{cluster_str}{val_str}{enc_str}"

    fig = go.Figure()
    top_n_point_set: set[int] = set()

    for rank, c in enumerate(sorted_clusters[:top_n]):
        indices = [ref_index[(m.dictionary_name, m.variable_name)] for m in c.members
                   if (m.dictionary_name, m.variable_name) in ref_index]
        if not indices:
            continue
        top_n_point_set.update(indices)
        legend_name = f"{rank}: {c.label} ({len(c.members)})"

        # Legend-only trace (circle, neutral color) for consistent legend icon
        cx = float(np.mean(coords[indices, 0]))
        cy = float(np.mean(coords[indices, 1]))
        fig.add_trace(go.Scatter(
            x=[cx], y=[cy],
            mode="markers",
            marker={"size": 8, "color": "black", "symbol": "circle", "opacity": 0.3},
            name=legend_name,
            legendgroup=f"cluster_{rank}",
            showlegend=True,
            hoverinfo="skip",
        ))

        # Data traces: one per (cohort, data_type) combination
        groups: dict[tuple[str, str], list[int]] = {}
        for i in indices:
            key = (field_refs[i].dictionary_name, _get_data_type(i))
            groups.setdefault(key, []).append(i)

        for (cohort, dt), group_indices in groups.items():
            color = cohort_color_map.get(cohort, "#6b7280")
            shape = shape_map.get(dt, _DATA_TYPE_SHAPE_DEFAULT)
            fig.add_trace(go.Scatter(
                x=coords[group_indices, 0],
                y=coords[group_indices, 1],
                mode="markers",
                marker={
                    "size": marker_size, "opacity": marker_opacity, "color": color,
                    "symbol": shape, "line": {"width": 0.5, "color": "white"},
                },
                name=legend_name,
                legendgroup=f"cluster_{rank}",
                showlegend=False,
                text=[_hover_text(i) for i in group_indices],
                hoverinfo="text",
            ))

        # Number at centroid
        fig.add_annotation(
            x=cx, y=cy,
            text=f"<b>{rank}</b>",
            showarrow=False,
            font={"size": 11, "color": "black", "family": "Arial Black"},
            bgcolor="rgba(255,255,255,0.8)",
            borderpad=2,
        )

    # Remaining points
    remaining = [i for i in range(len(field_refs)) if i not in top_n_point_set]
    if remaining:
        n_remaining = len(remaining)
        # Legend-only trace
        fig.add_trace(go.Scatter(
            x=[float(np.mean(coords[remaining, 0]))],
            y=[float(np.mean(coords[remaining, 1]))],
            mode="markers",
            marker={"size": 8, "color": "gray", "symbol": "circle"},
            name=f"Other ({n_remaining} fields)",
            legendgroup="other",
            showlegend=True,
            hoverinfo="skip",
        ))
        # Data traces by (cohort, data_type)
        groups_other: dict[tuple[str, str], list[int]] = {}
        for i in remaining:
            key = (field_refs[i].dictionary_name, _get_data_type(i))
            groups_other.setdefault(key, []).append(i)

        for (cohort, dt), group_indices in groups_other.items():
            color = cohort_color_map.get(cohort, "#6b7280")
            shape = shape_map.get(dt, _DATA_TYPE_SHAPE_DEFAULT)
            fig.add_trace(go.Scatter(
                x=coords[group_indices, 0],
                y=coords[group_indices, 1],
                mode="markers",
                marker={
                    "size": other_marker_size, "opacity": other_marker_opacity,
                    "color": color, "symbol": shape,
                },
                name=f"Other ({n_remaining} fields)",
                legendgroup="other",
                showlegend=False,
                text=[_hover_text(i) for i in group_indices],
                hoverinfo="text",
            ))

    # Subtitle key
    color_text = "    ".join(
        f'<span style="color:{cohort_color_map[n]}">●</span> {n}' for n in cohort_names
    )
    shape_text = "    ".join(
        f"{s}: {t}" for t, s in sorted(shape_map.items()) if t
    )
    fig.add_annotation(
        x=0.5, y=1.02, xref="paper", yref="paper",
        text=f"Colors — {color_text}        Shapes — {shape_text}",
        showarrow=False,
        font={"size": 10, "color": "gray"},
    )

    n_shown = min(top_n, len(sorted_clusters))
    auto_title = f"Cohorts + Types (UMAP) — top {n_shown} of {len(clusters)} clusters"
    fig.update_layout(
        **_shared_umap_layout(title or auto_title, coords, width=width, height=height),
        legend={
            "title": "Clusters (click to spotlight)",
            "font": {"size": 9},
            "yanchor": "top", "y": 1, "xanchor": "left", "x": 1.02,
            "bgcolor": "rgba(255,255,255,0.9)",
            "groupclick": "toggleitem",
        },
    )

    return fig


def export_png(
    fig: go.Figure,
    path: str | Path,
    width: int = 1200,
    height: int = 800,
) -> Path:
    """Export a Plotly figure to static PNG using kaleido.

    Args:
        fig: Plotly Figure to export.
        path: Output file path.
        width: Image width in pixels.
        height: Image height in pixels.

    Returns:
        Path to the created PNG file.
    """
    out = Path(path)
    fig.write_image(str(out), width=width, height=height, engine="kaleido")
    return out
