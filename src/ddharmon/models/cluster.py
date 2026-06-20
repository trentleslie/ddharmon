"""Data models for semantic clustering results.

Dataclasses for hierarchical clustering of fields across multiple
data dictionaries, including cut suggestions, cluster membership,
cohort coverage, and labeling.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
from numpy.typing import NDArray


@dataclass
class FieldReference:
    """Reference to a field within a specific dictionary.

    Denormalizes description so labeling and hover can work without
    looking up the original DataDictionary.
    """

    dictionary_name: str
    variable_name: str
    description: str  # Denormalized for labeling/hover


@dataclass
class SubClusterResult:
    """Value-vector sub-clustering outcome for one semantic (topic) cluster.

    ``sub_clusters`` maps a sub-label to its members; label ``-1`` is HDBSCAN
    noise. ``excluded`` holds members with no value vector (not harmonizable).
    ``status`` is one of: ``subclustered`` | ``single_group`` | ``too_few`` |
    ``all_noise``.
    """

    status: str
    sub_clusters: dict[int, list[FieldReference]] = field(default_factory=dict)
    excluded: list[FieldReference] = field(default_factory=list)


@dataclass
class CutSuggestion:
    """A suggested distance threshold with silhouette evidence.

    Silhouette scores range from -1 (bad) to +1 (good separation).
    """

    distance: float
    silhouette_score: float
    n_clusters: int


@dataclass
class FieldCluster:
    """A group of semantically related fields at a given cut distance.

    Tracks which cohorts contributed members and which are missing,
    surfacing universal concepts vs cohort-specific fields.
    """

    cluster_id: int
    label: str  # Derived or LLM-generated
    members: list[FieldReference] = field(default_factory=list)
    cohort_coverage: dict[str, int] = field(default_factory=dict)  # cohort_name -> member count
    missing_cohorts: list[str] = field(default_factory=list)  # Cohorts with no members


@dataclass
class ClusterHierarchy:
    """Complete clustering result.

    Contains the full linkage matrix for dendrogram rendering, ordered
    field references, data-driven cut suggestions, and flat cluster
    assignments at each suggested (or custom) cut distance.
    """

    linkage_matrix: NDArray[np.float64]  # scipy linkage matrix (N-1, 4)
    field_refs: list[FieldReference]  # Ordered list matching linkage matrix rows
    cut_suggestions: list[CutSuggestion]  # Data-driven recommended thresholds
    clusters_at_cuts: dict[float, list[FieldCluster]] = field(default_factory=dict)  # distance -> flat clusters
    model_name: str = ""  # Embedding model used
    all_cohort_names: list[str] = field(default_factory=list)  # All cohorts that contributed fields


@dataclass
class TopicModelResult:
    """BERTopic clustering result.

    Wraps the fitted BERTopic model with our domain additions (cohort
    coverage, field refs).  Use ``model.visualize_*()`` for built-in
    interactive Plotly charts; use ``clusters`` for cohort coverage
    inspection.

    Attributes:
        model: Fitted BERTopic instance.  Call ``model.visualize_documents(docs, embeddings=embeddings)``
            etc. for native interactive visualizations.
        docs: Text documents passed to BERTopic (``"var: description"``).
        embeddings: (N, D) pre-computed embedding matrix.
        field_refs: Ordered FieldReference list matching rows in *embeddings*.
        clusters: Non-outlier topic clusters with cohort coverage.
        outlier_cluster: Topic -1 fields, if any.
        all_cohort_names: Cohorts that contributed fields.
    """

    model: Any  # Fitted BERTopic instance
    docs: list[str]  # Documents for visualize_documents / c-TF-IDF
    embeddings: Any  # NDArray — kept as Any to avoid numpy import at class level
    field_refs: list[FieldReference]
    clusters: list[FieldCluster]
    outlier_cluster: FieldCluster | None
    all_cohort_names: list[str] = field(default_factory=list)

    @property
    def n_topics(self) -> int:
        """Number of non-outlier topics."""
        return len(self.clusters)

    @property
    def topic_info(self) -> Any:
        """BERTopic topic info DataFrame (delegates to model)."""
        try:
            return self.model.get_topic_info()
        except Exception:
            return None
