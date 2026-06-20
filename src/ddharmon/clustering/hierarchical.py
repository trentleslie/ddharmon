"""Hierarchical clustering: linkage computation, fcluster, and silhouette analysis.

Uses scipy for agglomerative clustering and scikit-learn for silhouette scoring.
"""

from __future__ import annotations

import logging
from collections import defaultdict

import numpy as np
from numpy.typing import NDArray
from scipy.cluster.hierarchy import fcluster, linkage
from scipy.spatial.distance import pdist
from sklearn.metrics import silhouette_score

from ddharmon.models.cluster import CutSuggestion, FieldCluster, FieldReference

logger = logging.getLogger(__name__)


def compute_linkage(vectors: NDArray, method: str = "average") -> NDArray[np.float64]:
    """Compute hierarchical clustering linkage matrix from embedding vectors.

    Args:
        vectors: (N, D) matrix of embedding vectors (typically L2-normalized).
        method: Linkage method ('average', 'complete', 'ward').
                Ward requires Euclidean distance; others use cosine.

    Returns:
        Linkage matrix of shape (N-1, 4) as float64.
    """
    distances = pdist(vectors, metric="euclidean") if method == "ward" else pdist(vectors, metric="cosine")

    z = linkage(distances, method=method)
    logger.info("compute_linkage: %d vectors, method=%s, max_dist=%.4f", len(vectors), method, float(z[:, 2].max()))
    return z


def suggest_cuts(
    linkage_matrix: NDArray,
    vectors: NDArray,
    n_steps: int = 50,
    top_n: int = 3,
) -> list[CutSuggestion]:
    """Scan distance thresholds and return top-scoring CutSuggestions.

    Evaluates silhouette scores across a range of distance thresholds,
    filtering out extremes (fewer than 2 clusters or more than N/2 clusters).

    Args:
        linkage_matrix: Linkage matrix from compute_linkage.
        vectors: Original (N, D) embedding vectors for silhouette computation.
        n_steps: Number of threshold steps to scan.
        top_n: Number of top suggestions to return.

    Returns:
        List of CutSuggestion sorted by silhouette_score descending.
    """
    n_samples = len(vectors)
    min_dist = float(linkage_matrix[:, 2].min())
    max_dist = float(linkage_matrix[:, 2].max())

    if min_dist >= max_dist:
        logger.warning("suggest_cuts: min_dist >= max_dist (%.4f >= %.4f), no suggestions", min_dist, max_dist)
        return []

    thresholds = np.linspace(min_dist + 0.01, max_dist - 0.01, n_steps)
    max_clusters = n_samples // 2

    results: list[CutSuggestion] = []
    for t in thresholds:
        labels = fcluster(linkage_matrix, t=t, criterion="distance")
        n_clusters = len(set(labels))

        # Filter extremes: fewer than 2 or more than N/2
        if n_clusters < 2 or n_clusters > max_clusters:
            continue

        # Compute silhouette score using cosine metric
        score = silhouette_score(vectors, labels, metric="cosine")
        results.append(CutSuggestion(distance=float(t), silhouette_score=float(score), n_clusters=n_clusters))

    # Sort by silhouette score descending, return top_n
    results.sort(key=lambda s: s.silhouette_score, reverse=True)
    suggestions = results[:top_n]

    logger.info(
        "suggest_cuts: scanned %d thresholds, %d valid, returning top %d",
        len(thresholds),
        len(results),
        len(suggestions),
    )
    return suggestions


def extract_clusters(
    linkage_matrix: NDArray,
    field_refs: list[FieldReference],
    distance: float,
    all_cohort_names: list[str],
) -> list[FieldCluster]:
    """Extract flat clusters at a given distance threshold.

    Groups field_refs by cluster assignment and computes cohort coverage.

    Args:
        linkage_matrix: Linkage matrix from compute_linkage.
        field_refs: Ordered list of FieldReference matching linkage matrix rows.
        distance: Distance threshold for flat cluster extraction.
        all_cohort_names: All cohort names to compute missing_cohorts.

    Returns:
        List of FieldCluster with members and cohort coverage.
    """
    labels = fcluster(linkage_matrix, t=distance, criterion="distance")

    # Group field refs by cluster label
    groups: dict[int, list[FieldReference]] = defaultdict(list)
    for ref, label in zip(field_refs, labels, strict=True):
        groups[int(label)].append(ref)

    all_cohorts_set = set(all_cohort_names)
    clusters: list[FieldCluster] = []

    for cluster_id, members in sorted(groups.items()):
        # Compute cohort coverage
        coverage: dict[str, int] = defaultdict(int)
        for member in members:
            coverage[member.dictionary_name] += 1

        missing = sorted(all_cohorts_set - set(coverage.keys()))

        clusters.append(
            FieldCluster(
                cluster_id=cluster_id,
                label="",  # Label assigned later by labeling module
                members=members,
                cohort_coverage=dict(coverage),
                missing_cohorts=missing,
            )
        )

    logger.info("extract_clusters: distance=%.4f, %d clusters from %d fields", distance, len(clusters), len(field_refs))
    return clusters
