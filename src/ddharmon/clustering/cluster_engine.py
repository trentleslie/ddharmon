"""Clustering orchestrator: vectors -> linkage -> cuts -> labels -> ClusterHierarchy.

Follows the same orchestrator pattern as pairwise_mapper.py: accepts
EmbeddedDictionary inputs, wires internal modules, returns a result dataclass.
"""

from __future__ import annotations

import logging
import time

import numpy as np
from numpy.typing import NDArray

from ddharmon.clustering.hierarchical import compute_linkage, extract_clusters, suggest_cuts
from ddharmon.clustering.labeling import derive_cluster_label, label_clusters_llm
from ddharmon.embedding.service import EmbeddedDictionary
from ddharmon.llm.base import BaseLLMClient
from ddharmon.models.cluster import ClusterHierarchy, FieldCluster, FieldReference

logger = logging.getLogger(__name__)


def cluster_dictionaries(
    embedded_dicts: list[EmbeddedDictionary],
    *,
    linkage_method: str = "average",
    distance_metric: str = "cosine",
    llm_client: BaseLLMClient | None = None,
    custom_cuts: list[float] | None = None,
) -> ClusterHierarchy:
    """Cluster all fields across multiple embedded dictionaries.

    Orchestrates the full pipeline: collect vectors, compute linkage,
    silhouette-driven cut suggestions, flat cluster extraction, and
    label generation (derived by default, LLM-upgraded if client provided).

    Args:
        embedded_dicts: List of EmbeddedDictionary to cluster.
        linkage_method: Linkage method for agglomerative clustering
            ('average', 'complete', 'ward').
        distance_metric: Distance metric (currently informational; cosine
            is used for non-ward, euclidean for ward).
        llm_client: Optional LLM client for upgraded cluster labels.
            If None, derived labels are used (no API key needed).
        custom_cuts: Optional list of distance thresholds. If None,
            uses silhouette-suggested cuts.

    Returns:
        ClusterHierarchy with linkage matrix, field references,
        cut suggestions, and flat clusters at each cut distance.
    """
    t0 = time.perf_counter()

    # Step 1: Collect vectors and field references from all dictionaries
    all_vectors: list[NDArray[np.float32]] = []
    all_refs: list[FieldReference] = []
    all_cohort_names: list[str] = []

    for ed in embedded_dicts:
        var_names = ed.get_variable_names()
        vectors = ed.get_all_vectors()
        cohort_name = ed.dictionary.cohort_name or ed.dictionary.name

        if cohort_name not in all_cohort_names:
            all_cohort_names.append(cohort_name)

        for i, var_name in enumerate(var_names):
            fld = ed.dictionary.fields[var_name]
            all_refs.append(
                FieldReference(
                    dictionary_name=cohort_name,
                    variable_name=var_name,
                    description=fld.description,
                )
            )
            all_vectors.append(vectors[i])

    stacked = np.stack(all_vectors)
    n_fields = len(all_refs)
    logger.info("cluster_dictionaries: %d fields from %d dictionaries", n_fields, len(embedded_dicts))

    # Step 2: Compute linkage
    linkage_matrix = compute_linkage(stacked, method=linkage_method)

    # Step 3: Suggest cuts via silhouette analysis
    cut_suggestions = suggest_cuts(linkage_matrix, stacked)

    # Step 4: Determine cut distances
    cut_distances = custom_cuts if custom_cuts is not None else [s.distance for s in cut_suggestions]

    # Step 5: Extract clusters at each cut distance
    clusters_at_cuts: dict[float, list[FieldCluster]] = {}
    for dist in cut_distances:
        clusters = extract_clusters(linkage_matrix, all_refs, dist, all_cohort_names)

        # Step 6: Apply derived labels
        for cluster in clusters:
            descriptions = [m.description for m in cluster.members]
            cluster.label = derive_cluster_label(descriptions)

        clusters_at_cuts[dist] = clusters

    # Step 7: Optional LLM label upgrade
    if llm_client is not None:
        try:
            # Wrap client.complete() as Callable[[str], str] for label_clusters_llm
            llm_call = llm_client.complete
            for _dist, clusters in clusters_at_cuts.items():
                label_clusters_llm(clusters, llm_call)
            logger.info("LLM labels applied to all clusters")
        except NotImplementedError:
            logger.info(
                "LLM client %s does not implement complete(); using derived labels",
                type(llm_client).__name__,
            )

    # Determine model name from first dictionary
    model_name = embedded_dicts[0].model_name if embedded_dicts else ""

    result = ClusterHierarchy(
        linkage_matrix=linkage_matrix,
        field_refs=all_refs,
        cut_suggestions=cut_suggestions,
        clusters_at_cuts=clusters_at_cuts,
        model_name=model_name,
        all_cohort_names=all_cohort_names,
    )

    elapsed = time.perf_counter() - t0
    logger.info(
        "cluster_dictionaries complete: %d fields, %d cut suggestions, %d cuts applied in %.2fs",
        n_fields,
        len(cut_suggestions),
        len(clusters_at_cuts),
        elapsed,
    )

    return result
