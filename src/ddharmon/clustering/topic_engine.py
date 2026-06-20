"""BERTopic clustering orchestrator: embeddings -> topics -> labeled clusters.

Thin orchestrator that builds a BERTopic model, fits it on pre-computed
embeddings, and converts results to FieldCluster objects for cohort
coverage tracking.  All heavy lifting is delegated to BERTopic itself —
use ``result.model.visualize_*()`` for built-in interactive Plotly viz.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict

import numpy as np
from numpy.typing import NDArray

from ddharmon.clustering.labeling import derive_cluster_label, label_clusters_llm
from ddharmon.embedding.service import EmbeddedDictionary
from ddharmon.llm.base import BaseLLMClient
from ddharmon.models.cluster import FieldCluster, FieldReference, TopicModelResult

logger = logging.getLogger(__name__)


# ── helpers ────────────────────────────────────────────────────


def collect_inputs(
    embedded_dicts: list[EmbeddedDictionary],
) -> tuple[list[str], NDArray[np.float32], list[FieldReference], list[str]]:
    """Flatten embedded dicts into parallel lists for BERTopic.

    Args:
        embedded_dicts: List of EmbeddedDictionary from embed_dictionary().

    Returns:
        Tuple of (docs, embeddings, field_refs, cohort_names) where:
        - docs: ``"var_name: description"`` strings for c-TF-IDF
        - embeddings: (N, D) stacked semantic vectors
        - field_refs: FieldReference per field (for cohort coverage)
        - cohort_names: unique cohort names in order
    """
    docs: list[str] = []
    vectors: list[NDArray[np.float32]] = []
    refs: list[FieldReference] = []
    cohort_names: list[str] = []

    for ed in embedded_dicts:
        cohort = ed.dictionary.cohort_name or ed.dictionary.name
        if cohort not in cohort_names:
            cohort_names.append(cohort)

        var_names = ed.get_variable_names()
        vecs = ed.get_all_vectors()
        for i, var in enumerate(var_names):
            fld = ed.dictionary.fields[var]
            docs.append(f"{var}: {fld.description}")
            refs.append(FieldReference(cohort, var, fld.description))
            vectors.append(vecs[i])

    return docs, np.stack(vectors), refs, cohort_names


def extract_topic_clusters(
    topics: list[int],
    field_refs: list[FieldReference],
    all_cohort_names: list[str],
) -> tuple[list[FieldCluster], FieldCluster | None]:
    """Convert BERTopic topic assignments to FieldCluster list.

    Produces the same FieldCluster format as hierarchical extract_clusters(),
    so downstream code (cohort coverage, cluster inspection) works unchanged.

    Args:
        topics: Per-document topic IDs from BERTopic (-1 = outlier).
        field_refs: Ordered list of FieldReference matching topic indices.
        all_cohort_names: All cohort names for coverage tracking.

    Returns:
        Tuple of (clusters, outlier_cluster).
    """
    all_cohorts_set = set(all_cohort_names)
    groups: dict[int, list[FieldReference]] = defaultdict(list)
    for ref, topic_id in zip(field_refs, topics, strict=True):
        groups[topic_id].append(ref)

    clusters: list[FieldCluster] = []
    outlier_cluster: FieldCluster | None = None

    for topic_id, members in sorted(groups.items()):
        coverage: dict[str, int] = defaultdict(int)
        for m in members:
            coverage[m.dictionary_name] += 1
        missing = sorted(all_cohorts_set - set(coverage.keys()))

        fc = FieldCluster(
            cluster_id=topic_id,
            label="",
            members=members,
            cohort_coverage=dict(coverage),
            missing_cohorts=missing,
        )
        if topic_id == -1:
            outlier_cluster = fc
        else:
            clusters.append(fc)

    return clusters, outlier_cluster


# ── main entry point ───────────────────────────────────────────


def topic_model_dictionaries(
    embedded_dicts: list[EmbeddedDictionary],
    *,
    min_cluster_size: int = 15,
    umap_n_components: int = 5,
    umap_n_neighbors: int = 15,
    nr_topics: int | None = None,
    reduce_outliers: bool = False,
    llm_client: BaseLLMClient | None = None,
    random_state: int = 42,
) -> TopicModelResult:
    """Cluster fields using BERTopic for large-scale topic discovery.

    Uses pre-computed embeddings from EmbeddedDictionary.  The returned
    ``TopicModelResult.model`` is the fitted BERTopic instance — call its
    ``visualize_*()`` methods directly for interactive Plotly charts::

        result = topic_model_dictionaries(embedded_dicts)
        result.model.visualize_documents(result.docs, embeddings=result.embeddings)
        result.model.visualize_hierarchy()
        result.model.visualize_topics()
        result.model.visualize_heatmap()

    Args:
        embedded_dicts: List of EmbeddedDictionary to cluster.
        min_cluster_size: HDBSCAN minimum cluster size.
        umap_n_components: UMAP dimensions for HDBSCAN (not visualization).
        umap_n_neighbors: UMAP n_neighbors for clustering step.
        nr_topics: If set, reduce to this many topics post-hoc.
        reduce_outliers: If True, reassign outlier fields to nearest topic.
        llm_client: Optional LLM client for upgraded cluster labels.
        random_state: Seed for reproducibility.

    Returns:
        TopicModelResult with FieldCluster list, fitted BERTopic model,
        docs, and embeddings for native visualization.
    """
    from bertopic import BERTopic
    from hdbscan import HDBSCAN
    from umap import UMAP

    t0 = time.perf_counter()

    # Step 1: Collect inputs
    docs, embeddings, field_refs, cohort_names = collect_inputs(embedded_dicts)
    logger.info("topic_model_dictionaries: %d fields from %d dicts", len(field_refs), len(embedded_dicts))

    # Step 2: Build and fit
    model = BERTopic(
        embedding_model=None,
        umap_model=UMAP(
            n_components=umap_n_components,
            n_neighbors=umap_n_neighbors,
            metric="cosine",
            random_state=random_state,
            low_memory=False,
        ),
        hdbscan_model=HDBSCAN(
            min_cluster_size=min_cluster_size,
            metric="euclidean",
            prediction_data=True,
        ),
        calculate_probabilities=True,
    )
    topics, probs = model.fit_transform(docs, embeddings)

    # Step 3: Optional post-hoc reduction
    if nr_topics is not None:
        topics, probs = model.reduce_topics(docs, nr_topics=nr_topics)
        logger.info("Reduced to %d topics", nr_topics)

    if reduce_outliers:
        topics = model.reduce_outliers(docs, topics)
        logger.info("Outliers reassigned to nearest topics")

    # Step 4: Convert to FieldCluster for cohort coverage tracking
    clusters, outlier_cluster = extract_topic_clusters(topics, field_refs, cohort_names)

    # Step 5: Label clusters
    for cluster in clusters:
        cluster.label = derive_cluster_label([m.description for m in cluster.members])
    if outlier_cluster:
        outlier_cluster.label = "Outliers"

    if llm_client is not None:
        try:
            label_clusters_llm(clusters, llm_client.complete)
            logger.info("LLM labels applied to %d topic clusters", len(clusters))
        except NotImplementedError:
            logger.info("LLM client does not implement complete(); using derived labels")

    elapsed = time.perf_counter() - t0
    n_outliers = len(outlier_cluster.members) if outlier_cluster else 0
    logger.info(
        "topic_model_dictionaries complete: %d fields, %d topics, %d outliers in %.2fs",
        len(field_refs), len(clusters), n_outliers, elapsed,
    )

    return TopicModelResult(
        model=model,
        docs=docs,
        embeddings=embeddings,
        field_refs=field_refs,
        clusters=clusters,
        outlier_cluster=outlier_cluster,
        all_cohort_names=cohort_names,
    )
