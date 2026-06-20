"""Value-vector sub-clustering within a semantic (topic) cluster.

Phase 1b of the ddharmon architecture: a semantic cluster groups fields by
*what concept* they measure; this step splits each cluster by *how the concept
is answered* (response-option / encoding structure), using the value embedding
vectors that are deliberately held out of semantic clustering.
"""

from __future__ import annotations

from collections import defaultdict

import numpy as np
from numpy.typing import NDArray

from ddharmon.embedding.service import EmbeddedDictionary
from ddharmon.models.cluster import FieldCluster, SubClusterResult

ValueVectorLookup = dict[tuple[str, str], NDArray[np.float32]]


def build_value_vector_lookup(embedded_dicts: list[EmbeddedDictionary]) -> ValueVectorLookup:
    """Build a ``(cohort, variable_name) -> value vector`` lookup across dicts.

    Only fields that produced a value embedding (i.e. had response options,
    data_type, or units) appear. The cohort key matches the ``dictionary_name``
    carried on each :class:`~ddharmon.models.cluster.FieldReference`.
    """
    lookup: ValueVectorLookup = {}
    for ed in embedded_dicts:
        cohort = ed.dictionary.cohort_name or ed.dictionary.name
        for var_name, vec in ed.value_embeddings.items():
            lookup[(cohort, var_name)] = vec
    return lookup


def value_subcluster(
    cluster: FieldCluster,
    value_vecs: ValueVectorLookup,
    *,
    min_value_members: int = 8,
) -> SubClusterResult:
    """Sub-cluster a topic's members by their value vectors via HDBSCAN.

    Calibrated assuming the input is an *effective* cluster (post semantic
    sub-topic splitting): value heterogeneity is already much lower than at the
    raw topic level, so the right ``min_cluster_size`` is small but not 2.

    - ``min_value_members=8``: below this, value sub-clustering can't recover
      reliable signal; treat the whole group as one ("single_group").
    - ``mcs = max(3, n // 20)``: a hard floor of 3 avoids HDBSCAN's
      every-pair-is-a-cluster behavior at mcs=2; the ``n // 20`` ratio (at most
      5% of members) stays gentle enough that legitimate small encoding-variant
      groups surface rather than getting absorbed into noise.

    Args:
        cluster: The semantic (topic) cluster to sub-cluster.
        value_vecs: ``(cohort, variable_name) -> value vector`` lookup.
        min_value_members: Minimum members-with-value-vectors to attempt HDBSCAN.

    Returns:
        A :class:`SubClusterResult`. ``sub_clusters`` keys are HDBSCAN labels
        (``-1`` is noise); ``excluded`` holds members lacking a value vector.
    """
    members_with: list[tuple] = []
    excluded = []
    for m in cluster.members:
        vec = value_vecs.get((m.dictionary_name, m.variable_name))
        if vec is not None:
            members_with.append((m, vec))
        else:
            excluded.append(m)

    # Too few fields with value vectors to recover signal — single group.
    if len(members_with) < min_value_members:
        if members_with:
            return SubClusterResult(
                status="single_group",
                sub_clusters={0: [m for m, _ in members_with]},
                excluded=excluded,
            )
        return SubClusterResult(status="too_few", sub_clusters={}, excluded=excluded)

    from hdbscan import HDBSCAN

    vecs = np.stack([v for _, v in members_with])
    mcs = max(3, len(members_with) // 20)
    sub_labels = HDBSCAN(min_cluster_size=mcs, metric="euclidean").fit_predict(vecs)

    sub_groups: dict[int, list] = defaultdict(list)
    for (m, _), label in zip(members_with, sub_labels, strict=True):
        sub_groups[int(label)].append(m)

    # All noise — no sub-cluster structure found; fall back to one group.
    real_clusters = {k: v for k, v in sub_groups.items() if k != -1}
    if not real_clusters:
        return SubClusterResult(
            status="all_noise",
            sub_clusters={0: [m for m, _ in members_with]},
            excluded=excluded,
        )

    return SubClusterResult(status="subclustered", sub_clusters=dict(sub_groups), excluded=excluded)
