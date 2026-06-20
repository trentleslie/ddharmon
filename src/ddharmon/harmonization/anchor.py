"""CDE anchor selection for a value sub-cluster.

Given a sub-cluster (a mix of cohort fields and, when present, NIH CDEs that
landed in the same value sub-cluster), pick the single CDE that best anchors the
group. Selection is deterministic: the medoid (most central member) defines the
center, and CDE members are ranked by similarity-to-medoid, then *canonicalness*
(multi-collection + cross-mapped CDEs win ties), then metadata *richness*.

When no CDE is present in the sub-cluster, ``has_cde`` is False — the caller
should generate a GenCDE (or, in v1, classify the sub-cluster as ``novel``).
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

from ddharmon.embedding.service import EmbeddedDictionary
from ddharmon.harmonization.models import AnchorResult
from ddharmon.models.cluster import FieldReference
from ddharmon.models.data_dictionary import Field

CDE_COHORT = "NIH_CDE"  # default cohort name used when CDEs are loaded as a dictionary

FieldLookup = dict[tuple[str, str], Field]


def build_field_lookup(embedded_dicts: list[EmbeddedDictionary]) -> FieldLookup:
    """Build a ``(cohort, variable_name) -> Field`` lookup across embedded dicts."""
    lookup: FieldLookup = {}
    for ed in embedded_dicts:
        cohort = ed.dictionary.cohort_name or ed.dictionary.name
        for var_name, fld in ed.dictionary.fields.items():
            lookup[(cohort, var_name)] = fld
    return lookup


def field_encoding_type(fld: Field | None) -> str | None:
    """Best-effort encoding-type proxy for a Field.

    The Field model has no nested Encoding object — derive a label from the
    actual fields: response_options / value_encoding_raw -> "categorical", else
    fall back to data_type, else None.
    """
    if fld is None:
        return None
    if fld.response_options or fld.value_encoding_raw:
        return "categorical"
    return fld.data_type


def canonicalness_score(fld: Field | None) -> float:
    """Canonicalness signal for CDE anchor selection.

    Distinct from richness (metadata completeness). Rewards CDEs referenced in
    multiple NIH Collections and cross-mapped to ontology codes — both stronger
    indicators of canonical status than how many metadata fields are populated.
    Uses only data already on the Field object, no side-lookup required.
    """
    if fld is None:
        return 0.0
    score = 0.0
    if fld.category:
        # ``category`` holds the loaded ``classification`` string; multiple NIH
        # Collections are ``;``-separated in the source TSV.
        n_coll = sum(1 for chunk in fld.category.split(";") if chunk.strip())
        score += float(n_coll)
    if fld.standard_codes:
        # Cross-mappings to NCI Thesaurus / SNOMED / LOINC / etc.
        score += 0.2 * sum(len(v) for v in fld.standard_codes.values())
    return score


def field_richness(fld: Field | None) -> float:
    """Score how complete a CDE's metadata is — richer definitions anchor better."""
    if fld is None:
        return 0.0
    score = 0.0
    if fld.description and fld.description != "No definition available":
        score += 2
    if fld.question_text and fld.question_text != fld.description:
        score += 1
    if fld.response_options:
        score += 1 + min(len(fld.response_options), 5) * 0.1
    if fld.data_type:
        score += 0.5
    if fld.units:
        score += 0.5
    if fld.category:
        score += 0.5
    return score


def find_anchor_cde(
    sub_members: list[FieldReference],
    embeddings_all: NDArray[np.float32],
    field_refs: list[FieldReference],
    field_lookup: FieldLookup,
    *,
    cde_cohort: str = CDE_COHORT,
) -> AnchorResult:
    """Find the best anchor CDE for a sub-cluster.

    Args:
        sub_members: Members of one value sub-cluster.
        embeddings_all: Full ``(N, D)`` semantic embedding matrix (row order
            matches ``field_refs``).
        field_refs: Full ordered FieldReference list (for index lookup).
        field_lookup: ``(cohort, variable_name) -> Field`` for CDE metadata.
        cde_cohort: Cohort name under which CDEs were loaded.

    Returns:
        An :class:`AnchorResult`. ``has_cde`` is False when the sub-cluster
        contains no CDE member (GenCDE needed).
    """
    ref_index = {(ref.dictionary_name, ref.variable_name): i for i, ref in enumerate(field_refs)}
    member_indices = [
        ref_index[(m.dictionary_name, m.variable_name)]
        for m in sub_members
        if (m.dictionary_name, m.variable_name) in ref_index
    ]

    if len(member_indices) < 2:
        return AnchorResult(has_cde=False)

    # Normalize so dot product == cosine (matches the notebook's cosine_similarity,
    # and stays correct even if upstream vectors aren't already L2-normalized).
    raw_vecs = embeddings_all[member_indices]
    norms = np.linalg.norm(raw_vecs, axis=1, keepdims=True)
    member_vecs = raw_vecs / np.where(norms == 0, 1.0, norms)
    sim_matrix = member_vecs @ member_vecs.T

    # Medoid = the actual member most central to the sub-cluster.
    mean_sims = sim_matrix.mean(axis=1)
    medoid_local_idx = int(mean_sims.argmax())
    medoid_ref = field_refs[member_indices[medoid_local_idx]]

    cde_local_indices = [j for j, idx in enumerate(member_indices) if field_refs[idx].dictionary_name == cde_cohort]

    if not cde_local_indices:
        return AnchorResult(
            has_cde=False,
            medoid_ref=medoid_ref,
            medoid_sim=float(mean_sims[medoid_local_idx]),
        )

    # Rank CDE members by (similarity to medoid, canonicalness, richness).
    medoid_vec = member_vecs[medoid_local_idx]
    cde_sims = member_vecs[cde_local_indices] @ medoid_vec

    candidates: list[tuple[float, float, float, FieldReference, Field]] = []
    for j, local_idx in enumerate(cde_local_indices):
        ref = field_refs[member_indices[local_idx]]
        fld = field_lookup.get((ref.dictionary_name, ref.variable_name))
        if fld is None:
            continue
        candidates.append(
            (
                float(cde_sims[j]),
                canonicalness_score(fld),
                field_richness(fld),
                ref,
                fld,
            )
        )

    if not candidates:
        return AnchorResult(
            has_cde=False,
            medoid_ref=medoid_ref,
            medoid_sim=float(mean_sims[medoid_local_idx]),
        )

    # Sort key: similarity wins; canonicalness breaks similarity ties (a
    # multi-collection NIH CDE beats a rich-but-niche one); richness is last.
    candidates.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    best_sim, _, _, best_ref, best_field = candidates[0]
    alternates = [(ref, fld, sim) for sim, _, _, ref, fld in candidates[1:]]

    return AnchorResult(
        has_cde=True,
        anchor_ref=best_ref,
        anchor_field=best_field,
        medoid_ref=medoid_ref,
        medoid_sim=float(best_sim),
        alternate_cdes=alternates,
    )
