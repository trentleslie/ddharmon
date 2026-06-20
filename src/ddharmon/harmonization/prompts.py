"""Prompt construction for the v1 classify-only adopt/refine/novel pass.

v1 keeps a *single* LLM call per sub-cluster: classify the cluster's alignment
to its recommended CDE(s) as ``adopt`` / ``refine`` / ``novel`` (or, when the
cluster has no machine-readable response options, the concept-only
``adopt`` / ``unaligned``). No spec authoring, no coherence judging, no concept
labeling — those LLM passes are cut from v1 and the verdict is routed to EITL
for human verification.

The user-prompt builder takes a derived label, a representative member sample,
and the CDE candidates from ``find_anchor_cde``.
"""

from __future__ import annotations

from ddharmon.harmonization.models import AnchorResult
from ddharmon.models.cluster import FieldReference
from ddharmon.models.data_dictionary import Field

HARMONIZE_CLASSIFY_SYSTEM_PROMPT = """You are a domain expert in biomedical data dictionary harmonization. You are given:
  (a) a semantic cluster of cohort field descriptions (with response-option data available),
  (b) candidate NIH Common Data Elements (CDEs) recommended for this cluster.

Your task is to classify the cluster's alignment to the candidate CDEs. Choose exactly one verdict:

  - "adopt": one of the candidate CDEs already captures the cluster faithfully — concept, intent, AND permissible-value shape. Return its canonical_id. No spec will be authored.

  - "refine": one of the candidate CDEs captures the concept but the cluster needs a cohort-tailored variant — e.g., tightened permissible values, additional units, narrower question wording, or value-set extensions. Return the parent canonical_id. A human reviewer will author the spec.

  - "novel": no candidate CDE adequately represents the cluster's concept. A human reviewer will author a new (GenCDE) spec.

Be conservative on "adopt" — partial conceptual overlap is not sufficient. Prefer "refine" if the concept matches but the encoding does not. Do NOT author a spec in this call.
"""

HARMONIZE_CLASSIFY_SCHEMA = """{
  "verdict": "adopt | refine | novel",
  "parent_cde_id": "<canonical_id> | null",
  "confidence": 0.0,
  "evidence": "<1-2 sentence justification of the verdict>"
}"""

KGONLY_SYSTEM_PROMPT = """You are a domain expert in biomedical data dictionary harmonization. You are given:
  (a) a semantic cluster of cohort field descriptions for which machine-readable response options are NOT available,
  (b) candidate NIH Common Data Elements (CDEs) recommended for this cluster.

The cluster cannot be harmonized to a canonical value encoding (no response-option data), but its concept may still align to an existing CDE for knowledge-graph linkage (variable -> CDE -> linked ontologies). Your task is limited to CONCEPT-level alignment. Choose exactly one verdict:

  - "adopt": one candidate CDE captures the cluster's concept faithfully. Return its canonical_id.

  - "unaligned": no candidate CDE adequately represents the cluster's concept. The cluster enters the EITL queue for ontology curation.

Be conservative on "adopt" — partial conceptual overlap is not sufficient. Without value-encoding evidence, judge purely on concept match, not encoding shape.
"""

KGONLY_SCHEMA = """{
  "verdict": "adopt | unaligned",
  "parent_cde_id": "<canonical_id> | null",
  "confidence": 0.0,
  "evidence": "<1-2 sentence justification>"
}"""


def system_prompt_for_mode(mode: str) -> tuple[str, str]:
    """Return ``(system_prompt, schema)`` for a gate mode (``harmonize`` | ``kg_only``)."""
    if mode == "kg_only":
        return KGONLY_SYSTEM_PROMPT, KGONLY_SCHEMA
    return HARMONIZE_CLASSIFY_SYSTEM_PROMPT, HARMONIZE_CLASSIFY_SCHEMA


def _cde_canonical_id(ref: FieldReference, fld: Field | None) -> str:
    """Prefer the CDE's tinyId (field_id) as canonical identifier; fall back to designation."""
    if fld is not None and fld.field_id:
        return fld.field_id
    return ref.variable_name


def anchor_candidates(anchor: AnchorResult, *, max_candidates: int = 5) -> list[dict]:
    """Flatten an :class:`AnchorResult` into candidate dicts for the prompt.

    The best anchor leads, followed by alternates, each as
    ``{canonical_id, designation, text, cosine}``.
    """
    if not anchor.has_cde or anchor.anchor_ref is None or anchor.anchor_field is None:
        return []

    candidates: list[tuple[FieldReference, Field, float]] = [
        (anchor.anchor_ref, anchor.anchor_field, anchor.medoid_sim or 0.0),
        *anchor.alternate_cdes,
    ]

    out: list[dict] = []
    for ref, fld, sim in candidates[:max_candidates]:
        out.append(
            {
                "canonical_id": _cde_canonical_id(ref, fld),
                "designation": ref.variable_name,
                "text": (fld.description or "")[:500],
                "cosine": float(sim),
            }
        )
    return out


def representative_members(
    members: list[FieldReference],
    field_lookup: dict[tuple[str, str], Field],
    *,
    max_members: int = 8,
) -> list[str]:
    """Build short ``variable: text`` lines for a sample of cohort members.

    CDE members are excluded (they appear as candidates, not cluster content).
    """
    lines: list[str] = []
    for m in members:
        fld = field_lookup.get((m.dictionary_name, m.variable_name))
        text = (fld.question_text or fld.description if fld else None) or m.description
        lines.append(f"{m.dictionary_name}/{m.variable_name}: {text}")
        if len(lines) >= max_members:
            break
    return lines


def build_classify_user_prompt(
    *,
    sub_cluster_id: str,
    label: str,
    member_lines: list[str],
    candidates: list[dict],
    mode: str,
) -> str:
    """Build the user prompt for the classify-only A/R/N call.

    Args:
        sub_cluster_id: ``f"{topic}:{sub_label}"`` identifier (echoed for traceability).
        label: Derived (c-TF-IDF) sub-cluster label.
        member_lines: Representative ``variable: text`` lines (see
            :func:`representative_members`).
        candidates: CDE candidate dicts (see :func:`anchor_candidates`).
        mode: ``harmonize`` (3-way verdict) or ``kg_only`` (concept-only).
    """
    members_block = "\n".join(f"  {i + 1}. {line}" for i, line in enumerate(member_lines))
    cand_block = "\n".join(
        f"  [{i + 1}] canonical_id={c['canonical_id']} (cos={c['cosine']:.2f}): " f"{c['designation']} — {c['text']}"
        for i, c in enumerate(candidates)
    )

    header = (
        f'Sub-cluster {sub_cluster_id} — label: "{label}"\n\n'
        f"Representative cohort members:\n{members_block}\n\n"
        f"Candidate CDEs (recommended anchor first):\n{cand_block}\n\n"
    )

    if mode == "kg_only":
        return header + (
            "NOTE: source variables in this cluster do not have machine-readable response "
            "options; judge concept-level alignment only.\n"
            "Pick exactly one verdict (adopt or unaligned). When adopting, use the canonical_id "
            "string exactly as shown. Do not author a spec."
        )

    return header + (
        "Classify the cluster's alignment. Pick exactly one verdict (adopt, refine, or novel). "
        "For adopt and refine, set parent_cde_id to the chosen canonical_id (exact string). "
        "For novel, set parent_cde_id to null. Do NOT author a spec — a human reviewer handles that."
    )
