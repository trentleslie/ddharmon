"""Export helpers for Metabolon mapping results.

Requires ``ddharmon[metabolon]`` (pandas, openpyxl).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ddharmon.models import MappingResult, MappingSummary


def flatten_results(results: list[MappingResult]) -> list[dict[str, Any]]:
    """Convert mapping results to a flat list of dicts suitable for DataFrame / CSV.

    Each dict has the columns::

        query_name, hmdb_hint, resolved, primary_curie, chosen_kg_id,
        confidence_score, confidence_tier,
        hmdb_ids, pubchem_ids, chebi_ids, refmet_ids, error

    Args:
        results: Output of :func:`ddharmon.map_entities` or equivalent.

    Returns:
        List of flat dicts, one per input result.
    """
    flat: list[dict[str, Any]] = []

    for r in results:
        flat.append(
            {
                "query_name": r.query_name,
                "hmdb_hint": r.hmdb_hint or "",
                "resolved": r.resolved,
                "primary_curie": r.primary_curie or "",
                "chosen_kg_id": r.chosen_kg_id or "",
                "confidence_score": r.confidence_score if r.confidence_score is not None else "",
                "confidence_tier": r.confidence_tier,
                "hmdb_ids": ";".join(r.ids_for("HMDB")),
                "pubchem_ids": ";".join(
                    r.ids_for("PUBCHEM.COMPOUND") or r.ids_for("PUBCHEM")
                ),
                "chebi_ids": ";".join(r.ids_for("CHEBI")),
                "refmet_ids": ";".join(r.ids_for("refmet_id")),
                "error": r.error or "",
            }
        )

    return flat


def results_to_dataframe(results: list[MappingResult]) -> Any:
    """Return a pandas DataFrame of flattened mapping results.

    Requires ``ddharmon[metabolon]`` (pandas).

    Args:
        results: Mapping results from any ddharmon mapping function.

    Returns:
        ``pandas.DataFrame`` with one row per result.
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "pandas is required for results_to_dataframe. "
            "Install with: pip install 'ddharmon[metabolon]'"
        ) from exc

    return pd.DataFrame(flatten_results(results))


def save_results(
    results: list[MappingResult],
    summary: MappingSummary | None = None,
    json_path: str | Path | None = None,
    tsv_path: str | Path | None = None,
) -> None:
    """Save mapping results to JSON and / or TSV.

    Args:
        results:   Mapping results to export.
        summary:   Optional :class:`~ddharmon.models.MappingSummary` to embed
                   in the JSON output.
        json_path: If provided, write full detail (summary + raw results) here.
        tsv_path:  If provided, write flat TSV suitable for spreadsheet review.

    Raises:
        ImportError: If tsv_path is given but pandas is not installed.
    """
    if json_path is not None:
        out: dict[str, Any] = {
            "summary": summary.model_dump() if summary else None,
            "mappings": [r.model_dump(exclude={"raw_response"}) for r in results],
        }
        Path(json_path).parent.mkdir(parents=True, exist_ok=True)
        with open(json_path, "w") as f:
            json.dump(out, f, indent=2, default=str)

    if tsv_path is not None:
        df = results_to_dataframe(results)
        Path(tsv_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(tsv_path, sep="\t", index=False)
