"""Metabolon-specific preprocessing for BioMapper2 mapping pipelines.

These utilities clean raw Metabolon output (compound names with quote artifacts
and collision-energy suffixes) and build a deduplicated mapping queue from a
pandas DataFrame.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


# ---------------------------------------------------------------------------
# Name cleaning
# ---------------------------------------------------------------------------


def clean_compound_name(name: Any) -> str | None:  # noqa: ANN401
    """Clean a raw Metabolon compound name for better API resolution.

    Steps applied in order:

    1. Return ``None`` for NA / empty values.
    2. Strip leading / trailing whitespace.
    3. Strip surrounding double-quotes (``"Glucose"`` → ``Glucose``).
    4. Remove Metabolon collision-energy suffixes (``_CE45``, ``_CE205060``).
    5. Strip any trailing whitespace left after suffix removal.

    Args:
        name: Raw value from a Metabolon compound-name column.  May be
              ``float("nan")``, ``None``, or any string-like object.

    Returns:
        Cleaned string, or ``None`` if the result is empty / was NA.

    Examples::

        clean_compound_name('"1,3-Diphenylguanidine_CE45"')
        # '1,3-Diphenylguanidine'

        clean_compound_name('L-Histidine')
        # 'L-Histidine'

        clean_compound_name(float('nan'))
        # None
    """
    # Handle pandas NA / None / empty string / Python float nan
    if name is None:
        return None
    # Handle Python native float nan (math.nan, float('nan'))
    if isinstance(name, float) and name != name:  # NaN != NaN
        return None
    try:
        import pandas as pd

        if pd.isna(name):
            return None
    except (ImportError, TypeError, ValueError):
        pass

    text = str(name).strip()
    if not text:
        return None

    # Strip surrounding quotes
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        text = text[1:-1]

    # Remove _CE## suffix (e.g. _CE45, _CE205060, _CE20_60)
    text = re.sub(r"_CE[\d_]+$", "", text)

    text = text.strip()
    return text or None


# ---------------------------------------------------------------------------
# HMDB hint extraction
# ---------------------------------------------------------------------------


def extract_hmdb_id(ms1_name: Any) -> str | None:  # noqa: ANN401
    """Extract an HMDB identifier from a Metabolon ``ms1_compound_name`` field.

    Handles these common formats:

    * ``HMDB:HMDB03349-2257 L-Dihydroorotic acid``  →  ``HMDB03349``
    * ``HMDB00177``                                 →  ``HMDB00177``
    * ``NaN`` / ``None``                            →  ``None``

    Args:
        ms1_name: Raw value from the ``ms1_compound_name`` column.

    Returns:
        Bare HMDB accession string (e.g. ``"HMDB03349"``), or ``None``.
    """
    if ms1_name is None:
        return None
    try:
        import pandas as pd

        if pd.isna(ms1_name):
            return None
    except (ImportError, TypeError, ValueError):
        pass

    text = str(ms1_name)

    # Format: "HMDB:HMDB03349-2257 ..."
    m = re.search(r"HMDB:(HMDB\d+)", text)
    if m:
        return m.group(1)

    # Format: bare HMDB accession anywhere in string
    m = re.search(r"\b(HMDB\d{5,7})\b", text)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Mapping queue builder
# ---------------------------------------------------------------------------


@dataclass
class MetabolonRecord:
    """A single row ready to send to the BioMapper2 API.

    Attributes:
        name:       Cleaned compound name (the query).
        hmdb_hint:  HMDB ID to pass as a resolver hint (may be ``None``).
        feature_ids: Original ``feature_id`` values that share this name
                     (deduplicated compounds expand back out via join).
        match_level: Highest-confidence match level among all features
                     (``"MS2"`` > ``"MS1"`` > ``"CURATION"``).
    """

    name: str
    hmdb_hint: str | None = None
    feature_ids: list[str] = field(default_factory=list)
    match_level: str | None = None

    def as_api_record(self) -> dict[str, Any]:
        """Return a dict suitable for :func:`ddharmon.map_entities`."""
        rec: dict[str, Any] = {"name": self.name}
        if self.hmdb_hint:
            rec["identifiers"] = {"HMDB": self.hmdb_hint}
        return rec


_MATCH_LEVEL_PRIORITY: dict[str, int] = {"MS2": 3, "MS1": 2, "CURATION": 1}


def build_mapping_queue(
    df: pd.DataFrame,
    name_col: str = "matched_name",
    hint_col: str = "ms1_compound_name",
    feature_id_col: str = "feature_id",
    match_level_col: str = "match_level",
    limit: int | None = None,
) -> list[MetabolonRecord]:
    """Build a deduplicated mapping queue from a Metabolon features DataFrame.

    Deduplication strategy:

    * Groups rows by cleaned ``name_col`` value.
    * Keeps the first non-null HMDB hint per name.
    * Tracks all ``feature_id`` values that share the name (for join-back).
    * Selects the highest-priority ``match_level`` per name.

    Args:
        df:             Metabolon features DataFrame (e.g. from ``.xlsx`` export).
        name_col:       Column containing compound names.  Default ``matched_name``.
        hint_col:       Column to extract HMDB hints from.  Default ``ms1_compound_name``.
        feature_id_col: Column with unique feature identifiers.  Default ``feature_id``.
        match_level_col:Column with match level strings.  Default ``match_level``.
        limit:          Cap the queue at this many records (useful for testing).

    Returns:
        Ordered list of :class:`MetabolonRecord`, one per unique cleaned name,
        ready for :func:`ddharmon.map_entities`.

    Example::

        import pandas as pd
        from ddharmon.extras.metabolon import build_mapping_queue

        df = pd.read_excel("Metabolon_features.xlsx")
        queue = build_mapping_queue(df)
        print(f"{len(queue)} unique names to map")
    """
    # name → (hmdb_hint, [feature_ids], best_match_level)
    seen: dict[str, MetabolonRecord] = {}

    for _, row in df.iterrows():
        raw_name = row.get(name_col)
        name = clean_compound_name(raw_name)
        if not name:
            continue

        feature_id = str(row.get(feature_id_col, ""))
        match_level: str | None = row.get(match_level_col)
        if isinstance(match_level, float):
            match_level = None

        if name not in seen:
            hmdb_hint: str | None = None
            if hint_col in df.columns:
                hmdb_hint = extract_hmdb_id(row.get(hint_col))
            seen[name] = MetabolonRecord(
                name=name,
                hmdb_hint=hmdb_hint,
                feature_ids=[feature_id] if feature_id else [],
                match_level=match_level,
            )
        else:
            rec = seen[name]
            # Absorb additional feature_ids
            if feature_id:
                rec.feature_ids.append(feature_id)
            # Update to highest-priority match_level
            if match_level is not None:
                existing_priority = _MATCH_LEVEL_PRIORITY.get(rec.match_level or "", 0)
                new_priority = _MATCH_LEVEL_PRIORITY.get(match_level, 0)
                if new_priority > existing_priority:
                    rec.match_level = match_level
            # Fill in missing hint from later rows
            if rec.hmdb_hint is None and hint_col in df.columns:
                candidate = extract_hmdb_id(row.get(hint_col))
                if candidate:
                    rec.hmdb_hint = candidate

    records = list(seen.values())
    if limit is not None:
        records = records[:limit]
    return records
