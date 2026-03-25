"""Synchronous-friendly mapping helpers for scripts and notebooks.

These functions wrap :class:`~ddharmon.client.BioMapperClient` so callers that
don't want to manage an event loop themselves can still use ddharmon.

For notebooks, use ``nest_asyncio`` (installed via ``ddharmon[notebook]``)::

    import nest_asyncio
    nest_asyncio.apply()

    from ddharmon import map_entities
    results = map_entities([{"name": "L-Histidine"}])
"""

from __future__ import annotations

import asyncio
from typing import Any

from ddharmon.client import BioMapperClient
from ddharmon.models import MappingResult, MappingSummary


def map_entities(
    records: list[dict[str, Any]],
    *,
    api_key: str | None = None,
    base_url: str | None = None,
    rate_limit_delay: float = 0.3,
    entity_type: str = "biolink:SmallMolecule",
    annotation_mode: str = "missing",
    annotators: list[str] | None = None,
    progress: bool = False,
    timeout: float = 30.0,
) -> list[MappingResult]:
    """Map a list of entity records synchronously.

    This is the highest-level entry point for non-async callers (scripts, Jupyter
    cells after ``nest_asyncio.apply()``).

    Args:
        records:           ``[{"name": str, "identifiers"?: dict}, ...]``
        api_key:           API key (falls back to ``BIOMAPPER_API_KEY`` env var).
        base_url:          Override the default API base URL.
        rate_limit_delay:  Seconds between successive API calls.
        entity_type:       Biolink entity type applied to all records.
        annotation_mode:   ``"missing"`` | ``"all"`` | ``"none"``.
        annotators:        Optional list of annotator names to use. When not
                           specified, BioMapper2 uses all available annotators.
                           Use ``["kestrel-vector-search"]`` for strict matching
                           that returns truly unresolved results for vendor codes.
        progress:          Show tqdm progress bar (requires ``ddharmon[notebook]``).
        timeout:           Per-request timeout in seconds.

    Returns:
        Ordered list of :class:`~ddharmon.models.MappingResult`.
    """
    client_kwargs: dict[str, Any] = {"timeout": timeout}
    if base_url is not None:
        client_kwargs["base_url"] = base_url

    async def _run() -> list[MappingResult]:
        async with BioMapperClient(api_key=api_key, **client_kwargs) as client:
            return await client.map_entities(
                records=records,
                rate_limit_delay=rate_limit_delay,
                entity_type=entity_type,
                annotation_mode=annotation_mode,
                annotators=annotators,
                progress=progress,
            )

    return asyncio.run(_run())


def map_entity(
    name: str,
    *,
    identifiers: dict[str, str] | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    entity_type: str = "biolink:SmallMolecule",
    annotation_mode: str = "missing",
    annotators: list[str] | None = None,
    timeout: float = 30.0,
) -> MappingResult:
    """Map a single entity name synchronously.

    Convenience wrapper for one-off lookups without managing async context.

    Args:
        name:            Compound name to map.
        identifiers:     Optional resolver hints, e.g. ``{"HMDB": "HMDB00177"}``.
        api_key:         API key (falls back to ``BIOMAPPER_API_KEY`` env var).
        base_url:        Override the default API base URL.
        entity_type:     Biolink entity type.
        annotation_mode: Annotation mode.
        annotators:      Optional list of annotator names to use.
        timeout:         Per-request timeout in seconds.

    Returns:
        :class:`~ddharmon.models.MappingResult`
    """
    results = map_entities(
        [{"name": name, "identifiers": identifiers or {}}],
        api_key=api_key,
        base_url=base_url,
        entity_type=entity_type,
        annotation_mode=annotation_mode,
        annotators=annotators,
        timeout=timeout,
    )
    return results[0]


def summarize(results: list[MappingResult]) -> MappingSummary:
    """Compute aggregate statistics from a list of mapping results.

    Args:
        results: Output of :func:`map_entities` or the async equivalents.

    Returns:
        :class:`~ddharmon.models.MappingSummary` with resolution rates and
        vocabulary coverage counts.
    """
    return MappingSummary.from_results(results)
