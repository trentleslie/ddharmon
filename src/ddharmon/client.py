"""Async HTTP client for the BioMapper2 API."""

from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx

from ddharmon.exceptions import (
    BioMapperAuthError,
    BioMapperConfigError,
    BioMapperRateLimitError,
    BioMapperServerError,
    BioMapperTimeoutError,
)
from ddharmon.models import MapEntityRequest, MappingResult

DEFAULT_BASE_URL = "https://biomapper.expertintheloop.io/api/v1"
DEFAULT_TIMEOUT = 30.0
DEFAULT_RATE_LIMIT_DELAY = 0.3  # seconds between calls in batch mode


class BioMapperClient:
    """Async client for the BioMapper2 API.

    Handles authentication, request serialization, error mapping, and optional
    rate-limited batch processing.

    Usage (minimal)::

        async with BioMapperClient() as client:
            result = await client.map_entity("L-Histidine")
            print(result.primary_curie)   # "RM:0129894"

    Usage (with explicit key and hint)::

        async with BioMapperClient(api_key="sk-...") as client:
            result = await client.map_entity(
                name="4,6-DIOXOHEPTANOIC ACID",
                identifiers={"HMDB": "HMDB03349"},
            )

    Args:
        api_key:    BioMapper API key.  Defaults to ``BIOMAPPER_API_KEY`` env var.
        base_url:   API root URL.  Override for staging/local instances.
        timeout:    Per-request timeout in seconds.
        httpx_kwargs: Extra kwargs forwarded to :class:`httpx.AsyncClient`.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        **httpx_kwargs: Any,
    ) -> None:
        resolved_key = api_key or os.getenv("BIOMAPPER_API_KEY")
        if not resolved_key:
            raise BioMapperConfigError(
                "No API key provided. Pass api_key= or set BIOMAPPER_API_KEY env var."
            )
        self._api_key = resolved_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._httpx_kwargs = httpx_kwargs
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> BioMapperClient:
        self._client = httpx.AsyncClient(
            headers={"X-API-Key": self._api_key},
            timeout=self._timeout,
            **self._httpx_kwargs,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError(
                "BioMapperClient must be used as an async context manager. "
                "Use `async with BioMapperClient() as client:`"
            )
        return self._client

    def _raise_for_status(self, response: httpx.Response) -> None:
        """Map HTTP status codes to typed exceptions."""
        code = response.status_code
        if code == 401 or code == 403:
            raise BioMapperAuthError(
                f"Authentication failed (HTTP {code}). Check your API key."
            )
        if code == 429:
            retry_after: float | None = None
            if ra := response.headers.get("Retry-After"):
                try:
                    retry_after = float(ra)
                except ValueError:
                    pass
            raise BioMapperRateLimitError(
                "Rate limit exceeded (HTTP 429).", retry_after=retry_after
            )
        if code >= 500:
            raise BioMapperServerError(
                f"Server error (HTTP {code}): {response.text[:200]}",
                status_code=code,
            )
        response.raise_for_status()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def health_check(self) -> dict[str, Any]:
        """Verify connectivity and API readiness.

        Returns:
            The parsed health JSON, e.g.
            ``{"status": "healthy", "version": "0.1.0", "mapper_initialized": True}``.

        Raises:
            BioMapperAuthError: If the key is rejected.
            BioMapperServerError: If the service is not healthy.
        """
        try:
            response = await self._http.get(f"{self._base_url}/health")
        except httpx.TimeoutException as exc:
            raise BioMapperTimeoutError("Health check timed out") from exc
        self._raise_for_status(response)
        return dict(response.json())

    async def map_entity(
        self,
        name: str,
        entity_type: str = "biolink:SmallMolecule",
        identifiers: dict[str, str] | None = None,
        annotation_mode: str = "missing",
        annotators: list[str] | None = None,
    ) -> MappingResult:
        """Map a single entity name to standardized knowledge-graph identifiers.

        Args:
            name:            Compound or entity name to resolve.
            entity_type:     Biolink entity type.  Use ``"biolink:SmallMolecule"``
                             for metabolites.
            identifiers:     Optional pre-existing IDs used as resolver hints,
                             e.g. ``{"HMDB": "HMDB00177"}``.
            annotation_mode: ``"missing"`` (default), ``"all"``, or ``"none"``.
            annotators:      Optional list of annotator names to use. When not
                             specified, BioMapper2 uses all available annotators.
                             Use ``["kestrel-vector-search"]`` for strict matching.

        Returns:
            A :class:`~ddharmon.models.MappingResult` with resolved identifiers.

        Raises:
            BioMapperAuthError: If the API key is rejected.
            BioMapperRateLimitError: If the API signals throttling.
            BioMapperServerError: For unrecoverable 5xx errors.
            BioMapperTimeoutError: If the request times out.
        """
        options: dict[str, Any] = {"annotation_mode": annotation_mode}
        if annotators is not None:
            options["annotators"] = annotators

        payload = MapEntityRequest(
            name=name,
            entity_type=entity_type,
            identifiers=identifiers or {},
            options=options,
        )

        hmdb_hint: str | None = (identifiers or {}).get("HMDB")

        try:
            response = await self._http.post(
                f"{self._base_url}/map/entity",
                json=payload.model_dump(exclude_none=False),
            )
        except httpx.TimeoutException as exc:
            raise BioMapperTimeoutError(f"Request timed out for '{name}'") from exc

        self._raise_for_status(response)
        data = dict[str, Any](response.json())
        return MappingResult.from_api_response(data, query_name=name, hmdb_hint=hmdb_hint)

    async def map_entities(
        self,
        records: list[dict[str, Any]],
        rate_limit_delay: float = DEFAULT_RATE_LIMIT_DELAY,
        entity_type: str = "biolink:SmallMolecule",
        annotation_mode: str = "missing",
        annotators: list[str] | None = None,
        progress: bool = False,
    ) -> list[MappingResult]:
        """Map a batch of entity records with rate limiting.

        Each record is a dict with at least a ``"name"`` key, and optionally
        ``"identifiers"`` (``{"HMDB": "HMDB00177"}``).

        Args:
            records:           List of ``{"name": str, "identifiers": dict}`` dicts.
            rate_limit_delay:  Seconds to sleep between API calls.  Default 0.3.
            entity_type:       Biolink entity type for all records.
            annotation_mode:   Annotation mode for all records.
            annotators:        Optional list of annotator names to use.
            progress:          Show a tqdm progress bar (requires ``ddharmon[notebook]``).

        Returns:
            List of :class:`~ddharmon.models.MappingResult`, one per input record,
            in the same order.  Failed records return a result with ``error`` set
            rather than raising.

        Example::

            async with BioMapperClient() as client:
                results = await client.map_entities(
                    [
                        {"name": "L-Histidine"},
                        {"name": "Glucose", "identifiers": {"HMDB": "HMDB00122"}},
                    ],
                    progress=True,
                )
        """
        iter_records: Any = records

        if progress:
            try:
                from tqdm.auto import tqdm

                iter_records = tqdm(records, desc="Mapping entities")
            except ImportError:
                pass  # silently degrade if tqdm not installed

        results: list[MappingResult] = []

        for i, record in enumerate(iter_records):
            if i > 0:
                await asyncio.sleep(rate_limit_delay)

            name: str = str(record.get("name", ""))
            identifiers: dict[str, str] = dict(record.get("identifiers") or {})

            try:
                result = await self.map_entity(
                    name=name,
                    entity_type=entity_type,
                    identifiers=identifiers or None,
                    annotation_mode=annotation_mode,
                    annotators=annotators,
                )
            except Exception as exc:  # noqa: BLE001
                result = MappingResult(
                    query_name=name,
                    hmdb_hint=identifiers.get("HMDB"),
                    error=str(exc),
                )

            results.append(result)

        return results
