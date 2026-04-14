"""Async HTTP client for the BioMapper2 API."""

from __future__ import annotations

import asyncio
import contextlib
import os
import warnings
from collections import defaultdict
from collections.abc import AsyncIterator, Iterable
from pathlib import Path
from typing import Any

import httpx
from pydantic import ValidationError

from ddharmon.exceptions import (
    BioMapperAuthError,
    BioMapperConfigError,
    BioMapperRateLimitError,
    BioMapperServerError,
    BioMapperTimeoutError,
)
from ddharmon.models import (
    AnnotatorInfo,
    BatchMappingResponse,
    EntityTypeInfo,
    MapEntityRequest,
    MappingResult,
    RawApiResult,
    VocabularyInfo,
)

DEFAULT_BASE_URL = "https://biomapper.expertintheloop.io/api/v1"
DEFAULT_TIMEOUT = 30.0
DEFAULT_RATE_LIMIT_DELAY = 0.0  # seconds between chunks; deprecated in 0.3.0
DEFAULT_MAX_BATCH_SIZE = 1000  # /map/batch API limit (OpenAPI maxItems)


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

    async def list_entity_types(self) -> list[EntityTypeInfo]:
        """Return the Biolink entity types supported by the API.

        The server returns a flat ``{alias: type}`` lookup; this method
        inverts it into per-type :class:`EntityTypeInfo` objects with
        sorted alias lists — the shape users actually want when enumerating.

        Returns:
            List of :class:`EntityTypeInfo` in the order the server returned
            ``entity_types``; each ``aliases`` list is sorted alphabetically.

        Raises:
            BioMapperAuthError: If the key is rejected.
            BioMapperServerError: For unrecoverable 5xx errors.
            BioMapperTimeoutError: If the request times out.
        """
        try:
            response = await self._http.get(f"{self._base_url}/entity-types")
        except httpx.TimeoutException as exc:
            raise BioMapperTimeoutError("list_entity_types timed out") from exc
        self._raise_for_status(response)
        payload = response.json()

        inverted: dict[str, list[str]] = defaultdict(list)
        for alias, type_name in payload.get("aliases", {}).items():
            inverted[type_name].append(alias)

        return [
            EntityTypeInfo(type=t, aliases=sorted(inverted.get(t, [])))
            for t in payload.get("entity_types", [])
        ]

    async def list_annotators(self) -> list[AnnotatorInfo]:
        """Return the annotators available to the mapping pipeline.

        Raises:
            BioMapperAuthError: If the key is rejected.
            BioMapperServerError: For unrecoverable 5xx errors.
            BioMapperTimeoutError: If the request times out.
        """
        try:
            response = await self._http.get(f"{self._base_url}/annotators")
        except httpx.TimeoutException as exc:
            raise BioMapperTimeoutError("list_annotators timed out") from exc
        self._raise_for_status(response)
        payload = response.json()
        return [AnnotatorInfo.model_validate(a) for a in payload.get("annotators", [])]

    async def list_vocabularies(self) -> list[VocabularyInfo]:
        """Return the identifier vocabularies supported by the API.

        Raises:
            BioMapperAuthError: If the key is rejected.
            BioMapperServerError: For unrecoverable 5xx errors.
            BioMapperTimeoutError: If the request times out.
        """
        try:
            response = await self._http.get(f"{self._base_url}/vocabularies")
        except httpx.TimeoutException as exc:
            raise BioMapperTimeoutError("list_vocabularies timed out") from exc
        self._raise_for_status(response)
        payload = response.json()
        return [VocabularyInfo.model_validate(v) for v in payload.get("vocabularies", [])]

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
        records: Iterable[dict[str, Any]],
        rate_limit_delay: float = DEFAULT_RATE_LIMIT_DELAY,
        entity_type: str = "biolink:SmallMolecule",
        annotation_mode: str = "missing",
        annotators: list[str] | None = None,
        progress: bool = False,
    ) -> list[MappingResult]:
        """Map a batch of entity records via the native ``/map/batch`` endpoint.

        Each record is a dict with at least a ``"name"`` key, and optionally
        ``"identifiers"`` (``{"HMDB": "HMDB00177"}``). Inputs are auto-chunked
        at ``DEFAULT_MAX_BATCH_SIZE`` entities per request (API limit).

        Args:
            records:           Iterable of ``{"name": str, "identifiers": dict}`` dicts.
                               Materialized into a list at entry so generators are
                               accepted.
            rate_limit_delay:  Seconds to sleep **between chunks** (not between
                               individual records). Default ``0.0``. Passing a
                               non-zero value emits a ``DeprecationWarning``;
                               the parameter will be removed in ddharmon 1.0.0.
            entity_type:       Biolink entity type applied to every record.
            annotation_mode:   Annotation mode applied to every record.
            annotators:        Optional list of annotator names to use.
            progress:          Show a tqdm progress bar (requires ``ddharmon[notebook]``).
                               The bar totals ``len(records)`` and advances by the
                               chunk size after each chunk completes.

        Returns:
            List of :class:`~ddharmon.models.MappingResult`, one per input record,
            in input order. Records that fail (either per-record errors in a
            successful response or every record in a chunk-level HTTP failure)
            return a result with ``error`` set rather than raising.

        Raises:
            asyncio.CancelledError: Propagated immediately so callers can cancel
                mid-batch. All other exceptions are caught and surfaced as
                per-record errors.

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
        records = list(records)  # materialize so generators work and len() is safe

        if rate_limit_delay > 0:
            # stacklevel=3 attributes the warning to user code when called through
            # the sync wrapper (mapper.map_entities → asyncio.run(_run) → here).
            # For direct async callers this points one frame past their call site,
            # which is still a useful approximate location.
            warnings.warn(
                "rate_limit_delay now applies between chunks rather than between "
                "records; this parameter will be removed in ddharmon 1.0.0",
                DeprecationWarning,
                stacklevel=3,
            )

        requests = [
            MapEntityRequest(
                name=str(r.get("name", "")),
                entity_type=entity_type,
                identifiers=dict(r.get("identifiers") or {}),
                options=self._build_options(annotation_mode, annotators),
            )
            for r in records
        ]

        chunks = [
            requests[i : i + DEFAULT_MAX_BATCH_SIZE]
            for i in range(0, len(requests), DEFAULT_MAX_BATCH_SIZE)
        ]

        pbar: Any = None
        if progress:
            try:
                from tqdm.auto import tqdm

                pbar = tqdm(total=len(records), desc="Mapping entities")
            except ImportError:
                pass  # silently degrade if tqdm not installed

        results: list[MappingResult] = []

        try:
            for idx, chunk in enumerate(chunks):
                if idx > 0 and rate_limit_delay > 0:
                    await asyncio.sleep(rate_limit_delay)

                try:
                    response = await self._http.post(
                        f"{self._base_url}/map/batch",
                        json={
                            "entities": [
                                r.model_dump(exclude_none=False) for r in chunk
                            ]
                        },
                    )
                    self._raise_for_status(response)
                    parsed = BatchMappingResponse.model_validate(response.json())
                    # strict=True: a length mismatch between sent entities and
                    # returned results raises ValueError, which the chunk-level
                    # except below converts into per-record errors — preserving the
                    # invariant that len(results) == len(records) end-to-end.
                    for req, raw in zip(chunk, parsed.results, strict=True):
                        if raw.name and raw.name != req.name:
                            warnings.warn(
                                f"Batch order mismatch: sent {req.name!r}, "
                                f"got {raw.name!r}",
                                RuntimeWarning,
                                stacklevel=2,
                            )
                        results.append(
                            MappingResult.from_batch_entry(
                                raw,
                                query_name=req.name,
                                hmdb_hint=req.identifiers.get("HMDB"),
                            )
                        )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 — broad catch preserves "one bad chunk doesn't abort the batch"
                    for req in chunk:
                        results.append(
                            MappingResult(
                                query_name=req.name,
                                hmdb_hint=req.identifiers.get("HMDB"),
                                error=str(exc),
                            )
                        )
                    if isinstance(exc, BioMapperRateLimitError) and exc.retry_after:
                        # Courtesy: honor server's Retry-After between chunks.
                        await asyncio.sleep(max(exc.retry_after, rate_limit_delay))
                finally:
                    if pbar is not None:
                        pbar.update(len(chunk))
        finally:
            # Outer finally ensures pbar.close() runs even if CancelledError
            # bubbles out of the loop (Jupyter widget cleanup).
            if pbar is not None:
                pbar.close()

        return results

    @staticmethod
    def _build_options(
        annotation_mode: str, annotators: list[str] | None
    ) -> dict[str, Any]:
        options: dict[str, Any] = {"annotation_mode": annotation_mode}
        if annotators is not None:
            options["annotators"] = annotators
        return options

    async def map_dataset_file_iter(
        self,
        path: Path,
        *,
        name_column: str,
        provided_id_columns: list[str],
        entity_type: str = "biolink:SmallMolecule",
        annotation_mode: str = "missing",
        annotators: list[str] | None = None,
        vocab: str | None = None,
    ) -> AsyncIterator[MappingResult]:
        """Stream per-row mapping results from ``POST /map/dataset/stream``.

        Uploads ``path`` as a multipart body and yields :class:`MappingResult`
        per NDJSON line as the server emits them. The iterator is the
        streaming primitive used by :func:`ddharmon.map_dataset_file_sync`
        and is also available to any async caller (e.g. the Entity Linker UI).

        Args:
            path:                Path to a TSV or CSV file.
            name_column:         Column name containing entity names (required).
            provided_id_columns: Columns carrying pre-existing identifiers, e.g.
                                 ``["hmdb_id"]`` (required). Values must not
                                 contain commas — see Raises below.
            entity_type:         Biolink entity type applied to every row.
            annotation_mode:     ``"missing"`` | ``"all"`` | ``"none"``.
            annotators:          Optional list of annotator names. ``None``
                                 uses all available annotators. Values must
                                 not contain commas.
            vocab:               Optional vocabulary hint forwarded to the API.

        Yields:
            :class:`~ddharmon.models.MappingResult` per NDJSON line. The
            ``hmdb_hint`` attribute is always ``None`` on yielded results — the
            server processes the dataset opaquely and the client has no
            per-row hint to echo back. The ``confidence_score`` attribute is
            also ``None``: the dataset-stream endpoint emits a slimmer
            per-row payload than ``/map/batch`` and omits the ``assigned_ids``
            block where per-annotator scores live. Use :func:`map_entity` /
            :func:`map_entities` if you need confidence scores.

        Raises:
            ValueError: If a value in ``provided_id_columns`` or ``annotators``
                contains a comma — these would silently split at the wire and
                corrupt the request.
            BioMapperAuthError: On initial-request 401/403.
            BioMapperRateLimitError: On initial-request 429.
            BioMapperServerError: On initial-request 5xx.
            BioMapperTimeoutError: On connect timeout (initial request only;
                mid-stream timeouts propagate as the raw ``httpx`` exception).
            httpx.HTTPStatusError: On initial-request 4xx other than 401/403/429
                (notably 422 validation errors from the server).
            httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.NetworkError:
                On mid-stream transport failures. Values yielded before the
                failure are delivered; the exception propagates when the
                caller awaits the next ``__anext__``.

        Stream truncation:
            If the connection closes without a trailing newline on the final
            line, ``aiter_lines()`` raises ``httpx.RemoteProtocolError`` after
            yielding all complete lines (treated as transport-level — propagates).
            If the server emits a syntactically-incomplete line followed by
            a newline, the line reaches the parser and is yielded as a
            per-row error :class:`MappingResult` (treated as per-record).

        Concurrency:
            Runs one upload + stream through the shared ``httpx.AsyncClient``.
            Running two ``map_dataset_file_iter`` calls concurrently on the
            same :class:`BioMapperClient` (e.g. via ``asyncio.gather``) may
            serialize or deadlock depending on the connection-pool
            configuration. For concurrent dataset jobs, construct a separate
            :class:`BioMapperClient` per job.

        Recommended timeout for long runs:
            The default ``timeout=30.0`` applies per-phase; the read phase
            times out if the server pauses more than 30 s between NDJSON
            lines. For datasets whose server-side annotator lookups exceed
            30 s, construct the client with
            ``BioMapperClient(timeout=httpx.Timeout(read=None, connect=30.0))``.
        """
        params = self._dataset_query_params(
            entity_type=entity_type,
            name_column=name_column,
            provided_id_columns=provided_id_columns,
            annotation_mode=annotation_mode,
            annotators=annotators,
            vocab=vocab,
        )
        content_type = self._dataset_content_type(path)

        # AsyncExitStack enforces LIFO cleanup: the HTTP stream context exits
        # first (any httpx finalization that reads from fh completes), and
        # only then does the file close. If cancellation fires during TLS
        # handshake or initial response-headers read, both resources unwind
        # correctly — no chance of closing fh while httpx still holds it.
        async with contextlib.AsyncExitStack() as stack:
            fh = stack.enter_context(path.open("rb"))
            try:
                response = await stack.enter_async_context(
                    self._http.stream(
                        "POST",
                        f"{self._base_url}/map/dataset/stream",
                        files={"file": (path.name, fh, content_type)},
                        params=params,
                    )
                )
            except httpx.TimeoutException as exc:
                raise BioMapperTimeoutError(
                    f"Dataset stream request timed out for {path.name!r}"
                ) from exc

            # `_raise_for_status` reads `response.text` on 5xx; that errors on a
            # still-streaming response. Eagerly consume the body for non-2xx
            # so the shared error mapper works uniformly for streaming and
            # non-streaming callers.
            if response.status_code >= 400:
                await response.aread()
            self._raise_for_status(response)

            async for line in response.aiter_lines():
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    raw = RawApiResult.model_validate_json(stripped)
                except ValidationError as exc:
                    yield MappingResult(
                        query_name="<unknown>",
                        error=f"Failed to parse NDJSON line: {exc}",
                    )
                    continue
                yield MappingResult.from_batch_entry(
                    raw, query_name=raw.name or "", hmdb_hint=None
                )

    @staticmethod
    def _dataset_query_params(
        *,
        entity_type: str,
        name_column: str,
        provided_id_columns: list[str],
        annotation_mode: str,
        annotators: list[str] | None,
        vocab: str | None,
    ) -> dict[str, str]:
        """Serialize dataset endpoint query params.

        ``provided_id_columns`` and ``annotators`` are joined with commas for
        the wire form. Commas inside any value are rejected as a ``ValueError``
        at the boundary — silently splitting ``"iupac,name"`` into two
        columns would corrupt the request undetectably.
        """
        BioMapperClient._reject_commas("provided_id_columns", provided_id_columns)
        if annotators is not None:
            BioMapperClient._reject_commas("annotators", annotators)

        params: dict[str, str] = {
            "entity_type": entity_type,
            "name_column": name_column,
            "provided_id_columns": ",".join(provided_id_columns),
            "annotation_mode": annotation_mode,
        }
        if annotators is not None:
            params["annotators"] = ",".join(annotators)
        if vocab is not None:
            params["vocab"] = vocab
        return params

    @staticmethod
    def _reject_commas(param_name: str, values: list[str]) -> None:
        for v in values:
            if "," in v:
                raise ValueError(
                    f"{param_name!r} values must not contain commas: {v!r}"
                )

    @staticmethod
    def _dataset_content_type(path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix == ".tsv":
            return "text/tab-separated-values"
        if suffix == ".csv":
            return "text/csv"
        return "application/octet-stream"
