"""Synchronous file-based mapping over ``POST /map/dataset/stream``.

This module exposes a single public function, :func:`map_dataset_file_sync`,
that wraps :meth:`ddharmon.BioMapperClient.map_dataset_file_iter` with an
``asyncio.run`` bridge, tqdm progress wiring, and an optional per-result
callback. Async callers skip this module and invoke the client method
directly.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from ddharmon.client import BioMapperClient
from ddharmon.models import DatasetMappingResult, MappingResult


def map_dataset_file_sync(
    path: Path,
    *,
    name_column: str,
    provided_id_columns: list[str],
    entity_type: str = "biolink:SmallMolecule",
    annotation_mode: str = "missing",
    annotators: list[str] | None = None,
    vocab: str | None = None,
    progress: bool = False,
    on_result: Callable[[MappingResult], None] | None = None,
    total_hint: int | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: float | httpx.Timeout = 30.0,
) -> DatasetMappingResult:
    """Map a TSV/CSV dataset through ``POST /map/dataset/stream`` synchronously.

    Streams results internally via
    :meth:`ddharmon.BioMapperClient.map_dataset_file_iter`, accumulates them
    into a :class:`~ddharmon.models.DatasetMappingResult`, and returns once
    the stream completes. Blocks the calling thread.

    Args:
        path:                Path to a TSV or CSV file.
        name_column:         Column name containing entity names (required).
        provided_id_columns: Columns carrying pre-existing identifiers, e.g.
                             ``["hmdb_id"]`` (required). Values must not
                             contain commas.
        entity_type:         Biolink entity type applied to every row.
        annotation_mode:     ``"missing"`` | ``"all"`` | ``"none"``.
        annotators:          Optional list of annotator names.
        vocab:               Optional vocabulary hint forwarded to the API.
        progress:            Show a tqdm progress bar (requires
                             ``ddharmon[notebook]``).
        on_result:           Optional callback invoked per result with the
                             corresponding :class:`MappingResult` after it is
                             appended and after the tqdm tick (if enabled).
                             See "on_result semantics" below.
        total_hint:          Optional row count for the progress bar. When
                             ``progress=True`` and ``total_hint`` is provided,
                             tqdm shows a percentage bar. When ``total_hint``
                             is ``None``, tqdm shows an indefinite spinner —
                             the client has not parsed the file so it does
                             not know the row count. Notebook authors and
                             UI callers that have already parsed the file
                             should pass the row count here.
        api_key:             API key (falls back to ``BIOMAPPER_API_KEY``
                             env var).
        base_url:            Override the default API base URL.
        timeout:             Per-phase request timeout. Accepts a float
                             (applied to every phase) or an
                             :class:`httpx.Timeout` instance for granular
                             control. See "Long-running datasets" below.

    Returns:
        :class:`DatasetMappingResult` with ``results`` in server-emitted
        order. On mid-stream transport failure, ``results`` holds every row
        received before the failure and ``error`` carries the exception text;
        call :meth:`DatasetMappingResult.raise_for_error` to convert back to
        an exception if you prefer that shape.

    Raises:
        BioMapperAuthError, BioMapperRateLimitError, BioMapperServerError,
        BioMapperTimeoutError, httpx.HTTPStatusError: On initial-request
            errors (before the first row is yielded). These propagate
            unwrapped — partial results are not captured into ``.error``.
        asyncio.CancelledError: Propagates unwrapped.
        Exception: Whatever ``on_result`` raises propagates unwrapped and
            replaces the return value. See "on_result semantics" below.

    On ``on_result`` semantics:
        The callback runs synchronously on the calling thread inside the
        ``asyncio.run`` event-loop driver. Callbacks must not:

        1. Call ``asyncio.run(...)`` or otherwise re-enter ddharmon async
           APIs — this raises ``RuntimeError: asyncio.run() cannot be called
           from a running event loop``. A common notebook footgun, not a
           deadlock but a crash.
        2. Block on other I/O.
        3. Perform heavyweight work (file writes, UI widget updates,
           matplotlib renders) that stalls the stream. ``tqdm.update`` is
           fine; heavier per-result processing belongs in the async
           iterator path.

        Exceptions raised inside ``on_result`` abort the stream and propagate
        through ``asyncio.run`` to the caller, replacing the return value.
        Partial results collected up to that point are **not** returned.
        This is a deliberate asymmetry with stream-transport errors (which
        *are* captured into ``.error`` and returned).

    Long-running datasets:
        The default ``timeout=30.0`` applies per-phase; the read phase trips
        if the server pauses more than 30 s between NDJSON lines. For
        datasets with multi-minute server-side annotator lookups, pass an
        explicit :class:`httpx.Timeout` with a ``None`` default (disables
        read/write) and an explicit ``connect``::

            result = map_dataset_file_sync(
                path,
                name_column="name",
                provided_id_columns=["hmdb_id"],
                timeout=httpx.Timeout(None, connect=30.0),
            )

    Checking for partial success:
        When a mid-stream error truncates the run, ``.error`` is set and
        ``.results`` holds what arrived before the failure. Two common
        patterns::

            # Exception semantics (notebooks, scripts):
            result = map_dataset_file_sync(path, ...)
            result.raise_for_error()
            summary = summarize(result.results)

            # Partial-results semantics (UIs, resumable workflows):
            result = map_dataset_file_sync(path, ...)
            if result.error:
                log_and_render_partial(result.results, result.error)
            else:
                render_complete(result.results)

        Not calling :meth:`~DatasetMappingResult.raise_for_error` and not
        checking ``.error`` is a silent-data-loss footgun — the tutorial
        always shows the check.
    """
    client_kwargs: dict[str, Any] = {"timeout": timeout}
    if base_url is not None:
        client_kwargs["base_url"] = base_url

    async def _run() -> DatasetMappingResult:
        results: list[MappingResult] = []
        error: str | None = None
        streaming_started = False

        pbar: Any = None
        if progress:
            try:
                from tqdm.auto import tqdm

                pbar = tqdm(total=total_hint, desc="Mapping dataset")
            except ImportError:
                pass  # silently degrade if tqdm not installed

        try:
            # `contextlib.aclosing` on the inner generator ensures its nested
            # file/stream contexts unwind promptly on break or user-callback
            # exception rather than waiting for GC.
            async with (
                BioMapperClient(api_key=api_key, **client_kwargs) as c,
                contextlib.aclosing(
                    c.map_dataset_file_iter(
                        path,
                        name_column=name_column,
                        provided_id_columns=provided_id_columns,
                        entity_type=entity_type,
                        annotation_mode=annotation_mode,
                        annotators=annotators,
                        vocab=vocab,
                    )
                ) as gen,
            ):
            # Manual driving of the iterator lets us wrap ONLY `__anext__`
            # in the try/except, so on_result exceptions (raised later,
            # outside the try) cannot be accidentally captured into
            # `.error`. See module docstring + plan.
            while True:
                # Only wrap generator consumption; do NOT capture callback errors
                try:
                    r = await gen.__anext__()
                except StopAsyncIteration:
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 — transport-layer bubble
                    if not streaming_started:
                        # Initial-request error — propagate unwrapped.
                        raise
                    error = str(exc)
                    break
                # Success branch: record the result, tick progress,
                # then invoke the user callback. Any exception the
                # callback raises propagates out of the while loop
                # (and out of _run) to the caller — NOT captured.
                results.append(r)
                streaming_started = True
                if pbar is not None:
                    pbar.update(1)
                if on_result is not None:
                    on_result(r)
        finally:
            if pbar is not None:
                pbar.close()

        return DatasetMappingResult(results=results, error=error)

    return asyncio.run(_run())
