"""Shared pytest fixtures for ddharmon tests."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

SAMPLE_API_RESPONSE: dict[str, Any] = {
    "result": {
        "name": "L-Histidine",
        "curies": ["RM:0129894", "CHEBI:15971"],
        "chosen_kg_id": "CHEBI:15971",
        "kg_ids": {"CHEBI:15971": ["RM:0129894", "CHEBI:15971"]},
        "assigned_ids": {
            "metabolomics-workbench": {
                "refmet_id": {"RM0129894": {}}
            },
            "kestrel-hybrid-search": {
                "CHEBI": {"15971": {"score": 2.4898567923359374}}
            },
        },
        "error": None,
    },
    "metadata": {
        "request_id": "db11c3a5-f88d-4030-a91f-f41937050767",
        "processing_time_ms": 12.68,
    },
}

SAMPLE_UNRESOLVED_RESPONSE: dict[str, Any] = {
    "result": {
        "name": "Z1005800534",
        "curies": [],
        "chosen_kg_id": None,
        "kg_ids": {},
        "assigned_ids": {},
        "error": None,
    },
    "metadata": {
        "request_id": "aaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "processing_time_ms": 5.0,
    },
}

HEALTH_RESPONSE: dict[str, Any] = {
    "status": "healthy",
    "version": "0.1.0",
    "mapper_initialized": True,
}


# Per-entity shape for /map/batch responses (flat — no outer `result:` wrapper).
SAMPLE_BATCH_ENTRY_RESOLVED: dict[str, Any] = {
    "name": "L-Histidine",
    "curies": ["RM:0129894", "CHEBI:15971"],
    "chosen_kg_id": "CHEBI:15971",
    "kg_ids": {"CHEBI:15971": ["RM:0129894", "CHEBI:15971"]},
    "assigned_ids": {
        "metabolomics-workbench": {"refmet_id": {"RM0129894": {}}},
        "kestrel-hybrid-search": {
            "CHEBI": {"15971": {"score": 2.4898567923359374}}
        },
    },
    "error": None,
}


def make_batch_entry(
    name: str, *, resolved: bool = True, error: str | None = None
) -> dict[str, Any]:
    """Build a /map/batch entry dict for a given name."""
    if error is not None:
        return {
            "name": name,
            "curies": [],
            "chosen_kg_id": None,
            "kg_ids": {},
            "assigned_ids": {},
            "error": error,
        }
    if not resolved:
        return {
            "name": name,
            "curies": [],
            "chosen_kg_id": None,
            "kg_ids": {},
            "assigned_ids": {},
            "error": None,
        }
    # Resolved: use the sample's field shape but swap in the requested name.
    return {**SAMPLE_BATCH_ENTRY_RESOLVED, "name": name}


def make_batch_response(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """Wrap a list of /map/batch entries in the full response envelope."""
    return {
        "results": entries,
        "metadata": {"request_id": "test-req", "processing_time_ms": 10.0},
        "summary": {
            "total": len(entries),
            "successful": sum(1 for e in entries if not e.get("error") and e.get("curies")),
            "failed": sum(1 for e in entries if e.get("error")),
        },
    }


def make_ndjson_body(entries: list[dict[str, Any]]) -> bytes:
    """Serialize per-entity dicts as newline-delimited JSON bytes.

    Mirrors what `/map/dataset/stream` emits, one entry per line.
    """
    return b"\n".join(json.dumps(e).encode("utf-8") for e in entries) + b"\n"


class TruncatingByteStream(httpx.AsyncByteStream):
    """Async byte stream that yields given byte chunks, then raises.

    Useful for simulating mid-stream transport failures against respx.
    Each chunk is delivered independently so `aiter_lines()` sees real
    streaming boundaries rather than a single buffered blob.
    """

    def __init__(self, chunks: list[bytes], exc: Exception) -> None:
        self._chunks = chunks
        self._exc = exc

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk
        raise self._exc


def make_truncating_stream(
    good_entries: list[dict[str, Any]], exc: Exception
) -> TruncatingByteStream:
    """Build a TruncatingByteStream that yields `good_entries` then raises `exc`.

    Each good entry is a separate chunk so aiter_lines() sees it complete
    before the raise. Use for mid-stream-failure scenarios.
    """
    chunks = [json.dumps(e).encode("utf-8") + b"\n" for e in good_entries]
    return TruncatingByteStream(chunks, exc)


@pytest.fixture()
def sample_api_response() -> dict[str, Any]:
    return SAMPLE_API_RESPONSE


@pytest.fixture()
def sample_unresolved_response() -> dict[str, Any]:
    return SAMPLE_UNRESOLVED_RESPONSE


@pytest.fixture()
def health_response() -> dict[str, Any]:
    return HEALTH_RESPONSE
