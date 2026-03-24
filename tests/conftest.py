"""Shared pytest fixtures for ddharmon tests."""

from __future__ import annotations

from typing import Any

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


@pytest.fixture()
def sample_api_response() -> dict[str, Any]:
    return SAMPLE_API_RESPONSE


@pytest.fixture()
def sample_unresolved_response() -> dict[str, Any]:
    return SAMPLE_UNRESOLVED_RESPONSE


@pytest.fixture()
def health_response() -> dict[str, Any]:
    return HEALTH_RESPONSE
