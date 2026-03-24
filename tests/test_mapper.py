"""Tests for ddharmon.mapper (sync wrappers)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddharmon.mapper import map_entities, map_entity, summarize
from ddharmon.models import MappingResult, MappingSummary


def _make_result(name: str, resolved: bool = True) -> MappingResult:
    return MappingResult(
        query_name=name,
        resolved=resolved,
        primary_curie="CHEBI:1" if resolved else None,
    )


class TestMapEntity:
    def test_single_result_returned(self) -> None:
        expected = _make_result("L-Histidine")
        with patch("ddharmon.mapper.map_entities", return_value=[expected]) as mock:
            result = map_entity("L-Histidine")
        assert result is expected
        mock.assert_called_once()

    def test_passes_identifiers(self) -> None:
        expected = _make_result("L-Histidine")
        with patch("ddharmon.mapper.map_entities", return_value=[expected]):
            result = map_entity("L-Histidine", identifiers={"HMDB": "HMDB00177"})
        assert result is expected


class TestMapEntities:
    def test_calls_client_map_entities(self, sample_api_response: dict[str, Any]) -> None:
        resolved = MappingResult.from_api_response(sample_api_response, "L-Histidine")

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.map_entities = AsyncMock(return_value=[resolved])

        with patch("ddharmon.mapper.BioMapperClient", return_value=mock_client):
            results = map_entities([{"name": "L-Histidine"}], rate_limit_delay=0.0)

        assert len(results) == 1
        assert results[0].resolved is True


class TestSummarize:
    def test_returns_summary(self) -> None:
        results = [_make_result("a"), _make_result("b", resolved=False)]
        summary = summarize(results)
        assert isinstance(summary, MappingSummary)
        assert summary.total_queried == 2
        assert summary.resolved == 1
        assert summary.resolution_rate == pytest.approx(0.5)
