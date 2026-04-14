"""Tests for ddharmon.mapper (sync wrappers)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddharmon.mapper import (
    list_annotators,
    list_entity_types,
    list_vocabularies,
    map_entities,
    map_entity,
    summarize,
)
from ddharmon.models import (
    AnnotatorInfo,
    EntityTypeInfo,
    MappingResult,
    MappingSummary,
    VocabularyInfo,
)


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


class TestListEntityTypes:
    def test_delegates_to_client(self) -> None:
        expected = [EntityTypeInfo(type="biolink:SmallMolecule", aliases=["metabolite"])]

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.list_entity_types = AsyncMock(return_value=expected)

        with patch("ddharmon.mapper.BioMapperClient", return_value=mock_client) as factory:
            result = list_entity_types(
                api_key="k", base_url="https://example.com", timeout=5.0
            )

        assert result == expected
        factory.assert_called_once_with(api_key="k", timeout=5.0, base_url="https://example.com")


class TestListAnnotators:
    def test_delegates_to_client(self) -> None:
        expected = [AnnotatorInfo(slug="kestrel", name="Kestrel")]

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.list_annotators = AsyncMock(return_value=expected)

        with patch("ddharmon.mapper.BioMapperClient", return_value=mock_client):
            result = list_annotators()

        assert result == expected


class TestListVocabularies:
    def test_delegates_to_client(self) -> None:
        expected = [VocabularyInfo(prefix="CHEBI", aliases=["chebi"])]

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.list_vocabularies = AsyncMock(return_value=expected)

        with patch("ddharmon.mapper.BioMapperClient", return_value=mock_client):
            result = list_vocabularies()

        assert result == expected


class TestMapEntitiesDeprecationSurfacesThroughSyncPath:
    def test_non_zero_rate_limit_delay_emits_warning(self) -> None:
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        async def fake_map_entities(**kwargs: Any) -> list[MappingResult]:
            # Emit the warning the way the real client does.
            import warnings

            if kwargs.get("rate_limit_delay", 0.0) > 0:
                warnings.warn(
                    "rate_limit_delay now applies between chunks",
                    DeprecationWarning,
                    stacklevel=2,
                )
            return [_make_result("x")]

        mock_client.map_entities = fake_map_entities

        with (
            patch("ddharmon.mapper.BioMapperClient", return_value=mock_client),
            pytest.warns(DeprecationWarning, match="rate_limit_delay"),
        ):
            map_entities([{"name": "x"}], rate_limit_delay=0.5)


class TestSummarize:
    def test_returns_summary(self) -> None:
        results = [_make_result("a"), _make_result("b", resolved=False)]
        summary = summarize(results)
        assert isinstance(summary, MappingSummary)
        assert summary.total_queried == 2
        assert summary.resolved == 1
        assert summary.resolution_rate == pytest.approx(0.5)
