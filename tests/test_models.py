"""Tests for ddharmon.models."""

from __future__ import annotations

from typing import Any

import pytest

from ddharmon.models import MappingResult, MappingSummary


class TestMappingResultFromApiResponse:
    def test_resolved_compound(self, sample_api_response: dict[str, Any]) -> None:
        result = MappingResult.from_api_response(sample_api_response, "L-Histidine")

        assert result.resolved is True
        assert result.primary_curie == "RM:0129894"
        assert result.chosen_kg_id == "CHEBI:15971"
        assert result.confidence_score == pytest.approx(2.4898567923359374)
        assert result.confidence_tier == "high"
        assert result.error is None

    def test_identifier_extraction(self, sample_api_response: dict[str, Any]) -> None:
        result = MappingResult.from_api_response(sample_api_response, "L-Histidine")

        assert result.ids_for("CHEBI") == ["15971"]
        assert result.ids_for("refmet_id") == ["RM0129894"]
        assert result.ids_for("HMDB") == []

    def test_hmdb_hint_preserved(self, sample_api_response: dict[str, Any]) -> None:
        result = MappingResult.from_api_response(
            sample_api_response, "L-Histidine", hmdb_hint="HMDB00177"
        )
        assert result.hmdb_hint == "HMDB00177"

    def test_unresolved_compound(self, sample_unresolved_response: dict[str, Any]) -> None:
        result = MappingResult.from_api_response(sample_unresolved_response, "Z1005800534")

        assert result.resolved is False
        assert result.primary_curie is None
        assert result.confidence_score is None
        assert result.confidence_tier == "unknown"
        assert result.error is None

    def test_network_error_response(self) -> None:
        result = MappingResult.from_api_response(
            {"error": "Connection timeout"}, "bad-compound"
        )
        assert result.resolved is False
        assert result.error == "Connection timeout"

    def test_empty_result_block(self) -> None:
        result = MappingResult.from_api_response({"result": None, "metadata": {}}, "x")
        assert result.resolved is False
        assert result.error is not None

    def test_confidence_tiers(self) -> None:
        def make(score: float | None) -> MappingResult:
            return MappingResult(query_name="x", confidence_score=score)

        assert make(2.5).confidence_tier == "high"
        assert make(2.0).confidence_tier == "high"
        assert make(1.5).confidence_tier == "medium"
        assert make(1.0).confidence_tier == "medium"
        assert make(0.9).confidence_tier == "low"
        assert make(None).confidence_tier == "unknown"


class TestMappingSummary:
    def test_from_results(self, sample_api_response: dict[str, Any]) -> None:
        resolved = MappingResult.from_api_response(sample_api_response, "L-Histidine")
        unresolved = MappingResult(query_name="Z123", error="not found")

        summary = MappingSummary.from_results([resolved, unresolved])

        assert summary.total_queried == 2
        assert summary.resolved == 1
        assert summary.unresolved == 1
        assert summary.errors == 1
        assert summary.resolution_rate == pytest.approx(0.5)

    def test_empty_results(self) -> None:
        summary = MappingSummary.from_results([])
        assert summary.total_queried == 0
        assert summary.resolution_rate == pytest.approx(0.0)

    def test_vocabulary_coverage(self, sample_api_response: dict[str, Any]) -> None:
        result = MappingResult.from_api_response(sample_api_response, "L-Histidine")
        summary = MappingSummary.from_results([result])

        assert summary.vocabulary_coverage["CHEBI"] == 1
        assert summary.vocabulary_coverage["refmet_id"] == 1
