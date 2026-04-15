"""Tests for ddharmon.models."""

from __future__ import annotations

from typing import Any

import pytest

from ddharmon.exceptions import BioMapperError
from ddharmon.models import (
    AnnotatorInfo,
    ApiMetadata,
    BatchMappingResponse,
    DatasetMappingResult,
    EntityTypeInfo,
    MappingResult,
    MappingSummary,
    RawApiResult,
    VocabularyInfo,
)


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


class TestDiscoveryModels:
    def test_entity_type_info_round_trip(self) -> None:
        info = EntityTypeInfo.model_validate(
            {"type": "biolink:SmallMolecule", "aliases": ["metabolite", "compound"]}
        )
        assert info.type == "biolink:SmallMolecule"
        assert info.aliases == ["metabolite", "compound"]

    def test_entity_type_info_defaults_empty_aliases(self) -> None:
        info = EntityTypeInfo.model_validate({"type": "biolink:Protein"})
        assert info.type == "biolink:Protein"
        assert info.aliases == []

    def test_annotator_info_optional_description(self) -> None:
        info = AnnotatorInfo.model_validate({"slug": "x", "name": "X"})
        assert info.slug == "x"
        assert info.name == "X"
        assert info.description is None

    def test_annotator_info_with_description(self) -> None:
        info = AnnotatorInfo.model_validate(
            {"slug": "kestrel", "name": "Kestrel", "description": "Vector search"}
        )
        assert info.description == "Vector search"

    def test_vocabulary_info_minimal(self) -> None:
        info = VocabularyInfo.model_validate({"prefix": "CHEBI"})
        assert info.prefix == "CHEBI"
        assert info.iri is None
        assert info.aliases == []

    def test_vocabulary_info_full(self) -> None:
        info = VocabularyInfo.model_validate(
            {
                "prefix": "CHEBI",
                "iri": "http://purl.obolibrary.org/obo/CHEBI_",
                "aliases": ["chebi"],
            }
        )
        assert info.iri == "http://purl.obolibrary.org/obo/CHEBI_"
        assert info.aliases == ["chebi"]


class TestBatchMappingResponse:
    def test_round_trips_flat_results(self) -> None:
        payload = {
            "results": [
                {
                    "name": "L-Histidine",
                    "curies": ["CHEBI:15971"],
                    "chosen_kg_id": "CHEBI:15971",
                    "kg_ids": {},
                    "assigned_ids": {},
                    "error": None,
                }
            ],
            "metadata": {"request_id": "abc", "processing_time_ms": 12.5},
            "summary": {"total": 1, "successful": 1, "failed": 0},
        }
        parsed = BatchMappingResponse.model_validate(payload)
        assert len(parsed.results) == 1
        assert isinstance(parsed.results[0], RawApiResult)
        assert parsed.results[0].name == "L-Histidine"
        assert parsed.results[0].curies == ["CHEBI:15971"]
        assert parsed.summary == {"total": 1, "successful": 1, "failed": 0}

    def test_empty_batch_response(self) -> None:
        parsed = BatchMappingResponse.model_validate(
            {"results": [], "metadata": {}, "summary": {}}
        )
        assert parsed.results == []
        assert parsed.summary == {}


class TestMappingResultFromBatchEntry:
    def test_resolved_entry(self) -> None:
        raw = RawApiResult(
            name="L-Histidine",
            curies=["RM:0129894", "CHEBI:15971"],
            chosen_kg_id="CHEBI:15971",
            assigned_ids={
                "kestrel-hybrid-search": {
                    "CHEBI": {"15971": {"score": 2.4898567923359374}}
                },
            },
        )
        result = MappingResult.from_batch_entry(raw, query_name="L-Histidine")

        assert result.resolved is True
        assert result.primary_curie == "RM:0129894"
        assert result.chosen_kg_id == "CHEBI:15971"
        assert result.confidence_score == pytest.approx(2.4898567923359374)
        assert result.ids_for("CHEBI") == ["15971"]
        assert result.raw_response is None
        assert result.error is None

    def test_error_entry(self) -> None:
        raw = RawApiResult(name="X", error="resolver failure")
        result = MappingResult.from_batch_entry(raw, query_name="X")

        assert result.resolved is False
        assert result.error == "resolver failure"
        assert result.query_name == "X"

    def test_unresolved_entry(self) -> None:
        raw = RawApiResult(name="X", curies=[], assigned_ids={})
        result = MappingResult.from_batch_entry(raw, query_name="X")

        assert result.resolved is False
        assert result.error is None
        assert result.primary_curie is None
        assert result.confidence_score is None

    def test_preserves_hmdb_hint(self) -> None:
        raw = RawApiResult(name="L-Histidine", curies=["CHEBI:15971"])
        result = MappingResult.from_batch_entry(
            raw, query_name="L-Histidine", hmdb_hint="HMDB00177"
        )
        assert result.hmdb_hint == "HMDB00177"


class TestDatasetMappingResult:
    def test_defaults(self) -> None:
        result = DatasetMappingResult()
        assert result.results == []
        assert result.stats == {}
        assert result.metadata.request_id == ""
        assert result.metadata.processing_time_ms == 0.0
        assert result.error is None

    def test_round_trip(self) -> None:
        result = DatasetMappingResult(
            results=[
                MappingResult(
                    query_name="x",
                    resolved=True,
                    primary_curie="CHEBI:1",
                )
            ],
            stats={"total": 1},
            metadata=ApiMetadata(request_id="r1", processing_time_ms=5.0),
        )
        dumped = result.model_dump()
        restored = DatasetMappingResult.model_validate(dumped)
        assert restored.results[0].query_name == "x"
        assert restored.stats == {"total": 1}
        assert restored.metadata.request_id == "r1"
        assert restored.error is None

    def test_partial_failure_shape(self) -> None:
        result = DatasetMappingResult(error="stream broke")
        assert result.error == "stream broke"
        assert result.results == []

    def test_raise_for_error_no_error_returns_none(self) -> None:
        # Clean run: no exception, no return value.
        assert DatasetMappingResult().raise_for_error() is None

    def test_raise_for_error_raises_with_message(self) -> None:
        result = DatasetMappingResult(error="stream broke")
        with pytest.raises(BioMapperError) as exc_info:
            result.raise_for_error()
        assert str(exc_info.value) == "stream broke"

    def test_raise_for_error_with_partial_results(self) -> None:
        # Partial results must not suppress the error; they also remain
        # accessible on the instance after the raise.
        partial = DatasetMappingResult(
            results=[MappingResult(query_name="x", resolved=True)],
            error="timeout",
        )
        with pytest.raises(BioMapperError):
            partial.raise_for_error()
        assert len(partial.results) == 1
