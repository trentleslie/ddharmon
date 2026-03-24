"""Tests for ddharmon.extras.metabolon.export."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pytest

from ddharmon.extras.metabolon.export import (
    flatten_results,
    results_to_dataframe,
    save_results,
)
from ddharmon.models import MappingResult, MappingSummary


@pytest.fixture()
def resolved_result(sample_api_response: dict[str, Any]) -> MappingResult:
    return MappingResult.from_api_response(
        sample_api_response, "L-Histidine", hmdb_hint="HMDB00177"
    )


@pytest.fixture()
def unresolved_result() -> MappingResult:
    return MappingResult(query_name="Z1005800534", error="not found")


class TestFlattenResults:
    def test_resolved_fields(self, resolved_result: MappingResult) -> None:
        flat = flatten_results([resolved_result])
        assert len(flat) == 1
        row = flat[0]
        assert row["query_name"] == "L-Histidine"
        assert row["hmdb_hint"] == "HMDB00177"
        assert row["resolved"] is True
        assert row["primary_curie"] == "RM:0129894"
        assert row["chebi_ids"] == "15971"
        assert row["refmet_ids"] == "RM0129894"
        assert row["error"] == ""

    def test_unresolved_fields(self, unresolved_result: MappingResult) -> None:
        flat = flatten_results([unresolved_result])
        row = flat[0]
        assert row["resolved"] is False
        assert row["primary_curie"] == ""
        assert row["error"] == "not found"

    def test_empty_list(self) -> None:
        assert flatten_results([]) == []

    def test_no_confidence_score_becomes_empty_string(self) -> None:
        r = MappingResult(query_name="x", resolved=True, primary_curie="CHEBI:1")
        flat = flatten_results([r])
        assert flat[0]["confidence_score"] == ""


class TestResultsToDataframe:
    def test_returns_dataframe(self, resolved_result: MappingResult) -> None:
        pd = pytest.importorskip("pandas")
        df = results_to_dataframe([resolved_result])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert "query_name" in df.columns

    def test_missing_pandas_raises(
        self, resolved_result: MappingResult, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import builtins
        real_import = builtins.__import__

        def patched_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "pandas":
                raise ImportError("no pandas")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", patched_import)
        with pytest.raises(ImportError, match="ddharmon\\[metabolon\\]"):
            results_to_dataframe([resolved_result])


class TestSaveResults:
    def test_saves_json(
        self,
        tmp_path: Path,
        resolved_result: MappingResult,
    ) -> None:
        out = tmp_path / "out.json"
        summary = MappingSummary.from_results([resolved_result])
        save_results([resolved_result], summary=summary, json_path=out)

        assert out.exists()
        data = json.loads(out.read_text())
        assert data["summary"]["total_queried"] == 1
        assert len(data["mappings"]) == 1
        assert data["mappings"][0]["query_name"] == "L-Histidine"

    def test_saves_tsv(
        self,
        tmp_path: Path,
        resolved_result: MappingResult,
    ) -> None:
        pytest.importorskip("pandas")
        out = tmp_path / "out.tsv"
        save_results([resolved_result], tsv_path=out)

        assert out.exists()
        lines = out.read_text().splitlines()
        assert lines[0].startswith("query_name")
        assert "L-Histidine" in lines[1]

    def test_creates_parent_dirs(
        self,
        tmp_path: Path,
        resolved_result: MappingResult,
    ) -> None:
        nested = tmp_path / "deep" / "nested" / "out.json"
        save_results([resolved_result], json_path=nested)
        assert nested.exists()
