"""Tests for ddharmon.extras.metabolon preprocessing utilities."""

from __future__ import annotations

import math

import pytest

from ddharmon.extras.metabolon.preprocessing import (
    build_mapping_queue,
    clean_compound_name,
    extract_hmdb_id,
)


class TestCleanCompoundName:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ('"1,3-Diphenylguanidine_CE45"', "1,3-Diphenylguanidine"),
            ('"3-Amino-1,2,4-triazol"', "3-Amino-1,2,4-triazol"),
            ("L-Histidine", "L-Histidine"),
            ("  Glucose  ", "Glucose"),
            ('"4,6-DIOXOHEPTANOIC ACID"', "4,6-DIOXOHEPTANOIC ACID"),
            ("compound_CE205060", "compound"),
            ("compound_CE20_60", "compound"),
            ("", None),
            (None, None),
            (math.nan, None),
        ],
    )
    def test_clean_compound_name(self, raw: object, expected: str | None) -> None:
        assert clean_compound_name(raw) == expected

    def test_unchanged_name_no_suffix(self) -> None:
        assert clean_compound_name("L-Carnitine") == "L-Carnitine"


class TestExtractHmdbId:
    @pytest.mark.parametrize(
        "ms1_name, expected",
        [
            ("HMDB:HMDB03349-2257 L-Dihydroorotic acid", "HMDB03349"),
            ("HMDB:HMDB00635-869 Succinylacetone", "HMDB00635"),
            ("HMDB00177", "HMDB00177"),
            ("no hmdb here", None),
            (math.nan, None),
            (None, None),
        ],
    )
    def test_extract_hmdb_id(self, ms1_name: object, expected: str | None) -> None:
        assert extract_hmdb_id(ms1_name) == expected


class TestBuildMappingQueue:
    @pytest.fixture()
    def sample_df(self) -> "object":
        pytest.importorskip("pandas")
        import pandas as pd

        return pd.DataFrame(
            {
                "feature_id": ["f1", "f2", "f3", "f4"],
                "matched_name": [
                    '"L-Histidine_CE45"',
                    "L-Histidine",  # duplicate
                    '"Glucose"',
                    None,  # no name — should be skipped
                ],
                "ms1_compound_name": [
                    "HMDB:HMDB00177-284 L-Histidine",
                    None,
                    None,
                    None,
                ],
                "match_level": ["MS2", "MS1", "CURATION", "MS2"],
            }
        )

    def test_deduplicates_by_cleaned_name(self, sample_df: "object") -> None:
        queue = build_mapping_queue(sample_df)
        names = [r.name for r in queue]
        assert len(names) == 2  # L-Histidine + Glucose, None skipped
        assert "L-Histidine" in names
        assert "Glucose" in names

    def test_preserves_hmdb_hint(self, sample_df: "object") -> None:
        queue = build_mapping_queue(sample_df)
        histidine = next(r for r in queue if r.name == "L-Histidine")
        assert histidine.hmdb_hint == "HMDB00177"

    def test_collects_all_feature_ids(self, sample_df: "object") -> None:
        queue = build_mapping_queue(sample_df)
        histidine = next(r for r in queue if r.name == "L-Histidine")
        assert set(histidine.feature_ids) == {"f1", "f2"}

    def test_highest_priority_match_level(self, sample_df: "object") -> None:
        queue = build_mapping_queue(sample_df)
        histidine = next(r for r in queue if r.name == "L-Histidine")
        # f1 is MS2 (priority 3), f2 is MS1 (priority 2) → should keep MS2
        assert histidine.match_level == "MS2"

    def test_limit_parameter(self, sample_df: "object") -> None:
        queue = build_mapping_queue(sample_df, limit=1)
        assert len(queue) == 1

    def test_as_api_record_with_hint(self, sample_df: "object") -> None:
        queue = build_mapping_queue(sample_df)
        histidine = next(r for r in queue if r.name == "L-Histidine")
        api_rec = histidine.as_api_record()
        assert api_rec["name"] == "L-Histidine"
        assert api_rec["identifiers"] == {"HMDB": "HMDB00177"}

    def test_as_api_record_without_hint(self, sample_df: "object") -> None:
        queue = build_mapping_queue(sample_df)
        glucose = next(r for r in queue if r.name == "Glucose")
        api_rec = glucose.as_api_record()
        assert "identifiers" not in api_rec
