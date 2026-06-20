"""Tests for TSV loading, delimiter detection, and minimum role validation.

Covers TSV parsing and silent schema fallback.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from ddharmon.ingestion import load_dictionary
from ddharmon.ingestion.csv_parser import GenericCSVParser
from ddharmon.models.enums import FieldRole


@pytest.fixture
def parser() -> GenericCSVParser:
    """Create a GenericCSVParser."""
    return GenericCSVParser()


# ---------------------------------------------------------------------------
# Delimiter detection
# ---------------------------------------------------------------------------


class TestDelimiterDetection:
    """Tests for GenericCSVParser._detect_delimiter and TSV loading."""

    def test_tsv_extension_detected(self, parser: GenericCSVParser, tmp_path: Path) -> None:
        """TSV file (tab-separated) loads correctly with proper field parsing."""
        tsv_content = "field_name\tdescription\tcategory\nalpha\tAlpha description\tDemographics\nbeta\tBeta description\tLabs\ngamma\tGamma description\tVitals\n"
        p = tmp_path / "test.tsv"
        p.write_text(tsv_content)

        column_map = {
            "field_name": FieldRole.VARIABLE_NAME,
            "description": FieldRole.DESCRIPTION,
            "category": FieldRole.CATEGORY,
        }
        dd = parser.load(p, column_map=column_map)
        assert dd.field_count == 3
        assert "alpha" in dd.fields
        assert "beta" in dd.fields
        assert "gamma" in dd.fields
        assert not any(name.startswith("_ROW_") for name in dd.fields)

    def test_csv_extension_detected(self, parser: GenericCSVParser, tmp_path: Path) -> None:
        """CSV file (comma-separated) still loads correctly."""
        csv_content = "variable_name,description,data_type,section\nage,Age of participant,integer,Demographics\nsex,Sex of participant,categorical,Demographics\n"
        p = tmp_path / "test.csv"
        p.write_text(csv_content)

        column_map = {
            "variable_name": FieldRole.VARIABLE_NAME,
            "description": FieldRole.DESCRIPTION,
            "data_type": FieldRole.DATA_TYPE,
            "section": FieldRole.CATEGORY,
        }
        dd = parser.load(p, column_map=column_map)
        assert dd.field_count == 2
        assert "age" in dd.fields

    def test_unknown_extension_sniffs_tab(self, parser: GenericCSVParser, tmp_path: Path) -> None:
        """Unknown extension (.txt) with tab content is detected via csv.Sniffer."""
        tsv_content = (
            "field_name\tdescription\tcategory\nalpha\tAlpha description\tDemographics\nbeta\tBeta description\tLabs\n"
        )
        p = tmp_path / "test.txt"
        p.write_text(tsv_content)

        column_map = {
            "field_name": FieldRole.VARIABLE_NAME,
            "description": FieldRole.DESCRIPTION,
            "category": FieldRole.CATEGORY,
        }
        dd = parser.load(p, column_map=column_map)
        assert dd.field_count == 2
        assert "alpha" in dd.fields

    def test_unknown_extension_sniffs_comma(self, parser: GenericCSVParser, tmp_path: Path) -> None:
        """Unknown extension (.txt) with comma content is detected via csv.Sniffer."""
        csv_content = (
            "variable_name,description,data_type\nage,Age of participant,integer\nbmi,Body mass index,continuous\n"
        )
        p = tmp_path / "test.txt"
        p.write_text(csv_content)

        column_map = {
            "variable_name": FieldRole.VARIABLE_NAME,
            "description": FieldRole.DESCRIPTION,
            "data_type": FieldRole.DATA_TYPE,
        }
        dd = parser.load(p, column_map=column_map)
        assert dd.field_count == 2
        assert "age" in dd.fields

    def test_detect_delimiter_tsv(self) -> None:
        """_detect_delimiter returns tab for .tsv files."""
        assert GenericCSVParser._detect_delimiter(Path("test.tsv")) == "\t"

    def test_detect_delimiter_csv(self) -> None:
        """_detect_delimiter returns comma for .csv files."""
        assert GenericCSVParser._detect_delimiter(Path("test.csv")) == ","


# ---------------------------------------------------------------------------
# Minimum role validation (via load_dictionary)
# ---------------------------------------------------------------------------


class TestMinimumRoleValidation:
    """Tests for load_dictionary() requiring semantic text for embedding."""

    def test_no_semantic_fields_raises(self, tmp_path: Path) -> None:
        """ValueError raised when no semantic text fields are provided."""
        p = tmp_path / "bad_schema.csv"
        p.write_text("col_a,col_b,col_c\n1,2,3\n4,5,6\n")

        with pytest.raises(ValueError, match="variable_name.*description.*question_text"):
            load_dictionary(p)

    def test_field_id_alone_raises(self, tmp_path: Path) -> None:
        """ValueError raised when only field_id is provided (no semantic text)."""
        p = tmp_path / "ids.csv"
        p.write_text("my_id,col_b\n20116,foo\n20117,bar\n")

        with pytest.raises(ValueError, match="variable_name.*description.*question_text"):
            load_dictionary(p, field_id="my_id")

    def test_variable_name_with_description_succeeds(self, tmp_path: Path) -> None:
        """Providing variable_name and description loads successfully."""
        csv_content = "col_a,col_b,col_c\nfoo,Some description,3\nbar,Another desc,6\n"
        p = tmp_path / "explicit.csv"
        p.write_text(csv_content)

        dd = load_dictionary(p, variable_name="col_a", description="col_b")
        assert dd.field_count == 2

    def test_no_description_warns(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """Warning logged when description is not provided."""
        csv_content = "variable_name,metric_val,score\nfoo,10,20\nbar,30,40\n"
        p = tmp_path / "no_desc.csv"
        p.write_text(csv_content)

        with caplog.at_level(logging.WARNING):
            # Will produce no fields since description column is not mapped and no fallback
            load_dictionary(p, variable_name="variable_name")
            assert any("description" in record.message.lower() for record in caplog.records)

