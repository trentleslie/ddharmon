"""Tests for GenericCSVParser CSV ingestion.

Tests CSV loading, SNOMED extraction, NA handling, variable name generation,
and explicit column-to-role mapping.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ddharmon.ingestion.csv_parser import GenericCSVParser
from ddharmon.models.enums import FieldRole


@pytest.fixture
def parser() -> GenericCSVParser:
    """Create a GenericCSVParser."""
    return GenericCSVParser()


@pytest.fixture
def simple_csv(tmp_path: Path) -> Path:
    """Create a simple CSV with known columns."""
    csv_content = (
        "variable_name,description,data_type,section\n"
        "age,Age of participant,integer,Demographics\n"
        "sex,Sex of participant,categorical,Demographics\n"
        "bmi,Body mass index,continuous,Measurements\n"
    )
    p = tmp_path / "simple.csv"
    p.write_text(csv_content)
    return p


SIMPLE_MAP = {
    "variable_name": FieldRole.VARIABLE_NAME,
    "description": FieldRole.DESCRIPTION,
    "data_type": FieldRole.DATA_TYPE,
    "section": FieldRole.CATEGORY,
}


@pytest.fixture
def study_csv(tmp_path: Path) -> Path:
    """Create a study-style CSV with SNOMED codes and two-population pattern."""
    csv_content = (
        "Data_Type,Historical_ID,Phenotype_Description,snomed_term_1,snomed_term_2,snomed_term_3,snomed_term_4\n"
        "Measurements,HID001,Serum cystatin C level,1002561000000109 | Serum cystatin C level (observable entity) |,NA,NA,NA\n"
        "Self-reported,HID002,Have you ever had knee pain?,NA,NA,NA,NA\n"
        "Measurements,NA,Serum albumin level,1018251000000107 | Serum albumin level (observable entity) |,NA,NA,NA\n"
        "Self-reported,NA,How often do you exercise?,NA,NA,NA,NA\n"
        "Other dataset,HID003,Smoking status,NA,NA,NA,NA\n"
    )
    p = tmp_path / "study.csv"
    p.write_text(csv_content)
    return p


STUDY_MAP = {
    "Historical_ID": FieldRole.VARIABLE_NAME,
    "Phenotype_Description": FieldRole.DESCRIPTION,
    "Data_Type": FieldRole.CATEGORY,
    "snomed_term_1": FieldRole.STANDARD_CODE,
    "snomed_term_2": FieldRole.STANDARD_CODE,
    "snomed_term_3": FieldRole.STANDARD_CODE,
    "snomed_term_4": FieldRole.STANDARD_CODE,
}


@pytest.fixture
def missing_description_csv(tmp_path: Path) -> Path:
    """Create a CSV with rows that have missing descriptions."""
    csv_content = (
        "variable_name,description,data_type\n"
        "age,Age of participant,integer\n"
        "empty_desc,,categorical\n"
        "valid_field,A valid description,continuous\n"
    )
    p = tmp_path / "missing_desc.csv"
    p.write_text(csv_content)
    return p


MINIMAL_MAP = {
    "variable_name": FieldRole.VARIABLE_NAME,
    "description": FieldRole.DESCRIPTION,
}


class TestGenericCSVParser:
    """Tests for GenericCSVParser.load()."""

    def test_load_simple_csv(self, parser: GenericCSVParser, simple_csv: Path) -> None:
        """Loading a simple CSV with explicit column_map produces correct DataDictionary."""
        dd = parser.load(simple_csv, cohort_name="test", column_map=SIMPLE_MAP)

        assert dd.name == "simple"
        assert dd.cohort_name == "test"
        assert dd.source_path == simple_csv
        assert dd.field_count == 3

        age = dd.fields["age"]
        assert age.variable_name == "age"
        assert age.description == "Age of participant"
        assert age.category == "Demographics"

    def test_load_returns_all_fields(self, parser: GenericCSVParser, simple_csv: Path) -> None:
        """All rows with descriptions produce fields keyed by variable_name."""
        dd = parser.load(simple_csv, column_map=SIMPLE_MAP)
        assert set(dd.fields.keys()) == {"age", "sex", "bmi"}

    def test_study_snomed_extraction(self, parser: GenericCSVParser, study_csv: Path) -> None:
        """SNOMED codes extracted from pipe-delimited format into standard_codes."""
        dd = parser.load(study_csv, cohort_name="study_a", column_map=STUDY_MAP)

        hid001 = dd.fields["HID001"]
        assert "SNOMED" in hid001.standard_codes
        assert "1002561000000109" in hid001.standard_codes["SNOMED"]

    def test_study_na_handling(self, parser: GenericCSVParser, study_csv: Path) -> None:
        """NA values in SNOMED columns and Historical_ID are handled as None/empty."""
        dd = parser.load(study_csv, cohort_name="study_a", column_map=STUDY_MAP)

        hid002 = dd.fields["HID002"]
        snomed_codes = hid002.standard_codes.get("SNOMED", [])
        assert len(snomed_codes) == 0

    def test_study_variable_name_generation(self, parser: GenericCSVParser, study_csv: Path) -> None:
        """Rows without Historical_ID get auto-generated variable names."""
        dd = parser.load(study_csv, cohort_name="study_a", column_map=STUDY_MAP)

        assert dd.field_count == 5
        generated_names = [name for name in dd.fields if name.startswith("_ROW_")]
        assert len(generated_names) == 2

    def test_missing_description_falls_back_to_variable_name(
        self, parser: GenericCSVParser, missing_description_csv: Path
    ) -> None:
        """Rows with empty description but valid variable_name use the name as description."""
        dd = parser.load(missing_description_csv, column_map=MINIMAL_MAP)

        assert dd.field_count == 3
        assert "age" in dd.fields
        assert "valid_field" in dd.fields
        # empty_desc now survives — description falls back to variable_name
        assert "empty_desc" in dd.fields
        assert dd.fields["empty_desc"].description == "empty_desc"

    def test_study_section_mapping(self, parser: GenericCSVParser, study_csv: Path) -> None:
        """study Data_Type column maps to Field.category via explicit mapping."""
        dd = parser.load(study_csv, cohort_name="study_a", column_map=STUDY_MAP)

        hid001 = dd.fields["HID001"]
        assert hid001.category == "Measurements"

        hid002 = dd.fields["HID002"]
        assert hid002.category == "Self-reported"

    def test_source_path_set(self, parser: GenericCSVParser, simple_csv: Path) -> None:
        """DataDictionary.source_path is set to the input path."""
        dd = parser.load(simple_csv, column_map=SIMPLE_MAP)
        assert dd.source_path == simple_csv

    def test_load_with_string_path(self, parser: GenericCSVParser, simple_csv: Path) -> None:
        """Load accepts string paths as well as Path objects."""
        dd = parser.load(str(simple_csv), column_map=SIMPLE_MAP)
        assert dd.field_count == 3

    def test_multiple_snomed_codes(self, tmp_path: Path) -> None:
        """Multiple SNOMED columns can each provide codes."""
        csv_content = (
            "Data_Type,Historical_ID,Phenotype_Description,snomed_term_1,snomed_term_2,snomed_term_3,snomed_term_4\n"
            "Measurements,HID001,Test field,12345 | Code A |,67890 | Code B |,NA,NA\n"
        )
        p = tmp_path / "multi_snomed.csv"
        p.write_text(csv_content)

        parser = GenericCSVParser()
        dd = parser.load(p, cohort_name="study_a", column_map=STUDY_MAP)

        hid001 = dd.fields["HID001"]
        assert "SNOMED" in hid001.standard_codes
        assert "12345" in hid001.standard_codes["SNOMED"]
        assert "67890" in hid001.standard_codes["SNOMED"]
        assert len(hid001.standard_codes["SNOMED"]) == 2

    def test_unmapped_columns_go_to_metadata(self, parser: GenericCSVParser, simple_csv: Path) -> None:
        """Columns not in column_map are stored in Field.metadata."""
        # Only map variable_name and description, leave data_type and section unmapped
        dd = parser.load(simple_csv, column_map=MINIMAL_MAP)

        age = dd.fields["age"]
        assert "data_type" in age.metadata
        assert age.metadata["data_type"] == "integer"
        assert "section" in age.metadata
        assert age.metadata["section"] == "Demographics"

    def test_invalid_column_name_raises(self, parser: GenericCSVParser, simple_csv: Path) -> None:
        """Mapping a non-existent column raises ValueError with helpful message."""
        bad_map = {
            "variable_name": FieldRole.VARIABLE_NAME,
            "nonexistent_col": FieldRole.DESCRIPTION,
        }
        with pytest.raises(ValueError, match="nonexistent_col"):
            parser.load(simple_csv, column_map=bad_map)
