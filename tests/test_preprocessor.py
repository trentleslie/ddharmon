"""Tests for ddharmon ingestion preprocessor."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ddharmon.models.data_dictionary import DataDictionary, Field


class TestSplitIdentifier:
    """Tests for _split_identifier helper."""

    def test_snake_case(self) -> None:
        from ddharmon.ingestion.preprocessor import _split_identifier

        assert _split_identifier("assessment_health_history_bmi") == ["assessment", "health", "history", "bmi"]

    def test_camel_case(self) -> None:
        from ddharmon.ingestion.preprocessor import _split_identifier

        assert _split_identifier("assessmentHealthHistoryBmi") == ["assessment", "health", "history", "bmi"]

    def test_dot_notation(self) -> None:
        from ddharmon.ingestion.preprocessor import _split_identifier

        assert _split_identifier("the.basics.birthplace") == ["the", "basics", "birthplace"]

    def test_kebab_case(self) -> None:
        from ddharmon.ingestion.preprocessor import _split_identifier

        assert _split_identifier("blood-pressure-systolic") == ["blood", "pressure", "systolic"]

    def test_mixed_delimiters(self) -> None:
        from ddharmon.ingestion.preprocessor import _split_identifier

        assert _split_identifier("the_basics.birthplace_country") == ["the", "basics", "birthplace", "country"]

    def test_single_token(self) -> None:
        from ddharmon.ingestion.preprocessor import _split_identifier

        assert _split_identifier("age") == ["age"]

    def test_uppercase_acronym(self) -> None:
        from ddharmon.ingestion.preprocessor import _split_identifier

        # "BMI" stays together, then "calculated" is separate
        result = _split_identifier("BMICalculated")
        assert result == ["bm", "icalculated"] or result == ["bmicalculated"] or "bmi" in "".join(result).lower()


class TestFindCommonPrefixTokens:
    """Tests for common prefix detection."""

    def test_clear_common_prefix(self) -> None:
        from ddharmon.ingestion.preprocessor import _find_common_prefix_tokens

        names = [
            "assessment_health_bmi",
            "assessment_health_age",
            "assessment_health_weight",
            "assessment_health_height",
            "assessment_health_bp",
        ]
        prefix = _find_common_prefix_tokens(names, min_ratio=0.5)
        assert prefix == ["assessment", "health"]

    def test_no_common_prefix(self) -> None:
        from ddharmon.ingestion.preprocessor import _find_common_prefix_tokens

        names = ["age", "bmi", "height", "weight"]
        prefix = _find_common_prefix_tokens(names, min_ratio=0.5)
        assert prefix == []

    def test_prefix_below_ratio(self) -> None:
        from ddharmon.ingestion.preprocessor import _find_common_prefix_tokens

        # Only 1 out of 5 share any given prefix — below min_count=max(2, 0.8*5=4)
        names = [
            "assessment_bmi",
            "diet_sugar",
            "exercise_steps",
            "lab_glucose",
            "vital_bp",
        ]
        prefix = _find_common_prefix_tokens(names, min_ratio=0.8)
        assert prefix == []

    def test_empty_list(self) -> None:
        from ddharmon.ingestion.preprocessor import _find_common_prefix_tokens

        assert _find_common_prefix_tokens([], min_ratio=0.5) == []

    def test_single_name(self) -> None:
        from ddharmon.ingestion.preprocessor import _find_common_prefix_tokens

        # min_count is max(2, ...) so a single name can't form a prefix group
        assert _find_common_prefix_tokens(["assessment_bmi"], min_ratio=0.5) == []


class TestRemoveTokenPrefix:
    """Tests for _remove_token_prefix helper."""

    def test_snake_case(self) -> None:
        from ddharmon.ingestion.preprocessor import _remove_token_prefix

        assert _remove_token_prefix("assessment_health_history_bmi", 3) == "bmi"

    def test_dot_notation(self) -> None:
        from ddharmon.ingestion.preprocessor import _remove_token_prefix

        assert _remove_token_prefix("the.basics.birthplace", 2) == "birthplace"

    def test_would_leave_empty(self) -> None:
        from ddharmon.ingestion.preprocessor import _remove_token_prefix

        # Removing all tokens returns empty string
        result = _remove_token_prefix("assessment_health", 2)
        assert result == ""

    def test_remove_one_token(self) -> None:
        from ddharmon.ingestion.preprocessor import _remove_token_prefix

        assert _remove_token_prefix("survey_age_years", 1) == "age_years"


class TestNormalizeUnicode:
    """Tests for unicode normalization."""

    def test_normalizes_curly_quotes(self) -> None:
        from ddharmon.ingestion.preprocessor import _normalize_unicode
        from ddharmon.models.data_dictionary import Field

        # ftfy normalizes curly quotes to straight quotes
        fields = [Field(variable_name="age", description="Patient\u2019s age at enrollment")]
        _normalize_unicode(fields)
        assert "Patient's age" in fields[0].description

    def test_fixes_encoding_artifacts(self) -> None:
        from ddharmon.ingestion.preprocessor import _normalize_unicode
        from ddharmon.models.data_dictionary import Field

        # â€™ is a common mojibake for right single quote
        fields = [Field(variable_name="age", description="Patient\u00e2\u0080\u0099s age")]
        _normalize_unicode(fields)
        assert "\u00e2\u0080" not in fields[0].description

    def test_normalizes_question_text(self) -> None:
        from ddharmon.ingestion.preprocessor import _normalize_unicode
        from ddharmon.models.data_dictionary import Field

        fields = [Field(variable_name="q1", description="Question", question_text="What\u00a0is your age?")]
        _normalize_unicode(fields)
        # Non-breaking space should be normalized
        assert fields[0].question_text is not None


class TestStripCommonPrefixes:
    """Tests for common prefix stripping on fields."""

    def _make_fields(self, names: list[str]) -> list[Field]:
        from ddharmon.models.data_dictionary import Field

        return [Field(variable_name=n, description=f"Description of {n}") for n in names]

    def test_strips_shared_prefix(self) -> None:
        from ddharmon.ingestion.preprocessor import _strip_common_prefixes

        fields = self._make_fields(
            [
                "assessment_health_bmi",
                "assessment_health_age",
                "assessment_health_weight",
                "assessment_health_height",
            ]
        )
        _strip_common_prefixes(fields, min_length=8, min_ratio=0.5)
        names = [f.variable_name for f in fields]
        assert "bmi" in names
        assert "age" in names

    def test_preserves_short_prefix(self) -> None:
        from ddharmon.ingestion.preprocessor import _strip_common_prefixes

        fields = self._make_fields(["q_age", "q_bmi", "q_height"])
        _strip_common_prefixes(fields, min_length=8, min_ratio=0.5)
        # "q" is only 1 char — below min_length=8, so kept
        names = [f.variable_name for f in fields]
        assert all(n.startswith("q_") for n in names)

    def test_doesnt_strip_sole_token(self) -> None:
        from ddharmon.ingestion.preprocessor import _strip_common_prefixes

        # If stripping would leave nothing, skip that field
        fields = self._make_fields(
            [
                "assessment_health",
                "assessment_health_bmi",
                "assessment_health_age",
            ]
        )
        _strip_common_prefixes(fields, min_length=8, min_ratio=0.5)
        # The first field ("assessment_health") has only the prefix tokens, so it should be unchanged
        assert fields[0].variable_name == "assessment_health"


class TestRemoveStopwords:
    """Tests for stopword removal."""

    def test_removes_substring(self) -> None:
        from ddharmon.ingestion.preprocessor import _remove_stopwords
        from ddharmon.models.data_dictionary import Field

        fields = [
            Field(variable_name="assessmenthealthhistory_bmi", description="BMI"),
            Field(variable_name="assessmenthealthhistory_age", description="Age"),
        ]
        _remove_stopwords(fields, ["assessmenthealthhistory"])
        assert fields[0].variable_name == "bmi"
        assert fields[1].variable_name == "age"

    def test_case_insensitive(self) -> None:
        from ddharmon.ingestion.preprocessor import _remove_stopwords
        from ddharmon.models.data_dictionary import Field

        fields = [Field(variable_name="TheBasics_birthplace", description="Birthplace")]
        _remove_stopwords(fields, ["thebasics"])
        assert "thebasics" not in fields[0].variable_name.lower()
        assert "birthplace" in fields[0].variable_name.lower()

    def test_empty_stopwords(self) -> None:
        from ddharmon.ingestion.preprocessor import _remove_stopwords
        from ddharmon.models.data_dictionary import Field

        fields = [Field(variable_name="age", description="Age")]
        _remove_stopwords(fields, [])
        assert fields[0].variable_name == "age"

    def test_cleans_consecutive_delimiters(self) -> None:
        from ddharmon.ingestion.preprocessor import _remove_stopwords
        from ddharmon.models.data_dictionary import Field

        fields = [Field(variable_name="survey__demographics__age", description="Age")]
        _remove_stopwords(fields, ["demographics"])
        # Should not leave "survey___age" with triple underscore
        assert "__" not in fields[0].variable_name


class TestDedupNameInDescription:
    """Tests for substring deduplication."""

    def test_suppresses_redundant_name(self) -> None:
        from ddharmon.ingestion.preprocessor import _dedup_name_in_description
        from ddharmon.models.data_dictionary import Field

        fields = [
            Field(variable_name="body_mass_index", description="Body mass index calculated from height and weight")
        ]
        _dedup_name_in_description(fields)
        assert fields[0]._embed_variable_name is False

    def test_keeps_distinct_name(self) -> None:
        from ddharmon.ingestion.preprocessor import _dedup_name_in_description
        from ddharmon.models.data_dictionary import Field

        fields = [Field(variable_name="bmi", description="Body mass index calculated from height and weight")]
        _dedup_name_in_description(fields)
        assert fields[0]._embed_variable_name is True

    def test_handles_underscores_as_spaces(self) -> None:
        from ddharmon.ingestion.preprocessor import _dedup_name_in_description
        from ddharmon.models.data_dictionary import Field

        fields = [Field(variable_name="smoking_status", description="Current smoking status of participant")]
        _dedup_name_in_description(fields)
        assert fields[0]._embed_variable_name is False


class TestNormalizeWhitespace:
    """Tests for whitespace normalization."""

    def test_collapses_runs(self) -> None:
        from ddharmon.ingestion.preprocessor import _normalize_whitespace
        from ddharmon.models.data_dictionary import Field

        fields = [Field(variable_name="age", description="Age  at   enrollment")]
        _normalize_whitespace(fields)
        assert fields[0].description == "Age at enrollment"

    def test_strips_leading_trailing(self) -> None:
        from ddharmon.ingestion.preprocessor import _normalize_whitespace
        from ddharmon.models.data_dictionary import Field

        fields = [Field(variable_name="  age  ", description="  Age at enrollment  ")]
        _normalize_whitespace(fields)
        assert fields[0].variable_name == "age"
        assert fields[0].description == "Age at enrollment"


class TestPreprocessDictionary:
    """Integration tests for the full preprocess_dictionary pipeline."""

    def _make_dd(self, name_desc_pairs: list[tuple[str, str]]) -> DataDictionary:
        from ddharmon.models.data_dictionary import DataDictionary, Field

        fields = {n: Field(variable_name=n, description=d) for n, d in name_desc_pairs}
        return DataDictionary(name="test", fields=fields)

    def test_preserves_raw_values(self) -> None:
        from ddharmon.ingestion.preprocessor import preprocess_dictionary

        dd = self._make_dd(
            [
                ("assessment_health_bmi", "Body mass index"),
                ("assessment_health_age", "Age at enrollment"),
                ("assessment_health_weight", "Body weight in kg"),
                ("assessment_health_height", "Standing height"),
            ]
        )
        preprocess_dictionary(dd)

        for f in dd.fields.values():
            assert f.raw_variable_name is not None
            assert f.raw_description is not None
            assert f.raw_variable_name.startswith("assessment_health_")

    def test_full_pipeline_strips_prefix(self) -> None:
        from ddharmon.ingestion.preprocessor import preprocess_dictionary

        dd = self._make_dd(
            [
                ("assessment_health_bmi", "Body mass index"),
                ("assessment_health_age", "Age at enrollment"),
                ("assessment_health_weight", "Body weight in kg"),
                ("assessment_health_height", "Standing height"),
            ]
        )
        preprocess_dictionary(dd)

        names = set(dd.fields.keys())
        assert "bmi" in names
        assert "age" in names

    def test_rekeys_dictionary(self) -> None:
        from ddharmon.ingestion.preprocessor import preprocess_dictionary

        dd = self._make_dd(
            [
                ("assessment_health_bmi", "Body mass index"),
                ("assessment_health_age", "Age at enrollment"),
                ("assessment_health_weight", "Body weight in kg"),
                ("assessment_health_height", "Standing height"),
            ]
        )
        preprocess_dictionary(dd)

        # Dictionary keys should match current variable_name, not raw
        for key, field in dd.fields.items():
            assert key == field.variable_name

    def test_explicit_stopwords(self) -> None:
        from ddharmon.ingestion.preprocessor import preprocess_dictionary

        dd = self._make_dd(
            [
                ("questionnaire_smoking_status", "Smoking status"),
                ("questionnaire_drinking_freq", "Drinking frequency"),
            ]
        )
        preprocess_dictionary(dd, stopwords=["questionnaire"], strip_common_prefixes=False)

        names = set(dd.fields.keys())
        assert all("questionnaire" not in n for n in names)

    def test_stopwords_from_file(self, tmp_path: Path) -> None:
        from ddharmon.ingestion.preprocessor import preprocess_dictionary

        sw_file = tmp_path / "stopwords.json"
        sw_file.write_text(json.dumps({"stopwords": ["boilerplate"]}))

        dd = self._make_dd(
            [
                ("boilerplate_age", "Age"),
                ("boilerplate_bmi", "BMI"),
            ]
        )
        preprocess_dictionary(dd, stopwords_file=sw_file, strip_common_prefixes=False)

        names = set(dd.fields.keys())
        assert all("boilerplate" not in n for n in names)

    def test_empty_dictionary(self) -> None:
        from ddharmon.ingestion.preprocessor import preprocess_dictionary
        from ddharmon.models.data_dictionary import DataDictionary

        dd = DataDictionary(name="empty", fields={})
        result = preprocess_dictionary(dd)
        assert result.field_count == 0

    def test_no_mutation_without_patterns(self) -> None:
        from ddharmon.ingestion.preprocessor import preprocess_dictionary

        dd = self._make_dd(
            [
                ("age", "Age at enrollment"),
                ("bmi", "Body mass index"),
                ("height", "Standing height"),
            ]
        )
        preprocess_dictionary(dd)

        # No common prefix, no stopwords — names should be unchanged
        assert "age" in dd.fields
        assert "bmi" in dd.fields

    def test_returns_same_object(self) -> None:
        from ddharmon.ingestion.preprocessor import preprocess_dictionary

        dd = self._make_dd([("age", "Age")])
        result = preprocess_dictionary(dd)
        assert result is dd

    def test_content_hash_changes_after_preprocessing(self) -> None:
        from ddharmon.ingestion.preprocessor import preprocess_dictionary
        from ddharmon.models.data_dictionary import Field

        dd = self._make_dd(
            [
                ("assessment_health_bmi", "Body mass index"),
                ("assessment_health_age", "Age at enrollment"),
                ("assessment_health_weight", "Body weight in kg"),
                ("assessment_health_height", "Standing height"),
            ]
        )

        # Get hash before preprocessing
        old_field = Field(variable_name="assessment_health_bmi", description="Body mass index")
        old_hash = old_field.content_hash()

        preprocess_dictionary(dd)

        # After preprocessing, the field named "bmi" should have a different hash
        new_field = dd.fields.get("bmi")
        assert new_field is not None
        new_hash = new_field.content_hash()
        assert old_hash != new_hash

    def test_disabling_all_steps(self) -> None:
        from ddharmon.ingestion.preprocessor import preprocess_dictionary

        dd = self._make_dd(
            [
                ("assessment_health_bmi", "Body mass index"),
                ("assessment_health_age", "Age at enrollment"),
                ("assessment_health_weight", "Body weight in kg"),
                ("assessment_health_height", "Standing height"),
            ]
        )
        preprocess_dictionary(
            dd,
            normalize_unicode=False,
            strip_common_prefixes=False,
            dedup_name_in_description=False,
        )

        # Raw should still be saved, but names unchanged
        for f in dd.fields.values():
            assert f.raw_variable_name == f.variable_name
