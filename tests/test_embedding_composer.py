"""Tests for embedding text composer with parent context injection."""

from __future__ import annotations

import logging

import pytest

from ddharmon.embedding.composer import (
    compose_embedding_text,
    compose_value_content_hash,
    compose_value_text,
    composed_content_hash,
)
from ddharmon.models.data_dictionary import DataDictionary, Field, ResponseOption


def make_field(
    variable_name: str,
    description: str,
    parent_field_id: str | None = None,
    children: list[str] | None = None,
    standard_codes: dict[str, list[str]] | None = None,
    question_text: str | None = None,
    category: str | None = None,
) -> Field:
    """Helper to create a Field with minimal required attributes."""
    return Field(
        variable_name=variable_name,
        description=description,
        parent_field_id=parent_field_id,
        children=children or [],
        standard_codes=standard_codes or {},
        question_text=question_text,
        category=category,
    )


def make_dictionary(fields: list[Field]) -> DataDictionary:
    """Helper to create a DataDictionary from a list of Fields."""
    return DataDictionary(
        name="test_dict",
        fields={f.variable_name: f for f in fields},
    )


class TestComposeEmbeddingText:
    """Tests for compose_embedding_text()."""

    def test_root_field_returns_base_text(self) -> None:
        """Root field (no parent) returns field.to_embedding_text() unchanged."""
        field = make_field("age", "Age of participant")
        dd = make_dictionary([field])
        result = compose_embedding_text(field, dd)
        assert result == field.to_embedding_text()

    def test_child_field_prepends_parent_description(self) -> None:
        """Child field with parent prepends parent.description to base text."""
        parent = make_field("pain_areas", "Please shade areas of pain", children=["pain_left_hand"])
        child = make_field("pain_left_hand", "Back of left hand", parent_field_id="pain_areas")
        dd = make_dictionary([parent, child])
        result = compose_embedding_text(child, dd)
        assert result.startswith("Please shade areas of pain: ")
        assert child.to_embedding_text() in result

    def test_child_missing_parent_returns_base_text(self) -> None:
        """Child field with missing parent in dictionary returns base text (graceful fallback)."""
        child = make_field("orphan_child", "Some description", parent_field_id="nonexistent_parent")
        dd = make_dictionary([child])
        result = compose_embedding_text(child, dd)
        assert result == child.to_embedding_text()

    def test_standard_codes_not_in_composed_text(self) -> None:
        """Standard codes in field.standard_codes do NOT appear in composed text."""
        field = make_field(
            "blood_pressure",
            "Systolic blood pressure",
            standard_codes={"SNOMED": ["271649006"], "LOINC": ["8480-6"]},
        )
        dd = make_dictionary([field])
        result = compose_embedding_text(field, dd)
        assert "271649006" not in result
        assert "8480-6" not in result
        assert "SNOMED" not in result
        assert "LOINC" not in result


class TestComposedContentHash:
    """Tests for composed_content_hash()."""

    def test_different_hash_with_parent_context(self) -> None:
        """composed_content_hash() returns different hash than field.content_hash() when parent context is added."""
        parent = make_field("pain_areas", "Please shade areas of pain", children=["pain_left_hand"])
        child = make_field("pain_left_hand", "Back of left hand", parent_field_id="pain_areas")
        dd = make_dictionary([parent, child])
        assert composed_content_hash(child, dd) != child.content_hash()

    def test_consistent_hash(self) -> None:
        """composed_content_hash() returns consistent hash for same input."""
        field = make_field("age", "Age of participant")
        dd = make_dictionary([field])
        hash1 = composed_content_hash(field, dd)
        hash2 = composed_content_hash(field, dd)
        assert hash1 == hash2
        assert len(hash1) == 16  # 16-char hex

    def test_root_field_hash_matches_field_hash(self) -> None:
        """For root fields, composed_content_hash equals field.content_hash()."""
        field = make_field("age", "Age of participant")
        dd = make_dictionary([field])
        # Root field composed text == base text, so hashes should match
        assert composed_content_hash(field, dd) == field.content_hash()


class TestComposerWarnings:
    """Tests for warning behavior on minimal text."""

    def test_short_text_logs_debug(self, caplog) -> None:
        """Fields with empty/minimal text produce a debug log."""
        field = make_field("x", "y")  # Very short text
        dd = make_dictionary([field])
        with caplog.at_level(logging.DEBUG, logger="ddharmon.embedding.composer"):
            compose_embedding_text(field, dd)
        assert any(
            "short" in msg.lower() for msg in caplog.messages
        ), f"Expected debug message about short text, got: {caplog.messages}"


def _make_value_field(
    variable_name: str = "test_var",
    description: str = "Test variable description",
    response_options: list[ResponseOption] | None = None,
    data_type: str | None = None,
    units: str | None = None,
) -> Field:
    """Helper to create a Field with value-related attributes."""
    return Field(
        variable_name=variable_name,
        description=description,
        response_options=response_options or [],
        data_type=data_type,
        units=units,
    )


class TestComposeValueText:
    """Tests for compose_value_text()."""

    def test_response_options_with_data_type(self) -> None:
        """Response options + data_type -> 'Never; Rarely; Sometimes | ordinal'."""
        field = _make_value_field(
            response_options=[
                ResponseOption(code="1", label="Never"),
                ResponseOption(code="2", label="Rarely"),
                ResponseOption(code="3", label="Sometimes"),
            ],
            data_type="ordinal",
        )
        result = compose_value_text(field)
        assert result == "Never; Rarely; Sometimes | ordinal"

    def test_continuous_with_units(self) -> None:
        """data_type='continuous' + units='kg' -> 'continuous | kg'."""
        field = _make_value_field(data_type="continuous", units="kg")
        result = compose_value_text(field)
        assert result == "continuous | kg"

    def test_response_options_only(self) -> None:
        """Response options alone -> labels joined with ';'."""
        field = _make_value_field(
            response_options=[
                ResponseOption(code="1", label="Yes"),
                ResponseOption(code="0", label="No"),
            ],
        )
        result = compose_value_text(field)
        assert result == "Yes; No"

    def test_continuous_only(self) -> None:
        """Only data_type='continuous' -> 'continuous'."""
        field = _make_value_field(data_type="continuous")
        result = compose_value_text(field)
        assert result == "continuous"

    def test_empty_when_no_value_fields(self) -> None:
        """No answer-meaning fields populated -> empty string."""
        field = _make_value_field()
        result = compose_value_text(field)
        assert result == ""

    def test_response_options_labels_only_no_codes(self) -> None:
        """Response option codes are NOT included, only labels."""
        field = _make_value_field(
            response_options=[ResponseOption(code="99", label="Unknown")],
        )
        result = compose_value_text(field)
        assert "99" not in result
        assert "Unknown" in result


class TestComposeValueContentHash:
    """Tests for compose_value_content_hash()."""

    def test_returns_16_char_hex(self) -> None:
        """compose_value_content_hash() returns 16-char hex string."""
        field = _make_value_field(data_type="continuous", units="kg")
        result = compose_value_content_hash(field)
        assert len(result) == 16
        assert all(c in "0123456789abcdef" for c in result)

    def test_differs_from_semantic_hash(self) -> None:
        """Value content hash differs from semantic composed_content_hash for same field."""
        field = _make_value_field(
            response_options=[
                ResponseOption(code="1", label="Yes"),
                ResponseOption(code="0", label="No"),
            ],
            data_type="binary",
        )
        dd = make_dictionary([field])
        value_hash = compose_value_content_hash(field)
        semantic_hash = composed_content_hash(field, dd)
        assert value_hash != semantic_hash

    def test_consistent(self) -> None:
        """Same input produces same hash."""
        field = _make_value_field(data_type="continuous", units="kg")
        assert compose_value_content_hash(field) == compose_value_content_hash(field)
