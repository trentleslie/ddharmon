"""Unit tests for ddharmon data models."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from ddharmon.models.data_dictionary import Field


class TestFieldRole:
    """Tests for FieldRole enum."""

    def test_field_role_values(self) -> None:
        from ddharmon.models.enums import FieldRole

        assert FieldRole.VARIABLE_NAME == "variable_name"
        assert FieldRole.FIELD_ID == "field_id"
        assert FieldRole.DESCRIPTION == "description"
        assert FieldRole.SHORT_LABEL == "short_label"
        assert FieldRole.DATA_TYPE == "data_type"
        assert FieldRole.UNITS == "units"
        assert FieldRole.CATEGORY == "category"
        assert FieldRole.VALUE_ENCODING == "value_encoding"
        assert FieldRole.STANDARD_CODE == "standard_code"
        assert FieldRole.PARENT_ID == "parent_id"
        assert FieldRole.QUESTION_TEXT == "question_text"
        assert FieldRole.VALIDATION == "validation"

    def test_field_role_is_str(self) -> None:
        from ddharmon.models.enums import FieldRole

        assert isinstance(FieldRole.VARIABLE_NAME, str)


class TestResponseOption:
    """Tests for ResponseOption dataclass."""

    def test_basic_creation(self) -> None:
        from ddharmon.models.data_dictionary import ResponseOption

        opt = ResponseOption(code="1", label="Yes")
        assert opt.code == "1"
        assert opt.label == "Yes"
        assert opt.order is None

    def test_with_order(self) -> None:
        from ddharmon.models.data_dictionary import ResponseOption

        opt = ResponseOption(code="2", label="Sometimes", order=2)
        assert opt.order == 2


class TestValueSet:
    """Tests for ValueSet dataclass."""

    def test_basic_creation(self) -> None:
        from ddharmon.models.data_dictionary import ValueSet

        vs = ValueSet(name="yes_no")
        assert vs.name == "yes_no"
        assert vs.options == []

    def test_with_options(self) -> None:
        from ddharmon.models.data_dictionary import ResponseOption, ValueSet

        opts = [
            ResponseOption(code="1", label="Yes"),
            ResponseOption(code="0", label="No"),
        ]
        vs = ValueSet(name="yes_no", options=opts)
        assert len(vs.options) == 2
        assert vs.options[0].label == "Yes"


class TestField:
    """Tests for Field dataclass."""

    def test_basic_creation(self) -> None:
        from ddharmon.models.data_dictionary import Field

        f = Field(variable_name="age", description="Age at enrollment")
        assert f.variable_name == "age"
        assert f.description == "Age at enrollment"
        assert f.data_type is None
        assert f.category is None
        assert f.response_options == []
        assert f.parent_field_id is None
        assert f.children == []
        assert f.standard_codes == {}
        assert f.metadata == {}
        assert f._synthetic is False

    def test_empty_variable_name_raises(self) -> None:
        from ddharmon.models.data_dictionary import Field

        with pytest.raises(ValueError, match="variable_name"):
            Field(variable_name="", description="Some description")

    def test_empty_description_raises(self) -> None:
        from ddharmon.models.data_dictionary import Field

        with pytest.raises(ValueError, match="description"):
            Field(variable_name="age", description="")

    def test_to_embedding_text_basic(self) -> None:
        from ddharmon.models.data_dictionary import Field

        f = Field(variable_name="age", description="Age at enrollment")
        text = f.to_embedding_text()
        assert text == "age | Age at enrollment"

    def test_to_embedding_text_prefers_question_text_over_description(self) -> None:
        """question_text is the primary semantic field; description is only a fallback."""
        from ddharmon.models.data_dictionary import Field

        f = Field(
            variable_name="sleep_q",
            description="Trouble sleeping",
            question_text="Do you have trouble falling asleep at night?",
        )
        assert f.to_embedding_text() == "sleep_q | Do you have trouble falling asleep at night?"

    def test_to_embedding_text_falls_back_to_description_when_no_question_text(self) -> None:
        from ddharmon.models.data_dictionary import Field

        f = Field(variable_name="age", description="Age at enrollment")
        assert f.to_embedding_text() == "age | Age at enrollment"

    def test_to_embedding_text_omits_response_options_and_type(self) -> None:
        """Response options, data_type, and units belong to the value vector, not semantic."""
        from ddharmon.models.data_dictionary import Field, ResponseOption

        f = Field(
            variable_name="smoker",
            description="Do you smoke?",
            response_options=[
                ResponseOption(code="1", label="Yes"),
                ResponseOption(code="0", label="No"),
            ],
            data_type="categorical",
            units="n/a",
        )
        text = f.to_embedding_text()
        assert text == "smoker | Do you smoke?"
        assert "Values:" not in text
        assert "Type:" not in text
        assert "Units:" not in text

    def test_to_embedding_text_with_category(self) -> None:
        from ddharmon.models.data_dictionary import Field

        f = Field(variable_name="height", description="Standing height", category="Measurements")
        text = f.to_embedding_text()
        assert text == "height | Standing height | Category: Measurements"

    def test_to_embedding_text_dedupes_variable_name_equal_to_primary_text(self) -> None:
        from ddharmon.models.data_dictionary import Field

        # NIH CDE case: designation == definition, don't emit twice.
        f = Field(variable_name="Age at enrollment", description="Age at enrollment")
        assert f.to_embedding_text() == "Age at enrollment"

        # Dedupe also applies when question_text is primary and duplicates variable_name.
        f2 = Field(
            variable_name="Do you smoke?",
            description="Smoking status",
            question_text="Do you smoke?",
        )
        assert f2.to_embedding_text() == "Do you smoke?"

    def test_to_embedding_text_with_all_parts(self) -> None:
        from ddharmon.models.data_dictionary import Field

        f = Field(
            variable_name="pain_scale",
            description="Fallback description",
            question_text="Rate your pain",
            category="Self-reported",
        )
        text = f.to_embedding_text()
        assert text == "pain_scale | Rate your pain | Category: Self-reported"

    def test_content_hash_deterministic(self) -> None:
        from ddharmon.models.data_dictionary import Field

        f1 = Field(variable_name="age", description="Age at enrollment")
        f2 = Field(variable_name="age", description="Age at enrollment")
        assert f1.content_hash() == f2.content_hash()

    def test_content_hash_is_16_char_hex(self) -> None:
        from ddharmon.models.data_dictionary import Field

        f = Field(variable_name="age", description="Age at enrollment")
        h = f.content_hash()
        assert len(h) == 16
        assert all(c in "0123456789abcdef" for c in h)

    def test_content_hash_changes_with_content(self) -> None:
        from ddharmon.models.data_dictionary import Field

        f1 = Field(variable_name="age", description="Age at enrollment")
        f2 = Field(variable_name="age", description="Age at first visit")
        assert f1.content_hash() != f2.content_hash()

    def test_content_hash_changes_with_category(self) -> None:
        from ddharmon.models.data_dictionary import Field

        f1 = Field(variable_name="age", description="Age at enrollment")
        f2 = Field(variable_name="age", description="Age at enrollment", category="Demographics")
        assert f1.content_hash() != f2.content_hash()


class TestDataDictionary:
    """Tests for DataDictionary dataclass."""

    def _make_fields(self) -> dict[str, Field]:
        from ddharmon.models.data_dictionary import Field

        parent = Field(
            variable_name="parent_q",
            description="Parent question",
            children=["child_a", "child_b"],
        )
        child_a = Field(
            variable_name="child_a",
            description="Child A",
            parent_field_id="parent_q",
        )
        child_b = Field(
            variable_name="child_b",
            description="Child B",
            parent_field_id="parent_q",
        )
        standalone = Field(
            variable_name="standalone",
            description="Standalone field",
        )
        synthetic = Field(
            variable_name="_SYNTHETIC_abc12345",
            description="Inferred parent",
            _synthetic=True,
            children=["child_c"],
        )
        child_c = Field(
            variable_name="child_c",
            description="Child C",
            parent_field_id="_SYNTHETIC_abc12345",
        )
        return {
            "parent_q": parent,
            "child_a": child_a,
            "child_b": child_b,
            "standalone": standalone,
            "_SYNTHETIC_abc12345": synthetic,
            "child_c": child_c,
        }

    def test_field_count(self) -> None:
        from ddharmon.models.data_dictionary import DataDictionary

        dd = DataDictionary(name="test", fields=self._make_fields())
        assert dd.field_count == 6

    def test_get_children(self) -> None:
        from ddharmon.models.data_dictionary import DataDictionary

        dd = DataDictionary(name="test", fields=self._make_fields())
        children = dd.get_children("parent_q")
        assert len(children) == 2
        child_names = {c.variable_name for c in children}
        assert child_names == {"child_a", "child_b"}

    def test_get_children_nonexistent_parent(self) -> None:
        from ddharmon.models.data_dictionary import DataDictionary

        dd = DataDictionary(name="test", fields=self._make_fields())
        children = dd.get_children("nonexistent")
        assert children == []

    def test_get_root_fields(self) -> None:
        from ddharmon.models.data_dictionary import DataDictionary

        dd = DataDictionary(name="test", fields=self._make_fields())
        roots = dd.get_root_fields()
        root_names = {r.variable_name for r in roots}
        assert root_names == {"parent_q", "standalone", "_SYNTHETIC_abc12345"}

    def test_synthetic_field_count(self) -> None:
        from ddharmon.models.data_dictionary import DataDictionary

        dd = DataDictionary(name="test", fields=self._make_fields())
        assert dd.synthetic_field_count == 1

    def test_date_loaded_default(self) -> None:
        from datetime import datetime

        from ddharmon.models.data_dictionary import DataDictionary, Field

        dd = DataDictionary(
            name="test",
            fields={"f1": Field(variable_name="f1", description="Test field")},
        )
        assert isinstance(dd.date_loaded, datetime)

    def test_source_metadata(self) -> None:
        from pathlib import Path

        from ddharmon.models.data_dictionary import DataDictionary, Field

        dd = DataDictionary(
            name="test",
            fields={"f1": Field(variable_name="f1", description="Test field")},
            source_path=Path("/data/test.csv"),
            cohort_name="study_a",
        )
        assert dd.source_path == Path("/data/test.csv")
        assert dd.cohort_name == "study_a"


class TestModelsReExport:
    """Tests that models __init__.py re-exports all public types."""

    def test_all_types_importable(self) -> None:
        from ddharmon.models import (
            DataDictionary,
            Field,
            FieldRole,
            ResponseOption,
            ValueSet,
        )

        # Verify they are the correct classes (not None)
        assert FieldRole is not None
        assert ResponseOption is not None
        assert ValueSet is not None
        assert Field is not None
        assert DataDictionary is not None


class TestRelationEnum:
    """Tests for Relation enum."""

    def test_relation_has_six_members(self) -> None:
        from ddharmon.models.enums import Relation

        assert len(Relation) == 6

    def test_relation_values(self) -> None:
        from ddharmon.models.enums import Relation

        assert Relation.EXACT == "exact"
        assert Relation.BROADER == "broader"
        assert Relation.NARROWER == "narrower"
        assert Relation.COMPOSITE == "composite"
        assert Relation.DERIVABLE == "derivable"
        assert Relation.NO_MATCH == "no_match"

    def test_relation_is_str(self) -> None:
        from ddharmon.models.enums import Relation

        assert isinstance(Relation.EXACT, str)


class TestReviewStatusEnum:
    """Tests for ReviewStatus enum."""

    def test_review_status_has_three_members(self) -> None:
        from ddharmon.models.enums import ReviewStatus

        assert len(ReviewStatus) == 3

    def test_review_status_values(self) -> None:
        from ddharmon.models.enums import ReviewStatus

        assert ReviewStatus.AUTO_APPROVED == "auto_approved"
        assert ReviewStatus.PENDING_REVIEW == "pending_review"
        assert ReviewStatus.AUTO_REJECTED == "auto_rejected"


class TestUnmappedReasonEnum:
    """Tests for UnmappedReason enum."""

    def test_unmapped_reason_has_three_members(self) -> None:
        from ddharmon.models.enums import UnmappedReason

        assert len(UnmappedReason) == 3

    def test_unmapped_reason_values(self) -> None:
        from ddharmon.models.enums import UnmappedReason

        assert UnmappedReason.NO_CANDIDATES == "no_candidates"
        assert UnmappedReason.LLM_REJECTED_ALL == "llm_rejected_all"
        assert UnmappedReason.DOMAIN_MISMATCH == "domain_mismatch"


class TestFieldMapping:
    """Tests for FieldMapping dataclass."""

    def test_construction_with_required_fields(self) -> None:
        from ddharmon.models.enums import Relation, ReviewStatus
        from ddharmon.models.mapping import FieldMapping

        fm = FieldMapping(
            source_variable="age",
            target_variable="age_at_enrollment",
            relation=Relation.EXACT,
            confidence=0.95,
            cosine_similarity=0.88,
            llm_confidence=0.99,
            rationale="Same concept",
            review_status=ReviewStatus.AUTO_APPROVED,
        )
        assert fm.source_variable == "age"
        assert fm.target_variable == "age_at_enrollment"
        assert fm.relation == Relation.EXACT
        assert fm.confidence == 0.95
        assert fm.method == "embed_rerank"

    def test_timestamp_auto_set(self) -> None:
        from datetime import datetime

        from ddharmon.models.enums import Relation, ReviewStatus
        from ddharmon.models.mapping import FieldMapping

        fm = FieldMapping(
            source_variable="age",
            target_variable="age_at_enrollment",
            relation=Relation.EXACT,
            confidence=0.95,
            cosine_similarity=0.88,
            llm_confidence=0.99,
            rationale="Same concept",
            review_status=ReviewStatus.AUTO_APPROVED,
        )
        assert isinstance(fm.timestamp, datetime)


class TestUnmappedField:
    """Tests for UnmappedField dataclass."""

    def test_holds_rejected_candidates(self) -> None:
        from ddharmon.models.enums import Relation, ReviewStatus, UnmappedReason
        from ddharmon.models.mapping import FieldMapping, UnmappedField

        rejected = FieldMapping(
            source_variable="rare_field",
            target_variable="some_field",
            relation=Relation.NO_MATCH,
            confidence=0.1,
            cosine_similarity=0.15,
            llm_confidence=0.05,
            rationale="No semantic overlap",
            review_status=ReviewStatus.AUTO_REJECTED,
        )
        uf = UnmappedField(
            variable_name="rare_field",
            reason=UnmappedReason.LLM_REJECTED_ALL,
            rejected_candidates=[rejected],
        )
        assert uf.variable_name == "rare_field"
        assert uf.reason == UnmappedReason.LLM_REJECTED_ALL
        assert len(uf.rejected_candidates) == 1

    def test_default_empty_candidates(self) -> None:
        from ddharmon.models.enums import UnmappedReason
        from ddharmon.models.mapping import UnmappedField

        uf = UnmappedField(variable_name="x", reason=UnmappedReason.NO_CANDIDATES)
        assert uf.rejected_candidates == []


class TestMappingResult:
    """Tests for MappingResult dataclass."""

    def _make_result(self) -> "MappingResult":  # noqa: F821
        from ddharmon.models.enums import Relation, ReviewStatus
        from ddharmon.models.mapping import FieldMapping, MappingResult

        mappings = [
            FieldMapping(
                source_variable="age",
                target_variable="age_years",
                relation=Relation.EXACT,
                confidence=0.95,
                cosine_similarity=0.9,
                llm_confidence=0.98,
                rationale="Exact match",
                review_status=ReviewStatus.AUTO_APPROVED,
            ),
            FieldMapping(
                source_variable="bmi",
                target_variable="body_mass_index",
                relation=Relation.EXACT,
                confidence=0.7,
                cosine_similarity=0.65,
                llm_confidence=0.75,
                rationale="Same concept",
                review_status=ReviewStatus.PENDING_REVIEW,
            ),
            FieldMapping(
                source_variable="smoke",
                target_variable="smoking_status",
                relation=Relation.BROADER,
                confidence=0.2,
                cosine_similarity=0.25,
                llm_confidence=0.15,
                rationale="Weak match",
                review_status=ReviewStatus.AUTO_REJECTED,
            ),
        ]
        return MappingResult(
            source_name="cohort_a",
            target_name="cohort_b",
            mappings=mappings,
            source_unmapped=[],
            target_unmapped=[],
        )

    def test_auto_approved_filters(self) -> None:
        result = self._make_result()
        approved = result.auto_approved
        assert len(approved) == 1
        assert approved[0].source_variable == "age"

    def test_pending_review_filters(self) -> None:
        result = self._make_result()
        pending = result.pending_review
        assert len(pending) == 1
        assert pending[0].source_variable == "bmi"

    def test_auto_rejected_filters(self) -> None:
        result = self._make_result()
        rejected = result.auto_rejected
        assert len(rejected) == 1
        assert rejected[0].source_variable == "smoke"
