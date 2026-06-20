"""Unit tests for ddharmon schema registry column role detection."""

from __future__ import annotations


class TestColumnRoleMatch:
    """Tests for ColumnRoleMatch dataclass."""

    def test_basic_creation(self) -> None:
        from ddharmon.ingestion.schema_registry import ColumnRoleMatch
        from ddharmon.models.enums import FieldRole

        match = ColumnRoleMatch(
            column_name="variable_name",
            role=FieldRole.VARIABLE_NAME,
            confidence=1.0,
            match_reason="exact match: variable_name",
        )
        assert match.column_name == "variable_name"
        assert match.role == FieldRole.VARIABLE_NAME
        assert match.confidence == 1.0
        assert match.match_reason == "exact match: variable_name"


class TestSchemaMapping:
    """Tests for SchemaMapping dataclass."""

    def test_basic_creation(self) -> None:
        from ddharmon.ingestion.schema_registry import ColumnRoleMatch, SchemaMapping
        from ddharmon.models.enums import FieldRole

        role_map = {
            "variable_name": ColumnRoleMatch(
                column_name="variable_name",
                role=FieldRole.VARIABLE_NAME,
                confidence=1.0,
                match_reason="exact match",
            )
        }
        mapping = SchemaMapping(
            role_map=role_map,
            unmatched_columns=["extra_col"],
            overall_confidence=0.9,
        )
        assert "variable_name" in mapping.role_map
        assert mapping.unmatched_columns == ["extra_col"]
        assert mapping.overall_confidence == 0.9


class TestSchemaRegistryGeneric:
    """Tests for generic column name detection."""

    def test_exact_variable_name(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(["variable_name", "description"])
        assert result.role_map["variable_name"].role == FieldRole.VARIABLE_NAME
        assert result.role_map["variable_name"].confidence == 1.0

    def test_exact_description(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(["variable_name", "description"])
        assert result.role_map["description"].role == FieldRole.DESCRIPTION
        assert result.role_map["description"].confidence == 1.0

    def test_case_insensitive(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(["Variable_Name", "Description"])
        assert result.role_map["Variable_Name"].role == FieldRole.VARIABLE_NAME
        assert result.role_map["Description"].role == FieldRole.DESCRIPTION

    def test_unmatched_columns_tracked(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry

        registry = SchemaRegistry()
        result = registry.detect_roles(["variable_name", "description", "zzz_random_col"])
        assert "zzz_random_col" in result.unmatched_columns

    def test_overall_confidence(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry

        registry = SchemaRegistry()
        result = registry.detect_roles(["variable_name", "description"])
        # Both columns have 1.0 confidence, overall should be high
        assert result.overall_confidence >= 0.9


class TestSchemaRegistryStudyColumns:
    """Tests for study-style column format detection.

    Columns: Data_Type, Historical_ID, Phenotype_Description,
    snomed_term_1, snomed_term_2, snomed_term_3, snomed_term_4
    """

    def _get_study_columns(self) -> list[str]:
        return [
            "Data_Type",
            "Historical_ID",
            "Phenotype_Description",
            "snomed_term_1",
            "snomed_term_2",
            "snomed_term_3",
            "snomed_term_4",
        ]

    def test_historical_id_is_variable_name(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(self._get_study_columns())
        assert result.role_map["Historical_ID"].role == FieldRole.VARIABLE_NAME

    def test_phenotype_description_is_description(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(self._get_study_columns())
        assert result.role_map["Phenotype_Description"].role == FieldRole.DESCRIPTION

    def test_snomed_columns_are_standard_code(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(self._get_study_columns())
        for col in ["snomed_term_1", "snomed_term_2", "snomed_term_3", "snomed_term_4"]:
            assert result.role_map[col].role == FieldRole.STANDARD_CODE, f"{col} should be STANDARD_CODE"

    def test_data_type_detection(self) -> None:
        """Data_Type in this context -- schema registry should detect it.

        The registry returns its best-guess (DATA_TYPE or SECTION); callers
        use hints to override when they know it's actually a section.
        """
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(self._get_study_columns())
        # Data_Type matches DATA_TYPE pattern with score 1.0
        # It also matches SECTION pattern with score 0.7
        # The registry picks the highest score
        assert result.role_map["Data_Type"].role == FieldRole.DATA_TYPE

    def test_data_type_override_with_hints(self) -> None:
        """Callers can override Data_Type -> SECTION using hints."""
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        hints = {"Data_Type": FieldRole.CATEGORY}
        result = registry.detect_roles(self._get_study_columns(), hints=hints)
        assert result.role_map["Data_Type"].role == FieldRole.CATEGORY

    def test_multiple_standard_code_columns(self) -> None:
        """Multiple columns can have the same STANDARD_CODE role."""
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(self._get_study_columns())
        standard_code_cols = [name for name, match in result.role_map.items() if match.role == FieldRole.STANDARD_CODE]
        assert len(standard_code_cols) >= 4


class TestSchemaRegistryUKBB:
    """Tests for UKBB-style column format detection.

    UKBB columns: FieldID, Field, ValueType, Category
    """

    def _get_ukbb_columns(self) -> list[str]:
        return ["FieldID", "Field", "ValueType", "Category"]

    def test_fieldid_is_field_id(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(self._get_ukbb_columns())
        assert result.role_map["FieldID"].role == FieldRole.FIELD_ID

    def test_field_is_description(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(self._get_ukbb_columns())
        assert result.role_map["Field"].role == FieldRole.DESCRIPTION

    def test_valuetype_is_data_type(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(self._get_ukbb_columns())
        assert result.role_map["ValueType"].role == FieldRole.DATA_TYPE

    def test_category_is_section(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(self._get_ukbb_columns())
        assert result.role_map["Category"].role == FieldRole.CATEGORY


class TestSchemaRegistryAmbiguous:
    """Tests for ambiguous column name detection."""

    def test_field_alone_is_description_lower_confidence(self) -> None:
        """When 'Field' appears alone (no 'FieldID'), it maps to DESCRIPTION at lower confidence."""
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(["Field", "Category"])
        assert result.role_map["Field"].role == FieldRole.DESCRIPTION
        # Confidence should be lower than exact match (0.6 for "field" pattern)
        assert result.role_map["Field"].confidence < 1.0

    def test_disambiguation_fieldid_and_field(self) -> None:
        """When both 'FieldID' and 'Field' exist, FieldID gets FIELD_ID, Field gets DESCRIPTION."""
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        result = registry.detect_roles(["FieldID", "Field", "ValueType"])
        assert result.role_map["FieldID"].role == FieldRole.FIELD_ID
        assert result.role_map["Field"].role == FieldRole.DESCRIPTION


class TestSchemaRegistryHints:
    """Tests for hints override functionality."""

    def test_hints_override_detection(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        # Force "Field" to be VARIABLE_NAME instead of DESCRIPTION
        hints = {"Field": FieldRole.VARIABLE_NAME}
        result = registry.detect_roles(["Field", "Category"], hints=hints)
        assert result.role_map["Field"].role == FieldRole.VARIABLE_NAME

    def test_hints_case_insensitive(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        # Hint key is case-insensitive match against column names
        hints = {"field": FieldRole.VARIABLE_NAME}
        result = registry.detect_roles(["Field", "Category"], hints=hints)
        assert result.role_map["Field"].role == FieldRole.VARIABLE_NAME

    def test_hints_high_confidence(self) -> None:
        from ddharmon.ingestion.schema_registry import SchemaRegistry
        from ddharmon.models.enums import FieldRole

        registry = SchemaRegistry()
        hints = {"Field": FieldRole.VARIABLE_NAME}
        result = registry.detect_roles(["Field", "Category"], hints=hints)
        assert result.role_map["Field"].confidence == 1.0


class TestSchemaRegistryDirectImport:
    """Tests that schema_registry module types are importable directly."""

    def test_all_types_importable(self) -> None:
        from ddharmon.ingestion.schema_registry import ColumnRoleMatch, SchemaMapping, SchemaRegistry

        assert SchemaRegistry is not None
        assert SchemaMapping is not None
        assert ColumnRoleMatch is not None
