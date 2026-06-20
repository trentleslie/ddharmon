"""Ingestion module for ddharmon data dictionary loading.

Re-exports public types for convenient importing:
    from ddharmon.ingestion import GenericCSVParser, detect_hierarchy, load_dictionary
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from ddharmon.ingestion.csv_parser import GenericCSVParser
from ddharmon.ingestion.hierarchy import detect_hierarchy
from ddharmon.ingestion.preprocessor import PreprocessingReport, preprocessing_diff, preprocess_dictionary
from ddharmon.models.data_dictionary import DataDictionary
from ddharmon.models.enums import FieldRole

logger = logging.getLogger(__name__)

__all__ = [
    "GenericCSVParser",
    "detect_hierarchy",
    "load_dictionary",
    "preprocess_dictionary",
    "preprocessing_diff",
    "PreprocessingReport",
]


def load_dictionary(
    path: Path | str,
    cohort_name: str | None = None,
    *,
    variable_name: str | None = None,
    field_id: str | None = None,
    description: str | None = None,
    short_label: str | None = None,
    data_type: str | None = None,
    units: str | None = None,
    category: str | None = None,
    coding_id: str | None = None,
    value_encoding: str | None = None,
    standard_code: str | None = None,
    question_text: str | None = None,
    validation: str | None = None,
    parent_id: str | None = None,
    embed_variable_name: bool = True,
) -> DataDictionary:
    """Load a CSV/TSV data dictionary into a structured DataDictionary.

    This is the primary entry point for ingestion. You must tell the loader
    which of your file's columns maps to which role using keyword arguments.

    Column Mapping — organized by which embedding vector they feed into:

        Semantic vector (question/description meaning):
            variable_name   Field name column [REQUIRED*]
                            (e.g., "bmi_calculated", "thebasics_birthplace")
            field_id        Numeric/alphanumeric ID column if separate from name [REQUIRED*]
                            (e.g., "20116", "LOINC:39156-5")
            description     Full text definition column
                            (e.g., "Body mass index calculated from height and weight")
            short_label     Brief display label if distinct from description
                            (e.g., "BMI", "Birth Country")
            question_text   Actual question wording column
                            (e.g., "In what country were you born?")
            category        Domain/section/assessment grouping
                            (e.g., "demographics", "the_basics", "diet")
            parent_id       Parent field column — parent description is prepended for context
                            (e.g., "thebasics", "assessment:diet")

            * At least one of variable_name, description, or question_text
              is required (bare minimum for semantic embedding).
            * field_id is optional — used as fallback key when variable_name
              is absent, but carries no semantic meaning on its own.

        Value vector (answer-pattern meaning):
            value_encoding  Inline response options column
                            (e.g., "1=Male|2=Female", "Yes/No", "Birthplace_USA, USA | PMI_Other, Other")
            data_type       Variable type column
                            (e.g., "Continuous", "Categorical", "radio", "int")
            units           Units of measurement column
                            (e.g., "kg", "mmHg", "years", "kg/m2")

            Value vectors are only generated when at least one of these is populated.
            Fields without value metadata get semantic-only matching (no penalty).

        Metadata (not embedded, used for provenance/review):
            coding_id       Opaque codebook reference column
                            (e.g., "1002", "053_01")
            standard_code   Ontology code column
                            (e.g., "SNOMED:60621009", "LOINC:39156-5")
            validation      Input constraints column
                            (e.g., "0-300", "date_ymd", "integer")

    Args:
        path: Path to the CSV or TSV file (delimiter auto-detected from extension).
        cohort_name: Optional cohort/study name (e.g., "study_a").
        embed_variable_name: Whether to include variable_name in semantic embedding
            text. Set to False when variable names are opaque IDs (e.g., row numbers,
            internal codes) that would add noise to the embedding. Default: True.

    Returns:
        DataDictionary with all fields populated, hierarchies linked, and
        encodings classified.

    Raises:
        ValueError: If none of variable_name, description, or question_text is provided
            (at least one is needed for semantic embedding).

    Examples:
        # Explicit mapping (recommended):
        dd = load_dictionary(
            "data/questionnaire.tsv",
            variable_name="Column Name",
            description="Description",
            category="Category",
            units="Units",
            data_type="Variable Type",
        )

        # Minimal mapping:
        dd = load_dictionary(
            "my_data_dict.csv",
            variable_name="var_id",
            description="var_definition",
        )
    """
    if variable_name is None and description is None and question_text is None:
        raise ValueError(
            "At least one of variable_name=, description=, or question_text= must be provided.\n"
            "These provide the semantic text needed for embedding and matching.\n"
            "A field_id alone (e.g., '20116') has no semantic meaning to embed.\n\n"
            "Example:\n"
            "  load_dictionary('file.csv', variable_name='your_column_name', description='your_desc_column')"
        )

    # Build column -> FieldRole mapping from explicit kwargs
    hints: dict[str, FieldRole] = {}
    _kwarg_role_map: list[tuple[str | None, FieldRole]] = [
        (variable_name, FieldRole.VARIABLE_NAME),
        (field_id, FieldRole.FIELD_ID),
        (description, FieldRole.DESCRIPTION),
        (short_label, FieldRole.SHORT_LABEL),
        (data_type, FieldRole.DATA_TYPE),
        (units, FieldRole.UNITS),
        (category, FieldRole.CATEGORY),
        (coding_id, FieldRole.CODING_ID),
        (value_encoding, FieldRole.VALUE_ENCODING),
        (standard_code, FieldRole.STANDARD_CODE),
        (question_text, FieldRole.QUESTION_TEXT),
        (validation, FieldRole.VALIDATION),
        (parent_id, FieldRole.PARENT_ID),
    ]
    for col_name, role in _kwarg_role_map:
        if col_name is not None:
            hints[col_name] = role

    if description is None:
        logger.warning(
            "No description= column provided. Fields will use available text "
            "but harmonization quality may be degraded."
        )

    parser = GenericCSVParser()
    t0 = time.perf_counter()
    result = parser.load(path, cohort_name=cohort_name, column_map=hints)
    if not embed_variable_name:
        for fld in result.fields.values():
            fld._embed_variable_name = False
    elapsed = time.perf_counter() - t0
    logger.info("load_dictionary(%s): %d fields in %.2fs", Path(path).name, result.field_count, elapsed)
    return result
