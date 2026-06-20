"""Enumeration types for ddharmon data models."""

from __future__ import annotations

from enum import StrEnum


class FieldRole(StrEnum):
    """Semantic role of a CSV column in a data dictionary.

    Used by SchemaRegistry to map variant column names to standard roles.
    Covers NIH CDE, REDCap, CDISC, and common biomedical data dictionary formats.

    Core roles (essential for harmonization):
        VARIABLE_NAME   Primary human-readable field name (e.g., "bmi", "age_at_enrollment")
        FIELD_ID        Numeric/alphanumeric ID when distinct from name (e.g., UKBB "21001")
        DESCRIPTION     Full text definition of the field
        SHORT_LABEL     Brief display label when distinct from full description

    Typing & measurement:
        DATA_TYPE       Variable type (categorical, continuous, text, date, integer...)
        UNITS           Unit of measure (kg, mmHg, mg/dL, years)

    Organization:
        CATEGORY        Domain, section, form, or grouping (e.g., "Demographics", "Diet")

    Values & coding:
        CODING_ID       Opaque reference to an external codebook (e.g., UKBB "Coding 1002", HPP "053_01")
        VALUE_ENCODING  Inline permissible values that can be parsed (e.g., "1=Yes|2=No")
        STANDARD_CODE   Ontology codes (SNOMED, LOINC, OMOP concept IDs)

    Structure:
        PARENT_ID       Parent field for hierarchical relationships

    Context:
        QUESTION_TEXT   The actual question asked, distinct from the definition
        VALIDATION      Input constraints (min, max, format, regex)
    """

    # Core
    VARIABLE_NAME = "variable_name"
    FIELD_ID = "field_id"
    DESCRIPTION = "description"
    SHORT_LABEL = "short_label"

    # Typing & measurement
    DATA_TYPE = "data_type"
    UNITS = "units"

    # Organization
    CATEGORY = "category"

    # Values & coding
    CODING_ID = "coding_id"
    VALUE_ENCODING = "value_encoding"
    STANDARD_CODE = "standard_code"

    # Structure
    PARENT_ID = "parent_id"

    # Context
    QUESTION_TEXT = "question_text"
    VALIDATION = "validation"


class Relation(StrEnum):
    """Semantic relationship between a source and target field.

    Used by the LLM reranker to classify candidate matches.
    """

    EXACT = "exact"
    BROADER = "broader"
    NARROWER = "narrower"
    COMPOSITE = "composite"
    DERIVABLE = "derivable"
    NO_MATCH = "no_match"


class ReviewStatus(StrEnum):
    """Triage status for a field mapping based on confidence thresholds."""

    AUTO_APPROVED = "auto_approved"
    PENDING_REVIEW = "pending_review"
    AUTO_REJECTED = "auto_rejected"


class UnmappedReason(StrEnum):
    """Reason a field could not be mapped."""

    NO_CANDIDATES = "no_candidates"
    LLM_REJECTED_ALL = "llm_rejected_all"
    DOMAIN_MISMATCH = "domain_mismatch"
