"""Schema registry for automatic column role detection in CSV data dictionaries.

Detects which CSV columns correspond to variable names, descriptions, data types,
sections, standard codes, etc. using scored heuristic matching.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from ddharmon.models.enums import FieldRole

logger = logging.getLogger(__name__)


@dataclass
class ColumnRoleMatch:
    """A matched column-to-role assignment with confidence and explanation."""

    column_name: str
    role: FieldRole
    confidence: float  # 0.0 - 1.0
    match_reason: str


@dataclass
class SchemaMapping:
    """Complete column-to-role mapping result for a set of CSV columns.

    Holds all matched columns, unmatched columns, and an overall confidence score.
    """

    role_map: dict[str, ColumnRoleMatch]  # column_name -> match
    unmatched_columns: list[str] = field(default_factory=list)
    overall_confidence: float = 0.0


# Heuristic patterns per role: (normalized_pattern, score)
# Matching is substring-based: if the pattern appears in the normalized column name,
# the score is a candidate. The highest-scoring role wins.
ROLE_PATTERNS: dict[FieldRole, list[tuple[str, float]]] = {
    FieldRole.VARIABLE_NAME: [
        ("variable_name", 1.0),
        ("column_name", 0.95),  # e.g. "Column Name"
        ("field_name", 0.95),  # e.g. "field_name"
        ("historical_id", 0.9),
        ("var_name", 0.9),
        ("name", 0.6),
    ],
    FieldRole.FIELD_ID: [
        ("field_id", 1.0),
        ("fieldid", 1.0),
        ("dataset_id", 0.9),
        ("variable_id", 0.9),
        ("cde_id", 0.95),
        ("id", 0.4),
    ],
    FieldRole.DESCRIPTION: [
        ("description", 1.0),
        ("phenotype_description", 1.0),
        ("field_description", 0.95),
        ("definition", 0.9),
        ("label", 0.7),
        ("field", 0.6),
    ],
    FieldRole.SHORT_LABEL: [
        ("short_label", 1.0),
        ("field_string", 0.9),  # HPP "field_string"
        ("field_label", 0.9),
        ("display_name", 0.9),
        ("title", 0.6),
    ],
    FieldRole.DATA_TYPE: [
        ("data_type", 1.0),
        ("valuetype", 1.0),
        ("value_type", 1.0),
        ("field_type", 0.95),  # e.g. "field_type"
        ("variable_type", 0.9),  # e.g. "Variable Type"
        ("data_type_pandas", 0.8),  # e.g. "data_type_pandas"
        ("type", 0.7),
        ("itemtype", 0.7),
    ],
    FieldRole.UNITS: [
        ("units", 1.0),
        ("unit_of_measure", 1.0),
        ("unit", 0.9),
        ("measurement_unit", 0.9),
        ("uom", 0.8),
    ],
    FieldRole.CATEGORY: [
        ("category", 1.0),
        ("section", 0.95),
        ("domain", 0.8),
        ("form_name", 0.85),
        ("parent_category", 0.8),
        ("subcategory", 0.7),
        ("path", 0.6),
        ("group", 0.5),
        # A "Data_Type" column as category is handled via cohort hint, not pattern
    ],
    FieldRole.VALUE_ENCODING: [
        ("value_encoding", 1.0),
        ("data_coding", 0.95),  # e.g. "data_coding"
        ("permissible_values", 1.0),
        ("allowed_values", 0.95),
        ("choices", 0.9),  # REDCap "Choices, Calculations, OR Slider Labels"
        ("response_options", 1.0),
        ("options", 0.7),
        ("values", 0.6),
        ("codelist", 0.8),
    ],
    FieldRole.STANDARD_CODE: [
        ("snomed", 0.95),
        ("loinc", 0.95),
        ("omop", 0.95),
        ("concept_id", 0.9),
        ("coding", 0.5),
    ],
    FieldRole.PARENT_ID: [
        ("parent_id", 1.0),
        ("parent_field", 0.9),
        ("parent", 0.6),
    ],
    FieldRole.QUESTION_TEXT: [
        ("question_text", 1.0),
        ("question", 0.8),
        ("prompt", 0.7),
        ("field_note", 0.6),
    ],
    FieldRole.VALIDATION: [
        ("validation", 1.0),
        ("text_validation", 0.95),
        ("input_restriction", 0.9),
        ("min_value", 0.7),
        ("max_value", 0.7),
        ("format", 0.5),
    ],
}


class SchemaRegistry:
    """Detects column roles in CSV data dictionaries using scored heuristics.

    Supports multiple column naming conventions across common data-dictionary and
    registry CSV/TSV formats (including REDCap). Uses substring matching against known
    patterns with context-aware disambiguation when multiple columns compete for the same role.
    """

    def __init__(self, patterns: dict[FieldRole, list[tuple[str, float]]] | None = None) -> None:
        """Initialize with optional custom patterns (defaults to ROLE_PATTERNS)."""
        self.patterns = patterns if patterns is not None else ROLE_PATTERNS

    def detect_roles(
        self,
        column_names: list[str],
        hints: dict[str, FieldRole] | None = None,
    ) -> SchemaMapping:
        """Detect column roles from a list of column names.

        Args:
            column_names: List of CSV column header names.
            hints: Optional dict mapping column name -> forced FieldRole.
                   Hints are case-insensitive. Overrides heuristic detection.

        Returns:
            SchemaMapping with role assignments, unmatched columns, and
            overall confidence score.
        """
        if hints is None:
            hints = {}

        # Build case-insensitive hints lookup
        hints_lower = {k.lower(): v for k, v in hints.items()}

        # Normalize column names for matching (preserve originals for output)
        normalized = {col: self._normalize(col) for col in column_names}

        # Phase 1: Score every column against every role pattern
        # scores[column_name] = list of (role, score, pattern_matched)
        scores: dict[str, list[tuple[FieldRole, float, str]]] = {}
        for col in column_names:
            norm = normalized[col]

            # Check if hint overrides this column
            if norm in hints_lower:
                scores[col] = [(hints_lower[norm], 1.0, "hint override")]
                continue

            col_scores: list[tuple[FieldRole, float, str]] = []
            for role, patterns in self.patterns.items():
                best_score = 0.0
                best_pattern = ""
                for pattern, score in patterns:
                    if pattern in norm:
                        # Prefer exact matches: if the normalized column IS the pattern, boost score.
                        # Allow scores > 1.0 for sorting; they represent higher confidence.
                        actual_score = score
                        if norm == pattern:
                            actual_score = score + 0.1
                        if actual_score > best_score:
                            best_score = actual_score
                            best_pattern = pattern
                if best_score > 0:
                    col_scores.append((role, best_score, best_pattern))
            scores[col] = col_scores

        # Phase 2: Assign roles using greedy best-match with disambiguation
        role_map: dict[str, ColumnRoleMatch] = {}
        unmatched: list[str] = []

        # Roles that should be unique (only one column per role, except STANDARD_CODE and CATEGORY)
        unique_roles = {
            FieldRole.VARIABLE_NAME,
            FieldRole.FIELD_ID,
            FieldRole.DESCRIPTION,
            FieldRole.SHORT_LABEL,
            FieldRole.DATA_TYPE,
            FieldRole.UNITS,
            FieldRole.PARENT_ID,
            FieldRole.QUESTION_TEXT,
            FieldRole.VALIDATION,
        }

        # Collect all (column, role, score, pattern) candidates
        candidates: list[tuple[str, FieldRole, float, str]] = []
        for col, col_scores in scores.items():
            for role, score, pattern in col_scores:
                candidates.append((col, role, score, pattern))

        # Sort by score descending (highest confidence assignments first)
        candidates.sort(key=lambda x: x[2], reverse=True)

        # Track assigned columns and filled unique roles
        assigned_columns: set[str] = set()
        filled_unique_roles: dict[FieldRole, str] = {}  # role -> assigned column

        for col, role, score, pattern in candidates:
            if col in assigned_columns:
                continue

            # For unique roles, check if already filled by a higher-confidence match
            if role in unique_roles and role in filled_unique_roles:
                continue

            # Assign this column to this role
            # Cap confidence at 1.0 for output (internal scores > 1.0 are used for sorting only)
            role_map[col] = ColumnRoleMatch(
                column_name=col,
                role=role,
                confidence=min(score, 1.0),
                match_reason=f"matched pattern: {pattern}" if pattern != "hint override" else "hint override",
            )
            assigned_columns.add(col)
            if role in unique_roles:
                filled_unique_roles[role] = col

        # Collect unmatched columns
        for col in column_names:
            if col not in assigned_columns:
                unmatched.append(col)

        # Calculate overall confidence
        overall = sum(m.confidence for m in role_map.values()) / len(role_map) if role_map else 0.0

        return SchemaMapping(
            role_map=role_map,
            unmatched_columns=unmatched,
            overall_confidence=overall,
        )

    @staticmethod
    def _normalize(column_name: str) -> str:
        """Normalize a column name for pattern matching.

        Lowercases, strips whitespace, replaces spaces with underscores.
        """
        return column_name.strip().lower().replace(" ", "_")
