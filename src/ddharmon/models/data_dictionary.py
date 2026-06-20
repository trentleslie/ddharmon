"""Core data models for ddharmon data dictionary harmonization.

Plain dataclasses (not Pydantic) with __post_init__ validation.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

@dataclass
class ResponseOption:
    """A single response option (code-label pair) for a field.

    Examples: ("1", "Yes"), ("0", "No"), ("3", "Sometimes").
    """

    code: str
    label: str
    order: int | None = None  # Ordinal position for ordered response sets


@dataclass
class ValueSet:
    """A named collection of response options.

    Reusable across fields that share the same response set
    (e.g., yes/no, Likert scales).
    """

    name: str
    options: list[ResponseOption] = field(default_factory=list)


@dataclass
class Field:
    """A single field (variable) in a data dictionary.

    Core unit of harmonization. Holds the variable name, description,
    hierarchy links, and standard code mappings.
    """

    # Core
    variable_name: str
    description: str
    field_id: str | None = None  # Numeric/alphanumeric ID when distinct from variable_name
    short_label: str | None = None  # Brief display label when distinct from description

    # Typing & measurement
    data_type: str | None = None  # Raw data type from source (e.g., "categorical", "continuous")
    units: str | None = None  # Unit of measure (e.g., "kg", "mmHg", "years")

    # Organization
    category: str | None = None  # Domain/section/form grouping (e.g., "Demographics", "Diet")

    # Values & coding
    coding_id: str | None = None  # Opaque reference to external codebook (e.g., "Coding 1002", "053_01")
    response_options: list[ResponseOption] = field(default_factory=list)
    value_encoding_raw: str | None = None  # Inline parseable values (e.g., "1=Yes|2=No")
    standard_codes: dict[str, list[str]] = field(default_factory=dict)  # {"SNOMED": ["1234", "5678"]}

    # Structure
    parent_field_id: str | None = None
    children: list[str] = field(default_factory=list)

    # Context
    question_text: str | None = None  # The actual question asked
    validation: str | None = None  # Input constraints (min, max, format)

    # Extra
    metadata: dict[str, str] = field(default_factory=dict)  # Unmapped columns stored here
    _synthetic: bool = field(default=False, repr=False)  # True for inferred parent fields
    _embed_variable_name: bool = field(default=True, repr=False)  # Include variable_name in embedding text

    # Raw (pre-preprocessing) text — populated by preprocess_dictionary()
    raw_variable_name: str | None = field(default=None, repr=False)
    raw_description: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.variable_name:
            raise ValueError("Field must have a variable_name")
        if not self.description:
            raise ValueError("Field must have a description")

    # Fields eligible for inclusion in embedding text (beyond variable_name + primary text)
    EMBEDDING_FIELDS: tuple[str, ...] = ("category",)

    def to_embedding_text(self, include: set[str] | None = None) -> str:
        """Compose rich text for semantic embedding generation.

        Primary text is question_text if populated, else description (fallback).
        variable_name is prepended unless _embed_variable_name is False or it
        duplicates the primary text. Category may be appended via include=.

        Response options, data_type, and units are intentionally omitted — they
        belong to the value vector (compose_value_text), not the semantic vector.

        Format: "variable_name | {question_text OR description} | Category: category"

        Args:
            include: Optional set of field names to include beyond variable_name
                and primary text. Valid values: "category". If None, all
                populated optional fields are included.
        """
        primary_text = self.question_text or self.description or ""

        # Drop variable_name if it duplicates primary_text (case/whitespace-insensitive).
        if self._embed_variable_name and self.variable_name.strip().lower() != primary_text.strip().lower():
            parts = [self.variable_name, primary_text]
        else:
            parts = [primary_text]

        if (include is None or "category" in include) and self.category:
            parts.append(f"Category: {self.category}")

        return " | ".join(parts)

    def content_hash(self) -> str:
        """Compute deterministic 16-char hex hash of embedding-relevant content.

        Used as cache key for embeddings. Changes when field content changes,
        triggering re-embedding.
        """
        content = self.to_embedding_text()
        return hashlib.sha256(content.encode()).hexdigest()[:16]


@dataclass
class DataDictionary:
    """A complete data dictionary loaded from a cohort CSV.

    Contains all fields keyed by variable_name, plus source metadata.
    """

    name: str
    fields: dict[str, Field]  # variable_name -> Field
    source_path: Path | None = None
    cohort_name: str | None = None
    date_loaded: datetime = field(default_factory=datetime.now)
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def field_count(self) -> int:
        """Total number of fields in the dictionary."""
        return len(self.fields)

    @property
    def synthetic_field_count(self) -> int:
        """Count of synthetic (inferred parent) fields."""
        return sum(1 for f in self.fields.values() if f._synthetic)

    def get_children(self, field_id: str) -> list[Field]:
        """Get child Field objects for a given parent field_id.

        Returns empty list if field_id not found or has no children.
        """
        parent = self.fields.get(field_id)
        if parent is None:
            return []
        return [self.fields[cid] for cid in parent.children if cid in self.fields]

    def get_root_fields(self) -> list[Field]:
        """Get all top-level fields (those with no parent_field_id)."""
        return [f for f in self.fields.values() if f.parent_field_id is None]
