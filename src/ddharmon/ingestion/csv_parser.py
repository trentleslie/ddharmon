"""Generic CSV parser for data dictionary ingestion.

Uses explicit column-to-role mappings to construct Field and DataDictionary
objects, extracts SNOMED codes from pipe-delimited format, and applies
hierarchy detection.
"""

from __future__ import annotations

import csv as csv_module
import logging
from pathlib import Path

import pandas as pd

from ddharmon.ingestion.hierarchy import detect_hierarchy
from ddharmon.models.data_dictionary import DataDictionary, Field
from ddharmon.models.enums import FieldRole
from ddharmon.values.response_parser import parse_value_encoding

logger = logging.getLogger(__name__)


class GenericCSVParser:
    """Parses CSV data dictionaries using explicit column-to-role mappings.

    Handles multiple cohort formats by accepting a column_map that specifies
    which columns correspond to variable names, descriptions, data types, etc.
    """

    def load(
        self,
        path: Path | str,
        cohort_name: str | None = None,
        column_map: dict[str, FieldRole] | None = None,
    ) -> DataDictionary:
        """Load a CSV data dictionary and return a structured DataDictionary.

        Steps:
        1. Read CSV with pandas (dtype=str, robust NA handling).
        2. Map columns to roles using the provided column_map.
        3. Construct Field objects from mapped roles.
        4. Extract SNOMED codes from standard_code columns.
        5. Apply hierarchy detection.
        Args:
            path: Path to the CSV or TSV file.
            cohort_name: Optional cohort/study name (e.g., "study_a").
            column_map: Dict mapping column name -> FieldRole. Only mapped
                        columns are used; unmapped columns go to Field.metadata.

        Returns:
            DataDictionary with all fields populated.
        """
        path = Path(path)
        if column_map is None:
            column_map = {}

        # Step 1: Read CSV with auto-detected delimiter
        delimiter = self._detect_delimiter(path)
        df = pd.read_csv(
            path,
            sep=delimiter,
            dtype=str,
            na_values=["NA", "na", "N/A", "n/a", ""],
            keep_default_na=True,
            on_bad_lines="warn",
        )

        # Normalize column names (strip whitespace)
        df.columns = pd.Index([c.strip() for c in df.columns])

        # Validate that all mapped columns exist in the file
        if column_map:
            actual_cols = set(df.columns)
            missing = [col for col in column_map if col not in actual_cols]
            if missing:
                raise ValueError(
                    f"Column(s) not found in {path.name}: {missing}\n"
                    f"Available columns: {list(df.columns)}"
                )

        # Step 2: Build role -> column_name(s) lookup from column_map
        role_columns = self._build_role_lookup(column_map)
        mapped_cols = set(column_map.keys())

        logger.info("Column role mapping: %s", {col: role.value for col, role in column_map.items()})

        # Step 3: Construct Fields
        var_name_col = role_columns.get(FieldRole.VARIABLE_NAME)
        field_id_col = role_columns.get(FieldRole.FIELD_ID)
        desc_col = role_columns.get(FieldRole.DESCRIPTION)
        short_label_col = role_columns.get(FieldRole.SHORT_LABEL)
        data_type_col = role_columns.get(FieldRole.DATA_TYPE)
        units_col = role_columns.get(FieldRole.UNITS)
        question_text_col = role_columns.get(FieldRole.QUESTION_TEXT)
        validation_col = role_columns.get(FieldRole.VALIDATION)
        coding_id_col = role_columns.get(FieldRole.CODING_ID)
        value_encoding_col = role_columns.get(FieldRole.VALUE_ENCODING)

        category_cols = role_columns.get(FieldRole.CATEGORY, [])
        if isinstance(category_cols, str):
            category_cols = [category_cols]

        standard_code_cols = role_columns.get(FieldRole.STANDARD_CODE, [])
        if isinstance(standard_code_cols, str):
            standard_code_cols = [standard_code_cols]

        fields: list[Field] = []
        for idx, row in df.iterrows():
            # Get variable name (fall back to field_id, then row number)
            var_name = self._get_cell_value(row, var_name_col)
            if var_name is None:
                var_name = self._get_cell_value(row, field_id_col)
            if var_name is None:
                var_name = f"_ROW_{int(idx):05d}"  # type: ignore[arg-type]

            # Get description (fall back to short_label, then variable_name)
            description = self._get_cell_value(row, desc_col)
            if description is None:
                description = self._get_cell_value(row, short_label_col)
            if description is None:
                description = var_name if var_name and not var_name.startswith("_ROW_") else None
            if var_name is None and description is None:
                continue

            # Get optional fields
            field_id = self._get_cell_value(row, field_id_col) if field_id_col else None
            short_label = self._get_cell_value(row, short_label_col) if short_label_col else None
            data_type = self._get_cell_value(row, data_type_col)
            units = self._get_cell_value(row, units_col) if units_col else None
            question_text = self._get_cell_value(row, question_text_col) if question_text_col else None
            validation = self._get_cell_value(row, validation_col) if validation_col else None
            coding_id = self._get_cell_value(row, coding_id_col) if coding_id_col else None
            value_encoding_raw = self._get_cell_value(row, value_encoding_col) if value_encoding_col else None

            # Category: use first non-null value from category columns
            category = None
            for cat_col in category_cols:
                category = self._get_cell_value(row, cat_col)
                if category is not None:
                    break

            # Extract standard codes (SNOMED)
            standard_codes = self._extract_snomed_codes(row, standard_code_cols)

            # Collect unmapped columns into metadata
            metadata: dict[str, str] = {}
            for col in df.columns:
                if col not in mapped_cols:
                    val = self._get_cell_value(row, col)
                    if val is not None:
                        metadata[col] = val

            field_obj = Field(
                variable_name=var_name,
                description=description,
                field_id=field_id,
                short_label=short_label,
                data_type=data_type,
                units=units,
                category=category,
                coding_id=coding_id,
                value_encoding_raw=value_encoding_raw,
                standard_codes=standard_codes,
                question_text=question_text,
                validation=validation,
                metadata=metadata,
            )

            # Parse value_encoding_raw into response_options
            if value_encoding_raw and not field_obj.response_options:
                field_obj.response_options = parse_value_encoding(value_encoding_raw)

            fields.append(field_obj)

        logger.info("Parsed %d fields from %d CSV rows", len(fields), len(df))

        # Step 4: Apply hierarchy detection
        fields = detect_hierarchy(fields)

        # Step 5: Build DataDictionary
        field_dict = {f.variable_name: f for f in fields}

        return DataDictionary(
            name=path.stem,
            fields=field_dict,
            source_path=path,
            cohort_name=cohort_name,
        )

    @staticmethod
    def _build_role_lookup(column_map: dict[str, FieldRole]) -> dict[FieldRole, str | list[str]]:
        """Build a role -> column_name(s) lookup from column_map.

        For multi-column roles (STANDARD_CODE, CATEGORY), returns a list of column names.
        For all other roles, returns a single column name string.
        """
        multi_roles = {FieldRole.STANDARD_CODE, FieldRole.CATEGORY}
        role_lookup: dict[FieldRole, str | list[str]] = {}
        multi_collect: dict[FieldRole, list[str]] = {r: [] for r in multi_roles}

        for col_name, role in column_map.items():
            if role in multi_roles:
                multi_collect[role].append(col_name)
            else:
                role_lookup[role] = col_name

        for role, cols in multi_collect.items():
            if cols:
                role_lookup[role] = cols

        return role_lookup

    @staticmethod
    def _detect_delimiter(path: Path) -> str:
        """Auto-detect file delimiter from extension, falling back to csv.Sniffer.

        Strategy:
        1. .tsv extension -> tab
        2. .csv extension -> comma
        3. Other -> read first 8KB and use csv.Sniffer to detect
        4. Fallback -> comma
        """
        suffix = path.suffix.lower()
        if suffix == ".tsv":
            return "\t"
        if suffix == ".csv":
            return ","
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                sample = f.read(8192)
            dialect = csv_module.Sniffer().sniff(sample, delimiters=",\t|;")
            return dialect.delimiter
        except csv_module.Error:
            return ","

    @staticmethod
    def _get_cell_value(row: pd.Series, column: str | list[str] | None) -> str | None:  # type: ignore[type-arg]
        """Get a string cell value from a row, returning None for NaN/empty."""
        if column is None:
            return None
        if isinstance(column, list):
            for col in column:
                val = row.get(col)
                if val is not None and pd.notna(val) and str(val).strip():
                    return str(val).strip()
            return None
        val = row.get(column)
        if val is None or pd.isna(val):
            return None
        val_str = str(val).strip()
        return val_str if val_str else None

    @staticmethod
    def _extract_snomed_codes(
        row: pd.Series, standard_code_columns: list[str]  # type: ignore[type-arg]
    ) -> dict[str, list[str]]:
        """Extract SNOMED codes from pipe-delimited standard code columns.

        Pipe-delimited format: "1002561000000109 | Serum cystatin C level (observable entity) |"
        Extracts the numeric code before the first pipe.
        """
        codes: list[str] = []

        for col in standard_code_columns:
            val = row.get(col)
            if val is None or pd.isna(val):
                continue
            val_str = str(val).strip()
            if not val_str:
                continue

            parts = val_str.split("|")
            if len(parts) >= 2:
                code = parts[0].strip()
                if code and code.isdigit():
                    codes.append(code)

        seen: set[str] = set()
        unique_codes: list[str] = []
        for code in codes:
            if code not in seen:
                seen.add(code)
                unique_codes.append(code)

        if unique_codes:
            return {"SNOMED": unique_codes}
        return {}
