"""Metabolon-specific utilities for ddharmon.

Provides preprocessing and export helpers for Metabolon metabolomics data.
Requires ``ddharmon[metabolon]`` (pandas, openpyxl).
"""

from ddharmon.extras.metabolon.export import (
    flatten_results,
    results_to_dataframe,
    save_results,
)
from ddharmon.extras.metabolon.preprocessing import (
    MetabolonRecord,
    build_mapping_queue,
    clean_compound_name,
    extract_hmdb_id,
)

__all__ = [
    # Preprocessing
    "clean_compound_name",
    "extract_hmdb_id",
    "MetabolonRecord",
    "build_mapping_queue",
    # Export
    "flatten_results",
    "results_to_dataframe",
    "save_results",
]
