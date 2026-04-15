"""ddharmon — Python client for the BioMapper2 API.

Quick start::

    from ddharmon import map_entity, map_entities, BioMapperClient

    # Single lookup (synchronous)
    result = map_entity("L-Histidine")
    print(result.primary_curie)      # RM:0129894
    print(result.confidence_tier)    # high

    # Batch (synchronous, with progress bar)
    results = map_entities(
        [{"name": "L-Histidine"}, {"name": "Glucose"}],
        progress=True,
    )

    # File-based (synchronous, with progress bar)
    from pathlib import Path
    from ddharmon import map_dataset_file_sync

    result = map_dataset_file_sync(
        Path("compounds.tsv"),
        name_column="name",
        provided_id_columns=["hmdb_id"],
        progress=True,
    )
    result.raise_for_error()  # opt-in: raise if the stream truncated
    print(f"resolved {sum(1 for r in result.results if r.resolved)}")

    # Async (in an async context)
    async with BioMapperClient() as client:
        result = await client.map_entity("L-Histidine")
"""

from ddharmon.client import BioMapperClient
from ddharmon.dataset import map_dataset_file_sync
from ddharmon.exceptions import (
    BioMapperAuthError,
    BioMapperConfigError,
    BioMapperError,
    BioMapperRateLimitError,
    BioMapperServerError,
    BioMapperTimeoutError,
)
from ddharmon.mapper import (
    list_annotators,
    list_entity_types,
    list_vocabularies,
    map_entities,
    map_entity,
    summarize,
)
from ddharmon.models import (
    AnnotatorInfo,
    DatasetMappingResult,
    EntityTypeInfo,
    MappingResult,
    MappingSummary,
    VocabularyInfo,
)

__version__ = "0.4.0"

__all__ = [
    # Client
    "BioMapperClient",
    # Sync helpers
    "map_entity",
    "map_entities",
    "map_dataset_file_sync",
    "list_entity_types",
    "list_annotators",
    "list_vocabularies",
    "summarize",
    # Models
    "MappingResult",
    "MappingSummary",
    "DatasetMappingResult",
    "EntityTypeInfo",
    "AnnotatorInfo",
    "VocabularyInfo",
    # Exceptions
    "BioMapperError",
    "BioMapperAuthError",
    "BioMapperConfigError",
    "BioMapperRateLimitError",
    "BioMapperServerError",
    "BioMapperTimeoutError",
]
