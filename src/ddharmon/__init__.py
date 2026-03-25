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

    # Async (in an async context)
    async with BioMapperClient() as client:
        result = await client.map_entity("L-Histidine")
"""

from ddharmon.client import BioMapperClient
from ddharmon.exceptions import (
    BioMapperAuthError,
    BioMapperConfigError,
    BioMapperError,
    BioMapperRateLimitError,
    BioMapperServerError,
    BioMapperTimeoutError,
)
from ddharmon.mapper import map_entities, map_entity, summarize
from ddharmon.models import MappingResult, MappingSummary

__version__ = "0.2.0"

__all__ = [
    # Client
    "BioMapperClient",
    # Sync helpers
    "map_entity",
    "map_entities",
    "summarize",
    # Models
    "MappingResult",
    "MappingSummary",
    # Exceptions
    "BioMapperError",
    "BioMapperAuthError",
    "BioMapperConfigError",
    "BioMapperRateLimitError",
    "BioMapperServerError",
    "BioMapperTimeoutError",
]
