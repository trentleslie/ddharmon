# ddharmon

Python client for the **BioMapper2 API** — map biological entity names to
standardized knowledge-graph identifiers (CHEBI, HMDB, PubChem, RefMet, and more).

```python
from ddharmon import map_entity

result = map_entity("L-Histidine")
print(result.primary_curie)     # RM:0129894
print(result.confidence_tier)   # high
print(result.ids_for("CHEBI"))  # ['15971']
```

---

## Installation

```bash
# Core (async HTTP client + Pydantic models)
pip install ddharmon

# With Metabolon preprocessing utilities (pandas, openpyxl)
pip install "ddharmon[metabolon]"

# With notebook progress bars (tqdm, nest-asyncio)
pip install "ddharmon[notebook]"

# Everything
pip install "ddharmon[all]"
```

Set your API key:

```bash
export BIOMAPPER_API_KEY=your-key-here
# or add to a .env file
```

---

## Quick start

### Single lookup (synchronous)

```python
from ddharmon import map_entity

result = map_entity("L-Histidine")

print(result.resolved)          # True
print(result.primary_curie)     # RM:0129894
print(result.chosen_kg_id)      # CHEBI:15971
print(result.confidence_score)  # 2.489
print(result.confidence_tier)   # high  (≥2.0)
print(result.ids_for("CHEBI"))  # ['15971']
print(result.ids_for("refmet_id"))  # ['RM0129894']
```

### Batch mapping (synchronous)

```python
from ddharmon import map_entities, summarize

records = [
    {"name": "L-Histidine"},
    {"name": "Glucose", "identifiers": {"HMDB": "HMDB00122"}},
    {"name": "Sphinganine"},
]

results = map_entities(records, progress=True)  # tqdm bar with [notebook]
summary = summarize(results)

print(f"{summary.resolved}/{summary.total_queried} resolved")
print(f"Resolution rate: {summary.resolution_rate:.1%}")
print(summary.vocabulary_coverage)
```

### Async usage

```python
import asyncio
from ddharmon import BioMapperClient

async def main() -> None:
    async with BioMapperClient() as client:
        # Verify connectivity
        health = await client.health_check()
        print(health)  # {'status': 'healthy', ...}

        # Single
        result = await client.map_entity(
            "L-Histidine",
            identifiers={"HMDB": "HMDB00177"},
        )

        # Batch with rate limiting
        results = await client.map_entities(
            [{"name": "L-Histidine"}, {"name": "Glucose"}],
            rate_limit_delay=0.3,
            progress=True,
        )

asyncio.run(main())
```

### Jupyter notebooks

Apply `nest_asyncio` before using sync helpers inside a running event loop:

```python
import nest_asyncio
nest_asyncio.apply()  # required in Jupyter

from ddharmon import map_entities
results = map_entities([{"name": "L-Histidine"}], progress=True)
```

---

## Metabolon extras

The `ddharmon[metabolon]` extra ships helpers that replicate and generalize the
preprocessing from the BioVector-eval Metabolon tutorial notebook.

```python
import pandas as pd
from ddharmon import map_entities, summarize
from ddharmon.extras.metabolon import (
    build_mapping_queue,
    clean_compound_name,
    extract_hmdb_id,
)
from ddharmon.extras.metabolon.export import save_results, results_to_dataframe

# 1. Load your Metabolon features spreadsheet
df = pd.read_excel("Metabolon_unknown_combined_features_metadata.xlsx")

# 2. Build a deduplicated mapping queue
#    - cleans compound names (strips quotes, _CE## suffixes)
#    - extracts HMDB hints from ms1_compound_name
#    - deduplicates by cleaned name, tracking all feature_ids
queue = build_mapping_queue(
    df,
    name_col="matched_name",
    hint_col="ms1_compound_name",
    limit=50,  # set to None for full run
)

print(f"{len(queue)} unique names to map")
print(f"  with HMDB hints: {sum(1 for r in queue if r.hmdb_hint)}")

# 3. Map (convert queue → API records first)
results = map_entities(
    [r.as_api_record() for r in queue],
    rate_limit_delay=0.3,
    progress=True,
)

# 4. Summarize
summary = summarize(results)
print(f"Resolution rate: {summary.resolution_rate:.1%}")

# 5. Export
save_results(
    results,
    summary=summary,
    json_path="output/mapping.json",
    tsv_path="output/mapping.tsv",
)

# Or work directly in pandas
result_df = results_to_dataframe(results)
print(result_df[["query_name", "primary_curie", "confidence_tier"]].head())
```

### Preprocessing functions

```python
from ddharmon.extras.metabolon import clean_compound_name, extract_hmdb_id

# Strip quotes and collision-energy suffixes
clean_compound_name('"1,3-Diphenylguanidine_CE45"')  # '1,3-Diphenylguanidine'
clean_compound_name('"4,6-DIOXOHEPTANOIC ACID"')     # '4,6-DIOXOHEPTANOIC ACID'
clean_compound_name('L-Histidine')                   # 'L-Histidine'  (unchanged)

# Extract HMDB accessions from ms1_compound_name format
extract_hmdb_id('HMDB:HMDB03349-2257 L-Dihydroorotic acid')  # 'HMDB03349'
extract_hmdb_id('HMDB00177')                                  # 'HMDB00177'
extract_hmdb_id(None)                                         # None
```

---

## API reference

### `MappingResult`

| Attribute | Type | Description |
|---|---|---|
| `query_name` | `str` | Name submitted to the API |
| `resolved` | `bool` | Whether any identifier was returned |
| `primary_curie` | `str \| None` | First CURIE in the response |
| `chosen_kg_id` | `str \| None` | Resolver-selected knowledge graph ID |
| `confidence_score` | `float \| None` | Highest score across annotators |
| `confidence_tier` | `str` | `"high"` (≥2.0) / `"medium"` (1–2) / `"low"` (<1) / `"unknown"` |
| `identifiers` | `dict[str, list[str]]` | Vocabulary → IDs, e.g. `{"CHEBI": ["15971"]}` |
| `hmdb_hint` | `str \| None` | HMDB hint passed in the request |
| `error` | `str \| None` | Error message if mapping failed |

```python
result.ids_for("CHEBI")        # ['15971']
result.ids_for("refmet_id")    # ['RM0129894']
result.ids_for("PUBCHEM.COMPOUND")  # []
```

### Confidence tiers

| Score | Tier | Recommended action |
|---|---|---|
| ≥ 2.0 | `high` | Accept without review |
| 1.0–2.0 | `medium` | Quick sanity check |
| < 1.0 | `low` | Manual review recommended |
| `None` | `unknown` | No score returned (e.g. HMDB-hint resolved) |

### Error handling

```python
from ddharmon import (
    BioMapperError,       # base class
    BioMapperAuthError,   # 401/403 — bad API key
    BioMapperRateLimitError,  # 429 — throttled
    BioMapperServerError,     # 5xx
    BioMapperTimeoutError,    # request timeout
    BioMapperConfigError,     # missing API key / bad config
)

try:
    result = map_entity("Glucose")
except BioMapperRateLimitError as e:
    print(f"Throttled. Retry after: {e.retry_after}s")
except BioMapperAuthError:
    print("Check your BIOMAPPER_API_KEY")
```

In batch mode (`map_entities`), per-record errors are caught and returned as
`MappingResult(error=...)` rather than aborting the batch.

---

## Development

```bash
git clone https://github.com/trentleslie/ddharmon
cd ddharmon
poetry install --with dev --extras all

make check          # format → lint → type-check → test
make test           # tests only
make coverage       # HTML coverage report
```

---

## License

MIT — see [LICENSE](LICENSE).

---

## Related

- **BioMapper2 API**: `https://biomapper.expertintheloop.io`
- **EITL platform**: `https://expertintheloop.io`
- **biovector-eval notebooks**: `https://github.com/trentleslie/biovector-eval`
