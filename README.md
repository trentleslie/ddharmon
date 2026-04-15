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
```

---

## Getting an API key

The BioMapper2 API requires an API key. To request access, email
[trent.leslie@phenomehealth.org](mailto:trent.leslie@phenomehealth.org).

Once you have a key, set it in your environment:
```bash
export BIOMAPPER_API_KEY=your-key-here
```

Or add it to a `.env` file in your project root:
```
BIOMAPPER_API_KEY=your-key-here
```

ddharmon will pick it up automatically from either location.

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

Inputs are auto-chunked at 1000 entities per request against the native
`POST /map/batch` endpoint, so 10,000 records cost 10 round-trips.

### Dataset upload (synchronous)

For larger inputs, hand the server a TSV/CSV file directly and stream
results back. The server processes the file row-by-row over the
`POST /map/dataset/stream` endpoint:

```python
from pathlib import Path
from ddharmon import map_dataset_file_sync

result = map_dataset_file_sync(
    Path("compounds.tsv"),
    name_column="name",
    provided_id_columns=["hmdb_id"],
    progress=True,         # tqdm bar
    total_hint=1000,       # optional; enables % progress
)
result.raise_for_error()   # opt-in: raise BioMapperError if the stream truncated
print(f"resolved {sum(1 for r in result.results if r.resolved)} of {len(result.results)}")
```

`name_column` and `provided_id_columns` are required — the server uses
them to map your file's columns to entity names and identifier hints.
For per-result streaming into a UI or custom processing, use the async
`BioMapperClient.map_dataset_file_iter` method (see the tutorial
notebook in `notebooks/`).

### Discovering what the API supports

```python
from ddharmon import list_annotators, list_vocabularies, list_entity_types

for a in list_annotators():
    print(f"{a.slug:30s} {a.name}")

# 300+ supported vocabularies (CHEBI, HMDB, PubChem, …)
vocabs = list_vocabularies()
print(f"{len(vocabs)} vocabularies supported")

# Biolink entity types with their known aliases
for et in list_entity_types():
    print(f"{et.type}: {', '.join(et.aliases)}")
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

        # Batch — auto-chunked at 1000 entities per request
        results = await client.map_entities(
            [{"name": "L-Histidine"}, {"name": "Glucose"}],
            progress=True,
        )

        # Stream from a file — per-result as they arrive
        from pathlib import Path
        async for r in client.map_dataset_file_iter(
            Path("compounds.tsv"),
            name_column="name",
            provided_id_columns=["hmdb_id"],
        ):
            print(r.query_name, r.primary_curie)

asyncio.run(main())
```

`map_dataset_file_iter` is the primitive for UIs and custom processing that
want per-result reactivity. Callers needing a blocking, fully-collected
result should use `map_dataset_file_sync` instead (see above).

### Jupyter notebooks

Apply `nest_asyncio` before using sync helpers inside a running event loop:

```python
import nest_asyncio
nest_asyncio.apply()  # required in Jupyter

from ddharmon import map_entities
results = map_entities([{"name": "L-Histidine"}], progress=True)
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

### `DatasetMappingResult`

Return type of `map_dataset_file_sync`. Captures per-row results plus an
opt-in error signal for partial runs.

| Attribute | Type | Description |
|---|---|---|
| `results` | `list[MappingResult]` | Per-row mapping outcomes in server-emitted order |
| `stats` | `dict[str, Any]` | Server-provided summary. Empty unless the stream emits a terminal summary line |
| `metadata` | `ApiMetadata` | Request metadata; stays at defaults when the stream truncates before completion |
| `error` | `str \| None` | Mid-stream transport failure text. `None` on clean runs |

```python
result.raise_for_error()   # raises BioMapperError if .error is set; else no-op
```

`raise_for_error` mirrors `httpx.Response.raise_for_status` and turns the
partial-result contract into an explicit caller opt-in — silent consumption
of a truncated run (using `.results` without checking `.error`) is the
footgun this model is designed to prevent.

> **Note:** `confidence_score` on dataset-stream results is always `None` —
> the `/map/dataset/stream` endpoint emits a slimmer per-row payload than
> `/map/batch` and does not include the annotator `assigned_ids` block.
> Use `map_entity` / `map_entities` if you need confidence tiers.

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

Dataset streaming (`map_dataset_file_sync`) uses a two-tier contract:

- **Initial-request errors** (401, 422, 500, connect timeout) raise as typed
  exceptions — these happen before any row is processed, so partial results
  don't exist to preserve.
- **Mid-stream transport failures** are captured into
  `DatasetMappingResult.error` with the partial results preserved in
  `.results`. Call `.raise_for_error()` to get exception semantics, or
  inspect `.error` directly for "accept partial, log the rest" workflows.

Callback exceptions raised from `on_result` propagate unwrapped and
**replace the return value** — partial results collected up to that point
are lost. For UI consumers with failure-prone callbacks, wrap the callback
body in your own try/except if you want partial data to survive.

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
