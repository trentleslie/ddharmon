# Changelog

All notable changes to ddharmon are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## [0.5.0] — 2026-05-29

First public release of the v1 harmonization pipeline (supersedes the early 0.x placeholder
releases). v1 is a deliberately-scoped **sub-cluster-anchored CDE harmonization** pipeline — an
extension of the embedding-clustering-for-variable-harmonization line of work
(see [`docs/v1_methods.md`](docs/v1_methods.md)).

### Added

- **`ddharmon.harmonization`** — the v1 pipeline package:
  - `harmonize_dictionaries(...)` — end-to-end orchestration: semantic cluster → value
    sub-cluster → CDE anchor → adopt/refine/novel classify.
  - `prepare_from_clusters(...)` / `assemble_verdicts(...)` — split clustering from LLM so the
    sub-cluster → anchor → gate → prompt logic is testable and the LLM call can run inline or
    via the offline Batch API workflow.
  - `find_anchor_cde(...)` — per-sub-cluster CDE recommendation (medoid → best in-cluster CDE,
    ranked by similarity, then canonicalness, then metadata richness; GenCDE fallback).
  - Classify-only adopt/refine/novel prompts (`harmonize` and concept-only `kg_only` modes),
    robust JSON response parsing, and `HarmonizationVerdict` / `AnchorResult` models.
  - Exporters: `write_prompts_jsonl`, `write_buckets`, `export_eitl_queue` (EITL review TSV).
- **`ddharmon.clustering.value_subcluster`** — HDBSCAN sub-clustering on value-encoding vectors
  within each semantic topic (Phase 1b), plus `build_value_vector_lookup`.
- **Multi-cohort + CDE ingestion** verified through the existing generic loader for the NIH CDE
  export (flatten via `scripts/flatten_cde_repo.py`; full repo ~22.7k CDEs or endorsed-only ~174).
- **Canonical notebook** `notebooks/clustering/v1_harmonization_pipeline.ipynb` — thin
  orchestration over the new API (cohorts + CDE → embed → cluster → sub-cluster → anchor →
  classify → EITL).
- **Docs**: `docs/v1_methods.md` (methods + citation lineage); README v1 section.
- Tests: `test_subcluster`, `test_anchor`, `test_harmonization_prompts`, `test_harmonization_pipeline`.

### Notes / scope

- The single LLM call is the **classify-only** adopt/refine/novel pass; recommendations are
  routed to **EITL** for human verification (no spec is auto-authored, nothing is auto-applied).
- CDEs participate in clustering (loaded as cohort `NIH_CDE`); the anchor is the best CDE that
  lands in a sub-cluster, with a GenCDE-needed (`novel`) fallback when none does.

### Deferred (publication-pending / future)

- LLM **coherence judging** (a dual-sample coherence pass).
- LLM **concept-labeling** (v1 uses derived c-TF-IDF labels) and **spec authoring** / transformation specs.
- **Granularity-loss** detection.
- **Deep recursive clustering** (v1 is topic → semantic split → value sub-cluster, bounded depth).
- Pairwise 1:1 matching (built, not in the v1 release surface), standards/KG mapping, and the
  Click CLI orchestrator (`ddharmon.cli:main` is declared but unimplemented).

[1.0.0]: https://github.com/Phenome-Health/ddharmon/releases/tag/v1.0.0
