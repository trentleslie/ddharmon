# ddharmon v1 — Methods & Lineage

**ddharmon v1** is a sub-cluster-anchored CDE harmonization pipeline. It is a deliberately-scoped
extension of a growing line of work that frames **biomedical variable matching / harmonization as
an embedding-clustering problem**: variables whose descriptions sit close in a language-model
embedding space form semantic clusters that correspond to a shared underlying concept.

This document describes (1) what v1 does and the algorithms it uses, (2) the literature lineage it
builds on and how it differs, and (3) what is deliberately deferred to a pending publication.

---

## 1. The v1 pipeline

```
ingest (cohorts + CDE catalog)
  → dual-vector embed (semantic + value)
    → semantic cluster (BERTopic)
      → value sub-cluster (HDBSCAN on value vectors, per topic)
        → CDE anchor per sub-cluster
          → adopt / refine / novel  (single classify-only LLM call)
            → EITL review queue
```

| Stage | Method | Notes |
|-------|--------|-------|
| **Ingest** | Generic CSV/TSV loader with role-mapped columns | Any study data dictionaries **and the NIH CDE repository** (loaded as cohort `NIH_CDE` so CDEs participate in clustering). |
| **Embed (dual vectors)** | sentence-transformers (`all-mpnet-base-v2`, 768-d), L2-normalized, SQLite-cached | Each field gets a **semantic** vector (question/description) *and* a **value** vector (response-option / encoding structure). The two axes are kept separate — semantic for concept clustering, value for sub-clustering. |
| **Semantic cluster** | BERTopic (UMAP → HDBSCAN → c-TF-IDF) on the semantic vectors | Clusters reflect cohort/CDE *concepts*; response options are excluded from this axis. |
| **Value sub-cluster** | HDBSCAN (Euclidean) on value vectors, within each topic | Splits a concept by *how it is answered* (encoding shape). `min_cluster_size = max(3, n // 20)`, gated at ≥ 8 value-vector members. |
| **CDE anchor** | Deterministic: medoid of the sub-cluster → best in-cluster CDE | Ranked by similarity-to-medoid, then **canonicalness** (multi-collection + cross-mapped CDEs win ties), then metadata **richness**. No CDE in the sub-cluster → GenCDE needed. |
| **Classify (A/R/N)** | One classify-only LLM call per sub-cluster, via the Anthropic Batch API | `adopt` / `refine` / `novel` against the recommended anchor + alternates (concept-only `adopt` / `unaligned` when the cluster lacks machine-readable response options). Gated to skip single-cohort sub-clusters. |
| **Review** | `eitl_queue.tsv` + per-verdict JSON buckets | Every recommendation is routed to expert-in-the-loop (EITL) verification — **nothing is auto-applied**, and no transformation spec is auto-authored in v1. |

The reusable logic lives in `ddharmon.harmonization` and `ddharmon.clustering`; the canonical
end-to-end demo is `notebooks/clustering/v1_harmonization_pipeline.ipynb`.

---

## 2. Lineage — variable matching as embedding clustering

v1 sits in a cluster of recent work that all share the embedding → (density) clustering → optional
LLM-labeling pattern, applied to biomedical variable/CDE harmonization. The closest antecedents:

- **Krishnamurthy et al., 2025** — *A Dynamic Framework for Semantic Grouping of Common Data
  Elements (CDE) Using Embeddings and Clustering.* arXiv:[2506.02160](https://arxiv.org/abs/2506.02160).
  Embeds ~24k NIH CDEs (OpenAI `text-embedding-3-small`), clusters with **HDBSCAN**, labels clusters
  with an LLM, and trains a classifier to assign new CDEs to clusters; validates against the Gravity
  Project SDOH domains. **Clusters the *target* (the CDE repository itself).**
- **Salimi Y, Adams T, Ay MC, Balabin H, Jacobs M, Hofmann-Apitius M (2025)** — *Evaluating
  language model embeddings for Parkinson's disease cohort harmonization using a novel manually
  curated variable mapping schema.* *Sci Rep* **15**:20210,
  doi:[10.1038/s41598-025-06447-2](https://doi.org/10.1038/s41598-025-06447-2) (PMID 40542087).
  Builds **PASSIONATE**, a curated PD variable-mapping ground truth, and shows LM embeddings beat
  fuzzy string matching for pairwise cohort harmonization (>80% accuracy, up to 96% in a wider
  neighborhood). Its discussion explicitly frames *the matching of embeddings as inherently a
  clustering task* — variables a small embedding distance apart form semantic clusters describing a
  common concept — but stops at t-SNE visualization (no clustering implemented). (Earlier preprint:
  Research Square rs-4108029, AD + PD.)

Related approaches that frame harmonization through embeddings / clustering / similarity:

- **A benchmark of text embedding models for semantic harmonization of Alzheimer's disease cohorts**
  (PMC12811766) — comparative evaluation of embedding models for the same task.
- **SONAR** — *Robust Automated Harmonization of Heterogeneous Data Through Ensemble Machine
  Learning.* *JMIR Med Inform* 2025, doi:[10.2196/54133](https://medinform.jmir.org/2025/1/e54133).
  Learns a per-variable embedding from description + value distribution; pairwise cosine similarity
  to score variable matches.
- **Automated Data Harmonization in Clinical Research: an NLP Approach** — *JMIR Form Res* 2025,
  doi:[10.2196/75608](https://formative.jmir.org/2025/1/e75608). BioBERT embeddings of variable
  descriptions to harmonize CVD-risk variables across FHS / MESA / ARIC.
- **Gottfried et al., 2025** — *Semantic search helper: embeddings in multi-item questionnaires as a
  harmonization tool*. Clusters *source* questionnaire items.
- **TopicForest** — *embedding-driven hierarchical clustering and labeling for biomedical
  literature* (J. Biomed. Inform.,
  [S153204642500187X](https://www.sciencedirect.com/science/article/abs/pii/S153204642500187X)).
  Relevant to the deferred recursive/hierarchical clustering direction.

Related open-source harmonization tools (positioning siblings rather than direct antecedents):

- **datastew** — a Python library for embedding-based data harmonization, mapping variables across
  data dictionaries and terminologies by LLM/embedding similarity
  (doi:[10.5281/zenodo.16871713](https://doi.org/10.5281/zenodo.16871713)). Closest in approach;
  ddharmon adds value-aware sub-clustering and a recommended CDE anchor per sub-cluster.
- **BDI-Kit** — *An AI-Powered Toolkit for Biomedical Data Harmonization* (Lopez et al., *Patterns*
  2026): schema matching, value matching, and transformation to a target schema / data model —
  oriented to table → target-schema mapping rather than multi-source cluster discovery.
- **Harmony** — NLP / generative-AI harmonization of questionnaire items across instruments and
  languages (McElroy et al., *BMC Psychiatry* **24**:530, 2024,
  doi:[10.1186/s12888-024-05954-2](https://doi.org/10.1186/s12888-024-05954-2)).

CDE mapping and generation (most directly comparable — v1 anchors variables to CDEs and, in the
deferred path, generates novel CDEs):

- **CDEMapper** (Wang et al., *JAMIA* **32**(7):1130–1139, 2025,
  doi:[10.1093/jamia/ocaf064](https://doi.org/10.1093/jamia/ocaf064); preprint
  arXiv:[2412.00491](https://arxiv.org/abs/2412.00491)) — an LLM-powered tool that maps local data
  elements to NIH CDEs via semantic indexing, BM25 + GPT candidate recommendation, and human review.
  ddharmon differs by clustering source variables *and* CDEs together and recommending one CDE per
  value sub-cluster rather than per-element lookup.
- **DataTecnica — DIVER / RoP** — DIVER applies LLMs to generate and audit CDEs at scale (Long et al.,
  *A new AI-assisted data standard accelerates interoperability in biomedical research*, medRxiv 2024,
  doi:[10.1101/2024.10.17.24315618](https://doi.org/10.1101/2024.10.17.24315618) — 43k+ generative
  CDEs (**GenCDEs**) across 31 studies, 94% needing no manual revision). The companion open-source
  **RoP** release ships ~1.33M harmonized CDEs with semantic embeddings, value sets, and governance
  parameters (Hugging Face, doi:[10.57967/hf/8781](https://doi.org/10.57967/hf/8781)). Closest to
  v1's GenCDE / novel-CDE direction (deferred in v1).

> Citation metadata above is drawn from the publishers' records.

### How v1 differs

| Axis | Krishnamurthy / Salimi (antecedents) | ddharmon v1 |
|------|--------------------------------------|-------------|
| What is clustered | CDEs (target) / single-disease cohort variables | **Cohort variables *and* CDEs together**, multi-cohort |
| Vectors | Single semantic vector | **Dual vectors** — semantic *and* value-encoding, used at different stages |
| Granularity | Flat clusters | Semantic cluster → **value sub-cluster** (encoding-aware) |
| CDE use | Organize / map | **Anchor a recommended CDE per sub-cluster** (medoid + canonicalness + richness; GenCDE fallback) |
| LLM role | Label clusters | **Classify adopt / refine / novel** per sub-cluster → EITL |

---

## 3. Deferred to the pending publication

v1 intentionally ships the engineering core and holds the research contributions for a forthcoming
paper. Not in v1:

- **LLM coherence judging** — a dual-sample pass that judges whether a cluster is one concept —
  and **LLM concept-labeling**; v1 uses deterministic gates and derived c-TF-IDF labels.
- **LLM spec authoring** / per-variable transformation specs — v1 stops at the adopt/refine/novel
  recommendation and hands off to EITL.
- **Granularity-loss detection**.
- **Deep recursive clustering** — v1 is topic → semantic split → value sub-cluster (bounded depth).
- **CDE common data model (CDM)** output.
