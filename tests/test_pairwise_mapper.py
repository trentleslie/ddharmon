"""Tests for the pairwise mapper orchestrator (match_dictionaries)."""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

from ddharmon.embedding.service import EmbeddedDictionary
from ddharmon.llm.base import BaseLLMClient
from ddharmon.llm.prompts import CandidateJudgment, RerankerResponse
from ddharmon.matching.confidence import ConfidenceConfig
from ddharmon.matching.pairwise_mapper import MatchingConfig, match_dictionaries
from ddharmon.models.data_dictionary import DataDictionary, Field
from ddharmon.models.enums import Relation, ReviewStatus, UnmappedReason

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class MockLLMClient(BaseLLMClient):
    """Mock LLM client that returns predetermined judgments."""

    def __init__(self, judgments_by_source: dict[str, list[CandidateJudgment]]) -> None:
        self._judgments_by_source = judgments_by_source
        self._calls: list[dict[str, str]] = []

    @property
    def provider_name(self) -> str:
        return "mock"

    @property
    def model_name(self) -> str:
        return "mock-v1"

    def rerank_candidates(
        self,
        source_context: dict[str, str],
        candidate_contexts: list[dict[str, str]],
        candidate_names: list[str],
    ) -> RerankerResponse:
        src_var = source_context["variable"]
        self._calls.append(source_context)
        judgments = self._judgments_by_source.get(src_var, [])
        return RerankerResponse(judgments=judgments)


def _make_field(name: str, desc: str) -> Field:
    return Field(variable_name=name, description=desc)


def _make_dict(name: str, fields: list[Field]) -> DataDictionary:
    return DataDictionary(name=name, fields={f.variable_name: f for f in fields})


def _make_embedded(dd: DataDictionary, vectors: dict[str, NDArray[np.float32]]) -> EmbeddedDictionary:
    return EmbeddedDictionary(dictionary=dd, embeddings=vectors, model_name="test-model")


def _normalized(vec: list[float]) -> NDArray[np.float32]:
    """Return L2-normalized vector."""
    a = np.array(vec, dtype=np.float32)
    return a / np.linalg.norm(a)


# ---------------------------------------------------------------------------
# Fixtures: build source/target with known cosine similarities
# ---------------------------------------------------------------------------

# Dimension = 4 for simplicity
_SRC_AGE_VEC = _normalized([1.0, 0.0, 0.0, 0.0])
_SRC_GENDER_VEC = _normalized([0.0, 1.0, 0.0, 0.0])
_SRC_ORPHAN_VEC = _normalized([0.0, 0.0, 0.0, 1.0])  # no match in target

_TGT_AGE_VEC = _normalized([0.95, 0.05, 0.0, 0.0])  # high sim to src_age
_TGT_SEX_VEC = _normalized([0.05, 0.95, 0.0, 0.0])  # high sim to src_gender
_TGT_BMI_VEC = _normalized([0.0, 0.0, 1.0, 0.0])  # low sim to all source


def _build_scenario() -> tuple[EmbeddedDictionary, EmbeddedDictionary, MockLLMClient]:
    """Build a standard test scenario with 3 source fields and 3 target fields."""
    src_fields = [
        _make_field("age", "Age at enrollment"),
        _make_field("gender", "Gender of participant"),
        _make_field("orphan_field", "Some obscure measure"),
    ]
    tgt_fields = [
        _make_field("age_years", "Age in years"),
        _make_field("sex", "Biological sex"),
        _make_field("bmi", "Body mass index"),
    ]

    src_dd = _make_dict("source_cohort", src_fields)
    tgt_dd = _make_dict("target_cohort", tgt_fields)

    src_emb = _make_embedded(
        src_dd,
        {"age": _SRC_AGE_VEC, "gender": _SRC_GENDER_VEC, "orphan_field": _SRC_ORPHAN_VEC},
    )
    tgt_emb = _make_embedded(
        tgt_dd,
        {"age_years": _TGT_AGE_VEC, "sex": _TGT_SEX_VEC, "bmi": _TGT_BMI_VEC},
    )

    # LLM judgments:
    # - age -> age_years: exact match, high confidence
    # - gender -> sex: exact match, high confidence
    # - orphan_field: no candidates (cosine too low), never reaches LLM
    judgments: dict[str, list[CandidateJudgment]] = {
        "age": [
            CandidateJudgment(
                candidate_variable="age_years", relation="exact", confidence=0.95, rationale="Same age concept"
            ),
        ],
        "gender": [
            CandidateJudgment(
                candidate_variable="sex", relation="exact", confidence=0.95, rationale="Gender/sex equivalent"
            ),
        ],
    }

    client = MockLLMClient(judgments)
    return src_emb, tgt_emb, client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMatchingConfig:
    def test_defaults(self) -> None:
        cfg = MatchingConfig()
        assert cfg.top_k == 5
        assert cfg.cosine_threshold == 0.3
        assert cfg.llm_provider == "anthropic"
        assert cfg.llm_model is None
        assert isinstance(cfg.confidence_config, ConfidenceConfig)
        assert cfg.semantic_weight == 1.0
        assert cfg.value_weight == 0.0

    def test_custom_values(self) -> None:
        cc = ConfidenceConfig(llm_weight=0.7, cosine_weight=0.3)
        cfg = MatchingConfig(
            top_k=10, cosine_threshold=0.5, llm_provider="openai", llm_model="gpt-4o", confidence_config=cc
        )
        assert cfg.top_k == 10
        assert cfg.confidence_config.llm_weight == 0.7

    def test_semantic_value_weights_custom_valid(self) -> None:
        cfg = MatchingConfig(semantic_weight=0.5, value_weight=0.5)
        assert cfg.semantic_weight == 0.5
        assert cfg.value_weight == 0.5

    def test_semantic_value_weights_invalid_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="sum to 1.0"):
            MatchingConfig(semantic_weight=0.8, value_weight=0.3)


class TestMatchDictionaries:
    def test_returns_mapping_result_with_names(self) -> None:
        src_emb, tgt_emb, client = _build_scenario()
        result = match_dictionaries(src_emb, tgt_emb, client)
        assert result.source_name == "source_cohort"
        assert result.target_name == "target_cohort"

    def test_successful_match_produces_field_mapping(self) -> None:
        src_emb, tgt_emb, client = _build_scenario()
        result = match_dictionaries(src_emb, tgt_emb, client)

        # age -> age_years should be mapped
        age_mappings = [m for m in result.mappings if m.source_variable == "age"]
        assert len(age_mappings) == 1
        m = age_mappings[0]
        assert m.target_variable == "age_years"
        assert m.relation == Relation.EXACT
        assert m.llm_confidence == 0.95
        assert m.cosine_similarity > 0.9  # known from our vectors
        # Composite score: 0.6*0.95 + 0.4*cosine > 0.9 -> auto_approved
        assert m.review_status == ReviewStatus.AUTO_APPROVED
        assert m.confidence > 0.0
        assert m.rationale == "Same age concept"

    def test_all_llm_rejected_produces_unmapped(self) -> None:
        """When LLM returns no_match for all candidates, field is unmapped."""
        src_fields = [_make_field("age", "Age at enrollment")]
        tgt_fields = [_make_field("age_years", "Age in years")]
        src_dd = _make_dict("src", src_fields)
        tgt_dd = _make_dict("tgt", tgt_fields)

        src_emb = _make_embedded(src_dd, {"age": _SRC_AGE_VEC})
        tgt_emb = _make_embedded(tgt_dd, {"age_years": _TGT_AGE_VEC})

        # LLM says no_match for the candidate
        judgments: dict[str, list[CandidateJudgment]] = {
            "age": [
                CandidateJudgment(
                    candidate_variable="age_years", relation="no_match", confidence=0.1, rationale="Not related"
                ),
            ],
        }
        client = MockLLMClient(judgments)
        result = match_dictionaries(src_emb, tgt_emb, client)

        assert len(result.mappings) == 0
        assert len(result.source_unmapped) == 1
        unmapped = result.source_unmapped[0]
        assert unmapped.variable_name == "age"
        assert unmapped.reason == UnmappedReason.LLM_REJECTED_ALL
        assert len(unmapped.rejected_candidates) >= 1

    def test_no_candidates_produces_unmapped(self) -> None:
        src_emb, tgt_emb, client = _build_scenario()
        # orphan_field vector is orthogonal to all target vectors -> no candidates above threshold
        result = match_dictionaries(src_emb, tgt_emb, client)

        orphan_unmapped = [u for u in result.source_unmapped if u.variable_name == "orphan_field"]
        assert len(orphan_unmapped) == 1
        assert orphan_unmapped[0].reason == UnmappedReason.NO_CANDIDATES

    def test_target_unmapped_reported(self) -> None:
        src_emb, tgt_emb, client = _build_scenario()
        result = match_dictionaries(src_emb, tgt_emb, client)

        # bmi has no source field mapping to it
        target_unmapped_names = [u.variable_name for u in result.target_unmapped]
        assert "bmi" in target_unmapped_names

    def test_auto_approved_pending_rejected_subsets(self) -> None:
        """MappingResult properties return correct subsets."""
        src_emb, tgt_emb, client = _build_scenario()
        result = match_dictionaries(src_emb, tgt_emb, client)

        # Both age and gender should be auto_approved (high confidence)
        assert len(result.auto_approved) == 2
        assert len(result.pending_review) == 0
        assert len(result.auto_rejected) == 0

    def test_config_metadata_in_result(self) -> None:
        src_emb, tgt_emb, client = _build_scenario()
        cfg = MatchingConfig(top_k=3, cosine_threshold=0.2)
        result = match_dictionaries(src_emb, tgt_emb, client, config=cfg)
        assert result.top_k == 3
        assert result.cosine_threshold == 0.2
        assert result.llm_provider == "mock"
        assert result.model_name == "mock-v1"
