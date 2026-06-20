"""Unit tests for ddharmon LLM client layer."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestCandidateJudgment:
    """Tests for CandidateJudgment Pydantic model."""

    def test_valid_construction(self) -> None:
        from ddharmon.llm.prompts import CandidateJudgment

        cj = CandidateJudgment(
            candidate_variable="age_years",
            relation="exact",
            confidence=0.95,
            rationale="Same concept, same measurement",
        )
        assert cj.candidate_variable == "age_years"
        assert cj.relation == "exact"
        assert cj.confidence == 0.95
        assert cj.rationale == "Same concept, same measurement"

    def test_confidence_bounded_low(self) -> None:
        from pydantic import ValidationError

        from ddharmon.llm.prompts import CandidateJudgment

        with pytest.raises(ValidationError):
            CandidateJudgment(
                candidate_variable="x",
                relation="exact",
                confidence=-0.1,
                rationale="test",
            )

    def test_confidence_bounded_high(self) -> None:
        from pydantic import ValidationError

        from ddharmon.llm.prompts import CandidateJudgment

        with pytest.raises(ValidationError):
            CandidateJudgment(
                candidate_variable="x",
                relation="exact",
                confidence=1.1,
                rationale="test",
            )


class TestRerankerResponse:
    """Tests for RerankerResponse Pydantic model."""

    def test_valid_construction(self) -> None:
        from ddharmon.llm.prompts import CandidateJudgment, RerankerResponse

        judgments = [
            CandidateJudgment(
                candidate_variable="age_years",
                relation="exact",
                confidence=0.95,
                rationale="Same concept",
            ),
            CandidateJudgment(
                candidate_variable="height_cm",
                relation="no_match",
                confidence=0.05,
                rationale="Different concept",
            ),
        ]
        resp = RerankerResponse(judgments=judgments)
        assert len(resp.judgments) == 2
        assert resp.judgments[0].candidate_variable == "age_years"


class TestBaseLLMClient:
    """Tests for BaseLLMClient ABC."""

    def test_cannot_instantiate(self) -> None:
        from ddharmon.llm.base import BaseLLMClient

        with pytest.raises(TypeError):
            BaseLLMClient()  # type: ignore[abstract]


class TestRerankerPrompt:
    """Tests for prompt template and builder."""

    def test_system_prompt_contains_relation_types(self) -> None:
        from ddharmon.llm.prompts import RERANKER_SYSTEM_PROMPT

        for relation in ["exact", "broader", "narrower", "composite", "derivable", "no_match"]:
            assert relation in RERANKER_SYSTEM_PROMPT

    def test_build_reranker_prompt_formats_source_and_candidates(self) -> None:
        from ddharmon.llm.prompts import build_reranker_prompt

        source_context = {
            "variable": "age",
            "description": "Age at enrollment",
            "options": "",
            "data_type": "continuous",
            "units": "years",
            "validation": "",
            "category": "Demographics",
            "codes": "",
            "parent_context": "",
        }
        candidate_contexts = [
            {
                "variable": "age_years",
                "description": "Age in years",
                "options": "",
                "data_type": "continuous",
                "units": "years",
                "validation": "",
                "category": "Demographics",
                "codes": "",
                "parent_context": "",
            },
        ]
        prompt = build_reranker_prompt(source_context, candidate_contexts)
        assert "age" in prompt
        assert "Age at enrollment" in prompt
        assert "age_years" in prompt
        assert "Age in years" in prompt
        assert "Candidate 1" in prompt


class TestAnthropicClient:
    """Tests for AnthropicClient."""

    def test_instantiates_with_defaults(self) -> None:
        mock_anthropic_mod = MagicMock()
        mock_client_instance = MagicMock()
        mock_anthropic_mod.Anthropic.return_value = mock_client_instance

        with patch.dict("sys.modules", {"anthropic": mock_anthropic_mod}):
            from ddharmon.llm.anthropic_client import AnthropicClient

            client = AnthropicClient()
            assert client.model_name == "claude-sonnet-4-20250514"
            assert client.provider_name == "anthropic"

    def test_has_rerank_candidates_method(self) -> None:
        mock_anthropic_mod = MagicMock()
        mock_client_instance = MagicMock()
        mock_anthropic_mod.Anthropic.return_value = mock_client_instance

        with patch.dict("sys.modules", {"anthropic": mock_anthropic_mod}):
            from ddharmon.llm.anthropic_client import AnthropicClient

            client = AnthropicClient()
            assert hasattr(client, "rerank_candidates")
            assert callable(client.rerank_candidates)


class TestOpenAIClient:
    """Tests for OpenAIClient."""

    def test_instantiates_with_defaults(self) -> None:
        mock_openai_mod = MagicMock()
        mock_client_instance = MagicMock()
        mock_openai_mod.OpenAI.return_value = mock_client_instance

        with patch.dict("sys.modules", {"openai": mock_openai_mod}):
            from ddharmon.llm.openai_client import OpenAIClient

            client = OpenAIClient()
            assert client.model_name == "gpt-4o"
            assert client.provider_name == "openai"

    def test_has_rerank_candidates_method(self) -> None:
        mock_openai_mod = MagicMock()
        mock_client_instance = MagicMock()
        mock_openai_mod.OpenAI.return_value = mock_client_instance

        with patch.dict("sys.modules", {"openai": mock_openai_mod}):
            from ddharmon.llm.openai_client import OpenAIClient

            client = OpenAIClient()
            assert hasattr(client, "rerank_candidates")
            assert callable(client.rerank_candidates)


class TestGetClientFactory:
    """Tests for get_client() factory function."""

    def test_get_client_anthropic(self) -> None:
        mock_anthropic_mod = MagicMock()
        mock_anthropic_mod.Anthropic.return_value = MagicMock()

        with patch.dict("sys.modules", {"anthropic": mock_anthropic_mod}):
            from ddharmon.llm import get_client

            client = get_client("anthropic")
            assert client.provider_name == "anthropic"

    def test_get_client_openai(self) -> None:
        mock_openai_mod = MagicMock()
        mock_openai_mod.OpenAI.return_value = MagicMock()

        with patch.dict("sys.modules", {"openai": mock_openai_mod}):
            from ddharmon.llm import get_client

            client = get_client("openai")
            assert client.provider_name == "openai"

    def test_get_client_unknown_raises(self) -> None:
        from ddharmon.llm import get_client

        with pytest.raises(ValueError, match="Unknown"):
            get_client("gemini")
