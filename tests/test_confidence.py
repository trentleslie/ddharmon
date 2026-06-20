"""Tests for confidence scoring and triage."""

from __future__ import annotations

import pytest

from ddharmon.models.enums import ReviewStatus


class TestConfidenceConfig:
    """Tests for ConfidenceConfig dataclass."""

    def test_default_weights(self):
        from ddharmon.matching.confidence import ConfidenceConfig

        config = ConfidenceConfig()
        assert config.llm_weight == 0.6
        assert config.cosine_weight == 0.4
        assert config.auto_approve_threshold == 0.9
        assert config.auto_reject_threshold == 0.3

    def test_weights_must_sum_to_one(self):
        from ddharmon.matching.confidence import ConfidenceConfig

        with pytest.raises(ValueError, match="sum to 1.0"):
            ConfidenceConfig(llm_weight=0.5, cosine_weight=0.3)

    def test_custom_thresholds(self):
        from ddharmon.matching.confidence import ConfidenceConfig

        config = ConfidenceConfig(
            auto_approve_threshold=0.95,
            auto_reject_threshold=0.2,
        )
        assert config.auto_approve_threshold == 0.95
        assert config.auto_reject_threshold == 0.2


class TestScoreMapping:
    """Tests for score_mapping()."""

    def test_weighted_composite(self):
        from ddharmon.matching.confidence import score_mapping

        # 0.6*0.95 + 0.4*0.85 = 0.57 + 0.34 = 0.91
        score = score_mapping(llm_confidence=0.95, cosine_similarity=0.85)
        assert abs(score - 0.91) < 1e-6

    def test_clamped_to_zero(self):
        from ddharmon.matching.confidence import score_mapping

        score = score_mapping(llm_confidence=-0.5, cosine_similarity=-0.5)
        assert score == 0.0

    def test_clamped_to_one(self):
        from ddharmon.matching.confidence import score_mapping

        score = score_mapping(llm_confidence=1.5, cosine_similarity=1.5)
        assert score == 1.0

    def test_custom_config(self):
        from ddharmon.matching.confidence import ConfidenceConfig, score_mapping

        config = ConfidenceConfig(llm_weight=0.5, cosine_weight=0.5)
        # 0.5*0.8 + 0.5*0.6 = 0.4 + 0.3 = 0.7
        score = score_mapping(llm_confidence=0.8, cosine_similarity=0.6, config=config)
        assert abs(score - 0.7) < 1e-6


class TestTriageMapping:
    """Tests for triage_mapping()."""

    def test_auto_approved(self):
        from ddharmon.matching.confidence import triage_mapping

        assert triage_mapping(0.91) == ReviewStatus.AUTO_APPROVED

    def test_pending_review(self):
        from ddharmon.matching.confidence import triage_mapping

        assert triage_mapping(0.5) == ReviewStatus.PENDING_REVIEW

    def test_auto_rejected(self):
        from ddharmon.matching.confidence import triage_mapping

        assert triage_mapping(0.2) == ReviewStatus.AUTO_REJECTED

    def test_boundary_approve(self):
        from ddharmon.matching.confidence import triage_mapping

        # Exactly at threshold = approved
        assert triage_mapping(0.9) == ReviewStatus.AUTO_APPROVED

    def test_boundary_reject(self):
        from ddharmon.matching.confidence import triage_mapping

        # Exactly at threshold = rejected
        assert triage_mapping(0.3) == ReviewStatus.AUTO_REJECTED

    def test_custom_thresholds(self):
        from ddharmon.matching.confidence import ConfidenceConfig, triage_mapping

        config = ConfidenceConfig(auto_approve_threshold=0.95, auto_reject_threshold=0.1)
        # 0.91 would normally be auto_approved, but with 0.95 threshold it's pending
        assert triage_mapping(0.91, config=config) == ReviewStatus.PENDING_REVIEW
        # 0.2 would normally be auto_rejected, but with 0.1 threshold it's pending
        assert triage_mapping(0.2, config=config) == ReviewStatus.PENDING_REVIEW
