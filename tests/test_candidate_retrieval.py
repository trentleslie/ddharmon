"""Tests for candidate retrieval and field context building."""

from __future__ import annotations

import numpy as np
import pytest

from ddharmon.models.data_dictionary import (
    DataDictionary,
    Field,
    ResponseOption,
)


@pytest.fixture()
def simple_dictionaries():
    """Create source and target EmbeddedDictionary with known vectors."""
    from ddharmon.embedding.service import EmbeddedDictionary

    # Source: 2 fields with known unit vectors
    src_fields = {
        "age": Field(variable_name="age", description="Age at enrollment"),
        "bmi": Field(variable_name="bmi", description="Body mass index"),
    }
    src_dict = DataDictionary(name="source", fields=src_fields)

    # 3D unit vectors for easy cosine computation
    age_vec = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    bmi_vec = np.array([0.0, 1.0, 0.0], dtype=np.float32)

    src_embedded = EmbeddedDictionary(
        dictionary=src_dict,
        embeddings={"age": age_vec, "bmi": bmi_vec},
        model_name="test-model",
    )

    # Target: 5 fields with vectors at known angles to source
    tgt_fields = {
        "age_years": Field(variable_name="age_years", description="Age in years"),
        "body_mass_index": Field(variable_name="body_mass_index", description="BMI"),
        "height": Field(variable_name="height", description="Height in cm"),
        "weight": Field(variable_name="weight", description="Weight in kg"),
        "age_category": Field(variable_name="age_category", description="Age group"),
    }
    tgt_dict = DataDictionary(name="target", fields=tgt_fields)

    # age_years: very similar to age (cosine ~0.95)
    age_years_vec = np.array([0.95, 0.05, 0.0], dtype=np.float32)
    age_years_vec /= np.linalg.norm(age_years_vec)
    # body_mass_index: very similar to bmi (cosine ~0.95)
    bmi_tgt_vec = np.array([0.05, 0.95, 0.0], dtype=np.float32)
    bmi_tgt_vec /= np.linalg.norm(bmi_tgt_vec)
    # height: orthogonal to both
    height_vec = np.array([0.0, 0.0, 1.0], dtype=np.float32)
    # weight: slightly related to bmi
    weight_vec = np.array([0.0, 0.4, 0.6], dtype=np.float32)
    weight_vec /= np.linalg.norm(weight_vec)
    # age_category: related to age
    age_cat_vec = np.array([0.7, 0.0, 0.3], dtype=np.float32)
    age_cat_vec /= np.linalg.norm(age_cat_vec)

    tgt_embedded = EmbeddedDictionary(
        dictionary=tgt_dict,
        embeddings={
            "age_category": age_cat_vec,
            "age_years": age_years_vec,
            "body_mass_index": bmi_tgt_vec,
            "height": height_vec,
            "weight": weight_vec,
        },
        model_name="test-model",
    )

    return src_embedded, tgt_embedded


class TestRetrieveCandidates:
    """Tests for retrieve_candidates()."""

    def test_returns_dict_mapping_source_to_candidates(self, simple_dictionaries):
        from ddharmon.matching.candidate_retrieval import retrieve_candidates

        src, tgt = simple_dictionaries
        result = retrieve_candidates(src, tgt, top_k=3, cosine_threshold=0.0)

        assert isinstance(result, dict)
        assert set(result.keys()) == {"age", "bmi"}
        # Each value is a list of (variable_name, score) tuples
        for candidates in result.values():
            assert isinstance(candidates, list)
            for item in candidates:
                assert len(item) == 2
                assert isinstance(item[0], str)
                assert isinstance(item[1], float)

    def test_candidates_below_threshold_filtered(self, simple_dictionaries):
        from ddharmon.matching.candidate_retrieval import retrieve_candidates

        src, tgt = simple_dictionaries
        # High threshold should filter out most candidates
        result = retrieve_candidates(src, tgt, top_k=5, cosine_threshold=0.9)

        # "age" should have age_years (cosine ~0.95) but NOT height (cosine ~0)
        age_candidates = result["age"]
        candidate_names = [name for name, _ in age_candidates]
        assert "age_years" in candidate_names
        assert "height" not in candidate_names

    def test_top_k_limits_results(self, simple_dictionaries):
        from ddharmon.matching.candidate_retrieval import retrieve_candidates

        src, tgt = simple_dictionaries
        result = retrieve_candidates(src, tgt, top_k=2, cosine_threshold=0.0)

        for candidates in result.values():
            assert len(candidates) <= 2

    def test_results_sorted_by_score_descending(self, simple_dictionaries):
        from ddharmon.matching.candidate_retrieval import retrieve_candidates

        src, tgt = simple_dictionaries
        result = retrieve_candidates(src, tgt, top_k=5, cosine_threshold=0.0)

        for candidates in result.values():
            scores = [score for _, score in candidates]
            assert scores == sorted(scores, reverse=True)


class TestBlendedScoring:
    """Tests for blended cosine scoring with semantic + value vectors."""

    def _make_embedded_with_values(self):
        """Build source/target with both semantic and value embeddings."""
        from ddharmon.embedding.service import EmbeddedDictionary

        src_fields = {
            "smoking": Field(variable_name="smoking", description="Do you smoke?"),
            "age": Field(variable_name="age", description="Age at enrollment"),
        }
        src_dict = DataDictionary(name="source", fields=src_fields)

        tgt_fields = {
            "tobacco_use": Field(variable_name="tobacco_use", description="Tobacco usage status"),
            "age_years": Field(variable_name="age_years", description="Age in years"),
        }
        tgt_dict = DataDictionary(name="target", fields=tgt_fields)

        # Semantic vectors (3D)
        src_sem_smoking = np.array([1.0, 0.0, 0.0], dtype=np.float32)
        src_sem_age = np.array([0.0, 1.0, 0.0], dtype=np.float32)

        tgt_sem_tobacco = np.array([0.9, 0.1, 0.0], dtype=np.float32)
        tgt_sem_tobacco /= np.linalg.norm(tgt_sem_tobacco)
        tgt_sem_age = np.array([0.1, 0.9, 0.0], dtype=np.float32)
        tgt_sem_age /= np.linalg.norm(tgt_sem_age)

        # Value vectors (3D) - different space, different similarities
        src_val_smoking = np.array([0.0, 0.0, 1.0], dtype=np.float32)
        tgt_val_tobacco = np.array([0.0, 0.1, 0.9], dtype=np.float32)
        tgt_val_tobacco /= np.linalg.norm(tgt_val_tobacco)

        src_embedded = EmbeddedDictionary(
            dictionary=src_dict,
            embeddings={"smoking": src_sem_smoking, "age": src_sem_age},
            value_embeddings={"smoking": src_val_smoking},  # age has no value vector
            model_name="test-model",
        )

        tgt_embedded = EmbeddedDictionary(
            dictionary=tgt_dict,
            embeddings={"tobacco_use": tgt_sem_tobacco, "age_years": tgt_sem_age},
            value_embeddings={"tobacco_use": tgt_val_tobacco},  # age_years has no value vector
            model_name="test-model",
        )

        return src_embedded, tgt_embedded

    def test_blended_score_when_both_have_value_vectors(self):
        """Score = 0.7*semantic + 0.3*value when both fields have value embeddings."""
        from ddharmon.matching.candidate_retrieval import retrieve_candidates

        src, tgt = self._make_embedded_with_values()
        result = retrieve_candidates(src, tgt, top_k=5, cosine_threshold=0.0, semantic_weight=0.7, value_weight=0.3)

        # smoking -> tobacco_use: both have value vectors
        smoking_candidates = result["smoking"]
        tobacco_match = [c for c in smoking_candidates if c[0] == "tobacco_use"]
        assert len(tobacco_match) == 1

        # Compute expected blended score
        sem_score = float(np.dot(src.embeddings["smoking"], tgt.embeddings["tobacco_use"]))
        val_score = float(np.dot(src.value_embeddings["smoking"], tgt.value_embeddings["tobacco_use"]))
        expected = 0.7 * sem_score + 0.3 * val_score

        assert abs(tobacco_match[0][1] - expected) < 1e-5

    def test_fallback_to_semantic_when_source_lacks_value(self):
        """Score = semantic-only when source field has no value vector."""
        from ddharmon.matching.candidate_retrieval import retrieve_candidates

        src, tgt = self._make_embedded_with_values()
        result = retrieve_candidates(src, tgt, top_k=5, cosine_threshold=0.0, semantic_weight=0.7, value_weight=0.3)

        # age -> age_years: age has no value vector -> fallback to semantic
        age_candidates = result["age"]
        age_match = [c for c in age_candidates if c[0] == "age_years"]
        assert len(age_match) == 1

        sem_score = float(np.dot(src.embeddings["age"], tgt.embeddings["age_years"]))
        assert abs(age_match[0][1] - sem_score) < 1e-5

    def test_fallback_to_semantic_when_target_lacks_value(self):
        """Score = semantic-only when target field has no value vector."""
        from ddharmon.matching.candidate_retrieval import retrieve_candidates

        src, tgt = self._make_embedded_with_values()
        result = retrieve_candidates(src, tgt, top_k=5, cosine_threshold=0.0, semantic_weight=0.7, value_weight=0.3)

        # smoking -> age_years: age_years has no value vector -> fallback to semantic
        smoking_candidates = result["smoking"]
        age_match = [c for c in smoking_candidates if c[0] == "age_years"]
        assert len(age_match) == 1

        sem_score = float(np.dot(src.embeddings["smoking"], tgt.embeddings["age_years"]))
        assert abs(age_match[0][1] - sem_score) < 1e-5

    def test_blended_results_sorted_descending(self):
        """After blended re-scoring, candidates are re-sorted by descending score."""
        from ddharmon.matching.candidate_retrieval import retrieve_candidates

        src, tgt = self._make_embedded_with_values()
        result = retrieve_candidates(src, tgt, top_k=5, cosine_threshold=0.0, semantic_weight=0.7, value_weight=0.3)

        for candidates in result.values():
            scores = [s for _, s in candidates]
            assert scores == sorted(scores, reverse=True)

    def test_no_value_embeddings_identical_to_semantic_only(self):
        """When neither dict has value embeddings, results are identical to semantic-only."""
        from ddharmon.matching.candidate_retrieval import retrieve_candidates

        src, tgt = self._make_embedded_with_values()
        # Remove all value embeddings
        src.value_embeddings = {}
        tgt.value_embeddings = {}

        result_blended = retrieve_candidates(
            src, tgt, top_k=5, cosine_threshold=0.0, semantic_weight=0.7, value_weight=0.3
        )
        result_semantic = retrieve_candidates(src, tgt, top_k=5, cosine_threshold=0.0)

        for src_name in result_blended:
            blended_scores = dict(result_blended[src_name])
            semantic_scores = dict(result_semantic[src_name])
            for name in blended_scores:
                assert abs(blended_scores[name] - semantic_scores[name]) < 1e-5

    def test_custom_weights(self):
        """Custom weights (e.g., 0.5/0.5) are applied correctly."""
        from ddharmon.matching.candidate_retrieval import retrieve_candidates

        src, tgt = self._make_embedded_with_values()
        result = retrieve_candidates(src, tgt, top_k=5, cosine_threshold=0.0, semantic_weight=0.5, value_weight=0.5)

        smoking_candidates = result["smoking"]
        tobacco_match = [c for c in smoking_candidates if c[0] == "tobacco_use"]
        assert len(tobacco_match) == 1

        sem_score = float(np.dot(src.embeddings["smoking"], tgt.embeddings["tobacco_use"]))
        val_score = float(np.dot(src.value_embeddings["smoking"], tgt.value_embeddings["tobacco_use"]))
        expected = 0.5 * sem_score + 0.5 * val_score

        assert abs(tobacco_match[0][1] - expected) < 1e-5

    def test_threshold_applied_to_blended_score(self):
        """Cosine threshold is applied to the blended score, not raw semantic."""
        from ddharmon.matching.candidate_retrieval import retrieve_candidates

        src, tgt = self._make_embedded_with_values()
        # Use a threshold that would filter some candidates after blending
        result = retrieve_candidates(src, tgt, top_k=5, cosine_threshold=0.8, semantic_weight=0.7, value_weight=0.3)

        for candidates in result.values():
            for _, score in candidates:
                assert score >= 0.8


class TestBuildFieldContext:
    """Tests for build_field_context()."""

    def test_extracts_basic_fields(self):
        from ddharmon.matching.candidate_retrieval import build_field_context

        fld = Field(
            variable_name="age",
            description="Age at enrollment in years",
            category="Demographics",
            response_options=[
                ResponseOption(code="1", label="18-30"),
                ResponseOption(code="2", label="31-50"),
            ],
        )
        dd = DataDictionary(name="test", fields={"age": fld})

        ctx = build_field_context(fld, dd)

        assert ctx["variable"] == "age"
        assert ctx["description"] == "Age at enrollment in years"
        assert "18-30" in ctx["options"]
        assert "31-50" in ctx["options"]
        assert ctx["category"] == "Demographics"

    def test_extracts_standard_codes(self):
        from ddharmon.matching.candidate_retrieval import build_field_context

        fld = Field(
            variable_name="bmi",
            description="Body mass index",
            standard_codes={"SNOMED": ["60621009"], "LOINC": ["39156-5"]},
        )
        dd = DataDictionary(name="test", fields={"bmi": fld})

        ctx = build_field_context(fld, dd)

        assert "SNOMED" in ctx["codes"]
        assert "60621009" in ctx["codes"]
        assert "LOINC" in ctx["codes"]

    def test_child_field_includes_parent_context(self):
        from ddharmon.matching.candidate_retrieval import build_field_context

        parent = Field(
            variable_name="diet_q1",
            description="How often do you eat vegetables?",
        )
        child = Field(
            variable_name="diet_q1_raw",
            description="raw score",
            parent_field_id="diet_q1",
        )
        dd = DataDictionary(
            name="test",
            fields={"diet_q1": parent, "diet_q1_raw": child},
        )

        ctx = build_field_context(child, dd)

        assert "parent_context" in ctx
        assert "How often do you eat vegetables?" in ctx["parent_context"]

    def test_root_field_empty_parent_context(self):
        from ddharmon.matching.candidate_retrieval import build_field_context

        fld = Field(variable_name="age", description="Age at enrollment")
        dd = DataDictionary(name="test", fields={"age": fld})

        ctx = build_field_context(fld, dd)

        assert ctx["parent_context"] == ""

    def test_context_dict_has_no_encoding_key(self):
        """Encoding detection was removed; the context dict no longer exposes 'encoding'."""
        from ddharmon.matching.candidate_retrieval import build_field_context

        fld = Field(variable_name="x", description="Some field")
        dd = DataDictionary(name="test", fields={"x": fld})

        ctx = build_field_context(fld, dd)

        assert "encoding" not in ctx

    def test_description_shows_both_when_question_text_and_description_differ(self):
        """LLM sees BOTH signals when both are populated (route 3 — max info for transforms)."""
        from ddharmon.matching.candidate_retrieval import build_field_context

        fld = Field(
            variable_name="sleep_q",
            description="Trouble sleeping (short label)",
            question_text="Do you have trouble falling asleep at night?",
        )
        dd = DataDictionary(name="test", fields={"sleep_q": fld})

        ctx = build_field_context(fld, dd)

        assert "Do you have trouble falling asleep at night?" in ctx["description"]
        assert "Trouble sleeping (short label)" in ctx["description"]
        assert "Definition:" in ctx["description"]

    def test_description_falls_back_to_description_when_no_question_text(self):
        from ddharmon.matching.candidate_retrieval import build_field_context

        fld = Field(variable_name="age", description="Age at enrollment")
        dd = DataDictionary(name="test", fields={"age": fld})

        ctx = build_field_context(fld, dd)

        assert ctx["description"] == "Age at enrollment"

    def test_description_dedupes_when_question_text_equals_description(self):
        """When both fields carry identical text, render once without a Definition line."""
        from ddharmon.matching.candidate_retrieval import build_field_context

        fld = Field(
            variable_name="q",
            description="Do you smoke?",
            question_text="Do you smoke?",
        )
        dd = DataDictionary(name="test", fields={"q": fld})

        ctx = build_field_context(fld, dd)

        assert ctx["description"] == "Do you smoke?"
        assert "Definition:" not in ctx["description"]

    def test_exposes_data_type_units_validation(self):
        """LLM needs data_type, units, and validation constraints to design transformations."""
        from ddharmon.matching.candidate_retrieval import build_field_context

        fld = Field(
            variable_name="weight",
            description="Body weight",
            data_type="continuous",
            units="kg",
            validation="min=20, max=250",
        )
        dd = DataDictionary(name="test", fields={"weight": fld})

        ctx = build_field_context(fld, dd)

        assert ctx["data_type"] == "continuous"
        assert ctx["units"] == "kg"
        assert ctx["validation"] == "min=20, max=250"
