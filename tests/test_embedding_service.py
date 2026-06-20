"""Unit tests for embed_dictionary() orchestrator and EmbeddedDictionary.

Tests use a MockProvider that returns deterministic vectors without requiring
sentence-transformers. Verifies caching, incremental embedding, and find_similar.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pytest
from numpy.typing import NDArray

from ddharmon.embedding.service import EmbeddedDictionary, embed_dictionary, find_similar
from ddharmon.models.data_dictionary import (
    DataDictionary,
    Field,
    ResponseOption,
)


class MockProvider:
    """Deterministic mock embedding provider for testing.

    Returns fixed-seed random vectors. Tracks embed() call count and arguments.
    """

    def __init__(self, dimension: int = 32) -> None:
        self._model_name = "mock-model"
        self._dimension = dimension
        self.embed_calls: list[list[str]] = []

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed(self, texts: list[str]) -> NDArray[np.float32]:
        self.embed_calls.append(texts)
        rng = np.random.default_rng(seed=42)
        vectors = rng.standard_normal((len(texts), self._dimension)).astype(np.float32)
        # L2 normalize
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        return vectors / norms


def _make_dictionary(name: str, field_names: list[str]) -> DataDictionary:
    """Helper to create a simple DataDictionary for testing."""
    fields = {fname: Field(variable_name=fname, description=f"Description of {fname}") for fname in field_names}
    return DataDictionary(name=name, fields=fields)


class TestEmbedDictionary:
    """Tests for embed_dictionary() orchestrator."""

    def test_returns_embedded_dictionary_with_all_fields(self, tmp_path: Path) -> None:
        """embed_dictionary() returns EmbeddedDictionary with one embedding per field."""
        dd = _make_dictionary("test", ["age", "height", "weight"])
        provider = MockProvider(dimension=32)

        result = embed_dictionary(dd, provider=provider, cache_dir=tmp_path)

        assert isinstance(result, EmbeddedDictionary)
        assert len(result.embeddings) == 3
        assert set(result.embeddings.keys()) == {"age", "height", "weight"}

    def test_embeddings_are_float32_correct_dimension(self, tmp_path: Path) -> None:
        """All embeddings are float32 arrays of correct dimension."""
        dd = _make_dictionary("test", ["age", "height"])
        provider = MockProvider(dimension=32)

        result = embed_dictionary(dd, provider=provider, cache_dir=tmp_path)

        for vec in result.embeddings.values():
            assert vec.dtype == np.float32
            assert vec.shape == (32,)

    def test_get_all_vectors_shape(self, tmp_path: Path) -> None:
        """get_all_vectors() returns (N, D) ndarray with N = field count."""
        dd = _make_dictionary("test", ["age", "height", "weight"])
        provider = MockProvider(dimension=32)

        result = embed_dictionary(dd, provider=provider, cache_dir=tmp_path)
        matrix = result.get_all_vectors()

        assert matrix.shape == (3, 32)
        assert matrix.dtype == np.float32

    def test_get_variable_names_sorted_matching_vectors(self, tmp_path: Path) -> None:
        """get_variable_names() returns sorted list matching get_all_vectors() row order."""
        dd = _make_dictionary("test", ["weight", "age", "height"])
        provider = MockProvider(dimension=32)

        result = embed_dictionary(dd, provider=provider, cache_dir=tmp_path)
        names = result.get_variable_names()

        assert names == ["age", "height", "weight"]
        # Verify row order matches
        for i, name in enumerate(names):
            np.testing.assert_array_equal(result.get_all_vectors()[i], result.embeddings[name])

    def test_cache_hit_no_recomputation(self, tmp_path: Path) -> None:
        """Cache hit -- embed same dictionary twice, provider.embed() called only once."""
        dd = _make_dictionary("test", ["age", "height"])
        provider = MockProvider(dimension=32)

        embed_dictionary(dd, provider=provider, cache_dir=tmp_path)
        embed_dictionary(dd, provider=provider, cache_dir=tmp_path)

        assert len(provider.embed_calls) == 1

    def test_incremental_separate_dictionaries(self, tmp_path: Path) -> None:
        """Incremental -- embed dict A then dict B, provider.embed() only called for B's fields."""
        dd_a = _make_dictionary("dict_a", ["age", "height"])
        dd_b = _make_dictionary("dict_b", ["weight", "bmi"])
        provider = MockProvider(dimension=32)

        embed_dictionary(dd_a, provider=provider, cache_dir=tmp_path)
        embed_dictionary(dd_b, provider=provider, cache_dir=tmp_path)

        assert len(provider.embed_calls) == 2
        # Second call should only have dict_b's field texts
        assert len(provider.embed_calls[1]) == 2

    def test_mixed_cache_partial_hit(self, tmp_path: Path) -> None:
        """Mixed cache -- 3 fields where 1 is already cached, provider.embed() called with only 2 texts."""
        dd_small = _make_dictionary("small", ["age"])
        dd_full = _make_dictionary("full", ["age", "height", "weight"])
        provider = MockProvider(dimension=32)

        # Pre-cache "age"
        embed_dictionary(dd_small, provider=provider, cache_dir=tmp_path)
        assert len(provider.embed_calls) == 1

        # Now embed full dict -- "age" should be cached
        result = embed_dictionary(dd_full, provider=provider, cache_dir=tmp_path)
        assert len(provider.embed_calls) == 2
        assert len(provider.embed_calls[1]) == 2  # Only height and weight
        assert len(result.embeddings) == 3  # But result has all 3

    def test_progress_logging(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """Progress logging shows 'Embedding N fields: semantic (N), value (M produced, K skipped)'."""
        dd = _make_dictionary("test", ["age", "height", "weight"])
        provider = MockProvider(dimension=32)

        with caplog.at_level(logging.INFO, logger="ddharmon.embedding.service"):
            embed_dictionary(dd, provider=provider, cache_dir=tmp_path)

        assert any("Embedding 3 fields" in rec.message for rec in caplog.records)
        assert any("semantic (3)" in rec.message for rec in caplog.records)
        assert any("3 skipped" in rec.message for rec in caplog.records)


class TestFindSimilar:
    """Tests for find_similar() utility."""

    def test_returns_sorted_by_descending_similarity(self) -> None:
        """find_similar() returns indices sorted by descending similarity score."""
        rng = np.random.default_rng(seed=123)
        candidates = rng.standard_normal((10, 32)).astype(np.float32)
        norms = np.linalg.norm(candidates, axis=1, keepdims=True)
        candidates = candidates / norms

        query = candidates[3]  # Use one of the candidates as query
        results = find_similar(query, candidates, top_k=5)

        scores = [score for _, score in results]
        assert scores == sorted(scores, reverse=True)

    def test_top_k_limits_results(self) -> None:
        """find_similar() with top_k=3 returns exactly 3 results."""
        rng = np.random.default_rng(seed=123)
        candidates = rng.standard_normal((10, 32)).astype(np.float32)
        norms = np.linalg.norm(candidates, axis=1, keepdims=True)
        candidates = candidates / norms

        query = candidates[0]
        results = find_similar(query, candidates, top_k=3)

        assert len(results) == 3

    def test_normalized_scores_in_range(self) -> None:
        """find_similar() with normalized vectors produces scores in [-1, 1] range."""
        rng = np.random.default_rng(seed=123)
        candidates = rng.standard_normal((20, 32)).astype(np.float32)
        norms = np.linalg.norm(candidates, axis=1, keepdims=True)
        candidates = candidates / norms

        query = candidates[5]
        results = find_similar(query, candidates, top_k=20)

        for _, score in results:
            assert -1.0 <= score <= 1.0 + 1e-6, f"Score {score} out of range"


class TestEmbeddedDictionaryValueAccessors:
    """Tests for value_embeddings field and accessor methods on EmbeddedDictionary."""

    def _make_embedded_dict(
        self,
        semantic_keys: list[str],
        value_keys: list[str] | None = None,
        dim: int = 32,
    ) -> EmbeddedDictionary:
        """Helper: create EmbeddedDictionary with given semantic and value keys."""
        rng = np.random.default_rng(seed=99)
        dd = _make_dictionary("test", semantic_keys)
        semantic_embs = {k: rng.standard_normal(dim).astype(np.float32) for k in semantic_keys}
        value_embs: dict[str, NDArray[np.float32]] = {}
        if value_keys:
            value_embs = {k: rng.standard_normal(dim).astype(np.float32) for k in value_keys}
        return EmbeddedDictionary(
            dictionary=dd,
            embeddings=semantic_embs,
            model_name="mock",
            value_embeddings=value_embs,
        )

    def test_value_embeddings_default_empty(self) -> None:
        """EmbeddedDictionary with no value_embeddings kwarg has empty dict."""
        dd = _make_dictionary("test", ["age"])
        ed = EmbeddedDictionary(dictionary=dd, embeddings={}, model_name="mock")
        assert ed.value_embeddings == {}

    def test_get_value_variable_names_returns_sorted(self) -> None:
        """get_value_variable_names() returns sorted keys of value_embeddings."""
        ed = self._make_embedded_dict(["a", "b", "c"], value_keys=["c", "a"])
        assert ed.get_value_variable_names() == ["a", "c"]

    def test_get_value_variable_names_empty_when_no_values(self) -> None:
        """get_value_variable_names() returns [] when value_embeddings is empty."""
        ed = self._make_embedded_dict(["a", "b"])
        assert ed.get_value_variable_names() == []

    def test_get_value_vectors_shape(self) -> None:
        """get_value_vectors() returns (N, D) matrix for N fields with value embeddings."""
        ed = self._make_embedded_dict(["a", "b", "c"], value_keys=["b", "c"], dim=16)
        matrix = ed.get_value_vectors()
        assert matrix.shape == (2, 16)
        assert matrix.dtype == np.float32

    def test_get_value_vectors_row_order_matches_names(self) -> None:
        """get_value_vectors() rows match get_value_variable_names() order."""
        ed = self._make_embedded_dict(["x", "y", "z"], value_keys=["z", "x"])
        names = ed.get_value_variable_names()
        matrix = ed.get_value_vectors()
        for i, name in enumerate(names):
            np.testing.assert_array_equal(matrix[i], ed.value_embeddings[name])

    def test_get_value_vectors_raises_when_empty(self) -> None:
        """get_value_vectors() raises ValueError when no value embeddings exist."""
        ed = self._make_embedded_dict(["a", "b"])
        with pytest.raises(ValueError, match="no value embeddings"):
            ed.get_value_vectors()

    def test_get_all_vectors_unchanged_with_value_embeddings(self) -> None:
        """get_all_vectors() still returns semantic vectors only, even when value_embeddings populated."""
        ed = self._make_embedded_dict(["a", "b"], value_keys=["a", "b"])
        semantic_matrix = ed.get_all_vectors()
        assert semantic_matrix.shape[0] == 2
        # Verify these are the semantic vectors, not value vectors
        names = ed.get_variable_names()
        for i, name in enumerate(names):
            np.testing.assert_array_equal(semantic_matrix[i], ed.embeddings[name])

    def test_get_variable_names_unchanged_with_value_embeddings(self) -> None:
        """get_variable_names() returns semantic embedding keys only."""
        ed = self._make_embedded_dict(["a", "b"], value_keys=["a", "b", "extra_value"])
        assert ed.get_variable_names() == ["a", "b"]


class TextTrackingProvider:
    """Mock provider that records all embedded texts for content verification.

    Returns unique vectors per text (hash-seeded) so different texts produce
    different embeddings. Tracks all embed() calls and texts.
    """

    def __init__(self, dimension: int = 32) -> None:
        self._model_name = "text-tracker"
        self._dimension = dimension
        self.all_texts: list[str] = []
        self.embed_call_count: int = 0

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed(self, texts: list[str]) -> NDArray[np.float32]:
        self.embed_call_count += 1
        self.all_texts.extend(texts)
        vectors = np.zeros((len(texts), self._dimension), dtype=np.float32)
        for i, t in enumerate(texts):
            seed = hash(t) % (2**31)
            rng = np.random.default_rng(seed=seed)
            vec = rng.standard_normal(self._dimension).astype(np.float32)
            vectors[i] = vec / np.linalg.norm(vec)
        return vectors


def _make_dictionary_with_values(name: str) -> DataDictionary:
    """Create a dictionary with fields that have response options (value metadata).

    Returns dict with:
    - "gender": has response_options -> should get value embedding
    - "smoker": has response_options + data_type -> should get value embedding
    - "age": no response_options, no data_type, no units -> should NOT get value embedding
    """
    gender = Field(
        variable_name="gender",
        description="Biological sex of participant",
        question_text="What is your biological sex?",
        category="Demographics",
        response_options=[
            ResponseOption(code="1", label="Male"),
            ResponseOption(code="2", label="Female"),
        ],
    )
    smoker = Field(
        variable_name="smoker",
        description="Smoking status",
        question_text="Do you currently smoke?",
        category="Lifestyle",
        response_options=[
            ResponseOption(code="0", label="No"),
            ResponseOption(code="1", label="Yes"),
        ],
        data_type="categorical",
    )
    # age has no response_options, no data_type, no units -> empty value text
    age = Field(
        variable_name="age",
        description="Age of participant at enrollment",
        question_text="What is your age?",
        category="Demographics",
    )
    fields = {f.variable_name: f for f in [gender, smoker, age]}
    return DataDictionary(name=name, fields=fields)


class TestDualVectorEmbedding:
    """Tests for dual semantic + value vector production in embed_dictionary()."""

    def test_embed_produces_both_semantic_and_value_embeddings(self, tmp_path: Path) -> None:
        """embed_dictionary() populates both .embeddings and .value_embeddings."""
        dd = _make_dictionary_with_values("test")
        provider = TextTrackingProvider(dimension=32)

        result = embed_dictionary(dd, provider=provider, cache_dir=tmp_path)

        # All 3 fields get semantic embeddings
        assert len(result.embeddings) == 3
        # Only gender and smoker have response_options -> value embeddings
        assert len(result.value_embeddings) == 2
        assert set(result.value_embeddings.keys()) == {"gender", "smoker"}

    def test_fields_without_value_metadata_skipped(self, tmp_path: Path) -> None:
        """Fields with empty compose_value_text() output have no value embedding."""
        dd = _make_dictionary("no_values", ["plain_field"])
        provider = TextTrackingProvider(dimension=32)

        result = embed_dictionary(dd, provider=provider, cache_dir=tmp_path)

        assert len(result.embeddings) == 1
        assert len(result.value_embeddings) == 0

    def test_semantic_text_excludes_response_options(self, tmp_path: Path) -> None:
        """Semantic embedding text does NOT contain response option labels."""
        dd = _make_dictionary_with_values("test")
        provider = TextTrackingProvider(dimension=32)

        embed_dictionary(dd, provider=provider, cache_dir=tmp_path)

        # Provider gets called with semantic texts then value texts
        # Semantic texts should NOT contain "Male", "Female", "Yes", "No" (response labels)
        # Value texts SHOULD contain them
        semantic_texts = []
        value_texts = []
        for text in provider.all_texts:
            # Value texts contain response option labels joined by ";"
            if "Male" in text or "No; Yes" in text:
                value_texts.append(text)
            else:
                semantic_texts.append(text)

        # There should be 3 semantic texts (one per field) and 2 value texts
        assert len(semantic_texts) == 3
        assert len(value_texts) == 2

    def test_cache_stores_both_types_separately(self, tmp_path: Path) -> None:
        """Re-running embed_dictionary() loads both semantic and value from cache (0 new)."""
        dd = _make_dictionary_with_values("test")
        provider = TextTrackingProvider(dimension=32)

        # First run: all new
        embed_dictionary(dd, provider=provider, cache_dir=tmp_path)
        first_run_texts = len(provider.all_texts)

        # Second run: all cached
        result2 = embed_dictionary(dd, provider=provider, cache_dir=tmp_path)
        second_run_texts = len(provider.all_texts) - first_run_texts

        assert second_run_texts == 0, "Expected 0 new embeddings on second run (all cached)"
        assert len(result2.embeddings) == 3
        assert len(result2.value_embeddings) == 2

    def test_custom_text_composer_no_value_embeddings(self, tmp_path: Path) -> None:
        """Custom text_composer produces only semantic vectors, no value embeddings."""
        dd = _make_dictionary_with_values("test")
        provider = TextTrackingProvider(dimension=32)

        def custom_composer(fld: Field, dd: DataDictionary) -> str:
            return f"custom: {fld.variable_name}"

        result = embed_dictionary(dd, provider=provider, cache_dir=tmp_path, text_composer=custom_composer)

        assert len(result.embeddings) == 3
        assert len(result.value_embeddings) == 0

    def test_existing_callers_unaffected(self, tmp_path: Path) -> None:
        """embed_dictionary() with simple fields (no value metadata) works exactly as before."""
        dd = _make_dictionary("simple", ["age", "height", "weight"])
        provider = MockProvider(dimension=32)

        result = embed_dictionary(dd, provider=provider, cache_dir=tmp_path)

        assert len(result.embeddings) == 3
        assert result.value_embeddings == {}
        assert result.get_variable_names() == ["age", "height", "weight"]
        assert result.get_all_vectors().shape == (3, 32)
