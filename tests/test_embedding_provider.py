"""Tests for embedding provider ABC and SentenceTransformerProvider."""

from __future__ import annotations

import numpy as np
import pytest

from ddharmon.embedding.provider import EmbeddingProvider


class MockProvider(EmbeddingProvider):
    """Mock provider implementing EmbeddingProvider for interface compliance."""

    def __init__(self, model_name: str = "mock-model", dimension: int = 128):
        self._model_name = model_name
        self._dimension = dimension

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed(self, texts: list[str]) -> np.ndarray:
        rng = np.random.default_rng(42)
        vecs = rng.standard_normal((len(texts), self._dimension)).astype(np.float32)
        # L2-normalize
        norms = np.linalg.norm(vecs, axis=1, keepdims=True)
        return vecs / norms


class TestEmbeddingProviderABC:
    """Tests for EmbeddingProvider abstract base class."""

    def test_cannot_instantiate_abc_directly(self) -> None:
        """EmbeddingProvider cannot be instantiated directly."""
        with pytest.raises(TypeError, match="abstract"):
            EmbeddingProvider()  # type: ignore[abstract]

    def test_mock_provider_satisfies_interface(self) -> None:
        """A mock provider implementing EmbeddingProvider satisfies the interface."""
        provider = MockProvider()
        assert isinstance(provider, EmbeddingProvider)
        assert provider.model_name == "mock-model"
        assert provider.dimension == 128
        result = provider.embed(["hello"])
        assert result.shape == (1, 128)
        assert result.dtype == np.float32


# All SentenceTransformerProvider tests require sentence-transformers
st = pytest.importorskip("sentence_transformers")


class TestSentenceTransformerProvider:
    """Tests for SentenceTransformerProvider with real model."""

    @pytest.fixture(scope="class")
    def provider(self):
        from ddharmon.embedding.provider import SentenceTransformerProvider

        return SentenceTransformerProvider()

    @pytest.fixture(scope="class")
    def minilm_provider(self):
        from ddharmon.embedding.provider import SentenceTransformerProvider

        return SentenceTransformerProvider(model_name="all-MiniLM-L6-v2")

    def test_model_name_returns_configured_string(self, provider) -> None:
        """SentenceTransformerProvider.model_name returns the configured model string."""
        assert provider.model_name == "all-mpnet-base-v2"

    def test_dimension_returns_768(self, provider) -> None:
        """SentenceTransformerProvider.dimension returns 768 for all-mpnet-base-v2."""
        assert provider.dimension == 768

    def test_embed_returns_correct_shape(self, provider) -> None:
        """embed(["hello", "world"]) returns ndarray shape (2, 768) with float32 dtype."""
        result = provider.embed(["hello", "world"])
        assert isinstance(result, np.ndarray)
        assert result.shape == (2, 768)
        assert result.dtype == np.float32

    def test_embeddings_are_l2_normalized(self, provider) -> None:
        """Returned embeddings are L2-normalized (norm ~= 1.0 for each row)."""
        result = provider.embed(["hello", "world", "test sentence"])
        norms = np.linalg.norm(result, axis=1)
        np.testing.assert_allclose(norms, 1.0, atol=1e-5)

    def test_custom_model_name_dimension(self, minilm_provider) -> None:
        """Custom model name accepted (all-MiniLM-L6-v2 returns dimension 384)."""
        assert minilm_provider.model_name == "all-MiniLM-L6-v2"
        assert minilm_provider.dimension == 384

    def test_custom_model_embed_shape(self, minilm_provider) -> None:
        """Custom model embeds to correct dimension."""
        result = minilm_provider.embed(["test"])
        assert result.shape == (1, 384)
