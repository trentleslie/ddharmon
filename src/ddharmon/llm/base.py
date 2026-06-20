"""Abstract base class for LLM providers."""

from __future__ import annotations

from abc import ABC, abstractmethod

from ddharmon.llm.prompts import RerankerResponse


class BaseLLMClient(ABC):
    """Abstract base class for LLM providers used in reranking.

    Mirrors the EmbeddingProvider ABC pattern from the embedding layer.
    All implementations must be synchronous (no async).
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Name of the LLM provider (e.g., 'anthropic', 'openai')."""
        ...

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Model identifier (e.g., 'claude-sonnet-4-5-20250514', 'gpt-4o')."""
        ...

    @abstractmethod
    def rerank_candidates(
        self,
        source_context: dict[str, str],
        candidate_contexts: list[dict[str, str]],
        candidate_names: list[str],
    ) -> RerankerResponse:
        """Rerank candidate matches for a source field using LLM judgment.

        Args:
            source_context: Dict with keys: variable, description, options,
                data_type, units, validation, category, codes, parent_context.
            candidate_contexts: List of dicts with same keys as source_context.
            candidate_names: List of candidate variable names (parallel to candidate_contexts).

        Returns:
            RerankerResponse with a judgment for each candidate.
        """
        ...

    def complete(self, prompt: str, *, system: str | None = None, max_tokens: int = 512) -> str:
        """Send a plain text prompt and return a plain text response.

        Default implementation raises NotImplementedError. Subclasses that
        support generic completion (not just reranking) should override this.

        Args:
            prompt: The user prompt text.
            system: Optional system prompt.
            max_tokens: Maximum response tokens.

        Returns:
            Plain text response string.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement complete(). "
            "Override complete() to enable generic LLM calls (e.g., cluster labeling)."
        )
