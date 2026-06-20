"""LLM client layer for reranking candidate field matches.

Public types are re-exported **lazily** (PEP 562) so lightweight entry points
don't drag in the whole stack. In particular the Batch API submitter
(``submit_batch`` / ``retrieve_batch``) needs only ``anthropic`` + stdlib — it
must stay importable in a thin environment that has no pydantic / no
sentence-transformers, which is the whole point of the offline "export prompts
anywhere, submit the batch separately" workflow.

The heavy (pydantic-backed) modules — ``base``, ``prompts``, ``cached_client``
— are imported only on first access of the symbol that needs them, e.g.
``ddharmon.llm.RerankerResponse``.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

__all__ = [
    "AnthropicClient",
    "BaseLLMClient",
    "CachedLLMClient",
    "CandidateJudgment",
    "OpenAIClient",
    "RerankerResponse",
    "get_client",
    "resume_and_wait",
    "retrieve_batch",
    "submit_and_wait",
    "submit_batch",
]

if TYPE_CHECKING:  # import-time only for type checkers; never at runtime
    from ddharmon.llm.anthropic_client import AnthropicClient
    from ddharmon.llm.base import BaseLLMClient
    from ddharmon.llm.cached_client import CachedLLMClient
    from ddharmon.llm.openai_client import OpenAIClient
    from ddharmon.llm.prompts import CandidateJudgment, RerankerResponse

# symbol → submodule that defines it. submit_batch/retrieve_batch live in
# ``batch`` (anthropic + stdlib only); everything else pulls in pydantic.
_LAZY_EXPORTS = {
    "submit_batch": "ddharmon.llm.batch",
    "submit_and_wait": "ddharmon.llm.batch",
    "resume_and_wait": "ddharmon.llm.batch",
    "retrieve_batch": "ddharmon.llm.batch",
    "BaseLLMClient": "ddharmon.llm.base",
    "CachedLLMClient": "ddharmon.llm.cached_client",
    "CandidateJudgment": "ddharmon.llm.prompts",
    "RerankerResponse": "ddharmon.llm.prompts",
    "AnthropicClient": "ddharmon.llm.anthropic_client",
    "OpenAIClient": "ddharmon.llm.openai_client",
}


def __getattr__(name: str) -> object:
    """PEP 562 lazy attribute resolution for the package's public exports."""
    target = _LAZY_EXPORTS.get(name)
    if target is not None:
        module = importlib.import_module(target)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)


def get_client(provider: str = "anthropic", **kwargs: object) -> BaseLLMClient:
    """Factory function to create an LLM client by provider name.

    Args:
        provider: One of 'anthropic' or 'openai'.
        **kwargs: Passed to the client constructor (e.g., model_name, max_tokens).

    Returns:
        A BaseLLMClient implementation.

    Raises:
        ValueError: If provider is not recognized.
    """
    if provider == "anthropic":
        from ddharmon.llm.anthropic_client import AnthropicClient

        return AnthropicClient(**kwargs)  # type: ignore[arg-type]
    elif provider == "openai":
        from ddharmon.llm.openai_client import OpenAIClient

        return OpenAIClient(**kwargs)  # type: ignore[arg-type]
    else:
        raise ValueError(f"Unknown LLM provider: {provider!r}. Choose 'anthropic' or 'openai'.")
