"""Cached LLM client that reads pre-computed responses from a JSONL file.

Workflow:
    1. Export prompts with ``export_reranking_prompts()``
    2. Process prompts locally (e.g., via ``claude`` CLI) → responses JSONL
    3. Use ``CachedLLMClient(responses_path)`` as a drop-in replacement for
       AnthropicClient/OpenAIClient — no API key needed.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from ddharmon.llm.base import BaseLLMClient
from ddharmon.llm.prompts import RerankerResponse

logger = logging.getLogger(__name__)


def _parse_json_response(text: str) -> RerankerResponse:
    """Parse a JSON response, stripping markdown fences if present."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3].strip()
    return RerankerResponse.model_validate(json.loads(text))


class CachedLLMClient(BaseLLMClient):
    """LLM client that serves pre-computed responses from a JSONL file.

    Each line in the JSONL file must have:
        {"id": "<source_variable>", "response": <RerankerResponse JSON>}

    The ``response`` value can be either:
        - A parsed JSON object (dict) with a ``judgments`` key
        - A raw string (will be parsed, stripping markdown fences if needed)

    Args:
        responses_path: Path to the JSONL file with pre-computed responses.
        model_label: Label to report as model_name (default: "cached").
    """

    def __init__(self, responses_path: str | Path, model_label: str = "cached") -> None:
        self._model_label = model_label
        self._responses: dict[str, RerankerResponse] = {}
        self._load(Path(responses_path))

    def _load(self, path: Path) -> None:
        with open(path) as f:
            for _lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                source_id = record["id"]
                raw = record["response"]
                if isinstance(raw, str):
                    self._responses[source_id] = _parse_json_response(raw)
                else:
                    self._responses[source_id] = RerankerResponse.model_validate(raw)
        logger.info("Loaded %d cached responses from %s", len(self._responses), path)

    @property
    def provider_name(self) -> str:
        return "cached"

    @property
    def model_name(self) -> str:
        return self._model_label

    def rerank_candidates(
        self,
        source_context: dict[str, str],
        candidate_contexts: list[dict[str, str]],
        candidate_names: list[str],
    ) -> RerankerResponse:
        source_var = source_context.get("variable", "")
        if source_var not in self._responses:
            logger.warning("No cached response for '%s' — returning empty judgments", source_var)
            return RerankerResponse(judgments=[])
        return self._responses[source_var]
