"""Export and import LLM reranking prompts for offline processing.

Workflow (export → process → import):
    1. ``export_reranking_prompts()`` → prompts.jsonl
    2. Run ``scripts/process_prompts.sh`` → responses.jsonl
    3. Use ``CachedLLMClient(responses.jsonl)``
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from ddharmon.embedding.service import EmbeddedDictionary
from ddharmon.llm.prompts import RERANKER_SYSTEM_PROMPT, build_reranker_prompt
from ddharmon.matching.candidate_retrieval import build_field_context, retrieve_candidates
from ddharmon.matching.pairwise_mapper import MatchingConfig

logger = logging.getLogger(__name__)


def export_reranking_prompts(
    source: EmbeddedDictionary,
    target: EmbeddedDictionary,
    output_path: str | Path,
    config: MatchingConfig | None = None,
) -> int:
    """Run cosine retrieval and export all LLM reranking prompts to a JSONL file.

    Each line contains:
        {
            "id": "<source_variable>",
            "system_prompt": "<system prompt text>",
            "user_prompt": "<fully formatted user prompt>",
            "candidates": ["<target_var_1>", ...],
            "cosine_scores": {"<target_var>": <score>, ...}
        }

    Fields with no candidates above the cosine threshold are skipped.

    Args:
        source: Embedded source dictionary.
        target: Embedded target dictionary.
        output_path: Where to write the JSONL file.
        config: Optional matching configuration (uses defaults if None).

    Returns:
        Number of prompts exported.
    """
    if config is None:
        config = MatchingConfig()

    output_path = Path(output_path)

    # Step 1: Cosine retrieval
    t0 = time.perf_counter()
    candidates = retrieve_candidates(
        source,
        target,
        top_k=config.top_k,
        cosine_threshold=config.cosine_threshold,
        semantic_weight=config.semantic_weight,
        value_weight=config.value_weight,
    )
    logger.info("Cosine retrieval: %.2fs", time.perf_counter() - t0)

    # Step 2: Build prompts and write JSONL
    count = 0
    with open(output_path, "w") as f:
        for src_var in sorted(source.embeddings.keys()):
            src_candidates = candidates.get(src_var, [])
            if not src_candidates:
                continue

            src_field = source.dictionary.fields[src_var]
            source_context = build_field_context(src_field, source.dictionary)

            candidate_contexts: list[dict[str, str]] = []
            candidate_names: list[str] = []
            cosine_scores: dict[str, float] = {}

            for tgt_var, cosine_score in src_candidates:
                tgt_field = target.dictionary.fields[tgt_var]
                ctx = build_field_context(tgt_field, target.dictionary)
                candidate_contexts.append(ctx)
                candidate_names.append(tgt_var)
                cosine_scores[tgt_var] = round(cosine_score, 6)

            user_prompt = build_reranker_prompt(source_context, candidate_contexts)

            record = {
                "id": src_var,
                "system_prompt": RERANKER_SYSTEM_PROMPT,
                "user_prompt": user_prompt,
                "candidates": candidate_names,
                "cosine_scores": cosine_scores,
            }
            f.write(json.dumps(record) + "\n")
            count += 1

    logger.info("Exported %d prompts to %s", count, output_path)
    return count
