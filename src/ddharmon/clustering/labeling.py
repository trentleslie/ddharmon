"""Cluster labeling: derived labels (no API key) and optional LLM upgrade.

Derived labels use word frequency from member field descriptions.
LLM labels accept an optional callable for richer domain concept names.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from collections.abc import Callable

from ddharmon.models.cluster import FieldCluster

logger = logging.getLogger(__name__)

# Comprehensive stopwords including generic data dictionary terms
STOPWORDS: frozenset[str] = frozenset(
    {
        "the",
        "a",
        "an",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "could",
        "should",
        "may",
        "might",
        "shall",
        "can",
        "need",
        "dare",
        "ought",
        "used",
        "to",
        "of",
        "in",
        "for",
        "on",
        "with",
        "at",
        "by",
        "from",
        "as",
        "into",
        "through",
        "during",
        "before",
        "after",
        "above",
        "below",
        "between",
        "out",
        "off",
        "over",
        "under",
        "again",
        "further",
        "then",
        "once",
        "here",
        "there",
        "when",
        "where",
        "why",
        "how",
        "all",
        "each",
        "every",
        "both",
        "few",
        "more",
        "most",
        "other",
        "some",
        "such",
        "no",
        "nor",
        "not",
        "only",
        "own",
        "same",
        "so",
        "than",
        "too",
        "very",
        "just",
        "don",
        "now",
        "and",
        "or",
        "but",
        "if",
        "that",
        "which",
        "what",
        "this",
        "these",
        "those",
        "it",
        "its",
        "your",
        "you",
        "please",
        "field",
        "variable",
        "question",
        "data",
        "type",
        "value",
        "values",
        "participant",
        "subject",
        "respondent",
    }
)


def derive_cluster_label(member_descriptions: list[str], top_n: int = 3) -> str:
    """Extract a descriptive label from member field descriptions using word frequency.

    Tokenizes descriptions, removes stopwords and short tokens, and joins
    the top-N most common words as a title-case label.

    Args:
        member_descriptions: List of field description strings.
        top_n: Number of top words to include in the label.

    Returns:
        Title-cased label like "Body / Mass / Index" or "Unlabeled cluster".
    """
    words: list[str] = []
    for desc in member_descriptions:
        tokens = re.findall(r"[a-z]+", desc.lower())
        words.extend(t for t in tokens if t not in STOPWORDS and len(t) > 2)

    common = Counter(words).most_common(top_n)
    if not common:
        return "Unlabeled cluster"
    return " / ".join(word for word, _ in common).title()


def label_clusters_llm(
    clusters: list[FieldCluster],
    llm_call: Callable[[str], str],
) -> list[FieldCluster]:
    """Upgrade cluster labels using an LLM.

    Sends member names and descriptions as a prompt, expects a concise
    domain concept noun phrase back.

    Args:
        clusters: List of FieldCluster to label.
        llm_call: A callable that takes a prompt string and returns a response string.

    Returns:
        Same clusters with updated labels.
    """
    for cluster in clusters:
        members_text = "\n".join(f"- {m.variable_name}: {m.description}" for m in cluster.members)
        prompt = (
            "Given these data dictionary fields that belong to the same semantic cluster, "
            "provide a concise domain concept label (2-4 words, noun phrase style). "
            "Only output the label, nothing else.\n\n"
            f"Fields:\n{members_text}"
        )
        try:
            label = llm_call(prompt).strip()
            if label:
                cluster.label = label
                logger.debug("LLM label for cluster %d: %s", cluster.cluster_id, label)
        except Exception:
            logger.warning("LLM labeling failed for cluster %d, keeping derived label", cluster.cluster_id)

    return clusters
