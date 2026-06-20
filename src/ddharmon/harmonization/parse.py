"""Parse LLM responses for the classify-only adopt/refine/novel pass.

Robust JSON extraction: models occasionally narrate before the JSON, wrap
it in ``` fences, or append trailing commentary. We never drop a sub-cluster
over our own parse strictness.
"""

from __future__ import annotations

import json
import re

_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def extract_json(text: str) -> dict:
    """Pull a JSON object out of an LLM response that may wrap it in prose/fences.

    Strategy: strip a fenced block if present, then ``raw_decode`` at each ``{``
    and keep the widest dict that decodes. ``raw_decode`` consumes exactly one
    JSON value and ignores trailing data, so prose preambles, nested objects, and
    trailing commentary are all handled.
    """
    t = text.strip()
    m = _FENCE_RE.search(t)
    if m:
        t = m.group(1).strip()

    decoder = json.JSONDecoder()
    best: dict | None = None
    best_span = -1
    i = t.find("{")
    while i != -1:
        try:
            obj, end = decoder.raw_decode(t, i)
        except json.JSONDecodeError:
            obj, end = None, i
        if isinstance(obj, dict) and end - i > best_span:
            best, best_span = obj, end - i
        i = t.find("{", i + 1)

    if best is not None:
        return best
    return json.loads(t)  # nothing decoded — raise a clear JSONDecodeError


def payload_from_response(resp: object) -> dict:
    """Extract a JSON payload dict from a runner/batch response record.

    The runner stores valid-JSON output inline as a parsed object, so ``resp`` is
    usually already a dict. Anthropic-style ``{"content": "<json string>"}``
    wrappers and raw string fallbacks (prose + fenced JSON) are also supported.
    """
    if isinstance(resp, dict):
        if "content" in resp and isinstance(resp["content"], str):
            return extract_json(resp["content"])
        return resp
    return extract_json(str(resp))


def parse_verdict_payload(resp: object) -> dict | None:
    """Parse one A/R/N response into its payload dict, or None on parse failure.

    Returns the dict with at least ``verdict``; callers attach cluster context.
    """
    try:
        payload = payload_from_response(resp)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(payload, dict) or "verdict" not in payload:
        return None
    return payload
