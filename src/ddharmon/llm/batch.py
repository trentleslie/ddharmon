"""Anthropic Message Batches API integration for offline LLM reranking.

Workflow:
    1. Export prompts with ``export_reranking_prompts()`` → prompts.jsonl
    2. ``submit_batch(prompts_path)`` → submits to Anthropic Batch API, returns batch_id
    3. ``retrieve_batch(batch_id, output_path)`` → polls status, writes responses.jsonl
    4. ``CachedLLMClient(output_path)`` → drop-in client for ``match_dictionaries()``

Cost: 50% cheaper than standard API calls. Processing: up to 24h (usually much faster).
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# Anthropic Batch API constrains custom_id to ^[a-zA-Z0-9_-]{1,64}$. Our record
# ids use a `prefix:suffix` scheme (e.g. "cluster:5", "harmonize:5") whose colon
# is illegal. We sanitize ids to legal custom_ids on submit and restore the
# originals on retrieve (via the manifest's id_map) so downstream parsers — and
# the subscription-path responses they also consume — see the original ids.
_CUSTOM_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


def _safe_custom_id(original: str, used: set[str]) -> str:
    """Map an arbitrary record id to a Batch-API-legal, batch-unique custom_id."""
    if _CUSTOM_ID_RE.match(original):
        candidate = original
    else:
        candidate = re.sub(r"[^a-zA-Z0-9_-]", "_", original)[:64] or "id"
    if candidate in used:
        base = candidate[:60]
        i = 1
        while f"{base}_{i}" in used:
            i += 1
        candidate = f"{base}_{i}"
    used.add(candidate)
    return candidate


# Default schema instruction for prompts that don't carry their own ``schema``
# field (back-compat with pairwise-reranker exports from before per-prompt
# schemas existed). Per-prompt schemas supersede this when the record sets a
# ``schema`` field. Mirrors ``scripts/process_prompts.sh``.
_DEFAULT_PAIRWISE_SCHEMA = """{
  "judgments": [
    {
      "candidate_variable": "variable_name",
      "relation": "exact|broader|narrower|composite|derivable|no_match",
      "confidence": 0.0,
      "rationale": "brief explanation"
    }
  ]
}"""

_SCHEMA_PREAMBLE = "\n\nRespond with ONLY valid JSON matching this schema (no markdown fences):\n"

# Used only when a record carries no ``model_tag`` and no explicit ``model`` is
# passed (e.g. legacy pairwise-reranker exports). Current Sonnet.
_FALLBACK_MODEL = "claude-sonnet-4-6"


def submit_batch(
    prompts_path: str | Path,
    *,
    model: str | None = None,
    max_tokens: int = 2048,
    manifest_path: str | Path | None = None,
) -> str:
    """Submit exported prompts to the Anthropic Message Batches API.

    Each prompts.jsonl record may carry an optional ``schema`` field; when
    present it is appended to the per-record system prompt (mirroring
    ``scripts/process_prompts.sh``). When absent, falls back to the pairwise-
    reranker schema for back-compat with older exports.

    Model selection is per-record so the prompts file stays the single source
    of truth (no silent divergence between what a notebook stamped and what the
    batch actually runs). Precedence per request:
    explicit ``model`` arg > record's ``model_tag`` > ``_FALLBACK_MODEL``.

    Args:
        prompts_path: Path to JSONL file with records ``{id, system_prompt,
            user_prompt, schema?, model_tag?}``. Compatible with both
            ``export_reranking_prompts()`` (no schema/model_tag) and the
            multi-pass pipeline (per-record schema + model_tag).
        model: Optional override applied to every request. When ``None`` (the
            default), each request uses its own ``model_tag``.
        max_tokens: Max response tokens per request.
        manifest_path: Where to save batch metadata. Defaults to
            ``<prompts_dir>/batch_manifest.json``.

    Returns:
        Batch ID string for use with ``retrieve_batch()``.
    """
    import anthropic

    prompts_path = Path(prompts_path)
    if manifest_path is None:
        # Per-prompts-file so a multi-pass workflow (j_signal / labels /
        # harmonize / spec) doesn't clobber a single shared manifest.
        manifest_path = prompts_path.parent / f"{prompts_path.name}.batch_manifest.json"
    manifest_path = Path(manifest_path)

    # Read prompts and build batch requests
    requests = []
    models_used: set[str] = set()
    id_map: dict[str, str] = {}  # custom_id (legal) -> original record id
    used_ids: set[str] = set()
    with open(prompts_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            schema = record.get("schema") or _DEFAULT_PAIRWISE_SCHEMA
            system = record["system_prompt"] + _SCHEMA_PREAMBLE + schema
            req_model = model or record.get("model_tag") or _FALLBACK_MODEL
            # Per-record max_tokens so a long-output pass (e.g. harmonize spec
            # authoring) can request more headroom than short coherence calls,
            # without truncating mid-JSON. Falls back to the function default.
            req_max_tokens = int(record.get("max_tokens") or max_tokens)
            models_used.add(req_model)
            custom_id = _safe_custom_id(record["id"], used_ids)
            id_map[custom_id] = record["id"]
            requests.append(
                {
                    "custom_id": custom_id,
                    "params": {
                        "model": req_model,
                        "max_tokens": req_max_tokens,
                        "system": system,
                        "messages": [{"role": "user", "content": record["user_prompt"]}],
                    },
                }
            )

    logger.info("Submitting %d requests to Batch API (models=%s)", len(requests), sorted(models_used))

    client = anthropic.Anthropic()
    batch = client.messages.batches.create(requests=requests)

    # Save manifest for later retrieval. id_map lets retrieve_batch restore the
    # original record ids that downstream parsers expect.
    manifest = {
        "batch_id": batch.id,
        "models": sorted(models_used),
        "num_requests": len(requests),
        "prompts_path": str(prompts_path),
        "id_map": id_map,
        "created_at": batch.created_at.isoformat() if batch.created_at else None,
        "expires_at": batch.expires_at.isoformat() if batch.expires_at else None,
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    logger.info("Batch submitted: %s (manifest: %s)", batch.id, manifest_path)

    return batch.id


def retrieve_batch(
    batch_id: str,
    output_path: str | Path,
    *,
    manifest_path: str | Path | None = None,
) -> int:
    """Check batch status and write responses if complete.

    Re-run this function until it returns > 0.

    Args:
        batch_id: Batch ID from ``submit_batch()``.
        output_path: Where to write the responses JSONL (compatible with
            ``CachedLLMClient``).
        manifest_path: Optional path to the manifest written by ``submit_batch``.
            When provided, its ``id_map`` restores the original record ids
            (e.g. "cluster:5") that downstream parsers expect, reversing the
            custom_id sanitization applied at submit time. Without it, the
            sanitized custom_ids are written as-is.

    Returns:
        Number of responses written, or 0 if batch is still processing.
    """
    import anthropic

    output_path = Path(output_path)

    id_map: dict[str, str] = {}
    if manifest_path is not None and Path(manifest_path).exists():
        with open(manifest_path) as mf:
            id_map = json.load(mf).get("id_map", {})

    client = anthropic.Anthropic()

    batch = client.messages.batches.retrieve(batch_id)
    counts = batch.request_counts

    logger.info(
        "Batch %s: status=%s, succeeded=%d, processing=%d, errored=%d, expired=%d, canceled=%d",
        batch_id,
        batch.processing_status,
        counts.succeeded,
        counts.processing,
        counts.errored,
        counts.expired,
        counts.canceled,
    )

    if batch.processing_status != "ended":
        print(f"Status: {batch.processing_status}")
        print(f"  Succeeded: {counts.succeeded}")
        print(f"  Processing: {counts.processing}")
        print(f"  Errored: {counts.errored}")
        return 0

    # Batch is done — stream results
    written = 0
    errors = 0
    with open(output_path, "w") as f:
        for result in client.messages.batches.results(batch_id):
            original_id = id_map.get(result.custom_id, result.custom_id)
            if result.result.type == "succeeded":
                text = result.result.message.content[0].text
                try:
                    parsed = _parse_response_text(text)
                    record = {"id": original_id, "response": parsed}
                except (json.JSONDecodeError, Exception) as e:
                    logger.warning("Failed to parse response for %s: %s", original_id, e)
                    record = {"id": original_id, "response": text}
                f.write(json.dumps(record) + "\n")
                written += 1
            else:
                errors += 1
                # Surface the actual error body (e.g. invalid model, rate
                # limit) instead of just the bare type — otherwise a uniform
                # failure like a bad model id is undiagnosable from the log.
                detail = getattr(result.result, "error", None)
                logger.warning(
                    "Non-success result for %s: type=%s detail=%s",
                    original_id,
                    result.result.type,
                    detail,
                )
                if errors <= 3:
                    print(f"  [error] {original_id}: {result.result.type} — {detail}")

    logger.info("Wrote %d responses to %s (%d errors)", written, output_path, errors)
    print(f"Done: {written} responses written to {output_path}")
    if errors:
        print(f"  {errors} requests failed (errored/canceled/expired)")
    return written


def submit_and_wait(
    prompts_path: str | Path,
    output_path: str | Path,
    *,
    model: str | None = None,
    max_tokens: int = 2048,
    poll_secs: int = 60,
    manifest_path: str | Path | None = None,
) -> int:
    """Submit a batch and block until it ends, writing responses.

    Convenience wrapper for running a batch inline (e.g. from a notebook cell)
    without the export → terminal → upload hop. Submits, polls
    ``processing_status`` until ``"ended"`` (distinguishing genuinely-done from
    still-processing via the status field, not the written count), then writes
    responses with original ids restored.

    Requires outbound HTTPS to the Anthropic API and ``ANTHROPIC_API_KEY`` in
    the environment. On an air-gapped host use ``submit_batch`` / ``retrieve_batch``
    across machines (or ``scripts/process_prompts_batch.sh``) instead.

    Returns:
        Number of responses written.
    """
    import time

    import anthropic

    prompts_path = Path(prompts_path)
    if manifest_path is None:
        manifest_path = prompts_path.parent / f"{prompts_path.name}.batch_manifest.json"

    batch_id = submit_batch(prompts_path, model=model, max_tokens=max_tokens, manifest_path=manifest_path)
    print(f"Submitted batch {batch_id}; polling every {poll_secs}s until it ends...")

    client = anthropic.Anthropic()
    while True:
        batch = client.messages.batches.retrieve(batch_id)
        if batch.processing_status == "ended":
            break
        c = batch.request_counts
        print(f"  {batch.processing_status}: succeeded={c.succeeded} processing={c.processing} errored={c.errored}")
        time.sleep(poll_secs)

    return retrieve_batch(batch_id, output_path, manifest_path=manifest_path)


def _ids_in_jsonl(path: str | Path) -> set[str]:
    """Set of record ids present in a JSONL file (skips blank lines)."""
    ids: set[str] = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                ids.add(json.loads(line)["id"])
    return ids


def resume_and_wait(
    prompts_path: str | Path,
    output_path: str | Path,
    *,
    model: str | None = None,
    max_tokens: int = 2048,
    poll_secs: int = 60,
) -> int:
    """Submit ONLY the prompts whose ids are missing from an existing responses
    file, then append the recovered responses — without re-running what already
    succeeded or overwriting the file.

    A prompt id is "missing" when ``output_path`` has no record for it. The
    Batch API path writes a record for every request that *succeeded* and none
    for ones that errored / expired / were canceled (see ``retrieve_batch``), so
    the gap is exactly the failures — e.g. a transient ``OverloadedError``.
    Resuming fills those at the cost of just the stragglers, instead of
    ``submit_and_wait`` resubmitting the whole file and clobbering prior results.

    Parse failures are NOT a gap: they ARE written (as raw text) and the
    response parsers recover them, so resubmitting wouldn't help and isn't done.

    Idempotent — safe to re-run: when nothing is missing it's a no-op; a
    straggler that errors again simply stays missing for the next attempt.

    Returns:
        Number of responses newly appended (0 if nothing was missing).
    """
    prompts_path = Path(prompts_path)
    output_path = Path(output_path)

    # No prior responses to resume from → just run the full pass.
    if not output_path.exists():
        return submit_and_wait(prompts_path, output_path, model=model, max_tokens=max_tokens, poll_secs=poll_secs)

    have = _ids_in_jsonl(output_path)
    missing_lines: list[str] = []
    with open(prompts_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if json.loads(line)["id"] not in have:
                missing_lines.append(line)

    total = len(have) + len(missing_lines)
    if not missing_lines:
        print(f"Nothing to resume: all {len(have)} prompt ids already have responses in {output_path}")
        return 0
    print(f"Resuming {len(missing_lines)} missing prompt(s) of {total} total (errored/expired on a prior run)")

    # Submit just the gap as its own batch to sidecar files, then append the
    # recovered responses into the main output. Sidecars are cleaned up after.
    gap_prompts = prompts_path.parent / f"{prompts_path.name}.resume"
    gap_responses = output_path.parent / f"{output_path.name}.resume"
    gap_manifest = gap_prompts.parent / f"{gap_prompts.name}.batch_manifest.json"
    gap_prompts.write_text("\n".join(missing_lines) + "\n")

    try:
        n = submit_and_wait(
            gap_prompts,
            gap_responses,
            model=model,
            max_tokens=max_tokens,
            poll_secs=poll_secs,
            manifest_path=gap_manifest,
        )
        with open(gap_responses) as src, open(output_path, "a") as dst:
            for line in src:
                if line.strip():
                    dst.write(line)
    finally:
        for p in (gap_prompts, gap_responses, gap_manifest):
            p.unlink(missing_ok=True)

    still_missing = len(missing_lines) - n
    msg = f"Appended {n} recovered response(s) to {output_path}"
    if still_missing:
        msg += f"; {still_missing} still failed — re-run this to retry"
    print(msg)
    return n


def _parse_response_text(text: str) -> dict:
    """Parse LLM response text to a JSON dict, tolerating prose preamble,
    markdown fences, AND trailing commentary after the closing brace.

    Some models narrate before emitting the JSON and/or wrap it in
    ```json ... ``` despite a "JSON only" instruction, and some append a
    remark after the object — occasionally with braces of its own, which
    defeats a naive first-``{`` to last-``}`` slice (json.loads then raises
    "Extra data"). Strategy: strip a fenced block if present, then try
    ``raw_decode`` at each ``{`` and keep the widest dict that decodes.
    ``raw_decode`` consumes exactly one JSON value and ignores trailing data,
    so a response never lands in the raw-text error branch over parse
    strictness we can recover from here."""
    t = text.strip()
    m = re.search(r"```(?:json)?\s*(.*?)\s*```", t, re.DOTALL)
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
