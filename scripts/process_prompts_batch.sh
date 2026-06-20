#!/usr/bin/env bash
# Process exported prompts through the Anthropic Message Batches API.
#
# Drop-in alternative to process_prompts.sh — same input/output JSONL format,
# but ~50% cheaper (Batch API discount) and not constrained by Claude Code
# subscription usage windows.
#
# Usage:
#   ./scripts/process_prompts_batch.sh prompts.jsonl responses.jsonl              # submit + poll until done
#   ./scripts/process_prompts_batch.sh prompts.jsonl responses.jsonl --submit     # submit only, print batch_id
#   ./scripts/process_prompts_batch.sh prompts.jsonl responses.jsonl --retrieve   # retrieve only (reads sidecar)
#
# Requires:
#   - ANTHROPIC_API_KEY in env
#   - Python with ``anthropic`` installed (the project's uv-managed env)
#   - The ddharmon package importable (``uv sync`` from repo root)
#
# Sidecar file:
#   <prompts.jsonl basename>.batch_id holds the batch_id between submit and
#   retrieve. Re-running the script picks up where it left off; delete the
#   sidecar to force re-submission.
set -euo pipefail

PROMPTS_FILE="${1:?Usage: $0 <prompts.jsonl> <responses.jsonl> [--submit|--retrieve]}"
RESPONSES_FILE="${2:?Usage: $0 <prompts.jsonl> <responses.jsonl> [--submit|--retrieve]}"
MODE="${3:-blocking}"

case "$MODE" in
    blocking|--submit|--retrieve) ;;
    *) echo "Error: unknown mode '$MODE'. Use --submit or --retrieve, or omit for blocking." >&2; exit 2 ;;
esac

# Sidecar lives next to the prompts file so multi-pass workflows (multiple
# JSONLs) each get their own batch_id without collision. The manifest (written
# by submit_batch) holds the custom_id→original-id map retrieve_batch needs to
# restore the original ids (e.g. "cluster:5").
SIDECAR="${PROMPTS_FILE}.batch_id"
MANIFEST="${PROMPTS_FILE}.batch_manifest.json"

# Resolve a python binary that has the project's deps. Prefer uv-managed env,
# fall back to system python if uv isn't installed.
if command -v uv &>/dev/null; then
    PY=(uv run python)
elif command -v python3 &>/dev/null; then
    PY=(python3)
else
    echo "Error: neither 'uv' nor 'python3' found." >&2
    exit 1
fi

submit() {
    if [[ -f "$SIDECAR" ]]; then
        echo "Sidecar already exists at $SIDECAR — refusing to re-submit." >&2
        echo "Delete $SIDECAR or run with --retrieve to fetch existing batch." >&2
        exit 3
    fi
    echo "Submitting $(wc -l < "$PROMPTS_FILE" | tr -d ' ') prompts from $PROMPTS_FILE..."
    # Model selection: by default submit_batch reads each record's model_tag
    # (the notebook is the single source of truth). Set BATCH_MODEL to override
    # every request — e.g. BATCH_MODEL=claude-sonnet-4-6 ./process_prompts_batch.sh ...
    MODEL_ARG=""
    if [[ -n "${BATCH_MODEL:-}" ]]; then
        MODEL_ARG=", model='${BATCH_MODEL}'"
        echo "  Overriding model for all requests: ${BATCH_MODEL}"
    fi
    BATCH_ID=$("${PY[@]}" -c "
from ddharmon.llm.batch import submit_batch
bid = submit_batch('$PROMPTS_FILE'${MODEL_ARG})
print(bid)
") || { echo "Submit failed (see traceback above). Sidecar not written." >&2; exit 5; }
    if [[ -z "$BATCH_ID" ]]; then
        echo "Submit returned no batch id. Sidecar not written." >&2
        exit 5
    fi
    echo "$BATCH_ID" > "$SIDECAR"
    echo "Batch submitted: $BATCH_ID"
    echo "  Sidecar:  $SIDECAR"
    echo "  Manifest: $MANIFEST"
}

retrieve() {
    if [[ ! -f "$SIDECAR" ]]; then
        echo "Error: sidecar $SIDECAR not found. Submit first." >&2
        exit 4
    fi
    BATCH_ID=$(cat "$SIDECAR")
    echo "Retrieving batch $BATCH_ID..."
    N=$("${PY[@]}" -c "
from ddharmon.llm.batch import retrieve_batch
n = retrieve_batch('$BATCH_ID', '$RESPONSES_FILE', manifest_path='$MANIFEST')
print(n)
")
    echo "$N"
}

poll_until_done() {
    # Sleep cadence: 60s default. Batch SLA is 24h but most runs finish in
    # under an hour for ~700-prompt workloads. Caller can ctrl-C and re-run
    # later — the sidecar holds the batch_id.
    SLEEP_SECS="${BATCH_POLL_SECS:-60}"
    while true; do
        N=$(retrieve)
        # retrieve_batch() prints "Status: ..." when still processing,
        # "Done: ..." when ended (regardless of success/error counts). Grep
        # for the prefix to distinguish — relying on count alone would loop
        # forever if the batch ended with all errors (count=0).
        echo "$N"
        if echo "$N" | grep -q "^Done:"; then
            COUNT=$(echo "$N" | tail -1)
            echo ""
            echo "Batch ended. Responses written: $COUNT → $RESPONSES_FILE"
            # Clear sidecar so the next run on this prompts file starts
            # fresh. Even on all-errors we clear it — the user needs to
            # rebuild prompts before resubmitting anyway.
            rm -f "$SIDECAR"
            return 0
        fi
        echo "Still processing — sleeping ${SLEEP_SECS}s..."
        sleep "$SLEEP_SECS"
    done
}

case "$MODE" in
    --submit)
        submit
        ;;
    --retrieve)
        retrieve
        ;;
    blocking)
        if [[ ! -f "$SIDECAR" ]]; then
            submit
        else
            echo "Sidecar exists at $SIDECAR — resuming poll on existing batch."
        fi
        poll_until_done
        ;;
esac
