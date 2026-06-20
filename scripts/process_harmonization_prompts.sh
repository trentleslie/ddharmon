#!/usr/bin/env bash
# Process exported harmonization prompts through Claude Code CLI.
#
# Usage:
#   ./scripts/process_harmonization_prompts.sh prompts.jsonl responses.jsonl
#
# Requires: claude CLI (Claude Code), jq
#
# Each prompt is sent to claude --print as-is. Unlike process_prompts.sh,
# this does NOT inject a hard-coded JSON schema — the harmonization templates
# already include their own JSON-output instructions in the prompt body.
# Responses are written as JSONL: {"id": "t{eid}_s{sub}", "response": <json|str>}
set -euo pipefail

PROMPTS_FILE="${1:?Usage: $0 <prompts.jsonl> <responses.jsonl>}"
RESPONSES_FILE="${2:?Usage: $0 <prompts.jsonl> <responses.jsonl>}"

if ! command -v claude &>/dev/null; then
    echo "Error: 'claude' CLI not found. Install Claude Code first." >&2
    exit 1
fi
if ! command -v jq &>/dev/null; then
    echo "Error: 'jq' not found. Install with: brew install jq" >&2
    exit 1
fi

TOTAL=$(wc -l < "$PROMPTS_FILE" | tr -d ' ')
DONE=0

: > "$RESPONSES_FILE"

while IFS= read -r line; do
    DONE=$((DONE + 1))
    ID=$(echo "$line" | jq -r '.id')
    USER_PROMPT=$(echo "$line" | jq -r '.user_prompt')

    echo "[$DONE/$TOTAL] Processing: $ID"

    RESPONSE=$(echo "$USER_PROMPT" | claude --print 2>/dev/null) || {
        echo "  WARNING: claude failed for $ID, writing empty response" >&2
        RESPONSE='{}'
    }

    # Try to parse as JSON; if it fails, wrap as raw string for later parsing.
    if echo "$RESPONSE" | jq -c . &>/dev/null; then
        COMPACT=$(echo "$RESPONSE" | jq -c .)
        echo "{\"id\": $(echo "$ID" | jq -R .), \"response\": $COMPACT}" >> "$RESPONSES_FILE"
    else
        echo "{\"id\": $(echo "$ID" | jq -R .), \"response\": $(echo "$RESPONSE" | jq -Rs .)}" >> "$RESPONSES_FILE"
    fi
done < "$PROMPTS_FILE"

echo "Done. $DONE response(s) written to $RESPONSES_FILE"
