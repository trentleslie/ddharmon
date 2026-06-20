#!/usr/bin/env bash
# Process exported reranking prompts through Claude Code CLI.
#
# Usage:
#   ./scripts/process_prompts.sh prompts.jsonl responses.jsonl
#
# Requires: claude CLI (Claude Code), jq
#
# Each prompt is sent to claude --print with the system prompt prepended.
# Responses are written as JSONL: {"id": "...", "response": {...}}
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

# Clear output file
: > "$RESPONSES_FILE"

while IFS= read -r line; do
    DONE=$((DONE + 1))
    ID=$(echo "$line" | jq -r '.id')
    SYSTEM=$(echo "$line" | jq -r '.system_prompt')
    USER_PROMPT=$(echo "$line" | jq -r '.user_prompt')
    # Per-prompt schema reminder. Falls back to the pairwise-reranker schema for
    # back-compat with src/ddharmon/matching/prompt_export.py records that don't
    # set their own (no "schema" field → jq returns "null").
    SCHEMA=$(echo "$line" | jq -r '.schema // empty')

    echo "[$DONE/$TOTAL] Processing: $ID"

    if [ -z "$SCHEMA" ]; then
        SCHEMA='{
  "judgments": [
    {
      "candidate_variable": "variable_name",
      "relation": "exact|broader|narrower|composite|derivable|no_match",
      "confidence": 0.0,
      "rationale": "brief explanation"
    }
  ]
}'
    fi

    FULL_PROMPT="$SYSTEM

Respond with ONLY valid JSON matching this schema (no markdown fences):
$SCHEMA

$USER_PROMPT"

    # Send to Claude Code CLI (--print outputs to stdout without interactive mode)
    RESPONSE=$(echo "$FULL_PROMPT" | claude --print 2>/dev/null) || {
        echo "  WARNING: claude failed for $ID, writing empty response" >&2
        # Empty object is schema-agnostic — downstream parsers .get() to defaults.
        RESPONSE='{}'
    }

    # Try to parse as JSON; if it fails, wrap as raw string for later parsing
    # Use jq -c to compact response onto a single line (claude output may contain newlines)
    if echo "$RESPONSE" | jq -c . &>/dev/null; then
        COMPACT=$(echo "$RESPONSE" | jq -c .)
        echo "{\"id\": $(echo "$ID" | jq -R .), \"response\": $COMPACT}" >> "$RESPONSES_FILE"
    else
        echo "{\"id\": $(echo "$ID" | jq -R .), \"response\": $(echo "$RESPONSE" | jq -Rs .)}" >> "$RESPONSES_FILE"
    fi
done < "$PROMPTS_FILE"

echo "Done. $DONE responses written to $RESPONSES_FILE"
