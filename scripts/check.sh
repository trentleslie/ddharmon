#!/usr/bin/env bash
# Run all code quality checks
# Uses venv directly, falls back to uv run

echo "========================================"
echo "Running ddharmon code quality checks"
echo "========================================"

# Find the right runner
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_BIN="$PROJECT_DIR/.venv/bin"

if [ -d "$VENV_BIN" ]; then
    # Call the venv binary directly. Quoting "$VENV_BIN/$1" keeps paths with
    # spaces (e.g. "Google Drive") intact.
    run() { "$VENV_BIN/$1" "${@:2}"; }
elif command -v uv &>/dev/null; then
    run() { uv run "$@"; }
else
    echo "Error: No .venv found and uv not installed"
    exit 1
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Track failures
FAILED=0

# Ruff linting
echo ""
echo ">>> Ruff (linting)..."
if run ruff check src/ tests/; then
    echo -e "${GREEN}✓ Ruff passed${NC}"
else
    echo -e "${RED}✗ Ruff failed${NC}"
    FAILED=1
fi

# Black formatting check
echo ""
echo ">>> Black (formatting)..."
if run black --check src/ tests/; then
    echo -e "${GREEN}✓ Black passed${NC}"
else
    echo -e "${RED}✗ Black failed (run ./scripts/fix.sh to auto-fix)${NC}"
    FAILED=1
fi

# Pyright type checking
echo ""
echo ">>> Pyright (type checking)..."
if run pyright src/; then
    echo -e "${GREEN}✓ Pyright passed${NC}"
else
    echo -e "${RED}✗ Pyright failed${NC}"
    FAILED=1
fi

# Pytest
echo ""
echo ">>> Pytest (tests)..."
if run pytest tests/ -q; then
    echo -e "${GREEN}✓ Pytest passed${NC}"
else
    echo -e "${RED}✗ Pytest failed${NC}"
    FAILED=1
fi

# Summary
echo ""
echo "========================================"
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All checks passed!${NC}"
    exit 0
else
    echo -e "${RED}Some checks failed${NC}"
    exit 1
fi
