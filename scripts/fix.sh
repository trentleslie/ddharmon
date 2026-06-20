#!/usr/bin/env bash
# Auto-fix linting and formatting issues
# Uses venv directly, falls back to uv run

echo "========================================"
echo "Auto-fixing ddharmon code issues"
echo "========================================"

# Find the right runner
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_BIN="$PROJECT_DIR/.venv/bin"

if [ -d "$VENV_BIN" ]; then
    RUN="$VENV_BIN/"
elif command -v uv &>/dev/null; then
    RUN="uv run "
else
    echo "Error: No .venv found and uv not installed"
    exit 1
fi

# Ruff fix (including import sorting)
echo ""
echo ">>> Ruff --fix (linting + import sorting)..."
${RUN}ruff check --fix src/ tests/ || true

# Black formatting
echo ""
echo ">>> Black (formatting)..."
${RUN}black src/ tests/ || true

echo ""
echo "========================================"
echo "Done! Run ./scripts/check.sh to verify."
echo "========================================"
