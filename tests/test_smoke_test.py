"""Tests for scripts/smoke_test.py (the post-install smoke test).

These run the script as a subprocess against the current (dev-installed)
ddharmon, exercising the same code path verify_release.py uses inside the
ephemeral venv. The --full path is intentionally not covered here: it downloads
an embedding model and is validated manually / in deep-mode release runs.
"""

from __future__ import annotations

import importlib.metadata
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "smoke_test.py"


def test_core_smoke_passes_for_installed_version():
    version = importlib.metadata.version("ddharmon")
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--expected", version],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SMOKE TEST PASSED" in result.stdout


def test_smoke_fails_on_version_mismatch():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--expected", "9.9.9"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "SMOKE TEST FAILED" in result.stdout
    assert "distribution version" in result.stdout
