"""Basic tests to verify project setup."""

from __future__ import annotations

import importlib.metadata


def test_import_ddharmon():
    """Verify ddharmon package can be imported."""
    import ddharmon

    assert ddharmon.__version__


def test_version_matches_distribution_metadata():
    """__version__ is single-sourced from the installed distribution metadata,
    so it never drifts from pyproject.toml's version."""
    import ddharmon

    assert ddharmon.__version__ == importlib.metadata.version("ddharmon")


def test_version_string_format():
    """Verify version string looks like a release version (e.g. X.Y.Z)."""
    import ddharmon

    parts = ddharmon.__version__.split(".")
    assert len(parts) >= 2
    # leading components are numeric (allow suffixes like 1.2.3rc1 on the last part)
    assert parts[0].isdigit() and parts[1].isdigit()
