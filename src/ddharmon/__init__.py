"""
ddharmon - Data Dictionary Harmonization Tool

Harmonizes data dictionaries across studies: identifies clusters of equivalent
variables and recommends a Common Data Element (CDE) anchor for each, routed to
expert review.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version

try:
    # Single source of truth: the installed distribution version (from pyproject.toml).
    # This guarantees ddharmon.__version__ never drifts from the published package.
    __version__ = _dist_version("ddharmon")
except PackageNotFoundError:  # pragma: no cover - only when running from an uninstalled tree
    __version__ = "0+unknown"
