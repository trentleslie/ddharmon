"""Tests for the ddharmon console entry point (ddharmon.cli:main)."""

from __future__ import annotations

from click.testing import CliRunner

import ddharmon
from ddharmon.cli import main


def test_cli_version_prints_single_sourced_version():
    result = CliRunner().invoke(main, ["--version"])
    assert result.exit_code == 0
    assert ddharmon.__version__ in result.output


def test_cli_help_exits_zero_and_names_program():
    result = CliRunner().invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "ddharmon" in result.output.lower()


def test_cli_no_args_shows_help():
    result = CliRunner().invoke(main, [])
    assert result.exit_code == 0
    assert "usage" in result.output.lower()
