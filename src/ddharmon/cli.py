"""Command-line entry point for ddharmon.

Wired to the ``ddharmon`` console script via ``[project.scripts]`` in
``pyproject.toml`` (``ddharmon = "ddharmon.cli:main"``). Intentionally minimal —
a versioned, help-bearing entry point that subcommands can hang off later.
"""

from __future__ import annotations

import click

from ddharmon import __version__


@click.group(
    invoke_without_command=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.version_option(__version__, "-V", "--version", prog_name="ddharmon")
@click.pass_context
def main(ctx: click.Context) -> None:
    """ddharmon — Data Dictionary Harmonization Tool.

    Cluster equivalent variables across data dictionaries and anchor them to
    Common Data Elements (CDEs). See the README and notebooks for the pipeline;
    this CLI is a thin entry point that will grow subcommands over time.
    """
    # With no subcommand, show help and exit cleanly rather than a usage error.
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


if __name__ == "__main__":  # pragma: no cover
    main()
