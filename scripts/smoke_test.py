#!/usr/bin/env python3
"""Post-install smoke test for ddharmon.

Run this INSIDE the environment where ddharmon is installed (e.g. the ephemeral
venv created by ``scripts/verify_release.py``). It asserts the installed
distribution version and exercises real package operations on tiny embedded toy
data — no external fixtures, no network, no API keys.

Modes:
  (default)  core: version + value-encoding parsing + ingestion + CLI entry point.
             Uses only the base install (no ML extras).
  --full     additionally runs the embed -> cluster -> sub-cluster -> anchor
             pipeline on toy dictionaries. Requires ``ddharmon[all]`` and is
             slow (downloads an embedding model). The classify (LLM) step is
             skipped, so no API key is needed.

Exit code 0 = every check passed; non-zero = at least one failure.

Usage:
    python smoke_test.py --expected 0.5.0 [--full]
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
import sys
import tempfile


class SmokeTestError(Exception):
    """Raised by a check when an assertion about the installed package fails."""


def _run_check(label: str, fn, failures: list[str]) -> None:
    try:
        fn()
        print(f"  PASS  {label}")
    except Exception as exc:  # noqa: BLE001 - smoke test reports every failure, never aborts early
        print(f"  FAIL  {label}: {exc}")
        failures.append(label)


# --- core checks -----------------------------------------------------------


def check_distribution_version(expected: str) -> None:
    import importlib.metadata as metadata

    got = metadata.version("ddharmon")
    if got != expected:
        raise SmokeTestError(f"installed distribution version {got!r} != expected {expected!r}")


def check_dunder_version(expected: str) -> None:
    import ddharmon

    if ddharmon.__version__ != expected:
        raise SmokeTestError(f"ddharmon.__version__ {ddharmon.__version__!r} != expected {expected!r}")


def check_value_encoding() -> None:
    from ddharmon.values import parse_value_encoding

    opts = parse_value_encoding("1=Yes|2=No")
    codes = [o.code for o in opts]
    labels = [o.label for o in opts]
    if codes != ["1", "2"] or labels != ["Yes", "No"]:
        raise SmokeTestError(f"parse_value_encoding gave codes={codes} labels={labels}")


def check_ingestion() -> None:
    from ddharmon.ingestion import load_dictionary, preprocess_dictionary

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "toy.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["var", "desc", "vals"])
            writer.writerow(["age_years", "Age in years", "(1) 18-65|(2) Over 65"])
            writer.writerow(["sex", "Biological sex", "(1) Male|(2) Female"])

        dd = load_dictionary(
            path,
            "ToyCohort",
            variable_name="var",
            description="desc",
            value_encoding="vals",
        )
        if len(dd.fields) != 2:
            raise SmokeTestError(f"expected 2 fields, got {len(dd.fields)}: {list(dd.fields)}")

        age = dd.fields.get("age_years")
        if age is None or not age.response_options:
            raise SmokeTestError("value_encoding column was not parsed into response_options")

        # Preprocessing must run cleanly on a real dictionary.
        preprocess_dictionary(dd)


def check_cli_entry_point(expected: str) -> None:
    exe = os.path.join(os.path.dirname(sys.executable), "ddharmon")
    if not os.path.exists(exe):
        exe = shutil.which("ddharmon")
    if not exe:
        raise SmokeTestError("`ddharmon` console script not found on the install")

    result = subprocess.run([exe, "--version"], capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise SmokeTestError(f"`ddharmon --version` exited {result.returncode}: {result.stderr.strip()}")
    if expected not in result.stdout:
        raise SmokeTestError(f"`ddharmon --version` output {result.stdout.strip()!r} missing {expected!r}")


# --- optional deep check ---------------------------------------------------


def check_full_pipeline() -> None:
    """Exercise the heavy ``ddharmon[all]`` embedding stack on toy data.

    Requires ``ddharmon[all]``; slow on first run (downloads an embedding
    model). This proves the optional ML dependencies (sentence-transformers /
    torch) import and produce real vectors for the installed wheel — the part
    of a release most likely to break under the extras. It deliberately stops
    at embedding: the full HDBSCAN/BERTopic clustering needs realistic data
    volume and a CDE catalogue, so it is not a reliable toy-data smoke signal.
    """
    try:
        from ddharmon.embedding import SentenceTransformerProvider, embed_dictionary
    except ImportError as exc:
        raise SmokeTestError(f"deep mode requires `ddharmon[all]` (missing: {exc.name})") from exc

    from ddharmon.ingestion import load_dictionary

    rows = [
        ("age_years", "Age of participant in years", "(1) 18-40|(2) 41-65|(3) Over 65"),
        ("sex", "Biological sex of participant", "(1) Male|(2) Female"),
        ("smoke_status", "Current smoking status", "(0) No|(1) Yes"),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "toy.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["var", "desc", "vals"])
            writer.writerows(rows)
        dd = load_dictionary(path, "CohortA", variable_name="var", description="desc", value_encoding="vals")

        provider = SentenceTransformerProvider()
        embedded = embed_dictionary(dd, provider=provider)

        if not embedded.embeddings:
            raise SmokeTestError("embed_dictionary produced no embeddings")
        vectors = embedded.get_all_vectors()
        if vectors.shape[0] != len(dd.fields) or vectors.shape[1] < 1:
            raise SmokeTestError(f"unexpected embedding matrix shape {vectors.shape} for {len(dd.fields)} fields")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Smoke test an installed ddharmon.")
    parser.add_argument("--expected", required=True, help="expected installed version, e.g. 0.5.0")
    parser.add_argument("--full", action="store_true", help="also run the [all] embed->cluster pipeline")
    args = parser.parse_args(argv)

    print(f"ddharmon smoke test — expecting version {args.expected} ({'full' if args.full else 'core'} mode)")
    failures: list[str] = []

    _run_check("distribution version", lambda: check_distribution_version(args.expected), failures)
    _run_check("__version__ matches", lambda: check_dunder_version(args.expected), failures)
    _run_check("value-encoding parsing", check_value_encoding, failures)
    _run_check("ingestion + preprocessing on toy data", check_ingestion, failures)
    _run_check("CLI entry point (ddharmon --version)", lambda: check_cli_entry_point(args.expected), failures)

    if args.full:
        _run_check("full embed->cluster->anchor pipeline", check_full_pipeline, failures)

    print()
    if failures:
        print(f"SMOKE TEST FAILED — {len(failures)} check(s) failed: {', '.join(failures)}")
        return 1
    print("SMOKE TEST PASSED — all checks green")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
