#!/usr/bin/env python3
"""Post-publish release verification for ddharmon.

Run this AFTER a release is published. It is a diagnostic gate, not a rollback:
a PyPI version can never be re-uploaded, so by the time this runs the release is
already permanent. Its job is to catch a broken release early and loudly.

Steps:
  1. Confirm the version is live on pypi.org (poll the JSON API; tolerant of
     index/CDN propagation lag).
  2. Create an ephemeral virtual environment (``uv venv``) and install the exact
     pinned version from PyPI (``ddharmon==X.Y.Z``, or ``ddharmon[all]==...``
     with --full).
  3. Run scripts/smoke_test.py with the venv's Python so it imports the installed
     wheel, asserting the version and exercising package operations on toy data.

Exit code 0 = released artifact verified; non-zero = verification failed (triage
a published, irreversible release — e.g. yank + patch release).

Usage:
    python verify_release.py 0.5.0 [--full] [--timeout 300]
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

PACKAGE = "ddharmon"
SMOKE_TEST = Path(__file__).resolve().parent / "smoke_test.py"


def pypi_version_live(package: str, version: str) -> bool:
    """True if the exact version is served by the PyPI JSON API (200)."""
    url = f"https://pypi.org/pypi/{package}/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310 - fixed https PyPI URL
            return resp.status == 200
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def wait_for_pypi(package: str, version: str, timeout: float, sleep: float = 10.0) -> bool:
    """Poll the PyPI JSON API until the version is live or the timeout elapses."""
    deadline = time.monotonic() + timeout
    attempt = 0
    while True:
        attempt += 1
        if pypi_version_live(package, version):
            print(f"  PyPI: {package} {version} is live (attempt {attempt}).")
            return True
        if time.monotonic() >= deadline:
            print(f"  PyPI: {package} {version} not visible after {timeout:.0f}s ({attempt} attempts).")
            return False
        print(f"  PyPI: {package} {version} not visible yet; retrying in {sleep:.0f}s...")
        time.sleep(sleep)


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, capture_output=True, text=True)


def install_into_venv(venv_dir: Path, spec: str, retries: int = 3, sleep: float = 15.0) -> Path:
    """Create an ephemeral venv and install ``spec`` from PyPI; return its python."""
    _run(["uv", "venv", str(venv_dir)]).check_returncode()
    py = venv_dir / "bin" / "python"
    last = None
    for attempt in range(1, retries + 1):
        result = _run(["uv", "pip", "install", "--python", str(py), spec])
        if result.returncode == 0:
            return py
        last = result
        print(f"  install attempt {attempt}/{retries} failed (propagation lag?); retrying in {sleep:.0f}s...")
        if attempt < retries:
            time.sleep(sleep)
    raise RuntimeError(f"could not install {spec}:\n{(last.stderr if last else '').strip()}")


def verify(version: str, *, full: bool, timeout: float) -> bool:
    """Run the full verification flow. Returns True on success."""
    print(f"Verifying {PACKAGE} {version} ({'full' if full else 'core'} mode)\n")

    print("[1/3] Confirming the release is live on pypi.org")
    if not wait_for_pypi(PACKAGE, version, timeout):
        return False

    tmp = Path(tempfile.mkdtemp(prefix="ddharmon-verify-"))
    try:
        print("\n[2/3] Installing the exact version into an ephemeral venv")
        spec = f"{PACKAGE}[all]=={version}" if full else f"{PACKAGE}=={version}"
        venv_python = install_into_venv(tmp / "venv", spec)

        print("\n[3/3] Running smoke test against the installed wheel")
        cmd = [str(venv_python), str(SMOKE_TEST), "--expected", version]
        if full:
            cmd.append("--full")
        smoke = subprocess.run(cmd, text=True)
        return smoke.returncode == 0
    except RuntimeError as exc:
        print(f"  install failed: {exc}")
        return False
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify a published ddharmon release.")
    parser.add_argument("version", help="released version to verify, e.g. 0.5.0")
    parser.add_argument("--full", action="store_true", help="install [all] and run the deep pipeline smoke")
    parser.add_argument("--timeout", type=float, default=300.0, help="seconds to wait for PyPI propagation")
    args = parser.parse_args(argv)

    ok = verify(args.version, full=args.full, timeout=args.timeout)

    print()
    if ok:
        print(f"VERIFIED — {PACKAGE} {args.version} installs from PyPI and passes the smoke test.")
        return 0
    print(
        f"VERIFICATION FAILED for {PACKAGE} {args.version}.\n"
        "The release is already published and PyPI versions are immutable, so this is a\n"
        "diagnostic failure to triage — investigate, then yank and cut a patch release if\n"
        "the artifact is broken. (This step does not and cannot roll back the publish.)"
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
