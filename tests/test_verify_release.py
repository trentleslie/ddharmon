"""Tests for scripts/verify_release.py.

The PyPI poll and venv orchestration are exercised with mocks (no network, no
real installs). The end-to-end run against a real published version is a manual
release-time check, not a unit test.
"""

from __future__ import annotations

import importlib.util
import subprocess
import urllib.error
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "verify_release", Path(__file__).resolve().parents[1] / "scripts" / "verify_release.py"
)
vr = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(vr)


class _Resp:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_pypi_version_live_true(monkeypatch):
    monkeypatch.setattr(vr.urllib.request, "urlopen", lambda *a, **k: _Resp())
    assert vr.pypi_version_live("ddharmon", "0.5.0") is True


def test_pypi_version_live_false_on_404(monkeypatch):
    def _raise(*a, **k):
        raise urllib.error.HTTPError("u", 404, "not found", {}, None)

    monkeypatch.setattr(vr.urllib.request, "urlopen", _raise)
    assert vr.pypi_version_live("ddharmon", "9.9.9") is False


def test_wait_for_pypi_success(monkeypatch):
    monkeypatch.setattr(vr, "pypi_version_live", lambda p, v: True)
    assert vr.wait_for_pypi("ddharmon", "0.5.0", timeout=5) is True


def test_wait_for_pypi_times_out_cleanly(monkeypatch):
    # never live + zero timeout -> returns False without hanging
    monkeypatch.setattr(vr, "pypi_version_live", lambda p, v: False)
    monkeypatch.setattr(vr.time, "sleep", lambda s: None)
    assert vr.wait_for_pypi("ddharmon", "9.9.9", timeout=0) is False


def test_wait_for_pypi_retries_transient_errors(monkeypatch):
    # A 5xx (URLError subclass) must be swallowed as "not yet live", then a
    # subsequent success returns True -- no raw traceback escapes.
    calls = {"n": 0}

    def _flaky(p, v):
        calls["n"] += 1
        if calls["n"] == 1:
            raise urllib.error.HTTPError("u", 503, "service unavailable", {}, None)
        return True

    monkeypatch.setattr(vr, "pypi_version_live", _flaky)
    monkeypatch.setattr(vr.time, "sleep", lambda s: None)
    assert vr.wait_for_pypi("ddharmon", "0.5.0", timeout=5) is True
    assert calls["n"] == 2


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("install failed"),
        subprocess.CalledProcessError(1, ["uv", "venv"]),  # uv venv non-zero exit
        FileNotFoundError("uv"),  # uv missing from PATH
    ],
)
def test_verify_cleans_up_tempdir_on_install_failure(monkeypatch, exc):
    monkeypatch.setattr(vr, "wait_for_pypi", lambda *a, **k: True)
    created: dict[str, str] = {}
    real_mkdtemp = vr.tempfile.mkdtemp

    def _tracking_mkdtemp(*a, **k):
        path = real_mkdtemp(*a, **k)
        created["dir"] = path
        return path

    monkeypatch.setattr(vr.tempfile, "mkdtemp", _tracking_mkdtemp)

    def _boom(*a, **k):
        raise exc

    monkeypatch.setattr(vr, "install_into_venv", _boom)

    # No traceback escapes; clean failure signal + tempdir cleaned up.
    assert vr.verify("0.5.0", full=False, timeout=1) is False
    assert not Path(created["dir"]).exists()


def test_verify_returns_false_when_pypi_never_appears(monkeypatch):
    monkeypatch.setattr(vr, "wait_for_pypi", lambda *a, **k: False)
    # install must never be reached if PyPI confirmation fails
    monkeypatch.setattr(vr, "install_into_venv", lambda *a, **k: pytest.fail("should not install"))
    assert vr.verify("9.9.9", full=False, timeout=1) is False
