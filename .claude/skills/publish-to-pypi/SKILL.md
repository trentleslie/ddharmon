---
name: publish-to-pypi
description: Use when cutting a new ddharmon release to PyPI — bumping the version, updating the changelog, tagging, and publishing the package to https://pypi.org/project/ddharmon/ via the GitHub Release → Trusted Publishing workflow. Triggers on "release ddharmon", "publish to pypi", "cut a release", "bump the version and publish".
---

# Publishing ddharmon to PyPI

## How releases work here

- **Canonical development *and* publishing** both happen in `Phenome-Health/ddharmon`.
  Anyone with write access here (e.g. Bhargav) can cut a release.
- Trent owns the `ddharmon` name on PyPI and registers this repo as the trusted publisher,
  so no token is needed and releases don't depend on Trent personally.
- `trentleslie/ddharmon` is a content mirror (used for Greptile reviews) and does **not** publish.
- Publishing is automated by [`.github/workflows/publish.yml`](../../../.github/workflows/publish.yml):
  when a **GitHub Release is published**, it builds the sdist + wheel and uploads them
  to PyPI using **Trusted Publishing (OIDC)** — no API token is stored anywhere.

## One-time setup (PyPI owner only — Trent)

Do this once on PyPI before the first OIDC release; it cannot be done by anyone except a
project owner, and the publish job fails auth until it exists:

PyPI → project `ddharmon` → **Settings → Publishing → Add a trusted publisher**:

| Field         | Value             |
|---------------|-------------------|
| Owner         | `Phenome-Health`  |
| Repository    | `ddharmon`        |
| Workflow name | `publish.yml`     |
| Environment   | `pypi`            |

## Cutting a release

Run from a clean checkout of `Phenome-Health/ddharmon` (canonical + publisher).

1. **Bump the version** in `pyproject.toml` → `[project] version = "X.Y.Z"` (semver).
2. **Update `CHANGELOG.md`** — add a `## [X.Y.Z] — YYYY-MM-DD` section describing the changes.
3. **Dry-run the build locally** (a PyPI version can never be re-uploaded, so catch problems now):
   ```bash
   python -m build
   python -m twine check dist/*
   ```
   Both must pass — valid metadata, wheel + sdist build cleanly.
4. **Commit, tag, push** (to `Phenome-Health/ddharmon`):
   ```bash
   git commit -am "release: vX.Y.Z"
   git tag vX.Y.Z
   git push origin main --tags
   ```
5. **Create the GitHub Release** for the tag — this is what triggers `publish.yml`:
   ```bash
   gh release create vX.Y.Z --repo Phenome-Health/ddharmon --title "vX.Y.Z" --notes-file <(sed -n '/## \[X.Y.Z\]/,/## \[/p' CHANGELOG.md)
   ```
   (or use the GitHub web UI). Watch the run: `gh run watch --repo Phenome-Health/ddharmon`.
6. **Verify the published release** with the automated gate:
   ```bash
   python scripts/verify_release.py X.Y.Z          # core (fast, every release)
   python scripts/verify_release.py X.Y.Z --full    # also runs the [all] embedding stack (slow)
   ```
   This runs three stages and exits non-zero if any fail:
   1. **PyPI is live** — polls `https://pypi.org/pypi/ddharmon/X.Y.Z/json` with retry/backoff,
      tolerating index/CDN propagation lag (default ~5 min, tunable via `--timeout`).
   2. **Installs from PyPI into a throwaway venv** — `pip install`s the *exact* pinned version
      (`ddharmon==X.Y.Z`), so a stale CDN copy can't masquerade as the new release.
   3. **Smoke test inside that venv** — `scripts/smoke_test.py` runs against the installed wheel:
      asserts the installed distribution version equals `X.Y.Z`, then exercises value-encoding
      parsing, ingestion + preprocessing on toy data, and the `ddharmon` CLI entry point. `--full`
      additionally installs `ddharmon[all]` and exercises the embedding stack on toy data (slow;
      downloads a model; no API key needed).

   **A verification failure means a published, immutable release is broken — triage it**
   (investigate, then **yank** the bad version on PyPI and cut a patch release). This step is a
   diagnostic gate; it does **not** and cannot roll back the publish.

## Notes

- **Irreversible:** a PyPI version number can never be reused or re-uploaded. Always do the
  local `build` + `twine check` dry-run first; `verify_release.py` is the *post*-publish safety net.
- Version is declared only in `pyproject.toml`; the build backend is **hatchling** and the
  package targets `requires-python = ">=3.12"`.
- `ddharmon` 0.1.0–0.4.0 on PyPI were an unrelated early package (a BioMapper2 client, since
  renamed to `biomapper`). The 0.5.0 line is the Data Dictionary Harmonization tool and
  supersedes them.
