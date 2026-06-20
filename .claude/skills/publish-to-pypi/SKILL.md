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
6. **Verify** at https://pypi.org/project/ddharmon/ that `X.Y.Z` is the new latest, then in a
   fresh virtualenv:
   ```bash
   pip install "ddharmon==X.Y.Z"
   python -c "import ddharmon; print('ok')"
   ddharmon --help
   ```

## Notes

- **Irreversible:** a PyPI version number can never be reused or re-uploaded. Always do the
  local `build` + `twine check` dry-run first.
- Version is declared only in `pyproject.toml`; the build backend is **hatchling** and the
  package targets `requires-python = ">=3.12"`.
- `ddharmon` 0.1.0–0.4.0 on PyPI were an unrelated early package (a BioMapper2 client, since
  renamed to `biomapper`). The 0.5.0 line is the Data Dictionary Harmonization tool and
  supersedes them.
