---
name: publish-to-pypi
description: Use when cutting a new ddharmon release to PyPI — bumping the version, updating the changelog, tagging, and publishing the package to https://pypi.org/project/ddharmon/ via the GitHub Release → Trusted Publishing workflow. Triggers on "release ddharmon", "publish to pypi", "cut a release", "bump the version and publish".
---

# Publishing ddharmon to PyPI

## How releases work here

- **Canonical development** happens in `Phenome-Health/ddharmon`.
- **PyPI publishing** happens from the mirror `trentleslie/ddharmon` — Trent owns the
  `ddharmon` name on PyPI. The two repos are content-mirrored; releases are cut from
  the trentleslie mirror.
- Publishing is automated by [`.github/workflows/publish.yml`](../../../.github/workflows/publish.yml):
  when a **GitHub Release is published**, it builds the sdist + wheel and uploads them
  to PyPI using **Trusted Publishing (OIDC)** — no API token is stored anywhere.

## One-time setup (PyPI owner only — Trent)

Do this once on PyPI before the first OIDC release; it cannot be done by anyone except a
project owner, and the publish job fails auth until it exists:

PyPI → project `ddharmon` → **Settings → Publishing → Add a trusted publisher**:

| Field         | Value          |
|---------------|----------------|
| Owner         | `trentleslie`  |
| Repository    | `ddharmon`     |
| Workflow name | `publish.yml`  |
| Environment   | `pypi`         |

## Cutting a release

Run from a clean checkout of `trentleslie/ddharmon` (the publishing mirror).

1. **Sync canonical content.** Development lives upstream, so refresh the mirror first:
   ```bash
   git fetch phenome
   git switch main && git reset --hard phenome/main   # or land it via PR
   git push origin main --force-with-lease
   ```
2. **Bump the version** in `pyproject.toml` → `[project] version = "X.Y.Z"` (semver).
3. **Update `CHANGELOG.md`** — add a `## [X.Y.Z] — YYYY-MM-DD` section describing the changes.
4. **Dry-run the build locally** (a PyPI version can never be re-uploaded, so catch problems now):
   ```bash
   python -m build
   python -m twine check dist/*
   ```
   Both must pass — valid metadata, wheel + sdist build cleanly.
5. **Commit, tag, push:**
   ```bash
   git commit -am "release: vX.Y.Z"
   git tag vX.Y.Z
   git push origin main --tags
   ```
6. **Create the GitHub Release** for the tag — this is what triggers `publish.yml`:
   ```bash
   gh release create vX.Y.Z --title "vX.Y.Z" --notes-file <(sed -n '/## \[X.Y.Z\]/,/## \[/p' CHANGELOG.md)
   ```
   (or use the GitHub web UI). Watch the run: `gh run watch`.
7. **Verify** at https://pypi.org/project/ddharmon/ that `X.Y.Z` is the new latest, then in a
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
