# Release automation – reference

## Paths (repository root)

| Item | Path |
|------|------|
| Package version | `multi-storage-client/pyproject.toml` → `[project]` → `version` |
| Unreleased notes | `.release_notes/.unreleased.md` |
| Versioned notes | `.release_notes/<version>.md` (e.g. `.release_notes/0.44.0.md`) |

## Commands (from repo root)

- `just build` – analyze, unit tests, ray tests; in CI also runs `just package`.
- `just package` – create sdist and wheels (multi-storage-client); MSFS has its own `just package` under `multi-storage-file-system/`.
- **Git (release notes):** Tags are unprefixed (`0.42.0` not `v0.42.0`). Commits since last release: `git log <previous_version>..HEAD --no-merges --format='%h %s%n%b'`.

## Release notes format

- `.unreleased.md` and versioned files use sections such as: Breaking Changes, New Features, Bug Fixes. Optional: Multi-Storage Client (MSC) vs Multi-Storage File System (MSFS) subsections (see `.release_notes/0.42.0.md`).
- To avoid merge conflicts, contributors add items at an arbitrary place in the list (see comment in `.unreleased.md`).
