---
name: release-automation
description: Prepares and automates the release process for multi-storage-client (version bump, release notes, checklist). Use when the user wants to cut a release, bump version, prepare release, publish a new version, or automate release steps.
---

# Release Automation

Guides the agent through preparing a release for the multi-storage-client repository. Tagging is done by the pipeline after the release MR is merged; this skill covers only the in-repo changes (version bump, release notes). Do not ask the user to run `git tag` or push a tag.

## When to Use

- User asks to "cut a release", "bump version", "prepare release", "publish new version", or "automate release".
- User wants to move unreleased notes into a versioned file and bump the package version.

## Prerequisites

- Work from repository root.
- Ensure `.release_notes/.unreleased.md` has been updated with notable changes (per PR template). If empty or placeholder-only, confirm with the user before releasing.

## Release Preparation Workflow

### 1. Determine the new version

- Read current version from `multi-storage-client/pyproject.toml` (`version = "X.Y.Z"`).
- If the user specified a version (e.g. "0.44.0"), use it. Otherwise propose:
  - **Patch** (e.g. 0.43.0 → 0.43.1): bug fixes only.
  - **Minor** (e.g. 0.43.0 → 0.44.0): new features, backward-compatible.
  - **Major**: breaking changes (rare; confirm with user).

### 2. Bump package version

- In `multi-storage-client/pyproject.toml`, set `version = "<new_version>"` (e.g. `"0.44.0"`).

### 3. Finalize release notes

- Read `.release_notes/.unreleased.md`.
- Read git history for changes since the last release: `git log <previous_version>..HEAD --no-merges --format='%h %s%n%b'` (tags are unprefixed, e.g. `0.42.0` not `v0.42.0`). Use this to find and add relevant entries; omit CI/CD, build, and chore-only commits.
- Create `.release_notes/<new_version>.md` with that content (e.g. `.release_notes/0.44.0.md`). Preserve structure (Breaking Changes, New Features, Bug Fixes, etc.). Split into Multi-Storage Client (MSC) and Multi-Storage File System (MSFS) sections when changes span both sub-projects.
- Skip mentioning any changes that are CI/CD related.
- Reset `.release_notes/.unreleased.md` to its template (the section headings under MSC and MSFS with `<!-- Add items here. -->` at the top). If unsure whether the repo convention expects a reset, ask the user before modifying.

### 4. Verification (recommended)

- Run `just build` from repo root (runs analyze, unit tests, ray tests; in CI also runs package).

### 5. Handoff checklist

Present a short checklist for the user:

- [ ] Version bumped in `multi-storage-client/pyproject.toml`.
- [ ] `.release_notes/<new_version>.md` created; `.release_notes/.unreleased.md` handled as above.
- [ ] `just build` passed (or user will run locally).
- [ ] Create a git commit with the release changes and open/merge an MR; the pipeline will create and push the tag after merge.

## Reference

- Repo-specific paths and CI details: [reference.md](reference.md)
