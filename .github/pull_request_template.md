## Description

_Change description._

_{Relates to/Closes} {Task ID}._

## Checklist

- Development PR
  - `.release_notes/.unreleased.md`
    - [ ] Notable changes to the client (i.e. not related to tooling, CI/CD, etc.) from this PR have been added.
- Release PR
  - CI/CD
    - [ ] The Beta stage is passing in GitLab CI/CD.
  - `multi-storage-client/pyproject.toml`
    - [ ] The package version has been bumped.
  - `.release_notes/.unreleased.md`
    - [ ] This file's contents have been moved into a `.release_notes/{bumped package version}.md` file.
