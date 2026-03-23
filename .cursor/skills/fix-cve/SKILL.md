---
name: fix-cve
description: >-
  Fix CVE vulnerabilities by upgrading affected dependencies across Python (uv),
  Go, and Rust layers. Use when the user mentions CVE numbers, asks to fix
  security vulnerabilities, or asks to patch dependencies for CVEs.
---

# Fix CVE

Fixes one or more CVE vulnerabilities in the multi-storage-client repository by
identifying and upgrading affected dependencies.

## Input

The user provides one or more CVE identifiers (e.g. `CVE-2026-27459`).

## Workflow

Copy this checklist and track progress with TodoWrite:

```
- [ ] Step 1: Research each CVE
- [ ] Step 2: Locate affected dependencies
- [ ] Step 3: Plan fixes (present to user)
- [ ] Step 4: Apply fixes
- [ ] Step 5: Verify lock files and builds
- [ ] Step 6: Output commit message and PR description
```

### Step 1: Research each CVE

For each CVE, web-search for:
- Affected package name and vulnerable version range
- Fixed version (minimum safe version)
- CVSS score and severity

Collect into a table:

| CVE | Package | Vulnerable | Fixed | CVSS | Severity |
|-----|---------|-----------|-------|------|----------|

### Step 2: Locate affected dependencies

Search the repo for each affected package across all layers:

| Layer | Files to check |
|-------|---------------|
| Python direct | `multi-storage-client/pyproject.toml` `[project.dependencies]` and `[project.optional-dependencies]` |
| Python transitive | `uv.lock` — grep for the package name |
| Go direct | `multi-storage-file-system/go.mod` `require` blocks |
| Go transitive | `multi-storage-file-system/go.sum` |
| Rust | `rust/Cargo.toml` and `rust/Cargo.lock` |

If a package is not found anywhere, tell the user and skip it.

### Step 3: Plan fixes (present to user before proceeding)

For each affected dependency, determine the fix strategy:

#### Python direct dependency
Bump the version floor in `multi-storage-client/pyproject.toml`. If the upper
bound excludes the fixed version, widen it.

#### Python transitive dependency
Check whether the parent package (the one that pulls it in) has a version
that already requires the fixed version. If so, bump the parent's floor.

If no parent version solves it, determine whether a `constraint-dependencies`
entry or an `override-dependencies` entry is needed:

- **Use `constraint-dependencies`** (preferred) when the upstream allows the
  fixed version but the `lowest-direct` resolver picks an older one. A
  constraint tightens the floor without conflicting with upstream bounds.
- **Use `override-dependencies`** (last resort) only when an upstream package
  has an explicit upper bound that blocks the fixed version. Overrides
  silently ignore upstream constraints and should be documented with a
  comment explaining why.

To test which is needed: try `constraint-dependencies` first. If `uv lock`
fails with an unsolvable conflict, the upstream has a blocking upper bound
and an override is required.

Both live in the workspace-root `pyproject.toml` under `[tool.uv]`.

#### Go dependency
```bash
cd multi-storage-file-system
go get <module>@<fixed-version>
go mod tidy
```

#### Rust dependency
Edit `rust/Cargo.toml` version requirement, then `cargo update -p <crate>`.

### Step 4: Apply fixes

Apply each fix. After modifying Python constraints/overrides, regenerate the
lock file:

```bash
uv lock --upgrade-package <package1> --upgrade-package <package2>
```

Verify the lock file contains the expected versions:

```bash
grep -A2 'name = "<package>"' uv.lock
```

### Step 5: Verify builds

| Layer | Verification command |
|-------|---------------------|
| Go | `cd multi-storage-file-system && go build ./...` |
| Python | `just run-unit-tests` (or `cd multi-storage-client && uv run pytest`) |
| Rust | `cd rust && cargo test` |

Only run verification for layers that were changed.

### Step 6: Output commit message and PR description

#### Commit message format

Single line only — no body.

```
fix: upgrade <packages> for <CVE list>
```

Example:

```
fix: upgrade grpc-go, pyOpenSSL, and authlib for CVE-2026-33186, CVE-2026-27459, CVE-2026-27962
```

#### PR description format

```markdown
## Summary
- <Bullet per CVE: ID, severity, package, what the vulnerability allows, fixed version.>

## Changes
- <Bullet per file changed and what was done.>

## Override justification
<If any override-dependencies were added, explain why a constraint was
insufficient (which upstream package blocks the fixed version and what its
upper bound is). If no overrides, omit this section.>

## Test plan
- [ ] Go module builds: `cd multi-storage-file-system && go build ./...`
- [ ] Python unit tests pass
- [ ] Lock file contains expected versions
- [ ] No new linter errors
```

## Key decisions to confirm with user

- If an override is the only option, explain why and ask for confirmation.
- If a fix requires widening an upper bound on a direct dependency (e.g.
  `cryptography>=44,<46` → `<47`), flag the change and its implications.
- If a CVE affects a package not in the dependency tree, report it and skip.
