# Multi-Storage-Client — Agent Guide

This file describes the project for any coding agent. Sub-projects (`multi-storage-client`, `multi-storage-file-system`, and `multi-storage-explorer`) have their own `AGENTS.md` with language-specific details.

**Before starting any task, read `agent-learnings.md`** — it contains accumulated mistakes and corrections from past sessions. Follow every entry.

## Repository Structure

```text
multi-storage-client/          Python + Rust library (core)
  src/multistorageclient/        Python core library
  rust/                          Rust performance layer (maturin + PyO3)
  tests/                         Test suite (unit, integration, e2e)
  pyproject.toml                 Python project config (maturin build backend)
multi-storage-file-system/     Go FUSE filesystem (POSIX file access)
multi-storage-explorer/        Explorer web UI
multi-storage-client-docs/     Sphinx documentation
  src/                           Doc source (rst files)
  examples/                      Usage examples and quickstart notebooks
nix/                           Nix build configuration
```

## Architecture

Multi-language project: **Python** (core) + **Rust** (performance) + **Go** (POSIX/FUSE).

- **Storage abstraction layer** — unified API across AWS S3, GCS, Azure Blob, OCI Object Storage, and local filesystem.
- **Python ↔ Rust bindings** — via maturin/PyO3. Python calls Rust through the `multistorageclient_rust` module. Rust exposes functions via `#[pyfunction]` and `#[pyclass]`. Type hints live in `multi-storage-client/src/multistorageclient_rust/multistorageclient_rust.pyi`.
- **POSIX access** — Go-based FUSE mount exposes object storage as a filesystem.
- **Credential management** — supports multiple auth methods per cloud provider.
- **Error handling** — translates provider-specific errors to unified exceptions.

## Cross-Language Sync Points

When changing one language layer, check whether the others need updating:

| Change in… | Also update… |
|---|---|
| Rust function signatures (`multi-storage-client/rust/src/`) | `.pyi` stub (`multi-storage-client/src/multistorageclient_rust/multistorageclient_rust.pyi`) |
| Rust types (`multi-storage-client/rust/src/types.rs`) | Python type wrappers, `.pyi` stub |
| Python storage providers | Verify Go FUSE layer still compatible (shared config patterns) |
| Config schema | All three layers (Python config parsing, Rust config, Go config) |

## Conventions

- **Plan first** — before editing, list files to change, testing plan, and doc updates.
- **Work from repo root** — all shell commands assume cwd = repository root.
- **Minimal comments** — do not add comments unless absolutely required for clarity or safety. Do not remove existing correct comments.
- **Backward compatibility** — maintain unless explicitly versioning a breaking change.

## Commands

| Task | Command |
|---|---|
| Build everything | `just build` |

See `multi-storage-client/AGENTS.md`, `multi-storage-file-system/AGENTS.md`, and `multi-storage-explorer/AGENTS.md` for language-specific commands.

## Testing Policy

- Add/update unit tests for **every** change (feature or bugfix).
- Prefer focused tests that validate specific behavior.
- Integration tests are complex to run — flag when needed, run manually.

## Documentation Requirements

For any externally visible change:

- Update `multi-storage-client-docs/src/user_guide/*.rst` for new behavior, APIs, or examples.
- Update `multi-storage-client-docs/src/references/*.rst` if API signatures change.
- Update `multi-storage-client-docs/examples/quickstart.ipynb` when SDK usage patterns change.
- Rebuild with `just multi-storage-client-docs/build` and verify.
- Include docstrings in Python code (Sphinx autodoc extracts them).

## Boundaries

- **Always:** run tests and linting before submitting; check cross-language sync points table above when changing any layer.
- **Ask first:** changes that affect the public Python API surface, config schema (impacts all three languages), or CI/CD pipelines.
- **Never:** commit secrets or credentials; force-push to main; remove or edit existing entries in `agent-learnings.md`.

## PR Checklist

- [ ] Plan posted and agreed.
- [ ] Changes implemented with minimal comments.
- [ ] If Rust signatures changed, `.pyi` stub updated.
- [ ] Unit tests added/updated and passing (Python, Rust, Go as applicable).
- [ ] Integration tests added/updated (documented if not run).
- [ ] Docs and examples updated.
- [ ] `just build` succeeds.
- [ ] Final summary posted.
