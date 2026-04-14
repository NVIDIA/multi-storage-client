# Multi-Storage-Client (Python + Rust)

Core library providing a unified API for multi-cloud object storage. See the root `AGENTS.md` for overall architecture and cross-language sync points.

## Structure

```text
src/multistorageclient/          Python core library
  client/                          Client implementations (single, composite)
  providers/                       Storage provider backends (S3, GCS, Azure, OCI, local)
  caching/                         Cache layer
  instrumentation/                 Telemetry and metrics
  mcp/                             MCP server integration
  commands/                        CLI entry points
  sync/                            Data sync manager
rust/                              Rust performance layer
  src/lib.rs                       PyO3 module entry point
  src/types.rs                     Shared type definitions
  src/credentials.rs               Credential handling
src/multistorageclient_rust/
  multistorageclient_rust.pyi      Type stubs for Rust bindings
tests/
  test_multistorageclient/unit/    Unit tests
  test_multistorageclient/e2e/     End-to-end tests
  test_multistorageclient_rust/    Rust binding tests
```

## Python ↔ Rust Interop

- Python calls Rust via the `multistorageclient_rust` module (built with maturin).
- Rust exposes functions via PyO3 `#[pyfunction]` and `#[pyclass]`.
- Keep type conversions explicit and efficient.
- **Always** update `src/multistorageclient_rust/multistorageclient_rust.pyi` when Rust API changes.

## Commands

| Task | Command |
|---|---|
| Prepare dev environment | `just multi-storage-client/prepare-toolchain` |
| Run unit tests | `just multi-storage-client/run-unit-tests` |
| Run Rust tests | `cd multi-storage-client/rust && cargo test` |
| Lint & type check | `just multi-storage-client/analyze` |
| Build release wheel | `just multi-storage-client/package` |

## Testing

- Python unit tests: `tests/test_multistorageclient/unit/`
- Rust inline tests: `rust/src/*.rs` (with `#[cfg(test)]`)
- Rust binding tests: `tests/test_multistorageclient_rust/`
- E2E tests: `tests/test_multistorageclient/e2e/` (require cloud credentials, run manually)

## Tooling

- **Package manager:** uv
- **Build backend:** maturin (Python ↔ Rust)
- **Linter/formatter:** ruff
- **Type checker:** pyright
- **Test runner:** pytest (with `--numprocesses auto`)
- **Rust toolchain:** cargo, rustup

## Code Style

- Python: enforced by ruff (formatting + linting) and pyright (type checking).
- Rust: standard `cargo fmt` + `cargo clippy`.
- All public Python functions must have docstrings (Sphinx autodoc extracts them).
- New storage providers: add to `providers/`, update `StorageProvider` enum, and update the provider factory.

## Boundaries

- **Always:** run `just multi-storage-client/analyze` and `just multi-storage-client/run-unit-tests` before submitting changes.
- **Ask first:** changes to the credential management (`instrumentation/auth.py`), Rust FFI boundary (`rust/src/lib.rs`), or sync worker logic (`sync/worker.py`).
- **Never:** modify `pyproject.toml` build backend config without testing `maturin develop` and `maturin build`; change Rust function signatures without updating `multistorageclient_rust.pyi`.
