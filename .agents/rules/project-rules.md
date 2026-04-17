Read `AGENTS.md` at the repo root for project structure, architecture, conventions, commands, testing policy, and cross-language sync points.

# Golden Rules (Do These Every Time)

1. **Present a Plan First**
   - Before editing files, produce a brief plan that lists:
     - **Files to change** and **how** they will change.
     - **Testing plan** (unit + integration scope).
     - **Documentation updates** (which docs to add/update).
2. **Work from the Repository Root**
   - For every shell command, assume current dir = repo **root**.
3. **Code Comments**
   - **Do not add comments** unless **absolutely required** for clarity or safety (e.g., non-obvious logic, invariants).
   - **Do not remove existing comments** unless **they are not correct** anymore.

# Standard Implementation Workflow (follow step-by-step)

## Phase 0 — Understand & Trace

- Map the request to the architecture:
  - **Python-only change:** Update `src/multistorageclient/` + tests + docs.
  - **Performance-critical change:** Consider Rust implementation in `rust/src/`.
  - **POSIX/filesystem change:** Update Go code in `posix/fuse/mscp/`.
  - **Cross-language change:** Coordinate Python ↔ Rust bindings via maturin. Check sync points in `AGENTS.md`.
- Identify **user-facing** behavior vs **internal** refactor.

## Phase 1 — Plan (Output to user)

- List concrete file edits with rationale (per file).
- Specify test coverage (unit + any targeted integration).
- Specify doc additions/changes (paths under `docs/src/`).
- Call out any **build steps** needed (maturin build, cargo build, etc.).
- Important: try to fix things at the cause, not the symptom.

## Phase 2 — Environment & Branch

- Ensure you are at repo root. Confirm required tooling is available via Nix.
- Create a topic branch as needed (follows user/org conventions).

## Phase 3 — Make the Changes (Layered Order)

1. **Rust Performance Layer** (if applicable)
   - Edit `rust/src/*.rs` for performance-critical operations.
   - Update type definitions in `rust/src/types.rs`.
   - Update Python bindings stub `src/multistorageclient_rust/multistorageclient_rust.pyi`.
2. **Python Core Library**
   - Update `src/multistorageclient/…` for core functionality.
   - Maintain backward compatibility unless explicitly versioning a breaking change.
   - Update credential management, storage providers, iterators as needed.
3. **Go POSIX/FUSE Layer** (if applicable)
   - Edit `posix/fuse/mscp/*.go` for filesystem operations.
4. **Docs & Examples**
   - Update `docs/src/…` (Sphinx rst files).
   - Update `examples/…` and `examples/quickstart.ipynb` to reflect changes.
5. **Configuration**
   - Update `pyproject.toml` if dependencies change.
   - Update `rust/Cargo.toml` if Rust dependencies change.

## Phase 4 — Unit Tests (Fast Feedback)

- **Always** add/update unit tests that reflect the change.
- **Run** relevant tests (see `AGENTS.md` for commands).
- Fix failures and repeat until **green**.

## Phase 5 — Integration Tests

- **Always** add/update integration tests for cross-component or cloud provider changes.
- Integration tests are more complex to run so I will run them manually.

## Phase 6 — Linting & Quality Gates

- After implementation + tests: run `just analyze`.
- All checks must pass. If anything fails, fix and re-run.

## Phase 7 — Deliverables & Handoff

- Produce:
  - **Implementation Plan** (final version with any deltas).
  - **Code diffs** across layers (Rust(if applicable) ➜ Python ➜ Go(if applicable)).
  - **Tests** (unit + integration) and their outcomes.
  - **Docs updated** (list specific files under `docs/src/…` and examples).
  - **Build verification** (maturin build succeeds, imports work).
- Ready for review.

# PR / Change Submission Checklist

See `AGENTS.md` for the full checklist. Verify all items before handoff.
