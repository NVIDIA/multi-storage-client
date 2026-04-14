# Agent Learnings

Mistakes and non-obvious patterns worth remembering. Any agent (any tool, any team member) reads this file at session start. Append new entries as bullet points — do not edit or remove existing ones.

- When adding a new storage provider, update both the `StorageProvider` enum and the provider factory.
- Always update `multistorageclient_rust.pyi` when changing Rust function signatures.
- Use `just prepare-toolchain` (not `pip install -e .` or `maturin develop` directly) to set up the Python+Rust dev environment.
- When `msc` is in a project venv (`.venv/bin/msc`), MCP server configs must use the absolute path — tools launch the server outside the venv.
