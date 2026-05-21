# Agent Learnings

Mistakes and non-obvious patterns worth remembering. Any agent (any tool, any team member) reads this file at session start. Append new entries as bullet points — do not edit or remove existing ones.

- When adding a new storage provider, update both the `StorageProvider` enum and the provider factory.
- Always update `multistorageclient_rust.pyi` when changing Rust function signatures.
- Use `just prepare-toolchain` (not `pip install -e .` or `maturin develop` directly) to set up the Python+Rust dev environment.
- When `msc` is in a project venv (`.venv/bin/msc`), MCP server configs must use the absolute path — tools launch the server outside the venv.
- `git commit --no-edit` on a merge commit uses `cleanup=whitespace`, which keeps any `#`-prefixed comment lines from the auto-generated `MERGE_MSG` template. To strip those without opening an editor, pass `--cleanup=strip` explicitly (e.g. `git commit --no-edit --cleanup=strip`) or supply `-m "..."` with the final message. Fixing it after the push requires `git commit --amend --cleanup=strip --no-edit` plus `git push --force-with-lease`, so prefer to get it right the first time.
