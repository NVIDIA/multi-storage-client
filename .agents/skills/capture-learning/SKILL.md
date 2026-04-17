---
name: capture-learning
description: >-
  Append a learning to agent-learnings.md when the agent or user identifies a
  mistake, non-obvious pattern, or useful convention worth remembering. Use when
  the user says "capture this", "remember this", or "add to learnings".
disable-model-invocation: true
invocable: manual
---

# Capture Learning

Append a concise learning to `agent-learnings.md` so future sessions (any tool, any team member) avoid the same mistake.

## When to Use

- User says "capture this", "remember this", "add to learnings", or similar.
- Never auto-invoke — wait for explicit user request.

## Workflow

### 1. Identify the learning

Ask the user (if not already clear):
- What went wrong or what's non-obvious?
- What should the agent do differently next time?

### 2. Draft the bullet

Write a single bullet point that is:
- **Concise** — one sentence, actionable.
- **Specific** — mention file paths, commands, or API names.
- **Prescriptive** — say what to do, not just what happened.

Good: *"Use `maturin develop` (not `pip install -e .`) for the Rust bindings during development."*
Bad: *"Had trouble installing the project."*

### 3. Append to agent-learnings.md

Add the bullet at the end of `agent-learnings.md`. Do not edit or reorder existing entries.

### 4. Confirm

Tell the user what was captured. The learning will be read by all agents in future sessions.
