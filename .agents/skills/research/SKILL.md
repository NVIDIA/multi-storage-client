---
name: research
description: >-
  Before designing a feature, research how comparable libraries solve the
  problem, check best practices, known pitfalls, and upstream changes. Use when
  starting a new feature, investigating an unfamiliar area, or the user asks to
  research something.
invocable: auto
---

# Research

Research the problem space before designing or implementing a feature to avoid reinventing the wheel and catch issues early.

## When to Use

- Starting a new feature or significant enhancement.
- Working with unfamiliar APIs, cloud SDKs, or library internals.
- User explicitly asks to research or investigate an approach.

## Workflow

### 1. Clarify scope

Confirm with the user:
- What problem are we solving?
- Which language layers are involved (Python, Rust, Go)?
- Any known constraints or preferences?

### 2. Research comparable solutions

Search for:
- How other multi-cloud storage libraries solve the same problem.
- Best practices for the relevant cloud SDKs (AWS S3, GCS, Azure, OCI).
- Known pitfalls or breaking changes in upstream dependencies.
- PyO3/maturin patterns (if Rust bindings are involved).
- FUSE implementation patterns (if Go layer is involved).

### 3. Check upstream

Look for:
- Recent releases of relevant dependencies.
- Open issues or PRs related to the feature area.
- Deprecation notices.

### 4. Produce a summary

Output using this format:

```markdown
## Research Summary: <topic>

### Approaches Found
- <approach 1>: <trade-offs>
- <approach 2>: <trade-offs>

### Recommendation
- <recommended approach with rationale>

### Risks / Blockers
- <anything that could derail implementation>

### References
- <links to docs, issues, examples>
```
