---
name: generate-test-plan
description: >-
  Produce a test plan with specific test names, assertions, and coverage across
  Python, Rust, and Go. Use when planning tests for a feature, after generating
  a spec, or the user asks for a test plan.
invocable: auto
---

# Generate Test Plan

Produce a concrete test plan with specific file paths, test names, and assertions. Covers all affected language layers.

## When to Use

- After a feature spec is approved, before implementation.
- User asks for a test plan or testing strategy.
- Reviewing test coverage for an existing feature.

## Workflow

### 1. Identify scope

From the feature spec (or user description), determine:
- Which language layers need tests (Python, Rust, Go)?
- What behaviors need to be verified?
- Are integration tests needed (cross-provider, cross-layer)?

### 2. Produce the test plan

Use this template:

```markdown
## Test Plan: <feature title>

### Python Unit Tests
File: `tests/test_multistorageclient/test_<module>.py`

| Test Name | What It Verifies |
|-----------|-----------------|
| `test_<name>` | <specific assertion> |
| `test_<name>` | <specific assertion> |

### Rust Tests (if applicable)
File: `rust/src/<module>.rs` (inline `#[cfg(test)]`)

| Test Name | What It Verifies |
|-----------|-----------------|
| `test_<name>` | <specific assertion> |

### Go Tests (if applicable)
File: `posix/fuse/mscp/<module>_test.go`

| Test Name | What It Verifies |
|-----------|-----------------|
| `Test<Name>` | <specific assertion> |

### Integration Tests (if applicable)
File: `tests/integration/test_<scenario>.py`

| Test Name | What It Verifies |
|-----------|-----------------|
| `test_<name>` | <specific assertion across providers/layers> |

### Edge Cases
- <edge case 1 and which test covers it>
- <edge case 2 and which test covers it>

### Run Commands
- Python: `just multi-storage-client/run-unit-tests`
- Rust: `cd multi-storage-client/rust && cargo test`
- Go: `cd multi-storage-file-system && go test ./...`
```

### 3. Get approval

Present the test plan to the user. Adjust scope if needed before proceeding.
