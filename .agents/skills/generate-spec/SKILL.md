---
name: generate-spec
description: >-
  Produce a feature specification covering API changes, backward compatibility,
  cross-language impact, performance, and a file-level change plan. Use when
  designing a new feature, planning a significant change, or the user asks to
  spec something out.
invocable: auto
---

# Generate Spec

Produce a detailed feature specification before implementation begins. The spec becomes the source of truth for implementation and verification.

## When to Use

- Designing a new feature or significant enhancement.
- Making changes that affect the public API.
- Cross-language changes (Python + Rust + Go).
- User asks to spec out, design, or plan a feature.

## Workflow

### 1. Gather requirements

Clarify with the user:
- What behavior should change or be added?
- Which language layers are affected?
- Performance requirements?
- Backward compatibility constraints?

### 2. Analyze impact

Determine:
- API surface changes (new functions, changed signatures, config options).
- Backward compatibility: additive or breaking?
- Cross-language sync: does a Rust change require `.pyi` stub and Python wrapper updates?
- Performance implications.
- Configuration changes.

### 3. Produce the spec

Use this template:

```markdown
## Feature Spec: <title>

### Summary
<1-2 sentence description>

### API Changes
- <new/changed functions, classes, config options with type signatures>

### Backward Compatibility
- <breaking or non-breaking>
- <migration path if breaking>

### Cross-Language Impact
| Layer | Changes Required |
|-------|-----------------|
| Python | … |
| Rust | … |
| Go (FUSE) | … |
| .pyi stubs | … |

### File-Level Change Plan
| File | Change |
|------|--------|
| … | … |

### Performance Considerations
- <expected impact, benchmarks needed>

### Acceptance Criteria
- [ ] <specific, testable criterion>
- [ ] <specific, testable criterion>
```

### 4. Get approval

Present the spec to the user. Do not proceed to implementation until approved.
