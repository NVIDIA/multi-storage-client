---
name: prepare-mr
description: >-
  Stage files, commit with correct format, push, and generate an MR description.
  Manual invocation only — has side effects (git operations). Use when the user
  explicitly asks to prepare, create, or submit an MR.
disable-model-invocation: true
invocable: manual
---

# Prepare MR

Stage, commit, push, and generate an MR description. This skill has side effects — only run when the user explicitly asks.

## When to Use

- User asks to "prepare MR", "create MR", "submit MR", or "push changes".
- Never auto-invoke — always wait for explicit user request.

## Workflow

### 1. Review changes

Run in parallel:
- `git status` — see all modified and untracked files.
- `git diff` — see unstaged changes only. Use `git diff --staged` (or `git diff --cached`) for staged changes, or `git diff HEAD` for all changes relative to the last commit.
- `git log --oneline -10` — check recent commit style.

Confirm with the user which files to include.

### 2. Stage files

```bash
git add <files>
```

Exclude files that likely contain secrets (`.env`, `credentials.json`, etc.). Warn the user if they ask to include such files.

### 3. Commit

Use a concise commit message that follows the repo's existing style. Focus on the "why" not the "what".

```bash
git commit -m "<message>"
```

### 4. Push

```bash
git push -u origin HEAD
```

### 5. Generate MR description

Use this format:

```markdown
## Summary
- <1-3 bullet points describing the change>

## Changes
- <bullet per file/area changed>

## Test Plan
- [ ] <specific test or verification step>
- [ ] <specific test or verification step>

## Cross-Language Impact
<if applicable — which layers were affected and how sync was maintained>
```

### 6. Create MR

```bash
glab mr create --title "<title>" --description "<description>"
```

Present the MR URL to the user.
