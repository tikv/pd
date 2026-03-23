---
name: apply-pd-go-best-practices
description: Compare Go changes against PD-specific coding conventions during targeted review or refactoring. Use when the task needs repository-local guidance not already enforced by `make check`, especially around failpoints, HTTP error handling, package boundaries, and compatibility-sensitive changes.
---

# Apply PD Go Best Practices

Use this skill as a repository-specific checklist for `pd` implementation and review work. Focus on conventions that are not reliably enforced by `make check`, and avoid broad rewrites unless the task explicitly requires them.

## Workflow

1. Read the repository-specific rules first, including `AGENTS.md`, deeper `AGENTS.md` files, and `.github/copilot-instructions.md`.
2. Load [references/pd-go-best-practices.md](references/pd-go-best-practices.md) and select only the sections that match the current task.
3. Skip issues already enforced mechanically by `make check` unless they matter as part of a larger repository-specific concern.
4. Compare the code against existing `pd` patterns first, then make the smallest change that improves consistency, safety, or maintainability.
5. If generic Go guidance conflicts with established `pd` conventions, keep the local pattern and explain the tradeoff.
6. In reviews, report concrete findings tied to repository rules. In implementation tasks, update code, tests, and docs together when behavior or public APIs change.

## Focus Areas

- Error handling: keep wrapping, inspection, and HTTP error responses consistent with `pd` conventions.
- Tests and failpoints: preserve failpoint-aware flows and use the repository's test entrypoints.
- API and package fit: match established `pd` package boundaries and compatibility expectations.
- Scope control: prefer behavior-preserving edits over broad cleanup or cross-package rewrites.

## Reference

Load [references/pd-go-best-practices.md](references/pd-go-best-practices.md) for the condensed PD-specific checklist. Use it as a practical repository reference, not a replacement for task-specific code reading.
