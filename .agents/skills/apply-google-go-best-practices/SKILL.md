---
name: apply-google-go-best-practices
description: Apply Google Go best practices when writing, reviewing, or refactoring Go code. Use when Codex needs to improve or evaluate Go code for naming, shadowing, package boundaries, error handling, documentation, variable declarations, option patterns, tests, string construction, or package-level state. For repository work, combine this skill with the local AGENTS.md, lint, and test rules rather than replacing them.
---

# Apply Google Go Best Practices

Use this skill as a focused checklist for Go implementation and code review tasks. Prefer local repository conventions when they are stricter, and avoid broad rewrites that change public APIs unless the task explicitly calls for them.

## Workflow

1. Read the repository-specific rules first, including `AGENTS.md`, deeper `AGENTS.md` files, lint configuration, and test workflow requirements.
2. Load [references/google-go-best-practices.md](references/google-go-best-practices.md) and select only the sections that match the task.
3. Compare the code against both the local conventions and the Google guidance, then make the smallest change that improves clarity, safety, or API shape.
4. If the guidance conflicts with established package patterns, compatibility requirements, or repository rules, keep the local pattern and explain the tradeoff.
5. In reviews, report concrete findings tied to the relevant guideline. In implementation tasks, update code, docs, and tests together when the change affects behavior or API usage.

## Focus Areas

- Naming and package design: remove redundant names, avoid `Get` prefixes, avoid weak `util` packages, and keep package boundaries coherent for callers.
- Correctness and maintainability: avoid accidental shadowing, prefer structured errors, and keep panic usage narrow.
- API and documentation quality: document non-obvious context, concurrency, cleanup, and error contracts.
- Test quality: keep setup scoped, prefer real transports where practical, and use `t.Error` or `t.Fatal` deliberately.
- Simplicity and performance: use zero values, composite literals, size hints, channel directions, and string-building techniques appropriately.

## Reference

Load [references/google-go-best-practices.md](references/google-go-best-practices.md) for the condensed checklist mapped to the official Google guide. Read only the sections that apply to the current code path to keep context tight.
