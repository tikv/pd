---
name: apply-google-go-best-practices
description: Compare Go code against Google Go best practices during targeted review or refactoring. Use when the task explicitly asks for Google Go style guidance, or when an external Go best-practices baseline is needed for naming, shadowing, package boundaries, error handling, documentation, tests, string construction, or package-level state. In pd, treat this as a supplemental checklist and follow local AGENTS.md, lint, and test rules first.
---

# Apply Google Go Best Practices

Use this skill as a focused comparison checklist, not as the default policy for routine `pd` development. Prefer local repository conventions when they are stricter, and avoid broad rewrites that change public APIs unless the task explicitly calls for them.

## Workflow

1. Read the repository-specific rules first, including `AGENTS.md`, deeper `AGENTS.md` files, lint configuration, and test workflow requirements.
2. Confirm that the task actually needs an external Google-style comparison. For routine `pd` work without that need, stop and follow the local rules only.
3. Load [references/google-go-best-practices.md](references/google-go-best-practices.md) and select only the sections that match the task.
4. Compare the code against both the local conventions and the Google guidance, then make the smallest change that improves clarity, safety, or API shape without weakening `pd` conventions.
5. If the guidance conflicts with established package patterns, compatibility requirements, or repository rules, keep the local pattern and explain the tradeoff.
6. In reviews, report concrete findings tied to the relevant guideline. In implementation tasks, update code, docs, and tests together when the change affects behavior or API usage.

## Focus Areas

- Naming and package design: remove redundant names, avoid `Get` prefixes, avoid weak `util` packages, and keep package boundaries coherent for callers.
- Correctness and maintainability: avoid accidental shadowing, prefer structured errors, and keep panic usage narrow.
- API and documentation quality: document non-obvious context, concurrency, cleanup, and error contracts.
- Test quality: keep setup scoped, prefer real transports where practical, and use `t.Error` or `t.Fatal` deliberately.
- Simplicity and performance: use zero values, composite literals, size hints, channel directions, and string-building techniques appropriately.

## Reference

Load [references/google-go-best-practices.md](references/google-go-best-practices.md) for the condensed checklist mapped to the official Google guide. Read only the sections that apply to the current code path, and use it as a secondary reference after the repository-local guidance.
