# PD Pull Request Template — Filling Guidelines

> **Template source**: Read `.github/pull_request_template.md` for the exact structure. The PR body must follow that template. Below are the complete filling guidelines.

## PR Title

Format: `pkg [, pkg2]: short description`

- Derive package names from the changed file paths (e.g., `server/`, `pkg/schedule/` → `server, schedule`).
- Use `*:` if changes span many packages.
- Keep title under 70 characters.

## Issue Number Line

- Use `Close #xxx` when the PR fully resolves the issue.
- Use `ref #xxx` when the PR is related but does not close the issue.
- Multiple issues: `Close #111, Close #222` or `ref #111, ref #222`.
- If issue numbers are unknown, **ask the user first**. Do not invent placeholders.
- If the user provides issue numbers, ask whether each should be `close` or `ref`.

## What is changed — commit-message Block

- Fill the `commit-message` code block with **only** a plain description of the code-logic changes.
- Do not include a title line, PR number, `Signed-off-by`, or any other metadata — the PR title is automatically used as the commit subject after squash merge.

## Check List

- **Remove** items that do not apply (per template comment).
- At least one test type must remain.
- **Tests**: Infer from the diff which test types apply (unit test if `_test.go` files changed, etc.). For "No code" PRs (docs, CI config), remove all other test types.
- **Code changes / Side effects / Related changes**: Only keep subsections that have applicable items. Omit empty subsections entirely.

## Release Note

- Bug fixes and new features need a meaningful release note.
- Style guide: https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/release-notes-style-guide.html
- If no user-facing change, keep `None.` in the release-note block.
