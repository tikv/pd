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

- Fill the `commit-message` code block with a plain description of what the code changes do and why. It becomes the commit body after squash merge, so it is the only rationale kept in git history.
- Do not include a title line, PR number, `Signed-off-by`, or any other metadata — the PR title is automatically used as the commit subject after squash merge.

## Check List

- **Remove** items that do not apply (per template comment).
- At least one test type must remain.
- **Tests**: Infer from the diff which test types apply (unit test if `_test.go` files changed, etc.). For "No code" PRs (docs, CI config), remove all other test types.
- **Code changes / Side effects / Related changes**: Only keep subsections that have applicable items. Omit empty subsections entirely.

## Sections After the Check List

- Keep all four sections. Unlike Check List items, they are not removed when empty; write `None.` instead.
- Do not repeat the Check List. It records which kinds of tests and changes apply; these sections add what the Check List cannot say.

### Design Trade-offs

- List the alternatives a reviewer is likely to ask about and why they do not work. The reason for the final approach belongs in the commit message, not here.
- Do not retell every attempt.

### Verification

- Fill `Commit:` with the commit the evidence was collected on. After pushing new commits, collect the evidence again, or append `(on <commit>)` to the row and state why it still holds.
- Write the claims first: each is something this PR must make true, such as "TSO allocation does not stall after a leader change". "Ran `make test`" is evidence, not a claim.
- One row per claim. Fill **Evidence** with the test name or command, its result, and any condition it needs (failpoints enabled, `NEXT_GEN=1`, build tags).
- For a bug fix, include a row showing that the new test fails without the fix, with one line of its output. If that is not possible, say why.
- Use only results with a source a reviewer can check (a command you ran, a CI run, a recorded result from someone else) that apply to the verified commit. Say which results you did not run yourself. List anything you cannot back this way under `Not verified:`, or write `Not verified: None.` if everything is covered.
- For "No code" PRs, one row describing how the change was checked is enough.

### Risks and Rollback

- Explain each item kept under Code changes and Side effects, and any other failure mode.
- State whether the change can be rolled back and whether downgrading PD stays safe, especially for configuration, API, or persistent data changes.
- Do not invent risks.

### Questions for Reviewers

- List decisions a reviewer needs to make (a changed contract, a compatibility trade-off, a residual risk to accept) and the places you are least sure about.
- Do not list what a reviewer checks anyway. This is where review starts, not a limit on what is reviewed.

## Release Note

- Bug fixes and new features need a meaningful release note.
- Style guide: https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/release-notes-style-guide.html
- If no user-facing change, keep `None.` in the release-note block.
