---
name: pd-flaky-fix
description: Use when automatically triaging and fixing flaky test issues in tikv/pd from GitHub issues to draft PR.
---

# PD Flaky Fix

## Scope

This skill is runtime-only. It selects one flaky issue from `tikv/pd`, performs root-cause-first analysis, attempts a minimal fix, validates with evidence, and opens a draft PR.

It consumes historical knowledge from:
- `references/flaky-pr-corpus.jsonl`
- `references/flaky-fix-playbook.md`

Use this together with:
- `superpowers:systematic-debugging`
- `superpowers:test-driven-development`
- `superpowers:verification-before-completion`

## Hard Boundaries

- Process exactly one issue per run.
- Candidate issue must be `state=open`, `label=type/ci`, and contain one of: `flaky|unstable|timeout|deadlock|data race|go leak|panic` in title/body.
- Skip issue if an open PR in `tikv/pd` already references `#<issue-number>` in title/body.
- Before modifying any code, fetch and pin base from `upstream/master`, verify clean workspace, and create a dedicated branch from that base.
- If workspace is dirty before branch creation, stop and report; do not continue on mixed local changes.
- Branch is pushed to `origin`.
- Draft PR is always created against `upstream` repo `tikv/pd` base `master`.
- Root-cause evidence is required before code changes.
- If evidence is insufficient, do not force a fix and do not open a PR.

### Forbidden in this skill

- Do not rebuild corpus.
- Do not refresh playbook.
- Do not run refresh scripts.
- Do not perform monthly maintenance tasks.

If corpus/playbook are missing or invalid, continue with root-cause-first flow and explicitly report `Historical analog: unavailable`.

## Workflow

1. Identify candidate issues (latest first) using commands from `references/pd-flaky-workflow.md`.
2. Dedupe by scanning open PRs for issue-number references.
3. Select first eligible issue.
4. Parse issue template sections:
- `Which jobs are failing`
- `CI link`
- `Reason for failure`
5. Pin base and switch branch before any code edits:
- Fetch and pin base: `git fetch upstream master`
- Verify clean workspace: `git diff --quiet && git diff --cached --quiet`
- Branch from base: `git checkout -b codex/flaky-<issue-number>-<short-test-name> upstream/master`
6. Root-cause-first analysis:
- Extract failing test name, stack trace, error pattern.
- Locate test and nearby stable tests.
- Compare setup, synchronization, assertions, cleanup, and timing assumptions.
- Build one explicit hypothesis and supporting evidence.
7. Historical analog lookup (read-only):
- Match current hypothesis against `references/flaky-fix-playbook.md` templates.
- Cite at least one analog PR from `references/flaky-pr-corpus.jsonl` if available.
8. TDD patching:
- Add or strengthen a test/assertion that fails before fix.
- Run focused test and confirm red.
- Implement minimal change to address the hypothesis (must use `make gotest` for tests).
- Run focused test and confirm green.
9. Verification gate:
- Run focused package/test commands (must use `make gotest` for tests).
- Run baseline validation for touched scope: at least `make basic-test`; run `make check` when shared logic, dependencies, or tooling are touched.
- Record full commands and outcomes.
- Do not claim success without command evidence.
10. Git + PR:
- Commit follows repo convention and includes sign-off.
- Push branch to `origin`.
- Create draft PR to `tikv/pd:master`.
- PR body must include (referencing pull request template):
  - `Issue Number: ref #<issue-number>`
  - root-cause evidence chain
  - fix summary and risk
  - verification commands/results

## Output Contract

Always end with this exact section structure:
- `Selected issue: #... (url)`
- `Why selected: ...`
- `Root-cause evidence:`
- `Historical analog:`
- `Code changes:`
- `Verification commands + results:`
- `PR: <url>` or `No PR, reason: ...`
- `Next action suggestion:`

## Failure Modes

If any of these occurs, stop and report clearly:
- No eligible issue found.
- `gh` authentication invalid.
- Cannot map failure to a credible hypothesis.
- Verification commands fail for touched scope.
- Push or PR creation fails.
- Workspace is dirty before branch creation.
