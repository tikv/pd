---
name: create-issue
description: Draft and create GitHub issues on tikv/pd using the repository's issue templates. Searches for duplicates, picks the right template, drafts the title/body from the current problem statement, shows the draft to the user for approval, and submits via `gh issue create`.
compatibility: Requires `gh` CLI authenticated with tikv/pd repo access. Works best from a local tikv/pd checkout so issue templates can be read directly.
---

# Create PD Issue

## Reference Files

| File | Contents | Load When |
|---|---|---|
| `.github/ISSUE_TEMPLATE/bug-report.md` | Bug report structure and template metadata | When reporting a bug, regression, or correctness problem |
| `.github/ISSUE_TEMPLATE/flaky-test.md` | Flaky test structure and template metadata | When filing CI instability or test flakiness |
| `.github/ISSUE_TEMPLATE/development-task.md` | Development task structure and template metadata | When filing a concrete engineering task or follow-up |
| `.github/ISSUE_TEMPLATE/enhancement-task.md` | Enhancement task structure and template metadata | When filing an improvement with a known technical direction |
| `.github/ISSUE_TEMPLATE/feature-request.md` | Feature request structure and template metadata | When filing a product or API capability request |

## Workflow

### Phase 1: Gather Context

1. Confirm the repo and GitHub auth are usable:
   ```bash
   git rev-parse --show-toplevel
   git remote -v
   gh auth status
   ```
   If the checkout is not `tikv/pd`, stop and switch to the correct repo before reading templates or creating the issue.

2. Identify the source material for the issue: user prompt, review findings, local diff, CI logs, panic stack, or a linked PR. If critical facts are missing, ask targeted follow-up questions before drafting.

3. Classify the issue into exactly one template:
   - `bug-report`: broken behavior, regression, or correctness issue
   - `flaky-test`: unstable test or CI job
   - `development-task`: routine engineering task, maintenance item, or scoped follow-up
   - `enhancement-task`: engineering improvement with a fairly clear implementation direction
   - `feature-request`: user-facing/API-facing new capability request

4. Search for duplicates before drafting. Start narrow, then broaden:
   ```bash
   gh issue list --repo tikv/pd --state open --search "<keywords>" --limit 10
   ```
   Search by error string, test name, component, API name, PR number, or the expected title prefix. If no open duplicates found, optionally broaden with `--state all` to catch recent regressions.

5. If a likely duplicate already exists, show it to the user and ask whether to reuse it instead of creating a new issue.

### Phase 2: Draft the Issue

> Load only the matching template file under `.github/ISSUE_TEMPLATE/` now.

1. Read the chosen template and keep its headings intact.
   Extract the `labels:` field from frontmatter — these must be passed as explicit `--label` arguments when creating the issue.

2. Draft a title that matches the issue type and real scope:
   - bug: concise symptom or component-level failure
   - flaky test: `<test-name> is flaky`
   - development or enhancement: imperative action or scoped task summary
   - feature request: concise statement of the requested capability

3. Fill every section of the template from concrete evidence. Use `N/A` only when a section truly does not apply. Do not invent repro steps, versions, CI links, or impact claims.

4. If the issue comes from code review or a PR follow-up, link the triggering PR, commit, or discussion in the body.

5. Show the drafted title and full body to the user before creating the issue. Ask for corrections at this stage.

### Phase 3: Create the Issue

After user approval:

1. Write the drafted body to a temporary file to preserve formatting.

2. Create the issue with `gh`, passing the template's labels explicitly:
   ```bash
   gh issue create --repo tikv/pd --label "<label>" --title "<title>" --body-file "<body-file-path>"
   ```
   Do **not** use `--template` — it is mutually exclusive with `--body-file` and triggers a GraphQL error when combined with `--label` ([cli/cli#5017](https://github.com/cli/cli/issues/5017)).

3. If creation fails (permission error, label not found, network issue), surface the error directly and stop.

4. Report the created issue number and URL back to the user.

5. For flaky-test issues, do not submit until the failing job name and at least one CI link are present in the body.

## Common Pitfalls

- Do not skip the duplicate search just because the issue seems routine.
- Do not mix templates: the body headings and `--label` values must come from the same template.
- Do not use `--template` with `gh issue create` — it conflicts with both `--body-file` and `--label` in the current `gh` CLI.
- Do not file flaky-test issues without the failing job name and a concrete CI link.

## Agent Constraints

- Always search for duplicates first.
- Always show the draft before submitting. User approval is required.
- Use `gh` CLI for GitHub operations.
- Keep the drafted body consistent with the selected template's headings.
- Keep the issue scoped to one problem or task.
- Do not modify code. This skill only drafts and creates issues.
