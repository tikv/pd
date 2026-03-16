---
name: create-issue
description: Draft and create GitHub issues on tikv/pd using the repository's issue templates. Searches for duplicates, picks the right template and label, drafts the title/body from the current problem statement, shows the draft to the user for approval, and submits via `gh issue create`. Use when you need a new PD bug report, flaky-test issue, development task, enhancement task, or feature request.
---

# Create PD Issue

## Persona & Goal

PD contributor assistant. Turn a concrete problem statement into a well-scoped `tikv/pd` issue that matches repo conventions, avoids duplicates, and is ready to act on.

Requires `gh` CLI authenticated with `tikv/pd` repo access. Works best from a local `tikv/pd` checkout so the current issue templates can be read directly.

## Reference Files

| File | Contents | Load When |
|---|---|---|
| `.github/ISSUE_TEMPLATE/bug-report.md` | Bug report structure and default label | When reporting a bug, regression, or correctness problem |
| `.github/ISSUE_TEMPLATE/flaky-test.md` | Flaky test structure and default label | When filing CI instability or test flakiness |
| `.github/ISSUE_TEMPLATE/development-task.md` | Development task structure and default label | When filing a concrete engineering task or follow-up |
| `.github/ISSUE_TEMPLATE/enhancement-task.md` | Enhancement task structure and default label | When filing an improvement with a known technical direction |
| `.github/ISSUE_TEMPLATE/feature-request.md` | Feature request structure and default label | When filing a product or API capability request |

## Workflow

### Phase 1: Gather Context

Collect the facts needed to file a real issue instead of a placeholder.

1. Confirm the repo and GitHub auth are usable:
   ```bash
   git rev-parse --show-toplevel
   gh auth status
   ```

2. Identify the source material for the issue: user prompt, review findings, local diff, CI logs, panic stack, or a linked PR. If critical facts are missing, ask targeted follow-up questions before drafting.

3. Classify the issue into exactly one template:
   - `bug-report`: broken behavior, regression, or correctness issue
   - `flaky-test`: unstable test or CI job
   - `development-task`: routine engineering task, maintenance item, or scoped follow-up
   - `enhancement-task`: engineering improvement with a fairly clear implementation direction
   - `feature-request`: user-facing/API-facing new capability request

4. Search for duplicates before drafting. Start narrow, then broaden:
   ```bash
   gh issue list --repo tikv/pd --state all --search "<keywords>" --limit 10
   ```
   Search by error string, test name, component, API name, PR number, or the expected title prefix.

5. If a likely duplicate already exists, show it to the user and ask whether to reuse it instead of creating a new issue.

### Phase 2: Draft the Issue

> Load only the matching file under `.github/ISSUE_TEMPLATE/` now.

1. Read the chosen template and keep its headings intact.

2. Draft a title that matches the issue type and real scope:
   - bug: concise symptom or component-level failure
   - flaky test: `<test-name> is flaky`
   - development or enhancement: imperative action or scoped task summary
   - feature request: concise statement of the requested capability

3. Fill every section of the template from concrete evidence. Use `N/A` only when a section truly does not apply. Do not invent repro steps, versions, CI links, or impact claims.

4. If the issue comes from code review or a PR follow-up, link the triggering PR, commit, or discussion in the body.

5. Show the drafted title, label, and full body to the user before creating the issue. Ask for any missing corrections at this stage.

### Phase 3: Create the Issue

After user approval:

1. Apply the label implied by the template:
   - `bug-report` -> `type/bug`
   - `flaky-test` -> `type/ci`
   - `development-task` -> `type/development`
   - `enhancement-task` -> `type/enhancement`
   - `feature-request` -> `type/feature-request`

2. Create the issue with `gh`. Preserve formatting with a temporary file or heredoc:
   ```bash
   gh issue create --repo tikv/pd --title "<title>" --body-file <body-file> --label "<label>"
   ```

3. Report the created issue number and URL back to the user.

### Phase 4: Post-Creation Handling

- If `gh issue create` reports an existing issue or permission problem, stop and surface the error directly.
- If the issue requires extra labels beyond the template default, add them only when the user asked for them or the repo convention is explicit.
- For flaky-test issues, do not submit until the failing job name and at least one CI link are available.

## Agent Constraints

- Always search for duplicates first.
- Always show the draft before submitting. User approval is required.
- Use `gh` CLI for GitHub operations.
- Keep the issue scoped to one problem or task.
- Do not modify code. This skill only drafts and creates issues.
