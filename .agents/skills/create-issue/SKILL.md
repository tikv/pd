---
name: create-issue
description: Draft and create GitHub issues on tikv/pd using the repository's issue templates. Searches for duplicates, picks the right template, drafts the title/body from the current problem statement, shows the draft to the user for approval, and submits via `gh issue create`. Use when you need a new PD bug report, flaky-test issue, development task, enhancement task, or feature request.
---

# Create PD Issue

## Persona & Goal

PD contributor assistant. Turn a concrete problem statement into a well-scoped `tikv/pd` issue that matches repo conventions, avoids duplicates, and is ready to act on.

Requires `gh` CLI authenticated with `tikv/pd` repo access. Works best from a local `tikv/pd` checkout so the current issue templates can be read directly.

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

Collect the facts needed to file a real issue instead of a placeholder.

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
   gh issue list --repo tikv/pd --state all --search "<keywords>" --limit 10
   ```
   Search by error string, test name, component, API name, PR number, or the expected title prefix.

5. If a likely duplicate already exists, show it to the user and ask whether to reuse it instead of creating a new issue.

### Phase 2: Draft the Issue

> Load only the matching file under `.github/ISSUE_TEMPLATE/` now.

1. Read the chosen template and keep its headings intact.
   Extract the template's `name:` field from frontmatter and use that exact value later with `gh issue create --template`.

2. Draft a title that matches the issue type and real scope:
   - bug: concise symptom or component-level failure
   - flaky test: `<test-name> is flaky`
   - development or enhancement: imperative action or scoped task summary
   - feature request: concise statement of the requested capability

3. Fill every section of the template from concrete evidence. Use `N/A` only when a section truly does not apply. Do not invent repro steps, versions, CI links, or impact claims.

4. If the issue comes from code review or a PR follow-up, link the triggering PR, commit, or discussion in the body.

5. Show the drafted title and full body to the user before creating the issue. Ask for any missing corrections at this stage.

### Phase 3: Create the Issue

After user approval:

1. Use the selected template's frontmatter `name:` field as the `gh --template` argument so GitHub can apply the template's own defaults.
   If the template metadata is missing or ambiguous, stop and ask the user before creation.

2. Create the issue with `gh`. Preserve formatting with a temporary file or heredoc:
   ```bash
   gh issue create --repo tikv/pd --template "<template-name>" --title "<title>" --body-file "<body-file>"
   ```
   Keep the selected template and the drafted body aligned. Do not mix a bug body with a development-task template, or vice versa.

3. Report the created issue number and URL back to the user.

### Phase 4: Post-Creation Handling

- If `gh issue create` reports an existing issue, permission problem, or template-name error, stop and surface the error directly.
- If the user explicitly asks for extra labels, milestones, or projects, add them deliberately. Otherwise, rely on the selected template's defaults instead of passing manual `--label` flags.
- For flaky-test issues, do not submit until the failing job name and at least one CI link are available.

## Common Pitfalls

- Do not skip duplicate search just because the issue seems routine.
- Do not use one template's headings with another template's `--template` name.
- Do not hard-code template names or labels when the selected issue template already defines them in frontmatter.
- Do not add manual `--label` flags for standard PD issue types unless the user explicitly asked for extra labels.
- Do not file flaky-test issues without the failing job name and a concrete CI link.

## Agent Constraints

- Always search for duplicates first.
- Always show the draft before submitting. User approval is required.
- Use `gh` CLI for GitHub operations.
- Keep the drafted body consistent with the selected template.
- Keep the issue scoped to one problem or task.
- Do not modify code. This skill only drafts and creates issues.
