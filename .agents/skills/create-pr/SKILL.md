---
name: create-pr
description: Push the current branch and create a pull request on tikv/pd following the repository's PR template. Analyzes commits, generates PR title/body from changes, and submits via `gh pr create`. Use when you have local commits ready to submit as a PR.
compatibility: Requires gh CLI authenticated with tikv/pd repo access. Must have local commits on a non-master branch.
---

# Create PR

## Responsibility

Push the current branch and open a pull request on `tikv/pd` that matches the
repository template and review expectations.

## Inputs

- current branch name, base branch, and committed diff to be submitted
- issue references and whether they should be `Close` or `ref`
- `.github/pull_request_template.md` plus
  `references/pr-template.md` for the exact PR structure
- optional head repository or remote details when the PR comes from a fork

## Outputs

- a validated PR title and body draft shown to the user before submission
- a pushed branch with upstream tracking information
- a created PR URL, or a blocking error if push/auth/template validation fails

## Implementation Details

- Validation commands are `git status`, `git branch --show-current`,
  `git log --oneline master..HEAD`, `git diff master...HEAD --stat`, and
  `git diff master...HEAD`.
- PR content must be rendered from `.github/pull_request_template.md` and
  `references/pr-template.md`, not improvised from memory.
- Push with `git push -u origin <branch-name>` unless the user specifies a
  different remote or head repo.
- Create the PR with `gh pr create --repo tikv/pd ... --body-file -` so the
  template formatting survives.
- Do not bypass local hooks with `--no-verify`; if a push hook or auth check
  blocks the operation, surface the error instead of silently skipping it.

## Constraints

- Never force-push without user confirmation.
- Always show the PR content before submitting. User approval is required.
- Use `gh` for GitHub operations. Do not guess API URLs.
- Follow PD commit conventions when deriving the PR title.
- Ask for the issue number when it is unknown.
- Do not invent issue references.
- Do not modify code or rewrite commits as part of this skill.

# Reference Files

| File | Contents | Load When |
|---|---|---|
| `.github/pull_request_template.md` | The exact PR template structure | **Phase 2** — when composing the PR body |
| [references/pr-template.md](references/pr-template.md) | Additional filling guidelines for the PR template | **Phase 2** — when composing the PR body |

# Workflow

## Phase 1: Gather Context

Collect all information needed to fill the PR template. Run these in parallel:

1. `git status` — confirm working tree is clean (no uncommitted changes).
2. `git branch --show-current` — get current branch name.
3. `git log --oneline master..HEAD` — list all commits being submitted.
4. `git diff master...HEAD --stat` — summary of changed files.
5. `git diff master...HEAD` — full diff for analysis.

**Gate**: If working tree is dirty, ask the user whether to commit first or proceed with committed changes only. If on `master`, stop and ask the user to create a branch.

## Phase 2: Compose PR Content

> **Load `.github/pull_request_template.md` and [references/pr-template.md](references/pr-template.md) now.**

1. Following the filling guidelines in `references/pr-template.md`, compose the PR title and fill every section of the PR body template.
2. Summarize the problem being solved based on commit messages and diff analysis.
3. Show the composed PR title and full body to the user for review **before** creating the PR. Ask if any section needs adjustment (especially the issue number).

## Phase 3: Push and Create PR

After user approval:

1. Push the branch:
   ```bash
   git push -u origin <branch-name>
   ```

2. Create the PR:
   ```bash
   gh pr create --repo tikv/pd --title "<title>" --body "<body>"
   ```
   Use a HEREDOC for the body to preserve formatting.

3. Report the PR URL back to the user.

## Phase 4: Post-Creation

If the push or PR creation fails:

- **Push rejected**: Check if the remote branch exists; suggest force-push only if user confirms.
- **PR already exists**: Show the existing PR URL; offer to update it instead.
- **Auth failure**: Remind user to authenticate with `gh auth login`.
