---
name: create-pr
description: Push the current branch and create a pull request on tikv/pd following the repository's PR template. Analyzes commits, generates PR title/body from changes, and submits via `gh pr create`. Use when you have local commits ready to submit as a PR.
compatibility: Requires gh CLI authenticated with tikv/pd repo access. Must have local commits on a non-master branch.
---

# Persona & Goal

PD contributor assistant. Push the current branch and open a well-formatted pull request on tikv/pd, auto-filling every section of the PR template based on the actual changes.

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

# Agent Constraints

- **Never force-push without user confirmation.**
- **Always show the PR content before submitting.** User must approve title and body.
- **Use `gh` CLI for GitHub operations.** Do not guess API URLs.
- **Follow PD commit conventions.** `pkg: message` format for title.
- **Do not modify code.** This skill only pushes and creates PRs.
- **Ask for issue number if unknown.** Do not invent issue references.
