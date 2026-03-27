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
6. `git log master..HEAD --format="%B"` — check each commit for **DCO sign-off**.
7. `make check` — run tidy + static + generate-errdoc; gate PR creation on success.

**Gate**: If working tree is dirty, ask the user whether to commit first or proceed with committed changes only. If on `master`, stop and ask the user to create a branch.

**CRITICAL COMMIT REQUIREMENTS** (per CONTRIBUTING.md):

Verify and fix commit requirements before pushing. These checks apply to all commits in the range `master..HEAD`:

- ✅ **Signed-off-by line**: Every commit MUST have `Signed-off-by: Name <email>` matching the commit author (the human developer)
  - Check: `git log master..HEAD --invert-grep --grep="Signed-off-by:" --pretty=oneline`
  - Fix: Prompt user for confirmation, then run `git rebase --signoff master`
  - **CRITICAL**: DCO requires human developer attestation; never use agent identity in Signed-off-by
- ✅ **Subject line ≤70 chars**: PR title / final commit subject must be ≤70 characters
- ✅ **Commit message format**: `pkg: message` or `pkg1, pkg2: message` or `*: message`
- ✅ **Body wrapped at 80 chars**: Commit message body lines must wrap at 80 characters
- ✅ **Body describes why and how**: Not just "what changed" — explain the rationale

## Phase 2: Compose PR Content

> **Load `.github/pull_request_template.md` and [references/pr-template.md](references/pr-template.md) now.**

1. Following the filling guidelines in `references/pr-template.md`, compose the PR title and fill every section of the PR body template.
2. **PR Title Requirements** (per CONTRIBUTING.md):
   - Maximum **70 characters** (enforced by bot)
   - Format depends on number of **non-test** packages changed:
     - 1 package: `pkg: message`
     - 2 packages: `pkg1, pkg2: message`
     - 3+ packages (test-only files excluded from count): `*: message`
   - Examples:
     - `server: fix race condition in leader election`
     - `pd-client, scheduling: improve region split handling`
     - `*: upgrade to Go 1.23`
3. **Commit Message Block Requirements**:
   - The bot extracts content from the `commit-message` code block for the final commit
   - Describe **why** the change was made and **how** it works (not just what)
   - Wrap body lines at **80 characters**
   - **CRITICAL**: Preserve language specifier `commit-message` — never strip to generic code block. The bot fails to parse the commit message without this specifier, breaking DCO compliance.
4. Summarize the problem being solved based on commit messages and diff analysis.
5. Show the composed PR title and full body to the user for review **before** creating the PR. Ask if any section needs adjustment (especially the issue number).

## Phase 3: Push and Create PR

**Gate**: Verify `make check` passed in Phase 1 before proceeding. If it failed, do not push or create the PR.

After user approval:

1. Push the branch:
   ```bash
   git push -u <fork-remote> <branch-name>
   ```
   `<fork-remote>` is the remote pointing to your fork (usually `origin`). Run `git remote -v` to identify it if unsure.

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
- **Follow PD commit conventions.** `pkg: message` format for title, subject ≤70 chars, body wrapped at 80 chars.
- **Preserve language specifiers.** Always use `commit-message` (not generic code blocks) — the PD merge bot extracts this for the final commit. Stripping the specifier breaks DCO compliance.
- **Enforce commit requirements.** Verify and fix DCO sign-offs, message format, and length limits per the canonical "CRITICAL COMMIT REQUIREMENTS" section before pushing. DCO requires human developer attestation; never use agent identity.
- **Do not modify code.** This skill only pushes and creates PRs.
- **Ask for issue number if unknown.** Do not invent issue references.
