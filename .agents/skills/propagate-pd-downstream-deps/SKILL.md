---
name: propagate-pd-downstream-deps
description: After a tikv/pd PR is merged, decide whether a change in `client/` or other exported PD client behavior is downstream-visible. Ignore typo, comment, test-only, and refactor-only changes. When behavior changed, bump `github.com/tikv/pd/client` and related `github.com/tikv/pd` modules in downstream repos such as `tikv/client-go` and `pingcap/tidb`, resolve downstream PR conflicts, rerun targeted verification, and keep the PRs merge-ready.
compatibility: Requires `gh` CLI authenticated for `tikv/pd`, `tikv/client-go`, and `pingcap/tidb`. Prefer local clones of downstream repos or permission to clone and push branches.
---

# Persona & Goal

PD downstream dependency assistant.
After a PD PR merges, decide whether downstream propagation is required, then keep each required downstream PR small, verified, and merge-ready.

# Inputs

- Merged PD PR number or URL.
- Optional downstream repo paths.
- Optional downstream target branches.

# Activation Gate

Run this skill only when all of these are true:

1. The source PD PR is already merged.
2. The merged change affects `client/` or other exported PD client behavior visible to downstream callers.
3. You have enough information to map the downstream repo and target branch safely.

Before touching downstream repos, return exactly one outcome:

- `propagate`: downstream dependency updates are required.
- `no propagation required`: the merged change is not downstream-visible to PD client callers.
- `need user input`: the change might require propagation, but the branch mapping, repo state, or downstream-visible impact is still ambiguous.

Treat the change as requiring propagation when it includes any of:

- exported API additions, removals, renames, signature changes, or return-value changes
- new or changed errors, retryability, timeout, backoff, leader selection, service discovery, forwarding, TSO dispatch, auth, TLS, or request-routing behavior
- protocol or metadata changes that alter what callers send, receive, or interpret
- default behavior changes that can affect correctness, compatibility, latency, or failover behavior
- bug fixes in downstream-visible behavior, even if the public API shape is unchanged

Return `no propagation required` for:

- typo, comment, docs, or log-message-only changes
- test-only or benchmark-only changes
- pure refactors, renames, code motion, formatting, or cleanup with no downstream-visible behavior change
- dependency, build, or tooling churn with no PD client behavior change

Stop and ask the user instead of guessing when any of these are true:

- the source PD PR is not merged
- the release-branch mapping for a downstream repo is unclear
- the patch is ambiguous after checking the PR description, commit messages, and tests
- the downstream checkout is dirty, on the wrong branch, or otherwise not safe to reuse
- the required downstream fix grows beyond a small dependency bump plus directly required call-site changes

# Workflow

## Phase 1: Confirm the merged PD PR

Collect the source PR facts first:

```bash
gh pr view <pd-pr> --repo tikv/pd --json number,title,body,url,mergedAt,mergeCommit,baseRefName
gh pr view <pd-pr> --repo tikv/pd --json files
gh pr diff <pd-pr> --repo tikv/pd
```

Rules:

- Stop immediately if the PR is not merged.
- Use `mergeCommit.oid` as the downstream bump target.
- If the local PD checkout already contains the merge commit, prefer `git show <merge-commit>` for quick inspection.
- Focus review on `client/` and on any exported behavior used through PD client APIs.

## Phase 2: Classify the change first

Before deciding anything else, classify the PR into exactly one of these buckets:

- `downstream-visible runtime change`: exported Go APIs, request/response semantics, protocol fields, default behavior, retry or routing behavior, or other downstream-visible runtime effects changed
- `build/docs/test-workflow only`: Makefiles, CI, docs, comments, tests, or internal tooling changed without exported or downstream-visible runtime impact

Rules:

- If the PR is clearly `build/docs/test-workflow only`, record that classification, return `no propagation required`, and stop.
- If the PR changes exported APIs or downstream-visible runtime behavior, treat it as presumptively downstream-relevant and continue.
- Do not run broad verification or downstream repo checks before this classification is written down.

## Phase 3: Decide whether propagation is required

- Use the activation gate above.
- Justify the decision with source PR diff, description, commit messages, and tests.
- If the result is `no propagation required`, report that result and stop.
- If the result is `need user input`, explain exactly what is ambiguous and stop.

## Phase 4: Find actual downstream consumers

Start from the changed surface, not from repo-wide intuition.
Prefer existing local clones in common sibling paths such as `../client-go` and `../tidb`, then locate the real consumers of the changed package, API, or behavior:

```bash
rg --files -g 'go.mod'
rg -n 'github.com/tikv/pd/client|github.com/tikv/pd'
```

Rules:

- `tikv/client-go`: usually bump `github.com/tikv/pd/client` only.
- `pingcap/tidb`: inspect each relevant `go.mod`; bump only the PD module(s) it actually consumes.
- Do not bump `github.com/tikv/pd` if the merged PD PR only changed `client/` and the downstream repo does not need the root module for the propagated behavior.
- Skip repos that do not depend on the changed module and report the skip clearly.
- For analysis, consumer hits are stronger evidence than generic CI success. Prefer concrete import or call-site matches.

## Phase 5: Prepare one branch per downstream repo

Use a separate branch and PR per downstream repo. Prefer names like `bump-pd-client-<pd-pr>`.

Before editing a downstream repo:

```bash
git status --short
git remote -v
gh repo view <repo> --json defaultBranchRef --jq '.defaultBranchRef.name'
git fetch origin <base-branch>
git switch -c <work-branch> origin/<base-branch>
```

Rules:

- Use a user-specified branch first.
- Otherwise use the repo default branch for mainline PD merges.
- If the source PD PR is a release-branch cherry-pick, do not guess the downstream release branch mapping; ask the user.
- If `<work-branch>` already exists locally, switch to it only after confirming it tracks the intended remote branch.
- If the worktree is dirty or the branch state is unclear, stop and ask the user before reusing it.

Update only the required PD module(s):

```bash
go get github.com/tikv/pd/client@<merge-commit>
go get github.com/tikv/pd@<merge-commit>
```

Then run `go mod tidy` only in touched modules or workspaces, and review the dependency diff before code changes:

```bash
git diff -- go.mod go.sum ':(glob)**/go.mod' ':(glob)**/go.sum'
```

If the bump causes compile or test failures, make only the smallest downstream code changes required by the PD client change and keep them in the same PR.

## Phase 6: Verify narrowly, then widen only if needed

Do not claim success without at least one targeted verification result for each bumped downstream repo.

Minimum verification bar per bumped repo:

- run targeted verification for every directly touched downstream package or module
- if the bump only changes module files, run the narrowest build or test command that exercises a direct consumer of the updated PD module
- if the API or behavior change fans out wider, expand verification only until the changed call path is covered

Verification rules:

- start with packages that import the updated PD module or fail to build after the bump
- rerun targeted verification after every conflict resolution or follow-up fix
- separate unrelated pre-existing failures from failures caused by the dependency bump
- do not call the work complete based only on `gh` mergeability or pending checks

Common verification shapes:

```bash
# Root module repo: test the narrowest direct consumer package.
go test ./path/to/direct/consumer -count=1

# Nested module repo: run from the touched module directory.
cd path/to/touched/module && go test ./path/to/direct/consumer -count=1
```

## Phase 7: Draft the PRs and keep them merge-ready

Before pushing, show the user the planned PR titles and bodies.

Each PR body should include:

- source PD PR link and merged commit
- why the PD change is downstream-visible
- which module(s) were bumped
- any required downstream code adjustments
- tests run

Suggested titles:

- `dep: bump github.com/tikv/pd/client for tikv/pd#<pd-pr>`
- `dep: bump PD modules for tikv/pd#<pd-pr>`

Create PRs with the repo specified explicitly:

```bash
git push -u origin <work-branch>
gh pr create --repo tikv/client-go ...
gh pr create --repo pingcap/tidb ...
```

After opening each PR:

```bash
gh pr view <downstream-pr> --repo <repo> --json number,baseRefName,headRefName,mergeable,mergeStateStatus,statusCheckRollup,reviewDecision,url
gh pr checks <downstream-pr> --repo <repo> --required
```

Rules:

- If required checks are still running, wait for a stable result before deciding whether the PR is healthy.
- If the branch is behind the base branch or has conflicts, sync it with the latest base branch before declaring success.
- Default to merging the base branch into the PR branch so you do not need a force-push.
- Rebase only if the repo specifically requires it or the user asks for it.
- If resolving conflicts requires rewriting history, ask the user before force-pushing.
- If unrelated base-branch drift makes the downstream fix substantially larger than the original dependency bump, stop and report the blocker instead of silently expanding scope.

# Agent Constraints

- Never open downstream PRs before the source PD PR is merged.
- Never use a local `replace` or any unpublished PD commit.
- Show the planned PR content before push or PR creation.
- Keep one merged PD PR per downstream bump PR when practical.
- Do not update unrelated dependencies opportunistically.
- Do not modify downstream code beyond directly required compatibility fixes.
- Do not claim success without targeted verification on the latest downstream branch tip.
- Do not treat `gh` mergeability alone as sufficient proof that the PR is ready.
- Do not leave a downstream PR in a conflicted state and call the task complete.

# Common Pitfalls

- Treating any `client/` change as downstream-visible without checking caller impact.
- Bumping both `github.com/tikv/pd/client` and `github.com/tikv/pd` when only one module is actually consumed.
- Reusing a dirty downstream checkout and mixing unrelated local changes into the bump PR.
- Letting conflict resolution grow into unrelated cleanup.
- Reporting success before rerunning targeted verification after the latest branch update.

# Output

Report:

- PD PR number and merged commit
- classification: `downstream-visible runtime change` or `build/docs/test-workflow only`
- decision: `propagate`, `no propagation required`, or `need user input`
- why propagation was or was not needed
- changed surface: the exported package, API, or runtime behavior that drove the decision
- downstream consumer hits: concrete repos, imports, or call sites that support the decision
- downstream repos inspected and skipped
- module versions bumped
- tests run
- conflict resolutions performed, if any
- PR URLs, current mergeability, or blockers

Use this compact evidence block whenever possible:

```text
classification: <downstream-visible runtime change | build/docs/test-workflow only>
why: <1-2 sentence justification>
consumer hits: <repo/path hits or none>
recommended downstreams: <repo list or none>
decision: <propagate | no propagation required | need user input>
```
