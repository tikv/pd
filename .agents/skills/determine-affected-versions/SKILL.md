---
name: determine-affected-versions
description: Determine affected PD release versions for a bug issue from its linked GitHub PRs and existing `may-affects-*` labels. Use when an issue already has associated fix PRs and tentative `may-affects-*` labels, and Codex needs to trace the bug-introducing change, then decide which labeled release lines are truly affected, including cherry-picked or equivalent copies, without inferring from the issue body alone.
---

# Goal

Start from linked fix PRs and existing `may-affects-*` labels, find the bug-introducing change, convert truly affected versions to `affects-*`, remove incorrect `may-affects-*`, and leave an issue comment that explains the evidence.

# Workflow

## Phase 1: Resolve candidate versions from the issue

Fetch the issue:

```bash
gh issue view <issue> --repo tikv/pd --json number,title,url,closedByPullRequestsReferences
```

Fetch the issue with labels as well:

```bash
gh issue view <issue> --repo tikv/pd --json number,title,url,labels,closedByPullRequestsReferences
```

Collect:

- every PR number from `closedByPullRequestsReferences`
- every label matching `may-affects-<version>`

Treat the `may-affects-*` labels as the candidate version set for this run.

If the issue has no linked PRs, stop. This skill does not infer affected versions from issue text or comments alone.

If the issue has no `may-affects-*` labels, stop and ask the user whether to add tentative versions first.

Use the official support policy only as a secondary sanity check when a tentative version looks stale or suspicious:

- Support policy: `https://www.pingcap.com/tidb-release-support-policy/`
- Secondary check only when needed: `https://docs.pingcap.com/tidb/stable/release-timeline/`

## Phase 2: Resolve linked fix PRs

For each linked PR, fetch:

```bash
gh pr view <pr> --repo tikv/pd --json number,title,url,state,baseRefName,headRefName,mergeCommit
gh pr diff <pr> --repo tikv/pd
```

Treat linked PRs as candidate fixes, not as direct affected-version evidence.

## Phase 3: Find the introducing change

Inspect the fix diff and isolate the exact buggy code path. Ignore test-only changes when tracing origin.

Find the introducing change with the narrowest search first:

```bash
git log --oneline -S '<token from buggy hunk>' -- <paths>
git log --oneline -G '<pattern from buggy hunk>' -- <paths>
git blame <file>
```

Record:

- introducing commit SHA
- introducing PR if identifiable
- the minimal buggy code path that must exist for a branch to be affected

## Phase 4: Check the candidate `may-affects-*` lines

For each version carried by a `may-affects-<version>` label:

1. Check direct containment:
   ```bash
   git branch -r --contains <introducing-sha>
   ```
2. If the branch does not contain that SHA, inspect the corresponding branch contents anyway.
   - Read the file from the branch with `git show <remote-branch>:<path>`.
   - If the branch is not fetched locally, use `gh api repos/tikv/pd/contents/...?...ref=<branch>`.
3. Compare the relevant code path, not just commit IDs.
   - Mark affected if the branch contains the same buggy implementation or an equivalent cherry-picked copy.
   - Mark not affected if the code path is absent or already differs in the relevant logic.

Important rules:

- Do not infer affected versions from the fix PR `baseRefName`.
- `master` or `main` is evidence that the fix started from trunk, not an `affects-*` label target.
- If the corresponding release branch lacks the file or feature entirely, treat it as not affected.
- Do not expand the candidate set beyond the existing `may-affects-*` labels unless the user explicitly asks for that.

Use a working table like:

- `candidate version`
- `introducing commit contained`
- `equivalent buggy code present`
- `final action`
- `reason`

## Phase 5: Apply labels and comment reasoning

If labeling is requested, handle each candidate version as follows:

- affected: add `affects-<version>` and remove `may-affects-<version>`
- not affected: remove `may-affects-<version>`
- still inconclusive: keep `may-affects-<version>` and explain why in the comment

Example:

```bash
gh issue edit <issue> --repo tikv/pd \
  --add-label affects-7.5 \
  --add-label affects-8.1 \
  --remove-label may-affects-7.1 \
  --remove-label may-affects-7.5 \
  --remove-label may-affects-8.1
```

Use the repository's actual label names. In tikv/pd these are `affects-<version>`.

At the same time, prepare and post a comment that explains why those versions are affected or not affected:

```bash
gh issue comment <issue> --repo tikv/pd --body '<comment body>'
```

The comment should include:

- the candidate versions considered
- the linked fix PRs
- the introducing commit or equivalent buggy code path
- which LTS lines are affected
- which LTS lines are not affected, when that distinction matters
- which `may-affects-*` labels were removed
- which `may-affects-*` labels remain because the result is still inconclusive

Use short factual bullets. Do not post a vague comment like `labeled affected versions`.

# Output

Report:

- `Affected version lines`
- `Labels to add`
- `Labels to remove`
- `Labels to keep`
- `Comment body`
- `Evidence`
- `Gaps / assumptions`

Use wording like:

```text
Affected version lines: 7.5, 8.1, 8.5
Labels to add: affects-7.5, affects-8.1, affects-8.5
Labels to remove: may-affects-6.5, may-affects-7.1, may-affects-7.5, may-affects-8.1, may-affects-8.5
Labels to keep: none
Comment body:
- Candidate versions from issue labels: 6.5, 7.1, 7.5, 8.1, 8.5
- Linked fix PR: #456
- The fix in #456 addresses code introduced by commit abcdef1
- abcdef1 or equivalent buggy logic exists in release-7.5, release-8.1, and release-8.5
- The code path is absent in release-6.5 and release-7.1
- Added labels: affects-7.5, affects-8.1, affects-8.5
- Removed labels: may-affects-6.5, may-affects-7.1, may-affects-7.5, may-affects-8.1, may-affects-8.5
- Kept labels: none
Evidence:
- issue labels provided candidate versions through may-affects-*
- issue #123 links fix PR #456
- PR #456 fixes code introduced by commit abcdef1
- abcdef1 is contained in release-7.5, release-8.1, and release-8.5
- release-6.5 and release-7.1 do not contain the code path
Gaps / assumptions:
- none
```

# Constraints

- Do not infer affected versions from issue text alone.
- Use `may-affects-*` labels on the issue as the primary candidate set.
- Use the official support policy only as a secondary sanity check unless the user asks to expand beyond the existing candidate set.
- Prefer introducing-change containment or equivalent-code evidence over branch-name heuristics.
- When applying labels, also leave a comment that summarizes the reasoning.
- When a candidate version is confirmed affected, replace `may-affects-*` with `affects-*`.
- When a candidate version is confirmed not affected, remove `may-affects-*`.
- When the result is inconclusive, keep `may-affects-*` and state the uncertainty in the comment.
- Do not post a comment without naming the linked fix PR and the concrete evidence used.
- If `closedByPullRequestsReferences` is empty, ask for the fix PR number or extend the workflow later with timeline queries.
- Do not edit code or branches while failpoints are enabled.
