---
name: fix-cherry-pick-pr
description: Repair cherry-pick pull requests in tikv/pd when automated cherry-picks leave committed conflict markers, drift from the source PR, or need parity verification against the original PR. Use when given a source PD PR and its cherry-pick PR, or a cherry-pick PR that references an original PR, and asked to compare diffs, resolve release-branch cherry-pick conflicts, run failpoint-aware verification, and push the fixed cherry-pick branch.
---

# Fix Cherry-Pick PR

## Overview

Repair and verify cherry-pick PRs in `tikv/pd`.
Compare the source PR and the cherry-pick PR first, then fix the cherry-pick branch without losing release-branch-only code.

## Inputs

- source PR number, or a cherry-pick PR that points back to the source PR
- cherry-pick PR number, head branch, and the remote that owns that branch
- touched files or packages that need parity and targeted verification
- release branch context that may contain branch-only APIs, helpers, or tests

## Outputs

- a repaired cherry-pick branch or PR with committed conflict markers removed
- a semantic parity report against the source PR, including any intentional
  release-branch-only differences
- targeted verification commands and results, especially for failpoint-sensitive
  code paths

## Implementation Details

- Preserve release-branch-only behavior by comparing the final aggregate patch,
  not raw commit count or GitHub mergeability state.
- When conflict markers land beside release-only APIs or helpers, keep the
  release-branch code and insert the source PR behavior in the correct final
  location instead of deleting branch-only code.
- Validate failpoint-sensitive diffs with the repo's explicit failpoint flow:
  `make failpoint-enable`, targeted `go test`, then `make failpoint-disable`.
- Core commands in this workflow are `gh pr view`, `gh pr diff`, `diff -u`,
  `rg -n '<<<<<<<|=======|>>>>>>>'`, `git diff`, `git status`, and `git push`.

## 1. Identify the PR pair

- Inspect the cherry-pick PR with `gh pr view <pr> --repo tikv/pd --json number,title,body,baseRefName,headRefName,headRepositoryOwner,url,commits`.
- Extract the source PR number from one of:
  - a title suffix like `(#10131)`
  - body text like `This is an automated cherry-pick of #10131`
  - the head commit message when the body is ambiguous
- Read the linked issue if present to confirm the real bug and expected behavior.
- Treat GitHub `mergeable` status as insufficient. A PR can be mergeable while still containing committed conflict markers in the patch.

## 2. Compare source and cherry-pick diffs before editing

- Compare changed files first:

```bash
gh pr diff <source-pr> --repo tikv/pd --name-only
gh pr diff <cherry-pick-pr> --repo tikv/pd --name-only
```

- Compare the final aggregate diff, not raw commit count. Cherry-pick PRs can have extra cleanup commits.
- Normalize diff output when checking parity:

```bash
diff -u \
  <(gh pr diff <source-pr> --repo tikv/pd | awk '
      /^diff --git a\// { file = $4; sub("^b/", "", file); next }
      /^\+\+\+ b\// { file = substr($0, 7); next }
      /^--- a\// { next }
      /^[+-][^+-]/ && file != "" { print file ":" $0 }
    ' | sort) \
  <(gh pr diff <cherry-pick-pr> --repo tikv/pd | awk '
      /^diff --git a\// { file = $4; sub("^b/", "", file); next }
      /^\+\+\+ b\// { file = substr($0, 7); next }
      /^--- a\// { next }
      /^[+-][^+-]/ && file != "" { print file ":" $0 }
    ' | sort)
```

- Accept differences caused only by base-branch context:
  - different line numbers or surrounding unchanged context
  - branch-only helper or API definitions that must remain in the release branch
  - different deleted lines when the release branch had already diverged before cherry-pick
- Do not accept:
  - committed `<<<<<<<`, `=======`, `>>>>>>>` markers
  - missing behavior from the source PR
  - extra functional changes not required by the release branch

## 3. Prepare the working branch

- Keep the current worktree clean before switching.
- Fetch the cherry-pick head branch from its actual remote, often `tichi` or `ti-chi-bot`.
- Create a local tracking branch for the cherry-pick head. If the local branch name is free, prefer reusing the remote branch name directly.
- Confirm the expected head commit before editing.

Example:

```bash
git fetch tichi cherry-pick-10131-to-release-8.5
git switch --track tichi/cherry-pick-10131-to-release-8.5
```

- If the local branch name already exists or is occupied by another worktree, create a differently named local branch that still tracks the cherry-pick head.

## 4. Repair the cherry-pick patch

- Search touched files for conflict markers:

```bash
rg -n '<<<<<<<|=======|>>>>>>>' <touched-files>
```

- Resolve each block by reconstructing the final intended code. Do not blindly keep one side.
- Preserve three things at once:
  - the source PR's intended behavior
  - release-branch-only code that existed before cherry-pick and must remain
  - branch-specific adjacent APIs, helpers, or tests
- When the automated cherry-pick inserted conflict markers around an existing release-branch API, keep the API and insert the source PR logic beside it in the correct final location.
- If the cherry-pick introduced no real semantic drift beyond committed markers, keep the follow-up commit minimal. Remove markers and restore the intended final code only.

## 5. Verify in PD with failpoint discipline

- Follow PD's failpoint rules. If the touched tests rely on failpoints, do not rely on plain `go test` results before instrumentation.
- Prefer the narrowest targeted verification first. For the store-limit workflow used in `#10131` and `#10301`:

```bash
make failpoint-enable
go test ./server/cluster -run 'TestCheckCache|TestStoreLimitChangeRefreshLimiter' -count=1 -v -timeout=2m
make failpoint-disable
```

- If you need broader signal, run the narrowest additional package or integration test target that covers the touched files.
- If that extra verification fails in unrelated packages, separate that from the cherry-pick fix. Report the failing package and test name rather than attributing it to the cherry-pick automatically.
- Before finishing, confirm:

```bash
rg -n '<<<<<<<|=======|>>>>>>>' <touched-files>
git diff -- <touched-files>
git status --short --branch
```

- If failpoint-generated files appear after an interrupted run, restore them with `make failpoint-disable` before committing or doing other non-test work.

## 6. Commit and push

- Use a focused follow-up commit that explains the cleanup, for example:

```bash
git commit -s -m 'storelimit: resolve cherry-pick markers in #10301'
```

- Push `HEAD` back to the existing cherry-pick head branch if you have permission.
- If push fails and the user did not specify a fallback, stop and report the push error instead of inventing a new branch plan.

## 7. Report the result

Include:

- source PR number and cherry-pick PR number
- whether the final cherry-pick diff matches the source PR semantically
- whether any extra behavior was introduced
- files repaired
- targeted tests run and their result
- any unrelated broader test failure that remains outside the cherry-pick fix

## Common pitfalls

- Do not equate GitHub `MERGEABLE` with "patch is clean".
- Do not compare raw commit lists. Compare final diffs.
- Do not delete release-branch-only code when resolving a cherry-pick block.
- Do not leave failpoint-generated files in the working tree.
- Do not claim parity until you compare the final aggregate diffs after the fix.
