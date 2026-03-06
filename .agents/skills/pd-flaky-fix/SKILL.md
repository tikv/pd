---
name: pd-flaky-fix
description: Use when automatically triaging and fixing flaky test issues in tikv/pd from GitHub issues to draft PR.
---

# PD Flaky Fix

## Scope

This skill is runtime-only. It selects one flaky issue from `tikv/pd`, performs root-cause-first analysis, attempts a minimal fix, validates with evidence, and opens a draft PR.

It uses one repository-maintained historical reference:
- `references/flaky-fix-playbook.md`

The raw corpus is intentionally not checked into the repository in this iteration; treat the playbook as the maintained summary artifact.

If the playbook is missing or does not fit the issue, continue with direct evidence and explicitly report `Historical analog: unavailable`.

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

- Do not rebuild or regenerate historical corpus artifacts in this runtime flow.
- Do not perform repository-maintenance refresh tasks.
- Do not perform monthly maintenance tasks.

## Workflow

1. Identify candidate issues (latest first):

```bash
gh search issues \
  --repo tikv/pd \
  --state open \
  --label type/ci \
  --sort created \
  --order desc \
  --limit 50 \
  --json number,title,createdAt,url,body \
  --jq '.[] | select((.title+"\n"+.body)|test("flaky|unstable|timeout|deadlock|data race|go leak|panic";"i")) | [.number,.createdAt,.title,.url] | @tsv'
```

2. Dedupe by scanning open PRs for issue-number references. For each candidate issue number (example: `10247`):

```bash
gh search prs '#10247' --repo tikv/pd --state open --limit 30 --json number,title,url,body \
  --jq '.[] | select((.title+"\n"+.body)|test("#10247|10247";"i")) | [.number,.title,.url] | @tsv'
```

If output is non-empty, skip this issue.

3. Read issue details and parse:
- `Which jobs are failing`
- `CI link`
- `Reason for failure`

```bash
gh issue view <issue-number> --repo tikv/pd --json number,title,body,labels,createdAt,url
```

4. Pin base and switch branch before any code edits:

```bash
git fetch upstream master
git diff --quiet && git diff --cached --quiet
git checkout -b flaky-<issue-number>-<short-test-name> upstream/master
```

If the clean-workspace check fails, stop and report `No PR, reason: dirty workspace before branching`.

5. Root-cause-first analysis:
- Extract failing test name, stack trace, and error pattern.
- Locate the failing test and nearby stable tests in the same suite or file.
- Compare setup, synchronization, assertions, cleanup, and timing assumptions.
- Build one explicit hypothesis and supporting evidence.

```bash
rg -n "<TestName>" tests pkg server client
```

6. Historical analog lookup (read-only):
- Match the current hypothesis against `references/flaky-fix-playbook.md`.
- Cite one or more representative PRs from the matching playbook pattern if available.

7. TDD patching:
- Add or strengthen a test/assertion that fails before the fix.
- Run the focused test and confirm red.
- Implement the minimal change that addresses the hypothesis.
- Run the focused test and confirm green.

Prefer narrow verification:

```bash
make gotest GOTEST_ARGS='./path/to/pkg -run <TestName> -count=1 -v'
```

If failpoints are required and direct `go test` is unavoidable:

```bash
make failpoint-enable
# run go test ...
make failpoint-disable
```

8. Verification gate:
- Run focused package/test commands.
- Run baseline validation for touched scope: at least `make basic-test`; run `make check` when shared logic, dependencies, or tooling are touched.
- Record the full commands and outcomes.
- Do not claim success without command evidence.

9. Git + PR:
- Commit follows repo convention and includes sign-off.
- Push branch to `origin`.
- Create a draft PR to `tikv/pd:master`.
- PR body must include:
  - `Issue Number: close #<issue-number>`
  - root-cause evidence chain
  - historical analog reference, or `unavailable`
  - fix summary and risk
  - verification commands/results

```bash
git add <files>
git commit -s -m "tests: stabilize <TestName>"
git push -u origin $(git branch --show-current)
```

```bash
GH_USER="${GH_USER:-$(gh api user --jq .login)}"
GH_PROMPT_DISABLED=1 GIT_TERMINAL_PROMPT=0 gh pr create \
  --repo tikv/pd \
  --base master \
  --head "${GH_USER}:$(git branch --show-current)" \
  --title "tests: stabilize <TestName>" \
  --body-file /tmp/pd_flaky_pr_body.md \
  --draft
```

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
