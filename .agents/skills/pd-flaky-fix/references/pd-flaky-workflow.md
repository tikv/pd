# PD Flaky Workflow Reference

## 1) Select One Eligible Issue

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

## 2) Dedupe Against Open PRs

For each candidate issue number (example: 10247):

```bash
gh search prs '#10247' --repo tikv/pd --state open --limit 30 --json number,title,url,body \
  --jq '.[] | select((.title+"\n"+.body)|test("#10247|10247";"i")) | [.number,.title,.url] | @tsv'
```

If output is non-empty, skip this issue.

## 3) Read Issue Details

```bash
gh issue view <issue-number> --repo tikv/pd --json number,title,body,labels,createdAt,url
```

## 4) Pin Base and Create Branch (Before Code Modifications)

```bash
git fetch upstream master
git diff --quiet && git diff --cached --quiet
git checkout -b codex/flaky-<issue-number>-<short-test-name> upstream/master
```

If clean-workspace check fails, stop and report `No PR, reason: dirty workspace before branching`.

## 5) Locate Test and Neighbors

```bash
rg -n "<TestName>" tests pkg server client
```

Look for nearby stable tests in same suite/file and compare setup/cleanup/assertion style.

## 6) Historical Analog Lookup (Read-Only)

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
SKILLS_ROOT="${SKILLS_ROOT:-$REPO_ROOT/.agents/skills}"
rg -n "<TestName>|<keyword>" "$SKILLS_ROOT/pd-flaky-fix/references/flaky-fix-playbook.md"
rg -n "\"pr_number\"|\"fix_patterns\"" "$SKILLS_ROOT/pd-flaky-fix/references/flaky-pr-corpus.jsonl"
```

Do not modify corpus/playbook in runtime flow.

## 7) Focused Verification Commands

Prefer narrow scope:

```bash
make gotest GOTEST_ARGS='./path/to/pkg -run <TestName> -count=1 -v'
```

If failpoints are required and you run `go test` directly:

```bash
make failpoint-enable
# run go test ...
make failpoint-disable
```

## 8) Commit / Push / Draft PR

```bash
git add <files>
git commit -s -m "tests: stabilize <TestName>"
git push -u origin $(git branch --show-current)
```

Create draft PR to upstream:

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

## 9) PR Body Minimum

- `Issue Number: ref #<issue-number>`
- Root-cause evidence (from issue + code + behavior)
- Historical analog (1+ PR links from corpus)
- What changed and why it is minimal
- Risks/unknowns (especially if no stable local reproduction)
- Verification commands and outcomes
