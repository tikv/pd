---
name: pd-flaky-corpus-refresh
description: Use when rebuilding the historical flaky-adjacent PR corpus and fix playbook for pd-flaky-fix.
---

# PD Flaky Corpus Refresh

## Scope

This skill is maintenance-only. It rebuilds the historical corpus and playbook consumed by `pd-flaky-fix`.

Output targets (atomic replace):
- `/Users/jiangxianjie/.codex/skills/pd-flaky-fix/references/flaky-pr-corpus.jsonl`
- `/Users/jiangxianjie/.codex/skills/pd-flaky-fix/references/flaky-fix-playbook.md`

## Hard Rules

- Do not triage issues.
- Do not modify product code.
- Do not open PRs.
- Build with flaky-adjacent scope.
- Fail if resulting corpus has fewer than 100 rows.
- Keep existing corpus/playbook untouched on failure.

## Workflow

1. Enter PD repo worktree.
2. Run:

```bash
PD_REPO=/Users/jiangxianjie/code/okjiang/pd.worktrees/flaky-fixer \
  /Users/jiangxianjie/.codex/skills/pd-flaky-corpus-refresh/scripts/build_flaky_pr_corpus.sh
```

3. Verify:

```bash
wc -l /Users/jiangxianjie/.codex/skills/pd-flaky-fix/references/flaky-pr-corpus.jsonl
rg -n '^## Pattern ' /Users/jiangxianjie/.codex/skills/pd-flaky-fix/references/flaky-fix-playbook.md | wc -l
```

4. Read refresh summary:

```bash
sed -n '1,220p' /Users/jiangxianjie/.codex/skills/pd-flaky-corpus-refresh/references/last-refresh-summary.md
```

## Output Contract

Report:
- corpus row count
- playbook pattern count (must be >=8)
- source mix (`local_git` vs `api_enriched`)
- any degradation reason (for example no auth/network)
