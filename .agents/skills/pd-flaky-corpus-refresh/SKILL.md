---
name: pd-flaky-corpus-refresh
description: Use when rebuilding the historical flaky-adjacent PR corpus and fix playbook for pd-flaky-fix.
---

# PD Flaky Corpus Refresh

## Scope

This skill is maintenance-only. It rebuilds the historical corpus and playbook consumed by `pd-flaky-fix`.

Output targets (atomic replace):
- `${SKILLS_ROOT:-<repo-root>/.agents/skills}/pd-flaky-fix/references/flaky-pr-corpus.jsonl`
- `${SKILLS_ROOT:-<repo-root>/.agents/skills}/pd-flaky-fix/references/flaky-fix-playbook.md`

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
REPO_ROOT="$(git rev-parse --show-toplevel)"
SKILLS_ROOT="${SKILLS_ROOT:-$REPO_ROOT/.agents/skills}"
PD_REPO="${PD_REPO:-$REPO_ROOT}" \
  "$SKILLS_ROOT/pd-flaky-corpus-refresh/scripts/build_flaky_pr_corpus.sh"
```

3. Verify:

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
SKILLS_ROOT="${SKILLS_ROOT:-$REPO_ROOT/.agents/skills}"
wc -l "$SKILLS_ROOT/pd-flaky-fix/references/flaky-pr-corpus.jsonl"
rg -n '^## Pattern ' "$SKILLS_ROOT/pd-flaky-fix/references/flaky-fix-playbook.md" | wc -l
```

4. Read refresh summary:

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
SKILLS_ROOT="${SKILLS_ROOT:-$REPO_ROOT/.agents/skills}"
sed -n '1,220p' "$SKILLS_ROOT/pd-flaky-corpus-refresh/references/last-refresh-summary.md"
```

## Output Contract

Report:
- corpus row count
- playbook pattern count (must be >=8)
- source mix (`local_git` vs `api_enriched`)
- any degradation reason (for example no auth/network)
