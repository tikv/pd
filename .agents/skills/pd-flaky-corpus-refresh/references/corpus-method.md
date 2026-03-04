# Corpus Build Method

## Goal

Build a flaky-adjacent merged PR corpus for `tikv/pd` with at least 100 rows, then derive an executable flaky-fix playbook.

## Input Sources

1. Required: local git history
2. Optional: GitHub API via `gh` if auth/network is available

## Scope Keywords

`flaky|unstable|timeout|deadlock|data race|go leak|panic`

## Row Schema (JSONL)

Each line contains:
- `pr_number` (number)
- `merge_commit` (string)
- `title` (string)
- `merged_at` (YYYY-MM-DD)
- `url` (string)
- `keywords` (array of strings)
- `is_test_related` (boolean)
- `files_changed` (array of strings)
- `test_names` (array of strings)
- `fix_patterns` (array of strings)
- `evidence_snippet` (string)
- `source` (`local_git` or `api_enriched`)

## Classification Heuristics

- `race_elimination`: contains `data race`
- `panic_guard`: contains `panic` or `nil`
- `timeout_budget`: contains `timeout`
- `deadlock_avoidance`: contains `deadlock`
- `leak_cleanup`: contains `go leak` or `leak`
- `flaky_stabilization`: contains `flaky` or `unstable`
- `leadership_watch_sync`: contains `leader`, `leadership`, `watch`, `redirect`, `notleader`
- `test_harness_alignment`: test-related files or title contains `test`

## Atomicity

Write to temp files first, then replace targets with `mv`/`os.replace`.
If validation fails (`rows < 100`), keep old targets unchanged.

## Refresh Cadence

Monthly refresh only. Runtime flaky-fix flow must treat corpus/playbook as read-only.
