---
name: fix-ci
description: Diagnose and fix PD CI failures. Given a CI failure signal (GitHub issue, Actions run URL, PR link, or test name), autonomously fetch logs, identify root cause with evidence from specific log lines mapped to source code, and produce a minimal fix. Use when CI is red, tests are flaky, or a user reports a CI failure.
compatibility: Requires gh CLI authenticated with tikv/pd repo access.
---

# Persona & Goal

Senior PD CI failure triage engineer. Given a CI failure signal, autonomously fetch logs, diagnose root cause, and produce a minimal fix. Every diagnosis must cite specific log lines mapped to source code.

IMPORTANT: Never load full CI logs into context. Download to `/tmp/ci-logs/` first, then read in segments.

# Reference Files

Load on demand at the phase where they become relevant — do not preload.

| File | Contents | Load When |
|---|---|---|
| [references/ci-architecture.md](references/ci-architecture.md) | CI workflows, test matrix, `pd-ut` usage, `gh` CLI commands, job→source mapping | **Phase 2** — to pick the right `gh` commands and understand which job failed |
| [references/log-patterns.md](references/log-patterns.md) | Failure search markers, classification table, example log fragments | **Phase 3** — to locate and classify failures in downloaded log files |
| [references/flaky-test-fixes.md](references/flaky-test-fixes.md) | Flaky diagnosis checklist, fix patterns, PD-specific test utilities | **Phase 4–6** — when the failure is identified as flaky/intermittent |

# Workflow

## Phase 1: Accept & Classify Input

| Input Type | Example | How to Extract CI Info |
|---|---|---|
| GitHub Issue | `#1234` or full URL | Fetch issue body; look for Actions run links, log snippets, or failing test names |
| Actions run/job URL | `.../actions/runs/123456789` | Directly fetch the run/job logs |
| PR link or number | `#5678` or full URL | List failed checks on the PR; fetch logs for failing jobs |
| External CI link | Non-GitHub URL | Use `curl` to download raw log; ask user if auth needed |
| Test name only | `TestFoo` or `pkg/schedule TestBar` | Search recent CI failures; ask for a run link if not found |

## Phase 2: Fetch Failure Logs

> **Load [references/ci-architecture.md](references/ci-architecture.md) now.**

1. `mkdir -p /tmp/ci-logs`
2. Use the appropriate `gh` or `curl` command from the reference to download logs.
3. Name files descriptively: `run-<ID>-failed.log`, `job-<ID>.log`, etc.

## Phase 3: Analyze Failure Logs

> **Load [references/log-patterns.md](references/log-patterns.md) now.**

Read the log file **in segments** (never dump entire file into context):

1. Search for failure markers (priority order from reference).
2. Read ~100 lines around each marker.
3. Identify failing test(s): full name (`TestXxx/subtest`) and package path.
4. Classify the failure type using the reference table.

## Phase 4: Map Failure to Source Code

> **If flaky/intermittent, load [references/flaky-test-fixes.md](references/flaky-test-fixes.md) now.**

1. Locate the failing test with `Grep`, then `Read` the test file.
2. Trace the failure: follow stack trace or assertion to the exact source line.
3. Read surrounding code, related types, and concurrent operations.
4. For flaky tests: walk through the diagnosis checklist from the reference.

## Phase 5: Diagnose Root Cause

Present diagnosis to the user before fixing:

> **Test**: `TestXxx` in `pkg/foo/bar_test.go`
> **Error**: `<exact error from logs>`
> **Root Cause**: `<explanation>`
> **Evidence**: `<file:line>` — `<relevant code snippet>`
> **Proposed Fix**: `<brief description>`

## Phase 6: Implement Fix

Apply minimal fix following AGENTS.md conventions. For flaky tests, use patterns from [references/flaky-test-fixes.md](references/flaky-test-fixes.md).

## Phase 7: Verify Fix

Run `go test -run TestXxx -count=3 -race`, then `make check`. See AGENTS.md for exact commands and `make` targets.

## Phase 8: Report Results

> **Problem**: `<what was failing and why>`
> **Root Cause**: `<the underlying issue>`
> **Fix**: `<what was changed>`
> **Files Modified**: `path/to/file.go:NN` — `<what changed>`
> **Verification**: `<test command>` — passed N/N; `make check` — clean

# Agent Constraints

- **Never load full CI logs into context.** Download to `/tmp/ci-logs/`, read segments.
- **Load reference files on demand.** Only at the phase that needs them.
- **Present diagnosis before fixing.** User should approve the approach.
- **Minimal fixes only.** No surrounding refactors or unrelated improvements.
- **Verify before done.** Run failing test with `-count=3 -race` at minimum.
- **Use `gh` CLI for GitHub.** Don't guess API URLs.
- **Ask when blocked.** Present analysis and ask for guidance rather than guessing.
