---
name: pd-ci-flaky-triage
description: Use when asked to triage recent tikv/pd CI failures and produce flaky-issue actions; agent orchestrates staged source-specific scripts and generates issue text from raw CI logs.
---

# PD CI Flaky Triage

Run staged flaky triage for recent PD CI failures. `Prow` and `GitHub Actions` are collected separately, analyzed separately in step 3, and then merged into shared review files for the later steps.

Workflow properties:
- source pipelines are explicit: `prow` and `actions` do not share an early normalized record type
- all stage handoff happens through JSON artifacts
- each run gets a fresh `RUN_DIR`; step 2 prints `RUN_DIR=<path>`, and every later JSON artifact for that run lives under that directory

## Hard Rules

1. `UNKNOWN_FAILURE` never becomes a GitHub action by itself.
2. Environment-filtered cases must be written to `$RUN_DIR/env_filtered.json` only and must not become GitHub writes.
3. If one issue action cannot be justified from stored failure items and GitHub search results, don't create the action.

## Review File Contracts

`$RUN_DIR/failure_items.json` is an agent-written artifact merged after step 3. The file must contain:

- top-level `window`, `counts`, `failure_items`
- one `failure_items[]` item per extracted failure item
- for each failure item:
  - `candidate_id`
  - `group_key`
  - `source`
  - `source_item_id`
  - `target.test_name`
  - `target.package_name`
  - `signatures`
  - `evidence_lines`
  - `debug_only_evidence_summary`
  - `ci_name`
  - `ci_url`
  - `log_ref`
  - `occurred_at`
  - `commit_sha`
  - `status`
  - `pr_number`
  - `source_details`
  - `failure_family`
  - `excerpt_lines`
  - `excerpt_start_line`
  - `excerpt_end_line`
  - `excerpt_confidence`
  - `excerpt_reason`

`$RUN_DIR/env_filtered.json` is an agent-written artifact merged after step 3. The file must contain:

- top-level `window`, `counts`, `env_filtered`
- one `env_filtered[]` item per filtered failure item
- for each filtered item:
  - `source`
  - `source_item_id`
  - `target.test_name`
  - `target.package_name`
  - `ci_name`
  - `ci_url`
  - `log_ref`
  - `excerpt_lines`
  - `reason`

`$RUN_DIR/flaky_tests.json` is an agent-written artifact from step 4. The file must contain:

- top-level `window`, `counts`, `flaky_tests`, `unknown_failures`
- one `flaky_tests[]` item per test chosen for GitHub handling after review
- for each flaky test:
  - `group_key`
  - `target.test_name`
  - `target.package_name`
  - `failure_item_ids`
  - `ci_links`
  - `pr_numbers`
- one `unknown_failures[]` item per candidate rejected by the review subagent
- for each unknown failure:
  - `group_key`
  - `failure_item_ids`
  - `status`
  - `reason`

## Workflow

1. Verify GitHub authentication.

```bash
gh auth status
```

Stop if:
- `gh auth status` fails

2. Collect recent failures and fetch raw logs.

```bash
python3 scripts/prepare_logs.py --repo tikv/pd --days 1
```

Record the `RUN_DIR=<path>` line printed by the script. Every artifact path below is relative to that run directory.

For testing, you can override the default rolling window with `--start-from`. When `--start-from` is set, `--days` still matters and controls the length of the fixed window starting from that time. Example:

```bash
python3 scripts/prepare_logs.py --repo tikv/pd --start-from 2026-03-20 --days 2
```

Outputs:
- `$RUN_DIR/prow_logs.json`: `Prow` failures with local `log_ref` paths that point to downloaded raw logs
- `$RUN_DIR/actions_logs.json`: `GitHub Actions` failures with local `log_ref` paths that point to downloaded raw logs

Intermediate artifact note:
- Inspect `$RUN_DIR/prow_failures.json` or `$RUN_DIR/actions_failures.json` when you need to view the original failure CI metadata
- `$RUN_DIR/prow_failures.json`: intermediate `Prow` failure index with source item ids, CI URLs, log URLs, PR/SHA metadata, and Prow-side outcome context
- `$RUN_DIR/actions_failures.json`: intermediate `GitHub Actions` failure index with workflow/job identity, CI URLs, run/job ids, and SHA metadata

Collection gap note:
- Do not stop the workflow only because `$RUN_DIR/prow_logs.json` or `$RUN_DIR/actions_logs.json` contains `skipped[]`, `summary.skipped_unknown`, or `summary.command_failed_after_retries`
- Carry those missing-log entries and retry-exhausted commands into the final summary so a human can review them before trusting the run completely

3. Parse raw logs, extract failure items, and select GitHub-facing excerpts.

You should delegate the work to two subagents (one for `Prow` and one for `GitHub Actions`). Each subagent should return structured results for its own source. Do not let either subagent write the merged output files directly.

For each subagent:
1. Read the corresponding `logs.json` artifact to get the list of failures and their `log_ref` paths (`$RUN_DIR/prow_logs.json` and `$RUN_DIR/actions_logs.json`)
2. Open each `log_ref` file directly, extract the failure items, and choose the excerpt. Use `references/stack_snippet_guidelines.md` and `references/stack_snippet_examples.jsonl` as guidelines and examples for excerpt selection.
3. When three or more test failures appear simultaneously in a single log file, categorize all of them as environmental factors and exclude them from further consideration. Return them under `env_filtered[]` for later merge into `$RUN_DIR/env_filtered.json`, and do not include them in `failure_items[]`.
4. Return source-specific `failure_items[]` and `env_filtered[]` data to the main agent. Keep `source` on every item. Store the chosen excerpt on each failure item as `failure_family`, `excerpt_lines`, `excerpt_start_line`, `excerpt_end_line`, `excerpt_confidence`, and `excerpt_reason`. If the exact test name is unclear, keep the best package-level or unknown target you can defend, but still preserve signatures and evidence lines.
5. Write each source result into its own handoff file before the main-agent merge:
   - `$RUN_DIR/prow_source_review.json`
   - `$RUN_DIR/actions_source_review.json`
6. Use this minimal JSON skeleton for each per-source handoff:

```json
{
  "source": "prow",
  "window": {
    "start": "2026-04-01T00:00:00+00:00",
    "end": "2026-04-02T00:00:00+00:00"
  },
  "counts": {
    "failure_items": 0,
    "env_filtered": 0
  },
  "failure_items": [],
  "env_filtered": []
}
```

7. `failure_items[]` in each per-source handoff must carry the fields required by the `$RUN_DIR/failure_items.json` contract. `env_filtered[]` in each per-source handoff must carry the fields required by the `$RUN_DIR/env_filtered.json` contract.
8. The main agent merges both sources and writes `$RUN_DIR/failure_items.json` and `$RUN_DIR/env_filtered.json`

Outputs:
- `$RUN_DIR/prow_source_review.json`: source-specific handoff from the `Prow` subagent
- `$RUN_DIR/actions_source_review.json`: source-specific handoff from the `GitHub Actions` subagent
- `$RUN_DIR/failure_items.json`: merged failure items with source context, excerpt fields, and stable candidate ids
- `$RUN_DIR/env_filtered.json`: merged failure items filtered out due to environmental issues

4. Read the `$RUN_DIR/failure_items.json` file, decide which extracted failures are possible flaky tests, and then run a review subagent on each candidate. Note:

- Skip `UNKNOWN_FAILURE` items. Keep them in `$RUN_DIR/failure_items.json` for inspection, but do not emit them to `$RUN_DIR/flaky_tests.json`.
- If an identical test appears three or more times across different Pull Requests within this file, it is highly likely to be a flaky test.
- If a test already has an open flaky test issue and the error message is nearly identical to the content in that issue, then it should be classified as a flaky test. In this specific scenario, there is no need to consider the number of times the error has occurred.
- If a failed test appears multiple times within a single Pull Request and no errors occurred in other pull requests, it is likely caused by specific commits in that Pull Request rather than being a genuine flaky test. In such cases, do not identify it as a flaky test.
- For each possible flaky test, assign one review subagent to inspect that test's failed CI links directly before keeping it.
- The review subagent should decide whether the candidate is truly flaky. If the subagent finds a concrete reason that the candidate should not be treated as flaky, mark it as `UNKNOWN_FAILURE` and record the reason in `unknown_failures[]`.
- Only candidates that pass this review should remain in `flaky_tests[]`.

Outputs:
- `$RUN_DIR/flaky_tests.json`: reviewed flaky tests ready for GitHub handling, plus reviewed candidates rejected as `UNKNOWN_FAILURE`

5. Based on the identified flaky tests, search GitHub for an existing issue before writing anything.

For each flaky test:
1. Search open `type/ci` issues first, then closed `type/ci` issues.
2. Only accept a match when the test or package evidence is exact enough to defend. `UNKNOWN_FAILURE` must not match on generic words such as `flaky`.
3. If a confident match exists and it is open, reply in the thread with the new CI link.
4. If a confident match exists and it is closed, reopen it and then reply with the new CI link.
5. If no confident match exists, create a new issue. The issue name should not include the test suite portion.

For every flaky test, assign a dedicated sub-agent to search GitHub and handle the corresponding operation independently.

When creating a new issue:
- Use stored `excerpt_lines` directly as the `### Which jobs are failing` code block body
- Do not use a suite summary alone as the final excerpt
- Re-check the matched target against the selected failed CI target before writing GitHub

## Idempotency

- Never comment the same CI link twice on the same issue.
- Never create more than one issue action for the same failure key in one run.
- Unknown items are exempt because they do not create issue actions.

## Body Format

Use this only for creating a new issue.

- `## Flaky Test`
- `### Which jobs are failing`
  - fenced code block with the selected excerpt
- `### CI link` or `### New CI link`
- `### Reason for failure (if possible)`
  - leave empty unless a human adds analysis
- `### Anything else`
  - optional short metadata

## Final Output Template

After the skill finishes, reply with a short execution summary. Do not dump raw JSON. Use this template:

```md
PD CI flaky triage finished.

Window:
- <absolute time window>

Summary:
- scanned failure items: <number>
- env filtered: <number>
- reviewed flaky tests: <number>
- rejected as UNKNOWN_FAILURE: <number>
  - with reasons if possible
- log fetch failures: <number or short list>
- command failures after retries: <number or short list>

GitHub actions:
- created issues: <number or list>
- reopened issues: <number or list>
- commented issues: <number or list>
- skipped actions: <number or list with short reasons>

Needs attention:
- <only include this section when there are blocked items, uncertain matches, skipped log downloads, retry-exhausted commands, or manual follow-up items>
```

Keep the reply concise. Lead with the outcome, then counts, then only the items that still need human attention.

## Resources

- Failure collection and log fetch: `scripts/prepare_logs.py`
- Legacy CI collection and raw-log helpers: `scripts/triage_pd_ci_flaky.py`
- Excerpt guidelines: `references/stack_snippet_guidelines.md`
- Excerpt examples: `references/stack_snippet_examples.jsonl`
