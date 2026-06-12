# Stack Snippet Guidelines

## Objective

Generate `### Which jobs are failing` excerpts that help a reviewer answer three questions quickly:

1. Which exact test or package failed?
2. What is the failure shape?
3. Which frames or traces should be inspected next to fix the flaky test?

The triage script does not generate these snippets. The agent must extract them from raw CI logs.

## Manual Investigation Flow

1. Open the raw log from the CI link.
2. Search the exact test or package first.
3. Treat `--- FAIL:` suite summaries as navigation only.
4. Locate the strongest failure block in the raw log.
5. Classify the block by excerpt shape, not by signature alone.
6. If no strong block exists, keep the item as `UNKNOWN_FAILURE` in `/tmp/failure_items.json` and do not draft an issue.

## Failure Families

### 1. Assertion / Condition

Use when the log contains a strong assertion block.

Keep:
- `=== NAME` or `=== RUN` when it identifies the target test
- the source `file:line` line
- the full `Error Trace`
- `Error:`
- `Test:`

Do not keep:
- unrelated INFO/WARN lines before or after the block
- trailing suite summary lines

Typical size:
- 5 to 20 non-empty lines

### 2. Timeout-Only

Use when there is no stronger assertion block for the same test.

Keep:
- `panic: test timed out after ...`
- `running tests:`
- the exact test line
- the first target frame that points into PD source

Do not keep:
- full goroutine census
- generic runtime frames beyond the first useful target frame

Typical size:
- 4 to 12 non-empty lines

### 3. Goleak / Package

Use package identity, not test identity.

Keep:
- `goleak: Errors on successful test run: found unexpected goroutines:`
- the full unexpected goroutines block
- the package fail line `FAIL github.com/...`

Do not keep:
- unrelated package pass lines after the failing package
- build trailer noise such as `make: ***`

Typical size:
- 8 to 220 non-empty lines

Important:
- This is a full-block exception. Do not reduce it to one goroutine.

### 4. Panic / Package

Use package identity when the panic is package-scoped.

Keep:
- the panic headline (`panic:` or `[panic]`)
- `recover=...` when present
- the embedded or following stack
- `FAIL github.com/...`

Do not keep:
- unrelated service shutdown logs
- generic package pass lines after the fail line

Typical size:
- 2 to 40 non-empty lines

### 5. Deadlock

Keep:
- the assertion header if present (`Error Trace`, `Error`, `Test`)
- `POTENTIAL DEADLOCK:`
- `Previous place where the lock was grabbed`
- `Have been trying to lock it again for more than 30s`
- `Here is what goroutine ... doing now`
- `Other goroutines holding locks`

Do not keep:
- trailing coverage or make output

Typical size:
- 12 to 140 non-empty lines

Important:
- This is also a full-block exception. Preserve the lock relationship report.

### 6. Data Race / Test

Keep:
- a test clue (`=== RUN`, `=== NAME`, or `--- FAIL`)
- `WARNING: DATA RACE`
- `Read at`
- `Previous write at`
- both `Goroutine ... created at` sections

Do not keep:
- long middleware tails after the race report ends

Typical size:
- 12 to 120 non-empty lines

### 7. Unknown Fallback

This is not an excerpt family. It is a stop condition.

If only suite summary lines are available:
- use them to locate the failing subtest
- search the raw log again with the exact test name
- if no stronger block exists, keep the item as `UNKNOWN_FAILURE` in `/tmp/failure_items.json`

Never:
- create a GitHub issue action from an unknown case
- paste a suite summary as the final issue excerpt

## Noise Filters

Drop these unless they are part of the failure block itself:

- timestamp-only prefixes
- `go: downloading ...`
- lifecycle INFO/WARN spam
- `coverage:`
- `run all tasks takes`
- `make: ***`
- copied triage summaries such as `type=...; signatures=...`

## Scoring Rule

When multiple candidate windows exist, prefer the window that maximizes:

1. exact test or package identity
2. strongest failure signal
3. actionable trace depth
4. lowest surrounding noise

## Excerpt Checklist

Before using an excerpt in issue text, confirm:

- the code block is non-empty
- a test or package clue exists
- at least one error anchor exists
- summary-template lines are rejected
- the line budget roughly matches the selected failure family
