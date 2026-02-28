# CI Log Failure Patterns

## Search Markers

Search in this priority order (most specific first):

| Priority | Pattern | Meaning |
|---|---|---|
| 1 | `--- FAIL:` | Individual Go test failure — followed by test name and duration |
| 2 | `WARNING: DATA RACE` | Go race detector — followed by goroutine stack traces |
| 3 | `panic:` | Runtime panic — followed by stack trace |
| 4 | `fatal error:` | Go runtime fatal (OOM, deadlock, etc.) |
| 5 | `FAIL\tgithub.com/tikv/pd/` | Package-level failure summary |
| 6 | `panic: test timed out` | Test timeout — usually deadlock or hung goroutine |
| 7 | `Error:` | testify assertion failure |
| 8 | `expected` ... `got` | Assertion value mismatch |
| 9 | `undefined:` / `cannot find` | Build/compilation error |
| 10 | `golangci-lint` / `gofmt` | Lint or formatting failure (Check PD workflow) |

## Failure Classification

| Type | Key Indicators | Typical Cause | Strategy |
|---|---|---|---|
| Assertion | `Error:`, `expected X got Y` | Logic bug, stale expectation, order dependency | Compare expected vs actual; check ordering assumptions |
| Data race | `WARNING: DATA RACE`, `Read at`/`Write at` | Missing mutex, concurrent map access | Both stacks show conflicting accesses — add sync at write site |
| Timeout | `test timed out`, `context deadline exceeded` | Deadlock, blocked channel, infinite loop | Find blocked goroutine; check channel ops and lock ordering |
| Panic (nil) | `nil pointer dereference` | Uninitialized field, use-after-close | Stack trace shows exact line; trace nil value origin |
| Panic (OOB) | `index out of range` | Off-by-one, empty slice, concurrent append | Check slice length vs index |
| Build | `undefined:`, `imported and not used` | Missing import, renamed symbol, stale generated code | `make tidy` + `make fmt`; regenerate if needed |
| Lint | `golangci-lint`, linter name in brackets | Style violation | Read message; common: depguard, revive, gofmt |
| Flaky | Passes on retry, "unstable"/"flaky" in issue | Race, timing, port conflict, map order | Run `-count=10 -race`; look for `time.Sleep` as sync |

## Example Log Fragments

**Test failure:**

```
    schedule_controller_test.go:187:
        Error:          Not equal:  expected: 3  actual  : 2
--- FAIL: TestScheduleController/test-pause (0.52s)
FAIL    github.com/tikv/pd/pkg/schedule 4.321s
```

**Data race:**

```
WARNING: DATA RACE
Read at 0x00c000123456 by goroutine 42:
  github.com/tikv/pd/pkg/schedule.(*Coordinator).GetSchedulersController()
Previous write at 0x00c000123456 by goroutine 37:
  github.com/tikv/pd/pkg/schedule.(*Coordinator).RunScheduler()
```

## Reading Logs Efficiently

1. **Start with the end.** Test summary lines (`FAIL`, `ok`, `---`) cluster near the bottom. Read the last 200 lines first.
2. **Search backwards from `FAIL`.** Error details are above the `--- FAIL:` line.
3. **Race conditions** — the `WARNING: DATA RACE` block is self-contained (~15-20 lines).
4. **Panics** — stack trace runs from `panic:` to the next blank line or goroutine boundary.
5. **Timeouts** — `running tests:` section after `panic: test timed out` lists stuck tests and durations.
6. **Ignore noise.** `=== RUN`, `=== PAUSE`, `=== CONT`, `--- PASS:` are test lifecycle noise.
