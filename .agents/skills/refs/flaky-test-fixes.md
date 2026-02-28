# Flaky Test Diagnosis & Fix Patterns

## Diagnosis Checklist

Investigate in this order when a test passes sometimes and fails sometimes:

**Timing & Synchronization**
- `time.Sleep` as synchronization — fragile on slow CI machines
- Polling with short timeout or fixed iteration count
- Bare `<-ch` without `select`/`time.After` — blocks forever if sender exits
- Missing `defer cancel()` on contexts — goroutines outlive the test

**Resource Conflicts**
- Hardcoded ports conflicting with parallel tests
- Multiple tests writing to the same temp path (use `t.TempDir()`)
- Global state mutation without `t.Cleanup()` to restore
- Stale cache/singleton data from previous tests

**Ordering & Determinism**
- Map iteration order assumed deterministic (use `sort` or `ElementsMatch`)
- Goroutine scheduling order assumed
- Timestamp precision — same value if executed fast enough

**External Dependencies**
- Embedded etcd/gRPC not fully ready when test starts issuing requests
- Hardcoded short network timeouts insufficient for CI

## Fix Patterns

| Problem | Fix | Key Change |
|---|---|---|
| `time.Sleep` as sync | `testutil.Eventually` / `require.Eventually` | Poll condition with timeout instead of fixed delay |
| Bare `<-ch` blocks forever | `select` with `time.After` | Adds timeout with useful failure message |
| Map iteration order | `sort.Strings(names)` or `require.ElementsMatch` | Remove ordering assumption |
| Global state pollution | Save + `t.Cleanup(func() { restore })` | Isolate test from package-level mutations |
| Goroutine leak | `context.WithCancel` + `t.Cleanup(cancel)` | Goroutine exits via `ctx.Done()` on test end |
| Server readiness | Poll health endpoint with `require.Eventually` | Replace `time.Sleep` before first request |
| Unprotected shared state | Add `sync.RWMutex` around concurrent access | Guard reads and writes |

**Representative example** — replacing `time.Sleep` with event-driven wait (most common PD pattern):

```go
// Before (fragile):
s.startScheduler()
time.Sleep(200 * time.Millisecond)
require.Equal(t, 3, s.getSchedulerCount())

// After (robust):
s.startScheduler()
testutil.Eventually(re, func() bool {
    return s.getSchedulerCount() == 3
}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))
```

## PD-Specific Utilities

- **`testutil.Eventually`** — Polls a condition with configurable timeout and tick. Preferred over `require.Eventually` when using PD's `re` convention.
- **`testutil.WaitUntil`** — Similar polling helper.
- **`tests.NewTestServer`** / **`tests.NewTestCluster`** — Embedded test PD servers/clusters with proper cleanup.
- **Failpoints** (`github.com/pingcap/failpoint`) — Inject faults in tests. Check if the flaky test depends on failpoints being enabled.

When fixing PD tests, prefer PD's own `testutil` helpers over raw `require.Eventually` or manual polling loops.
