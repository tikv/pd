---
name: code-review
description: "Review a pull request diff against PD project conventions, concurrency safety, backward compatibility, and accumulated team experience. Produces structured findings with severity levels."
---

# Code Review for tikv/pd

## Inputs

The user provides one of:
- A GitHub PR number (e.g. `#10438`) — fetch via `gh pr diff <number> --repo tikv/pd`.
- A local branch — diff against `origin/master` via `git diff origin/master...HEAD` (ensure the base ref is up-to-date).
- A specific commit range.

## Workflow

### Phase 1: Gather Context

1. Obtain the full diff and the list of changed files.
2. Read the PR description (if remote) to understand the author's intent.
3. **If the PR links an issue** (`Issue Number: close #xxx` or `ref #xxx`), fetch and read the issue content via `gh issue view <number> --repo tikv/pd` to understand the problem background, reproduction steps, and expected behavior.
4. For each changed file, read enough surrounding context (the full file or relevant sections) to understand the change in situ — **never review a diff in isolation**.

### Phase 2: Review with PD-Specific Focus

Perform a thorough code review covering correctness, design, and maintainability as usual. In addition, **pay special attention** to the following PD repo-specific invariants and team conventions:

#### 2.1 Naming and Semantics Clarity

Reviewers consistently challenge naming choices. This is the most frequent review theme.

- [ ] **All identifiers must be clear and consistent** — function, variable, type, and package names should accurately describe their scope, avoid stuttering (e.g. `router.RouterClient` → `router.Client`), and stay consistent with related names. If a function only operates on a subset of entities (e.g. one store engine type), the name should tell the caller.
- [ ] **Log messages should distinguish code paths clearly** so that an operator reading logs can tell which branch was taken.
- [ ] **Functions called with a lock held should use the `xxxLocked` suffix** to signal callers that the lock must already be acquired.
- [ ] **Non-obvious logic needs a "why" comment.** Intentional no-ops, Go switch fallthrough behavior, default value choices, and inverted conditions must have inline comments explaining the reasoning. Stale comments are bugs — update or remove them when the code changes.

#### 2.2 Error Handling and State Atomicity

Reviewers always ask: "what happens if the second operation fails after the first succeeded?"

- [ ] **No partial state on error paths.** If a function performs two mutations and the second can fail, the first must either be rolled back or the whole operation must be atomic.
- [ ] **Don't acknowledge before persisting.** Never update in-memory state or return success before the underlying storage write succeeds. When modifying state under lock and persisting after lock release (the common PD pattern to avoid holding locks across etcd calls), clone the old state before modification and rollback on save failure.
- [ ] **Prefer PD's normalized error system for new code.** New domain errors should be defined in `pkg/errs/errno.go` with RFC codes (`PD:<subsystem>:<ErrorName>`). Note: existing code still uses `errors.New()` in many places — don't flag accepted in-tree patterns.
- [ ] **Prefer `errcode` for new HTTP error responses in `server/api/`.** Note: many existing handlers still use `h.rd.JSON(...)` or `http.Error(...)` — this is accepted legacy; only flag it in newly added handlers.
- [ ] **Client sentinel strings are contract.** `errs.IsLeaderChanged(err)` matches specific string literals used for leader-change detection. Changing these breaks the client-server protocol.
- [ ] **Validate all inputs before mutating any state.** When a handler or function updates multiple fields, validate every field upfront. Go map iteration is non-deterministic — if validation fails mid-iteration, earlier fields are already committed.
- [ ] **Guard nil returns and empty slices.** Check for nil before calling methods on potentially-nil protobuf messages (e.g. `store.GetMeta()` on tombstone), optional returns, or indexing into potentially-empty slices. Prefer early return over deep nesting.
- [ ] **Error messages should include diagnostic values.** `"request count overflow"` is not debuggable; `"request count overflow: total=%d, requests=%d"` is. Always include the actual values that triggered the error.

#### 2.3 Zero-Value Ambiguity and API Design

Reviewers probe zero-value semantics deeply because PD's HTTP APIs and configs are consumed by TiDB/TiKV with strict compatibility expectations.

- [ ] **Zero-value ambiguity.** Watch for fields where `0`/`""` is ambiguous between "not set" and "intentionally zero" — a `POST` body omitting a field will silently bind to the zero value, potentially clearing an existing setting.
- [ ] **New config fields need sensible zero-value defaults** so existing deployments are unaffected on upgrade.

#### 2.4 Concurrency Safety

- [ ] **Prefer `syncutil.Mutex` / `syncutil.RWMutex` in core scheduling and cluster paths** — the `syncutil` wrappers enable deadlock detection in test builds (`deadlock` build tag). Note: peripheral modules still use `sync.Mutex` directly and that is accepted.
- [ ] **Clone-before-store for `atomic.Value` configs.** `PersistOptions` stores config sections in `atomic.Value`. Always clone before modifying, then store the clone. Never modify the loaded pointer directly.
- [ ] **`RegionsInfo` split-lock ordering.** `RegionsInfo` has two locks: `t` (main tree) and `st` (subtrees). Never hold both in the wrong order.
- [ ] **Don't hold locks across etcd calls.** But beware: if you modify a map in-place under lock, then release the lock and `json.Marshal` for etcd save, another goroutine can write to the same map concurrently → panic. Clone mutable data before the unlocked save.
- [ ] **Rollback safety for concurrent state.** When data can be updated by concurrent heartbeats, ask: "if an older heartbeat arrives after a newer one, does the state roll back?"
- [ ] **`RegionInfo` is immutable after creation.** To "modify" a region, create a new `RegionInfo` with options.
- [ ] **Goroutine lifecycle management.** Create a cancellable context before starting goroutines — never bind to a parent callback context that outlives the owner. Include `ctx.Done()` in all `select` statements inside retry/polling loops. Ensure `close()` can stop all owned goroutines cleanly.
- [ ] **`defer logutil.LogPanic()` at the top of goroutine functions.** Without this, a panic in a background goroutine crashes silently. This is a hard requirement in PD.
- [ ] **Use `RLock` for read-only access.** When a function only reads shared state under a mutex, prefer `RLock()`/`RUnlock()` over `Lock()`/`Unlock()` to reduce contention.
- [ ] **Clean up process-wide side effects on leadership change.** When a leader/primary steps down, reset process-global state (GC tuner settings, memory limits, cached leader values). These survive goroutine cancellation and can affect the next leader term.

#### 2.5 Backward Compatibility and Upgrade Safety

Reviewers probe what happens during rolling upgrades and mixed-version clusters.

- [ ] **Persistent data (etcd keys, format)**: changes need migration or versioning. Ask: "what happens if the new PD writes this format and then a rollback puts an old PD in charge?"
- [ ] **Scheduler type names**: if renaming an existing scheduler that was already persisted, add a mapping to `types.SchedulerTypeCompatibleMap`. Brand-new schedulers don't need compat entries.
- [ ] **Client compatibility**: `pd-client` is a submodule used by TiKV and TiDB. Adding a method to a public interface in `client/` is a breaking change for all external implementors.
- [ ] **go.mod alignment**: the repo has multiple Go modules (root + submodules). Dependency versions must be aligned across all of them — this is a guaranteed review blocker.
- [ ] **Metrics backward compatibility**: do not change type (Counter vs Gauge) or remove/rename labels of existing metrics — Grafana dashboards and alerts depend on them.

#### 2.6 Performance & Hot Paths

- [ ] **Hot path awareness**: TSO allocation, GetRegion/ScanRegions, Region heartbeat, Store heartbeat — these are PD's hottest paths. Changes to them require extra scrutiny on allocations, lock contention, and computational complexity.
- [ ] **Metrics on hot paths** should use cached `WithLabelValues` (see `add-metrics` skill).
- [ ] **Unbounded growth**: maps/slices that grow with cluster size need bounds or periodic cleanup.
- [ ] **Pre-allocate slices/maps** when size is known or can be estimated; avoid repeated `append` growth in hot paths.
- [ ] **Clone vs no-clone**: when passing data across goroutine boundaries, be explicit about whether a clone is needed.
- [ ] **Prefer Prometheus metrics over ad-hoc logging for observability.** If a PR adds `log.Info` to track an indicator, ask whether a Prometheus counter/gauge/histogram would be more appropriate — dashboards are better than log-grep.
- [ ] **Log level must match call frequency.** Use `log.Debug` on high-frequency paths (heartbeat, token request); `log.Info` for infrequent-but-expected events. Retry loops that log on every etcd failure will cause log storms during brief outages.

#### 2.7 Testing

Flaky test fixes are the single most active PR category. Reviewers are highly attentive to test correctness.

- [ ] **`testutil.Eventually` anti-pattern**: never use `re.Equal()` / `suite.Equal()` / `re.NoError()` inside `testutil.Eventually` callbacks — these call `t.Errorf()` and permanently mark the test as failed even if a later retry succeeds. Use plain Go comparisons + return bool instead.
- [ ] **`suite.Require()` in subtests**: inside `suite.T().Run(...)`, create a new `re := require.New(t)` — using the outer suite's `re` captures the wrong `*testing.T`.
- [ ] **Failpoint discipline**: if the test relies on failpoints, verify it works with `make gotest` (auto enable/disable). Never leave failpoints enabled after test runs.
- [ ] **Avoid `time.Sleep` for polling/synchronization** — prefer `testutil.Eventually` or channels with timeout. Note: `time.Sleep` is accepted for rate-limiting tests, waiting for a ticker to fire, or other cases where a real delay is the intent. When adding `testutil.Eventually` to fix a flaky test, understand the root cause first — if the flakiness stems from a race or missing synchronization, fix the root cause instead of retrying.
- [ ] **HTTP test helpers**: use `testutil.ReadGetJSON`, `testutil.CheckPostJSON`, `testutil.StatusOK(re)` for API testing.
- [ ] **Assert expected state positively.** `re.NotEqual(old, current)` can pass if the value is nil or unrelated. Always also assert the expected new state (e.g. `re.Equal(expected, current)`). Prefer exact cardinality (`len(x) == 1`) over loose checks (`len(x) > 0`).

#### 2.8 etcd Interaction

- [ ] **Prefer `etcdutil` / `kv.Base` wrappers for new etcd access paths.** Note: existing code legitimately composes `clientv3.Op` / `clientv3.Compare` with `kv.NewSlowLogTxn` — this is an accepted pattern.
- [ ] **Atomic writes need `kv.RunInTxn`.** Bare `Put` without revision check can cause lost updates under leader transfer.
- [ ] **`RunInTxn` is optimistic concurrency only**, not repeatable reads. `LoadRange` during a txn creates a condition per key, not for the range itself.
- [ ] **Respect `MaxEtcdTxnOps = 120`.**
- [ ] **Always check `resp.Succeeded`** after committing a txn.
- [ ] **All etcd transactions should go through `SlowLogTxn`** which logs slow requests (>1s).

#### 2.9 HTTP API Conventions

The PD server API (`server/api/`, gorilla/mux) and MCS APIs (`pkg/mcs/*/`, gin) follow different conventions:

- [ ] **PD server API** (`server/api/`): use `apiutil.ReadJSONRespondError` for body parsing, `apiutil.ParseUint64VarsField` for path params, `registerFunc`/`registerPrefix` for route registration with audit/rate-limiting middleware.
- [ ] **MCS APIs** (`pkg/mcs/`): use gin's `c.ShouldBindJSON()` for body parsing and gin router for route registration — this is the accepted pattern for microservice modules.
- [ ] **All mutating endpoints need audit logging** — `localLog` audit label (PD server API) or equivalent.
- [ ] **Keep Swagger annotations current** when modifying APIs.
- [ ] **HTTP status codes must match error semantics.** 400 = malformed client request; 403 = forbidden by server config; 404 = resource not found; 500 = internal error. Do not return 400 for server-side configuration issues or 500 for "not found."
- [ ] **Always `return` after writing an error response.** A missing `return` after `h.rd.JSON(w, code, err)` or `c.AbortWithStatusJSON(...)` allows the handler to continue executing, potentially double-writing the response or applying the mutation anyway.

#### 2.10 Architecture and Responsibility

- [ ] **Separation of concerns**: a struct should have one responsibility. Don't put unrelated state on a struct just because it's convenient.
- [ ] **Duplicate type definitions**: if a type is defined identically in both `client/` and `server/`, flag the maintenance risk if the struct evolves differently on either side.
- [ ] **Bypass paths**: if a generic config endpoint can overwrite specialized config bypassing its validation logic, flag it.
- [ ] **Scheduler registration**: new schedulers must follow the Register pattern — register both `SliceDecoderBuilder` and `Scheduler` factory.
- [ ] **Config persistence lifecycle**: new config fields must be included in `Persist`/`Reload` cycle. TTL overrides take precedence over persistent config.

### Phase 3: Report

Present findings in a structured, consistent format:

```
### <Section Title>

**<file_path>:<line>** — <one-line summary>
**Severity**: blocker | warning | question | nit
<explanation and suggested fix>
```

Severity definitions:
- **blocker** — correctness bug, data loss risk, or guaranteed CI/review rejection (e.g. data race, broken backward compatibility).
- **warning** — likely problem or significant design concern that should be addressed before merge.
- **question** — uncertain whether this is a problem; needs author clarification.
- **nit** — style, naming, or minor improvement; non-blocking.

End with a summary line: total findings by severity and an overall assessment (approve / request changes / needs discussion).
