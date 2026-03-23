---
name: code-review
description: "Review a pull request diff against PD project conventions, concurrency safety, backward compatibility, and accumulated team experience. Produces structured findings with severity levels."
---

# Code Review for tikv/pd

## Inputs

The user provides one of:
- A GitHub PR number (e.g. `#10438`) — fetch via `gh pr diff <number> --repo tikv/pd`.
- A local branch — diff against `master` via `git diff master...HEAD`.
- A specific commit range.

## Workflow

### Phase 1: Gather Context

1. Obtain the full diff and the list of changed files.
2. Read the PR description (if remote) to understand the author's intent.
3. **If the PR links an issue** (`Issue Number: close #xxx` or `ref #xxx`), fetch and read the issue content via `gh issue view <number> --repo tikv/pd` to understand the problem background, reproduction steps, and expected behavior.
4. For each changed file, read enough surrounding context (the full file or relevant sections) to understand the change in situ — **never review a diff in isolation**.

### Phase 2: PD-Specific Review Checklist

Apply the following checks to every changed file. These are PD repo-specific invariants and team conventions — not general code-review advice.

#### 2.1 Naming and Semantics Clarity

Reviewers consistently challenge naming choices. This is the most frequent review theme.

- [ ] Function names must accurately describe their scope. If a function only handles TiKV stores, name it `getUpTiKVStores`, not `getUpStores`. If it only considers one engine type, the name should tell the caller.
- [ ] Log messages should distinguish different code paths clearly. Example: `"ru version updated from controller config"` vs `"ru version updated to default from controller config"` — an operator reading logs must understand which path was taken.
- [ ] Prefer defining specific enum types over bare `string`/`int` for values with fixed semantics (e.g. `RUVersionPolicy` enum, not `int32`).

#### 2.2 Error Handling and State Atomicity

Reviewers always ask: "what happens if the second operation fails after the first succeeded?"

- [ ] **No partial state on error paths.** If a function performs two mutations (e.g. persist A then persist B), and B can fail, A must either be rolled back or the whole operation must be atomic. Real example: `SetKeyspaceServiceLimit` + `SetKeyspaceRuVersion` — if the second fails, the first is already persisted → partial state.
- [ ] **Don't acknowledge before persisting.** Never update in-memory state or return success to callers before the underlying storage write succeeds. When modifying state under lock and then persisting after lock release (the common PD pattern to avoid holding locks across etcd calls), clone the old state before modification and rollback on save failure.
- [ ] **Prefer PD's normalized error system for new code.** New domain errors should be defined in `pkg/errs/errno.go` with RFC codes (`PD:<subsystem>:<ErrorName>`), wrapped with `errs.ErrXxx.Wrap(err).GenWithStackByCause()` or `.FastGenByArgs(...)`. Note: existing code still uses `errors.New()` in many places — don't flag accepted in-tree patterns.
- [ ] **Prefer `errcode` for new HTTP error responses in `server/api/`.** Use `apiutil.ErrorResp(rd, w, errcode.NewInvalidInputErr(err))` for new endpoints. Note: many existing handlers still use `h.rd.JSON(...)` or `http.Error(...)` — this is accepted legacy; only flag it when reviewing newly added handlers.
- [ ] **Client sentinel strings are contract.** `errs.IsLeaderChanged(err)` matches strings like `"is not leader"`, `"not leader"`, `"is not served"`. Changing these breaks the client-server protocol.

#### 2.3 Zero-Value Ambiguity and API Design

Reviewers probe zero-value semantics deeply because PD's HTTP APIs and configs are consumed by TiDB/TiKV with strict compatibility expectations.

- [ ] Watch for fields where `0`/`""` is ambiguous between "not set" and "intentionally zero". Example: a `POST` body with `{ru_version: 1}` but no `service_limit` will bind `service_limit` to `0.0`, silently clearing the existing value.
- [ ] New config fields must have sensible zero-value defaults so existing deployments are unaffected on upgrade.

#### 2.4 Concurrency Safety

- [ ] **Prefer `syncutil.Mutex` / `syncutil.RWMutex` in core scheduling and cluster paths** — the `syncutil` wrappers enable deadlock detection in test builds (`deadlock` build tag). Note: not all code uses `syncutil`; peripheral modules (metering, tooling) still embed `sync.Mutex` directly and that is accepted.
- [ ] **Clone-before-store for `atomic.Value` configs.** `PersistOptions` stores config sections in `atomic.Value`. To update: `v := o.GetScheduleConfig().Clone(); v.X = y; o.SetScheduleConfig(v)`. Never modify the loaded pointer directly.
- [ ] **`RegionsInfo` split-lock ordering.** `RegionsInfo` has two locks: `t` (main tree + regions map) and `st` (subtrees: leaders/followers/learners). Never hold both in the wrong order.
- [ ] **Don't hold locks across etcd calls** — etcd round-trips add latency, holding a lock during them causes cascading contention. But beware the consequence: if you modify a map in-place under lock, then release the lock and call `json.Marshal` (for etcd save), another goroutine can write to the same map concurrently → panic. Clone mutable data (especially maps) before the unlocked save.
- [ ] **Rollback safety for concurrent state.** When data can be updated by concurrent heartbeats (e.g. region buckets), ask: "if an older heartbeat arrives after a newer one, does the state roll back?" Probe both the in-memory and visible-to-reader paths.
- [ ] **`RegionInfo` is immutable after creation.** To "modify" a region, create a new `RegionInfo` with options. The `buckets` field is the sole exception (modified via `unsafe.Pointer`).

#### 2.5 Backward Compatibility and Upgrade Safety

Reviewers probe what happens during rolling upgrades and mixed-version clusters.

- [ ] **Persistent data (etcd keys, format)**: changes need migration or versioning. Ask: "what happens if the new PD leader writes this format and then a rollback puts an old PD in charge?"
- [ ] **Scheduler type names**: `types.SchedulerTypeCompatibleMap` maps new names to old ones — new schedulers must register compatibility entries.
- [ ] **Client compatibility**: `pd-client` is a submodule used by TiKV and TiDB. Breaking changes require coordinated releases. Adding a method to a public interface in `client/` is a breaking change for all external implementors — treat it with the same scrutiny as removing a field.
- [ ] **go.mod alignment**: the repo has 4 Go modules (`go.mod`, `client/go.mod`, `tools/go.mod`, `tests/integrations/go.mod`). Dependency versions must be aligned across all of them. This is a guaranteed review blocker.
- [ ] **Metrics backward compatibility**: do not change type (Counter vs Gauge) or remove/rename labels of existing metrics — Grafana dashboards and alerts depend on them.

#### 2.6 Performance & Hot Paths

- [ ] **Hot path awareness**: the following are PD's hottest paths — changes to them require extra scrutiny:
  - **TSO allocation** (`/tso` gRPC): the most latency-sensitive path in the entire TiDB cluster; every transaction depends on it. The TSO uses a two-level save (memory + etcd window) — time must never go backward.
  - **GetRegion / ScanRegions** (`/region` gRPC and HTTP): called on every KV request cache miss; frequency scales with cluster size and traffic.
  - **Region heartbeat processing** (`RegionHeartbeat` gRPC stream): in large clusters this is millions of RPCs per minute.
  - **Store heartbeat processing**: less frequent than region heartbeats but still periodic per store.
- [ ] Metrics on hot paths should use cached `WithLabelValues` (see `add-metrics` skill).
- [ ] Unbounded growth: maps/slices that grow with cluster size (e.g. region count, store count) need bounds or periodic cleanup.
- [ ] **Clone vs no-clone**: when passing data across goroutine boundaries, be explicit about whether a clone is needed. Review conversations frequently discuss defensive copying decisions.

#### 2.7 Testing

Flaky test fixes are the single most active PR category. Reviewers are highly attentive to test correctness.

- [ ] **`testutil.Eventually` anti-pattern**: **never use `re.Equal()` / `suite.Equal()` / `re.NoError()` inside `testutil.Eventually` callbacks** — these call `t.Errorf()` and permanently mark the test as failed even if a later retry succeeds. Use plain Go comparisons + return bool instead.
- [ ] **`suite.Require()` in subtests**: `re := suite.Require()` captures the suite's `*testing.T`. Inside `suite.T().Run("subtest", func(t *testing.T) {...})`, create a new `re := require.New(t)` — using the outer `re` captures the wrong T.
- [ ] **Assertion precision**: prefer `re.Len(items, 1)` over `re.Greater(len(items), 0)` when exact count is known. Use `re.Empty(...)` instead of `re.Len(..., 0)`.
- [ ] **Failpoint discipline**: if the test relies on failpoints, verify it works with `make gotest` (auto enable/disable). Never leave failpoints enabled after test runs.
- [ ] **No `time.Sleep` for synchronization** — use `testutil.Eventually` with polling, or channels with timeout.
- [ ] **HTTP test helpers**: use `testutil.ReadGetJSON`, `testutil.CheckPostJSON`, `testutil.StatusOK(re)` — these are PD's standard test utilities for API testing.

#### 2.8 etcd Interaction

- [ ] **Prefer `etcdutil` / `kv.Base` wrappers for new etcd access paths.** Naked `clientv3.Get`/`Put`/`Delete` calls that bypass the established wrappers should be avoided in new code. Note: existing code legitimately composes `clientv3.Op` / `clientv3.Compare` with `kv.NewSlowLogTxn` — this is an accepted pattern, not a violation.
- [ ] Writes that must be atomic should use `kv.RunInTxn`. Bare `Put` without revision check can cause lost updates under leader transfer.
- [ ] `RunInTxn` provides only optimistic concurrency (compare-and-swap), not repeatable reads. `LoadRange` during a txn creates a condition per key, not for the range itself.
- [ ] Respect `MaxEtcdTxnOps = 120`. Exceeding this causes silent truncation.
- [ ] Always check `resp.Succeeded` after committing a txn.
- [ ] All etcd transactions should go through `SlowLogTxn` which logs slow requests (>1s).

#### 2.9 HTTP API Conventions

- [ ] Use `apiutil.ReadJSONRespondError(rd, w, r.Body, &input)` for body parsing — it handles error responses automatically.
- [ ] Path params: use `apiutil.ParseUint64VarsField(vars, "id")`.
- [ ] All mutating endpoints should have `localLog` audit label in route registration.
- [ ] Keep Swagger annotations (`@Tags`, `@Summary`, `@Param`, `@Success`, `@Failure`, `@Router`) current when modifying APIs.
- [ ] New routes need `registerFunc`/`registerPrefix` with proper method constraints and audit/rate-limiting middleware.

#### 2.10 Architecture and Responsibility

Reviewers challenge design decisions when responsibility boundaries are unclear.

- [ ] **Separation of concerns**: a struct should have one responsibility. Example review feedback: "RuVersion on serviceLimiter is a responsibility mismatch — this struct is about rate limiting, while RU version is a calculation strategy selector."
- [ ] **Duplicate type definitions**: if a type is defined identically in both `client/` and `server/`, flag the maintenance risk if the struct evolves differently on either side.
- [ ] **Bypass paths**: if a generic config endpoint can overwrite specialized config (bypassing validation logic designed for that config), flag it.
- [ ] **Scheduler registration**: new schedulers must follow the Register pattern in `init.go` — register both `SliceDecoderBuilder` and `Scheduler` factory. Default schedulers use `IsDefault()` for special disable-instead-of-remove behavior.
- [ ] **Config persistence lifecycle**: new config fields must be included in `Persist`/`Reload` cycle. TTL overrides take precedence over persistent config (`supportedTTLConfigs` whitelist).

## Agent Constraints

- **Read before judging.** Always read full file context, not just the diff hunk.
- **No false positives over real issues.** If uncertain, use severity `question` rather than `blocker`. If the codebase already does something a certain way, do not flag it as wrong unless it is genuinely buggy. Consistency > personal preference.
- **Do not rewrite the PR.** Suggest minimal, targeted fixes. The goal is to help the author, not to redesign.
- **Flag what CI cannot catch.** Linter issues are already caught by `make check`. Focus on logic, design, concurrency, and compatibility.
- **Be specific.** "This looks wrong" is not a finding. "Line 42: `store.GetMeta()` can return nil when store is tombstone, causing nil-pointer dereference on line 45" is a finding.

## References

| File | Purpose |
|---|---|
| `AGENTS.md` | Full project conventions, build/test commands, style rules |
| `.golangci.yml` | Linter configuration — know what is already enforced |
| `.github/pull_request_template.md` | PR template — verify author filled it properly |
| `CONTRIBUTING.md` | Commit message and linking conventions |
| `pkg/errs/errno.go` | Normalized error definitions — check new errors are added here |
| `pkg/utils/syncutil/` | Custom mutex wrappers with deadlock detection |
| `pkg/utils/testutil/` | Test utilities — `Eventually`, HTTP helpers, leak check |
| `pkg/storage/kv/` | etcd abstraction layer — `RunInTxn`, `SlowLogTxn` |
