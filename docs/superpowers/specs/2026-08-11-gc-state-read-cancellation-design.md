# GC state read cancellation design

This document designs an end-to-end context propagation path for PD GC state
reads. The change lets a canceled gRPC or HTTP request terminate its in-flight
etcd operations and release the GC state manager read lock without waiting for
the internal 10-second storage timeout.

Status: Draft for written review

Date: August 11, 2026

Related issue: [tikv/pd#11131](https://github.com/tikv/pd/issues/11131)

## Summary

PD currently drops the request context between the network handler and the GC
state storage layer. A canceled request can therefore continue issuing etcd
reads while holding `GCStateManager.mu.RLock()`. This blocks GC state writers
and leader transitions until the storage operation finishes or reaches its
internal timeout.

The design passes the request context through the shared GC state read chain
and introduces context-aware capabilities for etcd reads and raw transactions.
The context-aware GC transaction path requires those capabilities and never
silently falls back to a contextless operation.

The design preserves the current revision validation, cache, locking, public
protocol, and legacy GC write behavior.

## Problem

The GC state gRPC handler receives a request context, but the normal and
combined read paths do not pass it to `GCStateManager`. The storage interfaces
then create operation contexts from the long-lived etcd client context instead
of the request context.

The affected slow path performs several contextless operations while holding a
global manager read lock:

- It reads the GC state revision.
- It reads the transaction safe point.
- It reads the GC safe point.
- It optionally reads local GC barriers.
- It optionally reads global GC barriers.
- It commits a raw transaction that validates the revision.

The PD client uses a default request timeout that is shorter than the internal
etcd storage timeout. The client can therefore stop waiting while the PD server
continues the work and holds `GCStateManager.mu.RLock()`.

The lock is global across keyspaces. A single abandoned read can delay:

- GC state writes.
- GC barrier updates.
- Leader and follower transitions.
- Cache invalidation during a leadership change.

The current behavior does not create an identified unsafe GC or data corruption
path. It creates an availability and resource-lifetime problem.

## Scope

This design covers the shared GC state read chain used by the normal,
combined, and all-keyspace APIs. It keeps unrelated storage and write behavior
out of scope.

### Goals

The implementation must satisfy the following goals:

- Pass a live request context from gRPC and HTTP handlers to every etcd
  `Range` and final `Txn.Commit` in a GC state read.
- Cancel the shared `GetAllKeyspacesGCStates` execution only when its
  `OrderedSingleFlight` execution context is canceled.
- Release `GCStateManager.mu.RLock()` after an in-flight etcd operation
  observes cancellation.
- Preserve revision validation and the all-or-nothing combined result.
- Prevent a canceled single or combined read from updating the cache.
- Preserve valid cache entries produced by completed keyspace reads in a
  partially canceled all-keyspace scan.
- Preserve the existing public gRPC, protobuf, `pd/client`, HTTP URL, and JSON
  contracts.
- Preserve existing contextless storage APIs for legacy callers.
- Reject backends that do not support a context-aware GC state transaction
  instead of silently falling back.
- Avoid duplicate error logs for expected request cancellation.

### Non-goals

The implementation does not include the following changes:

- It does not replace `sync.RWMutex` with a context-aware lock.
- It does not cancel a goroutine while it waits to acquire
  `GCStateManager.mu.RLock()`.
- It does not change the lock ownership or cache generation model.
- It does not make every `kv.Base` operation context-aware.
- It does not change existing GC write transaction cancellation behavior.
- It does not change the standalone manager
  `LoadAllGlobalGCBarriers` method.
- It does not redesign the GC revision transaction as one batched etcd read.
- It does not add context methods to memory or LevelDB solely for this change.
- It does not add Prometheus metrics, labels, dashboards, or alerts.

## Backend constraints

Memory and LevelDB are production backends in PD, but they do not carry the GC
state revision transaction.

PD uses the backends as follows:

- etcd is the default metadata backend and stores GC state.
- The scheduling microservice uses a memory-backed storage for its in-process
  state.
- PD uses LevelDB for local Region metadata and hot-region history.

The current `RawTxnCapable` implementation exists only on the etcd backend.
`RunInGCStateTransaction` already requires this capability, so memory and
LevelDB cannot run the GC state revision transaction today.

The new contextual transaction can therefore require context-aware etcd
capabilities without adding a behavioral fallback for another production GC
state backend.

## Proposed architecture

The implementation propagates one context through the request, manager,
provider, endpoint, KV, and etcd client layers.

The data flow is:

```text
gRPC context or HTTP request context
    -> GCStateManager
    -> GCStateProvider context-aware methods
    -> StorageEndpoint context-aware capability checks
    -> etcdKVBase
    -> etcd client Range or Txn
```

No component stores the context in a long-lived struct. Functions that perform
external work receive `context.Context` as their first parameter.

### Network entry points

The protobuf and generated gRPC interfaces already provide a context. The
server passes it into the manager for both branches of `GetGCState`:

```go
gcStateManager.GetGCState(
    ctx,
    keyspaceID,
    request.GetExcludeGcBarriers(),
)

gcStateManager.GetGCStateWithGlobalGCBarriers(
    ctx,
    keyspaceID,
    request.GetExcludeGcBarriers(),
)
```

`GetAllKeyspacesGCStates` already accepts a context. Its public signature
remains unchanged.

The HTTP handlers pass their request contexts:

- The standard library handler uses `r.Context()`.
- The Gin handler uses `c.Request.Context()`.

These changes do not modify routes, parameters, response fields, or successful
status codes.

### Manager API

The internal manager read APIs become context-first:

```go
func (m *GCStateManager) GetGCState(
    ctx context.Context,
    keyspaceID uint32,
    excludeGCBarriers bool,
) (GCState, error)

func (m *GCStateManager) GetGCStateWithGlobalGCBarriers(
    ctx context.Context,
    keyspaceID uint32,
    excludeGCBarriers bool,
) (GCState, []*endpoint.GlobalGCBarrier, error)
```

The private slow-path methods also receive the context:

- `getGCStateImpl`
- `getGCStateImplSlow`
- `getGCStateInTransaction`

The manager checks `ctx.Err()` at the public entry and again after acquiring
`m.mu.RLock()`. The second check prevents a request that expired while waiting
for the lock from starting new storage work.

### All-keyspace singleflight context

`GetAllKeyspacesGCStates` must use the execution context supplied by
`OrderedSingleFlight`, not a context captured from an individual caller.

The required pattern is:

```go
singleflight.Do(callerCtx, func(execCtx context.Context) (...) {
    return m.iterateAllKeyspacesGCStates(execCtx, ...)
})
```

The implementation passes `execCtx` to:

- The null keyspace read.
- Every keyspace-level GC state read.
- Every provider method.
- Every etcd `Range` and `Txn.Commit`.

This preserves the existing reference-counted cancellation semantics:

- Canceling one waiter ends only that waiter's call.
- The shared execution continues while another waiter remains.
- Canceling all waiters cancels `execCtx`.
- Canceling `execCtx` terminates the current storage operation and scan.

### Provider API

The existing contextless provider methods remain available for GC write paths
and legacy callers. The new read path uses companion methods.

The provider adds:

```go
func (p GCStateProvider) RunInGCStateTransactionWithContext(
    ctx context.Context,
    f func(wb *GCStateWriteBatch) error,
) error

func (p GCStateProvider) LoadTxnSafePointWithContext(
    ctx context.Context,
    keyspaceID uint32,
) (uint64, error)

func (p GCStateProvider) LoadGCSafePointWithContext(
    ctx context.Context,
    keyspaceID uint32,
) (uint64, error)

func (p GCStateProvider) LoadAllGCBarriersWithContext(
    ctx context.Context,
    keyspaceID uint32,
) ([]*GCBarrier, error)

func (p GCStateProvider) LoadAllGlobalGCBarriersWithContext(
    ctx context.Context,
) ([]*GlobalGCBarrier, error)
```

The existing `RunInGCStateTransaction` and contextless read methods retain
their current behavior. This prevents the focused read fix from changing
advance-safe-point, barrier write, compatibility, and lifecycle code.

`RunInGCStateTransactionWithContext` uses the context for:

1. The initial revision read.
2. Every context-aware read invoked by its callback.
3. The final raw transaction and revision validation.

The callback continues to build a `GCStateWriteBatch`, although the new manager
read paths do not add write operations.

### Storage capabilities

The existing `kv.Base` and `RawTxnCapable` interfaces remain unchanged. The KV
package adds independent capabilities:

```go
type ContextReader interface {
    LoadWithContext(
        ctx context.Context,
        key string,
    ) (string, error)

    LoadRangeWithContext(
        ctx context.Context,
        key, endKey string,
        limit int,
    ) ([]string, []string, error)
}

type RawTxnCapableWithContext interface {
    CreateRawTxnWithContext(ctx context.Context) RawTxn
}
```

The etcd backend implements both interfaces.

The context-aware GC transaction checks both capabilities before its first
storage operation. If either capability is unavailable, it returns an explicit
unsupported error. It does not execute a partial read and does not call the
contextless methods.

The old interfaces remain available:

- `EtcdKVGet` continues to use the etcd client context.
- `CreateRawTxn` continues to use `NewSlowLogTxn`.
- `RunInGCStateTransaction` continues to use the old storage path.

### Etcd helper behavior

The etcd utility package adds:

```go
func EtcdKVGetWithContext(
    ctx context.Context,
    client *clientv3.Client,
    key string,
    opts ...clientv3.OpOption,
) (*clientv3.GetResponse, error)
```

The helper checks `ctx.Err()` before it derives an operation context or invokes
the etcd client. An already-canceled request therefore does not start an RPC.

The helper derives the operation timeout from the request context:

```go
opCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
defer cancel()
```

The effective deadline is the earlier of the request deadline and the internal
etcd operation timeout.

The legacy helper delegates with the client context:

```go
func EtcdKVGet(
    client *clientv3.Client,
    key string,
    opts ...clientv3.OpOption,
) (*clientv3.GetResponse, error) {
    return EtcdKVGetWithContext(
        client.Ctx(),
        client,
        key,
        opts...,
    )
}
```

The etcd KV backend also adds:

```go
func (kv *etcdKVBase) CreateRawTxnWithContext(
    ctx context.Context,
) RawTxn
```

It constructs the transaction with the existing
`NewSlowLogTxnWithContext` helper. It does not add a goroutine or another
timeout layer beyond the timeout that the old transaction already creates.

## Lock and cache semantics

The implementation retains the current critical section because the manager
lock coordinates cache updates with leader and follower transitions.

The slow path remains:

```text
acquire RLock
    -> check context
    -> check cache again
    -> read and validate storage
    -> update cache after success
    -> release RLock
```

Releasing the lock before storage I/O would permit a leadership transition to
clear the cache before an older read writes its result back. This design avoids
that race by retaining the lock and making the locked I/O cancellable.

### Cancellation stages

Each cancellation stage has a defined result.

- If the context is already canceled at manager entry, the call returns the
  context error without reading the cache or acquiring the manager lock.
- If cancellation races with a fast cache lookup, a completed lookup can still
  return successfully.
- If cancellation occurs while waiting for `RLock`, the mutex wait continues.
  The manager checks the context after acquiring the lock and returns without
  storage I/O.
- If cancellation occurs during any etcd `Range`, that operation returns the
  context error, the transaction fails, and the manager releases `RLock`.
- If cancellation occurs during the final `Txn.Commit`, the transaction fails,
  the result is discarded, and the manager releases `RLock`.
- If `Txn.Commit` succeeds before a concurrent cancellation, the read has
  reached its linearization point. The manager can return success and update
  the cache.

The implementation does not check `ctx.Err()` after a successful final commit.
A cancellation that races after the linearization point does not rewrite a
completed operation as a failure.

### Leadership transitions

`OnNodeBecomesLeader` and `OnNodeBecomesFollower` continue to acquire
`m.mu.Lock()`, update the leadership counter, and clear the cache.

When a transition waits for an in-flight read:

1. The request context cancels the current etcd operation.
2. The read transaction returns an error.
3. The read path skips its cache update.
4. The deferred `RUnlock` runs.
5. The transition acquires the write lock and clears the cache.

If the read completes first and updates the cache, the later transition still
clears that entry. Both orderings preserve the current leadership invariant.

### All-keyspace partial progress

An all-keyspace request runs one revision-validated transaction per keyspace.
It can complete and cache earlier keyspaces before a later keyspace is
canceled.

The call returns a context error and does not expose the partial map as a
successful API response. It retains cache entries from completed keyspaces
because each entry passed revision validation. It does not cache the keyspace
whose transaction was canceled.

The implementation does not roll back prior cache warming. A rollback would
need to capture old cache state and could overwrite a concurrent valid update.

## Transaction consistency

The context-aware transaction preserves the existing revision protocol and
does not change its atomicity model.

The read transaction performs:

1. Read the GC state revision.
2. Read safe points and optional barriers.
3. Commit a raw transaction with a condition on the original revision.
4. Accept the result only when the condition succeeds.

Cancellation has the following effects:

- Cancellation during the initial or data reads skips final validation and
  discards the result.
- Cancellation during final validation discards the result.
- A changed revision returns `ErrEtcdTxnConflict`.
- A successful final transaction makes the read result eligible for return and
  cache update.

The combined read loads the keyspace GC state and every global GC barrier under
one revision validation. Any cancellation returns `GCState{}`, a nil global
barrier slice, and an error. It never returns one half of the combined result.

## Error handling

The implementation distinguishes request cancellation from the internal etcd
operation timeout.

### Storage error classification

`EtcdKVGetWithContext` keeps both the parent request context and its derived
operation context. When the etcd call fails, it applies these rules:

```go
if requestCtx.Err() != nil {
    return resp, requestCtx.Err()
}

return resp, errs.ErrEtcdKVGet.
    Wrap(err).
    GenWithStackByCause()
```

This returns an exact `context.Canceled` or
`context.DeadlineExceeded` when the request ends. If only the internal
operation timeout expires, the request context remains live and the helper
keeps the existing `ErrEtcdKVGet` classification.

The provider applies the same rule after `Txn.Commit`:

```go
result, err := txn.Commit()
if err != nil {
    if ctx.Err() != nil {
        return ctx.Err()
    }
    return errs.ErrEtcdTxnInternal.
        Wrap(err).
        GenWithStackByArgs()
}
```

A normal commit response with a failed revision condition remains
`ErrEtcdTxnConflict`. A cancellation that occurs after a completed conflict
does not overwrite the conflict result.

### Manager logging

The manager does not log expected request completion as a server error. It
skips `log.Error` when:

```go
errors.Is(err, context.Canceled) ||
    errors.Is(err, context.DeadlineExceeded)
```

It retains the current error log for:

- Revision conflicts.
- Internal etcd timeouts.
- Other etcd failures.
- Invalid stored data.
- Missing context-aware backend capabilities.

The implementation does not add a debug or info log for each cancellation.

### Etcd logging

`EtcdKVGetWithContext` does not emit the existing
`"load from etcd meet error"` error log for a request context error. It keeps
the log for storage failures.

The existing slow-request warning remains. A request that spends several
seconds in storage before its deadline still produces useful diagnostic
information, while an immediate client disconnect normally exits before the
slow threshold.

The design does not change `SlowLogTxn` metrics or slow warnings.

### gRPC behavior

Non-context errors continue to use the existing application response header:

```go
grpcutil.WrapErrorToHeader(
    pdpb.ErrorType_UNKNOWN,
    err.Error(),
)
```

Request context errors use standard gRPC transport errors:

```go
return nil, status.FromContextError(err).Err()
```

This maps:

- `context.Canceled` to `codes.Canceled`.
- `context.DeadlineExceeded` to `codes.DeadlineExceeded`.

The protobuf schema and successful response remain unchanged. The PD client
that canceled the request normally observes the same error from its own
context.

### HTTP behavior

The HTTP handlers do not attempt to write a 500 response after their request
context has ended.

The standard library handler applies:

```go
if err != nil {
    if r.Context().Err() != nil {
        return
    }
    // Keep the existing 500 response.
}
```

The Gin handler checks `c.Request.Context().Err()` in the same way.

When the request context remains live, existing storage and data errors keep
their current HTTP 500 response. The design does not introduce a nonstandard
499 status.

### Unsupported backend behavior

The context-aware transaction returns an explicit error before any storage
operation when the backend lacks `ContextReader` or
`RawTxnCapableWithContext`.

The error follows the existing endpoint pattern and does not require a new
protobuf error type or public error code. The manager logs it because it
indicates an invalid backend configuration or a programming error.

## Observability

The design uses existing request, client, and etcd instrumentation. It does not
add a metric or label.

The observable changes are:

- Canceled reads release the manager lock earlier.
- Expected cancellation no longer generates duplicate error logs.
- A transaction canceled during `Commit` counts as a failed transaction
  attempt in the existing metric.
- A transaction that previously completed after its caller left can now end as
  a canceled attempt.

These changes make the metrics reflect the actual etcd operation outcome.

## Testing strategy

The tests must prove that cancellation reaches the actual etcd gRPC operation.
An outer goroutine returning early is not sufficient.

### Deterministic etcd interceptor

Tests inject a client-side unary gRPC interceptor through
`clientv3.Config.DialOptions`. The existing embedded etcd test helper exposes a
`ClientCfgModifier` for this purpose.

The interceptor:

1. Matches `/etcdserverpb.KV/Range` or
   `/etcdserverpb.KV/Txn`.
2. Inspects the request and matches a specific GC state key or revision
   condition.
3. Signals that the RPC reached the blocking point.
4. Waits for either `ctx.Done()` or an explicit test release channel.
5. Returns `ctx.Err()` when cancellation arrives.

Tests enable interception only after cluster and keyspace bootstrap completes.
This prevents health checks and setup requests from consuming the hook.

Each interceptor includes a cleanup release path guarded by `sync.Once`. A
failed assertion therefore cannot leak the blocked goroutine.

### Prohibited timing pattern

Tests do not use the existing `SlowEtcdKVGet` failpoint as their cancellation
oracle. That failpoint sleeps before the etcd call and does not observe the
request context.

Tests also do not use a fixed sleep to establish ordering. They use channels to
signal that the exact RPC has entered the blocking point. A bounded
`time.After` or equivalent acts only as a failure watchdog.

### KV tests

The low-level tests cover both etcd operation types:

- `EtcdKVGetWithContext` returns `context.Canceled` after a blocked
  `Range` observes cancellation.
- An already-canceled context prevents the target request from reaching the
  interceptor.
- `CreateRawTxnWithContext` returns a context error after a blocked `Txn`
  observes cancellation.
- Existing `EtcdKVGet` and `CreateRawTxn` tests continue to pass.

### Provider tests

Provider tests cover cancellation at each transaction stage:

- The initial revision read.
- A callback safe-point or barrier read.
- The final revision validation transaction.

Each test verifies:

- The interceptor observes `ctx.Done()`.
- `errors.Is(err, context.Canceled)` succeeds.
- The callback does not run when the initial revision read is canceled.
- The final transaction does not run after a callback read is canceled.
- No write batch or revision update is committed.

A memory-backed endpoint verifies the capability error. The callback must not
run, and the provider must not fall back to contextless reads.

A live-context success case verifies that the new path retains revision
validation. Existing contextless transaction tests continue to cover the
legacy path.

### Manager tests

The normal read cancellation test uses this sequence:

1. Initialize the manager and data with embedded etcd.
2. Mark the manager as leader and ensure the target cache entry is absent.
3. Block a target safe-point `Range`.
4. Start `GetGCState` with a cancelable context.
5. Wait for the interceptor to signal entry.
6. Start a leader or follower transition that requires `m.mu.Lock()`.
7. Cancel the read context without releasing the interceptor manually.
8. Verify that the read returns `context.Canceled`.
9. Verify that the transition completes after `RUnlock`.
10. Verify that the cache has no target entry.

The combined read test blocks a global barrier read or final revision
transaction. It verifies a zero GC state, nil global barriers, no cache update,
and lock release.

An already-canceled context test verifies that the manager does not issue a
storage request.

### All-keyspace tests

The all-keyspace tests cover two positions:

- The null keyspace read.
- A later keyspace-level GC state read after an earlier keyspace completes.

Canceling the sole waiter must cancel the `OrderedSingleFlight` execution
context, terminate the target RPC, and stop the scan. The current keyspace must
not enter the cache. Valid entries from earlier completed keyspaces can remain.

The existing `OrderedSingleFlight` tests continue to verify partial waiter
cancellation and all-waiter cancellation. The manager tests verify that the GC
scan passes the context from the singleflight body into storage. Code review
must confirm that the body passes its `execCtx` parameter rather than capturing
an individual caller context.

### API regression tests

API tests verify:

- Context cancellation maps to `codes.Canceled`.
- Context deadline expiration maps to `codes.DeadlineExceeded`.
- A non-context storage error retains the current response-header behavior.
- Existing v1 and v2 HTTP safe-point success and error tests pass.
- A canceled HTTP request does not receive a newly written 500 body.

The tests can use a package-private context error classification helper when
constructing a complete `GrpcServer` only for status mapping would add
unrelated setup.

### Verification commands

Run the narrow storage and manager tests first:

```bash
make gotest \
  GOTEST_ARGS='./pkg/utils/etcdutil ./pkg/storage/kv -count=1'
make gotest \
  GOTEST_ARGS='./pkg/storage/endpoint ./pkg/gc -count=1'
```

Run the HTTP API regression tests:

```bash
make gotest \
  GOTEST_ARGS='./tests/server/api -count=1'
make gotest \
  GOTEST_ARGS='./tests/server/apiv2/handlers -count=1'
```

Run repository checks after the focused tests pass:

```bash
make check
make basic-test
```

The make targets manage failpoint enablement and cleanup. The implementation
must not leave generated failpoint files in the worktree.

## Compatibility

The design preserves external and legacy interfaces while changing internal
manager call sites.

The compatibility boundaries are:

- TiDB, TiCDC, TiKV client-go, and other ecosystem repositories continue to
  use `github.com/tikv/pd/client/clients/gc`.
- The public client APIs already accept `context.Context`.
- The protobuf service definitions do not change.
- The HTTP routes and payloads do not change.
- The contextless `kv.Base`, `RawTxnCapable`, `EtcdKVGet`, and
  `RunInGCStateTransaction` interfaces remain available.
- The new manager signatures affect PD server code and tests only.

The target server packages are exported Go packages rather than Go
`internal` packages. Keeping the low-level contextless entry points protects
unknown private or un-cloned consumers at low cost.

## Performance

The design adds no meaningful hot-path overhead.

Cache hits return before the storage capability checks. A cache miss or a
barrier-inclusive request performs a small interface capability check before
several etcd network operations. The type assertion does not allocate and is
negligible compared with network and etcd processing time.

The context-aware helper creates the same per-operation timeout context that
the current helper creates. It changes the parent context from the long-lived
client context to the request context and does not add another timer.

The design does not create a goroutine per storage operation.

## Alternatives considered

The selected design limits the change to the GC read path while providing true
storage cancellation. Broader alternatives have higher risk or do not solve
the lock problem.

### Make all KV methods context-first

Changing `kv.Base` to require context for all reads and writes would produce a
uniform API. It would also touch every storage backend, endpoint, mock, and
unrelated storage caller.

This is a reasonable separate storage-layer refactor, but its review and
regression surface is too large for this issue.

### Use one raw etcd read transaction

Building every safe-point and barrier read into one raw transaction could
reduce round trips and provide one cancellable commit.

The current provider callback performs dynamic reads, including range reads,
and builds a write batch under a revision protocol. Replacing that model would
require a new result decoder and a new proof of transaction and compatibility
semantics. This design does not combine that refactor with a lifecycle fix.

### Return early from an outer goroutine

Wrapping the current contextless operation in a goroutine and selecting on
`ctx.Done()` would let the handler return early.

The goroutine would continue the etcd request while holding
`GCStateManager.mu.RLock()`. It would preserve the original lock problem and
add a goroutine lifecycle problem, so the design rejects this alternative.

### Release the manager lock during I/O

Releasing `RLock` before etcd I/O would prevent a slow read from blocking a
writer or leadership transition.

Without a cache generation or leadership epoch, the read could repopulate the
cache after a transition clears it. Solving that race requires a broader
concurrency redesign and remains outside this issue.

## Acceptance criteria

The implementation is complete only when all of these conditions hold:

- A gRPC or HTTP request context reaches every GC state `Range` and final
  `Txn.Commit`.
- Cancellation does not wait for the internal 10-second etcd timeout.
- A canceled in-flight read releases `GCStateManager.mu.RLock()`.
- A waiting leadership transition can proceed after cancellation.
- A canceled single read does not update the cache.
- A canceled combined read returns no partial state or barriers.
- `GetAllKeyspacesGCStates` uses the singleflight execution context.
- Valid cache entries from completed keyspace reads remain valid after a later
  cancellation.
- The context-aware transaction rejects an unsupported backend without a
  fallback or partial read.
- Request context errors retain their standard identity and gRPC code.
- Internal etcd timeouts retain the existing storage error classification.
- Expected request cancellation does not produce duplicate error logs.
- Existing revision conflict, cache, transaction, and HTTP regression tests
  pass.
- Public protobuf, client, HTTP, and JSON contracts remain unchanged.
- Legacy contextless storage and provider APIs retain their behavior.

## Next steps

After written approval of this specification, create an implementation plan
that orders the work from low-level etcd helpers through provider, manager,
handlers, and deterministic cancellation tests. Do not start implementation
before that plan is reviewed.
