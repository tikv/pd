# WatchGCStates server design

`WatchGCStates` provides an ordered stream of complete, effective GC states for keyspaces. This design adds the PD server implementation for the API introduced by [kvproto PR #1528](https://github.com/pingcap/kvproto/pull/1528), while keeping the watch mechanism isolated from the gRPC transport and from the legacy `WatchGCSafePointV2` API.

## Context

The original draft in [PD PR #10498](https://github.com/tikv/pd/pull/10498) established the motivation for a streaming API, but its implementation coupled initial loading and live delivery in ways that could reorder states, allowed a slow client to affect unrelated watchers, and included compatibility work that is no longer required. This design retains the useful product semantics from the original [WatchGCStates proposal](https://pingcap.feishu.cn/wiki/TNJSw3rWGiCwjJk8iOtcyTDrnSe) and incorporates the concerns from [review 5097472165](https://github.com/tikv/pd/pull/10498#pullrequestreview-5097472165).

The merged protobuf API represents every notification as a `GCStateChange` containing either a complete `upsert` state or a `removed` keyspace scope. Consumers apply changes in stream order to maintain a materialized view.

## Goals

The implementation has a deliberately narrow set of goals:

- Stream the initial effective GC state of every active keyspace when requested.
- Stream effective state changes that occur after a watcher is registered.
- Guarantee that a watcher never observes an older state for a keyspace after a newer live state for that keyspace.
- Prevent a slow watcher from blocking GC advancement or affecting another watcher.
- Terminate watchers promptly and predictably when PD loses leadership.
- Keep the watcher implementation independently testable in `pkg/gc`.
- Bound response sizes using the actual protobuf wire size.

## Non-goals

The first implementation intentionally excludes adjacent features that are not needed to deliver the API safely:

- Compatibility with `WatchGCSafePointV2`.
- A Go PD client implementation.
- Streaming GC barriers or global barriers.
- A globally atomic snapshot across keyspaces.
- A revision, cursor, replay log, or resume-from-revision protocol.
- Sharing one initial scan among multiple watchers.
- Producing `removed` events from keyspace lifecycle changes.

## Stream contract

The stream is a sequence of self-contained changes. An `upsert` replaces the consumer's entire state for its scope, and a `removed` change deletes that scope from the consumer's materialized view. Upserts do not contain GC barriers; barrier-only mutations do not directly produce changes.

For `skip_loading_initial=false`, PD registers the live listener before starting the initial scan. Initial and live changes may be interleaved, and the initial scan is not a cross-keyspace transaction. For each individual keyspace, however, the server suppresses an initial value if a post-registration live value for the same scope has already been emitted. The stream therefore cannot regress from a live value `v2` to an older initial value `v1`.

For `skip_loading_initial=true`, PD sends only effective changes produced after registration. This mode does not provide continuity with an earlier stream and is unsuitable for constructing a complete view on its own. A client establishing its first complete view or recovering from a disconnected stream uses `skip_loading_initial=false`.

Clients should clear their materialized GC-state view before an initial connection or reconnection with `skip_loading_initial=false`, unless they independently reconcile stale entries. This is a recommended client convention rather than a server-enforced requirement. The first server implementation does not yet produce lifecycle-driven `removed` events, so a client that retains an old view cannot otherwise guarantee removal of scopes deleted while it was disconnected.

The protocol does not expose an initial-scan completion marker. Consumers continuously apply changes in arrival order; correctness does not depend on distinguishing initial changes from live changes.

## Architecture

The design separates state observation, ordered merging, and transport adaptation into three layers. The deeper watcher module owns concurrency and lifecycle policy so the gRPC handler only deals with request validation, protobuf conversion, and response delivery.

### GC state manager

`GCStateManager` owns a registry of active watchers. Registration, live publication, watcher removal caused by a full live queue, and the leader-to-follower transition are serialized by the manager's existing mutex. This makes watcher registration linearizable with GC-state mutations and leadership changes.

The `pkg/gc` layer defines an internal `GCStateChange` representation with `upsert` and `removed` variants. The type is independent of protobuf. An upsert contains a complete effective GC state, including the scope, whether GC is managed at keyspace level, the transaction safe point, and the GC safe point. A removed change contains the affected scope.

The initial implementation produces upserts from successful GC-state mutations. It supports removed changes throughout the watcher and transport pipeline so keyspace lifecycle integration can be added without redesigning the stream. The implementation leaves an explicit TODO at the keyspace lifecycle integration point rather than adding an incomplete lifecycle dependency in this change.

### GC state watcher

Watcher mechanics live in a focused file such as `pkg/gc/gc_state_watcher.go`. Each watcher owns the following state:

- `initCh`, a buffered channel of initial-state batches.
- `liveCh`, a bounded channel of individual live changes.
- `initDone`, which records whether initial loading has completed or was skipped.
- A cause-aware cancellation mechanism used for both cleanup and error reporting.
- Merge state, including the set of scopes made dirty by live delivery while initial loading is active.

Only the initial loader writes to and closes `initCh`. Publishers write to `liveCh` only while holding the manager mutex, but `liveCh` is not closed; watcher cancellation is the termination signal. This ownership rule avoids send-versus-close races.

The watcher exposes a receive operation that returns at most a requested number of visible changes. `RecvBatch(1024)` blocks until at least one change or a terminal cause is available, then opportunistically collects already available changes without waiting to fill the batch. The watcher, rather than the gRPC handler, owns the initial/live merge and its ordering invariant.

### GC service

`server/gc_service.go` remains a thin adapter. Its public `WatchGCStates` method performs the rate-limit check directly, validates the request, registers a watcher, converts internal changes to protobuf, splits changes into wire-size-bounded responses, sends them, and closes the watcher on every return path.

Keeping `rateLimitCheck` in the public handler preserves the externally visible method name `WatchGCStates` in the caller-derived rate-limit label. The rate-limit token is held for the lifetime of the stream and released when the handler returns.

## Registration and initial loading

Registration establishes the boundary between pre-existing state and live changes. The sequence is:

1. Lock `GCStateManager.mu`.
2. Verify that this PD member is the GC-state leader.
3. Create the watcher and add it to the registry.
4. Unlock `GCStateManager.mu`.
5. If `skip_loading_initial=false`, start the initial loader. Otherwise, mark initial loading complete immediately.

Registering before scanning ensures that every effective mutation after the registration point is either queued as live data or causes that watcher to terminate as a slow consumer. No mutation can fall into a gap between snapshot setup and live subscription.

The initial loader reuses the manager's existing all-keyspace iteration behavior and requests states without barriers. It preserves the current handling of inactive keyspaces and unified GC mode. It does not hold `GCStateManager.mu` while reading storage, constructing batches, or waiting for `initCh` capacity.

Initial states are accumulated into batches of at most 1024 changes and sent through `initCh`. The loader closes `initCh` after a successful scan. If iteration fails, it records an initialization error as the watcher cancellation cause; a consumer may therefore have received a partial initial view before the stream terminates.

Cancellation of the RPC or removal of the watcher cancels the loader as well. Storage iteration and channel sends observe the watcher context so a disconnected client cannot leave an initial-scan goroutine behind.

## Live publication

Live changes are published only after a mutation has committed successfully and the manager cache reflects the resulting effective state. Publication occurs before releasing `GCStateManager.mu`, preserving the same order for all watchers and serializing it with follower transition and registration.

`AdvanceGC` and `AdvanceTxnSafePoint` publish a complete upsert only when the effective state changes. Rejected, no-op, or failed mutations do not publish. Setting or deleting a barrier does not itself publish because barriers are excluded from the API; if a later advancement changes the effective safe point, that successful advancement publishes the new complete state.

Publication to each `liveCh` is non-blocking. If a watcher's channel is full, the manager removes and cancels only that watcher with the slow-consumer cause and continues publishing to other watchers. There is no timeout, retry, or channel wait while holding the manager mutex.

## Initial/live ordering

A single consumer merges `initCh` and `liveCh`. While initial loading is active, it maintains `dirtyDuringInit`, a set keyed by keyspace scope:

- When the consumer emits a live change, it marks that scope dirty.
- When it encounters an initial upsert whose scope is already dirty, it drops the initial upsert.
- When initial loading finishes, it releases the dirty set because every later change is live and already ordered by `liveCh`.

There are two possible observations for an initial value `v1` and a later live value `v2`. If the consumer receives `v1` first, it emits `v1` followed by `v2`. If it receives `v2` first, it marks the scope dirty and suppresses `v1`. In neither case can it emit `v2` followed by `v1`.

The same rule supports the future `removed` producer: a live removal marks the scope dirty, preventing an older initial upsert from recreating it. Live changes retain FIFO order because mutation publication is serialized by the manager mutex and each watcher has a single live channel and a single consumer.

The implementation must explain this timing guarantee next to the merge logic, including the registration boundary and both possible delivery orders. A deterministic test pauses initial loading after reading `v1` but before placing it on `initCh`, advances the same keyspace to `v2`, observes `v2`, resumes initial loading, and verifies that `v1` is never emitted afterward.

## Capacity and backpressure

The default capacities balance burst tolerance against per-watcher memory consumption and are implementation constants so tests can exercise smaller limits deterministically.

`liveCh` holds 1024 individual changes. A 300,000-keyspace deployment in which every keyspace changes during a ten-minute interval produces roughly 500 changes per second; allowing for both transaction and GC safe-point changes gives an order-of-magnitude estimate of 1,000 changes per second. A capacity of 1024 therefore absorbs approximately one second of scheduling or network jitter at that scale without pretending to support an instantaneous 300,000-keyspace burst. Sustained delivery slower than production intentionally terminates and reconnects the affected watcher.

Initial batches contain at most 1024 changes, and `initCh` has capacity 1. This permits the RPC consumer to process the current batch while one completed batch waits and the loader constructs the next batch. Increasing the channel capacity would mainly increase per-watcher read-ahead and memory use because initial loading is allowed to backpressure storage iteration.

The response wire-size limit and the internal change-count limit solve different problems. `RecvBatch(1024)` bounds merge work and internal allocations; the gRPC adapter may split that result into multiple responses to enforce the protobuf size limit.

## Lifecycle and errors

Every watcher has exactly one terminal cause. Removal from the manager registry and cancellation are idempotent so concurrent RPC cleanup, initialization failure, leadership loss, and slow-consumer detection cannot leak or double-close resources.

The lifecycle cases are:

| Event | Manager behavior | Stream result |
| --- | --- | --- |
| Caller cancellation or send failure | Remove the watcher and cancel its loader | Return the caller or send error; record `client_cancel` |
| Leader becomes follower | Remove and cancel all registered watchers while holding the manager mutex | Return the domain not-leader error, mapped to gRPC `Unavailable` |
| Initial scan fails | Remove and cancel the watcher with the initialization cause | End after any already-sent partial initial data; map to gRPC `Unavailable` |
| `liveCh` is full | Remove and cancel only that watcher | Return the slow-consumer error, mapped to gRPC `ResourceExhausted` |
| Initial scan completes | Close `initCh` and release merge-only initial state | Continue streaming live changes |

The `pkg/gc` layer returns domain errors and does not depend on gRPC status codes. The service maps not-leader and storage/initialization failures to `Unavailable`, and a slow consumer to `ResourceExhausted`. Existing request-validation and rate-limit paths retain their current status semantics.

The receive path checks the terminal cause before returning buffered work after cancellation. This prevents an already-cancelled watcher from deliberately draining stale queued data, although an RPC send already in progress when leadership changes cannot be recalled. Clients treat any terminated stream as requiring reconnection and reinitialization.

## Protobuf conversion and response batching

The gRPC layer uses a dedicated converter from the internal change type to `pdpb.GCStateChange`. An upsert populates the complete effective state and never populates barrier fields. A removed change populates its `KeyspaceScope`. Unsupported or structurally invalid internal variants fail explicitly rather than producing an empty protobuf change.

Each `WatchGCStatesResponse` is limited to 1 MiB under normal operation. Batching accounts for the actual protobuf wire representation, including the repeated-field tag and the length-delimited message envelope.

The adapter starts with the serialized size of the response's non-change fields. For each change, it computes the serialized size of a headerless response containing only that one change; this is the exact additive delta for the repeated embedded-message field. If adding the delta would exceed the limit and the current response is non-empty, the adapter sends the current response first. It never sends an empty response.

If one change by itself exceeds 1 MiB, the adapter logs the anomaly and sends that single change. Dropping it would silently corrupt the consumer's materialized view, while repeatedly rejecting it would make progress impossible. The protobuf schema makes this case unexpected, but the behavior remains defined.

This approach avoids manually reproducing protobuf varint rules and avoids repeatedly serializing a growing candidate response, which would make batch construction quadratic.

## Leadership behavior

The cluster lifecycle continues to call `GCStateManager.OnNodeBecomesLeader` and `OnNodeBecomesFollower` synchronously. No independent service-level leadership callback is introduced.

Registration fails if the member is not the current GC-state leader. On transition to follower, the manager cancels every watcher under the same mutex used for registration and publication. A client reconnects to the newly advertised leader with `skip_loading_initial=false` and rebuilds its view according to the stream contract.

## Removed-event integration

The internal model, ordering logic, protobuf converter, and tests all accept `removed` changes, but the first PD implementation has no authoritative keyspace lifecycle hook that produces them. Adding a partial producer would create misleading convergence guarantees, so production is deferred.

A future lifecycle integration must publish removal and recreation events through the same manager-serialized live path as safe-point changes. It must also define how lifecycle ownership interacts with GC leadership. The implementation records this location with a targeted TODO so the limitation is discoverable without expanding the present scope.

## Observability

Metrics are intentionally low-cardinality and focus on operational decisions rather than per-message detail:

- A gauge reports the number of active GC-state watchers.
- A counter reports watcher terminations with the bounded reason label values `client_cancel`, `leader_lost`, `slow_consumer`, and `init_error`.
- A slow-consumer log records the watcher identifier, configured live capacity, and observed queue length.

Watcher identifiers, keyspace identifiers, client addresses, and error strings are not metric labels. The implementation does not add a per-send queue-length histogram because it would instrument the hot path without a demonstrated operational need.

## Test strategy

The watcher and service layers are tested separately, with focused integration coverage for leadership transitions. Tests use controllable capacities or failpoints instead of timing-dependent sleeps.

### `pkg/gc` tests

The domain tests cover:

- Registration succeeds only on the leader, and leadership loss terminates all current watchers.
- `skip_loading_initial=false` returns initial and subsequent live states; `true` returns only post-registration live changes.
- Successful effective state changes publish complete upserts, while no-op and failed mutations do not.
- Barrier-only mutations produce no change, while a later effective safe-point change does.
- An initial-first sequence emits `v1` followed by `v2`.
- A deterministic paused-initial sequence emits `v2` and suppresses the later initial `v1`.
- Filling watcher A's `liveCh` terminates A without delaying watcher B; A can reconnect and rebuild.
- Initial iteration failure, caller cancellation, and concurrent deregistration terminate without goroutine or registry leaks.
- A full `initCh` backpressures only that watcher's initial iterator.
- Upsert and removed changes use the same per-scope dirty ordering rule.

### Server tests

The transport tests cover:

- Conversion of complete upsert and removed variants, including an empty barrier list.
- Response splitting at a reduced test limit, with assertions based on the serialized protobuf size on both sides of the boundary.
- A rate-limit capacity of one: the first active stream holds the token, the second is rejected, and a third succeeds after the first closes.
- Actual leader transfer terminates the old stream and permits a fresh initial stream on the new leader.
- Client cancellation and send failure remove the watcher and release the rate-limit token.
- Domain error causes map to the specified gRPC status codes.

## Dependency and rollout

The root, client, tools, and tests Go modules are updated to a kvproto revision containing the merged `WatchGCStates` API from PR #1528. No compatibility wrapper or implementation is added for `WatchGCSafePointV2`.

The API can be rolled out server-first because existing clients do not call the new RPC. New consumers use an initial stream to construct their materialized view and use the same path after any disconnect. The absence of lifecycle-produced removals remains an explicit limitation until the future integration is implemented.

## Acceptance criteria

The implementation is complete when all of the following are true:

- `WatchGCStates` serves initial and live effective state changes with the documented `skip_loading_initial` behavior.
- The deterministic ordering test proves that no older initial value follows a newer live value for the same scope.
- A full live queue terminates only the affected watcher without blocking GC-state mutation.
- Leadership loss terminates all active streams and reconnecting with initial loading rebuilds the view.
- Response batches observe the 1 MiB target using exact protobuf size accounting, except for the defined oversized-single-change case.
- Metrics and logs use only the bounded dimensions described above.
- Targeted package and server tests pass with no failpoints left enabled.
- The implementation contains no `WatchGCSafePointV2` compatibility path and no Go client work.

## Next step

After this design is reviewed, the implementation work is decomposed into a test-first plan covering the dependency update, watcher domain model, mutation publication, gRPC adaptation, observability, and focused verification.
