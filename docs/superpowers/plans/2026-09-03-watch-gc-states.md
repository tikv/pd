# WatchGCStates server implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the PD `WatchGCStates` server stream with ordered initial and live delivery, isolated backpressure, local leadership lifecycle handling, exact protobuf response sizing, and bounded observability.

**Architecture:** `pkg/gc` owns the watcher registry, initial-state iteration, live publication, per-keyspace merge ordering, cancellation causes, and lifecycle metrics. `server/gc_service.go` validates the local streaming request, converts domain changes to protobuf, splits responses by exact wire size, and sends them. Focused package tests prove deterministic concurrency behavior, while `tests/server/gc` covers real gRPC and leader-transfer behavior.

**Tech stack:** Go 1.25, gRPC server streaming, gogo/protobuf, PingCAP failpoints, Prometheus client_golang, testify, and the existing PD test cluster.

**Spec:** [`../specs/2026-09-03-watch-gc-states-design.md`](../specs/2026-09-03-watch-gc-states-design.md)

## Global constraints

Every task inherits these requirements from the approved design and repository rules:

- Do not add `WatchGCSafePointV2` compatibility or a Go PD client API.
- Emit complete effective GC-state upserts only after a successful safe-point mutation updates the manager cache; do not emit barrier-only or no-op changes.
- Keep `removed` in the internal model and transport converter, but do not add keyspace lifecycle producers in this change.
- Register live delivery before initial scanning and suppress an initial state after a live change for the same scope has been emitted.
- Use `liveCh` capacity 1024, `initCh` capacity 1, initial batches of 1024 changes, and `RecvBatch(1024)` in production; expose capacities only through unexported test seams.
- Never block on watcher channels while holding `GCStateManager.mu`; evict only the watcher whose `liveCh` is full.
- Keep response wire size at or below 1 MiB except when one change alone exceeds the target, in which case send that change alone.
- Hold the `WatchGCStates` rate-limit token for the complete stream lifetime.
- Keep Prometheus labels bounded and pre-bind all `WithLabelValues` handles outside hot paths.
- Use `make gotest` for tests that rely on failpoints, and verify that failpoints are disabled before committing.
- Do not hard-wrap Markdown prose.

## File map

The implementation uses focused files and avoids unrelated refactoring:

- Create `pkg/gc/gc_state_watcher.go` for domain changes, watcher merge state, registration helpers, initial loading, cancellation, and live fan-out.
- Create `pkg/gc/gc_state_watcher_test.go` for deterministic watcher, leadership, initial-loading, backpressure, publication, and metric tests.
- Modify `pkg/gc/gc_state_manager.go` only where manager fields, leadership callbacks, safe-point mutation hooks, and the existing iterator are involved.
- Modify `pkg/gc/gc_state_manager_test.go` only to adapt existing leadership setup and reuse its embedded-etcd GC fixtures.
- Modify `pkg/gc/metrics.go` for the active watcher gauge and bounded termination counters.
- Modify `pkg/errs/errno.go` and `errors.toml` for the slow-consumer sentinel.
- Modify `server/cluster/cluster.go` so each leader startup stores its generation-aware teardown closure.
- Modify `server/gc_service.go` for protobuf conversion, exact response splitting, local request preflight, domain-error mapping, and streaming.
- Create `server/gc_service_test.go` for converter, batching, cancellation-before-send, and error-mapping unit tests.
- Modify `tests/server/gc/gc_test.go` for real RPC, rate-limit, validation, and leader-transfer coverage.
- Modify the root, `client`, `tools`, and `tests/integrations` `go.mod` and `go.sum` pairs to use kvproto commit `65b4e27a438de9274bf88c58e89e83749e62f646`.

---

### Task 1: Add the watcher change model and merge state

This task creates the transport-independent state machine that merges initial batches and individual live changes without regressing a keyspace.

**Files:**

- Create: `pkg/gc/gc_state_watcher.go`
- Create: `pkg/gc/gc_state_watcher_test.go`

**Interfaces:**

- Consumes: Existing `GCState` from `pkg/gc/gc_state_manager.go`.
- Produces: `GCStateChange`, `NewGCStateUpsert`, `NewGCStateRemoved`, `GCStateChange.Upsert`, `GCStateChange.RemovedKeyspaceID`, `GCStateChange.KeyspaceID`, `GCStateWatcher.RecvBatch`, and `GCStateWatcher.Err`.
- Produces for Task 2: `newGCStateWatcher`, `gcStateWatchConfig`, `GCStateWatcher.initCh`, `GCStateWatcher.liveCh`, and `GCStateWatcher.cancel`.

- [ ] **Step 1: Write failing tests for both observable delivery orders**

Create tests that drive the channels directly so selection is deterministic: send initial `v1` and receive it before sending live `v2` for the initial-first case; receive live `v2` first, then send initial `v1` for the live-first case.

```go
func TestGCStateWatcherInitialThenLive(t *testing.T) {
	w := newGCStateWatcher(context.Background(), gcStateWatchConfig{initChannelCapacity: 1, liveChannelCapacity: 2}, false)
	w.initCh <- []GCStateChange{NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 1})}

	got, err := w.RecvBatch(1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), mustUpsert(t, got[0]).TxnSafePoint)

	w.liveCh <- NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 2})
	got, err = w.RecvBatch(1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), mustUpsert(t, got[0]).TxnSafePoint)
}

func TestGCStateWatcherLiveSuppressesOlderInitial(t *testing.T) {
	w := newGCStateWatcher(context.Background(), gcStateWatchConfig{initChannelCapacity: 1, liveChannelCapacity: 2}, false)
	w.liveCh <- NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 2})

	got, err := w.RecvBatch(1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), mustUpsert(t, got[0]).TxnSafePoint)

	w.initCh <- []GCStateChange{NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 1})}
	close(w.initCh)
	_, ok, err := w.receiveOne(false)
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, w.initDone)
}
```

- [ ] **Step 2: Write failing tests for removed changes, closed-channel draining, batch bounds, and cancellation priority**

Use the same direct-channel seam and add these exact cases:

```go
func TestGCStateWatcherRemovedSuppressesInitial(t *testing.T) {
	w := newGCStateWatcher(context.Background(), gcStateWatchConfig{initChannelCapacity: 1, liveChannelCapacity: 2}, false)
	w.liveCh <- NewGCStateRemoved(7)
	got, err := w.RecvBatch(1)
	require.NoError(t, err)
	removed, ok := got[0].RemovedKeyspaceID()
	require.True(t, ok)
	require.Equal(t, uint32(7), removed)

	w.initCh <- []GCStateChange{NewGCStateUpsert(GCState{KeyspaceID: 7})}
	close(w.initCh)
	_, ok, err = w.receiveOne(false)
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, w.initDone)
}

func TestGCStateWatcherDrainsBufferedInitBeforeReleasingDirtySet(t *testing.T) {
	w := newGCStateWatcher(context.Background(), gcStateWatchConfig{initChannelCapacity: 1, liveChannelCapacity: 1}, false)
	w.liveCh <- NewGCStateUpsert(GCState{KeyspaceID: 7})
	_, err := w.RecvBatch(1)
	require.NoError(t, err)
	w.initCh <- []GCStateChange{NewGCStateUpsert(GCState{KeyspaceID: 8})}
	close(w.initCh)

	got, err := w.RecvBatch(1)
	require.NoError(t, err)
	require.Equal(t, uint32(8), mustUpsert(t, got[0]).KeyspaceID)
	require.False(t, w.initDone)
	require.NotNil(t, w.dirtyDuringInit)

	_, ok, err := w.receiveOne(false)
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, w.initDone)
	require.Nil(t, w.initCh)
	require.Nil(t, w.dirtyDuringInit)
}

func TestGCStateWatcherRecvBatchHonorsMaximum(t *testing.T) {
	w := newGCStateWatcher(context.Background(), gcStateWatchConfig{liveChannelCapacity: 3}, true)
	for id := uint32(1); id <= 3; id++ {
		w.liveCh <- NewGCStateUpsert(GCState{KeyspaceID: id})
	}
	got, err := w.RecvBatch(2)
	require.NoError(t, err)
	require.Len(t, got, 2)
	got, err = w.RecvBatch(2)
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestGCStateWatcherCancellationDiscardsBufferedWork(t *testing.T) {
	w := newGCStateWatcher(context.Background(), gcStateWatchConfig{liveChannelCapacity: 1}, true)
	w.liveCh <- NewGCStateUpsert(GCState{KeyspaceID: 7})
	want := errors.New("watch terminated")
	w.cancel(want)
	got, err := w.RecvBatch(1)
	require.ErrorIs(t, err, want)
	require.Nil(t, got)
}

func TestGCStateWatcherFirstCancellationCauseWins(t *testing.T) {
	w := newGCStateWatcher(context.Background(), gcStateWatchConfig{liveChannelCapacity: 1}, true)
	first := errors.New("first")
	w.cancel(first)
	w.cancel(errors.New("second"))
	require.ErrorIs(t, w.Err(), first)
}
```

Add `mustUpsert` as a test helper that calls `change.Upsert()`, requires the boolean result, and returns the state.

- [ ] **Step 3: Run the focused tests and confirm the red state**

Run:

```bash
make gotest GOTEST_ARGS='./pkg/gc -run ^TestGCStateWatcher -count=1'
```

Expected: compilation fails because the watcher types and constructors do not exist.

- [ ] **Step 4: Implement the domain change type and test helper accessors**

Use an unexported discriminator so the zero value remains structurally invalid for Task 5 converter tests.

```go
type gcStateChangeKind uint8

const (
	gcStateChangeUnknown gcStateChangeKind = iota
	gcStateChangeUpsert
	gcStateChangeRemoved
)

type GCStateChange struct {
	kind              gcStateChangeKind
	upsert            GCState
	removedKeyspaceID uint32
}

func NewGCStateUpsert(state GCState) GCStateChange {
	state.GCBarriers = nil
	return GCStateChange{kind: gcStateChangeUpsert, upsert: state}
}

func NewGCStateRemoved(keyspaceID uint32) GCStateChange {
	return GCStateChange{kind: gcStateChangeRemoved, removedKeyspaceID: keyspaceID}
}

func (c GCStateChange) Upsert() (GCState, bool) {
	return c.upsert, c.kind == gcStateChangeUpsert
}

func (c GCStateChange) RemovedKeyspaceID() (uint32, bool) {
	return c.removedKeyspaceID, c.kind == gcStateChangeRemoved
}

func (c GCStateChange) KeyspaceID() (uint32, bool) {
	if state, ok := c.Upsert(); ok {
		return state.KeyspaceID, true
	}
	return c.RemovedKeyspaceID()
}
```

Add GoDoc to every exported type and function.

- [ ] **Step 5: Implement the single-consumer merge**

Define the production defaults and the unexported configuration seam:

```go
const (
	defaultGCStateWatchInitialBatchSize      = 1024
	defaultGCStateWatchInitChannelCapacity   = 1
	defaultGCStateWatchLiveChannelCapacity   = 1024
)

type gcStateWatchConfig struct {
	initialBatchSize    int
	initChannelCapacity int
	liveChannelCapacity int
}

type GCStateWatcher struct {
	ctx             context.Context
	cancel          context.CancelCauseFunc
	initCh          chan []GCStateChange
	liveCh          chan GCStateChange
	initDone        bool
	pendingInit     []GCStateChange
	dirtyDuringInit map[uint32]struct{}
}

func newGCStateWatcher(parent context.Context, cfg gcStateWatchConfig, skipLoadingInitial bool) *GCStateWatcher {
	ctx, cancel := context.WithCancelCause(parent)
	watcher := &GCStateWatcher{
		ctx:      ctx,
		cancel:   cancel,
		initCh:   make(chan []GCStateChange, cfg.initChannelCapacity),
		liveCh:   make(chan GCStateChange, cfg.liveChannelCapacity),
		initDone: skipLoadingInitial,
	}
	if !skipLoadingInitial {
		watcher.dirtyDuringInit = make(map[uint32]struct{})
	}
	return watcher
}
```

Implement `receiveOne(block bool)` as the only place that selects from the channels. It performs these branches in order:

1. Return `context.Cause(w.ctx)` before inspecting buffered work.
2. Consume `pendingInit` first, dropping an initial change when its keyspace is in `dirtyDuringInit`.
3. If initial loading is complete, select only `ctx.Done()` and `liveCh`.
4. Otherwise, select `ctx.Done()`, `liveCh`, and `initCh`. Mark every emitted live scope dirty. Copy a received initial batch into `pendingInit`. When the closed `initCh` is observed after buffered batches are drained, set `initCh=nil`, `initDone=true`, and `dirtyDuringInit=nil`.
5. For opportunistic collection, add a `default` branch when `block=false`.

Implement `RecvBatch` by blocking for its first visible change, calling `receiveOne(false)` until `maxChanges` is reached or no visible work is ready, and checking `Err()` again before returning the batch. Panic on a non-positive `maxChanges`, because this is an internal programmer error and production always passes 1024.

```go
func (w *GCStateWatcher) Err() error {
	return context.Cause(w.ctx)
}

func (w *GCStateWatcher) RecvBatch(maxChanges int) ([]GCStateChange, error) {
	if maxChanges <= 0 {
		panic("GCStateWatcher.RecvBatch requires a positive maximum")
	}
	first, ok, err := w.receiveOne(true)
	if err != nil {
		return nil, err
	}
	if !ok {
		panic("blocking watcher receive returned no result")
	}
	result := []GCStateChange{first}
	for len(result) < maxChanges {
		change, ok, err := w.receiveOne(false)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		result = append(result, change)
	}
	if err := w.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
```

Place a comment beside the dirty-set branches explaining the two valid orders, `v1 -> v2` and `v2` with `v1` suppressed, as required by the spec.

- [ ] **Step 6: Run the watcher merge tests and confirm the green state**

Run:

```bash
make gotest GOTEST_ARGS='./pkg/gc -run ^TestGCStateWatcher -count=1'
```

Expected: all watcher merge tests pass, including cancellation under `-count=1`.

- [ ] **Step 7: Format and commit the watcher state machine**

Run:

```bash
gofmt -w pkg/gc/gc_state_watcher.go pkg/gc/gc_state_watcher_test.go
git diff --check
git add pkg/gc/gc_state_watcher.go pkg/gc/gc_state_watcher_test.go
git commit -s -m "gc: add GC state watcher merge model"
```

### Task 2: Register watchers and bind them to local leadership

This task connects the watcher state machine to `GCStateManager`, starts incremental initial loading, and replaces the counter-style follower callback with a generation-aware teardown closure.

**Files:**

- Modify: `pkg/gc/gc_state_watcher.go`
- Modify: `pkg/gc/gc_state_watcher_test.go`
- Modify: `pkg/gc/gc_state_manager.go:188-266`
- Modify: `pkg/gc/gc_state_manager_test.go:116-262`
- Modify: `server/cluster/cluster.go:495-496`

**Interfaces:**

- Consumes: Task 1's watcher channels, `RecvBatch`, and configuration seam; existing `iterateAllKeyspacesGCStates`.
- Produces: `GCStateManager.WatchGCStates(ctx context.Context, skipLoadingInitial bool) (*GCStateWatcher, error)`, `GCStateWatcher.Close()`, and `GCStateManager.OnNodeBecomesLeader() func()`.
- Produces for Tasks 3 and 4: `terminateGCStateWatcherLocked`, the watcher registry, watcher IDs, and bounded termination-reason constants.

- [ ] **Step 1: Write failing tests for registration and generation-aware teardown**

Add tests with these concrete sequences:

```go
func (s *gcStateManagerTestSuite) TestGCStateWatchRequiresActiveLeadership() {
	follower := NewGCStateManager(s.provider, s.manager.cfg, s.manager.keyspaceManager)
	_, err := follower.WatchGCStates(context.Background(), true)
	s.Require().ErrorIs(err, errs.ErrNotLeader)
}

func (s *gcStateManagerTestSuite) TestGCStateWatchLeadershipGeneration() {
	re := s.Require()
	stopFirst := s.manager.OnNodeBecomesLeader()
	first, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)

	stopSecond := s.manager.OnNodeBecomesLeader()
	re.ErrorIs(first.Err(), errs.ErrNotLeader)
	second, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)

	stopFirst()
	re.NoError(second.Err())
	stopSecond()
	re.ErrorIs(second.Err(), errs.ErrNotLeader)
}
```

Use the existing suite fixture rather than duplicating embedded-etcd setup. Construct the follower manager from the suite's provider, config, and keyspace manager so it has never received a leader callback.

- [ ] **Step 2: Write failing tests for initial loading and cleanup**

Add these exact tests:

- `TestGCStateWatchLoadsInitialStatesIncrementally`: call the unexported configured registration helper with batch size 2 and assert active keyspaces arrive in batches with no barriers.
- `TestGCStateWatchSkipsInitialLoading`: register with `skipLoadingInitial=true`, assert `initDone`, and verify the initial channel remains unused.
- `TestGCStateWatchLiveSuppressesPausedInitial`: persist transaction safe point `v1` for keyspace 2, pause `watchGCStatesInitialStateLoaded` only when that scope is read, register with initial loading, advance the same keyspace to `v2`, consume the live `v2`, release the loader, drain initial work, and assert that no emitted change contains `v1` for keyspace 2.
- `TestGCStateWatchInitialFailureTerminatesWatcher`: enable `iterateAllKeyspacesGCStatesError`, assert `RecvBatch` returns the injected error, and assert the registry no longer contains the watcher.
- `TestGCStateWatchFullInitChannelDoesNotHoldManagerMutex`: use initial batch size 1 and `initCh` capacity 1, wait until the channel is full, run a manager mutation and the leadership teardown with bounded channels, then close the watcher and assert the loader exits.
- `TestGCStateWatchConcurrentCloseIsIdempotent`: call `Close`, initialization termination, and leadership teardown concurrently and assert no registry or goroutine leak.

Implement the paused-initial case with this sequence; use a larger test-only `initCh` capacity so the loader can reach keyspace 2 before the consumer starts draining earlier keyspaces:

```go
func (s *gcStateManagerTestSuite) TestGCStateWatchLiveSuppressesPausedInitial() {
	re := s.Require()
	const keyspaceID = uint32(2)
	_, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 10, time.Now())
	re.NoError(err)

	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce, releaseOnce sync.Once
	releaseLoader := func() { releaseOnce.Do(func() { close(release) }) }
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/pkg/gc/watchGCStatesInitialStateLoaded", func(id uint32) {
		if id == keyspaceID {
			reachedOnce.Do(func() { close(reached) })
			<-release
		}
	}))
	defer func() { re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/gc/watchGCStatesInitialStateLoaded")) }()
	defer releaseLoader()

	w, err := s.manager.watchGCStates(context.Background(), false, gcStateWatchConfig{initialBatchSize: 1, initChannelCapacity: 16, liveChannelCapacity: 4})
	re.NoError(err)
	defer w.Close()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		re.FailNow("initial loader did not reach keyspace 2")
	}

	_, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 20, time.Now())
	re.NoError(err)
	for {
		changes, err := w.RecvBatch(1)
		re.NoError(err)
		state, ok := changes[0].Upsert()
		if ok && state.KeyspaceID == keyspaceID && state.TxnSafePoint == 20 {
			break
		}
	}
	releaseLoader()

	re.Eventually(func() bool {
		for {
			change, ok, err := w.receiveOne(false)
			re.NoError(err)
			if !ok {
				return w.initDone
			}
			state, upsert := change.Upsert()
			re.False(upsert && state.KeyspaceID == keyspaceID && state.TxnSafePoint == 10)
		}
	}, 5*time.Second, 10*time.Millisecond)
}
```

Add two failpoint call sites to make timing deterministic: `watchGCStatesRegistered` immediately after registration releases `GCStateManager.mu`, and `watchGCStatesInitialStateLoaded` after one state is read but before its batch can be sent. Neither call site can execute while holding the manager mutex.

- [ ] **Step 3: Run the focused lifecycle tests and confirm the red state**

Run:

```bash
make gotest GOTEST_ARGS='./pkg/gc -run "TestGCStateManager/TestGCStateWatch(Requires|Leadership|Loads|Skips|Live|Initial|Full|Concurrent)" -count=1'
```

Expected: compilation fails because manager registration, teardown closures, and watcher cleanup do not exist.

- [ ] **Step 4: Replace leadership counting with an active generation and teardown closure**

Keep lock-free leadership reads for existing cache fast paths while making generation creation manager-owned:

```go
watchers                   map[uint64]*GCStateWatcher
nextWatcherID              uint64
nextLeadershipGeneration   uint64
activeLeadershipGeneration atomic.Uint64

func (m *GCStateManager) OnNodeBecomesLeader() func() {
	m.mu.Lock()
	m.nextLeadershipGeneration++
	generation := m.nextLeadershipGeneration
	m.terminateAllGCStateWatchersLocked(errs.ErrNotLeader, watcherTerminationLeaderLost)
	m.activeLeadershipGeneration.Store(generation)
	m.gcStateCache.clearAll()
	m.mu.Unlock()

	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.activeLeadershipGeneration.Load() != generation {
			return
		}
		m.activeLeadershipGeneration.Store(0)
		m.terminateAllGCStateWatchersLocked(errs.ErrNotLeader, watcherTerminationLeaderLost)
		m.gcStateCache.clearAll()
	}
}

func (m *GCStateManager) nodeIsLeader() bool {
	return m.activeLeadershipGeneration.Load() != 0
}
```

Initialize the watcher map in `NewGCStateManager`. Remove `OnNodeBecomesFollower`; change cluster startup to the following direct assignment so the closure captured for this exact generation is invoked by `RaftCluster.Stop`:

```go
c.stopGCStateManager = s.GetGCStateManager().OnNodeBecomesLeader()
```

- [ ] **Step 5: Implement registration, idempotent removal, and initial loading**

Use these exact public and test-only entry points:

```go
func (m *GCStateManager) WatchGCStates(ctx context.Context, skipLoadingInitial bool) (*GCStateWatcher, error) {
	return m.watchGCStates(ctx, skipLoadingInitial, gcStateWatchConfig{
		initialBatchSize:    defaultGCStateWatchInitialBatchSize,
		initChannelCapacity: defaultGCStateWatchInitChannelCapacity,
		liveChannelCapacity: defaultGCStateWatchLiveChannelCapacity,
	})
}

func (m *GCStateManager) watchGCStates(ctx context.Context, skipLoadingInitial bool, cfg gcStateWatchConfig) (*GCStateWatcher, error)
func (m *GCStateManager) loadInitialGCStates(watcher *GCStateWatcher, batchSize int)
func (m *GCStateManager) terminateGCStateWatcher(watcher *GCStateWatcher, cause error, reason gcStateWatcherTerminationReason)
func (m *GCStateManager) terminateGCStateWatcherLocked(watcher *GCStateWatcher, cause error, reason gcStateWatcherTerminationReason) bool
func (m *GCStateManager) terminateAllGCStateWatchersLocked(cause error, reason gcStateWatcherTerminationReason)
func (w *GCStateWatcher) Close()
```

Add `manager *GCStateManager` and `id uint64` to `GCStateWatcher`. Define the bounded reasons now so Task 4 can attach metrics without changing lifecycle signatures:

```go
type gcStateWatcherTerminationReason string

const (
	watcherTerminationClientCancel gcStateWatcherTerminationReason = "client_cancel"
	watcherTerminationLeaderLost  gcStateWatcherTerminationReason = "leader_lost"
	watcherTerminationSlowConsumer gcStateWatcherTerminationReason = "slow_consumer"
	watcherTerminationInitError    gcStateWatcherTerminationReason = "init_error"
)
```

Registration locks the manager, rejects generation 0, allocates an ID, assigns the manager and ID to the watcher, inserts it, unlocks, invokes `watchGCStatesRegistered`, and only then starts the loader. For skipped initial loading, construct the watcher with `initDone=true` and do not start a loader.

The loader uses a local batch and a cancellation-aware blocking flush around the existing iterator:

```go
batch := make([]GCStateChange, 0, batchSize)
stopped := false
flush := func() bool {
	if len(batch) == 0 {
		return true
	}
	ready := batch
	batch = make([]GCStateChange, 0, batchSize)
	select {
	case watcher.initCh <- ready:
		return true
	case <-watcher.ctx.Done():
		return false
	}
}

err := m.iterateAllKeyspacesGCStates(
	watcher.ctx,
	true,
	func(uint32) bool { return true },
	func(state GCState) {
		if stopped {
			return
		}
		failpoint.InjectCall("watchGCStatesInitialStateLoaded", state.KeyspaceID)
		if watcher.Err() != nil {
			stopped = true
			return
		}
		batch = append(batch, NewGCStateUpsert(state))
		if len(batch) == batchSize {
			stopped = !flush()
		}
	},
	nil,
)

if stopped || watcher.Err() != nil {
	return
}
if err != nil {
	m.terminateGCStateWatcher(watcher, errors.Annotate(err, "load initial GC states"), watcherTerminationInitError)
	return
}
if !flush() {
	return
}
close(watcher.initCh)
```

The local `stopped` flag is required because the iterator callback cannot return an error. The failpoint runs before the state is appended, the flush replaces the batch backing slice before reuse, and only the loader closes `initCh` after successful iteration and final flush. If the watcher context already has a cause, the loader returns without replacing that cause.

`terminateGCStateWatcherLocked` first verifies that the ID still maps to the same watcher, deletes it, and calls its `CancelCauseFunc` without invoking any callback that reacquires `GCStateManager.mu`. `Close` delegates to the manager with `context.Canceled` and `watcherTerminationClientCancel`.

- [ ] **Step 6: Adapt existing manager tests to the teardown-returning callback**

In `newGCStateManagerForTest`, retain the teardown and include it in the returned cleanup:

```go
stopGCStateManager := gcStateManager.OnNodeBecomesLeader()
originalClean := clean
clean = func() {
	stopGCStateManager()
	originalClean()
}
```

Keep `ensureMarkedLeader` compatible by registering the returned closure with `s.T().Cleanup` whenever it creates a new leadership generation. Replace the one test that temporarily writes `nodeLeadership` with a save/set/restore of `activeLeadershipGeneration`, and update read-only assertions to call `nodeIsLeader()`. Search for every remaining `OnNodeBecomesLeader`, `OnNodeBecomesFollower`, and `nodeLeadership` reference so no test silently loses the teardown for the generation it creates.

- [ ] **Step 7: Run lifecycle and existing cache tests**

Run:

```bash
make gotest GOTEST_ARGS='./pkg/gc -run "TestGCStateManager/Test(GCStateWatch|GetGCStateCache|GetAllKeyspacesGCStates)" -count=1'
go test ./server/cluster -run '^$'
```

Expected: watcher lifecycle tests pass, existing leader-gated cache behavior remains green, and the cluster package compiles with the new callback.

- [ ] **Step 8: Format and commit manager integration**

Run:

```bash
gofmt -w pkg/gc/gc_state_watcher.go pkg/gc/gc_state_watcher_test.go pkg/gc/gc_state_manager.go pkg/gc/gc_state_manager_test.go server/cluster/cluster.go
git diff --check
git add pkg/gc/gc_state_watcher.go pkg/gc/gc_state_watcher_test.go pkg/gc/gc_state_manager.go pkg/gc/gc_state_manager_test.go server/cluster/cluster.go
git commit -s -m "gc: tie watchers to local leadership"
```

### Task 3: Publish effective safe-point changes without blocking mutations

This task attaches live publication exactly once to the successful shared mutation paths and proves that a full watcher queue affects only that watcher.

**Files:**

- Modify: `pkg/gc/gc_state_watcher.go`
- Modify: `pkg/gc/gc_state_watcher_test.go`
- Modify: `pkg/gc/gc_state_manager.go:350-413`
- Modify: `pkg/gc/gc_state_manager.go:445-604`
- Modify: `pkg/errs/errno.go:551-554`
- Modify: `errors.toml`

**Interfaces:**

- Consumes: Task 2's registry and termination helpers.
- Produces: `publishGCStateChangeLocked`, `errs.ErrGCStateWatcherSlowConsumer`, and complete live upserts used by the server stream.

- [ ] **Step 1: Write failing publication tests for modern, compatible, and barrier paths**

Register watchers with `skipLoadingInitial=true` and assert these exact cases:

```go
func (s *gcStateManagerTestSuite) TestGCStateWatchPublishesAdvanceGCSafePoint() {
	re := s.Require()
	const keyspaceID = uint32(2)
	_, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 20, time.Now())
	re.NoError(err)
	w, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)
	defer w.Close()

	_, _, err = s.manager.AdvanceGCSafePoint(keyspaceID, 10)
	re.NoError(err)
	changes, err := w.RecvBatch(1)
	re.NoError(err)
	state := mustUpsert(s.T(), changes[0])
	re.Equal(GCState{KeyspaceID: keyspaceID, IsKeyspaceLevel: true, TxnSafePoint: 20, GCSafePoint: 10}, state)
}

func (s *gcStateManagerTestSuite) TestGCStateWatchPublishesAdvanceTxnSafePoint() {
	re := s.Require()
	w, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)
	defer w.Close()

	_, err = s.manager.AdvanceTxnSafePoint(2, 20, time.Now())
	re.NoError(err)
	changes, err := w.RecvBatch(1)
	re.NoError(err)
	re.Equal(uint64(20), mustUpsert(s.T(), changes[0]).TxnSafePoint)
}

func (s *gcStateManagerTestSuite) TestGCStateWatchCompatiblePathsPublishOnce() {
	re := s.Require()
	const keyspaceID = uint32(2)
	_, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 30, time.Now())
	re.NoError(err)

	gcWatcher, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)
	_, _, err = s.manager.CompatibleUpdateGCSafePoint(keyspaceID, 10)
	re.NoError(err)
	changes, err := gcWatcher.RecvBatch(1)
	re.NoError(err)
	re.Len(changes, 1)
	re.Empty(gcWatcher.liveCh)
	gcWatcher.Close()

	txnWatcher, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)
	_, _, err = s.manager.CompatibleUpdateServiceGCSafePoint(keyspaceID, keypath.GCWorkerServiceSafePointID, 40, math.MaxInt64, time.Now())
	re.NoError(err)
	changes, err = txnWatcher.RecvBatch(1)
	re.NoError(err)
	re.Len(changes, 1)
	re.Empty(txnWatcher.liveCh)
	txnWatcher.Close()
}

func (s *gcStateManagerTestSuite) TestGCStateWatchDoesNotPublishNoOpOrFailure() {
	re := s.Require()
	const keyspaceID = uint32(2)
	_, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 20, time.Now())
	re.NoError(err)
	_, _, err = s.manager.AdvanceGCSafePoint(keyspaceID, 10)
	re.NoError(err)
	w, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)
	defer w.Close()

	_, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 20, time.Now())
	re.NoError(err)
	_, _, err = s.manager.CompatibleUpdateGCSafePoint(keyspaceID, 10)
	re.NoError(err)
	_, _, err = s.manager.AdvanceGCSafePoint(keyspaceID, 9)
	re.ErrorIs(err, errs.ErrDecreasingGCSafePoint)
	re.Empty(w.liveCh)
}

func (s *gcStateManagerTestSuite) TestGCStateWatchDoesNotPublishBarrierOnlyChanges() {
	re := s.Require()
	const keyspaceID = uint32(2)
	_, err := s.manager.AdvanceTxnSafePoint(keyspaceID, 20, time.Now())
	re.NoError(err)
	w, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)
	defer w.Close()

	_, err = s.manager.SetGCBarrier(keyspaceID, "backup", 30, time.Hour, time.Now())
	re.NoError(err)
	_, err = s.manager.DeleteGCBarrier(keyspaceID, "backup")
	re.NoError(err)
	re.Empty(w.liveCh)
}
```

For every complete upsert, assert `KeyspaceID`, `IsKeyspaceLevel`, `TxnSafePoint`, `GCSafePoint`, and an empty `GCBarriers` slice.

- [ ] **Step 2: Write the failing slow-consumer isolation test**

Create watcher A with `liveChannelCapacity=1` and watcher B with capacity 4. Publish two successful state changes without reading A, read B after each mutation, and assert:

```go
re.ErrorIs(watcherA.Err(), errs.ErrGCStateWatcherSlowConsumer)
re.NoError(watcherB.Err())
re.NotContains(s.manager.watchers, watcherA.id)
re.Contains(s.manager.watchers, watcherB.id)
```

Then reconnect A with initial loading enabled and assert its rebuilt state contains the latest safe points.

- [ ] **Step 3: Run the publication tests and confirm the red state**

Run:

```bash
make gotest GOTEST_ARGS='./pkg/gc -run "TestGCStateManager/TestGCStateWatch(Publishes|Compatible|DoesNotPublish|SlowConsumer)" -count=1'
```

Expected: tests fail because mutations do not publish and a full `liveCh` is not handled.

- [ ] **Step 4: Add the slow-consumer sentinel and regenerate error documentation**

Add the normalized error beside the existing GC errors:

```go
ErrGCStateWatcherSlowConsumer = errors.Normalize("GC state watcher is too slow", errors.RFCCodeText("PD:gc:ErrGCStateWatcherSlowConsumer"))
```

Run:

```bash
make generate-errdoc
```

Verify that `errors.toml` contains `PD:gc:ErrGCStateWatcherSlowConsumer` and no unrelated generated changes.

- [ ] **Step 5: Implement non-blocking fan-out under the manager mutex**

Add a locked helper that iterates the registry and uses a non-blocking send:

```go
func (m *GCStateManager) publishGCStateChangeLocked(change GCStateChange) {
	for _, watcher := range m.watchers {
		select {
		case watcher.liveCh <- change:
		default:
			log.Warn("GC state watcher is too slow", zap.Uint64("watcher-id", watcher.id), zap.Int("capacity", cap(watcher.liveCh)), zap.Int("queue-length", len(watcher.liveCh)))
			m.terminateGCStateWatcherLocked(watcher, errs.ErrGCStateWatcherSlowConsumer, watcherTerminationSlowConsumer)
		}
	}
	// TODO: Publish keyspace metadata upserts and removals through this same serialized path when an authoritative GC-leader-owned lifecycle hook exists.
}
```

Deleting the current watcher from a Go map during iteration is valid; do not collect a second removal list and do not wait, retry, or start one goroutine per watcher.

- [ ] **Step 6: Publish from the two successful post-cache-update paths**

Immediately after each existing `gcStateCache.store` call, publish only when the effective value changed:

```go
if newGCSafePoint != oldGCSafePoint {
	m.publishGCStateChangeLocked(NewGCStateUpsert(GCState{
		KeyspaceID:      keyspaceID,
		IsKeyspaceLevel: keyspaceID != constant.NullKeyspaceID,
		TxnSafePoint:    txnSafePoint,
		GCSafePoint:     newGCSafePoint,
	}))
}
```

```go
if newTxnSafePoint != oldTxnSafePoint {
	m.publishGCStateChangeLocked(NewGCStateUpsert(GCState{
		KeyspaceID:      keyspaceID,
		IsKeyspaceLevel: keyspaceID != constant.NullKeyspaceID,
		TxnSafePoint:    newTxnSafePoint,
		GCSafePoint:     gcSafePoint,
	}))
}
```

Keep these calls in `advanceGCSafePointImpl` and `advanceTxnSafePointImpl`; do not add publication at public entry points. This gives modern and compatible callers exactly one event.

- [ ] **Step 7: Run the publication and regression tests**

Run:

```bash
make gotest GOTEST_ARGS='./pkg/gc -run "TestGCStateManager/Test(GCStateWatch|Advance|Compatible|SetGCBarrier|DeleteGCBarrier)" -count=1'
```

Expected: complete upserts arrive once, no-op and barrier-only calls emit nothing, and only the slow watcher terminates.

- [ ] **Step 8: Format and commit live publication**

Run:

```bash
gofmt -w pkg/gc/gc_state_watcher.go pkg/gc/gc_state_watcher_test.go pkg/gc/gc_state_manager.go pkg/errs/errno.go
git diff --check
git add pkg/gc/gc_state_watcher.go pkg/gc/gc_state_watcher_test.go pkg/gc/gc_state_manager.go pkg/errs/errno.go errors.toml
git commit -s -m "gc: publish effective safe point changes"
```

### Task 4: Instrument the watcher lifecycle

This task adds low-cardinality metrics at the manager-owned registration and termination points, with pre-bound counter handles for every reason.

**Files:**

- Modify: `pkg/gc/metrics.go`
- Modify: `pkg/gc/gc_state_watcher.go`
- Modify: `pkg/gc/gc_state_watcher_test.go`

**Interfaces:**

- Consumes: Task 2's four termination-reason constants and the single idempotent termination helper.
- Produces: `pd_gc_watcher_count`, `pd_gc_watcher_termination_total{reason=...}`, and `recordGCStateWatcherTerminationMetrics`.

- [ ] **Step 1: Write failing metric-delta tests**

Use `prometheus/testutil.ToFloat64` and compare deltas so process-global counters do not make tests order-dependent:

```go
func (s *gcStateManagerTestSuite) TestGCStateWatcherMetrics() {
	re := s.Require()
	activeBefore := promtestutil.ToFloat64(gcStateWatcherGauge)
	leaderLostBefore := promtestutil.ToFloat64(gcStateWatcherTerminationLeaderLostCounter)

	stop := s.manager.OnNodeBecomesLeader()
	w, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)
	re.Equal(activeBefore+1, promtestutil.ToFloat64(gcStateWatcherGauge))

	stop()
	re.ErrorIs(w.Err(), errs.ErrNotLeader)
	re.Equal(activeBefore, promtestutil.ToFloat64(gcStateWatcherGauge))
	re.Equal(leaderLostBefore+1, promtestutil.ToFloat64(gcStateWatcherTerminationLeaderLostCounter))
	w.Close()
	re.Equal(leaderLostBefore+1, promtestutil.ToFloat64(gcStateWatcherTerminationLeaderLostCounter))
	stopRemainingCases := s.manager.OnNodeBecomesLeader()
	defer stopRemainingCases()
}
```

In the same test, record the three remaining counters before their triggers. For `client_cancel`, register one watcher and call `Close`. For `slow_consumer`, register with live capacity 1 and perform two successful `AdvanceTxnSafePoint` calls without reading the watcher, so both publications run through the production path while `GCStateManager.mu` is held. For `init_error`, enable `iterateAllKeyspacesGCStatesError`, register with initial loading, and call `RecvBatch`. After each trigger, assert that only its expected counter increased by one and the active gauge returned to `activeBefore`; never mutate a metric directly.

- [ ] **Step 2: Run the metric test and confirm the red state**

Run:

```bash
make gotest GOTEST_ARGS='./pkg/gc -run TestGCStateManager/TestGCStateWatcherMetrics -count=1'
```

Expected: compilation fails because the watcher metrics do not exist.

- [ ] **Step 3: Define and pre-bind the metrics**

Add these definitions to `pkg/gc/metrics.go`:

```go
gcStateWatcherGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "pd",
	Subsystem: "gc",
	Name:      "watcher_count",
	Help:      "Current number of active GC state watchers.",
})
gcStateWatcherTerminationCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "pd",
	Subsystem: "gc",
	Name:      "watcher_termination_total",
	Help:      "Total number of GC state watcher terminations by reason.",
}, []string{"reason"})

gcStateWatcherTerminationClientCancelCounter = gcStateWatcherTerminationCounter.WithLabelValues("client_cancel")
gcStateWatcherTerminationLeaderLostCounter = gcStateWatcherTerminationCounter.WithLabelValues("leader_lost")
gcStateWatcherTerminationSlowConsumerCounter = gcStateWatcherTerminationCounter.WithLabelValues("slow_consumer")
gcStateWatcherTerminationInitErrorCounter = gcStateWatcherTerminationCounter.WithLabelValues("init_error")
```

Register the gauge and vector with `prometheus.MustRegister`. Do not call `WithLabelValues` in registration, publication, or cleanup paths.

- [ ] **Step 4: Record metrics at the single lifecycle boundaries**

Increment the gauge only after successful insertion into the registry. In `terminateGCStateWatcherLocked`, decrement the gauge and invoke this switch only after deletion succeeds:

```go
func recordGCStateWatcherTerminationMetrics(reason gcStateWatcherTerminationReason) {
	switch reason {
	case watcherTerminationClientCancel:
		gcStateWatcherTerminationClientCancelCounter.Inc()
	case watcherTerminationLeaderLost:
		gcStateWatcherTerminationLeaderLostCounter.Inc()
	case watcherTerminationSlowConsumer:
		gcStateWatcherTerminationSlowConsumerCounter.Inc()
	case watcherTerminationInitError:
		gcStateWatcherTerminationInitErrorCounter.Inc()
	default:
		panic("unknown GC state watcher termination reason")
	}
}
```

No metric uses watcher IDs, keyspace IDs, client addresses, or error text as labels. The gauge has no labels, so teardown decrements it rather than deleting a label series.

- [ ] **Step 5: Run metric and lifecycle tests**

Run:

```bash
make gotest GOTEST_ARGS='./pkg/gc -run "TestGCStateManager/Test(GCStateWatcherMetrics|GCStateWatch)" -count=1'
```

Expected: each lifecycle increments exactly one reason counter and returns the active gauge to its baseline.

- [ ] **Step 6: Format and commit observability**

Run:

```bash
gofmt -w pkg/gc/metrics.go pkg/gc/gc_state_watcher.go pkg/gc/gc_state_watcher_test.go
git diff --check
git add pkg/gc/metrics.go pkg/gc/gc_state_watcher.go pkg/gc/gc_state_watcher_test.go
git commit -s -m "gc: add watcher lifecycle metrics"
```

### Task 5: Implement the gRPC transport adapter and upgrade kvproto

This task adopts the merged protobuf API, converts domain changes, batches by exact wire size, and implements the local server-streaming handler through a small test seam.

**Files:**

- Modify: `go.mod`
- Modify: `go.sum`
- Modify: `client/go.mod`
- Modify: `client/go.sum`
- Modify: `tools/go.mod`
- Modify: `tools/go.sum`
- Modify: `tests/integrations/go.mod`
- Modify: `tests/integrations/go.sum`
- Modify: `server/gc_service.go`
- Create: `server/gc_service_test.go`

**Interfaces:**

- Consumes: Task 2's `GCStateManager.WatchGCStates` and `GCStateWatcher` API; Task 3's slow-consumer sentinel and domain changes; kvproto `WatchGCStatesRequest`, `WatchGCStatesResponse`, and `GCStateChange`.
- Produces: `GrpcServer.WatchGCStates`, `gcStateChangeToProto`, `splitWatchGCStatesResponses`, `watchGCStatesErrorToStatus`, and `serveWatchGCStates`.

- [ ] **Step 1: Upgrade all four module scopes to the merged kvproto commit**

Use the exact pseudo-version derived from merged commit `65b4e27a438de9274bf88c58e89e83749e62f646`:

```bash
go get github.com/pingcap/kvproto@v0.0.0-20260903062353-65b4e27a438d
(cd client && go get github.com/pingcap/kvproto@v0.0.0-20260903062353-65b4e27a438d)
(cd tools && go get github.com/pingcap/kvproto@v0.0.0-20260903062353-65b4e27a438d)
(cd tests/integrations && go get github.com/pingcap/kvproto@v0.0.0-20260903062353-65b4e27a438d)
go mod tidy
(cd client && go mod tidy)
(cd tools && go mod tidy)
(cd tests/integrations && go mod tidy)
```

Run `git diff -- go.mod go.sum client/go.mod client/go.sum tools/go.mod tools/go.sum tests/integrations/go.mod tests/integrations/go.sum` and verify that the kvproto revision is the only dependency change.

- [ ] **Step 2: Confirm that the upgraded server has a missing-method red state**

Run:

```bash
go test ./server -run '^$'
```

Expected: compilation reports that `*GrpcServer` does not implement `pdpb.PDServer` because `WatchGCStates` is missing.

- [ ] **Step 3: Write failing converter and batching tests**

Create table-driven converter coverage for a complete upsert, a removed scope, and zero-value invalid `gc.GCStateChange`. Assert that upserts have no barriers. Add boundary tests that calculate `base := proto.Size(&pdpb.WatchGCStatesResponse{Header: grpcutil.WrapHeader()})` and `delta := proto.Size(&pdpb.WatchGCStatesResponse{Changes: []*pdpb.GCStateChange{change}})`.

```go
func TestSplitWatchGCStatesResponses(t *testing.T) {
	change := &pdpb.GCStateChange{Change: &pdpb.GCStateChange_Upsert{Upsert: &pdpb.GCState{
		KeyspaceScope: &pdpb.KeyspaceScope{Keyspace: &pdpb.KeyspaceScope_KeyspaceId{KeyspaceId: 7}},
		TxnSafePoint:  10,
		GcSafePoint:   5,
	}}}
	base := proto.Size(&pdpb.WatchGCStatesResponse{Header: grpcutil.WrapHeader()})
	delta := proto.Size(&pdpb.WatchGCStatesResponse{Changes: []*pdpb.GCStateChange{change}})

	exact := splitWatchGCStatesResponses([]*pdpb.GCStateChange{change, change}, base+2*delta)
	require.Len(t, exact, 1)
	require.LessOrEqual(t, proto.Size(exact[0]), base+2*delta)

	split := splitWatchGCStatesResponses([]*pdpb.GCStateChange{change, change}, base+2*delta-1)
	require.Len(t, split, 2)
	for _, response := range split {
		require.NotNil(t, response.GetHeader())
		require.NotEmpty(t, response.GetChanges())
		require.LessOrEqual(t, proto.Size(response), base+2*delta-1)
	}

	oversized := splitWatchGCStatesResponses([]*pdpb.GCStateChange{change}, base+delta-1)
	require.Len(t, oversized, 1)
	require.Greater(t, proto.Size(oversized[0]), base+delta-1)
	require.Empty(t, splitWatchGCStatesResponses(nil, base+delta))
}
```

- [ ] **Step 4: Write failing stream-loop and error-mapping tests**

Define a fake receiver implementing `RecvBatch(int) ([]gc.GCStateChange, error)` and `Err() error`, plus a fake `pdpb.PD_WatchGCStatesServer` whose `Send` callback can install a terminal cause. Cover these cases:

- Two responses derived from one batch: after the first `Send`, set `errs.ErrNotLeader`; assert only one response is recorded and the function returns `Unavailable`.
- Invalid internal change: assert zero sends and gRPC `Internal`.
- `errs.ErrNotLeader` and an arbitrary initialization error: assert `Unavailable`.
- `errs.ErrGCStateWatcherSlowConsumer`: assert `ResourceExhausted`.
- `context.Canceled` and `context.DeadlineExceeded`: assert `Canceled` and `DeadlineExceeded`.

Run:

```bash
go test ./server -run 'Test(GCStateChangeToProto|SplitWatchGCStatesResponses|ServeWatchGCStates|WatchGCStatesErrorToStatus)' -count=1
```

Expected: compilation fails because the adapter helpers do not exist.

- [ ] **Step 5: Implement conversion and exact wire-size batching**

Use these constants and receiver seam:

```go
const (
	watchGCStatesRecvBatchSize       = 1024
	maxWatchGCStatesResponseSize     = 1 << 20
)

type gcStateChangeReceiver interface {
	RecvBatch(maxChanges int) ([]gc.GCStateChange, error)
	Err() error
}
```

Convert the internal discriminator into the generated oneof and reject the zero value:

```go
func gcStateChangeToProto(change gc.GCStateChange) (*pdpb.GCStateChange, error) {
	if state, ok := change.Upsert(); ok {
		state.GCBarriers = nil
		return &pdpb.GCStateChange{Change: &pdpb.GCStateChange_Upsert{Upsert: gcStateToProto(state, time.Time{})}}, nil
	}
	if keyspaceID, ok := change.RemovedKeyspaceID(); ok {
		return &pdpb.GCStateChange{Change: &pdpb.GCStateChange_Removed{Removed: &pdpb.KeyspaceScope{Keyspace: &pdpb.KeyspaceScope_KeyspaceId{KeyspaceId: keyspaceID}}}}, nil
	}
	return nil, errors.New("invalid GC state change")
}
```

Implement `splitWatchGCStatesResponses(changes []*pdpb.GCStateChange, maxSize int)` with a fresh `grpcutil.WrapHeader()` response for each batch. Start `currentSize` with `proto.Size` of the header-only response. For each change, calculate the exact additive delta with `proto.Size(&pdpb.WatchGCStatesResponse{Changes: []*pdpb.GCStateChange{change}})`. Flush a non-empty current response before an addition that exceeds `maxSize`; never append an empty response. If the first change itself exceeds the target with the header, keep it alone, log its serialized size, and start a fresh response for the next change.

- [ ] **Step 6: Implement domain-error mapping and the send loop**

Map only watcher-domain errors; return `Send` errors unchanged:

```go
func watchGCStatesErrorToStatus(err error) error {
	switch {
	case errors.Is(err, errs.ErrGCStateWatcherSlowConsumer):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, errs.ErrNotLeader):
		return errs.ErrNotLeader
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}
```

`serveWatchGCStates` repeatedly receives at most 1024 changes, converts every change, splits them, checks `receiver.Err()` immediately before every `Send`, and returns the raw send error. Conversion failure logs the error and returns gRPC `Internal`; it does not create a watcher termination metric reason.

```go
func serveWatchGCStates(receiver gcStateChangeReceiver, stream pdpb.PD_WatchGCStatesServer, maxResponseSize int) error {
	for {
		changes, err := receiver.RecvBatch(watchGCStatesRecvBatchSize)
		if err != nil {
			return watchGCStatesErrorToStatus(err)
		}
		protoChanges := make([]*pdpb.GCStateChange, 0, len(changes))
		for _, change := range changes {
			converted, err := gcStateChangeToProto(change)
			if err != nil {
				log.Error("failed to convert GC state change", zap.Error(err))
				return status.Error(codes.Internal, err.Error())
			}
			protoChanges = append(protoChanges, converted)
		}
		for _, response := range splitWatchGCStatesResponses(protoChanges, maxResponseSize) {
			if err := receiver.Err(); err != nil {
				return watchGCStatesErrorToStatus(err)
			}
			if err := stream.Send(response); err != nil {
				return err
			}
		}
	}
}
```

- [ ] **Step 7: Implement local RPC preflight and stream ownership**

Implement the generated method directly in `server/gc_service.go`:

```go
func (s *GrpcServer) WatchGCStates(request *pdpb.WatchGCStatesRequest, stream pdpb.PD_WatchGCStatesServer) error {
	done, err := s.rateLimitCheck()
	if err != nil {
		return err
	}
	if done != nil {
		defer done()
	}
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return err
	}
	if s.GetRaftCluster() == nil {
		return status.Error(codes.Unavailable, errs.ErrNotBootstrapped.FastGenByArgs().Error())
	}

	watcher, err := s.gcStateManager.WatchGCStates(stream.Context(), request.GetSkipLoadingInitial())
	if err != nil {
		return watchGCStatesErrorToStatus(err)
	}
	defer watcher.Close()
	return serveWatchGCStates(watcher, stream, maxWatchGCStatesResponseSize)
}
```

Do not call `unaryMiddleware` or create a delegate client. Calling `rateLimitCheck` in this public method preserves the `WatchGCStates` limiter label, and deferring `done` here holds the token until the stream exits.

- [ ] **Step 8: Run transport tests and compile all kvproto consumers**

Run:

```bash
go test ./server -run 'Test(GCStateChangeToProto|SplitWatchGCStatesResponses|ServeWatchGCStates|WatchGCStatesErrorToStatus)' -count=1
go test ./pkg/gc ./server -run '^$'
(cd client && go test ./... -run '^$')
(cd tools && go test ./... -run '^$')
(cd tests/integrations && go test ./... -run '^$')
```

Expected: the converter, batching, cancellation, and status tests pass, and every module compiles against the same kvproto revision.

- [ ] **Step 9: Format and commit the transport implementation**

Run:

```bash
gofmt -w server/gc_service.go server/gc_service_test.go
git diff --check
git add go.mod go.sum client/go.mod client/go.sum tools/go.mod tools/go.sum tests/integrations/go.mod tests/integrations/go.sum server/gc_service.go server/gc_service_test.go
git commit -s -m "server: implement WatchGCStates stream"
```

### Task 6: Prove RPC behavior in a real PD cluster

This task covers the complete server stream, request preflight, lifetime rate limiting, and leader transfer through real generated gRPC clients.

**Files:**

- Modify: `tests/server/gc/gc_test.go`
- Modify if a test exposes an implementation defect: `server/gc_service.go`
- Modify if a test exposes a domain defect: `pkg/gc/gc_state_watcher.go`

**Interfaces:**

- Consumes: The generated `pdpb.PDClient.WatchGCStates` client and all production behavior from Tasks 1 through 5.
- Produces: End-to-end evidence for initial loading, skip-initial registration, validation, rate-limit token lifetime, and leadership reconnection.

- [ ] **Step 1: Add deterministic stream helpers**

Add a failpoint name constant for `github.com/tikv/pd/pkg/gc/watchGCStatesRegistered` and helpers that use bounded contexts:

```go
func recvWatchGCStateForKeyspace(t *testing.T, stream pdpb.PD_WatchGCStatesClient, keyspaceID uint32) *pdpb.GCState {
	t.Helper()
	for {
		response, err := stream.Recv()
		require.NoError(t, err)
		require.NotNil(t, response.GetHeader())
		for _, change := range response.GetChanges() {
			if state := change.GetUpsert(); state != nil && state.GetKeyspaceScope().GetKeyspaceId() == keyspaceID {
				return state
			}
		}
	}
}
```

Use `context.WithTimeout(..., 20*time.Second)` for every stream and clean up every connection, context, and failpoint with `t.Cleanup` or `defer`.

- [ ] **Step 2: Write the failing initial and skip-initial RPC test**

In a bootstrapped one-node cluster, create a keyspace-level GC keyspace and establish `skip_loading_initial=false`. Read until that keyspace's complete initial state arrives, advance its transaction safe point, and read the complete live state.

For `skip_loading_initial=true`, enable `watchGCStatesRegistered` with a `sync.Once` callback that closes a channel, establish the stream, wait for the callback, advance the same keyspace again, and assert the first received state contains the post-registration value. The registration callback removes timing sleeps and proves that no initial value was sent.

```go
stream, err := grpcPDClient.WatchGCStates(ctx, &pdpb.WatchGCStatesRequest{Header: header, SkipLoadingInitial: true})
require.NoError(t, err)
select {
case <-registered:
case <-time.After(5 * time.Second):
	require.FailNow(t, "WatchGCStates was not registered")
}
```

- [ ] **Step 3: Write failing request-preflight tests**

Use table-driven subtests that call `Recv` to observe server-stream establishment failures:

- A request with the wrong cluster ID returns `codes.FailedPrecondition`.
- A direct request to a follower returns `codes.Unavailable` and is not forwarded.
- A request to the elected leader before `BootstrapCluster` returns `codes.Unavailable`.

Assert that none of these cases returns a response message.

- [ ] **Step 4: Write the failing lifetime rate-limit test**

Enable gRPC rate limiting through `GetServiceMiddlewarePersistOptions().SetGRPCRateLimitConfig`, then call `GetGRPCRateLimiter().Update("WatchGCStates", ratelimit.UpdateConcurrencyLimiter(1))`. Use the registration failpoint to wait until stream 1 holds its token. Assert stream 2 returns `codes.ResourceExhausted`. Cancel stream 1, use `GetConcurrencyLimiterStatus("WatchGCStates")` with `testutil.Eventually` until current usage is zero, then establish stream 3 and receive a change after a safe-point advancement. Restore the prior rate-limit config and delete the test limiter with `ratelimit.UpdateConcurrencyLimiter(0)` during cleanup.

- [ ] **Step 5: Write the failing leader-transfer and reinitialization test**

Reuse `newGCStateLeaderTransitionCluster`. Start an initial watch directly against the old leader and receive its current null-keyspace state. Resign that leader, wait for a different leader, and drain the old stream until it returns `codes.Unavailable`. Advance the safe point on the new leader, connect to the new leader with `skip_loading_initial=false`, and assert its initial state contains the new value.

- [ ] **Step 6: Run the real-cluster tests and confirm the red or green state**

Run:

```bash
make gotest GOTEST_ARGS='./tests/server/gc -run ^TestWatchGCStates -count=1'
```

Expected before any required correction: at least one new test fails if the production path does not meet its contract. If all tests pass immediately, retain the tests as integration coverage and do not alter production code.

- [ ] **Step 7: Make only corrections demonstrated by the failing integration assertions**

Keep corrections inside the approved interfaces. Typical permitted corrections are preflight ordering, status mapping, cleanup order, or an additional cancellation check before `Send`. Do not add metadata producers, cross-PD transaction fencing, a replay protocol, a Go client, or `WatchGCSafePointV2` behavior.

After each correction, rerun:

```bash
make gotest GOTEST_ARGS='./tests/server/gc -run ^TestWatchGCStates -count=1'
make gotest GOTEST_ARGS='./pkg/gc -run TestGCStateManager/TestGCStateWatch -count=1'
make gotest GOTEST_ARGS='./server -run WatchGCStates -count=1'
```

Expected: all focused domain, transport, and integration tests pass.

- [ ] **Step 8: Format and commit real-cluster coverage**

Run:

```bash
gofmt -w tests/server/gc/gc_test.go server/gc_service.go pkg/gc/gc_state_watcher.go
git diff --check
git add tests/server/gc/gc_test.go server/gc_service.go pkg/gc/gc_state_watcher.go
git commit -s -m "tests: cover WatchGCStates lifecycle"
```

Omit unchanged production files from `git add`. If integration testing required no production correction, commit only `tests/server/gc/gc_test.go`.

### Task 7: Run final verification

This task verifies formatting, generated error documentation, module consistency, race safety, focused behavior, and the repository's required checks before handoff.

**Files:**

- Verify: all files changed in Tasks 1 through 6
- Modify: none unless a verification command reports a concrete defect

**Interfaces:**

- Consumes: The complete implementation and tests.
- Produces: Fresh command output demonstrating that the branch is ready for review.

- [ ] **Step 1: Ensure failpoints are disabled before non-test commands**

Run:

```bash
make failpoint-disable
```

Expected: failpoint-generated rewrites are removed before formatting or static analysis.

- [ ] **Step 2: Verify formatting and module tidiness**

Run:

```bash
make fmt
make generate-errdoc
make tidy
git diff --check
```

Expected: `make tidy` and `git diff --check` exit successfully. Inspect any formatting or generated change and commit it with the task that introduced the affected file; do not create an unexplained cleanup commit.

- [ ] **Step 3: Run focused package tests with failpoint handling**

Run:

```bash
make gotest GOTEST_ARGS='./pkg/gc -run ^TestGCStateWatcher -count=1'
make gotest GOTEST_ARGS='./pkg/gc -run TestGCStateManager/TestGCStateWatch -count=1'
make gotest GOTEST_ARGS='./server -run "Test(GCStateChangeToProto|SplitWatchGCStatesResponses|ServeWatchGCStates|WatchGCStatesErrorToStatus)" -count=1'
make gotest GOTEST_ARGS='./tests/server/gc -run ^TestWatchGCStates -count=1'
```

Expected: every focused test passes with zero failures.

- [ ] **Step 4: Run the focused race check**

Run:

```bash
make gotest GOTEST_ARGS='-race ./pkg/gc -run ^TestGCStateWatcher -count=1'
make gotest GOTEST_ARGS='-race ./pkg/gc -run TestGCStateManager/TestGCStateWatch -count=1'
make gotest GOTEST_ARGS='-race ./server ./tests/server/gc -run WatchGCStates -count=1'
```

Expected: all focused tests pass under the race detector with no race report or goroutine leak.

- [ ] **Step 5: Run repository-level checks**

Run:

```bash
make check
make basic-test
```

Expected: formatting, lint, leak checks, generated error documentation, and the basic unit-test suite pass.

- [ ] **Step 6: Verify scope and repository hygiene**

Run:

```bash
make failpoint-disable
! rg -n 'WatchGCSafePointV2' pkg/gc/gc_state_watcher.go server/gc_service.go tests/server/gc/gc_test.go
git status --short
git log --oneline --decorate -10
```

Expected: the `rg` command finds no new compatibility reference in the touched WatchGCStates paths, `git status --short` is empty, and the log shows the signed task commits in order.

## Execution handoff

The plan is complete when this document is reviewed and committed. Execute it with one of the required workflows:

1. **Subagent-driven:** Use `superpowers:subagent-driven-development`, dispatch a fresh worker for each task, and perform spec and code-quality review between tasks.
2. **Inline execution:** Use `superpowers:executing-plans`, execute tasks in batches, and stop at its review checkpoints.
