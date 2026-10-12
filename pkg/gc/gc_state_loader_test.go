// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestGCStateLoadConcurrent(t *testing.T) {
	var reads atomic.Int32
	var enabled atomic.Bool
	entered := make(chan struct{}, 16)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	_, provider, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{
		etcdClientCfgModifier: func(cfg *clientv3.Config) {
			cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				matches := false
				switch r := req.(type) {
				case *etcdserverpb.RangeRequest:
					matches = string(r.Key) == keypath.TxnSafePointPath(2)
				case *etcdserverpb.TxnRequest:
					for _, op := range r.Success {
						if read := op.GetRequestRange(); read != nil && string(read.Key) == keypath.TxnSafePointPath(2) {
							matches = true
						}
					}
				}
				if enabled.Load() && matches {
					reads.Add(1)
					entered <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			}))
		},
	})
	defer clean()
	defer cancel()
	defer unblock()
	require.NoError(t, provider.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		if err := wb.SetTxnSafePoint(2, 90); err != nil {
			return err
		}
		return wb.SetGCSafePoint(2, 70)
	}))
	joined := make(chan uint32, 15)
	const joinedFailpoint = "github.com/tikv/pd/pkg/gc/onGCStateLoadJoined"
	require.NoError(t, failpoint.EnableCall(joinedFailpoint, func(id uint32) { joined <- id }))
	defer func() { require.NoError(t, failpoint.Disable(joinedFailpoint)) }()
	enabled.Store(true)
	results := make(chan GCState, 16)
	failures := make(chan error, 16)
	for range 16 {
		go func() { state, err := m.GetGCState(2, true); results <- state; failures <- err }()
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("load did not start")
	}
	// Keep the owner in storage until every other reader has joined its flight.
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for range 15 {
		select {
		case id := <-joined:
			require.Equal(t, uint32(2), id)
		case <-deadline.C:
			t.Fatal("readers did not all join the blocked load")
		}
	}
	unblock()
	for range 16 {
		require.NoError(t, <-failures)
		require.Equal(t, GCState{KeyspaceID: 2, IsKeyspaceLevel: true, TxnSafePoint: 90, GCSafePoint: 70}, <-results)
	}
	require.Equal(t, int32(1), reads.Load(), "overlapping cold reads must share one storage transaction")
}

func TestGCStateLeadershipGatesBeforeReset(t *testing.T) {
	_, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{})
	defer clean()
	defer cancel()
	m.gcStateCache.store(2, gcStateCacheEntry{TxnSafePoint: 90, GCSafePoint: 70})
	m.mu.Lock()
	done := make(chan struct{})
	go func() { m.OnNodeBecomesLeader(); close(done) }()
	gated := false
	deadline := time.After(time.Second)
	for !gated {
		select {
		case <-deadline:
			goto checked
		default:
			gated = !m.nodeIsLeader()
			time.Sleep(time.Millisecond)
		}
	}
checked:
	m.mu.Unlock()
	<-done
	require.True(t, gated, "cache eligibility must be disabled before waiting for the reset lock")
}

func gcStateLoadCandidates(ids ...uint32) func() (uint32, bool) {
	return func() (uint32, bool) {
		if len(ids) == 0 {
			return 0, false
		}
		id := ids[0]
		ids = ids[1:]
		return id, true
	}
}

func TestGCStateLoadBatchAndTerminalResult(t *testing.T) {
	var batchSize atomic.Int32
	storage, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{
		etcdClientCfgModifier: func(cfg *clientv3.Config) {
			cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if txn := gcWarmupRead(req); txn != nil {
					batchSize.Store(int32(len(txn.Success) / 2))
				}
				return invoke(ctx, method, req, reply, cc, opts...)
			}))
		},
	})
	defer clean()
	defer cancel()
	generation := m.activeGeneration.Load()
	// Interleaved cached IDs must not consume any of the 60 read slots.
	ids := make([]uint32, 120)
	for i := range ids {
		ids[i] = uint32(i)
		if i%2 == 0 {
			m.gcStateCache.store(uint32(i), gcStateCacheEntry{TxnSafePoint: 20})
		}
	}
	require.NoError(t, storage.Save(keypath.TxnSafePointPath(1), "broken"))
	result, err := m.loadGCStateBatch(context.Background(), generation, gcStateLoadCandidates(ids...))
	require.NoError(t, err)
	require.Equal(t, int32(60), batchSize.Load())
	require.Len(t, result.completed, 119)
	require.Len(t, result.failed, 1)
	require.Error(t, result.failed[1])
	_, ok := m.gcStateCache.load(1)
	require.False(t, ok)

	// Join an owned flight, then invalidate its successful publication before
	// consuming the notification. The terminal success must remain observable.
	read := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	const afterRead = "github.com/tikv/pd/pkg/gc/afterGCStateLoadBatchRead"
	require.NoError(t, failpoint.EnableCall(afterRead, func() { close(read); <-release }))
	defer func() { require.NoError(t, failpoint.Disable(afterRead)) }()
	finished := make(chan gcStateLoadResult, 1)
	go func() {
		r, e := m.loadGCStateBatch(context.Background(), generation, gcStateLoadCandidates(121))
		if e != nil {
			r.failed = map[uint32]error{121: e}
		}
		finished <- r
	}()
	gcWarmupWait(t, read)
	generation.mu.Lock()
	flight := generation.flights[121]
	generation.mu.Unlock()
	require.NotNil(t, flight)
	joined, err := m.loadGCStateBatch(context.Background(), generation, gcStateLoadCandidates(121))
	require.NoError(t, err)
	require.Empty(t, joined.completed)
	require.Empty(t, joined.failed)
	require.Same(t, flight, joined.joined[121])
	unblock()
	require.Equal(t, []uint32{121}, (<-finished).completed)
	m.mu.Lock()
	m.gcStateCache.remove(121)
	m.mu.Unlock()
	require.NoError(t, flight.wait(context.Background()))
}

func TestGCStateLeadershipCleanupIsolation(t *testing.T) {
	_, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{})
	defer clean()
	defer cancel()
	oldStop := m.OnNodeBecomesLeader()
	old := m.activeGeneration.Load()
	newStop := m.OnNodeBecomesLeader()
	defer newStop()
	current := m.activeGeneration.Load()
	require.NotSame(t, old, current)
	m.gcStateCache.store(2, gcStateCacheEntry{TxnSafePoint: 30})
	oldStop()
	oldStop()
	require.Same(t, current, m.activeGeneration.Load())
	entry, ok := m.gcStateCache.load(2)
	require.True(t, ok)
	require.Equal(t, uint64(30), entry.TxnSafePoint)
	newStop()
	newStop()
	require.False(t, m.nodeIsLeader())
	_, ok = m.gcStateCache.load(2)
	require.False(t, ok)
}

func newGCStateLoaderManager(t *testing.T, interceptor grpc.UnaryClientInterceptor) *GCStateManager {
	t.Helper()
	_, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{
		etcdClientCfgModifier: func(cfg *clientv3.Config) {
			cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(interceptor))
		},
	})
	t.Cleanup(func() { m.stopGCStateGeneration(m.activeGeneration.Load()); cancel(); clean() })
	return m
}

func gcStateLoadTestTxn(req any) bool {
	txn, ok := req.(*etcdserverpb.TxnRequest)
	if !ok || len(txn.Compare) != 0 || len(txn.Success) != 2 {
		return false
	}
	first := txn.Success[0].GetRequestRange()
	return first != nil && string(first.Key) == keypath.TxnSafePointPath(2)
}

func TestGCStateLoadJoinedWaiterRetriesAfterInvalidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entered := make(chan struct{})
	releaseLoad := make(chan struct{})
	var reads atomic.Int32
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if gcStateLoadTestTxn(req) && reads.Add(1) == 1 {
			close(entered)
			select {
			case <-releaseLoad:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	var releaseLoadOnce sync.Once
	unblockLoad := func() { releaseLoadOnce.Do(func() { close(releaseLoad) }) }
	defer unblockLoad()
	require.NoError(t, m.gcMetaStorage.RunInGCStateTransaction(func(wb *endpoint.GCStateWriteBatch) error {
		if err := wb.SetTxnSafePoint(2, 90); err != nil {
			return err
		}
		return wb.SetGCSafePoint(2, 70)
	}))
	joined := make(chan uint32, 1)
	const joinedFailpoint = "github.com/tikv/pd/pkg/gc/onGCStateLoadJoined"
	require.NoError(t, failpoint.EnableCall(joinedFailpoint, func(id uint32) { joined <- id }))
	defer func() { require.NoError(t, failpoint.Disable(joinedFailpoint)) }()
	waited := make(chan struct{})
	releaseWaiter := make(chan struct{})
	const waitedFailpoint = "github.com/tikv/pd/pkg/gc/afterGCStateLoadJoinedWait"
	require.NoError(t, failpoint.EnableCall(waitedFailpoint, func() {
		close(waited)
		select {
		case <-releaseWaiter:
		case <-ctx.Done():
		}
	}))
	defer func() { require.NoError(t, failpoint.Disable(waitedFailpoint)) }()
	var releaseWaiterOnce sync.Once
	unblockWaiter := func() { releaseWaiterOnce.Do(func() { close(releaseWaiter) }) }
	defer unblockWaiter()

	// Own the initial cold load, then make a foreground request join it.
	generation := m.activeGeneration.Load()
	loaded := make(chan gcStateLoadResult, 1)
	go func() {
		result, err := m.loadGCStateBatch(ctx, generation, gcStateLoadCandidates(2))
		if err != nil {
			result.failed = map[uint32]error{2: err}
		}
		loaded <- result
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("cold load did not start")
	}
	type foregroundResult struct {
		state GCState
		err   error
	}
	foreground := make(chan foregroundResult, 1)
	go func() {
		state, err := m.getGCStateImpl(ctx, 2, true)
		foreground <- foregroundResult{state: state, err: err}
	}()
	select {
	case id := <-joined:
		require.Equal(t, uint32(2), id)
	case <-ctx.Done():
		t.Fatal("foreground reader did not join the cold load")
	}
	unblockLoad()
	select {
	case <-waited:
	case <-ctx.Done():
		t.Fatal("joined reader did not observe successful load completion")
	}
	select {
	case result := <-loaded:
		require.Empty(t, result.failed)
		require.Equal(t, []uint32{2}, result.completed)
	case <-ctx.Done():
		t.Fatal("cold load did not finish")
	}
	cached, ok := m.gcStateCache.load(2)
	require.True(t, ok)
	require.Equal(t, gcStateCacheEntry{TxnSafePoint: 90, GCSafePoint: 70}, cached)

	// Publish newer values, then invalidate the cache through a rejected writer
	// before the joined reader can recheck it. Successful writes alone refresh it.
	_, err := m.AdvanceTxnSafePoint(2, 100, time.Now())
	require.NoError(t, err)
	_, _, err = m.AdvanceGCSafePoint(2, 80)
	require.NoError(t, err)
	_, _, err = m.AdvanceGCSafePoint(2, 79)
	require.ErrorIs(t, err, errs.ErrDecreasingGCSafePoint)
	_, ok = m.gcStateCache.load(2)
	require.False(t, ok)
	unblockWaiter()
	select {
	case result := <-foreground:
		require.NoError(t, result.err)
		require.Equal(t, GCState{KeyspaceID: 2, IsKeyspaceLevel: true, TxnSafePoint: 100, GCSafePoint: 80}, result.state)
	case <-ctx.Done():
		t.Fatal("joined reader did not retry the invalidated load")
	}
	require.Equal(t, int32(2), reads.Load(), "the joined reader must reload invalidated values from storage")
}

func gcStateLoadInvalidationGate(t *testing.T, ctx context.Context) (<-chan struct{}, chan<- struct{}) {
	t.Helper()
	loaded := make(chan struct{}, 1)
	resume := make(chan struct{})
	const hook = "github.com/tikv/pd/pkg/gc/afterGCStateLoadCompleted"
	require.NoError(t, failpoint.EnableCall(hook, func() {
		select {
		case loaded <- struct{}{}:
		case <-ctx.Done():
			return
		}
		select {
		case <-resume:
		case <-ctx.Done():
		}
	}))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(hook)) })
	return loaded, resume
}

func gcStateInvalidateAfterWrite(t *testing.T, m *GCStateManager, gcSafePoint uint64) {
	t.Helper()
	_, err := m.AdvanceTxnSafePoint(2, gcSafePoint+20, time.Now())
	require.NoError(t, err)
	_, _, err = m.AdvanceGCSafePoint(2, gcSafePoint)
	require.NoError(t, err)
	_, _, err = m.AdvanceGCSafePoint(2, gcSafePoint-1)
	require.ErrorIs(t, err, errs.ErrDecreasingGCSafePoint)
	_, cached := m.gcStateCache.load(2)
	require.False(t, cached)
}

func TestGCStateLoadDirectFallback(t *testing.T) {
	for _, name := range []string{"success", "storage-error", "txn-decode-error", "gc-decode-error", "request-cancel", "request-deadline", "retirement", "request-cancel-before-return", "retirement-before-return"} {
		t.Run(name, func(t *testing.T) {
			var reads atomic.Int32
			rpcContexts := make(chan context.Context, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			loadErr := errors.New("fallback read unavailable")
			storage, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{
				etcdClientCfgModifier: func(cfg *clientv3.Config) {
					cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
						if gcStateLoadTestTxn(req) && reads.Add(1) == 3 {
							var err error
							if name == "storage-error" {
								err = loadErr
							} else {
								err = invoker(ctx, method, req, reply, cc, opts...)
							}
							// Return the already-read response even after cancellation so
							// the loader must check cancellation and leadership itself.
							rpcContexts <- ctx
							select {
							case <-release:
							case <-ctx.Done():
							}
							return err
						}
						return invoker(ctx, method, req, reply, cc, opts...)
					}))
				},
			})
			defer clean()
			defer cancel()
			defer m.stopGCStateGeneration(m.activeGeneration.Load())
			defer unblock()
			type valueKey struct{}
			ctx := context.WithValue(context.Background(), valueKey{}, "foreground")
			var cancelRequest context.CancelFunc
			if name == "request-deadline" {
				ctx, cancelRequest = context.WithTimeout(ctx, 2*time.Second)
			} else {
				ctx, cancelRequest = context.WithTimeout(ctx, 10*time.Second)
			}
			defer cancelRequest()
			shared, resume := gcStateLoadInvalidationGate(t, ctx)
			type foregroundResult struct {
				state GCState
				err   error
			}
			foreground := make(chan foregroundResult, 1)
			go func() {
				state, err := m.getGCStateImpl(ctx, 2, true)
				foreground <- foregroundResult{state: state, err: err}
			}()
			for _, target := range []uint64{80, 90} {
				gcWarmupWait(t, shared)
				gcStateInvalidateAfterWrite(t, m, target)
				if target == 90 {
					switch name {
					case "txn-decode-error":
						require.NoError(t, storage.Save(keypath.TxnSafePointPath(2), "broken"))
					case "gc-decode-error":
						require.NoError(t, storage.Save(keypath.GCSafePointPath(2), "broken"))
					}
				}
				select {
				case resume <- struct{}{}:
				case <-ctx.Done():
					t.Fatal("request canceled before resuming the shared reader")
				}
			}
			var rpcCtx context.Context
			select {
			case rpcCtx = <-rpcContexts:
			case <-ctx.Done():
				t.Fatal("fallback read did not start")
			}
			require.Equal(t, "foreground", rpcCtx.Value(valueKey{}))
			generation := m.activeGeneration.Load()
			var stopped <-chan struct{}
			switch name {
			case "request-cancel":
				cancelRequest()
			case "request-deadline":
				deadline, _ := ctx.Deadline()
				rpcDeadline, hasDeadline := rpcCtx.Deadline()
				require.True(t, hasDeadline)
				require.Equal(t, deadline, rpcDeadline)
				gcWarmupWait(t, ctx.Done())
			case "retirement":
				done := make(chan struct{})
				stopped = done
				go func() { m.stopGCStateGeneration(generation); close(done) }()
				gcWarmupWait(t, generation.done)
				gcWarmupWait(t, rpcCtx.Done())
			case "request-cancel-before-return", "retirement-before-return":
				consumed := make(chan struct{})
				resumeReturn := make(chan struct{})
				var returnOnce sync.Once
				unblockReturn := func() { returnOnce.Do(func() { close(resumeReturn) }) }
				const hook = "github.com/tikv/pd/pkg/gc/beforeGCStateLoadDirectReturn"
				require.NoError(t, failpoint.EnableCall(hook, func() {
					close(consumed)
					select {
					case <-resumeReturn:
					case <-ctx.Done():
					}
				}))
				defer func() { require.NoError(t, failpoint.Disable(hook)) }()
				defer unblockReturn()
				unblock()
				gcWarmupWait(t, consumed)
				// The reader has returned a successful pair and released its lock;
				// only the foreground result branch can now reject the stale result.
				if name == "retirement-before-return" {
					done := make(chan struct{})
					stopped = done
					m.stopGCStateGeneration(generation)
					close(done)
					stop := m.OnNodeBecomesLeader()
					defer stop()
					m.gcStateCache.store(2, gcStateCacheEntry{TxnSafePoint: 120, GCSafePoint: 100})
				} else {
					cancelRequest()
				}
				unblockReturn()
			default:
				unblock()
			}
			var result foregroundResult
			select {
			case result = <-foreground:
			case <-shared:
				t.Fatal("another shared reload started instead of returning the direct read")
			case <-time.After(3 * time.Second):
				t.Fatal("foreground fallback did not finish")
			}
			switch name {
			case "success":
				require.NoError(t, result.err)
				require.Equal(t, GCState{KeyspaceID: 2, IsKeyspaceLevel: true, TxnSafePoint: 110, GCSafePoint: 90}, result.state)
			case "storage-error":
				require.ErrorIs(t, result.err, loadErr)
			case "txn-decode-error", "gc-decode-error":
				require.Error(t, result.err)
			case "request-cancel", "request-cancel-before-return":
				require.ErrorIs(t, result.err, context.Canceled)
				gcWarmupWait(t, rpcCtx.Done())
			case "request-deadline":
				require.ErrorIs(t, result.err, context.DeadlineExceeded)
			case "retirement", "retirement-before-return":
				require.ErrorIs(t, result.err, errs.ErrNotLeader)
				gcWarmupWait(t, stopped)
				if name == "retirement" {
					stop := m.OnNodeBecomesLeader()
					defer stop()
					m.gcStateCache.store(2, gcStateCacheEntry{TxnSafePoint: 120, GCSafePoint: 100})
				}
				m.stopGCStateGeneration(generation)
				state, err := m.getGCStateImpl(context.Background(), 2, true)
				require.NoError(t, err)
				require.Equal(t, uint64(120), state.TxnSafePoint)
				require.Equal(t, uint64(100), state.GCSafePoint)
			}
			require.Equal(t, int32(3), reads.Load(), "two shared reads and one independent fallback must suffice")
			if name != "retirement" && name != "retirement-before-return" {
				_, cached := m.gcStateCache.load(2)
				require.False(t, cached, "fallback must not depend on or repopulate the cache")
			}
		})
	}
}

func TestGCStateLoadDirectFallbackCancellationBeforeManagerLock(t *testing.T) {
	var reads atomic.Int32
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if gcStateLoadTestTxn(req) {
			reads.Add(1)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shared, resume := gcStateLoadInvalidationGate(t, ctx)
	var lockAttempts atomic.Int32
	waiting := make(chan struct{})
	const hook = "github.com/tikv/pd/pkg/gc/beforeGCStateLoadManagerLock"
	require.NoError(t, failpoint.EnableCall(hook, func() {
		if lockAttempts.Add(1) == 3 {
			close(waiting)
		}
	}))
	defer func() { require.NoError(t, failpoint.Disable(hook)) }()
	foreground := make(chan error, 1)
	go func() { _, err := m.getGCStateImpl(ctx, 2, true); foreground <- err }()
	locked := false
	defer func() {
		if locked {
			m.mu.Unlock()
		}
	}()
	for _, target := range []uint64{80, 90} {
		gcWarmupWait(t, shared)
		gcStateInvalidateAfterWrite(t, m, target)
		if target == 90 {
			m.mu.Lock()
			locked = true
		}
		select {
		case resume <- struct{}{}:
		case <-ctx.Done():
			t.Fatal("request canceled before resuming the shared reader")
		}
	}
	gcWarmupWait(t, waiting)
	cancel()
	select {
	case err := <-foreground:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("request cancellation waited for the fallback's manager lock")
	}
	require.Equal(t, int32(2), reads.Load(), "direct I/O must not start while a writer holds the manager lock")
}

func TestGCStateLoadWaiterCancellationAndOverlap(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var reads atomic.Int32
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if gcStateLoadTestTxn(req) {
			reads.Add(1)
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	joined := make(chan uint32, 1)
	require.NoError(t, failpoint.EnableCall("github.com/tikv/pd/pkg/gc/onGCStateLoadJoined", func(id uint32) { joined <- id }))
	defer func() { require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/gc/onGCStateLoadJoined")) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner := make(chan error, 1)
	go func() { _, err := m.getGCStateImpl(ctx, 2, true); owner <- err }()
	<-entered
	waiter := make(chan error, 1)
	go func() { _, err := m.getGCStateImpl(context.Background(), 2, true); waiter <- err }()
	require.Equal(t, uint32(2), <-joined)
	cancel()
	require.ErrorIs(t, <-owner, context.Canceled)
	// Different scopes can complete while the first scope remains in storage I/O.
	other := make(chan error, 1)
	go func() { _, err := m.getGCStateImpl(context.Background(), 3, true); other <- err }()
	select {
	case err := <-other:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("unrelated scope blocked behind another flight")
	}
	unblock()
	require.NoError(t, <-waiter)
	require.Equal(t, int32(1), reads.Load())
}

func TestGCStateLoadFailureWakesJoinedReaders(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var fail atomic.Bool
	fail.Store(true)
	loadErr := errors.New("load unavailable")
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if gcStateLoadTestTxn(req) && fail.Load() {
			entered <- struct{}{}
			select {
			case <-release:
				return loadErr
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	generation := m.activeGeneration.Load()
	result := make(chan gcStateLoadResult, 1)
	go func() {
		r, err := m.loadGCStateBatch(context.Background(), generation, gcStateLoadCandidates(2))
		if err != nil {
			r.failed = map[uint32]error{2: err}
		}
		result <- r
	}()
	<-entered
	joined, err := m.loadGCStateBatch(context.Background(), generation, gcStateLoadCandidates(2))
	require.NoError(t, err)
	require.NotNil(t, joined.joined[2])
	unblock()
	require.ErrorIs(t, joined.joined[2].wait(context.Background()), loadErr)
	require.ErrorIs(t, (<-result).failed[2], loadErr)
	_, ok := m.gcStateCache.load(2)
	require.False(t, ok)
	fail.Store(false)
	state, err := m.getGCStateImpl(context.Background(), 2, true)
	require.NoError(t, err)
	require.Equal(t, GCState{KeyspaceID: 2, IsKeyspaceLevel: true}, state)
}

func TestGCStateLeadershipCancelsLoadBeforeLock(t *testing.T) {
	entered := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	var block atomic.Bool
	block.Store(true)
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if gcStateLoadTestTxn(req) && block.Load() {
			entered <- struct{}{}
			<-ctx.Done()
			canceled <- struct{}{}
			return ctx.Err()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	generation := m.activeGeneration.Load()
	loaded := make(chan error, 1)
	go func() { _, err := m.getGCStateImpl(context.Background(), 2, true); loaded <- err }()
	<-entered
	stopped := make(chan struct{})
	go func() { m.stopGCStateGeneration(generation); close(stopped) }()
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("retirement waited for manager lock before canceling I/O")
	}
	require.ErrorIs(t, <-loaded, errs.ErrNotLeader)
	<-stopped
	require.False(t, m.nodeIsLeader())
	block.Store(false)
	stop := m.OnNodeBecomesLeader()
	defer stop()
	state, err := m.getGCStateImpl(context.Background(), 2, true)
	require.NoError(t, err)
	require.Equal(t, uint64(0), state.TxnSafePoint)
}

func TestGCStateGenerationCancellationCancelsAllBatches(t *testing.T) {
	entered := make(chan struct{}, 2)
	canceled := make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if gcWarmupRead(req) != nil {
			entered <- struct{}{}
			select {
			case <-ctx.Done():
				canceled <- struct{}{}
				return ctx.Err()
			case <-release:
				return context.Canceled
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	generation := m.activeGeneration.Load()
	require.Nil(t, generation.index, "foreground-only generations must also own a cancellation domain")
	require.NotNil(t, generation.cancel)
	results := make(chan error, 2)
	for _, id := range []uint32{2, 3} {
		go func() {
			result, err := m.loadGCStateBatch(context.Background(), generation, gcStateLoadCandidates(id))
			if err == nil {
				err = result.failed[id]
			}
			results <- err
		}()
	}
	for range 2 {
		gcWarmupWait(t, entered)
	}
	// Exercise the common cancellation domain directly, without retirement's
	// flight notification or any per-batch registration traversal.
	generation.cancel()
	for range 2 {
		gcWarmupWait(t, canceled)
		select {
		case err := <-results:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(3 * time.Second):
			t.Fatal("batch did not complete after generation cancellation")
		}
	}
	require.Same(t, generation, m.activeGeneration.Load())
	for _, id := range []uint32{2, 3} {
		_, cached := m.gcStateCache.load(id)
		require.False(t, cached)
	}
}

func TestGCStateLoadBatchInputCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancellation"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			type valueKey struct{}
			entered := make(chan struct{}, 1)
			rpcContexts := make(chan context.Context, 1)
			m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if gcStateLoadTestTxn(req) {
					rpcContexts <- ctx
					entered <- struct{}{}
					<-ctx.Done()
					return ctx.Err()
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			})
			generation := m.activeGeneration.Load()
			ctx := context.WithValue(context.Background(), valueKey{}, "batch")
			var cancelLoad context.CancelFunc
			wantErr := context.Canceled
			if deadline {
				ctx, cancelLoad = context.WithTimeout(ctx, time.Second)
				wantErr = context.DeadlineExceeded
			} else {
				ctx, cancelLoad = context.WithCancel(ctx)
			}
			defer cancelLoad()
			loaded := make(chan error, 1)
			go func() {
				result, err := m.loadGCStateBatch(ctx, generation, gcStateLoadCandidates(2))
				if err == nil {
					err = result.failed[2]
				}
				loaded <- err
			}()
			gcWarmupWait(t, entered)
			rpcCtx := <-rpcContexts
			require.Equal(t, "batch", rpcCtx.Value(valueKey{}))
			if deadline {
				inputDeadline, _ := ctx.Deadline()
				rpcDeadline, hasDeadline := rpcCtx.Deadline()
				require.True(t, hasDeadline)
				require.Equal(t, inputDeadline, rpcDeadline)
			}
			if !deadline {
				cancelLoad()
			}
			select {
			case err := <-loaded:
				require.ErrorIs(t, err, wantErr)
			case <-time.After(3 * time.Second):
				t.Fatal("batch ignored input context cancellation")
			}
			require.Same(t, generation, m.activeGeneration.Load())
			_, cached := m.gcStateCache.load(2)
			require.False(t, cached)
		})
	}
}

func TestGCStateLoadCancelBeforePublication(t *testing.T) {
	_, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{})
	defer clean()
	defer cancel()
	read := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	require.NoError(t, failpoint.EnableCall("github.com/tikv/pd/pkg/gc/afterGCStateLoadBatchRead", func() { close(read); <-release }))
	defer func() { require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/gc/afterGCStateLoadBatchRead")) }()
	generation := m.activeGeneration.Load()
	loaded := make(chan error, 1)
	go func() { _, err := m.getGCStateImpl(context.Background(), 2, true); loaded <- err }()
	<-read
	stopped := make(chan struct{})
	go func() { m.stopGCStateGeneration(generation); close(stopped) }()
	<-generation.done
	unblock()
	require.ErrorIs(t, <-loaded, errs.ErrNotLeader)
	<-stopped
	_, ok := m.gcStateCache.load(2)
	require.False(t, ok)
}

func TestGCStateLoadChecksAfterManagerLock(t *testing.T) {
	_, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{})
	defer clean()
	defer cancel()
	generation := m.activeGeneration.Load()
	ctx, cancelLoad := context.WithCancel(context.Background())
	m.mu.Lock()
	started := make(chan struct{})
	require.NoError(t, failpoint.EnableCall("github.com/tikv/pd/pkg/gc/beforeGCStateLoadManagerLock", func() { close(started) }))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/gc/beforeGCStateLoadManagerLock"))
	}()
	result := make(chan error, 1)
	go func() {
		r, err := m.loadGCStateBatch(ctx, generation, gcStateLoadCandidates(2))
		if err == nil {
			err = r.failed[2]
		}
		result <- err
	}()
	<-started
	cancelLoad()
	m.mu.Unlock()
	require.ErrorIs(t, <-result, context.Canceled)
	_, ok := m.gcStateCache.load(2)
	require.False(t, ok)
}

func TestGCStateLoadCancellationDuringAssembly(t *testing.T) {
	for _, retire := range []bool{false, true} {
		name := "cancellation"
		if retire {
			name = "retirement"
		}
		t.Run(name, func(t *testing.T) {
			_, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{})
			defer clean()
			defer cancel()
			generation := m.activeGeneration.Load()
			defer m.stopGCStateGeneration(generation)
			ctx, cancelLoad := context.WithCancel(context.Background())
			defer cancelLoad()
			pending := true
			wantErr := context.Canceled
			if retire {
				wantErr = errs.ErrNotLeader
			}
			result, err := m.loadGCStateBatch(ctx, generation, func() (uint32, bool) {
				if pending {
					pending = false
					return 2, true
				}
				// End assembly after a flight has already been claimed. Retirement
				// gates new work before it can acquire the generation lock itself.
				if retire {
					m.activeGeneration.CompareAndSwap(generation, nil)
				} else {
					cancelLoad()
				}
				return 0, false
			})
			require.NoError(t, err)
			require.ErrorIs(t, result.failed[2], wantErr)
			require.Empty(t, result.completed)
			_, cached := m.gcStateCache.load(2)
			require.False(t, cached)
			generation.mu.Lock()
			flights := len(generation.flights)
			generation.mu.Unlock()
			require.Zero(t, flights, "claimed flights must complete on cancellation")
			writerDone := make(chan struct{})
			go func() {
				m.mu.Lock()
				m.gcStateCache.remove(2)
				m.mu.Unlock()
				close(writerDone)
			}()
			gcWarmupWait(t, writerDone)
		})
	}
}

func TestGCStateLoadMissBeforeFlightRetires(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var reads atomic.Int32
	m := newGCStateLoaderManager(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if gcStateLoadTestTxn(req) {
			reads.Add(1)
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	owner := make(chan error, 1)
	go func() { _, err := m.getGCStateImpl(context.Background(), 2, true); owner <- err }()
	<-entered
	missed := make(chan struct{})
	resume := make(chan struct{})
	var resumeOnce sync.Once
	resumeReader := func() { resumeOnce.Do(func() { close(resume) }) }
	defer resumeReader()
	require.NoError(t, failpoint.EnableCall("github.com/tikv/pd/pkg/gc/getGCStateBeforeSlowPath", func() { close(missed); <-resume }))
	defer func() { require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/gc/getGCStateBeforeSlowPath")) }()
	waiter := make(chan error, 1)
	go func() { _, err := m.getGCStateImpl(context.Background(), 2, true); waiter <- err }()
	<-missed
	unblock()
	require.NoError(t, <-owner)
	resumeReader()
	require.NoError(t, <-waiter)
	require.Equal(t, int32(1), reads.Load(), "cache recheck and flight claim must not race completed publication")
}

func TestGCStateLeadershipResetWaitsForOldPublication(t *testing.T) {
	_, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{})
	defer clean()
	defer cancel()
	oldStop := m.OnNodeBecomesLeader()
	old := m.activeGeneration.Load()
	read := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	require.NoError(t, failpoint.EnableCall("github.com/tikv/pd/pkg/gc/afterGCStateLoadBatchRead", func() { close(read); <-release }))
	defer func() { require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/gc/afterGCStateLoadBatchRead")) }()
	loaded := make(chan error, 1)
	go func() { _, err := m.getGCStateImpl(context.Background(), 2, true); loaded <- err }()
	<-read
	reset := make(chan func(), 1)
	go func() { reset <- m.OnNodeBecomesLeader() }()
	<-old.done
	require.Nil(t, m.activeGeneration.Load(), "new generation cannot publish before old batch releases manager RLock")
	unblock()
	require.ErrorIs(t, <-loaded, errs.ErrNotLeader)
	newStop := <-reset
	defer newStop()
	require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/gc/afterGCStateLoadBatchRead"))
	current := m.activeGeneration.Load()
	ready := make(chan struct{})
	execute := make(chan struct{})
	var executeOnce sync.Once
	unblockExecute := func() { executeOnce.Do(func() { close(execute) }) }
	defer unblockExecute()
	require.NoError(t, failpoint.EnableCall("github.com/tikv/pd/pkg/gc/afterGCStateLoadBatchRead", func() { close(ready); <-execute }))
	done := make(chan gcStateLoadResult, 1)
	go func() {
		r, err := m.loadGCStateBatch(context.Background(), current, gcStateLoadCandidates(2))
		if err != nil {
			r.failed = map[uint32]error{2: err}
		}
		done <- r
	}()
	gcWarmupWait(t, ready)
	current.mu.Lock()
	flight := current.flights[2]
	current.mu.Unlock()
	require.NotNil(t, flight)
	oldCleanup := make(chan struct{})
	go func() { oldStop(); oldStop(); close(oldCleanup) }()
	current.mu.Lock()
	currentFlight := current.flights[2]
	current.mu.Unlock()
	require.Same(t, flight, currentFlight)
	unblockExecute()
	require.Equal(t, []uint32{2}, (<-done).completed)
	<-oldCleanup
	require.Same(t, current, m.activeGeneration.Load())
	state, ok := m.gcStateCache.load(2)
	require.True(t, ok)
	require.Zero(t, state.TxnSafePoint)
}

func TestGCStateLeadershipResetDisablesFastReads(t *testing.T) {
	_, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{})
	defer clean()
	defer cancel()
	m.gcStateCache.store(2, gcStateCacheEntry{TxnSafePoint: 90, GCSafePoint: 70})
	resetting := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	require.NoError(t, failpoint.EnableCall("github.com/tikv/pd/pkg/gc/beforeLeaderGCStateCacheReset", func() { close(resetting); <-release }))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/tikv/pd/pkg/gc/beforeLeaderGCStateCacheReset"))
	}()
	reset := make(chan func(), 1)
	go func() { reset <- m.OnNodeBecomesLeader() }()
	<-resetting
	// Legacy reads can finish without manager.mu and must bypass the old cache.
	safePoint, err := m.CompatibleLoadGCSafePoint(2)
	require.NoError(t, err)
	require.Zero(t, safePoint)
	states := make(chan GCState, 1)
	failures := make(chan error, 1)
	go func() { state, err := m.GetGCState(2, true); states <- state; failures <- err }()
	select {
	case state := <-states:
		t.Fatalf("read returned during cache reset: %+v", state)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	stop := <-reset
	defer stop()
	require.NoError(t, <-failures)
	state := <-states
	require.Zero(t, state.TxnSafePoint)
	require.Zero(t, state.GCSafePoint)
}
