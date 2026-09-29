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
	storage, _, m, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{})
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
	batch, err := m.prepareGCStateLoadBatch(context.Background(), generation, gcStateLoadCandidates(ids...))
	require.NoError(t, err)
	require.Len(t, batch.keyspaceIDs, 60)
	result := m.executeGCStateLoadBatch(context.Background(), batch)
	require.Len(t, result.completed, 119)
	require.Len(t, result.failed, 1)
	require.Error(t, result.failed[1])
	_, ok := m.gcStateCache.load(1)
	require.False(t, ok)

	// Join an owned flight, then invalidate its successful publication before
	// consuming the notification. The terminal success must remain observable.
	owned := make(chan *gcStateLoadFlight, 1)
	execute := make(chan struct{})
	finished := make(chan gcStateLoadResult, 1)
	go func() {
		b, e := m.prepareGCStateLoadBatch(context.Background(), generation, gcStateLoadCandidates(121))
		if e != nil {
			finished <- gcStateLoadResult{failed: map[uint32]error{121: e}}
			return
		}
		owned <- b.flights[0]
		<-execute
		finished <- m.executeGCStateLoadBatch(context.Background(), b)
	}()
	flight := <-owned
	b, err := m.prepareGCStateLoadBatch(context.Background(), generation, gcStateLoadCandidates(121))
	require.NoError(t, err)
	require.Empty(t, b.keyspaceIDs)
	joined := m.executeGCStateLoadBatch(context.Background(), b)
	require.Same(t, flight, joined.joined[121])
	close(execute)
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
		b, err := m.prepareGCStateLoadBatch(context.Background(), generation, gcStateLoadCandidates(2))
		if err != nil {
			return
		}
		result <- m.executeGCStateLoadBatch(context.Background(), b)
	}()
	<-entered
	b, err := m.prepareGCStateLoadBatch(context.Background(), generation, gcStateLoadCandidates(2))
	require.NoError(t, err)
	joined := m.executeGCStateLoadBatch(context.Background(), b)
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
		b, err := m.prepareGCStateLoadBatch(ctx, generation, gcStateLoadCandidates(2))
		if err == nil {
			r := m.executeGCStateLoadBatch(ctx, b)
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
	ready := make(chan *gcStateLoadFlight, 1)
	execute := make(chan struct{})
	done := make(chan gcStateLoadResult, 1)
	go func() {
		b, err := m.prepareGCStateLoadBatch(context.Background(), current, gcStateLoadCandidates(2))
		if err != nil {
			return
		}
		ready <- b.flights[0]
		<-execute
		done <- m.executeGCStateLoadBatch(context.Background(), b)
	}()
	flight := <-ready
	oldCleanup := make(chan struct{})
	go func() { oldStop(); oldStop(); close(oldCleanup) }()
	current.mu.Lock()
	currentFlight := current.flights[2]
	current.mu.Unlock()
	require.Same(t, flight, currentFlight)
	close(execute)
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
