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
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/utils/keypath"
)

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

func TestGCStateWatcherQueuedLivePrecedesNewerInitial(t *testing.T) {
	for _, maxChanges := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("batch-size-%d", maxChanges), func(t *testing.T) {
			w := newGCStateWatcher(context.Background(), gcStateWatchConfig{initChannelCapacity: 2, liveChannelCapacity: 2}, false)
			t.Cleanup(w.Close)
			w.initCh <- []GCStateChange{
				NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 20}),
				NewGCStateUpsert(GCState{KeyspaceID: 8, TxnSafePoint: 1}),
			}
			w.initCh <- []GCStateChange{
				NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 20}),
				NewGCStateUpsert(GCState{KeyspaceID: 9, TxnSafePoint: 1}),
			}
			close(w.initCh)
			queueLiveWhenInitialReceived(t, w,
				NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 10}),
				NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 20}),
			)

			want := []GCState{
				{KeyspaceID: 7, TxnSafePoint: 10},
				{KeyspaceID: 7, TxnSafePoint: 20},
				{KeyspaceID: 8, TxnSafePoint: 1},
				{KeyspaceID: 9, TxnSafePoint: 1},
			}
			for offset := 0; offset < len(want); {
				got, err := w.RecvBatch(maxChanges)
				require.NoError(t, err)
				require.Len(t, got, min(maxChanges, len(want)-offset))
				for _, change := range got {
					require.Equal(t, want[offset], mustUpsert(t, change))
					offset++
				}
			}
			_, ok, err := w.receiveOne(false)
			require.NoError(t, err)
			require.False(t, ok, "initial duplicates must remain suppressed through the closed channel's buffered batches")
		})
	}
}

func TestGCStateWatcherLaterLiveDoesNotPostponePendingInitial(t *testing.T) {
	w := newGCStateWatcher(context.Background(), gcStateWatchConfig{initChannelCapacity: 1, liveChannelCapacity: 4}, false)
	t.Cleanup(w.Close)
	w.initCh <- []GCStateChange{
		NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 20}),
		NewGCStateUpsert(GCState{KeyspaceID: 8, TxnSafePoint: 1}),
		NewGCStateUpsert(GCState{KeyspaceID: 9, TxnSafePoint: 1}),
	}
	close(w.initCh)
	queueLiveWhenInitialReceived(t, w,
		NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 10}),
		NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 20}),
	)

	for i, want := range []GCState{
		{KeyspaceID: 7, TxnSafePoint: 10},
		{KeyspaceID: 7, TxnSafePoint: 20},
		{KeyspaceID: 8, TxnSafePoint: 1},
		{KeyspaceID: 9, TxnSafePoint: 1},
	} {
		got, err := w.RecvBatch(1)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, want, mustUpsert(t, got[0]))
		// Keep the live queue nonempty after the first receive. These arrivals
		// must not postpone the unrelated states in the acquired initial batch.
		w.liveCh <- NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: uint64(30 + i*10)})
	}
	got, err := w.RecvBatch(4)
	require.NoError(t, err)
	require.Len(t, got, 4)
	for i, want := range []uint64{30, 40, 50, 60} {
		require.Equal(t, GCState{KeyspaceID: 7, TxnSafePoint: want}, mustUpsert(t, got[i]))
	}
}

func TestGCStateWatcherQueuedRemovalSuppressesInitial(t *testing.T) {
	w := newGCStateWatcher(context.Background(), gcStateWatchConfig{initChannelCapacity: 1, liveChannelCapacity: 2}, false)
	t.Cleanup(w.Close)
	w.initCh <- []GCStateChange{
		NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 10}),
		NewGCStateUpsert(GCState{KeyspaceID: 8, TxnSafePoint: 1}),
	}
	close(w.initCh)
	queueLiveWhenInitialReceived(t, w, NewGCStateRemoved(7))

	got, err := w.RecvBatch(3)
	require.NoError(t, err)
	require.Len(t, got, 2)
	removed, ok := got[0].RemovedKeyspaceID()
	require.True(t, ok)
	require.Equal(t, uint32(7), removed)
	require.Equal(t, GCState{KeyspaceID: 8, TxnSafePoint: 1}, mustUpsert(t, got[1]))
}

func TestGCStateWatcherCancellationDiscardsPendingLivePrefix(t *testing.T) {
	w := newGCStateWatcher(context.Background(), gcStateWatchConfig{initChannelCapacity: 1, liveChannelCapacity: 2}, false)
	t.Cleanup(w.Close)
	w.initCh <- []GCStateChange{
		NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 20}),
		NewGCStateUpsert(GCState{KeyspaceID: 8, TxnSafePoint: 1}),
	}
	close(w.initCh)
	queueLiveWhenInitialReceived(t, w,
		NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 10}),
		NewGCStateUpsert(GCState{KeyspaceID: 7, TxnSafePoint: 20}),
	)
	got, err := w.RecvBatch(1)
	require.NoError(t, err)
	require.Equal(t, GCState{KeyspaceID: 7, TxnSafePoint: 10}, mustUpsert(t, got[0]))

	want := errors.New("watch terminated with pending initial and live changes")
	w.cancel(want)
	got, err = w.RecvBatch(3)
	require.ErrorIs(t, err, want)
	require.Nil(t, got)
}

func queueLiveWhenInitialReceived(t *testing.T, w *GCStateWatcher, changes ...GCStateChange) {
	t.Helper()
	const name = "github.com/tikv/pd/pkg/gc/watchGCStatesInitialBatchReceived"
	// Recreate the reachable merge state after select picks an initial batch
	// while older live changes are queued. Enqueue in the hook only to force
	// that branch deterministically, without depending on select randomness.
	require.NoError(t, failpoint.EnableCall(name, func() {
		for _, change := range changes {
			w.liveCh <- change
		}
		changes = nil
	}))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(name)) })
}

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
	re.Empty(state.GCBarriers)
}

func (s *gcStateManagerTestSuite) TestGCStateWatchPublishesAdvanceTxnSafePoint() {
	re := s.Require()
	const keyspaceID = uint32(2)
	w, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)
	defer w.Close()

	_, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 20, time.Now())
	re.NoError(err)
	changes, err := w.RecvBatch(1)
	re.NoError(err)
	state := mustUpsert(s.T(), changes[0])
	re.Equal(GCState{KeyspaceID: keyspaceID, IsKeyspaceLevel: true, TxnSafePoint: 20}, state)
	re.Empty(state.GCBarriers)
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
	state := mustUpsert(s.T(), changes[0])
	re.Equal(GCState{KeyspaceID: keyspaceID, IsKeyspaceLevel: true, TxnSafePoint: 30, GCSafePoint: 10}, state)
	re.Empty(state.GCBarriers)
	re.Empty(gcWatcher.liveCh)
	gcWatcher.Close()

	txnWatcher, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)
	_, _, err = s.manager.CompatibleUpdateServiceGCSafePoint(keyspaceID, keypath.GCWorkerServiceSafePointID, 40, math.MaxInt64, time.Now())
	re.NoError(err)
	changes, err = txnWatcher.RecvBatch(1)
	re.NoError(err)
	re.Len(changes, 1)
	state = mustUpsert(s.T(), changes[0])
	re.Equal(GCState{KeyspaceID: keyspaceID, IsKeyspaceLevel: true, TxnSafePoint: 40, GCSafePoint: 10}, state)
	re.Empty(state.GCBarriers)
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

func (s *gcStateManagerTestSuite) TestGCStateWatchSlowConsumerIsolation() {
	re := s.Require()
	const keyspaceID = uint32(2)
	watcherA, err := s.manager.registerGCStateWatcher(context.Background(), true, gcStateWatchConfig{liveChannelCapacity: 1})
	re.NoError(err)
	defer watcherA.Close()
	watcherB, err := s.manager.registerGCStateWatcher(context.Background(), true, gcStateWatchConfig{liveChannelCapacity: 4})
	re.NoError(err)
	defer watcherB.Close()

	_, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 10, time.Now())
	re.NoError(err)
	changes, err := watcherB.RecvBatch(1)
	re.NoError(err)
	state := mustUpsert(s.T(), changes[0])
	re.Equal(GCState{KeyspaceID: keyspaceID, IsKeyspaceLevel: true, TxnSafePoint: 10}, state)
	re.Empty(state.GCBarriers)

	_, err = s.manager.AdvanceTxnSafePoint(keyspaceID, 20, time.Now())
	re.NoError(err)
	changes, err = watcherB.RecvBatch(1)
	re.NoError(err)
	state = mustUpsert(s.T(), changes[0])
	re.Equal(GCState{KeyspaceID: keyspaceID, IsKeyspaceLevel: true, TxnSafePoint: 20}, state)
	re.Empty(state.GCBarriers)

	re.ErrorIs(watcherA.Err(), errs.ErrGCStateWatcherSlowConsumer)
	re.NoError(watcherB.Err())
	re.NotContains(s.manager.watchers, watcherA.id)
	re.Contains(s.manager.watchers, watcherB.id)

	reconnected, err := s.manager.WatchGCStates(context.Background(), false)
	re.NoError(err)
	defer reconnected.Close()
	for {
		changes, err = reconnected.RecvBatch(1)
		re.NoError(err)
		state = mustUpsert(s.T(), changes[0])
		if state.KeyspaceID == keyspaceID {
			break
		}
	}
	re.Equal(GCState{KeyspaceID: keyspaceID, IsKeyspaceLevel: true, TxnSafePoint: 20}, state)
	re.Empty(state.GCBarriers)
}

func (s *gcStateManagerTestSuite) TestGCStateWatcherMetrics() {
	re := s.Require()
	activeBefore := promtestutil.ToFloat64(gcStateWatcherGauge)
	clientCancelBefore := promtestutil.ToFloat64(gcStateWatcherTerminationClientCancelCounter)
	leaderLostBefore := promtestutil.ToFloat64(gcStateWatcherTerminationLeaderLostCounter)
	slowConsumerBefore := promtestutil.ToFloat64(gcStateWatcherTerminationSlowConsumerCounter)
	initErrorBefore := promtestutil.ToFloat64(gcStateWatcherTerminationInitErrorCounter)
	assertTerminationDeltas := func(clientCancel, leaderLost, slowConsumer, initError float64) {
		re.Equal(clientCancelBefore+clientCancel, promtestutil.ToFloat64(gcStateWatcherTerminationClientCancelCounter))
		re.Equal(leaderLostBefore+leaderLost, promtestutil.ToFloat64(gcStateWatcherTerminationLeaderLostCounter))
		re.Equal(slowConsumerBefore+slowConsumer, promtestutil.ToFloat64(gcStateWatcherTerminationSlowConsumerCounter))
		re.Equal(initErrorBefore+initError, promtestutil.ToFloat64(gcStateWatcherTerminationInitErrorCounter))
	}

	stop := s.manager.OnNodeBecomesLeader()
	w, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)
	re.Equal(activeBefore+1, promtestutil.ToFloat64(gcStateWatcherGauge))

	stop()
	re.ErrorIs(w.Err(), errs.ErrNotLeader)
	re.Equal(activeBefore, promtestutil.ToFloat64(gcStateWatcherGauge))
	assertTerminationDeltas(0, 1, 0, 0)
	w.Close()
	assertTerminationDeltas(0, 1, 0, 0)

	stopRemainingCases := s.manager.OnNodeBecomesLeader()
	defer stopRemainingCases()

	clientCanceled, err := s.manager.WatchGCStates(context.Background(), true)
	re.NoError(err)
	re.Equal(activeBefore+1, promtestutil.ToFloat64(gcStateWatcherGauge))
	clientCanceled.Close()
	clientCanceled.Close()
	re.Equal(activeBefore, promtestutil.ToFloat64(gcStateWatcherGauge))
	assertTerminationDeltas(1, 1, 0, 0)

	slowConsumer, err := s.manager.registerGCStateWatcher(context.Background(), true, gcStateWatchConfig{liveChannelCapacity: 1})
	re.NoError(err)
	re.Equal(activeBefore+1, promtestutil.ToFloat64(gcStateWatcherGauge))
	_, err = s.manager.AdvanceTxnSafePoint(2, 10, time.Now())
	re.NoError(err)
	re.Equal(activeBefore+1, promtestutil.ToFloat64(gcStateWatcherGauge))
	_, err = s.manager.AdvanceTxnSafePoint(2, 20, time.Now())
	re.NoError(err)
	re.ErrorIs(slowConsumer.Err(), errs.ErrGCStateWatcherSlowConsumer)
	re.Equal(activeBefore, promtestutil.ToFloat64(gcStateWatcherGauge))
	assertTerminationDeltas(1, 1, 1, 0)
	slowConsumer.Close()
	assertTerminationDeltas(1, 1, 1, 0)

	const errorMessage = "injected initial watch failure"
	func() {
		re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/gc/iterateAllKeyspacesGCStatesError", fmt.Sprintf(`return(%q)`, errorMessage)))
		defer func() { re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/gc/iterateAllKeyspacesGCStatesError")) }()
		initFailed, err := s.manager.WatchGCStates(context.Background(), false)
		re.NoError(err)
		_, err = initFailed.RecvBatch(1)
		re.ErrorContains(err, errorMessage)
		re.Equal(activeBefore, promtestutil.ToFloat64(gcStateWatcherGauge))
		assertTerminationDeltas(1, 1, 1, 1)
		initFailed.Close()
		assertTerminationDeltas(1, 1, 1, 1)
	}()
}

func mustUpsert(t testing.TB, change GCStateChange) GCState {
	state, ok := change.Upsert()
	require.True(t, ok)
	return state
}

func TestGCStateWatcherDonePublishesFirstCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	w := newGCStateWatcher(ctx, gcStateWatchConfig{initChannelCapacity: 1, liveChannelCapacity: 1}, true)
	defer w.Close()
	done := w.Done()
	require.Equal(t, done, w.Done())
	select {
	case <-done:
		require.FailNow(t, "watcher terminated before cancellation")
	default:
	}
	cancel(errs.ErrNotLeader)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "watcher termination was not notified")
	}
	require.ErrorIs(t, w.Err(), errs.ErrNotLeader)
	w.Close()
	require.ErrorIs(t, w.Err(), errs.ErrNotLeader)
	require.Equal(t, done, w.Done())
}

func TestGCStateWatcherUsesEnabledMetadataCache(t *testing.T) {
	_, _, manager, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{useEnabledKeyspaceCache: true})
	defer clean()
	defer cancel()
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	require.NoError(t, manager.enabledKeyspaces.waitReady(ctx))

	id := uint32(19)
	_, err := manager.keyspaceManager.CreateKeyspaceByID(&keyspace.CreateKeyspaceByIDRequest{
		ID: &id, Name: "watch-cache-created", Config: map[string]string{keyspace.GCManagementType: keyspace.KeyspaceLevelGC}, CreateTime: time.Now().Unix(),
	})
	require.NoError(t, err)
	// A full watcher must use the index even when the legacy iterator fails.
	const failpointName = "github.com/tikv/pd/pkg/gc/iterateAllKeyspacesGCStatesError"
	require.NoError(t, failpoint.Enable(failpointName, `return("legacy iterator used")`))
	defer func() { require.NoError(t, failpoint.Disable(failpointName)) }()
	w, err := manager.WatchGCStates(ctx, false)
	require.NoError(t, err)
	defer w.Close()
	for {
		changes, err := w.RecvBatch(16)
		require.NoError(t, err)
		for _, change := range changes {
			if state := mustUpsert(t, change); state.KeyspaceID == id {
				require.True(t, state.IsKeyspaceLevel)
				return
			}
		}
	}
}

func TestGCStateWatcherWaitReadyStopsOnCancelAndLeaderLoss(t *testing.T) {
	_, _, manager, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{
		useEnabledKeyspaceCache: true,
		beforeLeader: func(c *clientv3.Client) {
			_, err := c.Put(context.Background(), keypath.KeyspaceMetaPath(19), "invalid protobuf")
			require.NoError(t, err)
		},
	})
	defer clean()
	defer cancel()

	ctx, stop := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := manager.WatchGCStates(ctx, false)
		result <- err
	}()
	stop()
	require.ErrorIs(t, <-result, context.Canceled)
	require.Empty(t, manager.watchers)

	waiting := make(chan struct{}, 1)
	const waitHook = "github.com/tikv/pd/pkg/gc/watchGCStatesBeforeCacheReady"
	require.NoError(t, failpoint.EnableCall(waitHook, func() { waiting <- struct{}{} }))
	defer func() { require.NoError(t, failpoint.Disable(waitHook)) }()
	result = make(chan error, 1)
	go func() {
		_, err := manager.WatchGCStates(context.Background(), false)
		result <- err
	}()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not begin waiting for cache readiness")
	}
	stopTerm := manager.OnNodeBecomesLeader()
	defer stopTerm()
	select {
	case err := <-result:
		require.ErrorIs(t, err, errs.ErrNotLeader)
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not exit on leader change")
	}
	require.Empty(t, manager.watchers)
}

func TestGCStateWatcherIndexedInitialMergesConcurrentGCWrite(t *testing.T) {
	_, _, manager, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{useEnabledKeyspaceCache: true})
	defer clean()
	defer cancel()
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	require.NoError(t, manager.enabledKeyspaces.waitReady(ctx))
	_, err := manager.AdvanceTxnSafePoint(2, 10, time.Now())
	require.NoError(t, err)

	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	releaseLoader := func() { once.Do(func() { close(release) }) }
	defer releaseLoader()
	const hook = "github.com/tikv/pd/pkg/gc/watchGCStatesInitialStateLoaded"
	require.NoError(t, failpoint.EnableCall(hook, func(id uint32) {
		if id == 2 {
			close(reached)
			<-release
		}
	}))
	defer func() { require.NoError(t, failpoint.Disable(hook)) }()
	w, err := manager.WatchGCStates(ctx, false)
	require.NoError(t, err)
	defer w.Close()
	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatal("initial loader did not reach keyspace 2")
	}
	_, err = manager.AdvanceTxnSafePoint(2, 20, time.Now())
	require.NoError(t, err)
	for {
		changes, err := w.RecvBatch(1)
		require.NoError(t, err)
		if state := mustUpsert(t, changes[0]); state.KeyspaceID == 2 {
			require.Equal(t, uint64(20), state.TxnSafePoint)
			break
		}
	}
	releaseLoader()
	require.Eventually(t, func() bool {
		for {
			change, ok, err := w.receiveOne(false)
			require.NoError(t, err)
			if !ok {
				return w.initDone
			}
			state := mustUpsert(t, change)
			require.False(t, state.KeyspaceID == 2 && state.TxnSafePoint == 10)
		}
	}, 5*time.Second, 10*time.Millisecond)
}

func TestGCStateWatcherIndexedPostRegistrationErrorCleansUp(t *testing.T) {
	_, _, manager, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{useEnabledKeyspaceCache: true})
	defer clean()
	defer cancel()
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	require.NoError(t, manager.enabledKeyspaces.waitReady(ctx))
	const hook = "github.com/tikv/pd/pkg/gc/watchGCStatesRegistered"
	require.NoError(t, failpoint.EnableCall(hook, manager.cancelEnabledKeyspaces))
	defer func() { require.NoError(t, failpoint.Disable(hook)) }()
	w, err := manager.WatchGCStates(ctx, false)
	require.NoError(t, err)
	defer w.Close()
	_, err = w.RecvBatch(1)
	require.Error(t, err)
	require.NotContains(t, manager.watchers, w.id)
}
