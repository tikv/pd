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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
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
	watcherA, err := s.manager.watchGCStates(context.Background(), true, gcStateWatchConfig{liveChannelCapacity: 1})
	re.NoError(err)
	defer watcherA.Close()
	watcherB, err := s.manager.watchGCStates(context.Background(), true, gcStateWatchConfig{liveChannelCapacity: 4})
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

	slowConsumer, err := s.manager.watchGCStates(context.Background(), true, gcStateWatchConfig{liveChannelCapacity: 1})
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
