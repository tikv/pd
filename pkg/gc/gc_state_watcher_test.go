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
	"testing"

	"github.com/stretchr/testify/require"
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

func mustUpsert(t testing.TB, change GCStateChange) GCState {
	state, ok := change.Upsert()
	require.True(t, ok)
	return state
}
