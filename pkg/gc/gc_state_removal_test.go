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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestGCStateWatcherRemovesDisabledKeyspace(t *testing.T) {
	var client *clientv3.Client
	_, _, manager, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{
		useEnabledKeyspaceIndex: true,
		beforeLeader:            func(c *clientv3.Client) { client = c },
	})
	defer clean()
	defer cancel()
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	index := manager.generation.index
	require.NoError(t, index.waitReady(ctx))
	select {
	case <-manager.generation.warmup.done:
	case <-ctx.Done():
		t.Fatal("initial warmup did not finish")
	}
	w, err := manager.WatchGCStates(ctx, true)
	require.NoError(t, err)
	defer w.Close()

	_, err = manager.keyspaceManager.UpdateKeyspaceState("ks2", keyspacepb.KeyspaceState_DISABLED, time.Now().Unix())
	require.NoError(t, err)
	committed, err := client.Get(ctx, keypath.KeyspaceMetaPath(2))
	require.NoError(t, err)
	entries, _, err := index.snapshotAtLeast(ctx, committed.Header.Revision)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotEqual(t, uint32(2), entry.id)
	}

	changes, err := w.RecvBatch(1)
	require.NoError(t, err, "a keyspace leaving the enabled index must notify GC watchers")
	require.Len(t, changes, 1)
	id, removed := changes[0].RemovedKeyspaceID()
	require.True(t, removed)
	require.Equal(t, uint32(2), id)

	// Later non-enabled updates, including a keyspace absent from the initial
	// index, must not repeat removals. Deleting another indexed keyspace gives
	// this watcher a second event and a barrier for checking the whole batch.
	for _, update := range []struct {
		name  string
		state keyspacepb.KeyspaceState
	}{
		{"ks2", keyspacepb.KeyspaceState_DISABLED},
		{"ks2", keyspacepb.KeyspaceState_ARCHIVED},
		{"ks4", keyspacepb.KeyspaceState_ARCHIVED},
	} {
		_, err = manager.keyspaceManager.UpdateKeyspaceState(update.name, update.state, time.Now().Unix())
		require.NoError(t, err)
	}
	_, err = client.Delete(ctx, keypath.KeyspaceMetaPath(1))
	require.NoError(t, err)
	changes, err = w.RecvBatch(1)
	require.NoError(t, err)
	id, removed = changes[0].RemovedKeyspaceID()
	require.True(t, removed)
	require.Equal(t, uint32(1), id)
	manager.mu.Lock()
	queued := len(w.liveCh)
	manager.mu.Unlock()
	require.Zero(t, queued, "non-enabled updates must not repeat removals")
}

func TestEnabledKeyspaceIndexRemovalCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	index := newEnabledKeyspaceIndex(nil, "")
	var removed []uint32
	index.onRemoved = func(ids []uint32) {
		// Reentrant reads must see the complete batch without deadlocking.
		entries, _, err := index.snapshotAtLeast(ctx, 0)
		if err != nil {
			panic(err)
		}
		for _, entry := range entries {
			for _, id := range ids {
				if entry.id == id {
					panic("removed entry is still in the index")
				}
			}
		}
		removed = append(removed, ids...)
	}
	require.True(t, index.publish(ctx, map[uint32]enabledKeyspace{1: {id: 1}, 2: {id: 2}}, 1))
	for n, tc := range []struct {
		name    string
		changes map[uint32]*enabledKeyspace
		want    []uint32
	}{
		{"leaves enabled", map[uint32]*enabledKeyspace{2: nil}, []uint32{2}},
		{"already absent", map[uint32]*enabledKeyspace{2: nil, 4: nil}, nil},
		{"coalesced final enabled", map[uint32]*enabledKeyspace{1: {id: 1}, 2: {id: 2}}, nil},
		{"leaves enabled again", map[uint32]*enabledKeyspace{2: nil}, []uint32{2}},
	} {
		if !t.Run(tc.name, func(t *testing.T) {
			removed = nil
			done := make(chan struct{})
			go func() {
				index.publishProgress(ctx, tc.changes, int64(n+2))
				close(done)
			}()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("removal callback could not read the index")
			}
			require.Equal(t, tc.want, removed)
		}) {
			return
		}
	}
	// Recovery reloads replace the snapshot without synthesizing removals.
	removed = nil
	require.True(t, index.publish(ctx, map[uint32]enabledKeyspace{}, 6))
	require.Empty(t, removed)
}

func TestGCStateWatcherIgnoresOldRemovalCallback(t *testing.T) {
	_, _, manager, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{useEnabledKeyspaceIndex: true})
	defer clean()
	defer cancel()
	oldCallback := manager.generation.index.onRemoved
	require.NotNil(t, oldCallback)
	stop := manager.OnNodeBecomesLeader()
	defer stop()
	w, err := manager.WatchGCStates(context.Background(), true)
	require.NoError(t, err)
	defer w.Close()

	oldCallback([]uint32{2})
	require.Empty(t, w.liveCh)
	require.NoError(t, w.Err())
}

func TestGCStateWatcherBulkRemovalsKeepBackpressure(t *testing.T) {
	_, _, manager, clean, cancel := newGCStateManagerForTest(t, newGCStateManagerForTestOptions{})
	defer clean()
	defer cancel()
	slow, err := manager.registerGCStateWatcher(context.Background(), true, gcStateWatchConfig{liveChannelCapacity: 2})
	require.NoError(t, err)
	defer slow.Close()
	const count = 128
	healthy, err := manager.registerGCStateWatcher(context.Background(), true, gcStateWatchConfig{liveChannelCapacity: count})
	require.NoError(t, err)
	defer healthy.Close()
	index := newEnabledKeyspaceIndex(nil, "")
	generation := manager.generation
	index.onRemoved = func(ids []uint32) { manager.publishGCStateRemovals(generation, ids) }
	entries := make(map[uint32]enabledKeyspace, count)
	changes := make(map[uint32]*enabledKeyspace, count)
	want := make([]uint32, 0, count)
	for id := uint32(100); id < 100+count; id++ {
		entries[id] = enabledKeyspace{id: id}
		changes[id] = nil
		want = append(want, id)
	}
	require.True(t, index.publish(context.Background(), entries, 1))
	index.publishProgress(context.Background(), changes, 2)
	require.ErrorIs(t, slow.Err(), errs.ErrGCStateWatcherSlowConsumer)
	require.NotContains(t, manager.watchers, slow.id)
	require.NoError(t, healthy.Err())
	require.Len(t, healthy.liveCh, count)
	batch, err := healthy.RecvBatch(count)
	require.NoError(t, err)
	got := make([]uint32, 0, len(batch))
	for _, change := range batch {
		id, removed := change.RemovedKeyspaceID()
		require.True(t, removed)
		got = append(got, id)
	}
	require.ElementsMatch(t, want, got)
}
