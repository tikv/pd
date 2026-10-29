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

package affinity

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func newTargetTestManager(t *testing.T) (*Manager, storage.Storage) {
	store := storage.NewStorageWithMemoryBackend()
	return newTargetTestManagerWithStorage(t, store), store
}

func newTargetTestManagerWithStorage(t *testing.T, store storage.Storage) *Manager {
	re := require.New(t)
	ctx := t.Context()
	storeInfos := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: id, Address: fmt.Sprintf("s%d", id), NodeState: metapb.NodeState_Serving})
		storeInfos.PutStore(storeInfo.Clone(core.SetLastHeartbeatTS(time.Now())))
	}
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	return manager
}

func loadStoredGroup(re *require.Assertions, store storage.Storage, id string) *Group {
	value, err := store.LoadAffinityGroup(id)
	re.NoError(err)
	re.NotEmpty(value)
	group := &Group{}
	re.NoError(json.Unmarshal([]byte(value), group))
	return group
}

func targetFor(m *Manager, id string, leader uint64, voters ...uint64) GroupTarget {
	return GroupTarget{Expected: m.GetAffinityGroupState(id).Group, LeaderStoreID: leader, VoterStoreIDs: voters}
}

func TestUpdateAffinityGroupTargets(t *testing.T) {
	re := require.New(t)
	manager, store := newTargetTestManager(t)
	ids := []string{"stale"}
	for i := range etcdutil.MaxEtcdTxnOps {
		ids = append(ids, fmt.Sprintf("g%03d", i))
	}
	groups := make([]GroupKeyRanges, 0, len(ids))
	for _, id := range ids {
		groups = append(groups, GroupKeyRanges{GroupID: id})
	}
	re.NoError(manager.CreateAffinityGroups(groups))

	// The targets take two transactions. A target decided from another value of
	// its group, or for a missing group, is not written.
	targets := []GroupTarget{{Expected: Group{ID: "missing"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}}
	for _, id := range ids {
		target := targetFor(manager, id, 2, 3, 2, 1)
		if id == "stale" {
			target.Expected.CreateTimestamp++
		}
		targets = append(targets, target)
	}
	written, err := manager.UpdateAffinityGroupTargets(targets)
	re.NoError(err)
	re.Equal(ids[1:], written)
	for _, id := range ids[1:] {
		state := manager.GetAffinityGroupState(id)
		re.True(state.AffinitySchedulingAllowed)
		re.Equal([]uint64{1, 2, 3}, state.VoterStoreIDs)
		re.Equal(state.Group, *loadStoredGroup(re, store, id))
	}
	re.Zero(manager.GetAffinityGroupState("stale").LeaderStoreID)
	re.Zero(loadStoredGroup(re, store, "stale").LeaderStoreID)

	// A commit that the Manager did not see leaves its memory stale. A target
	// decided from the stale memory is not written, and the memory is refreshed
	// with the availability that the store conditions known now give.
	id := ids[1]
	committed := manager.GetAffinityGroupState(id).Group
	committed.LeaderStoreID = 1
	re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, id, &committed) }))
	manager.Lock()
	manager.unavailableStores = map[uint64]storeCondition{1: storeLeaderEvicted}
	manager.Unlock()
	written, err = manager.UpdateAffinityGroupTargets([]GroupTarget{targetFor(manager, id, 3, 1, 2, 3)})
	re.NoError(err)
	re.Empty(written)
	re.Equal(committed, manager.GetAffinityGroupState(id).Group)
	re.False(manager.GetAffinityGroupState(id).AffinitySchedulingAllowed)
	manager.Lock()
	manager.unavailableStores = map[uint64]storeCondition{}
	manager.Unlock()

	// The next target is decided from the refreshed group, whose voter order does not matter.
	target := targetFor(manager, id, 3, 1, 2, 3)
	slices.Reverse(target.Expected.VoterStoreIDs)
	written, err = manager.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.NoError(err)
	re.Equal([]string{id}, written)
	re.Equal(uint64(3), loadStoredGroup(re, store, id).LeaderStoreID)
}

// TestSyncRecreatedAffinityGroup checks that a group deleted and created again
// with the same ID replaces the old instance, both when the scheduling service
// reloads it and when a conflicting write refreshes it. The new instance keeps
// the old target, so only its create timestamp tells them apart.
func TestSyncRecreatedAffinityGroup(t *testing.T) {
	for _, reload := range []bool{true, false} {
		t.Run(fmt.Sprintf("reload=%t", reload), func(t *testing.T) {
			re := require.New(t)
			manager, store := newTargetTestManager(t)
			old := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
			manager.SyncGroupFromEtcd(old)
			ranges := GroupKeyRanges{GroupID: old.ID, KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}
			re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{ranges}, nil))
			manager.RestoreAffinityGroup(old.ID)
			_, isAffinity := manager.GetAndCacheRegionAffinityGroupState(generateRegionForTest(10, old.VoterStoreIDs, ranges.KeyRanges[0]))
			re.True(isAffinity)

			recreated := *old
			recreated.CreateTimestamp = 2
			re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, old.ID, &recreated) }))
			if reload {
				manager.SyncGroupFromEtcd(&recreated)
			} else {
				written, err := manager.UpdateAffinityGroupTargets([]GroupTarget{targetFor(manager, old.ID, 2, 1, 2, 3)})
				re.NoError(err)
				re.Empty(written)
			}
			state := manager.GetAffinityGroupState(old.ID)
			re.Equal(recreated, state.Group)
			re.Zero(state.RegionCount)
			re.Equal(1, state.RangeCount)
			written, err := manager.UpdateAffinityGroupTargets([]GroupTarget{targetFor(manager, old.ID, 2, 1, 2, 3)})
			re.NoError(err)
			re.Equal([]string{old.ID}, written)
		})
	}
}

func TestUpdateAffinityGroupTargetsRejectsInvalidInput(t *testing.T) {
	re := require.New(t)
	manager, store := newTargetTestManager(t)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
	_, err := manager.UpdateAffinityGroupTargets(nil)
	re.True(errs.ErrEmptyRequest.Equal(err))

	manager.Lock()
	manager.unavailableStores = map[uint64]storeCondition{4: storeDown}
	manager.Unlock()
	for _, targets := range [][]GroupTarget{
		{targetFor(manager, "g", 1, 1, 2), targetFor(manager, "g", 2, 1, 2)},
		{targetFor(manager, "g", 3, 1, 2)},
		{targetFor(manager, "g", 1, 1, 1, 2)},
		{targetFor(manager, "g", 0, 1, 2)},
		{targetFor(manager, "g", 9, 9)},
		{targetFor(manager, "g", 1, 1, 4)},
		{targetFor(manager, "h", 1, 1, 2), {Expected: Group{ID: "bad id"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}},
	} {
		_, err := manager.UpdateAffinityGroupTargets(targets)
		re.Error(err)
	}
	// An invalid request writes nothing.
	for _, id := range []string{"g", "h"} {
		re.Zero(manager.GetAffinityGroupState(id).LeaderStoreID)
		re.Zero(loadStoredGroup(re, store, id).LeaderStoreID)
	}
}

type fakeTargetWriter struct {
	targets []GroupTarget
	// write runs before the reply, like the watcher delivering a change.
	write func(GroupTarget)
	reply func(GroupTarget) ([]string, error)
}

func (w *fakeTargetWriter) WriteGroupTargets(_ context.Context, targets []GroupTarget) ([]string, error) {
	w.targets = append(w.targets, targets...)
	if w.write != nil {
		w.write(targets[0])
	}
	return w.reply(targets[0])
}

func TestObserveAvailableRegionWithTargetWriter(t *testing.T) {
	region := core.NewRegionInfo(
		&metapb.Region{Id: 10, Peers: []*metapb.Peer{{Id: 11, StoreId: 2, Role: metapb.PeerRole_Voter}, {Id: 12, StoreId: 1, Role: metapb.PeerRole_Voter}}},
		&metapb.Peer{Id: 12, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	written := func(target GroupTarget) ([]string, error) { return []string{target.Expected.ID}, nil }
	for _, tc := range []struct {
		name string
		// apply is the change that the watcher delivers before the reply, if any.
		apply     func(target GroupTarget) *Group
		reply     func(GroupTarget) ([]string, error)
		available bool
		leader    uint64
	}{
		{name: "written", reply: written, available: true, leader: 1},
		{name: "not written", reply: func(GroupTarget) ([]string, error) { return nil, nil }},
		{name: "write failed", reply: func(GroupTarget) ([]string, error) { return nil, errors.New("PD is unavailable") }},
		{name: "watcher first", apply: func(target GroupTarget) *Group { return target.result(&target.Expected) }, reply: written, available: true, leader: 1},
		// A target that the watcher delivers is used, while the written one is not published.
		{name: "changed meanwhile", apply: func(target GroupTarget) *Group {
			other := target.result(&target.Expected)
			other.LeaderStoreID, other.VoterStoreIDs = 3, []uint64{3, 4}
			return other
		}, reply: written, available: true, leader: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			manager, store := newTargetTestManager(t)
			re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
			group := manager.GetAffinityGroupState("g")
			writer := &fakeTargetWriter{reply: tc.reply}
			if tc.apply != nil {
				writer.write = func(target GroupTarget) { manager.SyncGroupFromEtcd(tc.apply(target)) }
			}
			manager.SetTargetWriter(writer)

			manager.ObserveAvailableRegion(region, group)
			re.Equal([]GroupTarget{{Expected: group.Group, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}}, writer.targets)
			state := manager.GetAffinityGroupState("g")
			re.Equal(tc.available, state.AffinitySchedulingAllowed)
			re.Equal(tc.leader, state.LeaderStoreID)
			// The writer stores the target; the Manager does not write its own storage.
			re.Zero(loadStoredGroup(re, store, "g").LeaderStoreID)
		})
	}
}

// TestTargetAvailability checks that a group takes the availability of its target
// from the store conditions known when the target is set, because the
// availability check applies the conditions only when they change.
func TestTargetAvailability(t *testing.T) {
	allowed := func(m *Manager, id string) bool { return m.GetAffinityGroupState(id).AffinitySchedulingAllowed }

	t.Run("loaded", func(t *testing.T) {
		re := require.New(t)
		store := storage.NewStorageWithMemoryBackend()
		group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
		re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, group.ID, group) }))
		manager := newTargetTestManagerWithStorage(t, store)
		re.False(allowed(manager, "g"))
		// The first check sets the loaded group, although no store condition changed.
		manager.checkGroupsAvailability()
		re.True(allowed(manager, "g"))
	})

	t.Run("synced", func(t *testing.T) {
		re := require.New(t)
		manager, _ := newTargetTestManager(t)
		manager.applyStoreConditions(map[uint64]storeCondition{})
		group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
		manager.SyncGroupFromEtcd(group)
		re.True(allowed(manager, "g"))

		// An admin target that reaches an expired group through the watcher is used.
		manager.ExpireAffinityGroup("g")
		group.VoterStoreIDs = []uint64{1, 2, 4}
		manager.SyncGroupFromEtcd(group)
		re.True(allowed(manager, "g"))

		// A target synced with an unavailable store is not.
		manager.applyStoreConditions(map[uint64]storeCondition{3: storeRemoving})
		group.VoterStoreIDs = []uint64{1, 2, 3}
		manager.SyncGroupFromEtcd(group)
		re.False(allowed(manager, "g"))
		re.True(manager.GetAffinityGroupState("g").RegularSchedulingAllowed)
		recreated := &Group{ID: "g", CreateTimestamp: 2, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 4}}
		manager.SyncGroupFromEtcd(recreated)
		re.True(allowed(manager, "g"))
	})

	t.Run("written", func(t *testing.T) {
		re := require.New(t)
		manager, _ := newTargetTestManager(t)
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		// Store 2 starts to be removed while the target is written.
		manager.SetTargetWriter(&fakeTargetWriter{
			write: func(GroupTarget) { manager.applyStoreConditions(map[uint64]storeCondition{2: storeRemoving}) },
			reply: func(target GroupTarget) ([]string, error) { return []string{target.Expected.ID}, nil },
		})
		region := core.NewRegionInfo(
			&metapb.Region{Id: 10, Peers: []*metapb.Peer{{Id: 11, StoreId: 2, Role: metapb.PeerRole_Voter}, {Id: 12, StoreId: 1, Role: metapb.PeerRole_Voter}}},
			&metapb.Peer{Id: 12, StoreId: 1, Role: metapb.PeerRole_Voter},
		)
		manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState("g"))
		state := manager.GetAffinityGroupState("g")
		re.Equal([]uint64{1, 2}, state.VoterStoreIDs)
		re.False(state.AffinitySchedulingAllowed)
		re.True(state.RegularSchedulingAllowed)
	})
}
