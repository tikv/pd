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
)

func newTargetTestManager(t *testing.T) (*Manager, storage.Storage) {
	re := require.New(t)
	ctx := t.Context()
	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: id, Address: fmt.Sprintf("s%d", id), NodeState: metapb.NodeState_Serving})
		storeInfos.PutStore(storeInfo.Clone(core.SetLastHeartbeatTS(time.Now())))
	}
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	return manager, store
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
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "a"}, {GroupID: "b"}}))

	stale := targetFor(manager, "b", 1, 1, 2, 3)
	stale.Expected.CreateTimestamp++
	missing := GroupTarget{Expected: Group{ID: "missing"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	written, err := manager.UpdateAffinityGroupTargets([]GroupTarget{targetFor(manager, "a", 2, 3, 2, 1), stale, missing})
	re.NoError(err)
	re.Equal([]string{"a"}, written)

	a := manager.GetAffinityGroupState("a")
	re.True(a.AffinitySchedulingAllowed)
	re.Equal(uint64(2), a.LeaderStoreID)
	re.Equal([]uint64{1, 2, 3}, a.VoterStoreIDs)
	stored := loadStoredGroup(re, store, "a")
	re.Equal(uint64(2), stored.LeaderStoreID)
	re.Equal([]uint64{1, 2, 3}, stored.VoterStoreIDs)
	re.Equal(a.CreateTimestamp, stored.CreateTimestamp)

	b := manager.GetAffinityGroupState("b")
	re.False(b.AffinitySchedulingAllowed)
	re.Zero(b.LeaderStoreID)
	re.Zero(loadStoredGroup(re, store, "b").LeaderStoreID)

	// A target decided from the old value of a is not written.
	old := GroupTarget{Expected: Group{ID: "a", CreateTimestamp: a.CreateTimestamp}, LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 2, 3}}
	written, err = manager.UpdateAffinityGroupTargets([]GroupTarget{old})
	re.NoError(err)
	re.Empty(written)
	re.Equal(uint64(2), manager.GetAffinityGroupState("a").LeaderStoreID)
	re.Equal(uint64(2), loadStoredGroup(re, store, "a").LeaderStoreID)

	// The expected group is compared regardless of voter order.
	expected := manager.GetAffinityGroupState("a").Group
	expected.VoterStoreIDs = []uint64{3, 1, 2}
	written, err = manager.UpdateAffinityGroupTargets([]GroupTarget{{Expected: expected, LeaderStoreID: 4, VoterStoreIDs: []uint64{4, 2, 1}}})
	re.NoError(err)
	re.Equal([]string{"a"}, written)
	re.Equal([]uint64{1, 2, 4}, loadStoredGroup(re, store, "a").VoterStoreIDs)
}

func TestUpdateAffinityGroupTargetsRejectsInvalidInput(t *testing.T) {
	re := require.New(t)
	manager, store := newTargetTestManager(t)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))

	_, err := manager.UpdateAffinityGroupTargets(nil)
	re.True(errs.ErrEmptyRequest.Equal(err))

	for _, targets := range [][]GroupTarget{
		{targetFor(manager, "g", 1, 1, 2), targetFor(manager, "g", 2, 1, 2)},
		{targetFor(manager, "g", 3, 1, 2)},
		{targetFor(manager, "g", 1, 1, 1, 2)},
		{targetFor(manager, "g", 0, 1, 2)},
		{targetFor(manager, "g", 9, 9)},
		{targetFor(manager, "h", 1, 1, 2), {Expected: Group{ID: "bad id"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}},
	} {
		_, err := manager.UpdateAffinityGroupTargets(targets)
		re.Error(err)
	}

	manager.Lock()
	manager.unavailableStores = map[uint64]storeCondition{2: storeDown}
	manager.Unlock()
	_, err = manager.UpdateAffinityGroupTargets([]GroupTarget{targetFor(manager, "h", 1, 1, 3), targetFor(manager, "g", 1, 1, 2)})
	re.True(errs.ErrAffinityGroupContent.Equal(err))

	// An invalid request writes nothing.
	for _, id := range []string{"g", "h"} {
		re.Zero(manager.GetAffinityGroupState(id).LeaderStoreID)
		re.Zero(loadStoredGroup(re, store, id).LeaderStoreID)
	}
}

// TestUpdateAffinityGroupTargetsRefreshesMemory covers a transaction that was
// committed although the Manager did not see the result.
func TestUpdateAffinityGroupTargetsRefreshesMemory(t *testing.T) {
	re := require.New(t)
	manager, store := newTargetTestManager(t)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	committed := manager.GetAffinityGroupState("g").Group
	committed.LeaderStoreID = 1
	committed.VoterStoreIDs = []uint64{1, 2, 3}
	re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return store.SaveAffinityGroup(txn, "g", &committed)
	}))

	// A target decided from the stale memory is not written, and the memory is refreshed.
	written, err := manager.UpdateAffinityGroupTargets([]GroupTarget{targetFor(manager, "g", 4, 2, 3, 4)})
	re.NoError(err)
	re.Empty(written)
	state := manager.GetAffinityGroupState("g")
	re.Equal(uint64(1), state.LeaderStoreID)
	re.Equal([]uint64{1, 2, 3}, state.VoterStoreIDs)

	// The next target is decided from the refreshed value and written.
	written, err = manager.UpdateAffinityGroupTargets([]GroupTarget{targetFor(manager, "g", 4, 2, 3, 4)})
	re.NoError(err)
	re.Equal([]string{"g"}, written)
	re.Equal(uint64(4), loadStoredGroup(re, store, "g").LeaderStoreID)
}

func TestUpdateAffinityGroupTargetsInChunks(t *testing.T) {
	re := require.New(t)
	manager, store := newTargetTestManager(t)
	const count = 250
	groups := make([]GroupKeyRanges, 0, count)
	for i := range count {
		groups = append(groups, GroupKeyRanges{GroupID: fmt.Sprintf("g%03d", i)})
	}
	re.NoError(manager.CreateAffinityGroups(groups))
	targets := make([]GroupTarget, 0, count)
	for _, group := range groups {
		targets = append(targets, targetFor(manager, group.GroupID, 1, 1, 2, 3))
	}

	written, err := manager.UpdateAffinityGroupTargets(targets)
	re.NoError(err)
	re.Len(written, count)
	for _, group := range groups {
		re.Equal(uint64(1), loadStoredGroup(re, store, group.GroupID).LeaderStoreID)
		re.True(manager.GetAffinityGroupState(group.GroupID).AffinitySchedulingAllowed)
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
	notWritten := func(GroupTarget) ([]string, error) { return nil, nil }
	failed := func(GroupTarget) ([]string, error) { return nil, errors.New("PD is unavailable") }

	for _, tc := range []struct {
		name      string
		write     func(m *Manager) func(GroupTarget)
		reply     func(GroupTarget) ([]string, error)
		available bool
		leader    uint64
	}{
		{name: "written", reply: written, available: true, leader: 1},
		{name: "not written", reply: notWritten},
		{name: "write failed", reply: failed},
		{
			// The watcher applies the written target before the reply arrives.
			name: "watcher first",
			write: func(m *Manager) func(GroupTarget) {
				return func(target GroupTarget) { m.SyncGroupFromEtcd(target.result(&target.Expected)) }
			},
			reply: written, available: true, leader: 1,
		},
		{
			// Another target reaches the group before the reply arrives.
			name: "changed meanwhile",
			write: func(m *Manager) func(GroupTarget) {
				return func(target GroupTarget) {
					other := target.result(&target.Expected)
					other.LeaderStoreID, other.VoterStoreIDs = 3, []uint64{3, 4}
					m.SyncGroupFromEtcd(other)
				}
			},
			reply: written, leader: 3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			manager, store := newTargetTestManager(t)
			re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
			group := manager.GetAffinityGroupState("g")
			writer := &fakeTargetWriter{reply: tc.reply}
			if tc.write != nil {
				writer.write = tc.write(manager)
			}
			manager.SetTargetWriter(writer)

			manager.ObserveAvailableRegion(region, group)
			re.Len(writer.targets, 1)
			re.Equal(GroupTarget{Expected: group.Group, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}, writer.targets[0])
			state := manager.GetAffinityGroupState("g")
			re.Equal(tc.available, state.AffinitySchedulingAllowed)
			re.Equal(tc.leader, state.LeaderStoreID)
			// The writer stores the target; the Manager does not write its own storage.
			re.Zero(loadStoredGroup(re, store, "g").LeaderStoreID)
		})
	}
}
