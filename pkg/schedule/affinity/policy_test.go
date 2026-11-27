// Copyright 2025 TiKV Project Authors.
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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/versioninfo/kerneltype"
)

func (m *Manager) applyStoreConditions(unavailableStores map[uint64]storeCondition) bool {
	m.RLock()
	generation := m.storeConditionsGeneration
	m.RUnlock()
	return m.applyStoreScan(unavailableStores, generation)
}

func TestStoreCondition(t *testing.T) {
	re := require.New(t)
	re.Equal(groupDegraded, storeLeaderEvicted.groupAvailability())
	re.Equal(groupDegraded, storeBusy.groupAvailability())
	re.Equal(groupDegraded, storeDisconnected.groupAvailability())
	re.Equal(groupDegraded, storeLowSpace.groupAvailability())
	re.Equal(groupExpired, storeDown.groupAvailability())
	re.Equal(groupExpired, storeRemoving.groupAvailability())
	re.Equal(groupExpired, storeRemoved.groupAvailability())

	re.True(storeLeaderEvicted.affectsLeaderOnly())
	re.True(storeBusy.affectsLeaderOnly())
	re.False(storeDisconnected.affectsLeaderOnly())
	re.False(storeDown.affectsLeaderOnly())
}

func TestCollectUnavailableStores(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	memoryStorage := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	conf := mockconfig.NewTestOptions()
	conf.SetLabelProperty("reject-leader", "reject", "leader")
	regionLabeler, err := labeler.NewRegionLabeler(ctx, memoryStorage, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, memoryStorage, storeInfos, conf, regionLabeler)
	re.NoError(err)

	stores := make([]*core.StoreInfo, 15)
	for i := range stores {
		stores[i] = core.NewStoreInfo(&metapb.Store{
			Id:        uint64(i),
			Address:   fmt.Sprintf("s%d", i),
			State:     metapb.StoreState_Up,
			NodeState: metapb.NodeState_Serving,
		}, core.SetLastHeartbeatTS(time.Now()))
	}
	// stores[0]: storeAvailable
	// stores[1..5]: storeEvicted
	stores[1] = stores[1].Clone(core.PauseLeaderTransfer(constant.In))
	stores[2] = stores[2].Clone(core.SetStoreLabels([]*metapb.StoreLabel{{Key: "reject", Value: "leader"}}))
	stores[3] = stores[3].Clone(core.SlowStoreEvicted())
	stores[4] = stores[4].Clone(core.StoppingStoreEvicted())
	stores[5] = stores[5].Clone(core.SlowTrendEvicted())
	// stores[6]: storeBusy
	busy := core.SetStoreStats(&pdpb.StoreStats{IsBusy: true})
	stores[6] = stores[6].Clone(busy)
	// stores[7]: storeDisconnected
	disconnected := core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Minute))
	stores[7] = stores[7].Clone(disconnected)
	// stores[8]: a healthy Preparing store is available.
	stores[8] = stores[8].Clone(core.SetNodeState(metapb.NodeState_Preparing))
	// stores[9]: storeDown
	stores[9] = stores[9].Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	// stores[10]: storeRemoving
	stores[10] = stores[10].Clone(core.SetStoreState(metapb.StoreState_Offline, false))
	// stores[11]: storeRemoved
	stores[11] = stores[11].Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	// stores[12..14]: the most severe degraded condition wins.
	stores[12] = stores[12].Clone(core.StoppingStoreEvicted(), busy)
	stores[13] = stores[13].Clone(core.StoppingStoreEvicted(), busy, disconnected)
	stores[14] = stores[14].Clone(core.StoppingStoreEvicted(), disconnected, core.SetStoreStats(&pdpb.StoreStats{
		Capacity: 100, Available: 1, IsBusy: true,
	}))

	for _, store := range stores {
		storeInfos.PutStore(store)
	}

	actual := manager.collectUnavailableStores()
	expected := map[uint64]storeCondition{
		1:  storeLeaderEvicted,
		2:  storeLeaderEvicted,
		3:  storeLeaderEvicted,
		4:  storeLeaderEvicted,
		5:  storeLeaderEvicted,
		6:  storeBusy,
		7:  storeDisconnected,
		9:  storeDown,
		10: storeRemoving,
		11: storeRemoved,
		12: storeBusy,
		13: storeDisconnected,
		14: storeLowSpace,
	}
	re.Equal(expected, actual)
}

func TestObserveAvailableRegion(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "s1", NodeState: metapb.NodeState_Preparing})
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2, Address: "s2", NodeState: metapb.NodeState_Serving})
	for _, s := range []*core.StoreInfo{store1, store2} {
		storeInfos.PutStore(s.Clone(core.SetLastHeartbeatTS(time.Now())))
	}
	conf := mockconfig.NewTestOptions()

	// Create region labeler
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	group := &Group{ID: "g", LeaderStoreID: 0, VoterStoreIDs: nil}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: group.ID}}))
	manager.checkGroupsAvailability()

	// First observation initializes the group on the healthy Preparing store.
	region1 := core.NewRegionInfo(
		&metapb.Region{
			Id:       10,
			StartKey: []byte(""),
			EndKey:   []byte("a"),
			Peers:    []*metapb.Peer{{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter}},
		},
		&metapb.Peer{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	manager.ObserveAvailableRegion(region1, manager.GetAffinityGroupState("g"))
	state := manager.GetAffinityGroupState("g")
	re.NotNil(state)
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(uint64(1), state.LeaderStoreID)
	re.ElementsMatch([]uint64{1}, state.VoterStoreIDs)

	// Second observation with different layout should not overwrite.
	region2 := core.NewRegionInfo(
		&metapb.Region{
			Id:       20,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
			Peers:    []*metapb.Peer{{Id: 21, StoreId: 2, Role: metapb.PeerRole_Voter}},
		},
		&metapb.Peer{Id: 21, StoreId: 2, Role: metapb.PeerRole_Voter},
	)
	manager.ObserveAvailableRegion(region2, manager.GetAffinityGroupState("g"))
	state = manager.GetAffinityGroupState("g")
	re.NotNil(state)
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(uint64(1), state.LeaderStoreID)
	re.ElementsMatch([]uint64{1}, state.VoterStoreIDs)

	// A degraded group must not change voterStoreIDs.
	manager.DegradeAffinityGroup("g")
	state = manager.GetAffinityGroupState("g")
	re.NotNil(state)
	re.False(state.AffinitySchedulingAllowed)
	re.False(state.RegularSchedulingAllowed)

	manager.ObserveAvailableRegion(region2, state) // region2 changes voter store IDs
	state = manager.GetAffinityGroupState("g")
	re.NotNil(state)
	re.False(state.AffinitySchedulingAllowed)
	re.False(state.RegularSchedulingAllowed)

	manager.ObserveAvailableRegion(region1, state) // region1 does not change voter store IDs
	state = manager.GetAffinityGroupState("g")
	re.NotNil(state)
	re.True(state.AffinitySchedulingAllowed)

	// An expired group can change voterStoreIDs.
	manager.ExpireAffinityGroup("g")
	state = manager.GetAffinityGroupState("g")
	re.NotNil(state)
	re.False(state.AffinitySchedulingAllowed)
	re.True(state.RegularSchedulingAllowed)

	manager.ObserveAvailableRegion(region2, state) // region2 changes voter store IDs
	state = manager.GetAffinityGroupState("g")
	re.NotNil(state)
	re.True(state.AffinitySchedulingAllowed)
}

func TestRangeConflictPreservesTarget(t *testing.T) {
	re := require.New(t)
	store, stores := storage.NewStorageWithMemoryBackend(), core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id}, core.SetLastHeartbeatTS(time.Now())))
	}
	manager := newTestAffinityManager(t, store, stores)
	overlap := testKeyRanges("b", "c")
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{
		{GroupID: "a", KeyRanges: testKeyRanges("a", "d")},
		{GroupID: "b"},
	}))
	oldVoters, newVoters := []uint64{1, 2, 3}, []uint64{2, 3, 4}
	beforeConflict, err := manager.UpdateAffinityGroupPeers("a", 1, oldVoters)
	re.NoError(err)

	rule := MakeLabelRule(GroupKeyRanges{GroupID: "b", KeyRanges: overlap})
	re.NoError(manager.regionLabeler.SetLabelRule(rule))
	re.NoError(manager.SyncKeyRangesFromEtcd(rule))
	manager.ReconcileRangeConflicts()
	conflicted := manager.GetAffinityGroupState("a")

	check := func(want Group) *GroupState {
		state := manager.GetAffinityGroupState("a")
		re.Equal(want, state.Group)
		saved, err := store.LoadAffinityGroup("a")
		re.NoError(err)
		expected, err := json.Marshal(want)
		re.NoError(err)
		re.Equal(string(expected), saved)
		return state
	}
	manager.ObserveAvailableRegion(generateRegionForTest(1, newVoters, overlap[0]), conflicted)
	re.True(check(beforeConflict.Group).rangeConflict)
	// The Region observation above covers the current state with new voters.
	_, err = manager.updateAffinityGroupPeersWithState("a", beforeConflict, 2, newVoters)
	re.NoError(err)
	re.True(check(beforeConflict.Group).rangeConflict)
	_, err = manager.updateAffinityGroupPeersWithState("a", conflicted, 2, oldVoters)
	re.NoError(err)
	re.True(check(beforeConflict.Group).rangeConflict)
	_, err = manager.UpdateAffinityGroupPeers("a", 2, newVoters)
	re.NoError(err)
	want := beforeConflict.Group
	want.LeaderStoreID, want.VoterStoreIDs = 2, newVoters
	re.True(check(want).rangeConflict)
	re.NoError(manager.regionLabeler.DeleteLabelRule(rule.ID))
	manager.SyncKeyRangesDeleteFromEtcd(rule.ID)
	manager.ReconcileRangeConflicts()
	state := check(want)
	re.True(state.AffinitySchedulingAllowed)
	re.False(state.RegularSchedulingAllowed)
}

func TestObserveAvailableRegionIgnoresRecreatedGroup(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, Address: fmt.Sprintf("s%d", id)},
			core.SetLastHeartbeatTS(time.Now())))
	}
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)

	const groupID = "recreated-group"
	ranges := createGroupForTest(re, manager, groupID, 1)
	original, err := manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 4})
	re.NoError(err)
	expired, ok := manager.ExpireAffinityGroupIf(original)
	re.True(ok)
	re.True(expired.RegularSchedulingAllowed)

	const adminLeaderStoreID uint64 = 2
	voters := []uint64{1, 2, 3}
	region := generateRegionForTest(1, voters, ranges[0])
	re.NoError(manager.DeleteAffinityGroups([]string{groupID}, true))
	createGroupForTest(re, manager, groupID, 1)
	_, err = manager.UpdateAffinityGroupPeers(groupID, adminLeaderStoreID, voters)
	re.NoError(err)
	// Updating the ranges brings the replacement to the same version as the expired snapshot.
	replacementRange := keyutil.KeyRange{
		StartKey: keyForTest(nonOverlappingRange.StartKey),
		EndKey:   keyForTest(nonOverlappingRange.EndKey),
	}
	re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{
		GroupID: groupID, KeyRanges: []keyutil.KeyRange{replacementRange},
	}}, nil))
	replacement := manager.GetAffinityGroupState(groupID)
	re.True(replacement.AffinitySchedulingAllowed)
	re.Equal(expired.affinityVer, replacement.affinityVer)
	re.NotSame(expired.groupInfoPtr, replacement.groupInfoPtr)

	manager.ObserveAvailableRegion(region, expired)
	re.Equal(adminLeaderStoreID, manager.GetAffinityGroupState(groupID).LeaderStoreID)
	persisted, err := store.LoadAffinityGroup(groupID)
	re.NoError(err)
	var group Group
	re.NoError(json.Unmarshal([]byte(persisted), &group))
	re.Equal(adminLeaderStoreID, group.LeaderStoreID)
}

func TestObserveAvailableRegionKeepsFixedTarget(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for _, id := range []uint64{1, 2} {
		s := core.NewStoreInfo(&metapb.Store{Id: id, Address: fmt.Sprintf("s%d", id), NodeState: metapb.NodeState_Serving})
		storeInfos.PutStore(s.Clone(core.SetLastHeartbeatTS(time.Now())))
	}
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))

	// Both Regions have voters on stores 1 and 2, with the leader on the given store.
	newRegion := func(id, leaderStoreID uint64) *core.RegionInfo {
		peers := []*metapb.Peer{
			{Id: id + 1, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: id + 2, StoreId: 2, Role: metapb.PeerRole_Voter},
		}
		return core.NewRegionInfo(&metapb.Region{Id: id, Peers: peers}, peers[leaderStoreID-1])
	}
	leaderOn1, leaderOn2 := newRegion(10, 1), newRegion(20, 2)
	observe := func(region *core.RegionInfo) *GroupState {
		manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState("g"))
		return manager.GetAffinityGroupState("g")
	}
	setPolicy := func(policy BalancePolicy) {
		_, err := manager.UpdateAffinityGroupPolicy("g", policy)
		re.NoError(err)
	}

	// A fixed group without a target takes the layout of the first Region.
	manager.checkGroupsAvailability()
	setPolicy(BalancePolicyFixed)
	state := observe(leaderOn1)
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(uint64(1), state.LeaderStoreID)

	// A degraded fixed group keeps its leader, and a Region with the same layout
	// confirms the target.
	manager.DegradeAffinityGroup("g")
	state = observe(leaderOn2)
	re.False(state.AffinitySchedulingAllowed)
	re.Equal(uint64(1), state.LeaderStoreID)
	state = observe(leaderOn1)
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(uint64(1), state.LeaderStoreID)

	// A degraded auto group takes the leader of the Region.
	setPolicy(BalancePolicyAuto)
	manager.DegradeAffinityGroup("g")
	state = observe(leaderOn2)
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(uint64(2), state.LeaderStoreID)

	// An expired fixed group takes a new target.
	setPolicy(BalancePolicyFixed)
	manager.ExpireAffinityGroup("g")
	state = observe(leaderOn1)
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(uint64(1), state.LeaderStoreID)

	// An observation of an auto group does not move the leader once the policy
	// is fixed. A policy update keeps the version that the observation carries.
	setPolicy(BalancePolicyAuto)
	manager.DegradeAffinityGroup("g")
	stale := manager.GetAffinityGroupState("g")
	setPolicy(BalancePolicyFixed)
	manager.ObserveAvailableRegion(leaderOn2, stale)
	state = manager.GetAffinityGroupState("g")
	re.False(state.AffinitySchedulingAllowed)
	re.Equal(uint64(1), state.LeaderStoreID)
}

func TestAvailabilityCheckInvalidatesGroup(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "s1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2, Address: "s2"})
	store2 = store2.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store2)

	conf := mockconfig.NewTestOptions()

	// Create region labeler
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	group := &Group{ID: "avail", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: group.ID}}))
	_, err = manager.UpdateAffinityGroupPeers("avail", 1, []uint64{1, 2})
	re.NoError(err)
	state := manager.GetAffinityGroupState("avail")
	re.NotNil(state)
	re.True(state.AffinitySchedulingAllowed)

	storeInfos.PutStore(store2.Clone(core.SetNodeState(metapb.NodeState_Removing)))
	manager.checkGroupsAvailability()

	state2 := manager.GetAffinityGroupState("avail")
	re.NotNil(state2)
	re.False(state2.AffinitySchedulingAllowed)
}

func TestStoreHealthCheck(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	// Create a test storage
	store := storage.NewStorageWithMemoryBackend()

	// Create mock stores
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1", NodeState: metapb.NodeState_Serving})
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2, Address: "test2", NodeState: metapb.NodeState_Serving})
	store3 := core.NewStoreInfo(&metapb.Store{Id: 3, Address: "test3", NodeState: metapb.NodeState_Serving})

	// Set store1 to be healthy
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	// Set store2 to be healthy
	store2 = store2.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store2)

	store3 = store3.Clone(core.SetLastHeartbeatTS(time.Now()), core.StoppingStoreEvicted())
	storeInfos.PutStore(store3)

	conf := mockconfig.NewTestOptions()

	// Create region labeler
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	// Create affinity manager
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a test affinity group with healthy stores
	group1 := &Group{
		ID:            "group1",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2},
	}
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: group1.ID}})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers(group1.ID, group1.LeaderStoreID, group1.VoterStoreIDs)
	re.NoError(err)

	// Create a test affinity group with store 3 as a follower.
	group2 := &Group{
		ID:            "group2",
		LeaderStoreID: 2,
		VoterStoreIDs: []uint64{2, 3},
	}
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: group2.ID}})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers(group2.ID, group2.LeaderStoreID, group2.VoterStoreIDs)
	re.NoError(err)

	// Verify initial state - all groups should be in effect
	groupInfo1 := manager.groups["group1"]
	re.True(groupInfo1.IsAffinitySchedulingAllowed())
	groupInfo2 := manager.groups["group2"]
	re.True(groupInfo2.IsAffinitySchedulingAllowed())

	// Disconnect store 3 after assigning it to the group.
	storeInfos.PutStore(store3.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Minute))))
	manager.checkGroupsAvailability()

	// After health check, group1 should still be in effect (all stores healthy)
	re.True(manager.groups["group1"].IsAffinitySchedulingAllowed())

	// After health check, group2 should be invalidated (store3 is unhealthy)
	re.False(manager.groups["group2"].IsAffinitySchedulingAllowed())

	// Now make store3 healthy again
	store3Healthy := store3.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store3Healthy)

	// Check health again
	manager.checkGroupsAvailability()

	// Group2 should be restored to available status
	re.True(manager.groups["group2"].IsAffinitySchedulingAllowed())

	// Set store3 to be down
	store3 = store3.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	storeInfos.PutStore(store3)

	// Check health again
	manager.checkGroupsAvailability()

	// After health check, group2 should be invalidated (store3 is unhealthy)
	re.False(manager.groups["group2"].IsAffinitySchedulingAllowed())

	// Now make store3 healthy again
	store3Healthy = store3.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store3Healthy)

	// Check health again
	manager.checkGroupsAvailability()

	// Group2 should not be restored from expired status
	re.False(manager.groups["group2"].IsAffinitySchedulingAllowed())

	// A store can recover while there are no groups to check.
	storeInfos.PutStore(store3)
	manager.checkGroupsAvailability()
	generation := manager.storeConditionsGeneration
	downStores := manager.collectUnavailableStores()
	re.NoError(manager.DeleteAffinityGroups([]string{group1.ID, group2.ID}, false))
	storeInfos.PutStore(store3Healthy)
	manager.checkGroupsAvailability()
	group2.ID = "recovered"
	manager.SyncGroupFromEtcdWithRevision(group2, 1)
	re.Equal(groupDegraded, getGroupForTest(re, manager, group2.ID).GetAvailability())
	re.False(manager.applyStoreScan(downStores, generation))
	manager.checkGroupsAvailability()
	re.Equal(groupAvailable, getGroupForTest(re, manager, group2.ID).GetAvailability())
}

func TestAvailabilityCheckWithUnchangedTombstone(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	now := time.Now()
	for _, id := range []uint64{1, 3, 4} {
		storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, NodeState: metapb.NodeState_Serving},
			core.SetLastHeartbeatTS(now)))
	}
	storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: 2}, core.SetStoreState(metapb.StoreState_Tombstone)))
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)

	// Cache the tombstone before syncing the group under test.
	manager.SyncGroupFromEtcd(&Group{ID: "seed"})
	manager.checkGroupsAvailability()
	manager.SyncGroupFromEtcd(&Group{ID: "g", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 3}})
	re.True(manager.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	re.Zero(getGroupForTest(re, manager, "g").degradedExpiredAt)
	re.False(manager.applyStoreConditions(manager.collectUnavailableStores()))

	manager.SyncGroupFromEtcd(&Group{ID: "g", LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 3}})
	state := manager.GetAffinityGroupState("g")
	re.True(state.AffinitySchedulingAllowed)
	re.Equal(uint64(3), state.LeaderStoreID)

	store4 := storeInfos.GetStore(4)
	storeInfos.PutStore(store4.Clone(core.SetLastHeartbeatTS(now.Add(-2 * time.Minute))))
	manager.checkGroupsAvailability()
	manager.SyncGroupFromEtcd(&Group{ID: "g", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 4}})
	state = manager.GetAffinityGroupState("g")
	re.False(state.AffinitySchedulingAllowed)
	re.False(state.RegularSchedulingAllowed)

	// Shorten the current window without expiring it.
	freshDeadline := newDegradedExpiredAtFromNow()
	manager.Lock()
	manager.groups["g"].degradedExpiredAt = freshDeadline - defaultDegradedExpirationSeconds/2
	manager.Unlock()
	manager.SyncGroupFromEtcd(&Group{ID: "g", LeaderStoreID: 3, VoterStoreIDs: []uint64{3, 4}})
	groupInfo := getGroupForTest(re, manager, "g")
	re.Equal(groupDegraded, groupInfo.GetAvailability())
	re.GreaterOrEqual(groupInfo.degradedExpiredAt, freshDeadline)

	storeInfos.PutStore(store4.Clone(core.SetLastHeartbeatTS(now.Add(-2 * time.Hour))))
	manager.checkGroupsAvailability()
	re.True(manager.GetAffinityGroupState("g").RegularSchedulingAllowed)

	storeInfos.PutStore(store4)
	manager.checkGroupsAvailability()
	re.True(manager.GetAffinityGroupState("g").RegularSchedulingAllowed)

	manager.SyncGroupFromEtcd(&Group{ID: "g", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 3}})
	state = manager.GetAffinityGroupState("g")
	re.True(state.AffinitySchedulingAllowed)
	re.False(state.RegularSchedulingAllowed)
	re.Zero(getGroupForTest(re, manager, "g").degradedExpiredAt)

	manager.SyncGroupFromEtcd(&Group{ID: "g", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}})
	groupInfo = getGroupForTest(re, manager, "g")
	re.Equal(groupExpired, groupInfo.GetAvailability())
	re.Zero(groupInfo.degradedExpiredAt)
	re.False(manager.applyStoreConditions(manager.collectUnavailableStores()))
}

func TestAvailabilityCheckWithoutGroups(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	healthy := core.NewStoreInfo(&metapb.Store{Id: 1, NodeState: metapb.NodeState_Serving},
		core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(healthy.Clone(core.PauseLeaderTransfer(constant.In)))
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)

	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "old"}}))
	manager.checkGroupsAvailability()
	re.Equal(float64(1), promtestutil.ToFloat64(groupCount))
	_, err = manager.UpdateAffinityGroupPeers("old", 1, []uint64{1})
	re.ErrorContains(err, "store 1 is leader-evicted")

	re.NoError(manager.DeleteAffinityGroups([]string{"old"}, false))
	storeInfos.PutStore(healthy)
	manager.checkGroupsAvailability()
	re.Zero(promtestutil.ToFloat64(groupCount))

	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "new"}}))
	_, err = manager.UpdateAffinityGroupPeers("new", 1, []uint64{1})
	re.NoError(err)

	re.NoError(manager.DeleteAffinityGroups([]string{"new"}, false))
	storeInfos.PutStore(healthy.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour))))
	manager.checkGroupsAvailability()
	manager.SyncGroupFromEtcd(&Group{ID: "synced", LeaderStoreID: 1, VoterStoreIDs: []uint64{1}})
	state := manager.GetAffinityGroupState("synced")
	re.NotNil(state)
	re.False(state.AffinitySchedulingAllowed)
	re.False(state.RegularSchedulingAllowed, "a target synced before the first store scan starts degraded")
	manager.checkGroupsAvailability()
	re.True(manager.GetAffinityGroupState("synced").RegularSchedulingAllowed)
	manager.checkGroupsAvailability()
	re.True(manager.GetAffinityGroupState("synced").RegularSchedulingAllowed)
}

// TestDegradedGroupShouldExpire verifies a degraded group should move to expired even when
// the set of unavailable stores does not change.
func TestDegradedGroupShouldExpire(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "s1", NodeState: metapb.NodeState_Serving})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2, Address: "s2", NodeState: metapb.NodeState_Serving})
	store2 = store2.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store2)

	conf := mockconfig.NewTestOptions()

	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a healthy group first.
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "expire"}}))
	_, err = manager.UpdateAffinityGroupPeers("expire", 1, []uint64{1, 2})
	re.NoError(err)
	manager.checkGroupsAvailability()
	groupInfo := getGroupForTest(re, manager, "expire")
	re.Equal(groupAvailable, groupInfo.GetAvailability())

	// Make store2 unhealthy so the group becomes degraded.
	store2Disconnected := store2.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Minute)))
	storeInfos.PutStore(store2Disconnected)
	manager.checkGroupsAvailability()
	groupInfo = getGroupForTest(re, manager, "expire")
	re.Equal(groupDegraded, groupInfo.GetAvailability())

	// Force the degraded status to be considered expired.
	manager.Lock()
	groupInfo.degradedExpiredAt = uint64(time.Now().Add(-time.Hour).Unix())
	manager.Unlock()

	// Run availability check again without changing the unavailable store set.
	manager.checkGroupsAvailability()
	re.True(groupInfo.IsExpired())
}

// TestGroupAvailabilityPriority verifies availability picks the strongest condition
// and respects leader-only constraints.
func TestGroupAvailabilityPriority(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := uint64(1); i <= 3; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: i, Address: "s"})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}
	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Case 1: leader-only constraint should degrade when on leader, ignored on followers.
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "leader-only"}}))
	_, err = manager.UpdateAffinityGroupPeers("leader-only", 1, []uint64{1, 2})
	re.NoError(err)

	// evict-leader on leader -> degraded
	unavailable := map[uint64]storeCondition{1: storeLeaderEvicted}
	re.True(manager.applyStoreConditions(unavailable))
	groupInfo := getGroupForTest(re, manager, "leader-only")
	re.Equal(groupDegraded, groupInfo.GetAvailability())

	// evict-leader only on follower should not change availability
	unavailable = map[uint64]storeCondition{2: storeLeaderEvicted}
	re.True(manager.applyStoreConditions(unavailable))
	groupInfo = getGroupForTest(re, manager, "leader-only")
	re.Equal(groupAvailable, groupInfo.GetAvailability())

	// Case 2: when multiple conditions exist, higher severity (expired) wins.
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "priority"}}))
	_, err = manager.UpdateAffinityGroupPeers("priority", 1, []uint64{1, 2})
	re.NoError(err)
	unavailable = map[uint64]storeCondition{
		1: storeDisconnected, // degraded
		2: storeRemoved,      // expired
	}
	re.True(manager.applyStoreConditions(unavailable))
	groupInfo = getGroupForTest(re, manager, "priority")
	re.Equal(groupExpired, groupInfo.GetAvailability())
}

func TestPolicyInheritanceScope(t *testing.T) {
	for _, pair := range [][2]string{
		{"custom-target", "custom-source"},
		{"custom-target", "_tidb_ks1_t_1"},
		{"_tidb_ks1_t_2", "custom-source"},
		{"_tidb_ks2_t_2", "_tidb_ks1_t_1"},
		{"_tidb_ks1_t_2", "_tidb_ks1_t_1"},
	} {
		t.Run(pair[1]+"-to-"+pair[0], func(t *testing.T) {
			re := require.New(t)
			test := newTargetTest(t)
			m, store := test.Manager, test.store
			re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: pair[1]}}))
			_, err := m.UpdateAffinityGroupPolicy(pair[1], BalancePolicyFixed)
			re.NoError(err)
			// Even ranges proving membership cannot authorize a custom ID.
			lower := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 1)
			upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 2)
			for _, ranges := range [][]keyutil.KeyRange{nil, {{StartKey: lower[:], EndKey: upper[:]}}} {
				err = m.CreateAffinityGroupsIfNotExists([]GroupKeyRanges{{GroupID: pair[0], InheritPolicyFrom: pair[1], KeyRanges: ranges}})
				if kerneltype.IsNextGen() && (pair[0] != "_tidb_ks1_t_2" || pair[1] != "_tidb_ks1_t_1") {
					re.ErrorIs(err, errs.ErrAffinityGroupContent)
					re.False(m.IsGroupExist(pair[0]))
					value, loadErr := store.LoadAffinityGroup(pair[0])
					re.NoError(loadErr)
					re.Empty(value)
				} else {
					re.NoError(err)
					re.Equal(BalancePolicyFixed, m.GetAffinityGroupState(pair[0]).BalancePolicy)
					re.Equal(BalancePolicyFixed, test.stored(pair[0]).BalancePolicy)
					re.NoError(m.DeleteAffinityGroups([]string{pair[0]}, true))
				}
			}
		})
	}
}

func TestPolicyInheritanceRejectsSameRequestSource(t *testing.T) {
	for _, skipExisting := range []bool{false, true} {
		for _, sourceIndex := range []int{0, 1, etcdutil.MaxEtcdTxnOps} {
			t.Run(fmt.Sprintf("skip-%t-source-%d", skipExisting, sourceIndex), func(t *testing.T) {
				re := require.New(t)
				test := newTargetTest(t)
				m, store := test.Manager, test.store
				const original = "_tidb_ks1_t_999"
				re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: original}}))
				_, err := m.UpdateAffinityGroupPolicy(original, BalancePolicyFixed)
				re.NoError(err)
				changes := make([]GroupKeyRanges, etcdutil.MaxEtcdTxnOps+2)
				for i := range changes {
					changes[i].GroupID = fmt.Sprintf("_tidb_ks1_t_%d", i+1)
				}
				source := changes[sourceIndex].GroupID
				changes[sourceIndex].InheritPolicyFrom = original
				// Source after destination, same chunk, cross-chunk and multi-hop.
				changes[0].InheritPolicyFrom = source
				changes[len(changes)-1].InheritPolicyFrom = source
				if skipExisting {
					re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: source}}))
				}
				_, err = m.createGroupsWithSkipExisting(t.Context(), changes, skipExisting, false)
				re.ErrorIs(err, errs.ErrAffinityGroupContent)
				for _, change := range changes {
					if skipExisting && change.GroupID == source {
						continue
					}
					re.False(m.IsGroupExist(change.GroupID))
					value, loadErr := store.LoadAffinityGroup(change.GroupID)
					re.NoError(loadErr)
					re.Empty(value, "validation must precede all chunk writes")
				}
			})
		}
	}
}
