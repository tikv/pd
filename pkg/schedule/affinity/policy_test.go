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

	stores := make([]*core.StoreInfo, 14)
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
	stores[6] = stores[6].Clone(core.SetStoreStats(&pdpb.StoreStats{IsBusy: true}))
	// stores[7]: storeDisconnected
	stores[7] = stores[7].Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Minute)))
	// stores[8]: Preparing is available
	stores[8] = stores[8].Clone(core.SetNodeState(metapb.NodeState_Preparing))
	// stores[9]: storeDown
	stores[9] = stores[9].Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	// stores[10]: storeRemoving
	stores[10] = stores[10].Clone(core.SetStoreState(metapb.StoreState_Offline, false))
	// stores[11]: storeRemoved
	stores[11] = stores[11].Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	// stores[12..13]: a store in several conditions takes the most severe one.
	stores[12] = stores[12].Clone(core.StoppingStoreEvicted(), core.SetLastHeartbeatTS(time.Now().Add(-2*time.Minute)))
	stores[13] = stores[13].Clone(core.SetStoreStats(&pdpb.StoreStats{IsBusy: true}), core.SetLastHeartbeatTS(time.Now().Add(-2*time.Minute)))

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
		12: storeDisconnected,
		13: storeDisconnected,
	}
	re.Equal(expected, actual)
	manager.SyncGroupFromEtcd(&Group{ID: "severity", LeaderStoreID: 8, VoterStoreIDs: []uint64{8, 12}})
	manager.applyStoreConditions(actual)
	re.Equal(groupDegraded, getGroupForTest(re, manager, "severity").GetAvailability())
}

func TestObserveAvailableRegion(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "s1", NodeState: metapb.NodeState_Serving})
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

	// First observation makes group available with store 1.
	manager.CheckGroupsAvailability()
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

func TestObserveAvailableRegionKeepsFixedTarget(t *testing.T) {
	re := require.New(t)
	manager, store := newTargetTestManager(t)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	leaderOn1 := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
	leaderOn2 := generateRegionForTest(20, []uint64{2, 1}, nonOverlappingRange)
	observe := func(region *core.RegionInfo) *GroupState {
		manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState("g"))
		return manager.GetAffinityGroupState("g")
	}
	setPolicy := func(policy BalancePolicy) {
		_, err := manager.UpdateAffinityGroupPolicy("g", policy)
		re.NoError(err)
	}

	// A fixed group without a target takes the layout of the first Region.
	manager.CheckGroupsAvailability()
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

	// Reloaded targets wait for the first health check under either policy.
	for i, policy := range []BalancePolicy{BalancePolicyFixed, BalancePolicyAuto} {
		setPolicy(policy)
		loaded := newTargetTestManagerWithStorage(t, t.Context(), store)
		observeLoaded := func() uint64 {
			loaded.ObserveAvailableRegion(leaderOn2, loaded.GetAffinityGroupState("g"))
			leader := loaded.GetAffinityGroupState("g").LeaderStoreID
			re.Equal(leader, loadStoredGroup(re, store, "g").LeaderStoreID)
			return leader
		}
		re.False(loaded.GetAffinityGroupState("g").AffinitySchedulingAllowed)
		re.Equal(uint64(1), observeLoaded())
		loaded.applyStoreConditions(map[uint64]storeCondition{1: storeBusy})
		re.Equal(uint64(i+1), observeLoaded())
	}
}

func TestRangeConflictPreservesTarget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		observe bool
		admin   bool
		voters  []uint64
	}{
		{name: "observe", observe: true, voters: []uint64{2, 3, 4}},
		{name: "versioned-update", voters: []uint64{2, 3, 4}},
		{name: "leader-only-update", voters: []uint64{1, 2, 3}},
		{name: "admin-update", admin: true, voters: []uint64{2, 3, 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			ctx := t.Context()
			store := storage.NewStorageWithMemoryBackend()
			storeInfos := core.NewStoresInfo()
			for id := uint64(1); id <= 4; id++ {
				storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, NodeState: metapb.NodeState_Serving},
					core.SetLastHeartbeatTS(time.Now())))
			}
			regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
			re.NoError(err)
			manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
			re.NoError(err)
			re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{
				{GroupID: "a", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("d")}}},
				{GroupID: "b"},
			}))
			state, err := manager.UpdateAffinityGroupPeers("a", 1, []uint64{1, 2, 3})
			re.NoError(err)
			re.True(state.AffinitySchedulingAllowed)
			re.False(state.RegularSchedulingAllowed)
			before, err := store.LoadAffinityGroup("a")
			re.NoError(err)

			// Bypass affinity write validation, as a label watcher can receive an overlap from storage.
			overlap := keyutil.KeyRange{StartKey: []byte("b"), EndKey: []byte("c")}
			rule := MakeLabelRule(GroupKeyRanges{GroupID: "b", KeyRanges: []keyutil.KeyRange{overlap}})
			re.NoError(regionLabeler.SetLabelRule(rule))
			re.NoError(manager.SyncKeyRangesFromEtcd(rule))
			manager.ReconcileRangeConflicts()
			state = manager.GetAffinityGroupState("a")
			re.True(state.rangeConflict)
			re.True(state.RegularSchedulingAllowed)
			re.False(state.AffinitySchedulingAllowed)
			re.NotZero(state.affinityVer)

			if tc.observe {
				manager.ObserveAvailableRegion(generateRegionForTest(1, tc.voters, overlap), state)
			} else if tc.admin {
				_, err = manager.UpdateAffinityGroupPeers("a", 2, tc.voters)
				re.NoError(err)
			} else {
				_, err = manager.updateAffinityGroupPeersWithState("a", state, 2, tc.voters)
				re.NoError(err)
			}

			leader, voters := uint64(1), []uint64{1, 2, 3}
			if tc.admin {
				leader, voters = 2, tc.voters
			}
			assertTarget := func() {
				state := manager.GetAffinityGroupState("a")
				re.Equal(leader, state.LeaderStoreID)
				re.Equal(voters, state.VoterStoreIDs)
				saved, err := store.LoadAffinityGroup("a")
				re.NoError(err)
				var persisted Group
				re.NoError(json.Unmarshal([]byte(saved), &persisted))
				re.Equal(leader, persisted.LeaderStoreID)
				re.Equal(voters, persisted.VoterStoreIDs)
				if !tc.admin {
					re.Equal(before, saved)
				}
			}
			assertTarget()
			re.True(manager.GetAffinityGroupState("a").rangeConflict)

			re.NoError(regionLabeler.DeleteLabelRule(rule.ID))
			manager.SyncKeyRangesDeleteFromEtcd(rule.ID)
			manager.ReconcileRangeConflicts()
			state = manager.GetAffinityGroupState("a")
			re.False(state.rangeConflict)
			re.True(state.AffinitySchedulingAllowed)
			re.False(state.RegularSchedulingAllowed)
			assertTarget()
		})
	}
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
	re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{
		GroupID: groupID, KeyRanges: []keyutil.KeyRange{nonOverlappingRange},
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

	// Simulate store 2 unavailable.
	unavailable := map[uint64]storeCondition{2: storeRemoved}
	re.True(manager.applyStoreConditions(unavailable))

	state2 := manager.GetAffinityGroupState("avail")
	re.NotNil(state2)
	re.False(state2.AffinitySchedulingAllowed)
}

func TestStoreConditionsWithoutGroups(t *testing.T) {
	re := require.New(t)
	manager, _ := newTargetTestManager(t)
	stores := manager.storeSetInformer.(*core.StoresInfo)
	for _, evicted := range []bool{true, false, true} {
		change := core.ResumeLeaderTransfer(constant.In)
		if evicted {
			change = core.PauseLeaderTransfer(constant.In)
		}
		stores.PutStore(stores.GetStore(3).Clone(change))
		manager.CheckGroupsAvailability()
		re.Equal(evicted, manager.unavailableStores[3] == storeLeaderEvicted)
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		_, err := manager.UpdateAffinityGroupPeers("g", 3, []uint64{1, 2, 3})
		re.Equal(evicted, err != nil)
		re.NoError(manager.DeleteAffinityGroups([]string{"g"}, true))
	}
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

	store3 = store3.Clone(core.SetLastHeartbeatTS(time.Now()))
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

	// Create a test affinity group with unhealthy store
	group2 := &Group{
		ID:            "group2",
		LeaderStoreID: 3,
		VoterStoreIDs: []uint64{3, 2},
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

	// Disconnect store3 after creating the targets.
	store3 = store3.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Minute)))
	storeInfos.PutStore(store3)
	manager.CheckGroupsAvailability()

	// After health check, group1 should still be in effect (all stores healthy)
	re.True(manager.groups["group1"].IsAffinitySchedulingAllowed())

	// After health check, group2 should be invalidated (store3 is unhealthy)
	re.False(manager.groups["group2"].IsAffinitySchedulingAllowed())

	// Now make store3 healthy again
	store3Healthy := store3.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store3Healthy)

	// Check health again
	manager.CheckGroupsAvailability()

	// Group2 should be restored to available status
	re.True(manager.groups["group2"].IsAffinitySchedulingAllowed())

	// Set store3 to be down
	store3 = store3.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	storeInfos.PutStore(store3)

	// Check health again
	manager.CheckGroupsAvailability()

	// After health check, group2 should be invalidated (store3 is unhealthy)
	re.False(manager.groups["group2"].IsAffinitySchedulingAllowed())

	// Now make store3 healthy again
	store3Healthy = store3.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store3Healthy)

	// Check health again
	manager.CheckGroupsAvailability()

	// Group2 should not be restored from expired status
	re.False(manager.groups["group2"].IsAffinitySchedulingAllowed())
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
	manager.CheckGroupsAvailability()
	groupInfo := getGroupForTest(re, manager, "expire")
	re.Equal(groupAvailable, groupInfo.GetAvailability())

	// Make store2 unhealthy so the group becomes degraded.
	store2Disconnected := store2.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Minute)))
	storeInfos.PutStore(store2Disconnected)
	manager.CheckGroupsAvailability()
	groupInfo = getGroupForTest(re, manager, "expire")
	re.Equal(groupDegraded, groupInfo.GetAvailability())

	// Force the degraded status to be considered expired.
	manager.Lock()
	groupInfo.degradedExpiredAt = uint64(time.Now().Add(-time.Hour).Unix())
	manager.Unlock()

	// Run availability check again without changing the unavailable store set.
	manager.CheckGroupsAvailability()
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

// TestObserveAvailableRegionOnPreparingStore checks that a Region with a peer on
// a new store that is still Preparing can initialize a group.
func TestObserveAvailableRegionOnPreparingStore(t *testing.T) {
	re := require.New(t)
	manager, _ := newTargetTestManager(t)
	stores := manager.storeSetInformer.(*core.StoresInfo)
	stores.PutStore(stores.GetStore(2).Clone(core.SetNodeState(metapb.NodeState_Preparing)))
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	unavailable := manager.collectUnavailableStores()
	re.Empty(unavailable)
	manager.applyStoreConditions(unavailable)

	region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
	manager.ObserveAvailableRegion(region, manager.GetAffinityGroupState("g"))
	state := manager.GetAffinityGroupState("g")
	re.True(state.AffinitySchedulingAllowed)
	re.Equal([]uint64{1, 2}, state.VoterStoreIDs)
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
			m, store := newTargetTestManager(t)
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
					re.Equal(BalancePolicyFixed, loadStoredGroup(re, store, pair[0]).BalancePolicy)
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
				m, store := newTargetTestManager(t)
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
				re.ErrorIs(m.createGroupsWithSkipExisting(changes, skipExisting), errs.ErrAffinityGroupContent)
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
