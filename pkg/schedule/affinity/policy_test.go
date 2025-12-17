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
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
)

func TestStoreCondition(t *testing.T) {
	re := require.New(t)
	re.Equal(groupDegraded, storeEvicted.groupAvailability())
	re.Equal(groupDegraded, storeBusy.groupAvailability())
	re.Equal(groupDegraded, storeDisconnected.groupAvailability())
	re.Equal(groupDegraded, storePreparing.groupAvailability())
	re.Equal(groupDegraded, storeLowSpace.groupAvailability())
	re.Equal(groupExpired, storeDown.groupAvailability())
	re.Equal(groupExpired, storeRemoving.groupAvailability())
	re.Equal(groupExpired, storeRemoved.groupAvailability())

	re.True(storeEvicted.affectsLeaderOnly())
	re.True(storeBusy.affectsLeaderOnly())
	re.False(storeDisconnected.affectsLeaderOnly())
	re.False(storeDown.affectsLeaderOnly())
}

func TestCollectUnavailableStores(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	memoryStorage := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	conf := mockconfig.NewTestOptions()
	conf.SetLabelProperty("reject-leader", "reject", "leader")
	regionLabeler, err := labeler.NewRegionLabeler(ctx, memoryStorage, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, memoryStorage, storeInfos, conf, regionLabeler)
	re.NoError(err)

	stores := make([]*core.StoreInfo, 11)
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
	// stores[6]: storeDisconnected
	stores[6] = stores[6].Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Minute)))
	// stores[7]: storePreparing
	stores[7] = stores[7].Clone(core.SetNodeState(metapb.NodeState_Preparing))
	// stores[8]: storeDown
	stores[8] = stores[8].Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	// stores[9]: storeRemoving
	stores[9] = stores[9].Clone(core.SetStoreState(metapb.StoreState_Offline, false))
	// stores[12]: storeRemoved
	stores[10] = stores[10].Clone(core.SetStoreState(metapb.StoreState_Tombstone))

	for _, store := range stores {
		storeInfos.PutStore(store)
	}

	actual := manager.collectUnavailableStores()
	expected := map[uint64]storeCondition{
		1:  storeEvicted,
		2:  storeEvicted,
		3:  storeEvicted,
		4:  storeEvicted,
		5:  storeEvicted,
		6:  storeDisconnected,
		7:  storePreparing,
		8:  storeDown,
		9:  storeRemoving,
		10: storeRemoved,
	}
	re.True(maps.Equal(expected, actual))
}

func TestObserveAvailableRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

func TestAvailabilityCheckInvalidatesGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "s1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2, Address: "s2"})
	store2 = store2.Clone(core.SetLastHeartbeatTS(time.Now()), core.SetNodeState(metapb.NodeState_Removing))
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
	isUnavailableStoresChanged, groupAvailabilityChanges := manager.getGroupAvailabilityChanges(unavailable)
	re.True(isUnavailableStoresChanged)
	manager.setGroupAvailabilityChanges(unavailable, groupAvailabilityChanges)

	state2 := manager.GetAffinityGroupState("avail")
	re.NotNil(state2)
	re.False(state2.AffinitySchedulingAllowed)
}

func TestStoreHealthCheck(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	// Set store3 to be disconnected
	store3 = store3.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Minute)))
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

	// Manually call checkStoreHealth to test
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
}

// TestDegradedGroupShouldExpire verifies a degraded group should move to expired even when
// the set of unavailable stores does not change.
func TestDegradedGroupShouldExpire(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	unavailable := map[uint64]storeCondition{1: storeEvicted}
	changed, changes := manager.getGroupAvailabilityChanges(unavailable)
	re.True(changed)
	manager.setGroupAvailabilityChanges(unavailable, changes)
	groupInfo := getGroupForTest(re, manager, "leader-only")
	re.Equal(groupDegraded, groupInfo.GetAvailability())

	// evict-leader only on follower should not change availability
	unavailable = map[uint64]storeCondition{2: storeEvicted}
	changed, changes = manager.getGroupAvailabilityChanges(unavailable)
	re.True(changed)
	manager.setGroupAvailabilityChanges(unavailable, changes)
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
	changed, changes = manager.getGroupAvailabilityChanges(unavailable)
	re.True(changed)
	manager.setGroupAvailabilityChanges(unavailable, changes)
	groupInfo = getGroupForTest(re, manager, "priority")
	re.Equal(groupExpired, groupInfo.GetAvailability())
}
