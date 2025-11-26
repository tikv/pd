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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/storage"
)

func TestObserveAvailableRegionOnlyFirstTime(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "s1"})
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2, Address: "s2"})
	for _, s := range []*core.StoreInfo{store1, store2} {
		storeInfos.PutStore(s.Clone(core.SetLastHeartbeatTS(time.Now())))
	}
	conf := mockconfig.NewTestOptions()

	manager, err := NewManager(ctx, store, storeInfos, conf, nil)
	re.NoError(err)

	group := &Group{ID: "g", LeaderStoreID: 0, VoterStoreIDs: nil}
	re.NoError(manager.SaveAffinityGroups([]GroupWithRanges{{Group: group}}))

	// First observation makes group effective with store 1.
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
	re.True(state.IsAffinitySchedulingAllowed)
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
	state2 := manager.GetAffinityGroupState("g")
	re.True(state2.IsAffinitySchedulingAllowed)
	re.Equal(uint64(1), state2.LeaderStoreID)
	re.ElementsMatch([]uint64{1}, state2.VoterStoreIDs)
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
	manager, err := NewManager(ctx, store, storeInfos, conf, nil)
	re.NoError(err)

	group := &Group{ID: "avail", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
	re.NoError(manager.SaveAffinityGroups([]GroupWithRanges{{Group: group}}))
	_, err = manager.UpdateGroupPeers("avail", 1, []uint64{1, 2})
	re.NoError(err)
	state := manager.GetAffinityGroupState("avail")
	re.True(state.IsAffinitySchedulingAllowed)

	// Simulate store 2 unavailable.
	unavailable := map[uint64]condition{2: storeRemovingOrRemoved}
	isUnavailableStoresChanged, groupStateChanges := manager.getGroupStateChanges(unavailable)
	re.True(isUnavailableStoresChanged)
	manager.setGroupStateChanges(unavailable, groupStateChanges)

	state2 := manager.GetAffinityGroupState("avail")
	re.False(state2.IsAffinitySchedulingAllowed)
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

	// Set store3 to be unhealthy (disconnected)
	store3 = store3.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	storeInfos.PutStore(store3)

	conf := mockconfig.NewTestOptions()

	// Create affinity manager
	manager, err := NewManager(ctx, store, storeInfos, conf, nil)
	re.NoError(err)

	// Create a test affinity group with healthy stores
	group1 := &Group{
		ID:            "group1",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2},
	}
	err = manager.SaveAffinityGroups([]GroupWithRanges{{Group: group1}})
	re.NoError(err)

	// Create a test affinity group with unhealthy store
	group2 := &Group{
		ID:            "group2",
		LeaderStoreID: 3,
		VoterStoreIDs: []uint64{3, 2},
	}
	err = manager.SaveAffinityGroups([]GroupWithRanges{{Group: group2}})
	re.NoError(err)

	// Verify initial state - all groups should be in effect
	groupInfo1 := manager.groups["group1"]
	re.True(groupInfo1.IsAffinitySchedulingAllowed())
	groupInfo2 := manager.groups["group2"]
	re.True(groupInfo2.IsAffinitySchedulingAllowed())

	// Manually call checkStoreHealth to test
	manager.checkStoresAvailability()

	// After health check, group1 should still be in effect (all stores healthy)
	re.True(manager.groups["group1"].IsAffinitySchedulingAllowed())

	// After health check, group2 should be invalidated (store3 is unhealthy)
	re.False(manager.groups["group2"].IsAffinitySchedulingAllowed())

	// Now make store3 healthy again
	store3Healthy := store3.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store3Healthy)

	// Check health again
	manager.checkStoresAvailability()

	// Group2 should be restored to effect state
	re.False(manager.groups["group2"].IsAffinitySchedulingAllowed())
}
