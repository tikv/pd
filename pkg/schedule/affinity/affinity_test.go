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
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

// TestIsRegionAffinity tests the IsRegionAffinity method of Manager.
func TestIsRegionAffinity(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := uint64(1); i <= 4; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: i, Address: "test"})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	manager := NewManager(ctx, store, storeInfos, conf, nil)
	err := manager.Initialize()
	re.NoError(err)

	// Create affinity group
	group := &Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err = manager.SaveAffinityGroups([]GroupWithRanges{{Group: group}})
	re.NoError(err)

	// Test 1: Region not belonging to any affinity group should return false
	region1 := core.NewRegionInfo(
		&metapb.Region{Id: 1, Peers: []*metapb.Peer{
			{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 12, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 13, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	re.False(manager.IsRegionAffinity(region1), "Region not in group should return false")

	// Add region to group
	manager.SetRegionGroup(1, "test_group")

	// Test 2: Region conforming to affinity requirements should return true
	re.True(manager.IsRegionAffinity(region1), "Region conforming to affinity should return true")

	// Test 3: Region with wrong leader should return false
	region2 := core.NewRegionInfo(
		&metapb.Region{Id: 2, Peers: []*metapb.Peer{
			{Id: 21, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 22, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 23, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 22, StoreId: 2, Role: metapb.PeerRole_Voter}, // Leader on store 2, not 1
	)
	manager.SetRegionGroup(2, "test_group")
	re.False(manager.IsRegionAffinity(region2), "Region with wrong leader should return false")

	// Test 4: Region with wrong voter stores should return false
	region3 := core.NewRegionInfo(
		&metapb.Region{Id: 3, Peers: []*metapb.Peer{
			{Id: 31, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 32, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 33, StoreId: 4, Role: metapb.PeerRole_Voter}, // Store 4 instead of 3
		}},
		&metapb.Peer{Id: 31, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	manager.SetRegionGroup(3, "test_group")
	re.False(manager.IsRegionAffinity(region3), "Region with wrong voter stores should return false")

	// Test 5: Region with different number of voters should return false
	region4 := core.NewRegionInfo(
		&metapb.Region{Id: 4, Peers: []*metapb.Peer{
			{Id: 41, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 42, StoreId: 2, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 41, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	manager.SetRegionGroup(4, "test_group")
	re.False(manager.IsRegionAffinity(region4), "Region with wrong number of voters should return false")

	// Test 6: Region without leader should return false
	region5 := core.NewRegionInfo(
		&metapb.Region{Id: 5, Peers: []*metapb.Peer{
			{Id: 51, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 52, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 53, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		nil, // No leader
	)
	manager.SetRegionGroup(5, "test_group")
	re.False(manager.IsRegionAffinity(region5), "Region without leader should return false")

	// Test 7: Group not in effect should return false
	groupInfo := manager.GetGroups()["test_group"]
	groupInfo.Effect = false
	region6 := core.NewRegionInfo(
		&metapb.Region{Id: 6, Peers: []*metapb.Peer{
			{Id: 61, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 62, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 63, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 61, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	manager.SetRegionGroup(6, "test_group")
	re.False(manager.IsRegionAffinity(region6), "Group not in effect should return false")
}

func TestKeyRangeOverlapValidation(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	// Create manager without region labeler for basic validation testing
	manager := NewManager(ctx, store, storeInfos, conf, nil)
	err := manager.Initialize()
	re.NoError(err)

	validate := func(ranges []keyRange) error {
		manager.Lock()
		defer manager.Unlock()
		return manager.validateNoKeyRangeOverlap(ranges)
	}

	// Test 1: Non-overlapping ranges should succeed
	keyRanges1 := []keyRange{
		{startKey: []byte("a"), endKey: []byte("b"), groupID: "group1"},
		{startKey: []byte("c"), endKey: []byte("d"), groupID: "group1"},
	}
	err = validate(keyRanges1)
	re.NoError(err, "Non-overlapping ranges should pass validation")

	// Test 2: Overlapping ranges within same request should fail
	keyRanges2 := []keyRange{
		{startKey: []byte("a"), endKey: []byte("c"), groupID: "group1"},
		{startKey: []byte("b"), endKey: []byte("d"), groupID: "group1"},
	}
	err = validate(keyRanges2)
	re.Error(err, "Overlapping ranges should fail validation")
	re.Contains(err.Error(), "overlap")

	// Test 3: Adjacent ranges (not overlapping) should succeed
	keyRanges3 := []keyRange{
		{startKey: []byte("a"), endKey: []byte("b"), groupID: "group1"},
		{startKey: []byte("b"), endKey: []byte("c"), groupID: "group1"},
	}
	err = validate(keyRanges3)
	re.NoError(err, "Adjacent ranges should pass validation")

	// Test 4: Verify checkKeyRangesOverlap function directly
	overlaps := checkKeyRangesOverlap([]byte("a"), []byte("c"), []byte("b"), []byte("d"))
	re.True(overlaps, "Ranges [a,c) and [b,d) should overlap")

	overlaps = checkKeyRangesOverlap([]byte("a"), []byte("b"), []byte("c"), []byte("d"))
	re.False(overlaps, "Ranges [a,b) and [c,d) should not overlap")
}

// TestKeyRangeOverlapRebuild tests rebuild after restart
// Note: Full labeler integration test requires proper JSON serialization handling
func TestKeyRangeOverlapRebuild(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	manager := NewManager(ctx, store, storeInfos, conf, nil)
	err := manager.Initialize()
	re.NoError(err)

	// Create two groups without key ranges for basic testing
	groupsWithRanges := []GroupWithRanges{
		{
			Group: &Group{
				ID:            "group1",
				LeaderStoreID: 1,
				VoterStoreIDs: []uint64{1},
			},
		},
		{
			Group: &Group{
				ID:            "group2",
				LeaderStoreID: 1,
				VoterStoreIDs: []uint64{1},
			},
		},
	}
	err = manager.SaveAffinityGroups(groupsWithRanges)
	re.NoError(err)

	// Verify groups were created
	re.True(manager.IsGroupExist("group1"))
	re.True(manager.IsGroupExist("group2"))

	// Create a new manager to simulate restart
	manager2 := NewManager(ctx, store, storeInfos, conf, nil)
	err = manager2.Initialize()
	re.NoError(err)

	// Verify groups were loaded from storage
	re.True(manager2.IsGroupExist("group1"))
	re.True(manager2.IsGroupExist("group2"))
}

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

	manager := NewManager(ctx, store, storeInfos, conf, nil)
	re.NoError(manager.Initialize())

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
	re.True(state.Effect)
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
	re.True(state2.Effect)
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
	manager := NewManager(ctx, store, storeInfos, conf, nil)
	re.NoError(manager.Initialize())

	group := &Group{ID: "avail", LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
	re.NoError(manager.SaveAffinityGroups([]GroupWithRanges{{Group: group}}))
	_, err := manager.UpdateGroupPeers("avail", 1, []uint64{1, 2})
	re.NoError(err)
	state := manager.GetAffinityGroupState("avail")
	re.True(state.Effect)

	// Simulate store 2 unavailable.
	unavailable := map[uint64]storeState{2: removingOrRemoved}
	manager.setUnavailableStores(unavailable)

	state2 := manager.GetAffinityGroupState("avail")
	re.False(state2.Effect)
}

func TestParseKeyRangesFromDataInvalidHex(t *testing.T) {
	re := require.New(t)
	_, err := parseKeyRangesFromData([]*labeler.KeyRangeRule{
		{StartKeyHex: "zz", EndKeyHex: "10"},
	}, "g1")
	re.Error(err)
	re.ErrorContains(err, "invalid hex start key")
}

func TestAffinityPersistenceWithLabeler(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(manager.Initialize())

	gwr := GroupWithRanges{
		Group: &Group{
			ID:            "persist",
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		},
		KeyRanges: []keyutil.KeyRange{{StartKey: []byte{0x00}, EndKey: []byte{0x10}}},
	}
	re.NoError(manager.SaveAffinityGroups([]GroupWithRanges{gwr}))

	// RangeCount should be recorded and label rule created.
	state := manager.GetAffinityGroupState("persist")
	re.NotNil(state)
	re.Equal(1, state.RangeCount)
	re.NotNil(regionLabeler.GetLabelRule(GetLabelRuleID("persist")))

	// Reload manager to verify persistence and loadRegionLabel integration.
	manager2 := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(manager2.Initialize())
	state2 := manager2.GetAffinityGroupState("persist")
	re.NotNil(state2)
	re.Equal(1, state2.RangeCount)

	// Remove all ranges and ensure cache/label are cleared.
	ranges := []keyRange{{
		startKey: []byte{0x00},
		endKey:   []byte{0x10},
		groupID:  "persist",
	}}
	re.NoError(manager2.updateGroupRanges("persist", ranges))
	re.NoError(manager2.updateGroupRanges("persist", nil))
	state3 := manager2.GetAffinityGroupState("persist")
	re.NotNil(state3)
	re.Equal(0, state3.RangeCount)
	re.Nil(regionLabeler.GetLabelRule(GetLabelRuleID("persist")))
}

// TestLabelRuleIntegration tests basic label rule creation and deletion
// Note: Full integration with JSON serialization requires additional handling
func TestLabelRuleIntegration(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	// Create region labeler
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	err = manager.Initialize()
	re.NoError(err)

	// Test: Group with no key ranges should not create label rule
	group1 := &Group{
		ID:            "group_no_label",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1},
	}
	err = manager.SaveAffinityGroups([]GroupWithRanges{{Group: group1}})
	re.NoError(err)

	labelRuleID := GetLabelRuleID("group_no_label")
	labelRule := regionLabeler.GetLabelRule(labelRuleID)
	re.Nil(labelRule, "No label rule should be created when keyRanges is nil")

	// Verify group was created
	re.True(manager.IsGroupExist("group_no_label"))

	// Delete group (no key ranges, so force=false should work)
	err = manager.DeleteAffinityGroup("group_no_label", false)
	re.NoError(err)
	re.False(manager.IsGroupExist("group_no_label"))
}

// TestBasicGroupOperations tests basic group CRUD operations
func TestBasicGroupOperations(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	manager := NewManager(ctx, store, storeInfos, conf, nil)
	err := manager.Initialize()
	re.NoError(err)

	// Create a group
	group1 := &Group{
		ID:            "group1",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1},
	}
	err = manager.SaveAffinityGroups([]GroupWithRanges{{Group: group1}})
	re.NoError(err)
	re.True(manager.IsGroupExist("group1"))

	// Delete the group (no key ranges, so force=false should work)
	err = manager.DeleteAffinityGroup("group1", false)
	re.NoError(err)
	re.False(manager.IsGroupExist("group1"))
}

func TestStoreHealthCheck(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a test storage
	store := storage.NewStorageWithMemoryBackend()

	// Create mock stores
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2, Address: "test2"})
	store3 := core.NewStoreInfo(&metapb.Store{Id: 3, Address: "test3"})

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
	manager := NewManager(ctx, store, storeInfos, conf, nil)
	err := manager.Initialize()
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
	re.True(groupInfo1.Effect)
	groupInfo2 := manager.groups["group2"]
	re.True(groupInfo2.Effect)

	// Manually call checkStoreHealth to test
	manager.checkStoresAvailability()

	// After health check, group1 should still be in effect (all stores healthy)
	re.True(manager.groups["group1"].Effect)

	// After health check, group2 should be invalidated (store3 is unhealthy)
	re.False(manager.groups["group2"].Effect)

	// Now make store3 healthy again
	store3Healthy := store3.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store3Healthy)

	// Check health again
	manager.checkStoresAvailability()

	// Group2 should be restored to effect state
	re.True(manager.groups["group2"].Effect)
}
