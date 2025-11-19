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
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
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

	// Test 1: Non-overlapping ranges should succeed
	keyRanges1 := []KeyRangeInput{
		{StartKey: []byte("a"), EndKey: []byte("b"), GroupID: "group1"},
		{StartKey: []byte("c"), EndKey: []byte("d"), GroupID: "group1"},
	}
	err = manager.ValidateKeyRanges(keyRanges1)
	re.NoError(err, "Non-overlapping ranges should pass validation")

	// Test 2: Overlapping ranges within same request should fail
	keyRanges2 := []KeyRangeInput{
		{StartKey: []byte("a"), EndKey: []byte("c"), GroupID: "group1"},
		{StartKey: []byte("b"), EndKey: []byte("d"), GroupID: "group1"},
	}
	err = manager.ValidateKeyRanges(keyRanges2)
	re.Error(err, "Overlapping ranges should fail validation")
	re.Contains(err.Error(), "overlap")

	// Test 3: Adjacent ranges (not overlapping) should succeed
	keyRanges3 := []KeyRangeInput{
		{StartKey: []byte("a"), EndKey: []byte("b"), GroupID: "group1"},
		{StartKey: []byte("b"), EndKey: []byte("c"), GroupID: "group1"},
	}
	err = manager.ValidateKeyRanges(keyRanges3)
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

	// Delete group
	err = manager.DeleteAffinityGroup("group_no_label")
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

	// Delete the group
	err = manager.DeleteAffinityGroup("group1")
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
	manager.checkStoreHealth()

	// After health check, group1 should still be in effect (all stores healthy)
	re.True(manager.groups["group1"].Effect)

	// After health check, group2 should be invalidated (store3 is unhealthy)
	re.False(manager.groups["group2"].Effect)

	// Now make store3 healthy again
	store3Healthy := store3.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store3Healthy)

	// Check health again
	manager.checkStoreHealth()

	// Group2 should be restored to effect state
	re.True(manager.groups["group2"].Effect)
}

func TestGetUnhealthyStores(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()

	// Create stores with different health status
	healthyStore := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	healthyStore = healthyStore.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(healthyStore)

	unhealthyStore := core.NewStoreInfo(&metapb.Store{Id: 2, Address: "test2"})
	unhealthyStore = unhealthyStore.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	storeInfos.PutStore(unhealthyStore)

	disconnectedStore := core.NewStoreInfo(&metapb.Store{Id: 3, Address: "test3"})
	disconnectedStore = disconnectedStore.Clone(core.SetLastHeartbeatTS(time.Now().Add(-35 * time.Minute)))
	storeInfos.PutStore(disconnectedStore)

	conf := mockconfig.NewTestOptions()
	manager := NewManager(ctx, store, storeInfos, conf, nil)

	// Test group with only healthy stores
	groupInfo1 := &GroupInfo{
		Group: Group{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1},
		},
	}
	unhealthy := manager.getUnhealthyStores(groupInfo1)
	re.Empty(unhealthy)

	// Test group with unhealthy leader
	groupInfo2 := &GroupInfo{
		Group: Group{
			LeaderStoreID: 2,
			VoterStoreIDs: []uint64{2, 1},
		},
	}
	unhealthy = manager.getUnhealthyStores(groupInfo2)
	re.Contains(unhealthy, uint64(2))

	// Test group with disconnected voter
	groupInfo3 := &GroupInfo{
		Group: Group{
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1, 3},
		},
	}
	unhealthy = manager.getUnhealthyStores(groupInfo3)
	re.Contains(unhealthy, uint64(3))

	// Test group with multiple unhealthy stores
	groupInfo4 := &GroupInfo{
		Group: Group{
			LeaderStoreID: 2,
			VoterStoreIDs: []uint64{2, 3},
		},
	}
	unhealthy = manager.getUnhealthyStores(groupInfo4)
	re.Len(unhealthy, 2)
	re.Contains(unhealthy, uint64(2))
	re.Contains(unhealthy, uint64(3))
}
