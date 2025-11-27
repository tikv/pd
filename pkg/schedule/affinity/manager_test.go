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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
)

// TestGetRegionAffinityGroupState tests the GetRegionAffinityGroupState method of Manager.
func TestGetRegionAffinityGroupState(t *testing.T) {
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

	// Create region labeler
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create affinity group
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "test_group"}})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers("test_group", 1, []uint64{1, 2, 3})
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
	_, isAffinity := manager.GetRegionAffinityGroupState(region1)
	re.False(isAffinity, "Region not in group should return false")

	// Add region to group
	manager.SetRegionGroup(1, "test_group")

	// Test 2: Region conforming to affinity requirements should return true
	_, isAffinity = manager.GetRegionAffinityGroupState(region1)
	re.True(isAffinity, "Region conforming to affinity should return true")

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
	_, isAffinity = manager.GetRegionAffinityGroupState(region2)
	re.False(isAffinity, "Region with wrong leader should return false")

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
	_, isAffinity = manager.GetRegionAffinityGroupState(region3)
	re.False(isAffinity, "Region with wrong voter stores should return false")

	// Test 5: Region with different number of voters should return false
	region4 := core.NewRegionInfo(
		&metapb.Region{Id: 4, Peers: []*metapb.Peer{
			{Id: 41, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 42, StoreId: 2, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 41, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	manager.SetRegionGroup(4, "test_group")
	_, isAffinity = manager.GetRegionAffinityGroupState(region4)
	re.False(isAffinity, "Region with wrong number of voters should return false")

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
	_, isAffinity = manager.GetRegionAffinityGroupState(region5)
	re.False(isAffinity, "Region without leader should return false")

	// Test 7: Group not in effect should return false
	groupInfo := manager.GetGroups()["test_group"]
	groupInfo.State = groupExpired
	region6 := core.NewRegionInfo(
		&metapb.Region{Id: 6, Peers: []*metapb.Peer{
			{Id: 61, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 62, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 63, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 61, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	manager.SetRegionGroup(6, "test_group")
	_, isAffinity = manager.GetRegionAffinityGroupState(region6)
	re.False(isAffinity, "Group not in effect should return false")
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

	// Create region labeler
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a group
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "group1"}})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers("group1", 1, []uint64{1})
	re.NoError(err)
	re.True(manager.IsGroupExist("group1"))

	// Delete the group (no key ranges, so force=false should work)
	err = manager.DeleteAffinityGroups([]string{"group1"}, false)
	re.NoError(err)
	re.False(manager.IsGroupExist("group1"))
}

// TestRegionCountStaleCache documents that RegionCount counts stale cache entries when group changes.
func TestRegionCountStaleCache(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i < 7; i++ {
		storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i)}))
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	_, err = manager.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Region matches old peers (1,2).
	region := core.NewRegionInfo(
		&metapb.Region{Id: 100, Peers: []*metapb.Peer{
			{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 101, StoreId: 1},
	)
	manager.SetRegionGroup(region.GetID(), "g")
	_, isAffinity := manager.GetRegionAffinityGroupState(region)
	re.True(isAffinity)
	groupInfo := manager.GetGroups()["g"]
	re.Equal(1, groupInfo.AffinityRegionCount)
	re.Len(groupInfo.Regions, 1)

	// Change peers, which bumps AffinityVer and invalidates affinity for the cached region.
	_, err = manager.UpdateAffinityGroupPeers("g", 4, []uint64{4, 5, 6})
	re.NoError(err)
	group2 := manager.GetAffinityGroupState("g")

	// Region cache should be cleared when group changes.
	re.Zero(group2.AffinityRegionCount)
	re.Zero(group2.RegionCount)
}

// TestDeleteGroupClearsCache verifies that deleting a group clears all related region caches.
func TestDeleteGroupClearsCache(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i <= 3; i++ {
		storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i)}))
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a group and add regions
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "test-group"}}))
	_, err = manager.UpdateAffinityGroupPeers("test-group", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Create and associate multiple regions
	region1 := core.NewRegionInfo(
		&metapb.Region{Id: 100, Peers: []*metapb.Peer{
			{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 101, StoreId: 1},
	)
	region2 := core.NewRegionInfo(
		&metapb.Region{Id: 200, Peers: []*metapb.Peer{
			{Id: 201, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 202, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 203, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 201, StoreId: 1},
	)

	manager.SetRegionGroup(region1.GetID(), "test-group")
	manager.SetRegionGroup(region2.GetID(), "test-group")

	// Trigger cache population
	_, isAffinity1 := manager.GetRegionAffinityGroupState(region1)
	re.True(isAffinity1)
	_, isAffinity2 := manager.GetRegionAffinityGroupState(region2)
	re.True(isAffinity2)

	// Verify regions are in cache
	groupInfo := manager.GetGroups()["test-group"]
	re.Len(groupInfo.Regions, 2)
	re.Equal(2, groupInfo.AffinityRegionCount)

	// Verify regions in global cache
	manager.RLock()
	_, exists1 := manager.regions[100]
	_, exists2 := manager.regions[200]
	manager.RUnlock()
	re.True(exists1)
	re.True(exists2)

	// Delete the group (no key ranges, so force=false should work)
	err = manager.DeleteAffinityGroups([]string{"test-group"}, false)
	re.NoError(err)

	// Verify group is deleted
	re.False(manager.IsGroupExist("test-group"))

	// Verify all regions are cleared from global cache
	manager.RLock()
	_, exists1After := manager.regions[100]
	_, exists2After := manager.regions[200]
	globalAffinityCount := manager.affinityRegionCount
	manager.RUnlock()
	re.False(exists1After, "region 100 should be removed from global cache")
	re.False(exists2After, "region 200 should be removed from global cache")
	re.Zero(globalAffinityCount, "global affinity region count should be 0")
}

// TestStateChangeRegionCount verifies that changing group state clears region cache.
func TestStateChangeRegionCount(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i <= 3; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i)})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a group
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "state-test"}}))
	_, err = manager.UpdateAffinityGroupPeers("state-test", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Add regions to cache
	region := core.NewRegionInfo(
		&metapb.Region{Id: 100, Peers: []*metapb.Peer{
			{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 101, StoreId: 1},
	)
	manager.SetRegionGroup(region.GetID(), "state-test")
	_, isAffinity := manager.GetRegionAffinityGroupState(region)
	re.True(isAffinity)

	// Verify region is cached
	groupState1 := manager.GetAffinityGroupState("state-test")
	re.Equal(1, groupState1.RegionCount)
	re.Equal(1, groupState1.AffinityRegionCount)

	// Make store 2 unhealthy to trigger state change to degraded
	store2 := storeInfos.GetStore(2)
	store2Down := store2.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	storeInfos.PutStore(store2Down)

	// Trigger availability check
	manager.checkStoresAvailability()

	// Verify group state changed
	groupInfo := manager.GetGroups()["state-test"]
	re.False(groupInfo.IsAffinitySchedulingAllowed())

	// Verify cache is cleared
	groupState2 := manager.GetAffinityGroupState("state-test")
	re.Zero(groupState2.RegionCount, "RegionCount should be 0 after state change")
	re.Zero(groupState2.AffinityRegionCount, "AffinityRegionCount should be 0 after state change")

	// Verify global cache is also cleared
	manager.RLock()
	_, exists := manager.regions[100]
	globalAffinityCount := manager.affinityRegionCount
	manager.RUnlock()
	re.False(exists, "region should be removed from global cache after state change")
	re.Zero(globalAffinityCount, "global affinity count should be 0")
}

// TestInvalidCacheMultipleTimes verifies that InvalidCache can be called multiple times safely.
func TestInvalidCacheMultipleTimes(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i <= 3; i++ {
		storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i)}))
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a group
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "invalid-test"}}))
	_, err = manager.UpdateAffinityGroupPeers("invalid-test", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Add region
	region := core.NewRegionInfo(
		&metapb.Region{Id: 100, Peers: []*metapb.Peer{
			{Id: 101, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 102, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 103, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 101, StoreId: 1},
	)
	manager.SetRegionGroup(region.GetID(), "invalid-test")
	_, isAffinity := manager.GetRegionAffinityGroupState(region)
	re.True(isAffinity)

	// Verify region is in cache
	groupState := manager.GetAffinityGroupState("invalid-test")
	re.Equal(1, groupState.RegionCount)
	re.Equal(1, groupState.AffinityRegionCount)

	// Invalidate cache first time
	manager.InvalidCache(100)

	// Verify cache is cleared
	groupState2 := manager.GetAffinityGroupState("invalid-test")
	re.Zero(groupState2.RegionCount)
	re.Zero(groupState2.AffinityRegionCount)

	manager.RLock()
	_, exists := manager.regions[100]
	manager.RUnlock()
	re.False(exists)

	// Invalidate cache second time - should not panic or error
	manager.InvalidCache(100)

	// Verify still cleared
	groupState3 := manager.GetAffinityGroupState("invalid-test")
	re.Zero(groupState3.RegionCount)
	re.Zero(groupState3.AffinityRegionCount)

	// Invalidate non-existent region - should not panic
	manager.InvalidCache(999)
}

// TestConcurrentOperations verifies concurrent operations don't cause race conditions.
// Run with: go test -race
func TestConcurrentOperations(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i <= 5; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i)})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create initial groups
	for i := 1; i <= 3; i++ {
		groupID := fmt.Sprintf("concurrent-group-%d", i)
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: groupID}}))
		_, err = manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 3})
		re.NoError(err)
	}

	// Create test regions
	regions := make([]*core.RegionInfo, 10)
	for i := range 10 {
		regions[i] = core.NewRegionInfo(
			&metapb.Region{Id: uint64(100 + i), Peers: []*metapb.Peer{
				{Id: uint64(1000 + i*3), StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: uint64(1001 + i*3), StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: uint64(1002 + i*3), StoreId: 3, Role: metapb.PeerRole_Voter},
			}},
			&metapb.Peer{Id: uint64(1000 + i*3), StoreId: 1},
		)
		groupID := fmt.Sprintf("concurrent-group-%d", (i%3)+1)
		manager.SetRegionGroup(regions[i].GetID(), groupID)
	}

	// Run concurrent operations
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// Goroutine 1: Read operations
	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for range 50 {
				region := regions[idx%len(regions)]
				_, _ = manager.GetRegionAffinityGroupState(region)
				groupID := fmt.Sprintf("concurrent-group-%d", (idx%3)+1)
				_ = manager.GetAffinityGroupState(groupID)
				_ = manager.IsGroupExist(groupID)
			}
		}(i)
	}

	// Goroutine 2: Update peers
	for i := range 3 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			groupID := fmt.Sprintf("concurrent-group-%d", idx+1)
			for j := range 10 {
				_, err := manager.UpdateAffinityGroupPeers(groupID, uint64((j%3)+1), []uint64{1, 2, 3})
				if err != nil {
					errChan <- err
					return
				}
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Goroutine 3: InvalidCache operations
	for i := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := range 30 {
				regionID := uint64(100 + (idx*2+j)%10)
				manager.InvalidCache(regionID)
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Goroutine 4: Check availability
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 20 {
			manager.checkStoresAvailability()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Wait for all goroutines
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		re.NoError(err, "concurrent operation failed")
	}

	// Verify final state is consistent
	for i := 1; i <= 3; i++ {
		groupID := fmt.Sprintf("concurrent-group-%d", i)
		re.True(manager.IsGroupExist(groupID))
		state := manager.GetAffinityGroupState(groupID)
		re.NotNil(state)
	}
}

// TestDegradedExpiration verifies that a degraded group automatically expires after the configured duration.
func TestDegradedExpiration(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i <= 3; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i)})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a healthy group
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "expiration-test"}}))
	_, err = manager.UpdateAffinityGroupPeers("expiration-test", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Verify group is available
	groupState := manager.GetAffinityGroupState("expiration-test")
	re.True(groupState.IsAffinitySchedulingAllowed)

	// Make store 2 unhealthy to trigger degraded state
	store2 := storeInfos.GetStore(2)
	store2Down := store2.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	storeInfos.PutStore(store2Down)
	manager.checkStoresAvailability()

	// Verify group became degraded
	groupInfo := manager.GetGroups()["expiration-test"]
	re.Equal(groupDegraded, groupInfo.State.toGroupState())
	re.False(groupInfo.IsAffinitySchedulingAllowed())

	// Record the expiration time
	manager.RLock()
	expirationTime := groupInfo.DegradedExpiredAt
	manager.RUnlock()

	// Verify the expiration time is approximately 10 minutes (600 seconds) from now
	expectedExpiration := uint64(time.Now().Unix()) + defaultDegradedExpirationSeconds
	// Allow 5 seconds tolerance for test execution time
	re.InDelta(expectedExpiration, expirationTime, 5)

	// Simulate time passing beyond expiration
	manager.Lock()
	groupInfo.DegradedExpiredAt = uint64(time.Now().Add(-time.Hour).Unix())
	manager.Unlock()

	// Run availability check again
	manager.checkStoresAvailability()

	// Verify group is now expired
	re.True(groupInfo.IsExpired())
	re.Equal(groupExpired, groupInfo.getState())

	// Verify scheduling is still disallowed
	groupState2 := manager.GetAffinityGroupState("expiration-test")
	re.False(groupState2.IsAffinitySchedulingAllowed)
}
