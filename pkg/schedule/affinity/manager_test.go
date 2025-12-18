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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
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
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create affinity group with 6 key ranges for testing
	ranges := createGroupForTest(re, manager, "test_group", 6)

	// Setup: voterStoreIDs will be sorted.
	_, err = manager.UpdateAffinityGroupPeers("test_group", 1, []uint64{3, 2, 1})
	re.NoError(err)
	groupInfo := getGroupForTest(re, manager, "test_group")
	re.True(slices.Equal([]uint64{1, 2, 3}, groupInfo.VoterStoreIDs))

	// Test the positive case first
	t.Run("region conforming to affinity", func(t *testing.T) {
		re := require.New(t)
		region := generateRegionForTest(1, []uint64{1, 2, 3}, ranges[0])
		_, isAffinity := manager.GetRegionAffinityGroupState(region)
		re.True(isAffinity)
	})

	// Test negative cases: all should return false
	testCases := []struct {
		name          string
		regionID      uint64
		peers         []uint64
		keyRange      keyutil.KeyRange
		withoutLeader bool
		setup         func()
	}{
		{
			name:     "region not in any affinity group",
			regionID: 1,
			peers:    []uint64{1, 2, 3},
			keyRange: nonOverlappingRange,
		},
		{
			name:     "region with wrong leader",
			regionID: 1,
			peers:    []uint64{2, 1, 3},
			keyRange: ranges[1],
		},
		{
			name:     "region with wrong voter stores",
			regionID: 3,
			peers:    []uint64{1, 2, 4},
			keyRange: ranges[2],
		},
		{
			name:     "region with different number of voters",
			regionID: 4,
			peers:    []uint64{1, 2},
			keyRange: ranges[3],
		},
		{
			name:          "region without leader",
			regionID:      5,
			peers:         []uint64{1, 2, 3},
			keyRange:      ranges[4],
			withoutLeader: true,
		},
		{
			name:     "group not in effect",
			regionID: 6,
			peers:    []uint64{1, 2, 3},
			keyRange: ranges[5],
			setup: func() {
				manager.ExpireAffinityGroup("test_group")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			if tc.setup != nil {
				tc.setup()
			}

			region := generateRegionForTest(tc.regionID, tc.peers, tc.keyRange)
			if tc.withoutLeader {
				region = region.Clone(core.WithLeader(nil))
			}

			_, isAffinity := manager.GetRegionAffinityGroupState(region)
			re.False(isAffinity)
		})
	}
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

	ranges := createGroupForTest(re, manager, "g", 2)
	_, err = manager.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)
	region := generateRegionForTest(100, []uint64{1, 2, 3}, ranges[0])

	// test skipSaveCache = true
	_, isAffinity := manager.GetRegionAffinityGroupState(region, true /* skipSaveCache */)
	re.True(isAffinity)
	groupInfo := getGroupForTest(re, manager, "g")
	re.Zero(groupInfo.AffinityRegionCount)
	re.Empty(groupInfo.Regions)

	// test skipSaveCache = false
	_, isAffinity = manager.GetRegionAffinityGroupState(region)
	re.True(isAffinity)
	groupInfo = getGroupForTest(re, manager, "g")
	re.Equal(1, groupInfo.AffinityRegionCount)
	re.Len(groupInfo.Regions, 1)

	// Change peers, which bumps AffinityVer and invalidates affinity for the cached region.
	_, err = manager.UpdateAffinityGroupPeers("g", 4, []uint64{4, 5, 6})
	re.NoError(err)
	group2 := manager.GetAffinityGroupState("g")
	re.NotNil(group2)
	re.Zero(group2.AffinityRegionCount)
	testCacheStale(re, manager, region)

	// Remove key ranges, which bumps AffinityVer and invalidates affinity for the cached region.
	region = generateRegionForTest(200, []uint64{4, 5, 6}, ranges[0])
	_, isAffinity = manager.GetRegionAffinityGroupState(region)
	re.True(isAffinity)
	groupInfo = getGroupForTest(re, manager, "g")
	re.Equal(1, groupInfo.AffinityRegionCount)
	re.Len(groupInfo.Regions, 2)
	re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{GroupID: "g", KeyRanges: ranges[1:]}}))
	groupInfo = getGroupForTest(re, manager, "g")
	re.Equal(0, groupInfo.AffinityRegionCount)
	re.Empty(groupInfo.Regions)

	// Add key ranges, which bumps AffinityVer and invalidates affinity for the cached region.
	_, isAffinity = manager.GetRegionAffinityGroupState(region)
	re.True(isAffinity)
	groupInfo = getGroupForTest(re, manager, "g")
	re.Equal(1, groupInfo.AffinityRegionCount)
	re.Len(groupInfo.Regions, 1)
	re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: "g", KeyRanges: ranges[1:]}}, nil))
	groupInfo = getGroupForTest(re, manager, "g")
	re.Equal(0, groupInfo.AffinityRegionCount)
	re.Len(groupInfo.Regions, 1)
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
	ranges := createGroupForTest(re, manager, "test-group", 1)
	_, err = manager.UpdateAffinityGroupPeers("test-group", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Create and associate multiple regions
	region1 := generateRegionForTest(100, []uint64{1, 2, 3}, ranges[0])
	region2 := generateRegionForTest(200, []uint64{1, 2, 3}, ranges[0])

	// Trigger cache population
	_, isAffinity1 := manager.GetRegionAffinityGroupState(region1)
	re.True(isAffinity1)
	_, isAffinity2 := manager.GetRegionAffinityGroupState(region2)
	re.True(isAffinity2)

	// Verify regions are in cache
	groupInfo := getGroupForTest(re, manager, "test-group")
	re.Len(groupInfo.Regions, 2)
	re.Equal(2, groupInfo.AffinityRegionCount)

	// Verify regions in global cache
	manager.RLock()
	_, exists1 := manager.regions[100]
	_, exists2 := manager.regions[200]
	manager.RUnlock()
	re.True(exists1)
	re.True(exists2)

	// Delete the group
	re.Error(manager.DeleteAffinityGroups([]string{"test-group"}, false))
	re.True(manager.IsGroupExist("test-group"))
	re.NoError(manager.DeleteAffinityGroups([]string{"test-group"}, true))
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

// TestAvailabilityChangeRegionCount verifies that changing group availability clears region cache.
func TestAvailabilityChangeRegionCount(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i <= 3; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i), NodeState: metapb.NodeState_Serving})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a group
	ranges := createGroupForTest(re, manager, "availability-test", 1)
	_, err = manager.UpdateAffinityGroupPeers("availability-test", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Add regions to cache
	region := generateRegionForTest(100, []uint64{1, 2, 3}, ranges[0])
	_, isAffinity := manager.GetRegionAffinityGroupState(region)
	re.True(isAffinity)

	// Verify region is cached
	groupState1 := manager.GetAffinityGroupState("availability-test")
	re.NotNil(groupState1)
	re.Equal(1, groupState1.RegionCount)
	re.Equal(1, groupState1.AffinityRegionCount)

	// Make store 2 unhealthy to trigger availability change to degraded
	store2 := storeInfos.GetStore(2)
	store2Down := store2.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	storeInfos.PutStore(store2Down)

	// Trigger availability check
	manager.checkGroupsAvailability()

	// Verify group state changed
	groupInfo := getGroupForTest(re, manager, "availability-test")
	re.False(groupInfo.IsAffinitySchedulingAllowed())

	// Verify cache is cleared
	groupState2 := manager.GetAffinityGroupState("availability-test")
	re.NotNil(groupState2)
	testCacheStale(re, manager, region)
	re.Zero(groupState2.AffinityRegionCount, "AffinityRegionCount should be 0 after availability change")

	// Verify global cache is also cleared
	manager.RLock()
	globalAffinityCount := manager.affinityRegionCount
	manager.RUnlock()
	testCacheStale(re, manager, region)
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
	ranges := createGroupForTest(re, manager, "invalid-test", 1)
	_, err = manager.UpdateAffinityGroupPeers("invalid-test", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Add region
	region := generateRegionForTest(100, []uint64{1, 2, 3}, ranges[0])
	_, isAffinity := manager.GetRegionAffinityGroupState(region)
	re.True(isAffinity)

	// Verify region is in cache
	groupState := manager.GetAffinityGroupState("invalid-test")
	re.NotNil(groupState)
	re.Equal(1, groupState.RegionCount)
	re.Equal(1, groupState.AffinityRegionCount)

	// Invalidate cache first time
	manager.InvalidCache(100)

	// Verify cache is cleared
	groupState2 := manager.GetAffinityGroupState("invalid-test")
	re.NotNil(groupState2)
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
	re.NotNil(groupState3)
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
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i), NodeState: metapb.NodeState_Serving})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create initial groups
	groups := make([][]keyutil.KeyRange, 3)
	for i := range 3 {
		groupID := fmt.Sprintf("concurrent-group-%d", i)
		groups[i] = createGroupForTest(re, manager, groupID, 10)
		_, err = manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 3})
		re.NoError(err)
	}

	// Create test regions
	regions := make([]*core.RegionInfo, 10)
	for i := range uint64(10) {
		regions[i] = generateRegionForTest(100+i, []uint64{1, 2, 3}, groups[i%3][i])
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
				groupID := fmt.Sprintf("concurrent-group-%d", idx%3)
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
			groupID := fmt.Sprintf("concurrent-group-%d", idx)
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
			manager.checkGroupsAvailability()
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
	for i := range 3 {
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
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i), NodeState: metapb.NodeState_Serving})
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
	re.NotNil(groupState)
	re.True(groupState.AffinitySchedulingAllowed)

	// Make store 2 unhealthy to trigger degraded status
	store2 := storeInfos.GetStore(2)
	store2Down := store2.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Minute)))
	storeInfos.PutStore(store2Down)
	manager.checkGroupsAvailability()

	// Verify group became degraded
	groupInfo := getGroupForTest(re, manager, "expiration-test")
	re.Equal(groupDegraded, groupInfo.GetAvailability())
	re.False(groupInfo.IsAffinitySchedulingAllowed())

	// Record the expiration time
	manager.RLock()
	expirationTime := groupInfo.degradedExpiredAt
	manager.RUnlock()

	// Verify the expiration time is approximately 10 minutes (600 seconds) from now
	expectedExpiration := uint64(time.Now().Unix()) + defaultDegradedExpirationSeconds
	// Allow 5 seconds tolerance for test execution time
	re.InDelta(expectedExpiration, expirationTime, 5)

	// Simulate time passing beyond expiration
	manager.Lock()
	groupInfo.degradedExpiredAt = uint64(time.Now().Add(-time.Hour).Unix())
	manager.Unlock()

	// Run availability check again
	manager.checkGroupsAvailability()

	// Verify group is now expired
	re.True(groupInfo.IsExpired())
	re.Equal(groupExpired, groupInfo.GetAvailability())

	// Verify scheduling is still disallowed
	groupState2 := manager.GetAffinityGroupState("expiration-test")
	re.NotNil(groupState2)
	re.False(groupState2.AffinitySchedulingAllowed)
}
