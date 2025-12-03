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

package server

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestInitDefaultResourceGroup(t *testing.T) {
	re := require.New(t)

	krgm := newKeyspaceResourceGroupManager(1, storage.NewStorageWithMemoryBackend())
	re.NotNil(krgm)
	re.Equal(uint32(1), krgm.keyspaceID)
	re.Empty(krgm.groups)

	// No default resource group initially.
	_, exists := krgm.groups[DefaultResourceGroupName]
	re.False(exists)

	// Initialize the default resource group.
	krgm.initDefaultResourceGroup()

	// Verify the default resource group is created.
	defaultGroup, exists := krgm.groups[DefaultResourceGroupName]
	re.True(exists)
	re.Equal(DefaultResourceGroupName, defaultGroup.Name)
	re.Equal(rmpb.GroupMode_RUMode, defaultGroup.Mode)
	re.Equal(uint32(middlePriority), defaultGroup.Priority)

	// Verify the default resource group has unlimited rate and burst limit.
	re.Equal(float64(UnlimitedRate), defaultGroup.getFillRate())
	re.Equal(int64(UnlimitedBurstLimit), defaultGroup.getBurstLimit())
}

func TestAddResourceGroup(t *testing.T) {
	re := require.New(t)

	krgm := newKeyspaceResourceGroupManager(1, storage.NewStorageWithMemoryBackend())

	// Test adding invalid resource group (empty name).
	group := &rmpb.ResourceGroup{
		Name: "",
		Mode: rmpb.GroupMode_RUMode,
	}
	err := krgm.addResourceGroup(group)
	re.Error(err)
	// Test adding invalid resource group (too long name).
	group = &rmpb.ResourceGroup{
		Name: "test_the_resource_group_name_is_too_long",
		Mode: rmpb.GroupMode_RUMode,
	}
	err = krgm.addResourceGroup(group)
	re.Error(err)

	// Test adding a valid resource group.
	group = &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}
	err = krgm.addResourceGroup(group)
	re.NoError(err)

	// Verify the group was added.
	addedGroup, exists := krgm.groups["test_group"]
	re.True(exists)
	re.Equal(group.GetName(), addedGroup.Name)
	re.Equal(group.GetMode(), addedGroup.Mode)
	re.Equal(group.GetPriority(), addedGroup.Priority)
	re.Equal(
		float64(group.GetRUSettings().GetRU().GetSettings().GetFillRate()),
		addedGroup.getFillRate(),
	)
	re.Equal(group.GetRUSettings().GetRU().GetSettings().GetBurstLimit(), addedGroup.RUSettings.RU.getBurstLimitSetting())
}

func TestModifyResourceGroup(t *testing.T) {
	re := require.New(t)

	krgm := newKeyspaceResourceGroupManager(1, storage.NewStorageWithMemoryBackend())

	// Add a resource group first.
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}
	err := krgm.addResourceGroup(group)
	re.NoError(err)

	// Modify the resource group.
	modifiedGroup := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 10,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   200,
					BurstLimit: 300,
				},
			},
		},
	}
	err = krgm.modifyResourceGroup(modifiedGroup)
	re.NoError(err)

	// Verify the group was modified.
	updatedGroup, exists := krgm.groups["test_group"]
	re.True(exists)
	re.Equal(modifiedGroup.GetName(), updatedGroup.Name)
	re.Equal(modifiedGroup.GetPriority(), updatedGroup.Priority)
	re.Equal(
		float64(modifiedGroup.GetRUSettings().GetRU().GetSettings().GetFillRate()),
		updatedGroup.getFillRate(),
	)
	re.Equal(modifiedGroup.GetRUSettings().GetRU().GetSettings().GetBurstLimit(), updatedGroup.RUSettings.RU.getBurstLimitSetting())

	// Try to modify a non-existent group.
	nonExistentGroup := &rmpb.ResourceGroup{
		Name: "non_existent",
		Mode: rmpb.GroupMode_RUMode,
	}
	err = krgm.modifyResourceGroup(nonExistentGroup)
	re.Error(err)
}

func TestDeleteResourceGroup(t *testing.T) {
	re := require.New(t)

	krgm := newKeyspaceResourceGroupManager(1, storage.NewStorageWithMemoryBackend())

	// Add a resource group first.
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
	}
	err := krgm.addResourceGroup(group)
	re.NoError(err)

	// Verify the group exists.
	re.NotNil(krgm.getResourceGroup(group.GetName(), false))

	// Delete the group.
	err = krgm.deleteResourceGroup(group.GetName())
	re.NoError(err)

	// Verify the group was deleted.
	re.Nil(krgm.getResourceGroup(group.GetName(), false))

	// Try to delete the default group.
	krgm.initDefaultResourceGroup()
	err = krgm.deleteResourceGroup(DefaultResourceGroupName)
	re.Error(err) // Should not be able to delete default group.

	// Verify default group still exists.
	re.NotNil(krgm.getResourceGroup(DefaultResourceGroupName, false))
}

func TestGetResourceGroup(t *testing.T) {
	re := require.New(t)

	krgm := newKeyspaceResourceGroupManager(1, storage.NewStorageWithMemoryBackend())

	// Add a resource group.
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}
	err := krgm.addResourceGroup(group)
	re.NoError(err)

	// Get the resource group without stats.
	retrievedGroup := krgm.getResourceGroup(group.GetName(), false)
	re.NotNil(retrievedGroup)
	re.Equal(group.GetName(), retrievedGroup.Name)
	re.Equal(group.GetMode(), retrievedGroup.Mode)
	re.Equal(group.GetPriority(), retrievedGroup.Priority)

	// Get a non-existent group.
	nonExistentGroup := krgm.getResourceGroup("non_existent", false)
	re.Nil(nonExistentGroup)
}

func TestGetResourceGroupList(t *testing.T) {
	re := require.New(t)

	krgm := newKeyspaceResourceGroupManager(1, storage.NewStorageWithMemoryBackend())

	// Add some resource groups.
	for i := 1; i <= 3; i++ {
		name := "group" + string(rune('0'+i))
		group := &rmpb.ResourceGroup{
			Name:     name,
			Mode:     rmpb.GroupMode_RUMode,
			Priority: uint32(i),
		}
		err := krgm.addResourceGroup(group)
		re.NoError(err)
	}

	// Get all resource groups.
	groups := krgm.getResourceGroupList(false, false)
	re.Len(groups, 3)

	// Verify groups are sorted by name.
	re.Equal("group1", groups[0].Name)
	re.Equal("group2", groups[1].Name)
	re.Equal("group3", groups[2].Name)

	krgm.initDefaultResourceGroup()
	groups = krgm.getResourceGroupList(false, true)
	re.Len(groups, 4)
	groups = krgm.getResourceGroupList(false, false)
	re.Len(groups, 3)
}

func TestAddResourceGroupFromRaw(t *testing.T) {
	re := require.New(t)

	krgm := newKeyspaceResourceGroupManager(1, storage.NewStorageWithMemoryBackend())

	// Create a resource group.
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}

	// Marshal to bytes.
	data, err := proto.Marshal(group)
	re.NoError(err)

	// Add from raw.
	err = krgm.addResourceGroupFromRaw(group.GetName(), string(data))
	re.NoError(err)

	// Verify the group was added correctly.
	addedGroup, exists := krgm.groups[group.GetName()]
	re.True(exists)
	re.Equal(group.GetName(), addedGroup.Name)
	re.Equal(group.GetMode(), addedGroup.Mode)
	re.Equal(group.GetPriority(), addedGroup.Priority)

	// Test with invalid raw value.
	err = krgm.addResourceGroupFromRaw(group.GetName(), "invalid_data")
	re.Error(err)
}

func TestSetRawStatesIntoResourceGroup(t *testing.T) {
	re := require.New(t)

	krgm := newKeyspaceResourceGroupManager(1, storage.NewStorageWithMemoryBackend())

	// Add a resource group first.
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}
	err := krgm.addResourceGroup(group)
	re.NoError(err)

	// Create group states.
	tokens := 150.0
	lastUpdate := time.Now()
	states := &GroupStates{
		RU: &GroupTokenBucketState{
			Tokens:     tokens,
			LastUpdate: &lastUpdate,
		},
		RUConsumption: &rmpb.Consumption{
			RRU: 50,
			WRU: 30,
		},
	}

	// Marshal to JSON.
	data, err := json.Marshal(states)
	re.NoError(err)

	// Set raw states.
	err = krgm.setRawStatesIntoResourceGroup(group.GetName(), string(data))
	re.NoError(err)

	// Verify states were updated.
	updatedGroup := krgm.groups[group.GetName()]
	re.InDelta(tokens, updatedGroup.RUSettings.RU.Tokens, 0.001)
	re.Equal(states.RUConsumption.RRU, updatedGroup.RUConsumption.RRU)
	re.Equal(states.RUConsumption.WRU, updatedGroup.RUConsumption.WRU)

	// Test with invalid raw value.
	err = krgm.setRawStatesIntoResourceGroup(group.GetName(), "invalid_data")
	re.Error(err)
}

func TestPersistResourceGroupRunningState(t *testing.T) {
	re := require.New(t)

	storage := storage.NewStorageWithMemoryBackend()
	krgm := newKeyspaceResourceGroupManager(1, storage)

	// Add a resource group
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}
	err := krgm.addResourceGroup(group)
	re.NoError(err)

	// Check the states before persist.
	err = storage.LoadResourceGroupStates(func(keyspaceID uint32, name, rawValue string) {
		re.Equal(uint32(1), keyspaceID)
		re.Equal(group.GetName(), name)
		states := &GroupStates{}
		err := json.Unmarshal([]byte(rawValue), states)
		re.NoError(err)
		re.Equal(0.0, states.RU.Tokens)
	})
	re.NoError(err)

	mutableGroup := krgm.getMutableResourceGroup(group.GetName())
	mutableGroup.RUSettings.RU.Tokens = 100.0
	// Persist the running state.
	krgm.persistResourceGroupRunningState()

	// Verify state was persisted.
	err = storage.LoadResourceGroupStates(func(keyspaceID uint32, name, rawValue string) {
		re.Equal(uint32(1), keyspaceID)
		re.Equal(group.GetName(), name)
		states := &GroupStates{}
		err := json.Unmarshal([]byte(rawValue), states)
		re.NoError(err)
		re.Equal(mutableGroup.RUSettings.RU.Tokens, states.RU.Tokens)
	})
	re.NoError(err)
}

func TestRUTracker(t *testing.T) {
	const floatDelta = 0.1
	re := require.New(t)

	rt := newRUTracker(time.Second)
	now := time.Now()
	rt.sample(now, 100)
	re.Zero(rt.getRUPerSec())
	now = now.Add(time.Second)
	rt.sample(now, 100)
	re.Equal(100.0, rt.getRUPerSec())
	now = now.Add(time.Second)
	rt.sample(now, 100)
	re.InDelta(100.0, rt.getRUPerSec(), floatDelta)
	now = now.Add(time.Second)
	rt.sample(now, 200)
	re.InDelta(150.0, rt.getRUPerSec(), floatDelta)
	// EMA should eventually converge to 10000 RU/s.
	const targetRUPerSec = 10000.0
	testutil.Eventually(re, func() bool {
		now = now.Add(time.Second)
		rt.sample(now, targetRUPerSec)
		return math.Abs(rt.getRUPerSec()-targetRUPerSec) < floatDelta
	})
}

func TestGroupRUTracker(t *testing.T) {
	const floatDelta = 0.1
	re := require.New(t)

	grt := newGroupRUTracker()
	re.NotNil(grt)
	re.NotNil(grt.ruTrackers)
	re.Empty(grt.ruTrackers)

	now := time.Now()
	totalRUPerSec := 0.0
	for i := range 10 {
		clientUniqueID := uint64(i)
		grt.sample(clientUniqueID, now, 100)
		grt.sample(clientUniqueID, now.Add(time.Second), 100)
		ruPerSec := grt.getOrCreateRUTracker(clientUniqueID).getRUPerSec()
		re.InDelta(100.0, ruPerSec, floatDelta)
		totalRUPerSec += ruPerSec
	}

	re.Len(grt.ruTrackers, 10)
	re.InDelta(totalRUPerSec, grt.getRUPerSec(), floatDelta)
}

func TestPersistAndReloadIntegrity(t *testing.T) {
	re := require.New(t)
	storage := storage.NewStorageWithMemoryBackend()
	keyspaceID := uint32(101)
	groupName := "persist_test_group"

	// Add resource group to storage
	krgm := newKeyspaceResourceGroupManager(keyspaceID, storage)
	groupProto := &rmpb.ResourceGroup{
		Name:     groupName,
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 10,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{FillRate: 500},
			},
		},
	}
	err := krgm.addResourceGroup(groupProto)
	re.NoError(err)

	// Modify the resource group to set initial state
	mutableGroup := krgm.getMutableResourceGroup(groupName)
	re.NotNil(mutableGroup)
	mutableGroup.RUSettings.RU.Tokens = 12345.67
	mutableGroup.RUConsumption = &rmpb.Consumption{RRU: 100, WRU: 200}

	// Persist the resource group running state
	krgm.persistResourceGroupRunningState()

	// Load the resource group settings and states from storage
	foundSettings := false
	err = storage.LoadResourceGroupSettings(func(kid uint32, name string, rawValue string) {
		if kid == keyspaceID && name == groupName {
			foundSettings = true
			groupSetting := &rmpb.ResourceGroup{}
			err = proto.Unmarshal([]byte(rawValue), groupSetting)
			re.NoError(err)
			re.NotNil(groupSetting.KeyspaceId)
			re.Equal(keyspaceID, groupSetting.KeyspaceId.Value)
			re.Equal(uint64(500), groupSetting.RUSettings.RU.Settings.FillRate)
		}
	})
	re.NoError(err)
	re.True(foundSettings)

	foundStates := false
	err = storage.LoadResourceGroupStates(func(kid uint32, name string, rawValue string) {
		if kid == keyspaceID && name == groupName {
			foundStates = true
			loadedStates := &GroupStates{}
			err = json.Unmarshal([]byte(rawValue), loadedStates)
			re.NoError(err)
			re.NotNil(loadedStates.RU)
			re.InDelta(12345.67, loadedStates.RU.Tokens, 0.001)
			re.NotNil(loadedStates.RUConsumption)
			re.Equal(float64(100), loadedStates.RUConsumption.RRU)
		}
	})
	re.NoError(err)
	re.True(foundStates)

	// Reload the keyspace resource group manager
	reloadedManager := newKeyspaceResourceGroupManager(keyspaceID, storage)
	err = storage.LoadResourceGroupSettings(func(kid uint32, name string, rawValue string) {
		if kid == keyspaceID {
			re.NoError(reloadedManager.addResourceGroupFromRaw(name, rawValue))
		}
	})
	re.NoError(err)
	err = storage.LoadResourceGroupStates(func(kid uint32, name string, rawValue string) {
		if kid == keyspaceID {
			re.NoError(reloadedManager.setRawStatesIntoResourceGroup(name, rawValue))
		}
	})
	re.NoError(err)

	reloadedGroup := reloadedManager.getResourceGroup(groupName, true)
	re.NotNil(reloadedGroup)
	re.Equal(groupName, reloadedGroup.Name)
	re.InDelta(12345.67, reloadedGroup.RUSettings.RU.Tokens, 0.001)
	re.Equal(float64(100), reloadedGroup.RUConsumption.RRU)
	re.Equal(float64(200), reloadedGroup.RUConsumption.WRU)
	re.Equal(uint32(10), reloadedGroup.Priority)
}

func TestGetPriorityQueues(t *testing.T) {
	re := require.New(t)

	krgm := newKeyspaceResourceGroupManager(1, storage.NewStorageWithMemoryBackend())

	// Add some resource groups with different priorities.
	groups := map[uint32]*rmpb.ResourceGroup{
		1: {
			Name:     "group_with_priority_1",
			Mode:     rmpb.GroupMode_RUMode,
			Priority: 1,
		},
		2: {
			Name:     "group_with_priority_2",
			Mode:     rmpb.GroupMode_RUMode,
			Priority: 2,
		},
		3: {
			Name:     "group_with_priority_3",
			Mode:     rmpb.GroupMode_RUMode,
			Priority: 3,
		},
	}
	for _, group := range groups {
		err := krgm.addResourceGroup(group)
		re.NoError(err)
	}
	// Verify the priority queues.
	priorityQueues := krgm.getPriorityQueues()
	re.Len(priorityQueues, 3)
	// Check if the priority queues are sorted in descending order.
	for i := range len(priorityQueues) - 1 {
		re.Greater(priorityQueues[i][0].Priority, priorityQueues[i+1][0].Priority)
	}
	// Check if the priority queues are correct.
	for _, queue := range priorityQueues {
		group := queue[0]
		re.Equal(groups[group.Priority].Name, group.Name)
	}
}

func TestOverrideFillRate(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		originalFillRate float64
		overrideFillRate float64
		expectedFillRate float64
	}{
		{
			originalFillRate: -1,
			overrideFillRate: -1,
			expectedFillRate: -1,
		},
		{
			originalFillRate: -1,
			overrideFillRate: 0,
			expectedFillRate: 0,
		},
		{
			originalFillRate: -1,
			overrideFillRate: 100,
			expectedFillRate: 100,
		},
		{
			originalFillRate: 0,
			overrideFillRate: -1,
			expectedFillRate: -1,
		},
		{
			originalFillRate: 0,
			overrideFillRate: 0,
			expectedFillRate: 0,
		},
		{
			originalFillRate: 0,
			overrideFillRate: 100,
			expectedFillRate: 100,
		},
		{
			originalFillRate: 100,
			overrideFillRate: -1,
			expectedFillRate: -1,
		},
		{
			originalFillRate: 100,
			overrideFillRate: 0,
			expectedFillRate: 0,
		},
		{
			originalFillRate: 100,
			overrideFillRate: 96,
			expectedFillRate: 100,
		},
		{
			originalFillRate: 100,
			overrideFillRate: 95,
			expectedFillRate: 100,
		},
		{
			originalFillRate: 100,
			overrideFillRate: 94,
			expectedFillRate: 94,
		},
		{
			originalFillRate: 100,
			overrideFillRate: 104,
			expectedFillRate: 100,
		},
		{
			originalFillRate: 100,
			overrideFillRate: 105,
			expectedFillRate: 100,
		},
		{
			originalFillRate: 100,
			overrideFillRate: 106,
			expectedFillRate: 106,
		},
	}
	krgm := newKeyspaceResourceGroupManager(1, storage.NewStorageWithMemoryBackend())
	groupName := "test_group"
	group := &rmpb.ResourceGroup{
		Name: groupName,
		Mode: rmpb.GroupMode_RUMode,
	}
	err := krgm.addResourceGroup(group)
	re.NoError(err)
	for idx, tc := range testCases {
		group := krgm.getMutableResourceGroup(groupName)
		group.RUSettings.RU.overrideFillRate = tc.originalFillRate
		group.overrideFillRate(tc.overrideFillRate)
		re.Equal(tc.expectedFillRate, group.RUSettings.RU.overrideFillRate, "case %d", idx)
	}
}

func TestConciliateFillRate(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		name                           string
		serviceLimit                   float64
		priorityList                   []uint32
		fillRateSettingList            []uint64
		burstLimitSettingList          []int64 // If not provided, the burst limit is set to the same as the fill rate.
		ruDemandList                   []float64
		expectedOverrideFillRateList   []float64
		expectedOverrideBurstLimitList []int64
	}{
		{
			name:                "One priority with sufficient service limit",
			serviceLimit:        100,
			priorityList:        []uint32{1, 1, 1},
			fillRateSettingList: []uint64{10, 20, 30},
			ruDemandList:        []float64{10, 20, 30},
			// Total demand: 60 < service limit 100, so all groups get their original settings
			expectedOverrideFillRateList:   []float64{-1, -1, -1}, // -1 means use original fill rate settings
			expectedOverrideBurstLimitList: []int64{-1, -1, -1},   // -1 means use original burst limit settings
		},
		{
			name:                "One priority with insufficient service limit",
			serviceLimit:        50,
			priorityList:        []uint32{1, 1, 1},
			fillRateSettingList: []uint64{20, 30, 50},
			ruDemandList:        []float64{20, 30, 50},
			// Total demand: 100 > service limit 50, normalized demand: 20/20=1, 30/30=1, 50/50=1
			// Proportional allocation: 50*20/100=10, 50*30/100=15, 50*50/100=25
			expectedOverrideFillRateList:   []float64{10, 15, 25},
			expectedOverrideBurstLimitList: []int64{10, 15, 25}, // All groups are non-burstable, so burst limit is set to the same as fill rate
		},
		{
			name:                "Multiple priorities with sufficient service limit",
			serviceLimit:        200,
			priorityList:        []uint32{3, 3, 2, 2, 1},
			fillRateSettingList: []uint64{20, 30, 25, 35, 40},
			ruDemandList:        []float64{10, 15, 20, 30, 40},
			// Total demand: 115 < service limit 200, so all groups get their original settings
			expectedOverrideFillRateList:   []float64{-1, -1, -1, -1, -1},
			expectedOverrideBurstLimitList: []int64{-1, -1, -1, -1, -1},
		},
		{
			name:                "Multiple priorities with insufficient service limit - higher groups get partial",
			serviceLimit:        80,
			priorityList:        []uint32{3, 3, 2, 1, 1},
			fillRateSettingList: []uint64{30, 20, 30, 20, 10},
			ruDemandList:        []float64{30, 20, 30, 20, 10},
			// Priority 3: demand 50, gets full allocation (remaining: 30)
			// Priority 2: demand 30, gets full allocation (remaining: 0)
			// Priority 1: demand 30, gets 0 (no remaining service limit)
			expectedOverrideFillRateList:   []float64{-1, -1, 30, 0, 0},
			expectedOverrideBurstLimitList: []int64{-1, -1, 30, 0, 0},
		},
		{
			name:                "Multiple priorities with insufficient service limit - higher groups get all",
			serviceLimit:        100,
			priorityList:        []uint32{5, 5, 3, 2, 1},
			fillRateSettingList: []uint64{60, 60, 30, 20, 10},
			ruDemandList:        []float64{60, 60, 30, 20, 10},
			// Priority 5: demand 120 > service limit 100, proportional allocation: 100*(60/120)=50, 100*(60/120)=50
			// Lower priorities get 0 (no remaining service limit)
			expectedOverrideFillRateList:   []float64{50, 50, 0, 0, 0},
			expectedOverrideBurstLimitList: []int64{50, 50, 0, 0, 0},
		},
		{
			name:                "Zero service limit",
			serviceLimit:        0,
			priorityList:        []uint32{3, 2, 1},
			fillRateSettingList: []uint64{10, 20, 30},
			ruDemandList:        []float64{10, 20, 30},
			// Service limit is 0, so no conciliation is performed, all groups keep original settings
			expectedOverrideFillRateList:   []float64{-1, -1, -1},
			expectedOverrideBurstLimitList: []int64{-1, -1, -1},
		},
		{
			name:                "Zero demand from all groups",
			serviceLimit:        100,
			priorityList:        []uint32{3, 2, 1},
			fillRateSettingList: []uint64{10, 20, 30},
			ruDemandList:        []float64{0, 0, 0},
			// No demand from any group, so all groups keep their original settings
			expectedOverrideFillRateList:   []float64{-1, -1, -1},
			expectedOverrideBurstLimitList: []int64{-1, -1, -1},
		},
		{
			name:                "Partial throttling across priorities - priority 3 gets full, priority 2 gets partial, priority 1 gets none",
			serviceLimit:        120,
			priorityList:        []uint32{3, 3, 2, 2, 1},
			fillRateSettingList: []uint64{40, 30, 30, 30, 20},
			ruDemandList:        []float64{40, 30, 30, 30, 20},
			// Priority 3: demand 70, gets full allocation (remaining: 50)
			// Priority 2: demand 60 > remaining 50, proportional allocation: 50*(30/60)=25, 50*(30/60)=25
			// Priority 1: demand 20, gets 0 (no remaining service limit)
			expectedOverrideFillRateList:   []float64{-1, -1, 25, 25, 0}, // Priority 3 gets full, priority 2 gets partial, priority 1 gets none
			expectedOverrideBurstLimitList: []int64{-1, -1, 25, 25, 0},
		},
		{
			name:                  "Burst demand with insufficient service limit for all",
			serviceLimit:          70,
			priorityList:          []uint32{2, 2, 1},
			fillRateSettingList:   []uint64{20, 30, 40},
			burstLimitSettingList: []int64{20, 35, 70},
			ruDemandList:          []float64{25, 50, 65},
			// Priority 2: basic 20+30=50, busrt 0+20=20, total 70
			// 	 - All groups get their basic demand satisfied (remaining: 70-20-30=20)
			//   - group 0 is non-burstable, so it gets no extra burst supply. (remaining: 20)
			//   - group 1 is burstable, but 35 < 30+20, so it still gets the 35 burst supply. (remaining: 20-(35-30)=15)
			// Priority 1: demand 65 > remaining 15, gets 15 basic and 0 burst
			expectedOverrideFillRateList:   []float64{-1, -1, 15},
			expectedOverrideBurstLimitList: []int64{20, 35, 15},
		},
		{
			name:                  "Mixed burst scenarios - some with burst, some without",
			serviceLimit:          100,
			priorityList:          []uint32{2, 2, 1, 1},
			fillRateSettingList:   []uint64{30, 40, 20, 30},
			burstLimitSettingList: []int64{30, -1, 20, 30},
			ruDemandList:          []float64{35, 45, 22, 20},
			// Priority 2: basic 30+40=70, burst 0+5=5, total 75 < service limit 100, so gets full allocation (remaining: 25)
			// Priority 1: basic 20+30=50, burst 2+0=2, total 52 > service limit 25, so gets basic demand proportionally
			expectedOverrideFillRateList:   []float64{-1, -1, 10, 15},
			expectedOverrideBurstLimitList: []int64{-1, 100, 10, 15},
		},
		{
			name:                  "Unlimited burst limit with service limit constraint",
			serviceLimit:          100,
			priorityList:          []uint32{2, 1, 1},
			fillRateSettingList:   []uint64{20, 30, 40},
			burstLimitSettingList: []int64{-1, -1, -1},
			ruDemandList:          []float64{30, 50, 60}, // Basic: 20,30,40=90; Burst: 10,20,20=50; Total: 140
			// Priority 2: demand 30, gets full allocation (remaining: 70)
			// Priority 1: demand 110 > remaining 70, basic demand 70 = remaining 70, proportional allocation: 70*(30/70)=30, 70*(40/70)=40
			expectedOverrideFillRateList:   []float64{-1, 30, 40},
			expectedOverrideBurstLimitList: []int64{100, 30, 40},
		},
		{
			name:                  "Partial burst demand satisfied with unlimited burst limit",
			serviceLimit:          60,
			priorityList:          []uint32{2, 2, 1},
			fillRateSettingList:   []uint64{20, 30, 40},
			burstLimitSettingList: []int64{-1, -1, -1},
			ruDemandList:          []float64{35, 40, 55}, // Basic: 20,30,40=90; Burst: 15,10,15=40; Total: 130
			// Priority 2: demand 75 > service limit 60, basic demand 50 < service limit 60, basic satisfied, burst gets: 60-50=10
			// Burst allocation: 10*(15/25)=6, 10*(10/25)=4, so burst limits: 20+6=26, 30+4=34
			// Priority 1: gets 0 (no remaining service limit)
			expectedOverrideFillRateList:   []float64{-1, -1, 0},
			expectedOverrideBurstLimitList: []int64{26, 34, 0}, // 20+10*15/(15+10), 30+10*10/(15+10), 0
		},
		{
			name:                  "Default group with unlimited burst limit",
			serviceLimit:          100,
			priorityList:          []uint32{1},
			fillRateSettingList:   []uint64{UnlimitedRate},
			burstLimitSettingList: []int64{UnlimitedBurstLimit},
			ruDemandList:          []float64{1000},
			// Default group with unlimited settings, basic demand = min(1000, unlimitedRate) = 1000
			// Basic demand 1000 > service limit 100, so gets proportional allocation: 100*(1000/1000)=100
			expectedOverrideFillRateList:   []float64{100},
			expectedOverrideBurstLimitList: []int64{100},
		},
		{
			name:                           "Proportional allocation with normalization check - case 1",
			serviceLimit:                   100,
			priorityList:                   []uint32{1, 1, 1},
			fillRateSettingList:            []uint64{100, 100, 100},
			ruDemandList:                   []float64{40, 80, 10},
			expectedOverrideFillRateList:   []float64{40, 50, 10},
			expectedOverrideBurstLimitList: []int64{40, 50, 10},
		},
		{
			name:                           "Proportional allocation with normalization check - case 2",
			serviceLimit:                   90,
			priorityList:                   []uint32{1, 1, 1},
			fillRateSettingList:            []uint64{100, 100, 100},
			ruDemandList:                   []float64{50, 60, 70},
			expectedOverrideFillRateList:   []float64{30, 30, 30},
			expectedOverrideBurstLimitList: []int64{30, 30, 30},
		},
	}
	genGroupName := func(caseIdx, i int) string {
		return fmt.Sprintf("case_%d_group_%d", caseIdx, i)
	}
	for idx, tc := range testCases {
		krgm := newKeyspaceResourceGroupManager(1, storage.NewStorageWithMemoryBackend())
		// Set the service limit.
		krgm.setServiceLimit(tc.serviceLimit)
		// Add the resource groups.
		for i, priority := range tc.priorityList {
			fillRate := tc.fillRateSettingList[i]
			burstLimit := int64(fillRate)
			if len(tc.burstLimitSettingList) > 0 {
				burstLimit = tc.burstLimitSettingList[i]
			}
			group := &rmpb.ResourceGroup{
				Name:     genGroupName(idx, i),
				Mode:     rmpb.GroupMode_RUMode,
				Priority: priority,
				RUSettings: &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate:   fillRate,
							BurstLimit: burstLimit,
						},
					},
				},
			}
			err := krgm.addResourceGroup(group)
			re.NoError(err, "case %s, group %d", tc.name, i)
		}
		// Mock the RU demand of each resource group.
		now := time.Now()
		for i, ruDemand := range tc.ruDemandList {
			grt := krgm.getOrCreateGroupRUTracker(genGroupName(idx, i))
			// Warm up the RU tracker.
			grt.sample(0, now, 0)
			// Sample the RU demand.
			now = now.Add(time.Second)
			grt.sample(0, now, ruDemand)
		}
		// Conciliate the fill rate.
		krgm.conciliateFillRates()
		// Verify the override fill rate and burst limit of each resource group.
		for i, expectedOverrideFillRate := range tc.expectedOverrideFillRateList {
			group := krgm.getMutableResourceGroup(genGroupName(idx, i))
			re.Equal(
				expectedOverrideFillRate,
				group.getOverrideFillRate(),
				"check fill rate of case %s, group %d", tc.name, i,
			)
			var expectedOverrideBurstLimit int64
			if len(tc.expectedOverrideBurstLimitList) == 0 {
				expectedOverrideBurstLimit = -1
			} else {
				expectedOverrideBurstLimit = tc.expectedOverrideBurstLimitList[i]
			}
			re.Equal(
				expectedOverrideBurstLimit,
				group.getOverrideBurstLimit(),
				"check burst limit of case %s, group %d", tc.name, i,
			)
		}
		// Test the cleanup overrides.
		krgm.setServiceLimit(0)
		for i := range tc.priorityList {
			group := krgm.getMutableResourceGroup(genGroupName(idx, i))
			re.Equal(float64(-1), group.getOverrideFillRate(), "check fill rate of case %s, group %d", tc.name, i)
			re.Equal(int64(-1), group.getOverrideBurstLimit(), "check burst limit of case %s, group %d", tc.name, i)
		}
	}
}
