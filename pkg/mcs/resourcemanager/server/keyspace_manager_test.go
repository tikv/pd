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
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/testutil"
)

// mockCentralManager implements the centralManager interface for testing.
type mockCentralManager struct {
	mu      sync.RWMutex
	config  *ControllerConfig
	storage endpoint.ResourceGroupStorage
}

func newMockCentralManager() *mockCentralManager {
	config := &ControllerConfig{
		RequestUnit: RequestUnitConfig{
			CPUMsCost:             1.0,
			ReadBaseCost:          1.0,
			ReadCostPerByte:       1.0,
			ReadPerBatchBaseCost:  1.0,
			WriteBaseCost:         1.0,
			WriteCostPerByte:      1.0,
			WritePerBatchBaseCost: 1.0,
		},
	}
	return &mockCentralManager{
		config:  config,
		storage: storage.NewStorageWithMemoryBackend(),
	}
}

// GetControllerConfig implements the centralManager interface.
func (mcm *mockCentralManager) GetControllerConfig() *ControllerConfig {
	mcm.mu.RLock()
	defer mcm.mu.RUnlock()
	return mcm.config
}

// GetStorage implements the centralManager interface.
func (mcm *mockCentralManager) GetStorage() endpoint.ResourceGroupStorage {
	return mcm.storage
}

func TestNewKeyspaceResourceGroupManager(t *testing.T) {
	re := require.New(t)

	centralMgr := newMockCentralManager()
	krgm := newKeyspaceResourceGroupManager(1, centralMgr)

	re.NotNil(krgm)
	re.Equal(uint32(1), krgm.keyspaceID)
	re.Empty(krgm.groups)
	re.NotNil(krgm.consumptionDispatcher)
	re.NotNil(krgm.consumptionRecord)
}

func TestInitDefaultResourceGroup(t *testing.T) {
	re := require.New(t)

	centralMgr := newMockCentralManager()
	krgm := newKeyspaceResourceGroupManager(1, centralMgr)

	// No default resource group initially.
	_, exists := krgm.groups[reservedDefaultGroupName]
	re.False(exists)

	// Initialize the default resource group.
	krgm.initDefaultResourceGroup()

	// Verify the default resource group is created.
	defaultGroup, exists := krgm.groups[reservedDefaultGroupName]
	re.True(exists)
	re.Equal(reservedDefaultGroupName, defaultGroup.Name)
	re.Equal(resource_manager.GroupMode_RUMode, defaultGroup.Mode)
	re.Equal(uint32(middlePriority), defaultGroup.Priority)

	// Verify the default resource group has unlimited rate and burst limit.
	re.Equal(uint64(unlimitedRate), defaultGroup.RUSettings.RU.Settings.FillRate)
	re.Equal(int64(unlimitedBurstLimit), defaultGroup.RUSettings.RU.Settings.BurstLimit)
}

func TestAddResourceGroup(t *testing.T) {
	re := require.New(t)

	centralMgr := newMockCentralManager()
	krgm := newKeyspaceResourceGroupManager(1, centralMgr)

	// Test adding invalid resource group (empty name).
	group := &resource_manager.ResourceGroup{
		Name: "",
		Mode: resource_manager.GroupMode_RUMode,
	}
	err := krgm.addResourceGroup(group)
	re.Error(err)

	// Test adding a valid resource group.
	group = &resource_manager.ResourceGroup{
		Name:     "test_group",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
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
	re.Equal("test_group", addedGroup.Name)
	re.Equal(resource_manager.GroupMode_RUMode, addedGroup.Mode)
	re.Equal(uint32(5), addedGroup.Priority)
	re.Equal(uint64(100), addedGroup.RUSettings.RU.Settings.FillRate)
	re.Equal(int64(200), addedGroup.RUSettings.RU.Settings.BurstLimit)
}

func TestModifyResourceGroup(t *testing.T) {
	re := require.New(t)

	centralMgr := newMockCentralManager()
	krgm := newKeyspaceResourceGroupManager(1, centralMgr)

	// Add a resource group first.
	group := &resource_manager.ResourceGroup{
		Name:     "test_group",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}
	err := krgm.addResourceGroup(group)
	re.NoError(err)

	// Modify the resource group.
	modifiedGroup := &resource_manager.ResourceGroup{
		Name:     "test_group",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 10,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
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
	re.Equal("test_group", updatedGroup.Name)
	re.Equal(uint32(10), updatedGroup.Priority)
	re.Equal(uint64(200), updatedGroup.RUSettings.RU.Settings.FillRate)
	re.Equal(int64(300), updatedGroup.RUSettings.RU.Settings.BurstLimit)

	// Try to modify a non-existent group.
	nonExistentGroup := &resource_manager.ResourceGroup{
		Name: "non_existent",
		Mode: resource_manager.GroupMode_RUMode,
	}
	err = krgm.modifyResourceGroup(nonExistentGroup)
	re.Error(err)
}

func TestDeleteResourceGroup(t *testing.T) {
	re := require.New(t)

	centralMgr := newMockCentralManager()
	krgm := newKeyspaceResourceGroupManager(1, centralMgr)

	// Add a resource group first.
	group := &resource_manager.ResourceGroup{
		Name:     "test_group",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 5,
	}
	err := krgm.addResourceGroup(group)
	re.NoError(err)

	// Verify the group exists.
	_, exists := krgm.groups["test_group"]
	re.True(exists)

	// Delete the group.
	err = krgm.deleteResourceGroup("test_group")
	re.NoError(err)

	// Verify the group was deleted.
	_, exists = krgm.groups["test_group"]
	re.False(exists)

	// Try to delete the default group.
	krgm.initDefaultResourceGroup()
	err = krgm.deleteResourceGroup(reservedDefaultGroupName)
	re.Error(err) // Should not be able to delete default group.

	// Verify default group still exists.
	_, exists = krgm.groups[reservedDefaultGroupName]
	re.True(exists)
}

func TestGetResourceGroup(t *testing.T) {
	re := require.New(t)

	centralMgr := newMockCentralManager()
	krgm := newKeyspaceResourceGroupManager(1, centralMgr)

	// Add a resource group.
	group := &resource_manager.ResourceGroup{
		Name:     "test_group",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}
	err := krgm.addResourceGroup(group)
	re.NoError(err)

	// Get the resource group without stats.
	retrievedGroup := krgm.getResourceGroup("test_group", false)
	re.NotNil(retrievedGroup)
	re.Equal("test_group", retrievedGroup.Name)
	re.Equal(resource_manager.GroupMode_RUMode, retrievedGroup.Mode)
	re.Equal(uint32(5), retrievedGroup.Priority)

	// Get a non-existent group.
	nonExistentGroup := krgm.getResourceGroup("non_existent", false)
	re.Nil(nonExistentGroup)
}

func TestGetResourceGroupList(t *testing.T) {
	re := require.New(t)

	centralMgr := newMockCentralManager()
	krgm := newKeyspaceResourceGroupManager(1, centralMgr)

	// Add some resource groups.
	for i := 1; i <= 3; i++ {
		name := "group" + string(rune('0'+i))
		group := &resource_manager.ResourceGroup{
			Name:     name,
			Mode:     resource_manager.GroupMode_RUMode,
			Priority: uint32(i),
		}
		err := krgm.addResourceGroup(group)
		re.NoError(err)
	}

	// Get all resource groups.
	groups := krgm.getResourceGroupList(false)
	re.Len(groups, 3)

	// Verify groups are sorted by name.
	re.Equal("group1", groups[0].Name)
	re.Equal("group2", groups[1].Name)
	re.Equal("group3", groups[2].Name)
}

func TestAddResourceGroupFromRaw(t *testing.T) {
	re := require.New(t)

	centralMgr := newMockCentralManager()
	krgm := newKeyspaceResourceGroupManager(1, centralMgr)

	// Create a resource group.
	group := &resource_manager.ResourceGroup{
		Name:     "test_group",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
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
	err = krgm.addResourceGroupFromRaw("test_group", string(data))
	re.NoError(err)

	// Verify the group was added correctly.
	addedGroup, exists := krgm.groups["test_group"]
	re.True(exists)
	re.Equal("test_group", addedGroup.Name)
	re.Equal(resource_manager.GroupMode_RUMode, addedGroup.Mode)
	re.Equal(uint32(5), addedGroup.Priority)

	// Test with invalid raw value.
	err = krgm.addResourceGroupFromRaw("invalid", "invalid_data")
	re.Error(err)
}

func TestSetRawStatesIntoResourceGroup(t *testing.T) {
	re := require.New(t)

	centralMgr := newMockCentralManager()
	krgm := newKeyspaceResourceGroupManager(1, centralMgr)

	// Add a resource group first.
	group := &resource_manager.ResourceGroup{
		Name:     "test_group",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
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
		RUConsumption: &resource_manager.Consumption{
			RRU: 50,
			WRU: 30,
		},
	}

	// Marshal to JSON.
	data, err := json.Marshal(states)
	re.NoError(err)

	// Set raw states.
	err = krgm.setRawStatesIntoResourceGroup("test_group", string(data))
	re.NoError(err)

	// Verify states were updated.
	updatedGroup := krgm.groups["test_group"]
	re.InDelta(tokens, updatedGroup.RUSettings.RU.Tokens, 0.001)
	re.Equal(float64(50), updatedGroup.RUConsumption.RRU)
	re.Equal(float64(30), updatedGroup.RUConsumption.WRU)

	// Test with invalid raw value.
	err = krgm.setRawStatesIntoResourceGroup("test_group", "invalid_data")
	re.Error(err)
}

func TestBackgroundMetricsFlush(t *testing.T) {
	re := require.New(t)

	centralMgr := newMockCentralManager()
	krgm := newKeyspaceResourceGroupManager(1, centralMgr)

	// Add a resource group.
	group := &resource_manager.ResourceGroup{
		Name:     "test_group",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}
	err := krgm.addResourceGroup(group)
	re.NoError(err)

	// Start the background metrics flush.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go krgm.backgroundMetricsFlush(ctx)

	// Send consumption to the dispatcher.
	krgm.consumptionDispatcher <- &consumptionItem{
		resourceGroupName: "test_group",
		Consumption: &resource_manager.Consumption{
			RRU:               10.0,
			WRU:               20.0,
			ReadBytes:         1000,
			WriteBytes:        2000,
			TotalCpuTimeMs:    30,
			SqlLayerCpuTimeMs: 10,
			KvReadRpcCount:    5,
			KvWriteRpcCount:   10,
		},
	}

	// Verify consumption was added to the resource group.
	testutil.Eventually(re, func() bool {
		updatedGroup := krgm.getResourceGroup("test_group", true)
		re.NotNil(updatedGroup)
		return updatedGroup.RUConsumption.RRU == 10.0 && updatedGroup.RUConsumption.WRU == 20.0
	})
}

func TestPersistResourceGroupRunningState(t *testing.T) {
	re := require.New(t)

	centralMgr := newMockCentralManager()
	krgm := newKeyspaceResourceGroupManager(1, centralMgr)

	// Add a resource group
	group := &resource_manager.ResourceGroup{
		Name:     "test_group",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
	}
	err := krgm.addResourceGroup(group)
	re.NoError(err)

	// Check the states before persist.
	centralMgr.GetStorage().LoadResourceGroupStates(func(keyspaceID uint32, name, rawValue string) {
		re.Equal(uint32(1), keyspaceID)
		re.Equal("test_group", name)
		states := &GroupStates{}
		err := json.Unmarshal([]byte(rawValue), states)
		re.NoError(err)
		re.Equal(0.0, states.RU.Tokens)
	})

	mutableGroup := krgm.getMutableResourceGroup("test_group")
	mutableGroup.RUSettings.RU.Tokens = 100.0
	// Persist the running state.
	krgm.persistResourceGroupRunningState()

	// Verify state was persisted.
	centralMgr.GetStorage().LoadResourceGroupStates(func(keyspaceID uint32, name, rawValue string) {
		re.Equal(uint32(1), keyspaceID)
		re.Equal("test_group", name)
		states := &GroupStates{}
		err := json.Unmarshal([]byte(rawValue), states)
		re.NoError(err)
		re.Equal(100.0, states.RU.Tokens)
	})
}
