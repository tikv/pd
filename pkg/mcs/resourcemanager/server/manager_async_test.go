// Copyright 2024 TiKV Project Authors.
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
	keyspacepb "github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
)

// mockStorage implements ResourceGroupStorage interface for testing
// It can be controlled via channels to block bulk loading operations
type mockStorage struct {
	mu       sync.RWMutex
	settings map[string]string
	states   map[string]string
	config   string

	// Control channels for blocking operations
	blockBeforeLoad chan struct{} // Block before starting load
	blockAfterLoad  chan struct{} // Block after loading is completed
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		settings: make(map[string]string),
		states:   make(map[string]string),

		// Initialize blocking channels
		blockBeforeLoad: make(chan struct{}),
		blockAfterLoad:  make(chan struct{}),
	}
}

// BlockBeforeLoad blocks before starting the load operation
func (m *mockStorage) BlockBeforeLoad() {
	<-m.blockBeforeLoad
}

// UnblockBeforeLoad unblocks the load operation
func (m *mockStorage) UnblockBeforeLoad() {
	close(m.blockBeforeLoad)
}

// BlockAfterLoad blocks after loading is completed
func (m *mockStorage) BlockAfterLoad() {
	<-m.blockAfterLoad
}

// UnblockAfterLoad unblocks the load operation
func (m *mockStorage) UnblockAfterLoad() {
	close(m.blockAfterLoad)
}

func (m *mockStorage) LoadResourceGroupSettings(f func(k, v string)) error {
	// Block until unblocked
	m.BlockBeforeLoad()

	m.mu.RLock()
	for k, v := range m.settings {
		f(k, v)
	}
	m.mu.RUnlock()

	return nil
}

func (m *mockStorage) LoadResourceGroupSetting(name string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.settings[name]; ok {
		return v, nil
	}
	return "", errs.ErrResourceGroupNotExists.FastGenByArgs(name)
}

func (m *mockStorage) SaveResourceGroupSetting(name string, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.settings[name] = string(data)
	m.mu.Unlock()
	return nil
}

func (m *mockStorage) DeleteResourceGroupSetting(name string) error {
	m.mu.Lock()
	delete(m.settings, name)
	m.mu.Unlock()
	return nil
}

func (m *mockStorage) LoadResourceGroupStates(f func(k, v string)) error {
	m.mu.RLock()
	for k, v := range m.states {
		f(k, v)
	}
	m.mu.RUnlock()

	return nil
}

func (m *mockStorage) LoadResourceGroupState(name string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.states[name]; ok {
		return v, nil
	}
	return "", nil
}

func (m *mockStorage) SaveResourceGroupStates(name string, obj any) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.states[name] = string(data)
	m.mu.Unlock()
	return nil
}

func (m *mockStorage) DeleteResourceGroupStates(name string) error {
	m.mu.Lock()
	delete(m.states, name)
	m.mu.Unlock()
	return nil
}

func (m *mockStorage) SaveControllerConfig(config any) error {
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.config = string(data)
	m.mu.Unlock()
	return nil
}

func (m *mockStorage) LoadControllerConfig() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config, nil
}

// KeyspaceStorage stubs (not used in resource group tests but required by the interface).

func (*mockStorage) SaveKeyspaceMeta(kv.Txn, *keyspacepb.KeyspaceMeta) error { return nil }
func (*mockStorage) LoadKeyspaceMeta(kv.Txn, uint32) (*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}
func (*mockStorage) SaveKeyspaceID(kv.Txn, uint32, string) error { return nil }
func (*mockStorage) LoadKeyspaceID(kv.Txn, string) (bool, uint32, error) {
	return false, 0, nil
}
func (*mockStorage) LoadRangeKeyspace(kv.Txn, uint32, int) ([]*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}
func (*mockStorage) RunInTxn(_ context.Context, f func(txn kv.Txn) error) error {
	return f(nil)
}
func (*mockStorage) GetGlobalSafePointVersion(kv.Txn) (string, error) { return "", nil }
func (*mockStorage) SaveGlobalSafePointVersion(kv.Txn, string) error  { return nil }

func TestAsyncLoadingWith(t *testing.T) {
	// Create manager directly
	manager := &Manager{
		controllerConfig: &ControllerConfig{
			RequestUnit: RequestUnitConfig{
				ReadBaseCost:     0.25,
				ReadCostPerByte:  1.0,
				WriteBaseCost:    1.0,
				WriteCostPerByte: 1.0,
				CPUMsCost:        3.0,
			},
		},
		groups:           make(map[string]*ResourceGroup),
		loadingState:     LoadingStateNotStarted,
		syncLoadedGroups: make(map[string]bool),
		consumptionDispatcher: make(chan struct {
			resourceGroupName string
			*resource_manager.Consumption
			isBackground bool
			isTiFlash    bool
		}, defaultConsumptionChanSize),
		consumptionRecord: make(map[consumptionRecordKey]time.Time),
	}

	// Create mock storage
	mockStorage := newMockStorage()
	manager.storage = mockStorage

	// Add some test resource groups to mock storage
	testGroup1 := &resource_manager.ResourceGroup{
		Name:     "test-group-1",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 8,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   1000,
					BurstLimit: 10000,
				},
			},
		},
	}
	testGroup2 := &resource_manager.ResourceGroup{
		Name:     "test-group-2",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 8,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   1000,
					BurstLimit: 10000,
				},
			},
		},
	}
	testGroup3 := &resource_manager.ResourceGroup{
		Name:     "test-group-3",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 8,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   1000,
					BurstLimit: 10000,
				},
			},
		},
	}
	testGroup4 := &resource_manager.ResourceGroup{
		Name:     "test-group-4",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 8,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   1000,
					BurstLimit: 10000,
				},
			},
		},
	}

	mockStorage.SaveResourceGroupSetting("test-group-1", testGroup1)
	mockStorage.SaveResourceGroupSetting("test-group-2", testGroup2)
	mockStorage.SaveResourceGroupSetting("test-group-3", testGroup3)
	mockStorage.SaveResourceGroupSetting("test-group-4", testGroup4)

	// Start async loading manually (this will be blocked)
	go manager.asyncLoadResourceGroups(context.TODO())

	// At this point, async loading should be blocked on LoadResourceGroupSettings
	// Test operations during async loading

	// Test 1: GetResourceGroupList should return error during loading
	groups, err := manager.GetResourceGroupList(false)
	require.Error(t, err)
	assert.Equal(t, errs.ErrResourceGroupsLoading, err)
	assert.Nil(t, groups)

	// Test 2: GetResourceGroup should work (lazy loading)
	group1 := manager.GetResourceGroup("test-group-1", false)
	assert.NotNil(t, group1)
	assert.Equal(t, "test-group-1", group1.Name)

	// Test 3: GetMutableResourceGroup should work
	mutableGroup1 := manager.GetMutableResourceGroup("test-group-1")
	assert.NotNil(t, mutableGroup1)
	assert.Equal(t, "test-group-1", mutableGroup1.Name)

	// Test 4: Add a new resource group
	newGroup := &resource_manager.ResourceGroup{
		Name:     "new-group",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 8,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   1000,
					BurstLimit: 10000,
				},
			},
		},
	}
	err = manager.AddResourceGroup(newGroup)
	require.NoError(t, err)

	// Test 5: Modify existing resource group
	modifiedGroup := &resource_manager.ResourceGroup{
		Name:     "test-group-1",
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: 9, // Changed priority
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   1000,
					BurstLimit: 10000,
				},
			},
		},
	}
	err = manager.ModifyResourceGroup(modifiedGroup)
	require.NoError(t, err)

	// Test 6: Delete a resource group
	err = manager.DeleteResourceGroup("test-group-2")
	require.NoError(t, err)

	// Now unblock the async loading
	mockStorage.UnblockBeforeLoad()

	// Wait for the entire load operation to complete
	mockStorage.UnblockAfterLoad()

	// Wait for async loading to complete
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("async loading did not complete within timeout")
		case <-ticker.C:
			if manager.isResourceGroupLoadingComplete() {
				goto loadingComplete
			}
		}
	}

loadingComplete:
	// Test operations after async loading completes

	// Test 7: Delete a resource group after async loading
	err = manager.DeleteResourceGroup("test-group-3")
	require.NoError(t, err)

	// Test 8: Update a resource group after async loading
	testGroup4.Priority = 10
	err = manager.ModifyResourceGroup(testGroup4)
	require.NoError(t, err)

	// Test final state after async loading completes

	// Test 1: GetResourceGroupList should work now
	groups, err = manager.GetResourceGroupList(false)
	require.NoError(t, err)
	assert.NotNil(t, groups)

	// Should have: test-group-1 + new-group (test-group-2 was deleted, default group not loaded by async loading)
	groupNames := make(map[string]bool)
	for _, group := range groups {
		groupNames[group.Name] = true
	}

	assert.True(t, groupNames["test-group-1"], "test-group-1 should exist")
	assert.True(t, groupNames["new-group"], "new-group should exist")
	assert.False(t, groupNames["test-group-2"], "test-group-2 should not exist (was deleted)")

	// Test 2: Verify test-group-1 has the modified priority
	group1 = manager.GetResourceGroup("test-group-1", false)
	assert.NotNil(t, group1)
	assert.Equal(t, uint32(9), group1.Priority) // Should have the modified priority

	// Test 3: Verify new-group exists
	newGroupResult := manager.GetResourceGroup("new-group", false)
	assert.NotNil(t, newGroupResult)
	assert.Equal(t, "new-group", newGroupResult.Name)

	// Test 4: Verify test-group-2 was deleted
	deletedGroup := manager.GetResourceGroup("test-group-2", false)
	assert.Nil(t, deletedGroup)

	// Test 5: Verify test-group-3 was deleted
	deletedGroup = manager.GetResourceGroup("test-group-3", false)
	assert.Nil(t, deletedGroup)

	// Test 6: Verify test-group-4 has the modified priority
	group4 := manager.GetResourceGroup("test-group-4", false)
	assert.NotNil(t, group4)
	assert.Equal(t, uint32(10), group4.Priority)

	// Test 7: syncLoadedGroups should be nil after async loading completes
	assert.Nil(t, manager.syncLoadedGroups)
}
