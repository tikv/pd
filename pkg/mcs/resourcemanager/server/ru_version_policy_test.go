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

	keyspacepb "github.com/pingcap/kvproto/pkg/keyspacepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/storage/kv"
)

// mockKeyspaceStorage implements endpoint.KeyspaceStorage for unit tests.
type mockKeyspaceStorage struct {
	mu         sync.RWMutex
	keyspaceID map[string]uint32 // name → ID
}

func newMockKeyspaceStorage() *mockKeyspaceStorage {
	return &mockKeyspaceStorage{
		keyspaceID: make(map[string]uint32),
	}
}

func (*mockKeyspaceStorage) SaveKeyspaceMeta(kv.Txn, *keyspacepb.KeyspaceMeta) error { return nil }
func (*mockKeyspaceStorage) LoadKeyspaceMeta(kv.Txn, uint32) (*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}
func (s *mockKeyspaceStorage) SaveKeyspaceID(_ kv.Txn, id uint32, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keyspaceID[name] = id
	return nil
}
func (s *mockKeyspaceStorage) LoadKeyspaceID(_ kv.Txn, name string) (bool, uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.keyspaceID[name]
	return ok, id, nil
}
func (*mockKeyspaceStorage) LoadRangeKeyspace(kv.Txn, uint32, int) ([]*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}
func (*mockKeyspaceStorage) RunInTxn(_ context.Context, f func(txn kv.Txn) error) error {
	return f(nil)
}
func (*mockKeyspaceStorage) GetGlobalSafePointVersion(kv.Txn) (string, error) { return "", nil }
func (*mockKeyspaceStorage) SaveGlobalSafePointVersion(kv.Txn, string) error  { return nil }

// prepareTestManager creates a Manager suitable for unit tests without requiring a real server.
func prepareTestManager() *Manager {
	storage := newMockStorage()
	// Unblock the mock storage so loads don't hang.
	storage.UnblockBeforeLoad()
	m := &Manager{
		controllerConfig: &ControllerConfig{},
		groups:           make(map[string]*ResourceGroup),
		loadingState:     LoadingStateNotStarted,
		syncLoadedGroups: make(map[string]bool),
		consumptionDispatcher: make(chan struct {
			resourceGroupName string
			*rmpb.Consumption
			isBackground bool
			isTiFlash    bool
		}, defaultConsumptionChanSize),
		consumptionRecord: make(map[consumptionRecordKey]time.Time),
	}
	m.storage = storage
	m.keyspaceStorage = newMockKeyspaceStorage()
	return m
}

func TestRUVersionPolicyJSONSerialization(t *testing.T) {
	re := require.New(t)

	// Test with overrides.
	policy := &RUVersionPolicy{
		Default:   1,
		Overrides: map[uint32]int32{42: 2, 100: 3},
	}
	data, err := json.Marshal(policy)
	re.NoError(err)
	re.Contains(string(data), `"default":1`)
	re.Contains(string(data), `"overrides"`)

	var loaded RUVersionPolicy
	err = json.Unmarshal(data, &loaded)
	re.NoError(err)
	re.Equal(int32(1), loaded.Default)
	re.Equal(int32(2), loaded.Overrides[42])
	re.Equal(int32(3), loaded.Overrides[100])

	// Test without overrides (omitempty).
	policy2 := &RUVersionPolicy{Default: 1}
	data, err = json.Marshal(policy2)
	re.NoError(err)
	re.NotContains(string(data), `"overrides"`)

	// Test nil policy.
	var nilPolicy *RUVersionPolicy
	data, err = json.Marshal(nilPolicy)
	re.NoError(err)
	re.Equal("null", string(data))
}

func TestRUVersionPolicyInControllerConfig(t *testing.T) {
	re := require.New(t)

	// Test ControllerConfig with embedded policy serializes correctly.
	config := &ControllerConfig{
		RUVersionPolicy: &RUVersionPolicy{
			Default:   1,
			Overrides: map[uint32]int32{42: 2},
		},
	}
	data, err := json.Marshal(config)
	re.NoError(err)
	re.Contains(string(data), `"ru-version-policy"`)

	var loaded ControllerConfig
	err = json.Unmarshal(data, &loaded)
	re.NoError(err)
	re.NotNil(loaded.RUVersionPolicy)
	re.Equal(int32(1), loaded.RUVersionPolicy.Default)
	re.Equal(int32(2), loaded.RUVersionPolicy.Overrides[42])

	// Test ControllerConfig without policy (nil).
	config2 := &ControllerConfig{}
	data, err = json.Marshal(config2)
	re.NoError(err)
	re.NotContains(string(data), `"ru-version-policy"`)

	var loaded2 ControllerConfig
	err = json.Unmarshal(data, &loaded2)
	re.NoError(err)
	re.Nil(loaded2.RUVersionPolicy)
}

func TestManagerSetKeyspaceRUVersion(t *testing.T) {
	re := require.New(t)
	m := prepareTestManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	// Initially no policy.
	re.Nil(m.GetRUVersionPolicy())

	// Set RU version for keyspace 1.
	err = m.SetKeyspaceRUVersion(1, 2)
	re.NoError(err)

	policy := m.GetRUVersionPolicy()
	re.NotNil(policy)
	re.Equal(int32(1), policy.Default)
	re.Equal(int32(2), policy.Overrides[1])

	// Verify persistence by reloading.
	s := m.storage
	m2 := prepareTestManager()
	m2.storage = s
	err = m2.Init(ctx)
	re.NoError(err)
	policy = m2.GetRUVersionPolicy()
	re.NotNil(policy)
	re.Equal(int32(2), policy.Overrides[1])
}

func TestManagerSetKeyspaceRUVersionMultipleKeyspaces(t *testing.T) {
	re := require.New(t)
	m := prepareTestManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	// Set different versions for different keyspaces.
	err = m.SetKeyspaceRUVersion(1, 2)
	re.NoError(err)
	err = m.SetKeyspaceRUVersion(2, 3)
	re.NoError(err)
	err = m.SetKeyspaceRUVersion(3, 4)
	re.NoError(err)

	policy := m.GetRUVersionPolicy()
	re.NotNil(policy)
	re.Equal(int32(2), policy.Overrides[1])
	re.Equal(int32(3), policy.Overrides[2])
	re.Equal(int32(4), policy.Overrides[3])
}

func TestUpdateControllerConfigRUVersionPolicy(t *testing.T) {
	re := require.New(t)
	m := prepareTestManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	// Valid policy: update via generic config endpoint should succeed.
	err = m.UpdateControllerConfigItem("ru-version-policy", map[string]any{
		"default":   float64(1),
		"overrides": map[string]any{"42": float64(2)},
	})
	re.NoError(err)
	policy := m.GetRUVersionPolicy()
	re.NotNil(policy)
	re.Equal(int32(1), policy.Default)
	re.Equal(int32(2), policy.Overrides[42])

	// Invalid: negative default should be rejected.
	err = m.UpdateControllerConfigItem("ru-version-policy", map[string]any{
		"default": float64(-1),
	})
	re.Error(err)
	re.Contains(err.Error(), "default must be positive")

	// Invalid: negative override should be rejected.
	err = m.UpdateControllerConfigItem("ru-version-policy", map[string]any{
		"default":   float64(1),
		"overrides": map[string]any{"10": float64(-5)},
	})
	re.Error(err)
	re.Contains(err.Error(), "must be positive")

	// Invalid: zero default should be rejected.
	err = m.UpdateControllerConfigItem("ru-version-policy", map[string]any{
		"default": float64(0),
	})
	re.Error(err)
	re.Contains(err.Error(), "default must be positive")

	// After rejections, original valid policy should remain unchanged.
	policy = m.GetRUVersionPolicy()
	re.NotNil(policy)
	re.Equal(int32(1), policy.Default)
	re.Equal(int32(2), policy.Overrides[42])
}

func TestUpdateControllerConfigNilRUVersionPolicy(t *testing.T) {
	re := require.New(t)
	m := prepareTestManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	// Initially RUVersionPolicy is nil.
	re.Nil(m.GetRUVersionPolicy())

	// Updating a non-RUVersionPolicy field should succeed when policy is nil.
	err = m.UpdateControllerConfigItem("enable-controller-trace-log", "true")
	re.NoError(err)
	// Policy should still be nil after updating an unrelated field.
	re.Nil(m.GetRUVersionPolicy())
}

func TestUpdateControllerConfigValidationViaDefaultPath(t *testing.T) {
	re := require.New(t)
	m := prepareTestManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	// First set a valid policy via the dedicated API.
	err = m.SetKeyspaceRUVersion(1, 2)
	re.NoError(err)
	policy := m.GetRUVersionPolicy()
	re.NotNil(policy)
	re.Equal(int32(2), policy.Overrides[1])

	// Updating an unrelated config item should not break the existing valid policy.
	err = m.UpdateControllerConfigItem("enable-controller-trace-log", "true")
	re.NoError(err)
	policy = m.GetRUVersionPolicy()
	re.NotNil(policy)
	re.Equal(int32(2), policy.Overrides[1])
}

func TestManagerSetKeyspaceRUVersionResetToDefault(t *testing.T) {
	re := require.New(t)
	m := prepareTestManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	// Set and then unset.
	err = m.SetKeyspaceRUVersion(1, 2)
	re.NoError(err)
	policy := m.GetRUVersionPolicy()
	re.Equal(int32(2), policy.Overrides[1])

	// Setting ruVersion=1 removes the override.
	err = m.SetKeyspaceRUVersion(1, 1)
	re.NoError(err)
	policy = m.GetRUVersionPolicy()
	re.NotNil(policy)
	_, exists := policy.Overrides[1]
	re.False(exists)
}

func TestGetKeyspaceIDByName(t *testing.T) {
	re := require.New(t)
	m := prepareTestManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	ks := m.keyspaceStorage.(*mockKeyspaceStorage)

	// Keyspace not found.
	_, err = m.GetKeyspaceIDByName(ctx, "nonexistent")
	re.Error(err)
	re.Contains(err.Error(), "not found")

	// Add a keyspace and look it up.
	ks.mu.Lock()
	ks.keyspaceID["my_keyspace"] = 42
	ks.mu.Unlock()

	id, err := m.GetKeyspaceIDByName(ctx, "my_keyspace")
	re.NoError(err)
	re.Equal(uint32(42), id)
}
