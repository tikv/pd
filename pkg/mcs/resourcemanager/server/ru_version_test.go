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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

func TestRuConfigPersistence(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	// Test saving and loading RU config.
	ruVersion := int32(2)
	err := s.SaveRuConfig(1, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion})
	re.NoError(err)
	loaded, err := s.LoadRuConfig(1)
	re.NoError(err)
	re.NotNil(loaded)
	re.NotNil(loaded.RuVersion)
	re.Equal(int32(2), *loaded.RuVersion)

	// Test loading non-existent RU config returns nil.
	loaded, err = s.LoadRuConfig(999)
	re.NoError(err)
	re.Nil(loaded)

	// Test loading all RU configs.
	ruVersion3 := int32(3)
	ruVersion1 := int32(1)
	err = s.SaveRuConfig(2, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion3})
	re.NoError(err)
	err = s.SaveRuConfig(3, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion1})
	re.NoError(err)
	configs := make(map[uint32]*endpoint.KeyspaceRuConfig)
	err = s.LoadRuConfigs(func(keyspaceID uint32, config *endpoint.KeyspaceRuConfig) {
		configs[keyspaceID] = config
	})
	re.NoError(err)
	re.Len(configs, 3)
	re.NotNil(configs[1].RuVersion)
	re.Equal(int32(2), *configs[1].RuVersion)
	re.NotNil(configs[2].RuVersion)
	re.Equal(int32(3), *configs[2].RuVersion)
	re.NotNil(configs[3].RuVersion)
	re.Equal(int32(1), *configs[3].RuVersion)
}

func TestRuConfigPersistenceNilRuVersion(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	// Save config with nil RuVersion (omitted in JSON).
	err := s.SaveRuConfig(1, &endpoint.KeyspaceRuConfig{RuVersion: nil})
	re.NoError(err)
	loaded, err := s.LoadRuConfig(1)
	re.NoError(err)
	re.NotNil(loaded)
	re.Nil(loaded.RuVersion)
}

func TestRuConfigPersistenceZeroRuVersion(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	// Save config with zero RuVersion (should be explicitly set via pointer).
	ruVersion0 := int32(0)
	err := s.SaveRuConfig(1, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion0})
	re.NoError(err)
	loaded, err := s.LoadRuConfig(1)
	re.NoError(err)
	re.NotNil(loaded)
	re.NotNil(loaded.RuVersion)
	re.Equal(int32(0), *loaded.RuVersion)
}

func TestRuConfigPersistenceOverwrite(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	// Save initial config.
	ruVersion := int32(1)
	err := s.SaveRuConfig(1, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion})
	re.NoError(err)

	// Overwrite with new value.
	ruVersion2 := int32(5)
	err = s.SaveRuConfig(1, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion2})
	re.NoError(err)

	// Verify the latest value.
	loaded, err := s.LoadRuConfig(1)
	re.NoError(err)
	re.NotNil(loaded)
	re.NotNil(loaded.RuVersion)
	re.Equal(int32(5), *loaded.RuVersion)
}

func TestRuConfigJSONSerialization(t *testing.T) {
	re := require.New(t)

	// Test with RuVersion set.
	ruVersion := int32(3)
	config := &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion}
	data, err := json.Marshal(config)
	re.NoError(err)
	re.Contains(string(data), `"ru_version":3`)

	// Test with RuVersion nil (omitempty).
	config2 := &endpoint.KeyspaceRuConfig{RuVersion: nil}
	data, err = json.Marshal(config2)
	re.NoError(err)
	re.Equal(`{}`, string(data))

	// Test unmarshal with ru_version.
	var loaded endpoint.KeyspaceRuConfig
	err = json.Unmarshal([]byte(`{"ru_version":7}`), &loaded)
	re.NoError(err)
	re.NotNil(loaded.RuVersion)
	re.Equal(int32(7), *loaded.RuVersion)

	// Test unmarshal with empty JSON.
	var loaded2 endpoint.KeyspaceRuConfig
	err = json.Unmarshal([]byte(`{}`), &loaded2)
	re.NoError(err)
	re.Nil(loaded2.RuVersion)

	// Test unmarshal with ru_version = 0.
	var loaded3 endpoint.KeyspaceRuConfig
	err = json.Unmarshal([]byte(`{"ru_version":0}`), &loaded3)
	re.NoError(err)
	re.NotNil(loaded3.RuVersion)
	re.Equal(int32(0), *loaded3.RuVersion)
}

func TestKeyspaceRuConfig(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	krgm := newKeyspaceResourceGroupManager(1, s, ResourceGroupWriteRoleLegacyAll)

	// Default RU config should be nil.
	re.Nil(krgm.getRuConfig())

	// Set RU config via keyspace manager.
	ruVersion := int32(2)
	krgm.setRuConfig(&endpoint.KeyspaceRuConfig{RuVersion: &ruVersion})
	config := krgm.getRuConfig()
	re.NotNil(config)
	re.NotNil(config.RuVersion)
	re.Equal(int32(2), *config.RuVersion)

	// Verify persistence.
	loaded, err := s.LoadRuConfig(1)
	re.NoError(err)
	re.NotNil(loaded)
	re.NotNil(loaded.RuVersion)
	re.Equal(int32(2), *loaded.RuVersion)

	// Test setRuConfigFromStorage (no persist).
	krgm2 := newKeyspaceResourceGroupManager(1, s, ResourceGroupWriteRoleLegacyAll)
	ruVersion3 := int32(3)
	krgm2.setRuConfigFromStorage(&endpoint.KeyspaceRuConfig{RuVersion: &ruVersion3})
	config = krgm2.getRuConfig()
	re.NotNil(config)
	re.NotNil(config.RuVersion)
	re.Equal(int32(3), *config.RuVersion)
}

func TestKeyspaceRuConfigReturnsCopy(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	krgm := newKeyspaceResourceGroupManager(1, s, ResourceGroupWriteRoleLegacyAll)

	// Set config.
	ruVersion := int32(2)
	krgm.setRuConfig(&endpoint.KeyspaceRuConfig{RuVersion: &ruVersion})

	// Get config and modify the copy.
	config := krgm.getRuConfig()
	re.NotNil(config)
	modified := int32(999)
	config.RuVersion = &modified

	// Original should not be affected.
	original := krgm.getRuConfig()
	re.NotNil(original)
	re.NotNil(original.RuVersion)
	re.Equal(int32(2), *original.RuVersion)
}

func TestKeyspaceRuConfigNilRuVersion(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	krgm := newKeyspaceResourceGroupManager(1, s, ResourceGroupWriteRoleLegacyAll)

	// Set config with nil RuVersion.
	krgm.setRuConfig(&endpoint.KeyspaceRuConfig{RuVersion: nil})
	config := krgm.getRuConfig()
	re.NotNil(config)
	re.Nil(config.RuVersion)
}

func TestKeyspaceRuConfigWriteRoleDisabled(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	// Create with write role that disallows metadata write.
	krgm := newKeyspaceResourceGroupManager(1, s, ResourceGroupWriteRoleRMTokenOnly)

	// Set config — should update in memory but not persist.
	ruVersion := int32(5)
	krgm.setRuConfig(&endpoint.KeyspaceRuConfig{RuVersion: &ruVersion})

	// In-memory value should be set.
	config := krgm.getRuConfig()
	re.NotNil(config)
	re.NotNil(config.RuVersion)
	re.Equal(int32(5), *config.RuVersion)

	// Storage should not have been written (write role disallows metadata write).
	loaded, err := s.LoadRuConfig(1)
	re.NoError(err)
	re.Nil(loaded)
}

func TestKeyspaceRuConfigNilStorage(t *testing.T) {
	re := require.New(t)

	// Create with nil storage.
	krgm := &keyspaceResourceGroupManager{
		groups:     make(map[string]*ResourceGroup),
		keyspaceID: 1,
		storage:    nil,
		writeRole:  ResourceGroupWriteRoleLegacyAll,
	}

	// Set config — should not panic even with nil storage.
	ruVersion := int32(5)
	krgm.setRuConfig(&endpoint.KeyspaceRuConfig{RuVersion: &ruVersion})

	config := krgm.getRuConfig()
	re.NotNil(config)
	re.NotNil(config.RuVersion)
	re.Equal(int32(5), *config.RuVersion)
}

func TestManagerRuConfig(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	keyspaceID := uint32(1)

	// Get RU config for non-existent keyspace.
	config := m.GetKeyspaceRuConfig(keyspaceID)
	re.Nil(config)

	// Set RU config.
	ruVersion := int32(2)
	err = m.SetKeyspaceRuConfig(keyspaceID, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion})
	re.NoError(err)
	config = m.GetKeyspaceRuConfig(keyspaceID)
	re.NotNil(config)
	re.NotNil(config.RuVersion)
	re.Equal(int32(2), *config.RuVersion)

	// Update RU config.
	ruVersion3 := int32(3)
	err = m.SetKeyspaceRuConfig(keyspaceID, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion3})
	re.NoError(err)
	config = m.GetKeyspaceRuConfig(keyspaceID)
	re.NotNil(config)
	re.NotNil(config.RuVersion)
	re.Equal(int32(3), *config.RuVersion)

	// Rebuild the manager and verify RU configs are loaded from storage.
	s := m.storage
	m2 := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m2.storage = s
	err = m2.Init(ctx)
	re.NoError(err)
	config = m2.GetKeyspaceRuConfig(keyspaceID)
	re.NotNil(config)
	re.NotNil(config.RuVersion)
	re.Equal(int32(3), *config.RuVersion)
}

func TestManagerRuConfigMultipleKeyspaces(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	// Set RU config for multiple keyspaces.
	ruVersion1 := int32(1)
	ruVersion2 := int32(2)
	ruVersion3 := int32(3)
	err = m.SetKeyspaceRuConfig(1, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion1})
	re.NoError(err)
	err = m.SetKeyspaceRuConfig(2, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion2})
	re.NoError(err)
	err = m.SetKeyspaceRuConfig(3, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion3})
	re.NoError(err)

	// Verify each keyspace has independent config.
	config := m.GetKeyspaceRuConfig(1)
	re.NotNil(config)
	re.Equal(int32(1), *config.RuVersion)
	config = m.GetKeyspaceRuConfig(2)
	re.NotNil(config)
	re.Equal(int32(2), *config.RuVersion)
	config = m.GetKeyspaceRuConfig(3)
	re.NotNil(config)
	re.Equal(int32(3), *config.RuVersion)

	// Reload from storage and verify.
	s := m.storage
	m2 := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m2.storage = s
	err = m2.Init(ctx)
	re.NoError(err)
	config = m2.GetKeyspaceRuConfig(1)
	re.NotNil(config)
	re.Equal(int32(1), *config.RuVersion)
	config = m2.GetKeyspaceRuConfig(2)
	re.NotNil(config)
	re.Equal(int32(2), *config.RuVersion)
	config = m2.GetKeyspaceRuConfig(3)
	re.NotNil(config)
	re.Equal(int32(3), *config.RuVersion)
}

func TestManagerRuConfigWithNilRuVersion(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	// Set RU config with nil RuVersion.
	err = m.SetKeyspaceRuConfig(1, &endpoint.KeyspaceRuConfig{RuVersion: nil})
	re.NoError(err)
	config := m.GetKeyspaceRuConfig(1)
	re.NotNil(config)
	re.Nil(config.RuVersion)
}

func TestManagerRuConfigIndependentFromServiceLimit(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	keyspaceID := uint32(1)

	// Set service limit without RU config.
	err = m.SetKeyspaceServiceLimit(keyspaceID, 100.0)
	re.NoError(err)

	// RU config should still be nil (decoupled).
	config := m.GetKeyspaceRuConfig(keyspaceID)
	re.Nil(config)

	// Set RU config independently.
	ruVersion := int32(2)
	err = m.SetKeyspaceRuConfig(keyspaceID, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion})
	re.NoError(err)

	// Service limit should not be affected.
	limiter := m.GetKeyspaceServiceLimiter(keyspaceID)
	re.NotNil(limiter)
	re.Equal(100.0, limiter.ServiceLimit)

	// RU config should be set.
	config = m.GetKeyspaceRuConfig(keyspaceID)
	re.NotNil(config)
	re.NotNil(config.RuVersion)
	re.Equal(int32(2), *config.RuVersion)
}

func TestManagerRuConfigWriteDisabled(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()
	m := NewManager[*mockRoleConfigProvider](&mockRoleConfigProvider{
		role: ResourceGroupWriteRoleRMTokenOnly,
	})
	m.storage = s

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	ruVersion := int32(2)
	err = m.SetKeyspaceRuConfig(1, &endpoint.KeyspaceRuConfig{RuVersion: &ruVersion})
	re.Error(err)
	re.True(IsMetadataWriteDisabledError(err))
}

func TestManagerGetRuConfigForNonExistentKeyspace(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	// Non-existent keyspace should return nil.
	config := m.GetKeyspaceRuConfig(12345)
	re.Nil(config)
}

func TestLoadRuConfigsEmpty(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	// Loading from empty storage should not fail.
	count := 0
	err := s.LoadRuConfigs(func(_ uint32, _ *endpoint.KeyspaceRuConfig) {
		count++
	})
	re.NoError(err)
	re.Equal(0, count)
}
