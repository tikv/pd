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
	re.NotNil(configs[1].RuVersion)
	re.Equal(int32(2), *configs[1].RuVersion)
	re.NotNil(configs[2].RuVersion)
	re.Equal(int32(3), *configs[2].RuVersion)
	re.NotNil(configs[3].RuVersion)
	re.Equal(int32(1), *configs[3].RuVersion)
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
