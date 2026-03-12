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
)

func TestRuVersionPersistence(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	// Test saving and loading RU version.
	err := s.SaveRuVersion(1, 2)
	re.NoError(err)
	loaded, err := s.LoadRuVersion(1)
	re.NoError(err)
	re.Equal(int32(2), loaded)

	// Test loading non-existent RU version returns 0.
	loaded, err = s.LoadRuVersion(999)
	re.NoError(err)
	re.Equal(int32(0), loaded)

	// Test loading all RU versions.
	err = s.SaveRuVersion(2, 3)
	re.NoError(err)
	err = s.SaveRuVersion(3, 1)
	re.NoError(err)
	versions := make(map[uint32]int32)
	err = s.LoadRuVersions(func(keyspaceID uint32, ruVersion int32) {
		versions[keyspaceID] = ruVersion
	})
	re.NoError(err)
	re.Equal(int32(2), versions[1])
	re.Equal(int32(3), versions[2])
	re.Equal(int32(1), versions[3])
}

func TestServiceLimiterRuVersion(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	// Default RU version should be 0.
	limiter := newServiceLimiter(1, 0.0, s)
	re.Equal(int32(0), limiter.getRuVersion())

	// Set RU version and verify it's persisted.
	limiter.setRuVersion(2)
	re.Equal(int32(2), limiter.getRuVersion())

	// Verify persistence.
	loaded, err := s.LoadRuVersion(1)
	re.NoError(err)
	re.Equal(int32(2), loaded)

	// Test setRuVersionNoPersist.
	limiter2 := newServiceLimiter(1, 0.0, s)
	limiter2.setRuVersionNoPersist(3)
	re.Equal(int32(3), limiter2.getRuVersion())

	// Test Clone includes RU version.
	limiter.setRuVersion(5)
	cloned := limiter.Clone()
	re.Equal(int32(5), cloned.RuVersion)

	// Test nil limiter returns 0.
	var nilLimiter *serviceLimiter
	re.Equal(int32(0), nilLimiter.getRuVersion())
}

func TestKeyspaceRuVersion(t *testing.T) {
	re := require.New(t)
	s := storage.NewStorageWithMemoryBackend()

	krgm := newKeyspaceResourceGroupManager(1, s, ResourceGroupWriteRoleLegacyAll)

	// Default RU version should be 0.
	re.Equal(int32(0), krgm.getServiceLimiter().getRuVersion())

	// Set RU version via keyspace manager.
	krgm.setRuVersion(2)
	re.Equal(int32(2), krgm.getServiceLimiter().getRuVersion())

	// Verify persistence.
	loaded, err := s.LoadRuVersion(1)
	re.NoError(err)
	re.Equal(int32(2), loaded)

	// Test setRuVersionFromStorage (no persist).
	krgm2 := newKeyspaceResourceGroupManager(1, s, ResourceGroupWriteRoleLegacyAll)
	krgm2.setRuVersionFromStorage(3)
	re.Equal(int32(3), krgm2.getServiceLimiter().getRuVersion())
}

func TestManagerRuVersion(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	keyspaceID := uint32(1)

	// Get RU version for non-existent keyspace via service limiter clone.
	limiter := m.GetKeyspaceServiceLimiter(keyspaceID)
	re.Nil(limiter)

	// Set RU version.
	err = m.SetKeyspaceRuVersion(keyspaceID, 2)
	re.NoError(err)
	limiter = m.GetKeyspaceServiceLimiter(keyspaceID)
	re.NotNil(limiter)
	re.Equal(int32(2), limiter.RuVersion)

	// Update RU version.
	err = m.SetKeyspaceRuVersion(keyspaceID, 3)
	re.NoError(err)
	limiter = m.GetKeyspaceServiceLimiter(keyspaceID)
	re.Equal(int32(3), limiter.RuVersion)

	// Rebuild the manager and verify RU versions are loaded from storage.
	s := m.storage
	m2 := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m2.storage = s
	err = m2.Init(ctx)
	re.NoError(err)
	limiter = m2.GetKeyspaceServiceLimiter(keyspaceID)
	re.NotNil(limiter)
	re.Equal(int32(3), limiter.RuVersion)
}

func TestManagerRuVersionWriteDisabled(t *testing.T) {
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

	err = m.SetKeyspaceRuVersion(1, 2)
	re.Error(err)
	re.True(IsMetadataWriteDisabledError(err))
}
