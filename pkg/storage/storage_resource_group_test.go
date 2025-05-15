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

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
)

func TestResourceGroupStorage(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()

	keyspaceGroups := map[uint32][]string{
		constant.DefaultKeyspaceID: {"rg1", "rg2", "rg/3", "4/rg/5"},
		1:                          {"rg1", "rg2", "rg/3", "4/rg/5"},
		2:                          {"rg1", "rg2", "rg/3", "4/rg/5"},
	}
	// Test legacy and keyspace resource group settings.
	for keyspaceID, names := range keyspaceGroups {
		for _, name := range names {
			err := storage.SaveResourceGroupSetting(keyspaceID, name, &rmpb.ResourceGroup{Name: name})
			re.NoError(err)
		}
	}
	err := storage.LoadResourceGroupSettings(func(keyspaceID uint32, name, _ string) {
		re.Contains(keyspaceGroups[keyspaceID], name)
	})
	re.NoError(err)
	for keyspaceID, names := range keyspaceGroups {
		for _, name := range names {
			err := storage.DeleteResourceGroupSetting(keyspaceID, name)
			re.NoError(err)
		}
	}
	err = storage.LoadResourceGroupSettings(func(_ uint32, _, _ string) {
		re.Fail("should not load any resource group setting")
	})
	re.NoError(err)

	// Test legacy and keyspace resource group states.
	for keyspaceID, names := range keyspaceGroups {
		for _, name := range names {
			err := storage.SaveResourceGroupStates(keyspaceID, name, nil)
			re.NoError(err)
		}
	}
	err = storage.LoadResourceGroupStates(func(keyspaceID uint32, name, _ string) {
		re.Contains(keyspaceGroups[keyspaceID], name)
	})
	re.NoError(err)
	for keyspaceID, names := range keyspaceGroups {
		for _, name := range names {
			err := storage.DeleteResourceGroupStates(keyspaceID, name)
			re.NoError(err)
		}
	}
	err = storage.LoadResourceGroupStates(func(_ uint32, _, _ string) {
		re.Fail("should not load any resource group state")
	})
	re.NoError(err)
}
