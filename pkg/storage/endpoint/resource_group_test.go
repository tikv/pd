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

package endpoint

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/storage/kv"
)

func TestRuConfig(t *testing.T) {
	re := require.New(t)
	storage := NewStorageEndpoint(kv.NewMemoryKV(), nil)

	// Case 1: Load non-existent RU config
	config, err := storage.LoadRuConfig(1)
	re.NoError(err)
	re.Nil(config)

	// Case 2: Save and Load RU config
	v3 := int32(3)
	err = storage.SaveRuConfig(1, &KeyspaceRuConfig{RuVersion: &v3})
	re.NoError(err)
	config, err = storage.LoadRuConfig(1)
	re.NoError(err)
	re.NotNil(config)
	re.Equal(int32(3), *config.RuVersion)

	// Case 3: Save and Load multiple RU configs
	v5 := int32(5)
	err = storage.SaveRuConfig(2, &KeyspaceRuConfig{RuVersion: &v5})
	re.NoError(err)

	configs := make(map[uint32]int32)
	err = storage.LoadRuConfigs(func(keyspaceID uint32, config *KeyspaceRuConfig) {
		configs[keyspaceID] = *config.RuVersion
	})
	re.NoError(err)
	re.Len(configs, 2)
	re.Equal(int32(3), configs[1])
	re.Equal(int32(5), configs[2])
}
