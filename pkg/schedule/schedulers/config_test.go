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

package schedulers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/storage"
)

func TestSchedulerConfig(t *testing.T) {
	s := storage.NewStorageWithMemoryBackend()

	type testConfig struct {
		schedulerConfig
		Value string `json:"value"`
	}

	cfg := &testConfig{
		schedulerConfig: &baseSchedulerConfig{},
	}
	cfg.init("test", s, cfg)

	cfg.Value = "test"
	require.NoError(t, cfg.save())
	newTc := &testConfig{}
	require.NoError(t, cfg.load(newTc))
	require.Equal(t, cfg.Value, newTc.Value)

	// config with another name cannot loaded the previous config
	cfg2 := &testConfig{
		schedulerConfig: &baseSchedulerConfig{},
	}
	cfg2.init("test2", s, cfg2)
	// report error because the config is empty and cannot be decoded
	require.Error(t, cfg2.load(newTc))
}

func TestDefaultSchedulerConfig(t *testing.T) {
	s := storage.NewStorageWithMemoryBackend()

	type testConfig struct {
		balanceLeaderSchedulerConfig
		Value string `json:"value"`
	}

	cfg := &testConfig{
		balanceLeaderSchedulerConfig: balanceLeaderSchedulerConfig{
			baseDefaultSchedulerConfig: newBaseDefaultSchedulerConfig(),
		},
		Value: "test",
	}
	cfg.init("test", s, cfg)
	require.False(t, cfg.isDisable())
	require.NoError(t, cfg.setDisable(true))
	require.True(t, cfg.isDisable())

	cfg2 := &testConfig{
		balanceLeaderSchedulerConfig: balanceLeaderSchedulerConfig{
			baseDefaultSchedulerConfig: newBaseDefaultSchedulerConfig(),
		},
	}
	cfg2.init("test", s, cfg2)
	require.True(t, cfg2.isDisable())
	require.Equal(t, "", cfg2.Value)

	cfg3 := &testConfig{}
	require.NoError(t, cfg2.load(cfg3))
	require.Equal(t, cfg.Value, cfg3.Value)
	require.True(t, cfg3.Disabled)
}
