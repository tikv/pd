// Copyright 2026 TiKV Project Authors.
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

package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core/storelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
)

func TestPersistConfigDefaultStoreLimit(t *testing.T) {
	re := require.New(t)
	oldAddPeer := sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer)
	oldRemovePeer := sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer)
	defer func() {
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, oldAddPeer)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, oldRemovePeer)
	}()
	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, 15)
	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, 15)

	cfg := NewConfig()
	re.NoError(cfg.adjust(nil))
	persistConfig := NewPersistConfig(cfg, nil)
	persistConfig.GetScheduleConfig().StoreLimit[1] = sc.StoreLimitConfig{AddPeer: 10, RemovePeer: 20}

	persistConfig.SetAllStoresLimit(storelimit.AddPeer, 60)
	re.Equal(sc.StoreLimitConfig{AddPeer: 60, RemovePeer: 15}, persistConfig.GetScheduleConfig().DefaultStoreLimit)
	re.Equal(sc.StoreLimitConfig{AddPeer: 60, RemovePeer: 20}, persistConfig.GetStoreLimit(1))

	data, err := json.Marshal(persistConfig.GetScheduleConfig())
	re.NoError(err)
	var reloadedScheduleConfig sc.ScheduleConfig
	re.NoError(json.Unmarshal(data, &reloadedScheduleConfig))
	restartedConfig := NewConfig()
	restartedConfig.Schedule = reloadedScheduleConfig
	restartedPersistConfig := NewPersistConfig(restartedConfig, nil)

	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, 15)
	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, 25)
	re.Equal(sc.StoreLimitConfig{AddPeer: 60, RemovePeer: 15}, restartedPersistConfig.GetStoreLimit(2))
}

func TestAdjustScheduleConfigDefaultStoreLimit(t *testing.T) {
	oldAddPeer := sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer)
	oldRemovePeer := sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer)
	defer func() {
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, oldAddPeer)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, oldRemovePeer)
	}()

	testCases := []struct {
		name     string
		config   string
		expected sc.StoreLimitConfig
	}{
		{
			name:     "legacy config without store limit default",
			config:   `{"store-limit":{}}`,
			expected: sc.StoreLimitConfig{AddPeer: 15, RemovePeer: 15},
		},
		{
			name:     "legacy store balance rate",
			config:   `{"store-balance-rate":60,"store-limit":{}}`,
			expected: sc.StoreLimitConfig{AddPeer: 60, RemovePeer: 60},
		},
		{
			name:     "explicit zero wins over legacy store balance rate",
			config:   `{"store-balance-rate":60,"default-store-limit":{"add-peer":0,"remove-peer":0},"store-limit":{}}`,
			expected: sc.StoreLimitConfig{AddPeer: 0, RemovePeer: 0},
		},
		{
			name:     "legacy store balance rate backfills an omitted field",
			config:   `{"store-balance-rate":60,"default-store-limit":{"add-peer":0},"store-limit":{}}`,
			expected: sc.StoreLimitConfig{AddPeer: 0, RemovePeer: 60},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, 15)
			sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, 15)
			watchedConfig := &persistedConfig{
				Schedule: sc.ScheduleConfig{DefaultStoreLimit: sc.DefaultStoreLimitConfig()},
			}
			re.NoError(json.Unmarshal([]byte(`{"schedule":`+testCase.config+`}`), watchedConfig))
			AdjustScheduleCfg(&watchedConfig.Schedule)
			re.Equal(testCase.expected, watchedConfig.Schedule.DefaultStoreLimit)
			re.Zero(watchedConfig.Schedule.StoreBalanceRate)

			cfg := NewConfig()
			cfg.Schedule = watchedConfig.Schedule
			persistConfig := NewPersistConfig(cfg, nil)
			re.Equal(testCase.expected, persistConfig.GetStoreLimit(100))
		})
	}
}
