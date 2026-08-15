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

package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core/storelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
)

func TestUnmarshalRemoteConfigMigratesDefaultStoreLimit(t *testing.T) {
	oldAddPeer := sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer)
	oldRemovePeer := sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer)
	t.Cleanup(func() {
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, oldAddPeer)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, oldRemovePeer)
	})

	testCases := []struct {
		name     string
		data     string
		expected sc.StoreLimitConfig
	}{
		{
			name:     "missing default uses process default",
			data:     `{"schedule":{}}`,
			expected: sc.StoreLimitConfig{AddPeer: 15, RemovePeer: 15},
		},
		{
			name:     "legacy rate backfills missing default",
			data:     `{"schedule":{"store-balance-rate":60}}`,
			expected: sc.StoreLimitConfig{AddPeer: 60, RemovePeer: 60},
		},
		{
			name:     "explicit zero remains unlimited",
			data:     `{"schedule":{"store-balance-rate":60,"default-store-limit":{"add-peer":0,"remove-peer":0}}}`,
			expected: sc.StoreLimitConfig{AddPeer: 0, RemovePeer: 0},
		},
		{
			name:     "compatibility entry survives old primary rewrite",
			data:     `{"schedule":{"store-limit":{"0":{"add-peer":70,"remove-peer":0}}}}`,
			expected: sc.StoreLimitConfig{AddPeer: 70, RemovePeer: 0},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, 15)
			sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, 15)

			cfg, err := unmarshalRemoteConfig([]byte(testCase.data))
			require.NoError(t, err)
			require.Equal(t, testCase.expected, cfg.Schedule.DefaultStoreLimit)
			require.Equal(t, testCase.expected,
				cfg.Schedule.StoreLimit[sc.DefaultStoreLimitCompatStoreID])
			require.Zero(t, cfg.Schedule.StoreBalanceRate)
			_, exposed := cfg.Schedule.CloneWithoutDefaultStoreLimitCompat().StoreLimit[sc.DefaultStoreLimitCompatStoreID]
			require.False(t, exposed)

			data, err := json.Marshal(cfg)
			require.NoError(t, err)
			var roundTrip struct {
				Schedule sc.ScheduleConfig `json:"schedule"`
			}
			require.NoError(t, json.Unmarshal(data, &roundTrip))
			require.Equal(t, testCase.expected, roundTrip.Schedule.DefaultStoreLimit)
		})
	}
}
