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
	"math"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/utils/configutil"
)

func TestTransferLeaderInLimitDefaults(t *testing.T) {
	previous := DefaultStoreLimitConfig()
	t.Cleanup(func() {
		DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, previous.AddPeer)
		DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, previous.RemovePeer)
		DefaultStoreLimit.SetDefaultStoreLimit(storelimit.TransferLeaderIn, previous.TransferLeaderIn)
	})
	DefaultStoreLimit.SetDefaultStoreLimit(storelimit.TransferLeaderIn, storelimit.Unlimited)
	data := []byte(`{"store-limit":{"1":{"add-peer":10,"remove-peer":20},"2":{"transfer-leader-in":30},"3":{"transfer-leader-in":0}}}`)
	var cfg ScheduleConfig
	require.NoError(t, json.Unmarshal(data, &cfg))
	require.NoError(t, cfg.MigrateDeprecatedFlagsFromJSON(data))
	require.Equal(t, storelimit.Unlimited, cfg.DefaultStoreLimit.TransferLeaderIn)
	require.Equal(t, StoreLimitConfig{AddPeer: 10, RemovePeer: 20, TransferLeaderIn: storelimit.Unlimited}, cfg.StoreLimit[1])
	require.Equal(t, float64(30), cfg.StoreLimit[2].TransferLeaderIn)
	require.Zero(t, cfg.StoreLimit[3].TransferLeaderIn)

	for _, tc := range []struct {
		config   string
		expected float64
	}{
		{"", storelimit.Unlimited},
		{"[default-store-limit]\ntransfer-leader-in = 30", 30},
		{"[default-store-limit]\ntransfer-leader-in = 0", 0},
	} {
		var cfg ScheduleConfig
		meta, err := toml.Decode(tc.config, &cfg)
		require.NoError(t, err)
		require.NoError(t, cfg.Adjust(configutil.NewConfigMetadata(&meta), false))
		require.Equal(t, tc.expected, cfg.DefaultStoreLimit.TransferLeaderIn)
	}
}

func TestStoreLimitConfigSetLimitPreservesOtherTypes(t *testing.T) {
	limit := StoreLimitConfig{
		AddPeer:          10,
		RemovePeer:       20,
		TransferLeaderIn: 30,
	}

	updated := limit.SetLimit(storelimit.AddPeer, 40)
	require.Equal(t, StoreLimitConfig{
		AddPeer:          40,
		RemovePeer:       20,
		TransferLeaderIn: 30,
	}, updated)
}

func TestScheduleConfigValidateTransferLeaderInLimit(t *testing.T) {
	config := &ScheduleConfig{}
	require.NoError(t, config.Adjust(configutil.NewConfigMetadata(nil), false))

	for _, rate := range []float64{-1, math.NaN(), math.Inf(1)} {
		config.DefaultStoreLimit.TransferLeaderIn = rate
		require.EqualError(t, config.Validate(),
			"default-store-limit.transfer-leader-in should be finite and non-negative")
	}

	config.DefaultStoreLimit.TransferLeaderIn = 0
	for _, rate := range []float64{-1, math.NaN(), math.Inf(1)} {
		config.StoreLimit[1] = StoreLimitConfig{TransferLeaderIn: rate}
		require.EqualError(t, config.Validate(),
			"store-limit[1].transfer-leader-in should be finite and non-negative")
	}
}
