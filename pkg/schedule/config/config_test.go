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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/utils/configutil"
)

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
