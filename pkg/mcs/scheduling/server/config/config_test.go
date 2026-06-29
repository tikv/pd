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

	// Simulate a restarted process whose package-level default goes back to the built-in value.
	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, 15)
	sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, 15)
	re.Equal(sc.StoreLimitConfig{AddPeer: 60, RemovePeer: 15}, persistConfig.GetStoreLimit(2))
}
