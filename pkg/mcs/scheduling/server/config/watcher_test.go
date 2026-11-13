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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/operator"
)

type storeLimitReadHook struct {
	sc.SharedConfigProvider
	afterRead func()
}

func (c *storeLimitReadHook) PeekStoreLimitByType(storeID uint64, typ storelimit.Type) float64 {
	rate := c.SharedConfigProvider.PeekStoreLimitByType(storeID, typ)
	if afterRead := c.afterRead; afterRead != nil {
		c.afterRead = nil
		afterRead()
	}
	return rate
}

func TestRefreshStoreRateLimitsWithOperator(t *testing.T) {
	for _, typ := range []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer, storelimit.TransferLeaderIn} {
		t.Run(typ.String(), func(t *testing.T) {
			re := require.New(t)
			const initialRate = 0.00006
			persistConfig := NewPersistConfig(&Config{Schedule: sc.ScheduleConfig{
				StoreLimit: map[uint64]sc.StoreLimitConfig{1: {
					AddPeer: initialRate, RemovePeer: initialRate, TransferLeaderIn: initialRate,
				}},
			}}, nil)
			basicCluster := core.NewBasicCluster()
			limiter := storelimit.NewStoreRateLimit(initialRate / operator.StoreBalanceBaseTime)
			store := core.NewStoreInfo(&metapb.Store{Id: 1}, core.SetStoreLimit(limiter))
			basicCluster.PutStore(store)
			peers := []*metapb.Peer{{Id: 2, StoreId: 2}}
			var step operator.OpStep = operator.AddPeer{ToStore: 1, PeerID: 1}
			if typ != storelimit.AddPeer {
				peers = append(peers, &metapb.Peer{Id: 1, StoreId: 1})
				step = operator.RemovePeer{FromStore: 1, PeerID: 1}
				if typ == storelimit.TransferLeaderIn {
					step = operator.TransferLeader{FromStore: 2, ToStore: 1}
				}
			}
			region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[0], core.SetApproximateSize(96))
			basicCluster.PutRegion(region)
			watcher := &Watcher{basicCluster: basicCluster}
			cfg := &storeLimitReadHook{SharedConfigProvider: persistConfig}
			// Complete a watcher update after the controller reads the old rate,
			// before it can reset the limiter with that stale value.
			cfg.afterRead = func() {
				updated := persistConfig.GetScheduleConfig().Clone()
				updated.StoreLimit[1] = updated.StoreLimit[1].SetLimit(typ, storelimit.Unlimited)
				persistConfig.SetScheduleConfig(updated)
				watcher.refreshStoreRateLimits(updated)
			}
			controller := operator.NewController(t.Context(), basicCluster, cfg, nil)
			op := operator.NewTestOperator(1, region.GetRegionEpoch(), operator.OpRegion, step)

			re.False(controller.ExceedStoreLimit(op))
			re.Nil(cfg.afterRead)
			re.Equal(storelimit.Unlimited/operator.StoreBalanceBaseTime, limiter.(*storelimit.StoreRateLimit).Rate(typ))
			re.True(limiter.Take(storelimit.RegionInfluence[typ], typ, constant.Medium))
			re.True(store.IsAvailable(typ, constant.Medium))
		})
	}
}

func TestRefreshStoreRateLimits(t *testing.T) {
	re := require.New(t)
	initialRate := 0.00006
	persistConfig := NewPersistConfig(&Config{
		Schedule: sc.ScheduleConfig{
			StoreLimit: map[uint64]sc.StoreLimitConfig{1: {
				AddPeer:          initialRate,
				RemovePeer:       initialRate,
				TransferLeaderIn: initialRate,
			}},
		},
	}, nil)
	basicCluster := core.NewBasicCluster()
	limiter := storelimit.NewStoreRateLimit(initialRate / 60)
	store := core.NewStoreInfo(
		&metapb.Store{Id: 1},
		core.SetLastHeartbeatTS(time.Now()),
		core.SetStoreLimit(limiter),
	)
	for _, typ := range []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer, storelimit.TransferLeaderIn} {
		re.True(limiter.Take(storelimit.RegionInfluence[typ], typ, constant.Medium))
		re.False(store.IsAvailable(typ, constant.Medium))
	}
	basicCluster.PutStore(store)
	watcher := &Watcher{basicCluster: basicCluster}
	watcher.refreshStoreRateLimits(persistConfig.GetScheduleConfig())
	for _, typ := range []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer, storelimit.TransferLeaderIn} {
		re.False(store.IsAvailable(typ, constant.Medium), typ.String())
	}

	updated := persistConfig.GetScheduleConfig().Clone()
	updated.StoreLimit[1] = sc.StoreLimitConfig{
		AddPeer:          storelimit.Unlimited,
		RemovePeer:       storelimit.Unlimited,
		TransferLeaderIn: storelimit.Unlimited,
	}
	persistConfig.SetScheduleConfig(updated)
	watcher.refreshStoreRateLimits(persistConfig.GetScheduleConfig())
	store = basicCluster.GetStore(1)
	limit, ok := store.GetStoreLimit().(*storelimit.StoreRateLimit)
	re.True(ok)
	for _, typ := range []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer, storelimit.TransferLeaderIn} {
		re.Equal(storelimit.Unlimited/60, limit.Rate(typ))
		re.True(store.IsAvailable(typ, constant.Medium), typ.String())
	}
}

func TestRefreshStoreRateLimitsPreservesConfig(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		storeLimit map[uint64]sc.StoreLimitConfig
	}{
		{name: "nil store limits"},
		{name: "empty store limits", storeLimit: map[uint64]sc.StoreLimitConfig{}},
		{
			name: "explicit store limits",
			storeLimit: map[uint64]sc.StoreLimitConfig{1: {
				AddPeer:          120,
				RemovePeer:       180,
				TransferLeaderIn: 240,
			}},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			cfg := &Config{Schedule: sc.ScheduleConfig{
				DefaultStoreLimit: sc.StoreLimitConfig{
					AddPeer:          30,
					RemovePeer:       60,
					TransferLeaderIn: 120,
				},
				StoreLimit: testCase.storeLimit,
			}}
			expected := cfg.Schedule.Clone()
			persistConfig := NewPersistConfig(cfg, nil)
			basicCluster := core.NewBasicCluster()
			basicCluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
			// Tombstoned stores remain in the cluster after PD removes their
			// store-limit entry. Refreshing must not recreate that entry.
			basicCluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 2, NodeState: metapb.NodeState_Removed}))
			watcher := &Watcher{basicCluster: basicCluster}

			watcher.refreshStoreRateLimits(persistConfig.GetScheduleConfig())

			re.Same(&cfg.Schedule, persistConfig.GetScheduleConfig())
			re.Equal(expected, persistConfig.GetScheduleConfig())
			for _, store := range basicCluster.GetStores() {
				limit, ok := store.GetStoreLimit().(*storelimit.StoreRateLimit)
				re.True(ok)
				expectedLimit, ok := expected.StoreLimit[store.GetID()]
				if !ok {
					expectedLimit = expected.GetDefaultStoreLimit()
				}
				re.Equal(expectedLimit.AddPeer/60, limit.Rate(storelimit.AddPeer))
				re.Equal(expectedLimit.RemovePeer/60, limit.Rate(storelimit.RemovePeer))
				re.Equal(expectedLimit.TransferLeaderIn/60, limit.Rate(storelimit.TransferLeaderIn))
			}
		})
	}
}
