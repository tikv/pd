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

package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	sc "github.com/tikv/pd/pkg/schedule/config"
)

func TestRefreshStoreRateLimits(t *testing.T) {
	re := require.New(t)
	initialRate := 0.00006
	persistConfig := config.NewPersistConfig(&config.Config{
		Schedule: sc.ScheduleConfig{
			StoreLimit: map[uint64]sc.StoreLimitConfig{1: {
				AddPeer:          initialRate,
				RemovePeer:       initialRate,
				TransferLeaderIn: initialRate,
			}},
		},
	}, nil)
	basicCluster := core.NewBasicCluster()
	limiter := storelimit.NewStoreRateLimit(0.000001)
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
	cluster := &Cluster{BasicCluster: basicCluster, persistConfig: persistConfig}

	updated := persistConfig.GetScheduleConfig().Clone()
	updated.StoreLimit[1] = sc.StoreLimitConfig{
		AddPeer:          storelimit.Unlimited,
		RemovePeer:       storelimit.Unlimited,
		TransferLeaderIn: storelimit.Unlimited,
	}
	persistConfig.SetScheduleConfig(updated)
	cluster.RefreshStoreRateLimits()
	store = cluster.GetStore(1)
	limit, ok := store.GetStoreLimit().(*storelimit.StoreRateLimit)
	re.True(ok)
	for _, typ := range []storelimit.Type{storelimit.AddPeer, storelimit.RemovePeer, storelimit.TransferLeaderIn} {
		re.Equal(storelimit.Unlimited/60, limit.Rate(typ))
		re.True(store.IsAvailable(typ, constant.Medium), typ.String())
	}
}
