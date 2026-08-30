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

package statistics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestIsHot(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := utils.RWType(0); i < utils.RWTypeLen; i++ {
		cluster := core.NewBasicCluster()
		cache := NewHotCache(ctx, cluster)
		region, err := buildRegion(cluster, i, 3, 60)
		re.NoError(err)
		loads := make([]float64, utils.RegionStatCount)
		loads[utils.RegionReadBytes] = 100000000
		loads[utils.RegionReadKeys] = 1000
		loads[utils.RegionReadQueryNum] = 1000
		stats := cache.CheckReadPeerSync(region, region.GetPeers(), loads, 60)
		cache.Update(stats[0], i)
		for range 100 {
			re.True(cache.IsRegionHot(region, 1))
		}
	}
}

func TestHotCacheGetHotPeerStatsForStores(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cache := NewHotCache(ctx, core.NewBasicCluster())

	for _, kind := range []utils.RWType{utils.Read, utils.Write} {
		peerCache := cache.readCache
		if kind == utils.Write {
			peerCache = cache.writeCache
		}
		antiCount := kind.DefaultAntiCount()
		putTestHotPeer(peerCache, 1, 11, 3, antiCount, false)
		putTestHotPeer(peerCache, 2, 21, 3, antiCount, false)
		putTestHotPeer(peerCache, 3, 31, 3, antiCount, false)

		stats := cache.GetHotPeerStatsForStores(kind, []uint64{1, 3}, 1)
		re.NotNil(stats)
		re.Equal([]uint64{11}, hotPeerRegionIDs(stats[1]))
		re.Equal([]uint64{31}, hotPeerRegionIDs(stats[3]))
		re.NotContains(stats, uint64(2))

		stats = cache.GetHotPeerStatsForStores(kind, []uint64{99}, 1)
		re.NotNil(stats)
		re.Empty(stats)
	}

	canceledCtx, cancelImmediately := context.WithCancel(context.Background())
	cancelImmediately()
	canceledCache := &HotCache{
		ctx:        canceledCtx,
		readCache:  NewHotPeerCache(canceledCtx, core.NewBasicCluster(), utils.Read),
		writeCache: NewHotPeerCache(canceledCtx, core.NewBasicCluster(), utils.Write),
	}
	re.Nil(canceledCache.GetHotPeerStatsForStores(utils.Read, []uint64{1}, 1))
}
