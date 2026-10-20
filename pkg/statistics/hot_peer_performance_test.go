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

package statistics

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/statistics/utils"
)

func TestGetHotPeerStatsFiltersAllPeers(t *testing.T) {
	cache := &HotPeerCache{kind: utils.Read, peersOfStore: make(map[uint64]*utils.TopN)}
	peers := utils.NewTopN(utils.DimLen, 2, time.Hour)
	cache.peersOfStore[1] = peers
	cache.peersOfStore[2] = utils.NewTopN(utils.DimLen, 2, time.Hour)
	for i := range 20 {
		peer := &HotPeerStat{
			RegionID: uint64(i + 1), StoreID: 1, HotDegree: 4,
			AntiCount: utils.Read.DefaultAntiCount(), Loads: []float64{float64(i), 1, 1, 1},
		}
		switch i % 4 {
		case 1:
			peer.HotDegree = 3
		case 2:
			peer.inCold = true
		case 3:
			peer.AntiCount--
		}
		peers.Put(peer)
	}
	stats := cache.GetHotPeerStats(4)
	require.Len(t, stats, 2)
	require.Empty(t, stats[2])
	var ids []uint64
	for _, peer := range stats[1] {
		ids = append(ids, peer.RegionID)
		require.Same(t, peers.Get(peer.RegionID), peer)
	}
	require.ElementsMatch(t, []uint64{1, 5, 9, 13, 17}, ids)
}

func TestSummaryHotPeerLoadsUseSnapshot(t *testing.T) {
	peer := &HotPeerStat{
		RegionID: 1, StoreID: 1, isLeader: true,
		Loads: make([]float64, utils.DimLen), rollingLoads: make([]*dimStat, utils.DimLen),
	}
	for dim := range utils.DimLen {
		peer.rollingLoads[dim] = newDimStat(time.Second)
		peer.rollingLoads[dim].rolling.Set(float64(100 + dim))
	}
	result := SummaryStoresLoad(
		SummaryStoreInfos([]*core.StoreInfo{core.NewStoreInfo(&metapb.Store{Id: 1})}),
		map[uint64]StoreKindLoads{1: {}}, nil,
		map[uint64][]*HotPeerStat{1: {peer}}, true, utils.Write, constant.LeaderKind,
	)
	require.Len(t, result[1].HotPeers, 1)
	snapshot := result[1].HotPeers[0]
	require.NotSame(t, peer, snapshot)
	require.Nil(t, snapshot.rollingLoads)
	require.Equal(t, []float64{100, 101, 102, 103}, snapshot.Loads)
	require.Equal(t, snapshot.Loads[utils.ByteDim], result[1].LoadPred.Current.Loads[utils.ByteDim])
	require.Equal(t, snapshot.Loads[utils.KeyDim], result[1].LoadPred.Current.Loads[utils.KeyDim])
	peer.rollingLoads[utils.ByteDim].rolling.Set(500)
	require.Equal(t, float64(100), snapshot.GetLoad(utils.ByteDim))
}

var benchmarkHotPeerStats map[uint64][]*HotPeerStat

func BenchmarkHotPeerStats(b *testing.B) {
	for _, stores := range []int{1, 16} {
		for _, peers := range []int{100, 10000} {
			for _, eligiblePercent := range []int{10, 100} {
				b.Run(fmt.Sprintf("stores=%d/peers=%d/eligible=%d", stores, peers, eligiblePercent), func(b *testing.B) {
					cache := &HotPeerCache{kind: utils.Read, peersOfStore: make(map[uint64]*utils.TopN)}
					for storeID := uint64(1); storeID <= uint64(stores); storeID++ {
						top := utils.NewTopN(utils.DimLen, TopNN, time.Hour)
						cache.peersOfStore[storeID] = top
						for i := range peers {
							peer := &HotPeerStat{
								RegionID: uint64(i), StoreID: storeID, HotDegree: 4,
								AntiCount: utils.Read.DefaultAntiCount(), inCold: i%100 >= eligiblePercent,
								Loads: []float64{float64(i), 1, 1, 1},
							}
							top.Put(peer)
						}
					}
					b.ReportAllocs()
					for b.Loop() {
						benchmarkHotPeerStats = cache.GetHotPeerStats(4)
					}
				})
			}
		}
	}
}
