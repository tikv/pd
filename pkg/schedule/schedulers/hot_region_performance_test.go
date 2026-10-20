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

package schedulers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
)

var benchmarkHotPeerSelection []*statistics.HotPeerStat

func benchmarkHotPeers(storeID uint64, count int) []*statistics.HotPeerStat {
	peers := make([]*statistics.HotPeerStat, count)
	for i := range peers {
		peers[i] = &statistics.HotPeerStat{
			StoreID: storeID, RegionID: storeID*1000000 + uint64(i),
			Loads: []float64{
				float64((i*7919)%count + 1),
				float64((i*3571)%count + 1),
				float64((i*101)%count + 1),
				float64(i + 1),
			},
		}
	}
	return peers
}

func benchmarkHotStore(id uint64, peers []*statistics.HotPeerStat, loads statistics.Loads) *statistics.StoreLoadDetail {
	detail := &statistics.StoreLoadDetail{
		StoreSummaryInfo: &statistics.StoreSummaryInfo{StoreInfo: core.NewStoreInfoWithLabel(id, nil)},
		HotPeers:         peers,
		LoadPred:         (statistics.StoreLoad{Loads: loads}).ToLoadPred(utils.Write, nil),
	}
	detail.LoadPred.Expect.Loads = statistics.Loads{15e6, 15e6}
	detail.LoadPred.Stddev.Loads = statistics.Loads{0.5, 0.5}
	return detail
}

func BenchmarkHotPeerSelection(b *testing.B) {
	for _, count := range []int{100, 1000, 10000} {
		for _, limit := range []int{10, 1000} {
			for _, eligiblePercent := range []int{25, 100} {
				b.Run(fmt.Sprintf("peers=%d/limit=%d/eligible=%d", count, limit, eligiblePercent), func(b *testing.B) {
					peers := benchmarkHotPeers(1, count)
					pending := make(map[uint64]*pendingInfluence)
					for i, peer := range peers {
						if i%100 >= eligiblePercent {
							pending[peer.RegionID] = &pendingInfluence{}
						}
					}
					bs := &balanceSolver{
						sche:           &hotScheduler{baseHotScheduler: &baseHotScheduler{regionPendings: pending}},
						nthHotPeer:     map[uint64][]*statistics.HotPeerStat{1: make([]*statistics.HotPeerStat, utils.DimLen)},
						firstPriority:  utils.ByteDim,
						secondPriority: utils.KeyDim,
						maxPeerNum:     limit,
					}
					detail := benchmarkHotStore(1, peers, statistics.Loads{})
					b.ReportAllocs()
					for b.Loop() {
						benchmarkHotPeerSelection = bs.filterHotPeers(detail)
					}
				})
			}
		}
	}
}

func BenchmarkHotReadSolverPreparation(b *testing.B) {
	for _, stores := range []int{16, 128} {
		for _, peers := range []int{100, 1000} {
			b.Run(fmt.Sprintf("stores=%d/peers=%d", stores, peers), func(b *testing.B) {
				cancel, _, cluster, controller := prepareSchedulersTest()
				b.Cleanup(cancel)
				scheduler, err := CreateScheduler(types.BalanceHotRegionScheduler, controller,
					storage.NewStorageWithMemoryBackend(), ConfigSliceDecoder(types.BalanceHotRegionScheduler, nil))
				require.NoError(b, err)
				hot := scheduler.(*hotScheduler)
				for id := uint64(1); id <= uint64(stores); id++ {
					detail := benchmarkHotStore(id, benchmarkHotPeers(id, peers), statistics.Loads{15e6, 15e6})
					hot.stLoadInfos[readLeader][id] = detail
					hot.stLoadInfos[readPeer][id] = detail
				}
				b.ReportAllocs()
				for b.Loop() {
					benchmarkBalanceSolvers[0], benchmarkBalanceSolvers[1] = newBalanceReadSolvers(hot, cluster)
				}
			})
		}
	}
}

type hotSolverTestCluster struct {
	sche.SchedulerCluster
	regionLookups int
}

func (c *hotSolverTestCluster) GetRegion(id uint64) *core.RegionInfo {
	c.regionLookups++
	return c.SchedulerCluster.GetRegion(id)
}

var benchmarkHotOperators []*operator.Operator

type hotSolverCountingRank struct {
	rank
	evaluations int
}

func (r *hotSolverCountingRank) calcProgressiveRank() {
	r.evaluations++
	r.rank.calcProgressiveRank()
}

func BenchmarkHotSolverSearch(b *testing.B) {
	for _, stores := range []int{4, 16} {
		for _, peers := range []int{8, 64} {
			for _, scenario := range []string{"no-source", "no-destination", "move", "revert"} {
				b.Run(fmt.Sprintf("stores=%d/peers=%d/%s", stores, peers, scenario), func(b *testing.B) {
					hot, counted := prepareHotSolverSearch(b, stores, peers, scenario)
					hot.searchRevertRegions[writePeer] = scenario == "revert"
					ops := newBalanceSolver(hot, counted, utils.Write, movePeer).solve()
					switch scenario {
					case "no-source", "no-destination":
						require.Empty(b, ops)
					case "move":
						require.Len(b, ops, 1)
					case "revert":
						require.Len(b, ops, 2)
					}
					counted.regionLookups = 0
					evaluations := 0
					b.ReportAllocs()
					for b.Loop() {
						hot.searchRevertRegions[writePeer] = scenario == "revert"
						bs := newBalanceSolver(hot, counted, utils.Write, movePeer)
						rank := &hotSolverCountingRank{rank: bs.rank}
						bs.rank = rank
						benchmarkHotOperators = bs.solve()
						evaluations += rank.evaluations
					}
					b.ReportMetric(float64(counted.regionLookups)/float64(b.N), "region-lookups/op")
					b.ReportMetric(float64(evaluations)/float64(b.N), "candidate-evals/op")
				})
			}
		}
	}
}

func prepareHotSolverSearch(tb testing.TB, stores, peers int, scenario string) (*hotScheduler, *hotSolverTestCluster) {
	cancel, _, cluster, controller := prepareSchedulersTest()
	tb.Cleanup(cancel)
	scheduler, err := CreateScheduler(writeType, controller, storage.NewStorageWithMemoryBackend(), nil)
	require.NoError(tb, err)
	hot := scheduler.(*hotScheduler)
	for id := uint64(1); id <= uint64(stores+2); id++ {
		cluster.AddRegionStore(id, peers)
	}
	for id := uint64(1); id <= uint64(stores); id++ {
		loads, peerLoads := statistics.Loads{20e6, 14e6}, []float64{3e6, 1.8e6, 0, 0}
		if int(id) > stores/2 {
			loads, peerLoads = statistics.Loads{10e6, 16e6}, []float64{0.1e6, 2e6, 0, 0}
		}
		switch scenario {
		case "no-source":
			loads = statistics.Loads{15e6, 15e6}
		case "no-destination":
			loads = statistics.Loads{20e6, 20e6}
		}
		stats := make([]*statistics.HotPeerStat, peers)
		for i := range stats {
			regionID := id*1000000 + uint64(i)
			regionPeers := []*metapb.Peer{
				{Id: regionID*10 + 1, StoreId: id},
				{Id: regionID*10 + 2, StoreId: uint64(stores + 1)},
				{Id: regionID*10 + 3, StoreId: uint64(stores + 2)},
			}
			cluster.PutRegion(core.NewRegionInfo(&metapb.Region{
				Id: regionID, Peers: regionPeers,
				StartKey: []byte(fmt.Sprintf("%020d", regionID)),
				EndKey:   []byte(fmt.Sprintf("%020d", regionID+1)),
			}, regionPeers[1], core.SetApproximateSize(10)))
			stats[i] = &statistics.HotPeerStat{RegionID: regionID, StoreID: id, Loads: peerLoads}
		}
		detail := benchmarkHotStore(id, stats, loads)
		detail.StoreInfo = cluster.GetStore(id)
		hot.stLoadInfos[writePeer][id] = detail
	}
	return hot, &hotSolverTestCluster{SchedulerCluster: cluster}
}
