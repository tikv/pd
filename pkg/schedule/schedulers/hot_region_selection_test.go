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
	"math/rand/v2"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
)

func TestSelectHotPeers(t *testing.T) {
	for _, count := range []int{0, 1, 9, 10, 11, 100, 10000} {
		peers := benchmarkHotPeers(1, count)
		// Include ties and put some of the hottest peers beyond the initial heap.
		for i, peer := range peers {
			peer.Loads[utils.CPUDim] = float64(i / 3)
		}
		original := make([]*statistics.HotPeerStat, len(peers))
		copy(original, peers)
		for dim := range utils.DimLen {
			expected := append([]*statistics.HotPeerStat(nil), peers...)
			sort.Slice(expected, func(i, j int) bool {
				a, b := expected[i], expected[j]
				if a.GetLoad(dim) == b.GetLoad(dim) {
					return a.RegionID < b.RegionID
				}
				return a.GetLoad(dim) > b.GetLoad(dim)
			})
			for _, limit := range []int{0, 1, 10, 1000, count + 1} {
				t.Run(fmt.Sprintf("peers=%d/dim=%d/limit=%d", count, dim, limit), func(t *testing.T) {
					selected := selectHotPeers(peers, dim, limit)
					if min(limit, count) == 0 {
						require.Empty(t, selected)
					} else {
						require.Equal(t, expected[:min(limit, count)], selected)
					}
					require.Equal(t, original, peers)
				})
			}
		}
	}
}

func TestHotSolutionLoadSnapshot(t *testing.T) {
	s := &solution{
		srcStore:       benchmarkHotStore(1, nil, statistics.Loads{100, 40, 30, 80}),
		dstStore:       benchmarkHotStore(2, nil, statistics.Loads{40, 100, 70, 20}),
		mainPeerStat:   &statistics.HotPeerStat{Loads: []float64{20, 10, 4, 2}},
		revertPeerStat: &statistics.HotPeerStat{Loads: []float64{5, 15, 1, 1}},
	}
	s.srcStore.LoadPred.Future.Loads = statistics.Loads{80, 50, 20, 90}
	s.dstStore.LoadPred.Future.Loads = statistics.Loads{60, 90, 80, 10}
	s.calcPeersRate(utils.ByteDim, utils.KeyDim)
	best := *s
	for dim := range utils.DimLen {
		src, dst := s.getExtremeLoad(dim)
		if s.getPeersRateFromCache(dim) >= 0 {
			require.Equal(t, s.srcStore.LoadPred.Min().Loads[dim], src)
			require.Equal(t, s.dstStore.LoadPred.Max().Loads[dim], dst)
		} else {
			require.Equal(t, s.srcStore.LoadPred.Max().Loads[dim], src)
			require.Equal(t, s.dstStore.LoadPred.Min().Loads[dim], dst)
		}
		src, dst = s.getPendingLoad(dim)
		require.Equal(t, s.srcStore.LoadPred.Pending().Loads[dim], src)
		require.Equal(t, s.dstStore.LoadPred.Pending().Loads[dim], dst)
	}
	s.revertPeerStat = nil
	s.calcPeersRate(utils.QueryDim, utils.CPUDim)
	require.Equal(t, [utils.DimLen]float64{15, -5, 0, 0}, best.cachedPeersRate)
	require.Equal(t, [utils.DimLen]float64{0, 0, 4, 2}, s.cachedPeersRate)
}

func TestHotStoreComparison(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	stores := make([]*statistics.StoreLoadDetail, 20)
	for i := range stores {
		detail := benchmarkHotStore(uint64(i+1), nil, statistics.Loads{})
		for _, load := range []*statistics.StoreLoad{&detail.LoadPred.Current, &detail.LoadPred.Future} {
			for dim := range utils.DimLen {
				load.Loads[dim] = float64(rng.IntN(50))
			}
			load.HotPeerCount = float64(rng.IntN(5))
		}
		stores[i] = detail
	}
	// Include equal extrema with different pending influence and peer counts.
	stores[1].LoadPred.Current = stores[0].LoadPred.Future
	stores[1].LoadPred.Future = stores[0].LoadPred.Current
	for ty := resourceType(0); ty < resourceTypeLen; ty++ {
		for first := range utils.DimLen {
			for second := range utils.DimLen {
				bs := &balanceSolver{resourceTy: ty, firstPriority: first, secondPriority: second,
					maxSrc:   &statistics.StoreLoad{Loads: statistics.Loads{100, 100, 100, 100}},
					minDst:   &statistics.StoreLoad{},
					rankStep: &statistics.StoreLoad{Loads: statistics.Loads{10, 10, 10, 10}, HotPeerCount: 2}}
				for _, a := range stores {
					for _, b := range stores {
						for _, source := range []bool{false, true} {
							expected := slices.Compare(hotStoreComparisonRanks(bs, a, source), hotStoreComparisonRanks(bs, b, source))
							require.Equal(t, expected, bs.compareStoreLoad(a, b, source))
						}
					}
				}
			}
		}
	}
}

// hotStoreComparisonRanks expresses the original ordering using complete StoreLoad objects.
func hotStoreComparisonRanks(bs *balanceSolver, detail *statistics.StoreLoadDetail, source bool) []int64 {
	load, baseline, sign := detail.LoadPred.Max(), bs.minDst, int64(1)
	if source {
		load, baseline, sign = detail.LoadPred.Min(), bs.maxSrc, -1
	}
	ranks := []int64{}
	for _, dim := range []int{bs.firstPriority, bs.secondPriority} {
		ranks = append(ranks, sign*int64((load.Loads[dim]-baseline.Loads[dim])/10))
	}
	pending := detail.LoadPred.Diff()
	if bs.resourceTy == writeLeader {
		ranks = append(ranks, int64(pending.HotPeerCount/2))
	}
	ranks = append(ranks, int64(pending.Loads[bs.firstPriority]/10))
	if bs.resourceTy == writeLeader {
		ranks = append(ranks, int64(pending.Loads[bs.secondPriority]/10))
	}
	return ranks
}

func TestHotSolverRegionSnapshotLifetime(t *testing.T) {
	hot, cluster := prepareHotSolverSearch(t, 4, 8, "revert")
	bs := newBalanceSolver(hot, cluster, utils.Write, movePeer)
	hot.searchRevertRegions[writePeer] = true
	require.Len(t, bs.solve(), 2)
	// Regions checked before revert search starts may be fetched a second time.
	// Subsequent combinations reuse their snapshots regardless of source ordering.
	require.GreaterOrEqual(t, cluster.regionLookups, 32)
	require.LessOrEqual(t, cluster.regionLookups, 64)
	require.Nil(t, bs.regionCache)
	var regions []*core.RegionInfo
	for _, detail := range hot.stLoadInfos[writePeer] {
		for _, peer := range detail.HotPeers {
			region := cluster.SchedulerCluster.GetRegion(peer.RegionID)
			regions = append(regions, region)
			cluster.GetBasicCluster().PutRegion(region.Clone(core.WithDownPeers([]*pdpb.PeerStats{{Peer: region.GetLeader(), DownSeconds: 1}})))
		}
	}
	// A subsequent solve sees unhealthy regions instead of reusing the old snapshot.
	bs = newBalanceSolver(hot, cluster, utils.Write, movePeer)
	require.Empty(t, bs.solve())
	require.Nil(t, bs.regionCache)
	for _, region := range regions {
		cluster.GetBasicCluster().PutRegion(region)
	}
	hot.searchRevertRegions[writePeer] = true
	require.Len(t, newBalanceSolver(hot, cluster, utils.Write, movePeer).solve(), 2)
}

func TestHotSolverRegionSnapshotChecksSource(t *testing.T) {
	hot, cluster := prepareHotSolverSearch(t, 4, 8, "move")
	peer := hot.stLoadInfos[writePeer][1].HotPeers[0]
	for _, ty := range []opType{movePeer, transferLeader} {
		bs := newBalanceSolver(hot, cluster, utils.Write, ty)
		bs.regionCache = make(map[uint64]hotRegionSnapshot)
		cluster.regionLookups = 0
		region, _ := bs.getRegion(peer, 4)
		require.Nil(t, region)
		sourceID := uint64(1)
		if ty == transferLeader {
			sourceID = 5
		}
		region, _ = bs.getRegion(peer, sourceID)
		require.NotNil(t, region)
		require.Equal(t, 1, cluster.regionLookups)
		missing := &statistics.HotPeerStat{RegionID: 999}
		for range 2 {
			region, _ = bs.getRegion(missing, sourceID)
			require.Nil(t, region)
		}
		require.Equal(t, 2, cluster.regionLookups)
	}
}

func TestHotSolverFindsLatePeer(t *testing.T) {
	hot, cluster := prepareHotSolverSearch(t, 4, 64, "move")
	useful := hot.stLoadInfos[writePeer][1].HotPeers[63]
	useful.Loads = []float64{3.1e6, 1.9e6, 0, 0}
	for _, detail := range hot.stLoadInfos[writePeer] {
		for _, peer := range detail.HotPeers {
			if peer != useful {
				hot.regionPendings[peer.RegionID] = &pendingInfluence{}
			}
		}
	}
	bs := newBalanceSolver(hot, cluster, utils.Write, movePeer)
	bs.maxPeerNum = 4
	ops := bs.solve()
	require.Len(t, ops, 1)
	require.Equal(t, useful.RegionID, ops[0].RegionID())
}

func TestHotPeerThresholdIncludesPendingPeers(t *testing.T) {
	peers := make([]*statistics.HotPeerStat, 100)
	pending := make(map[uint64]*pendingInfluence)
	for i := range peers {
		id := uint64(i + 1)
		peers[i] = &statistics.HotPeerStat{RegionID: id, StoreID: 1, Loads: []float64{float64(100 - i), float64(i)}}
		if i < 10 {
			pending[id] = &pendingInfluence{}
		}
	}
	bs := &balanceSolver{
		sche:             &hotScheduler{baseHotScheduler: &baseHotScheduler{regionPendings: pending}},
		stLoadDetail:     map[uint64]*statistics.StoreLoadDetail{1: benchmarkHotStore(1, peers, statistics.Loads{})},
		filteredHotPeers: make(map[uint64][]*statistics.HotPeerStat),
		nthHotPeer:       make(map[uint64][]*statistics.HotPeerStat),
		firstPriority:    utils.ByteDim,
		secondPriority:   utils.KeyDim,
		maxPeerNum:       4,
	}
	// The small-region safeguard uses the whole snapshot, including pending peers.
	require.Equal(t, float64(91), bs.getNthHotPeer(1, utils.ByteDim).GetLoad(utils.ByteDim))
	require.Equal(t, float64(90), bs.getNthHotPeer(1, utils.KeyDim).GetLoad(utils.KeyDim))
	var selected []uint64
	for _, peer := range bs.getFilteredHotPeers(1) {
		require.NotContains(t, pending, peer.RegionID)
		selected = append(selected, peer.RegionID)
	}
	require.ElementsMatch(t, []uint64{99, 100}, selected)
}
