// Copyright 2025 TiKV Project Authors.
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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
)

type solution struct {
	srcStore     *statistics.StoreLoadDetail
	region       *core.RegionInfo // The region of the main balance effect. Relate mainPeerStat. srcStore -> dstStore
	mainPeerStat *statistics.HotPeerStat

	dstStore       *statistics.StoreLoadDetail
	revertRegion   *core.RegionInfo // The regions to hedge back effects. Relate revertPeerStat. dstStore -> srcStore
	revertPeerStat *statistics.HotPeerStat

	cachedPeersRate []float64

	// progressiveRank measures the contribution for balance.
	// The bigger the rank, the better this solution is.
	// If progressiveRank >= 0, this solution makes thing better.
	// 0 indicates that this is a solution that cannot be used directly, but can be optimized.
	// -1 indicates that this is a non-optimizable solution.
	// See `calcProgressiveRank` for more about progressive rank.
	progressiveRank int64
	// only for rank v2
	firstScore  int
	secondScore int
}

// getExtremeLoad returns the closest load in the selected src and dst statistics.
// in other word, the min load of the src store and the max load of the dst store.
// If peersRate is negative, the direction is reversed.
func (s *solution) getExtremeLoad(dim int) (src float64, dst float64) {
	if s.getPeersRateFromCache(dim) >= 0 {
		return s.srcStore.LoadPred.Min().Loads[dim], s.dstStore.LoadPred.Max().Loads[dim]
	}
	return s.srcStore.LoadPred.Max().Loads[dim], s.dstStore.LoadPred.Min().Loads[dim]
}

// getCurrentLoad returns the current load of the src store and the dst store.
func (s *solution) getCurrentLoad(dim int) (src float64, dst float64) {
	return s.srcStore.LoadPred.Current.Loads[dim], s.dstStore.LoadPred.Current.Loads[dim]
}

// getPendingLoad returns the pending load of the src store and the dst store.
func (s *solution) getPendingLoad(dim int) (src float64, dst float64) {
	return s.srcStore.LoadPred.Pending().Loads[dim], s.dstStore.LoadPred.Pending().Loads[dim]
}

// calcPeersRate precomputes the peer rate and stores it in cachedPeersRate.
func (s *solution) calcPeersRate(dims ...int) {
	s.cachedPeersRate = make([]float64, utils.DimLen)
	for _, dim := range dims {
		peersRate := s.mainPeerStat.GetLoad(dim)
		if s.revertPeerStat != nil {
			peersRate -= s.revertPeerStat.GetLoad(dim)
		}
		s.cachedPeersRate[dim] = peersRate
	}
}

// getPeersRateFromCache returns the load of the peer. Need to calcPeersRate first.
func (s *solution) getPeersRateFromCache(dim int) float64 {
	return s.cachedPeersRate[dim]
}
