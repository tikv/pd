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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
)

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
