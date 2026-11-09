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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/statistics/utils"
)

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
