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

func TestCollectHotPeerInfosWithObserver(t *testing.T) {
	stores := []*core.StoreInfo{
		core.NewStoreInfo(&metapb.Store{Id: 1}),
		core.NewStoreInfo(&metapb.Store{Id: 2}),
		core.NewStoreInfo(&metapb.Store{Id: 3}),
	}
	stats := map[uint64][]*HotPeerStat{
		1: {
			{RegionID: 1, isLeader: true, Loads: []float64{1.4, 2.6, 3.4, 4.6}},
			{RegionID: 2, Loads: []float64{10, 20, 30, 40}},
			{RegionID: 3, isLeader: true},
		},
		2:  nil,
		99: {{RegionID: 99, Loads: []float64{100}}},
	}
	want := &StoreHotPeersInfos{
		AsPeer: StoreHotPeersStat{
			1: {TotalBytesRate: 11, TotalKeysRate: 23, TotalQueryRate: 33, TotalCPURate: 45, Count: 3},
			2: {},
		},
		AsLeader: StoreHotPeersStat{
			1: {TotalBytesRate: 1, TotalKeysRate: 3, TotalQueryRate: 3, TotalCPURate: 5, Count: 2},
			2: {},
		},
	}
	// Keep the original function's nil/empty Store behavior.
	collect := CollectHotPeerInfos
	require.Equal(t, want, collect(stores, stats))
	observedStores := make([]uint64, 0)
	observedLoads := make(map[uint64]Loads)
	got := CollectHotPeerInfosWithObserver(stores, stats, func(store *core.StoreInfo) func(*HotPeerStat, Loads) {
		observedStores = append(observedStores, store.GetID())
		if store.GetID() == 2 {
			return nil
		}
		return func(peer *HotPeerStat, loads Loads) { observedLoads[peer.RegionID] = loads }
	})
	require.Equal(t, want, got)
	require.Equal(t, []uint64{1, 2}, observedStores)
	require.Equal(t, map[uint64]Loads{1: {1, 3, 3, 5}, 2: {10, 20, 30, 40}, 3: {}}, observedLoads)
	empty := collect(nil, nil)
	require.NotNil(t, empty.AsPeer)
	require.NotNil(t, empty.AsLeader)
	require.Empty(t, empty.AsPeer)
	require.Empty(t, empty.AsLeader)
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
