// Copyright 2018 TiKV Project Authors.
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
	"strconv"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/statistics/utils"
)

func TestStoreStatistics(t *testing.T) {
	re := require.New(t)
	opt := mockconfig.NewTestOptions()
	rep := opt.GetReplicationConfig().Clone()
	rep.LocationLabels = []string{"zone", "host"}
	opt.SetReplicationConfig(rep)

	metaStores := []*metapb.Store{
		{Id: 1, Address: "mock://tikv-1", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h1"}}},
		{Id: 2, Address: "mock://tikv-2", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h2"}}},
		{Id: 3, Address: "mock://tikv-3", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z2"}, {Key: "host", Value: "h1"}}},
		{Id: 4, Address: "mock://tikv-4", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z2"}, {Key: "host", Value: "h2"}}},
		{Id: 5, Address: "mock://tikv-5", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z3"}, {Key: "host", Value: "h1"}}},
		{Id: 6, Address: "mock://tikv-6", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z3"}, {Key: "host", Value: "h2"}}},
		{Id: 7, Address: "mock://tikv-7", Labels: []*metapb.StoreLabel{{Key: "host", Value: "h1"}}},
		{Id: 8, Address: "mock://tikv-8", Labels: []*metapb.StoreLabel{{Key: "host", Value: "h2"}}},
		{Id: 8, Address: "mock://tikv-9", Labels: []*metapb.StoreLabel{{Key: "host", Value: "h3"}}, State: metapb.StoreState_Tombstone, NodeState: metapb.NodeState_Removed},
	}
	storesStats := NewStoresStats()
	stores := make([]*core.StoreInfo, 0, len(metaStores))
	for _, m := range metaStores {
		s := core.NewStoreInfo(m, core.SetLastHeartbeatTS(time.Now()))
		storesStats.GetOrCreateRollingStoreStats(m.GetId())
		stores = append(stores, s)
	}

	store3 := stores[3].Clone(core.SetStoreState(metapb.StoreState_Offline, false))
	stores[3] = store3
	store4 := stores[4].Clone(core.SetLastHeartbeatTS(stores[4].GetLastHeartbeatTS().Add(-time.Hour)))
	stores[4] = store4
	store5 := stores[5].Clone(core.SetStoreStats(&pdpb.StoreStats{
		Capacity:  512 * units.MiB,
		Available: 100 * units.MiB,
		UsedSize:  0,
	}))
	stores[5] = store5
	storeStats := NewStoreStatisticsMap(opt)
	for _, store := range stores {
		storeStats.Observe(store)
		ObserveHotStat(store, storesStats)
	}
	stats := storeStats.stats

	re.Equal(6, stats.Up)
	re.Equal(7, stats.Preparing)
	re.Equal(0, stats.Serving)
	re.Equal(1, stats.Removing)
	re.Equal(1, stats.Removed)
	re.Equal(1, stats.Down)
	re.Equal(1, stats.Offline)
	re.Equal(0, stats.RegionCount)
	re.Equal(0, stats.WitnessCount)
	re.Equal(0, stats.Unhealthy)
	re.Equal(0, stats.Disconnect)
	re.Equal(1, stats.Tombstone)
	re.Equal(1, stats.LowSpace)
	re.Len(stats.LabelCounter["zone:z1"], 2)
	re.Equal([]uint64{1, 2}, stats.LabelCounter["zone:z1"])
	re.Len(stats.LabelCounter["zone:z2"], 2)
	re.Len(stats.LabelCounter["zone:z3"], 2)
	re.Len(stats.LabelCounter["host:h1"], 4)
	re.Equal([]uint64{1, 3, 5, 7}, stats.LabelCounter["host:h1"])
	re.Len(stats.LabelCounter["host:h2"], 4)
	re.Len(stats.LabelCounter["zone:unknown"], 2)
}

func TestSummaryStoreInfos(t *testing.T) {
	re := require.New(t)
	rw := utils.Read
	kind := constant.LeaderKind
	collector := newTikvCollector()
	storeHistoryLoad := NewStoreHistoryLoads(utils.DimLen, DefaultHistorySampleDuration, DefaultHistorySampleInterval)
	storeInfos := make(map[uint64]*StoreSummaryInfo)
	storeLoads := make(map[uint64][]float64)
	for _, storeID := range []int{1, 3} {
		storeInfos[uint64(storeID)] = &StoreSummaryInfo{
			isTiFlash: false,
			StoreInfo: core.NewStoreInfo(&metapb.Store{Id: uint64(storeID), Address: "mock://tikv" + strconv.Itoa(storeID)}, core.SetLastHeartbeatTS(time.Now())),
		}
		storeLoads[uint64(storeID)] = []float64{1, 2, 0, 0, 5}
		for i, v := range storeLoads[uint64(storeID)] {
			storeLoads[uint64(storeID)][i] = v * float64(storeID)
		}
	}

	// case 1: put one element into history load
	details := summaryStoresLoadByEngine(storeInfos, storeLoads, storeHistoryLoad, nil, rw, kind, collector)
	re.Len(details, 2)
	re.Empty(details[0].LoadPred.Current.HistoryLoads)
	re.Empty(details[1].LoadPred.Current.HistoryLoads)
	expectHistoryLoads := []float64{1, 2, 5}
	for _, storeID := range []uint64{1, 3} {
		loads := storeHistoryLoad.Get(storeID, rw, kind)
		for i := range loads {
			for j := range loads[0] {
				if loads[i][j] != 0 {
					re.Equal(loads[i][j]/float64(storeID), expectHistoryLoads[i])
				}
			}
		}
	}

	// case 2: put many elements into history load
	storeHistoryLoad.sampleDuration = 0
	for i := 1; i < 10; i++ {
		details = summaryStoresLoadByEngine(storeInfos, storeLoads, storeHistoryLoad, nil, rw, kind, collector)
		expect := []float64{2, 4, 10}
		for _, detail := range details {
			loads := detail.LoadPred.Current.HistoryLoads
			storeID := detail.GetID()
			for i := range loads {
				for j := range loads[0] {
					if loads[i][j] != 0 {
						re.Equal(loads[i][j]/float64(storeID), expectHistoryLoads[i])
					}
				}
			}

			for i, loads := range detail.LoadPred.Expect.HistoryLoads {
				for _, load := range loads {
					if load != 0 {
						re.Equal(load, expect[i])
					}
				}
			}
		}
	}
}
