// Copyright 2019 TiKV Project Authors.
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
	"math"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/statistics/utils"
)

// StoreHotPeersInfos is used to get human-readable description for hot regions.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type StoreHotPeersInfos struct {
	AsPeer   StoreHotPeersStat `json:"as_peer"`
	AsLeader StoreHotPeersStat `json:"as_leader"`
}

// StoreHotPeersStat is used to record the hot region statistics group by store.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type StoreHotPeersStat map[uint64]*HotPeersStat

// CollectHotPeerInfos only returns TotalBytesRate,TotalKeysRate,TotalQueryRate,Count
func CollectHotPeerInfos(stores []*core.StoreInfo, regionStats map[uint64][]*HotPeerStat) *StoreHotPeersInfos {
	peerLoadSum := make([]float64, utils.DimLen)
	collect := func(kind constant.ResourceKind) StoreHotPeersStat {
		ret := make(StoreHotPeersStat, len(stores))
		for _, store := range stores {
			id := store.GetID()
			hotPeers, ok := regionStats[id]
			if !ok {
				continue
			}
			for i := range peerLoadSum {
				peerLoadSum[i] = 0
			}
			peers := filterHotPeers(kind, hotPeers)
			for _, peer := range peers {
				for j := range peerLoadSum {
					peerLoadSum[j] += peer.GetLoad(j)
				}
			}
			ret[id] = &HotPeersStat{
				TotalBytesRate: peerLoadSum[utils.ByteDim],
				TotalKeysRate:  peerLoadSum[utils.KeyDim],
				TotalQueryRate: peerLoadSum[utils.QueryDim],
				Count:          len(peers),
			}
		}
		return ret
	}
	return &StoreHotPeersInfos{
		AsPeer:   collect(constant.RegionKind),
		AsLeader: collect(constant.LeaderKind),
	}
}

// GetHotStatus returns the hot status for a given type.
// NOTE: This function is exported by HTTP API. It does not contain `isLearner` and `LastUpdateTime` field. If need, please call `updateRegionInfo`.
func GetHotStatus(stores []*core.StoreInfo, storesLoads map[uint64][]float64, regionStats map[uint64][]*HotPeerStat, typ utils.RWType, isTraceRegionFlow bool) *StoreHotPeersInfos {
	stInfos := SummaryStoreInfos(stores)
	stLoadInfosAsLeader := SummaryStoresLoad(
		stInfos,
		storesLoads,
		nil,
		regionStats,
		isTraceRegionFlow,
		typ, constant.LeaderKind)
	stLoadInfosAsPeer := SummaryStoresLoad(
		stInfos,
		storesLoads,
		nil,
		regionStats,
		isTraceRegionFlow,
		typ, constant.RegionKind)

	asLeader := make(StoreHotPeersStat, len(stLoadInfosAsLeader))
	asPeer := make(StoreHotPeersStat, len(stLoadInfosAsPeer))

	for id, detail := range stLoadInfosAsLeader {
		asLeader[id] = detail.ToHotPeersStat()
	}
	for id, detail := range stLoadInfosAsPeer {
		asPeer[id] = detail.ToHotPeersStat()
	}
	return &StoreHotPeersInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
}

// SummaryStoresLoad Load information of all available stores.
// it will filter the hot peer and calculate the current and future stat(rate,count) for each store
func SummaryStoresLoad(
	storeInfos map[uint64]*StoreSummaryInfo,
	storesLoads map[uint64][]float64,
	storesHistoryLoads *StoreHistoryLoads,
	storeHotPeers map[uint64][]*HotPeerStat,
	isTraceRegionFlow bool,
	rwTy utils.RWType,
	kind constant.ResourceKind,
) map[uint64]*StoreLoadDetail {
	// loadDetail stores the storeID -> hotPeers stat and its current and future stat(rate,count)
	loadDetail := make(map[uint64]*StoreLoadDetail, len(storesLoads))

	tikvLoadDetail := summaryStoresLoadByEngine(
		storeInfos,
		storesLoads,
		storesHistoryLoads,
		storeHotPeers,
		rwTy, kind,
		newTikvCollector(),
	)
	tiflashLoadDetail := summaryStoresLoadByEngine(
		storeInfos,
		storesLoads,
		storesHistoryLoads,
		storeHotPeers,
		rwTy, kind,
		newTiFlashCollector(isTraceRegionFlow),
	)

	for _, detail := range append(tikvLoadDetail, tiflashLoadDetail...) {
		loadDetail[detail.GetID()] = detail
	}
	return loadDetail
}

func summaryStoresLoadByEngine(
	storeInfos map[uint64]*StoreSummaryInfo,
	storesLoads map[uint64][]float64,
	storesHistoryLoads *StoreHistoryLoads,
	storeHotPeers map[uint64][]*HotPeerStat,
	rwTy utils.RWType,
	kind constant.ResourceKind,
	collector storeCollector,
) []*StoreLoadDetail {
	loadDetail := make([]*StoreLoadDetail, 0, len(storeInfos))
	allStoreLoadSum := make([]float64, utils.DimLen)
	allStoreHistoryLoadSum := make([][]float64, utils.DimLen) // row: dim, column: time
	allStoreCount := 0
	allHotPeersCount := 0

	for _, info := range storeInfos {
		store := info.StoreInfo
		id := store.GetID()
		storeLoads, ok := storesLoads[id]
		if !ok || !collector.filter(info, kind) {
			continue
		}

		// Find all hot peers first
		var hotPeers []*HotPeerStat
		peerLoadSum := make([]float64, utils.DimLen)
		// For hot leaders, we need to calculate the sum of the leader's write and read flow rather than the all peers.
		for _, peer := range filterHotPeers(kind, storeHotPeers[id]) {
			for i := range peerLoadSum {
				peerLoadSum[i] += peer.GetLoad(i)
			}
			hotPeers = append(hotPeers, peer.Clone())
		}
		currentLoads := collector.getLoads(storeLoads, peerLoadSum, rwTy, kind)

		var historyLoads [][]float64
		if storesHistoryLoads != nil {
			for i, historyLoads := range storesHistoryLoads.Get(id, rwTy, kind) {
				if allStoreHistoryLoadSum[i] == nil || len(allStoreHistoryLoadSum[i]) < len(historyLoads) {
					allStoreHistoryLoadSum[i] = make([]float64, len(historyLoads))
				}
				for j, historyLoad := range historyLoads {
					allStoreHistoryLoadSum[i][j] += historyLoad
				}
			}
			storesHistoryLoads.Add(id, rwTy, kind, currentLoads)
		}

		for i := range allStoreLoadSum {
			allStoreLoadSum[i] += currentLoads[i]
		}
		allStoreCount += 1
		allHotPeersCount += len(hotPeers)

		// Build store load prediction from current load and pending influence.
		stLoadPred := (&StoreLoad{
			Loads:        currentLoads,
			Count:        float64(len(hotPeers)),
			HistoryLoads: historyLoads,
		}).ToLoadPred(rwTy, info.PendingSum)

		// Construct store load info.
		loadDetail = append(loadDetail, &StoreLoadDetail{
			StoreSummaryInfo: info,
			LoadPred:         stLoadPred,
			HotPeers:         hotPeers,
		})
	}

	if allStoreCount == 0 {
		return loadDetail
	}

	expectCount := float64(allHotPeersCount) / float64(allStoreCount)
	expectLoads := make([]float64, len(allStoreLoadSum))
	for i := range expectLoads {
		expectLoads[i] = allStoreLoadSum[i] / float64(allStoreCount)
	}

	// TODO: remove some the max value or min value to avoid the effect of extreme value.
	expectHistoryLoads := make([][]float64, utils.DimLen) // row: dim, column: time
	for i := range allStoreHistoryLoadSum {
		expectHistoryLoads[i] = make([]float64, len(allStoreHistoryLoadSum[i]))
		for j := range allStoreHistoryLoadSum[i] {
			expectHistoryLoads[i][j] = allStoreHistoryLoadSum[i][j] / float64(allStoreCount)
		}
	}
	stddevLoads := make([]float64, len(allStoreLoadSum))
	if allHotPeersCount != 0 {
		for _, detail := range loadDetail {
			for i := range expectLoads {
				stddevLoads[i] += math.Pow(detail.LoadPred.Current.Loads[i]-expectLoads[i], 2)
			}
		}
		for i := range stddevLoads {
			stddevLoads[i] = math.Sqrt(stddevLoads[i]/float64(allStoreCount)) / expectLoads[i]
		}
	}

	{
		// Metric for debug.
		engine := collector.engine()
		ty := "exp-byte-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectLoads[utils.ByteDim])
		ty = "exp-key-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectLoads[utils.KeyDim])
		ty = "exp-query-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectLoads[utils.QueryDim])
		ty = "exp-count-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectCount)
		ty = "stddev-byte-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(stddevLoads[utils.ByteDim])
		ty = "stddev-key-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(stddevLoads[utils.KeyDim])
		ty = "stddev-query-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(stddevLoads[utils.QueryDim])
	}
	expect := StoreLoad{
		Loads:        expectLoads,
		Count:        expectCount,
		HistoryLoads: expectHistoryLoads,
	}
	stddev := StoreLoad{
		Loads: stddevLoads,
		Count: expectCount,
	}
	for _, detail := range loadDetail {
		detail.LoadPred.Expect = expect
		detail.LoadPred.Stddev = stddev
	}
	return loadDetail
}

// filterHotPeers filters hot peers according to kind.
// If kind is RegionKind, all hot peers will be returned.
// If kind is LeaderKind, only leader hot peers will be returned.
func filterHotPeers(kind constant.ResourceKind, peers []*HotPeerStat) []*HotPeerStat {
	ret := make([]*HotPeerStat, 0, len(peers))
	for _, peer := range peers {
		switch kind {
		case constant.RegionKind:
			ret = append(ret, peer)
		case constant.LeaderKind:
			if peer.IsLeader() {
				ret = append(ret, peer)
			}
		}
	}
	return ret
}
