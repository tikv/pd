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
	"fmt"

	"github.com/tikv/pd/server/core"
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

// GetHotStatus returns the hot status for a given type.
func GetHotStatus(stores []*core.StoreInfo, storesLoads map[uint64][]float64, regionStats map[uint64][]*HotPeerStat, typ RWType, isTraceRegionFlow bool) *StoreHotPeersInfos {
	stInfos := SummaryStoreInfos(stores)
	var stLoadInfosAsLeader map[uint64]*StoreLoadDetail
	var stLoadInfosAsPeer map[uint64]*StoreLoadDetail
	switch typ {
	case Read:
		stLoadInfosAsLeader = SummaryStoresLoad(
			stInfos,
			storesLoads,
			regionStats,
			isTraceRegionFlow,
			Read, core.LeaderKind)
		stLoadInfosAsPeer = SummaryStoresLoad(
			stInfos,
			storesLoads,
			regionStats,
			isTraceRegionFlow,
			Read, core.RegionKind)
	case Write:
		stLoadInfosAsLeader = SummaryStoresLoad(
			stInfos,
			storesLoads,
			regionStats,
			isTraceRegionFlow,
			Write, core.LeaderKind)
		stLoadInfosAsPeer = SummaryStoresLoad(
			stInfos,
			storesLoads,
			regionStats,
			isTraceRegionFlow,
			Write, core.RegionKind)
	}

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
	storeHotPeers map[uint64][]*HotPeerStat,
	isTraceRegionFlow bool,
	rwTy RWType,
	kind core.ResourceKind,
) map[uint64]*StoreLoadDetail {
	// loadDetail stores the storeID -> hotPeers stat and its current and future stat(rate,count)
	loadDetail := make(map[uint64]*StoreLoadDetail, len(storesLoads))

	tikvLoadDetail := summaryStoresLoadByEngine(
		storeInfos,
		storesLoads,
		storeHotPeers,
		rwTy, kind,
		newTikvCollector(),
	)
	tiflashLoadDetail := summaryStoresLoadByEngine(
		storeInfos,
		storesLoads,
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
	storeHotPeers map[uint64][]*HotPeerStat,
	rwTy RWType,
	kind core.ResourceKind,
	collector storeCollector,
) []*StoreLoadDetail {
	loadDetail := make([]*StoreLoadDetail, 0, len(storeInfos))
	allStoreLoadSum := make([]float64, DimLen)
	allStoreCount := 0
	allHotPeersCount := 0

	for _, info := range storeInfos {
		store := info.Store
		id := store.GetID()
		storeLoads, ok := storesLoads[id]
		if !ok || !collector.Filter(info, kind) {
			continue
		}

		// Find all hot peers first
		var hotPeers []*HotPeerStat
		peerLoadSum := make([]float64, DimLen)
		// TODO: To remove `filterHotPeers`, we need to:
		// HotLeaders consider `Write{Bytes,Keys}`, so when we schedule `writeLeader`, all peers are leader.
		for _, peer := range filterHotPeers(kind, storeHotPeers[id]) {
			for i := range peerLoadSum {
				peerLoadSum[i] += peer.GetLoad(GetRegionStatKind(rwTy, i))
			}
			hotPeers = append(hotPeers, peer.Clone())
		}
		{
			// Metric for debug.
			ty := "byte-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[ByteDim])
			ty = "key-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[KeyDim])
			ty = "query-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[QueryDim])
		}

		loads := collector.GetLoads(storeLoads, peerLoadSum, rwTy, kind)
		for i := range allStoreLoadSum {
			allStoreLoadSum[i] += loads[i]
		}
		allStoreCount += 1
		allHotPeersCount += len(hotPeers)

		// Build store load prediction from current load and pending influence.
		stLoadPred := (&StoreLoad{
			Loads: loads,
			Count: float64(len(hotPeers)),
		}).ToLoadPred(rwTy, info.PendingSum)

		// Construct store load info.
		loadDetail = append(loadDetail, &StoreLoadDetail{
			Info:     info,
			LoadPred: stLoadPred,
			HotPeers: hotPeers,
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
	{
		// Metric for debug.
		engine := collector.Engine()
		ty := "exp-byte-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectLoads[ByteDim])
		ty = "exp-key-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectLoads[KeyDim])
		ty = "exp-query-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectLoads[QueryDim])
		ty = "exp-count-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectCount)
	}
	expect := StoreLoad{
		Loads: expectLoads,
		Count: float64(allHotPeersCount) / float64(allStoreCount),
	}
	for _, detail := range loadDetail {
		detail.LoadPred.Expect = expect
	}
	return loadDetail
}

func filterHotPeers(kind core.ResourceKind, peers []*HotPeerStat) []*HotPeerStat {
	ret := make([]*HotPeerStat, 0, len(peers))
	for _, peer := range peers {
		if kind != core.LeaderKind || peer.IsLeader() {
			ret = append(ret, peer)
		}
	}
	return ret
}
