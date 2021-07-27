// Copyright 2021 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/statistics"
)

// GetReadStoresLoad returns read store load
func GetReadStoresLoad(cluster opt.Cluster) *statistics.StoreHotPeersInfos {
	followStoresDetails := getStoresLoad(cluster, read, core.RegionKind)
	leaderStoresDetails := getStoresLoad(cluster, read, core.LeaderKind)
	asLeader := make(statistics.StoreHotPeersStat, len(followStoresDetails))
	asPeer := make(statistics.StoreHotPeersStat, len(leaderStoresDetails))
	for id, detail := range leaderStoresDetails {
		asLeader[id] = detail.toHotPeersStat()
	}
	for id, detail := range followStoresDetails {
		asPeer[id] = detail.toHotPeersStat()
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
}

// GetWriteStoresLoad returns write store load
func GetWriteStoresLoad(cluster opt.Cluster) *statistics.StoreHotPeersInfos {
	followStoresDetails := getStoresLoad(cluster, write, core.RegionKind)
	leaderStoresDetails := getStoresLoad(cluster, write, core.LeaderKind)
	asLeader := make(statistics.StoreHotPeersStat, len(followStoresDetails))
	asPeer := make(statistics.StoreHotPeersStat, len(leaderStoresDetails))
	for id, detail := range leaderStoresDetails {
		asLeader[id] = detail.toHotPeersStat()
	}
	for id, detail := range followStoresDetails {
		asPeer[id] = detail.toHotPeersStat()
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
}

// getStoresLoad returns hot region store details
func getStoresLoad(cluster opt.Cluster, rwTy rwType, kind core.ResourceKind) map[uint64]*storeLoadDetail {
	stores := cluster.GetStores()
	storesLoads := cluster.GetStoresLoads()
	regionRead := cluster.RegionReadStats()
	return summaryStoresLoad(stores, storesLoads, nil, regionRead, rwTy, kind)
}
