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
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
)

// RegionStats records a list of regions' statistics and distribution status.
type RegionStats struct {
	Count                int               `json:"count"`
	EmptyCount           int               `json:"empty_count"`
	StorageSize          int64             `json:"storage_size"`
	UserStorageSize      int64             `json:"user_storage_size"`
	StorageKeys          int64             `json:"storage_keys"`
	StoreLeaderCount     map[uint64]int    `json:"store_leader_count"`
	StorePeerCount       map[uint64]int    `json:"store_peer_count"`
	StoreLeaderSize      map[uint64]int64  `json:"store_leader_size"`
	StoreLeaderKeys      map[uint64]int64  `json:"store_leader_keys"`
	StorePeerSize        map[uint64]int64  `json:"store_peer_size"`
	StorePeerKeys        map[uint64]int64  `json:"store_peer_keys"`
	StoreWriteBytes      map[uint64]uint64 `json:"store_write_bytes"`
	StoreWriteKeys       map[uint64]uint64 `json:"store_write_keys"`
	StoreLeaderReadBytes map[uint64]uint64 `json:"store_leader_read_bytes"`
	StoreLeaderReadKeys  map[uint64]uint64 `json:"store_leader_read_keys"`
	StorePeerReadBytes   map[uint64]uint64 `json:"store_peer_read_bytes"`
	StorePeerReadKeys    map[uint64]uint64 `json:"store_peer_read_keys"`
	StorePeerReadQuery   map[uint64]uint64 `json:"store_peer_read_query"`
}

// GetRegionStats sums regions' statistics.
func GetRegionStats(regions []*core.RegionInfo, cluster RegionStatInformer) *RegionStats {
	stats := newRegionStats()
	for _, region := range regions {
		stats.Observe(region, cluster)
	}
	return stats
}

func newRegionStats() *RegionStats {
	return &RegionStats{
		StoreLeaderCount: make(map[uint64]int),
		StorePeerCount:   make(map[uint64]int),
		StoreLeaderSize:  make(map[uint64]int64),
		StoreLeaderKeys:  make(map[uint64]int64),
		StorePeerSize:    make(map[uint64]int64),
		StorePeerKeys:    make(map[uint64]int64),
	}
}

// Observe adds a region's statistics into RegionStats.
func (s *RegionStats) Observe(r *core.RegionInfo, cluster RegionStatInformer) {
	s.Count++
	approximateKeys := r.GetApproximateKeys()
	approximateSize := r.GetApproximateSize()
	approximateKvSize := r.GetApproximateKvSize()
	if approximateSize <= core.EmptyRegionApproximateSize {
		s.EmptyCount++
	}
	s.StorageSize += approximateSize
	s.UserStorageSize += approximateKvSize
	s.StorageKeys += approximateKeys
	leader := r.GetLeader()
	if leader != nil {
		storeID := leader.GetStoreId()
		s.StoreLeaderCount[storeID]++
		s.StoreLeaderSize[storeID] += approximateSize
		s.StoreLeaderKeys[storeID] += approximateKeys
		s.StoreLeaderReadBytes[storeID] += r.GetBytesRead()
		s.StoreLeaderReadKeys[storeID] += r.GetKeysRead()
	}
	peers := r.GetMeta().GetPeers()
	for _, p := range peers {
		storeID := p.GetStoreId()
		s.StorePeerCount[storeID]++
		s.StorePeerSize[storeID] += r.GetStorePeerApproximateSize(storeID)
		s.StorePeerKeys[storeID] += r.GetStorePeerApproximateKeys(storeID)
		s.StoreWriteKeys[storeID] += r.GetKeysWritten()
		s.StoreWriteBytes[storeID] += r.GetBytesWritten()
		if cluster != nil {
			stat := cluster.GetHotPeerStat(utils.Read, r.GetID(), p.GetStoreId())
			bytes := stat.GetLoad(utils.ByteDim)
			s.StorePeerReadBytes[storeID] = uint64(bytes)
			keys := stat.GetLoad(utils.KeyDim)
			s.StorePeerReadKeys[storeID] = uint64(keys)
			qps := stat.GetLoad(utils.QueryDim)
			s.StorePeerReadQuery[storeID] = uint64(qps)
		}
	}
}
