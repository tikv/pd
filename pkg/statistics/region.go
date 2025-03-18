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
	"github.com/pingcap/kvproto/pkg/metapb"

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
	StorePeerKeys        map[uint64]int64  `json:"store_peer_keys,omitempty"`
	StoreWriteBytes      map[uint64]uint64 `json:"store_write_bytes,omitempty"`
	StoreWriteKeys       map[uint64]uint64 `json:"store_write_keys,omitempty"`
	StoreWriteQuery      map[uint64]uint64 `json:"store_write_query,omitempty"`
	StoreLeaderReadBytes map[uint64]uint64 `json:"store_leader_read_bytes,omitempty"`
	StoreLeaderReadKeys  map[uint64]uint64 `json:"store_leader_read_keys,omitempty"`
	StoreLeaderReadQuery map[uint64]uint64 `json:"store_leader_read_query,omitempty"`
	StorePeerReadBytes   map[uint64]uint64 `json:"store_peer_read_bytes,omitempty"`
	StorePeerReadKeys    map[uint64]uint64 `json:"store_peer_read_keys,omitempty"`
	StorePeerReadQuery   map[uint64]uint64 `json:"store_peer_read_query,omitempty"`
	StoreEngine          map[uint64]string `json:"store_engine,omitempty"`
}

// GetRegionStatsOption is used to filter the peer statistics.
type GetRegionStatsOption func(peer *metapb.Peer) bool

// WithStoreMapOption returns a GetRegionStatsOption that filters the peer by store map.
func WithStoreMapOption(storeMap map[uint64]string) GetRegionStatsOption {
	return func(peer *metapb.Peer) bool {
		_, exist := storeMap[peer.GetStoreId()]
		return exist
	}
}

// GetRegionStats sums regions' statistics.
func GetRegionStats(regions []*core.RegionInfo, cluster RegionStatInformer, opts ...GetRegionStatsOption) *RegionStats {
	stats := newRegionStats()
	for _, region := range regions {
		stats.Observe(region, cluster, opts...)
	}
	return stats
}

func newRegionStats() *RegionStats {
	return &RegionStats{
		StoreLeaderCount:     make(map[uint64]int),
		StorePeerCount:       make(map[uint64]int),
		StoreLeaderSize:      make(map[uint64]int64),
		StoreLeaderKeys:      make(map[uint64]int64),
		StorePeerSize:        make(map[uint64]int64),
		StorePeerKeys:        make(map[uint64]int64),
		StoreWriteBytes:      make(map[uint64]uint64),
		StoreWriteKeys:       make(map[uint64]uint64),
		StoreWriteQuery:      make(map[uint64]uint64),
		StoreLeaderReadBytes: make(map[uint64]uint64),
		StoreLeaderReadKeys:  make(map[uint64]uint64),
		StoreLeaderReadQuery: make(map[uint64]uint64),
		StorePeerReadBytes:   make(map[uint64]uint64),
		StorePeerReadKeys:    make(map[uint64]uint64),
		StorePeerReadQuery:   make(map[uint64]uint64),
		StoreEngine:          make(map[uint64]string),
	}
}

// Observe adds a region's statistics into RegionStats.
func (s *RegionStats) Observe(r *core.RegionInfo, cluster RegionStatInformer, opts ...GetRegionStatsOption) {
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
	checkFn := func(p *metapb.Peer) bool {
		for _, opt := range opts {
			if !opt(p) {
				return false
			}
		}
		return true
	}
	if leader != nil && checkFn(leader) {
		storeID := leader.GetStoreId()
		s.StoreLeaderCount[storeID]++
		s.StoreLeaderSize[storeID] += approximateSize
		s.StoreLeaderKeys[storeID] += approximateKeys
		if cluster != nil {
			{
				stat := cluster.GetHotPeerStat(utils.Read, r.GetID(), storeID)
				if stat != nil {
					bytes := stat.GetLoad(utils.ByteDim)
					s.StoreLeaderReadBytes[storeID] += uint64(bytes)
					keys := stat.GetLoad(utils.KeyDim)
					s.StoreLeaderReadKeys[storeID] += uint64(keys)
					qps := stat.GetLoad(utils.QueryDim)
					s.StoreLeaderReadQuery[storeID] += uint64(qps)
				}
			}
		}
	}
	peers := r.GetMeta().GetPeers()
	for _, p := range peers {
		if !checkFn(p) {
			continue
		}
		storeID := p.GetStoreId()
		s.StorePeerCount[storeID]++
		s.StorePeerSize[storeID] += r.GetStorePeerApproximateSize(storeID)
		s.StorePeerKeys[storeID] += r.GetStorePeerApproximateKeys(storeID)
		if cluster != nil {
			s.StoreWriteKeys[storeID] += r.GetKeysWritten()
			s.StoreWriteBytes[storeID] += r.GetBytesWritten()
			// peer read statistics
			{
				stat := cluster.GetHotPeerStat(utils.Read, r.GetID(), p.GetStoreId())
				if stat != nil {
					bytes := stat.GetLoad(utils.ByteDim)
					s.StorePeerReadBytes[storeID] += uint64(bytes)
					keys := stat.GetLoad(utils.KeyDim)
					s.StorePeerReadKeys[storeID] += uint64(keys)
					qps := stat.GetLoad(utils.QueryDim)
					s.StorePeerReadQuery[storeID] += uint64(qps)
				}
			}
			// peer write statistics
			{
				stat := cluster.GetHotPeerStat(utils.Write, r.GetID(), p.GetStoreId())
				if stat != nil {
					bytes := stat.GetLoad(utils.ByteDim)
					s.StoreWriteBytes[storeID] += uint64(bytes)
					keys := stat.GetLoad(utils.KeyDim)
					s.StoreWriteKeys[storeID] += uint64(keys)
					qps := stat.GetLoad(utils.QueryDim)
					s.StoreWriteQuery[storeID] += uint64(qps)
				}
			}
		}
	}
}
