// Copyright 2019 PingCAP, Inc.
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

package statistics

import (
	"time"

	"github.com/pingcap/pd/server/core"
)

// HotSpotPeerStat records each hot region's statistics
type HotSpotPeerStat struct {
	RegionID  uint64 `json:"region_id"`
	FlowBytes uint64 `json:"flow_bytes"`
	FlowKeys  uint64 `json:"flow_keys"`
	// HotDegree records the hot region update times
	HotDegree int `json:"hot_degree"`
	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`
	// StoreID is the store id of the region peer
	StoreID uint64   `json:"store_id"`
	Kind    FlowKind `json:"kind"`
	// AntiCount used to eliminate some noise when remove region in cache
	AntiCount int
	// Version used to check the region split times
	Version uint64
	// Stats is a rolling statistics, recording some recently added records.
	Stats *RollingStats

	needDelete bool
	isLeader   bool
	isNew      bool
}

// IsNeedDelete to delete the item in cache.
func (stat HotSpotPeerStat) IsNeedDelete() bool {
	return stat.needDelete
}

// IsLeader indicaes the item belong to the leader.
func (stat HotSpotPeerStat) IsLeader() bool {
	return stat.isLeader
}

// IsNew indicaes the item is first update in the cache of the region.
func (stat HotSpotPeerStat) IsNew() bool {
	return stat.isNew
}

// RegionsStat is a list of a group region state type
type RegionsStat []HotSpotPeerStat

func (m RegionsStat) Len() int           { return len(m) }
func (m RegionsStat) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m RegionsStat) Less(i, j int) bool { return m[i].FlowBytes < m[j].FlowBytes }

// HotRegionsStat records all hot regions statistics
type HotRegionsStat struct {
	TotalFlowBytes uint64      `json:"total_flow_bytes"`
	RegionsCount   int         `json:"regions_count"`
	RegionsStat    RegionsStat `json:"statistics"`
}

// RegionStats records a list of regions' statistics and distribution status.
type RegionStats struct {
	Count            int              `json:"count"`
	EmptyCount       int              `json:"empty_count"`
	StorageSize      int64            `json:"storage_size"`
	StorageKeys      int64            `json:"storage_keys"`
	StoreLeaderCount map[uint64]int   `json:"store_leader_count"`
	StorePeerCount   map[uint64]int   `json:"store_peer_count"`
	StoreLeaderSize  map[uint64]int64 `json:"store_leader_size"`
	StoreLeaderKeys  map[uint64]int64 `json:"store_leader_keys"`
	StorePeerSize    map[uint64]int64 `json:"store_peer_size"`
	StorePeerKeys    map[uint64]int64 `json:"store_peer_keys"`
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
func (s *RegionStats) Observe(r *core.RegionInfo) {
	s.Count++
	approximateKeys := r.GetApproximateKeys()
	approximateSize := r.GetApproximateSize()
	if approximateSize <= core.EmptyRegionApproximateSize {
		s.EmptyCount++
	}
	s.StorageSize += approximateSize
	s.StorageKeys += approximateKeys
	leader := r.GetLeader()
	if leader != nil {
		storeID := leader.GetStoreId()
		s.StoreLeaderCount[storeID]++
		s.StoreLeaderSize[storeID] += approximateSize
		s.StoreLeaderKeys[storeID] += approximateKeys
	}
	peers := r.GetMeta().GetPeers()
	for _, p := range peers {
		storeID := p.GetStoreId()
		s.StorePeerCount[storeID]++
		s.StorePeerSize[storeID] += approximateSize
		s.StorePeerKeys[storeID] += approximateKeys
	}
}

// GetRegionStats sums regions' statistics.
func GetRegionStats(regions []*core.RegionInfo) *RegionStats {
	stats := newRegionStats()
	for _, region := range regions {
		stats.Observe(region)
	}
	return stats
}
