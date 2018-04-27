// Copyright 2017 PingCAP, Inc.
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

package schedule

import (
	"bytes"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
)

var (
	// HotRegionLowThreshold is the low threadshold of hot region
	HotRegionLowThreshold = 3
)

const (
	// RegionHeartBeatReportInterval is the heartbeat report interval of a region
	RegionHeartBeatReportInterval = 60

	statCacheMaxLen              = 1000
	hotWriteRegionMinFlowRate    = 16 * 1024
	hotReadRegionMinFlowRate     = 128 * 1024
	storeHeartBeatReportInterval = 10
	minHotRegionReportInterval   = 3
	hotRegionAntiCount           = 1
)

// BasicCluster provides basic data member and interface for a tikv cluster.
type BasicCluster struct {
	Stores   *core.StoresInfo
	Regions  *core.RegionsInfo
	HotCache *HotSpotCache
}

// NewOpInfluence creates a OpInfluence.
func NewOpInfluence(operators []*Operator, cluster Cluster) OpInfluence {
	m := make(map[uint64]*StoreInfluence)

	for _, op := range operators {
		if !op.IsTimeout() && !op.IsFinish() {
			region := cluster.GetRegion(op.RegionID())
			if region != nil {
				op.Influence(m, region)
			}
		}
	}

	return m
}

// OpInfluence is a map of StoreInfluence.
type OpInfluence map[uint64]*StoreInfluence

// GetStoreInfluence get storeInfluence of specific store.
func (m OpInfluence) GetStoreInfluence(id uint64) *StoreInfluence {
	storeInfluence, ok := m[id]
	if !ok {
		storeInfluence = &StoreInfluence{}
		m[id] = storeInfluence
	}
	return storeInfluence
}

// StoreInfluence records influences that pending operators will make.
type StoreInfluence struct {
	RegionSize  int64
	RegionCount int64
	LeaderSize  int64
	LeaderCount int64
}

// ResourceSize returns delta size of leader/region by influence.
func (s StoreInfluence) ResourceSize(kind core.ResourceKind) int64 {
	switch kind {
	case core.LeaderKind:
		return s.LeaderSize
	case core.RegionKind:
		return s.RegionSize
	default:
		return 0
	}
}

// NewBasicCluster creates a BasicCluster.
func NewBasicCluster() *BasicCluster {
	return &BasicCluster{
		Stores:   core.NewStoresInfo(),
		Regions:  core.NewRegionsInfo(),
		HotCache: newHotSpotCache(),
	}
}

// GetStores returns all Stores in the cluster.
func (bc *BasicCluster) GetStores() []*core.StoreInfo {
	return bc.Stores.GetStores()
}

// GetStore searches for a store by ID.
func (bc *BasicCluster) GetStore(storeID uint64) *core.StoreInfo {
	return bc.Stores.GetStore(storeID)
}

// GetRegion searches for a region by ID.
func (bc *BasicCluster) GetRegion(regionID uint64) *core.RegionInfo {
	return bc.Regions.GetRegion(regionID)
}

// GetRegionStores returns all Stores that contains the region's peer.
func (bc *BasicCluster) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	var Stores []*core.StoreInfo
	for id := range region.GetStoreIds() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetFollowerStores returns all Stores that contains the region's follower peer.
func (bc *BasicCluster) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	var Stores []*core.StoreInfo
	for id := range region.GetFollowers() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetLeaderStore returns all Stores that contains the region's leader peer.
func (bc *BasicCluster) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	return bc.Stores.GetStore(region.Leader.GetStoreId())
}

// GetAdjacentRegions returns region's info that is adjacent with specific region
func (bc *BasicCluster) GetAdjacentRegions(region *core.RegionInfo) (*core.RegionInfo, *core.RegionInfo) {
	return bc.Regions.GetAdjacentRegions(region)
}

// BlockStore stops balancer from selecting the store.
func (bc *BasicCluster) BlockStore(storeID uint64) error {
	return errors.Trace(bc.Stores.BlockStore(storeID))
}

// UnblockStore allows balancer to select the store.
func (bc *BasicCluster) UnblockStore(storeID uint64) {
	bc.Stores.UnblockStore(storeID)
}

// RandFollowerRegion returns a random region that has a follower on the store.
func (bc *BasicCluster) RandFollowerRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	return bc.Regions.RandFollowerRegion(storeID, opts...)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (bc *BasicCluster) RandLeaderRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	return bc.Regions.RandLeaderRegion(storeID, opts...)
}

// IsRegionHot checks if a region is in hot state.
func (bc *BasicCluster) IsRegionHot(id uint64, hotThreshold int) bool {
	return bc.HotCache.isRegionHot(id, hotThreshold)
}

// RegionWriteStats returns hot region's write stats.
func (bc *BasicCluster) RegionWriteStats() []*core.RegionStat {
	return bc.HotCache.RegionStats(WriteFlow)
}

// RegionReadStats returns hot region's read stats.
func (bc *BasicCluster) RegionReadStats() []*core.RegionStat {
	return bc.HotCache.RegionStats(ReadFlow)
}

// PutStore put a store
func (bc *BasicCluster) PutStore(store *core.StoreInfo) error {
	bc.Stores.SetStore(store)
	return nil
}

// PutRegion put a region
func (bc *BasicCluster) PutRegion(region *core.RegionInfo) error {
	bc.Regions.SetRegion(region)
	return nil
}

// CheckWriteStatus checks the write status, returns whether need update statistics and item.
func (bc *BasicCluster) CheckWriteStatus(region *core.RegionInfo) (bool, *core.RegionStat) {
	return bc.HotCache.CheckWrite(region, bc.Stores)
}

// CheckReadStatus checks the read status, returns whether need update statistics and item.
func (bc *BasicCluster) CheckReadStatus(region *core.RegionInfo) (bool, *core.RegionStat) {
	return bc.HotCache.CheckRead(region, bc.Stores)
}

// RangeCluster isolates the cluster by range.
type RangeCluster struct {
	Cluster
	regions           *core.RegionsInfo
	tolerantSizeRatio float64
}

const scanLimit = 128

// GenRangeCluster gets a range cluster by specifying start key and end key.
func GenRangeCluster(cluster Cluster, startKey, endKey []byte) *RangeCluster {
	regions := core.NewRegionsInfo()
	scanKey := startKey
	loopEnd := false
	for !loopEnd {
		collect := cluster.ScanRegions(scanKey, scanLimit)
		if len(collect) == 0 {
			break
		}
		for _, r := range collect {
			if bytes.Compare(r.StartKey, endKey) < 0 {
				regions.SetRegion(r)
			} else {
				loopEnd = true
				break
			}
			if string(r.EndKey) == "" {
				loopEnd = true
				break
			}
			scanKey = r.EndKey
		}
	}
	return &RangeCluster{
		Cluster: cluster,
		regions: regions,
	}
}

func (r *RangeCluster) updateStoreInfo(s *core.StoreInfo) {
	id := s.GetId()
	s.LeaderCount = r.regions.GetStoreLeaderCount(id)
	s.LeaderSize = r.regions.GetStoreLeaderRegionSize(id)
	s.RegionCount = r.regions.GetStoreRegionCount(id)
	s.RegionSize = r.regions.GetStoreRegionSize(id)
	s.PendingPeerCount = r.regions.GetStorePendingPeerCount(id)
}

// GetStore searches for a store by ID.
func (r *RangeCluster) GetStore(id uint64) *core.StoreInfo {
	s := r.Cluster.GetStore(id)
	r.updateStoreInfo(s)
	return s
}

// GetStores returns all Stores in the cluster.
func (r *RangeCluster) GetStores() []*core.StoreInfo {
	stores := r.Cluster.GetStores()
	for _, s := range stores {
		r.updateStoreInfo(s)
	}
	return stores
}

// SetTolerantSizeRatio sets the tolerant size ratio.
func (r *RangeCluster) SetTolerantSizeRatio(ratio float64) {
	r.tolerantSizeRatio = ratio
}

// GetTolerantSizeRatio gets the tolerant size ratio.
func (r *RangeCluster) GetTolerantSizeRatio() float64 {
	if r.tolerantSizeRatio != 0 {
		return r.tolerantSizeRatio
	}
	return r.Cluster.GetTolerantSizeRatio()
}
