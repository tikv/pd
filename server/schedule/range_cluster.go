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

package schedule

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
)

// RangeCluster isolates the cluster by range.
type RangeCluster struct {
	opt.Cluster
	subCluster        *core.BasicCluster // Collect all regions belong to the range.
	tolerantSizeRatio float64
}

// GenRangeCluster gets a range cluster by specifying start key and end key.
// The cluster can only know the regions within [startKey, endKey].
func GenRangeCluster(cluster opt.Cluster, startKey, endKey []byte) *RangeCluster {
	subCluster := core.NewBasicCluster()
	for _, r := range cluster.ScanRegions(startKey, endKey, -1) {
		subCluster.Regions.SetRegion(r)
	}
	return &RangeCluster{
		Cluster:    cluster,
		subCluster: subCluster,
	}
}

func (r *RangeCluster) updateStoreInfo(s *core.StoreInfo) *core.StoreInfo {
	id := s.GetID()

	used := float64(s.GetUsedSize()) / (1 << 20)
	if used == 0 {
		return s
	}
	amplification := float64(s.GetRegionSize()) / used
	leaderCount := r.subCluster.GetStoreLeaderCount(id)
	leaderSize := r.subCluster.GetStoreLeaderRegionSize(id)
	regionCount := r.subCluster.GetStoreRegionCount(id)
	regionSize := r.subCluster.GetStoreRegionSize(id)
	pendingPeerCount := r.subCluster.GetStorePendingPeerCount(id)
	newStats := proto.Clone(s.GetStoreStats()).(*pdpb.StoreStats)
	newStats.UsedSize = uint64(float64(regionSize)/amplification) * (1 << 20)
	newStats.Available = s.GetCapacity() - newStats.UsedSize
	newStore := s.Clone(
		core.SetNewStoreStats(newStats), // it means to use instant value directly
		core.SetLeaderCount(leaderCount),
		core.SetRegionCount(regionCount),
		core.SetPendingPeerCount(pendingPeerCount),
		core.SetLeaderSize(leaderSize),
		core.SetRegionSize(regionSize),
	)
	return newStore
}

// GetStore searches for a store by ID.
func (r *RangeCluster) GetStore(id uint64) *core.StoreInfo {
	s := r.Cluster.GetStore(id)
	if s == nil {
		return nil
	}
	return r.updateStoreInfo(s)
}

// GetStores returns all Stores in the cluster.
func (r *RangeCluster) GetStores() []*core.StoreInfo {
	stores := r.Cluster.GetStores()
	newStores := make([]*core.StoreInfo, 0, len(stores))
	for _, s := range stores {
		newStores = append(newStores, r.updateStoreInfo(s))
	}
	return newStores
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
	return r.Cluster.GetOpts().GetTolerantSizeRatio()
}

// RandFollowerRegion returns a random region that has a follower on the store.
func (r *RangeCluster) RandFollowerRegion(storeID uint64, ranges []core.KeyRange, opts ...core.RegionOption) *core.RegionInfo {
	return r.subCluster.RandFollowerRegion(storeID, ranges, opts...)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (r *RangeCluster) RandLeaderRegion(storeID uint64, ranges []core.KeyRange, opts ...core.RegionOption) *core.RegionInfo {
	return r.subCluster.RandLeaderRegion(storeID, ranges, opts...)
}

// GetAverageRegionSize returns the average region approximate size.
func (r *RangeCluster) GetAverageRegionSize() int64 {
	return r.subCluster.GetAverageRegionSize()
}

// GetRegionStores returns all stores that contains the region's peer.
func (r *RangeCluster) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	stores := r.Cluster.GetRegionStores(region)
	newStores := make([]*core.StoreInfo, 0, len(stores))
	for _, s := range stores {
		newStores = append(newStores, r.updateStoreInfo(s))
	}
	return newStores
}

// GetFollowerStores returns all stores that contains the region's follower peer.
func (r *RangeCluster) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	stores := r.Cluster.GetFollowerStores(region)
	newStores := make([]*core.StoreInfo, 0, len(stores))
	for _, s := range stores {
		newStores = append(newStores, r.updateStoreInfo(s))
	}
	return newStores
}

// GetLeaderStore returns all stores that contains the region's leader peer.
func (r *RangeCluster) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	s := r.Cluster.GetLeaderStore(region)
	if s != nil {
		return r.updateStoreInfo(s)
	}
	return s
}
