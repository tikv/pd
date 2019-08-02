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

package core

// BasicCluster provides basic data member and interface for a tikv cluster.
type BasicCluster struct {
	Stores  *StoresInfo
	Regions *RegionsInfo
}

// NewBasicCluster creates a BasicCluster.
func NewBasicCluster() *BasicCluster {
	return &BasicCluster{
		Stores:  NewStoresInfo(),
		Regions: NewRegionsInfo(),
	}
}

// GetStores returns all Stores in the cluster.
func (bc *BasicCluster) GetStores() []*StoreInfo {
	return bc.Stores.GetStores()
}

// GetStore searches for a store by ID.
func (bc *BasicCluster) GetStore(storeID uint64) *StoreInfo {
	return bc.Stores.GetStore(storeID)
}

// GetRegion searches for a region by ID.
func (bc *BasicCluster) GetRegion(regionID uint64) *RegionInfo {
	return bc.Regions.GetRegion(regionID)
}

// GetRegionStores returns all Stores that contains the region's peer.
func (bc *BasicCluster) GetRegionStores(region *RegionInfo) []*StoreInfo {
	var Stores []*StoreInfo
	for id := range region.GetStoreIds() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetFollowerStores returns all Stores that contains the region's follower peer.
func (bc *BasicCluster) GetFollowerStores(region *RegionInfo) []*StoreInfo {
	var Stores []*StoreInfo
	for id := range region.GetFollowers() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetLeaderStore returns all Stores that contains the region's leader peer.
func (bc *BasicCluster) GetLeaderStore(region *RegionInfo) *StoreInfo {
	return bc.Stores.GetStore(region.GetLeader().GetStoreId())
}

// GetAdjacentRegions returns region's info that is adjacent with specific region
func (bc *BasicCluster) GetAdjacentRegions(region *RegionInfo) (*RegionInfo, *RegionInfo) {
	return bc.Regions.GetAdjacentRegions(region)
}

// BlockStore stops balancer from selecting the store.
func (bc *BasicCluster) BlockStore(storeID uint64) error {
	return bc.Stores.BlockStore(storeID)
}

// UnblockStore allows balancer to select the store.
func (bc *BasicCluster) UnblockStore(storeID uint64) {
	bc.Stores.UnblockStore(storeID)
}

// AttachOverloadStatus attaches the overload status to a store.
func (bc *BasicCluster) AttachOverloadStatus(storeID uint64, f func() bool) {
	bc.Stores.AttachOverloadStatus(storeID, f)
}

// RandFollowerRegion returns a random region that has a follower on the store.
func (bc *BasicCluster) RandFollowerRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return bc.Regions.RandFollowerRegion(storeID, opts...)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (bc *BasicCluster) RandLeaderRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return bc.Regions.RandLeaderRegion(storeID, opts...)
}

// RandPendingRegion returns a random region that has a pending peer on the store.
func (bc *BasicCluster) RandPendingRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return bc.Regions.RandPendingRegion(storeID, opts...)
}

// GetAverageRegionSize returns the average region approximate size.
func (bc *BasicCluster) GetAverageRegionSize() int64 {
	return bc.Regions.GetAverageRegionSize()
}

// PutStore put a store
func (bc *BasicCluster) PutStore(store *StoreInfo) {
	bc.Stores.SetStore(store)
}

// DeleteStore deletes a store
func (bc *BasicCluster) DeleteStore(store *StoreInfo) {
	bc.Stores.DeleteStore(store)
}

// PutRegion put a region
func (bc *BasicCluster) PutRegion(region *RegionInfo) {
	bc.Regions.SetRegion(region)
}

// RegionSetInformer provides access to a shared informer of regions.
type RegionSetInformer interface {
	RandFollowerRegion(storeID uint64, opts ...RegionOption) *RegionInfo
	RandLeaderRegion(storeID uint64, opts ...RegionOption) *RegionInfo
	RandPendingRegion(storeID uint64, opts ...RegionOption) *RegionInfo
	GetAverageRegionSize() int64
	GetStoreRegionCount(storeID uint64) int
	GetRegion(id uint64) *RegionInfo
	GetAdjacentRegions(region *RegionInfo) (*RegionInfo, *RegionInfo)
	ScanRegions(startKey []byte, limit int) []*RegionInfo
}

// StoreSetInformer provides access to a shared informer of stores.
type StoreSetInformer interface {
	GetStores() []*StoreInfo
	GetStore(id uint64) *StoreInfo

	GetRegionStores(region *RegionInfo) []*StoreInfo
	GetFollowerStores(region *RegionInfo) []*StoreInfo
	GetLeaderStore(region *RegionInfo) *StoreInfo
}

// StoreSetController is used to control stores' status.
type StoreSetController interface {
	BlockStore(id uint64) error
	UnblockStore(id uint64)

	AttachOverloadStatus(id uint64, f func() bool)
}
