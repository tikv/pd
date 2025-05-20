// Copyright 2017 TiKV Project Authors.
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

package core

import (
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

// BasicCluster provides basic data member and interface for a tikv cluster.
type BasicCluster struct {
	*StoresInfo
	*RegionsInfo
}

// NewBasicCluster creates a BasicCluster.
func NewBasicCluster() *BasicCluster {
	return &BasicCluster{
		StoresInfo:  NewStoresInfo(),
		RegionsInfo: NewRegionsInfo(),
	}
}

// UpdateStoreStatus updates the information of the store.
func (bc *BasicCluster) UpdateStoreStatus(storeID uint64) {
	leaderCount, regionCount, witnessCount, learnerCount, pendingPeerCount, leaderRegionSize, regionSize := bc.GetStoreStats(storeID)
	bc.StoresInfo.UpdateStoreStatus(storeID, leaderCount, regionCount, witnessCount, learnerCount, pendingPeerCount, leaderRegionSize, regionSize)
}

/* Regions read operations */

// GetLeaderStoreByRegionID returns the leader store of the given region.
func (bc *BasicCluster) GetLeaderStoreByRegionID(regionID uint64) *StoreInfo {
	region := bc.GetRegion(regionID)
	if region == nil || region.GetLeader() == nil {
		return nil
	}

	return bc.GetStore(region.GetLeader().GetStoreId())
}

func (bc *BasicCluster) getWriteRate(
	f func(storeID uint64) (bytesRate, keysRate float64),
) (storeIDs []uint64, bytesRates, keysRates []float64) {
	storeIDs = bc.GetStoreIDs()
	count := len(storeIDs)
	bytesRates = make([]float64, 0, count)
	keysRates = make([]float64, 0, count)
	for _, id := range storeIDs {
		bytesRate, keysRate := f(id)
		bytesRates = append(bytesRates, bytesRate)
		keysRates = append(keysRates, keysRate)
	}
	return
}

// GetStoresLeaderWriteRate get total write rate of each store's leaders.
func (bc *BasicCluster) GetStoresLeaderWriteRate() (storeIDs []uint64, bytesRates, keysRates []float64) {
	return bc.getWriteRate(bc.GetStoreLeaderWriteRate)
}

// GetStoresWriteRate get total write rate of each store's regions.
func (bc *BasicCluster) GetStoresWriteRate() (storeIDs []uint64, bytesRates, keysRates []float64) {
	return bc.getWriteRate(bc.GetStoreWriteRate)
}

// UpdateAllStoreStatus updates the information of all stores.
func (bc *BasicCluster) UpdateAllStoreStatus() {
	// Update related stores.
	stores := bc.GetStores()
	for _, store := range stores {
		if store.IsRemoved() {
			continue
		}
		bc.UpdateStoreStatus(store.GetID())
	}
}

// RegionSetInformer provides access to a shared informer of regions.
type RegionSetInformer interface {
	GetTotalRegionCount() int
	RandFollowerRegions(storeID uint64, ranges []keyutil.KeyRange) []*RegionInfo
	RandLeaderRegions(storeID uint64, ranges []keyutil.KeyRange) []*RegionInfo
	RandLearnerRegions(storeID uint64, ranges []keyutil.KeyRange) []*RegionInfo
	RandWitnessRegions(storeID uint64, ranges []keyutil.KeyRange) []*RegionInfo
	RandPendingRegions(storeID uint64, ranges []keyutil.KeyRange) []*RegionInfo
	GetAverageRegionSize() int64
	GetStoreRegionCount(storeID uint64) int
	GetRegion(id uint64) *RegionInfo
	GetAdjacentRegions(region *RegionInfo) (*RegionInfo, *RegionInfo)
	ScanRegions(startKey, endKey []byte, limit int) []*RegionInfo
	GetRegionByKey(regionKey []byte) *RegionInfo
	BatchScanRegions(keyRanges *keyutil.KeyRanges, opts ...BatchScanRegionsOptionFunc) ([]*RegionInfo, error)
}

type batchScanRegionsOptions struct {
	limit                        int
	outputMustContainAllKeyRange bool
}

// BatchScanRegionsOptionFunc is the option function for BatchScanRegions.
type BatchScanRegionsOptionFunc func(*batchScanRegionsOptions)

// WithLimit is an option for batchScanRegionsOptions.
func WithLimit(limit int) BatchScanRegionsOptionFunc {
	return func(opt *batchScanRegionsOptions) {
		opt.limit = limit
	}
}

// WithOutputMustContainAllKeyRange is an option for batchScanRegionsOptions.
func WithOutputMustContainAllKeyRange() BatchScanRegionsOptionFunc {
	return func(opt *batchScanRegionsOptions) {
		opt.outputMustContainAllKeyRange = true
	}
}

// StoreSetInformer provides access to a shared informer of stores.
type StoreSetInformer interface {
	GetStores() []*StoreInfo
	GetStore(id uint64) *StoreInfo

	GetRegionStores(region *RegionInfo) []*StoreInfo
	GetNonWitnessVoterStores(region *RegionInfo) []*StoreInfo
	GetFollowerStores(region *RegionInfo) []*StoreInfo
	GetLeaderStore(region *RegionInfo) *StoreInfo
}

// StoreSetController is used to control stores' status.
type StoreSetController interface {
	PauseLeaderTransfer(id uint64, d constant.Direction) error
	ResumeLeaderTransfer(id uint64, d constant.Direction)

	SlowStoreEvicted(id uint64) error
	SlowStoreRecovered(id uint64)
	SlowTrendEvicted(id uint64) error
	SlowTrendRecovered(id uint64)
}
