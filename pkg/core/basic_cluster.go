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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// BasicCluster provides basic data member and interface for a tikv cluster.
type BasicCluster struct {
	Stores struct {
		mu syncutil.RWMutex
		*StoresInfo
	}

	*RegionsInfo
}

// NewBasicCluster creates a BasicCluster.
func NewBasicCluster() *BasicCluster {
	return &BasicCluster{
		Stores: struct {
			mu syncutil.RWMutex
			*StoresInfo
		}{StoresInfo: NewStoresInfo()},

		RegionsInfo: NewRegionsInfo(),
	}
}

/* Stores read operations */

// GetStores returns all Stores in the cluster.
func (bc *BasicCluster) GetStores() []*StoreInfo {
	bc.Stores.mu.RLock()
	defer bc.Stores.mu.RUnlock()
	return bc.Stores.GetStores()
}

// GetMetaStores gets a complete set of metapb.Store.
func (bc *BasicCluster) GetMetaStores() []*metapb.Store {
	bc.Stores.mu.RLock()
	defer bc.Stores.mu.RUnlock()
	return bc.Stores.GetMetaStores()
}

// GetStore searches for a store by ID.
func (bc *BasicCluster) GetStore(storeID uint64) *StoreInfo {
	bc.Stores.mu.RLock()
	defer bc.Stores.mu.RUnlock()
	return bc.Stores.GetStore(storeID)
}

// GetRegionStores returns all Stores that contains the region's peer.
func (bc *BasicCluster) GetRegionStores(region *RegionInfo) []*StoreInfo {
	bc.Stores.mu.RLock()
	defer bc.Stores.mu.RUnlock()
	var Stores []*StoreInfo
	for id := range region.GetStoreIDs() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetNonWitnessVoterStores returns all Stores that contains the non-witness's voter peer.
func (bc *BasicCluster) GetNonWitnessVoterStores(region *RegionInfo) []*StoreInfo {
	bc.Stores.mu.RLock()
	defer bc.Stores.mu.RUnlock()
	var Stores []*StoreInfo
	for id := range region.GetNonWitnessVoters() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetFollowerStores returns all Stores that contains the region's follower peer.
func (bc *BasicCluster) GetFollowerStores(region *RegionInfo) []*StoreInfo {
	bc.Stores.mu.RLock()
	defer bc.Stores.mu.RUnlock()
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
	bc.Stores.mu.RLock()
	defer bc.Stores.mu.RUnlock()
	return bc.Stores.GetStore(region.GetLeader().GetStoreId())
}

// GetStoreCount returns the total count of storeInfo.
func (bc *BasicCluster) GetStoreCount() int {
	bc.Stores.mu.RLock()
	defer bc.Stores.mu.RUnlock()
	return bc.Stores.GetStoreCount()
}

/* Stores Write operations */

// PauseLeaderTransfer prevents the store from been selected as source or
// target store of TransferLeader.
func (bc *BasicCluster) PauseLeaderTransfer(storeID uint64) error {
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	return bc.Stores.PauseLeaderTransfer(storeID)
}

// ResumeLeaderTransfer cleans a store's pause state. The store can be selected
// as source or target of TransferLeader again.
func (bc *BasicCluster) ResumeLeaderTransfer(storeID uint64) {
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	bc.Stores.ResumeLeaderTransfer(storeID)
}

// SlowStoreEvicted marks a store as a slow store and prevents transferring
// leader to the store
func (bc *BasicCluster) SlowStoreEvicted(storeID uint64) error {
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	return bc.Stores.SlowStoreEvicted(storeID)
}

// SlowTrendEvicted marks a store as a slow store by trend and prevents transferring
// leader to the store
func (bc *BasicCluster) SlowTrendEvicted(storeID uint64) error {
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	return bc.Stores.SlowTrendEvicted(storeID)
}

// SlowTrendRecovered cleans the evicted by slow trend state of a store.
func (bc *BasicCluster) SlowTrendRecovered(storeID uint64) {
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	bc.Stores.SlowTrendRecovered(storeID)
}

// SlowStoreRecovered cleans the evicted state of a store.
func (bc *BasicCluster) SlowStoreRecovered(storeID uint64) {
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	bc.Stores.SlowStoreRecovered(storeID)
}

// ResetStoreLimit resets the limit for a specific store.
func (bc *BasicCluster) ResetStoreLimit(storeID uint64, limitType storelimit.Type, ratePerSec ...float64) {
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	bc.Stores.ResetStoreLimit(storeID, limitType, ratePerSec...)
}

// UpdateStoreStatus updates the information of the store.
func (bc *BasicCluster) UpdateStoreStatus(storeID uint64) {
	leaderCount, regionCount, witnessCount, learnerCount, pendingPeerCount, leaderRegionSize, regionSize := bc.RegionsInfo.GetStoreStats(storeID)
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	bc.Stores.UpdateStoreStatus(storeID, leaderCount, regionCount, witnessCount, learnerCount, pendingPeerCount, leaderRegionSize, regionSize)
}

// PutStore put a store.
func (bc *BasicCluster) PutStore(store *StoreInfo) {
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	bc.Stores.SetStore(store)
}

// ResetStores resets the store cache.
func (bc *BasicCluster) ResetStores() {
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	bc.Stores.StoresInfo = NewStoresInfo()
}

// DeleteStore deletes a store.
func (bc *BasicCluster) DeleteStore(store *StoreInfo) {
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	bc.Stores.DeleteStore(store)
}

/* Regions read operations */

// GetLeaderStoreByRegionID returns the leader store of the given region.
func (bc *BasicCluster) GetLeaderStoreByRegionID(regionID uint64) *StoreInfo {
	region := bc.RegionsInfo.GetRegion(regionID)
	if region == nil || region.GetLeader() == nil {
		return nil
	}

	bc.Stores.mu.RLock()
	defer bc.Stores.mu.RUnlock()
	return bc.Stores.GetStore(region.GetLeader().GetStoreId())
}

func (bc *BasicCluster) getWriteRate(
	f func(storeID uint64) (bytesRate, keysRate float64),
) (storeIDs []uint64, bytesRates, keysRates []float64) {
	bc.Stores.mu.RLock()
	count := len(bc.Stores.stores)
	storeIDs = make([]uint64, 0, count)
	for _, store := range bc.Stores.stores {
		storeIDs = append(storeIDs, store.GetID())
	}
	bc.Stores.mu.RUnlock()
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
	return bc.getWriteRate(bc.RegionsInfo.GetStoreLeaderWriteRate)
}

// GetStoresWriteRate get total write rate of each store's regions.
func (bc *BasicCluster) GetStoresWriteRate() (storeIDs []uint64, bytesRates, keysRates []float64) {
	return bc.getWriteRate(bc.RegionsInfo.GetStoreWriteRate)
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
	RandFollowerRegions(storeID uint64, ranges []KeyRange) []*RegionInfo
	RandLeaderRegions(storeID uint64, ranges []KeyRange) []*RegionInfo
	RandLearnerRegions(storeID uint64, ranges []KeyRange) []*RegionInfo
	RandWitnessRegions(storeID uint64, ranges []KeyRange) []*RegionInfo
	RandPendingRegions(storeID uint64, ranges []KeyRange) []*RegionInfo
	GetAverageRegionSize() int64
	GetStoreRegionCount(storeID uint64) int
	GetRegion(id uint64) *RegionInfo
	GetAdjacentRegions(region *RegionInfo) (*RegionInfo, *RegionInfo)
	ScanRegions(startKey, endKey []byte, limit int) []*RegionInfo
	GetRegionByKey(regionKey []byte) *RegionInfo
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
	PauseLeaderTransfer(id uint64) error
	ResumeLeaderTransfer(id uint64)

	SlowStoreEvicted(id uint64) error
	SlowStoreRecovered(id uint64)
	SlowTrendEvicted(id uint64) error
	SlowTrendRecovered(id uint64)
}

// KeyRange is a key range.
type KeyRange struct {
	StartKey []byte `json:"start-key"`
	EndKey   []byte `json:"end-key"`
}

// NewKeyRange create a KeyRange with the given start key and end key.
func NewKeyRange(startKey, endKey string) KeyRange {
	return KeyRange{
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
	}
}

// KeyRanges is a slice of KeyRange.
type KeyRanges struct {
	krs []*KeyRange
}

// Append appends a KeyRange.
func (rs *KeyRanges) Append(startKey, endKey []byte) {
	rs.krs = append(rs.krs, &KeyRange{
		StartKey: startKey,
		EndKey:   endKey,
	})
}

// Ranges returns the slice of KeyRange.
func (rs *KeyRanges) Ranges() []*KeyRange {
	if rs == nil {
		return nil
	}
	return rs.krs
}
