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
