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
	"bytes"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/core/storelimit"
	"go.uber.org/zap"
)

var scanRegionLimit = 1000

// BasicCluster provides basic data member and interface for a tikv cluster.
type BasicCluster struct {
	Stores struct {
		mu syncutil.RWMutex
		*StoresInfo
	}

	Regions struct {
		mu syncutil.RWMutex
		*RegionsInfo
	}
}

// NewBasicCluster creates a BasicCluster.
func NewBasicCluster() *BasicCluster {
	return &BasicCluster{
		Stores: struct {
			mu syncutil.RWMutex
			*StoresInfo
		}{StoresInfo: NewStoresInfo()},

		Regions: struct {
			mu syncutil.RWMutex
			*RegionsInfo
		}{RegionsInfo: NewRegionsInfo()},
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
	for id := range region.GetStoreIds() {
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
	bc.Regions.mu.RLock()
	leaderCount := bc.Regions.GetStoreLeaderCount(storeID)
	regionCount := bc.Regions.GetStoreRegionCount(storeID)
	pendingPeerCount := bc.Regions.GetStorePendingPeerCount(storeID)
	leaderRegionSize := bc.Regions.GetStoreLeaderRegionSize(storeID)
	regionSize := bc.Regions.GetStoreRegionSize(storeID)
	bc.Regions.mu.RUnlock()
	bc.Stores.mu.Lock()
	defer bc.Stores.mu.Unlock()
	bc.Stores.UpdateStoreStatus(storeID, leaderCount, regionCount, pendingPeerCount, leaderRegionSize, regionSize)
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

// GetRegion searches for a region by ID.
func (bc *BasicCluster) GetRegion(regionID uint64) *RegionInfo {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetRegion(regionID)
}

// GetRegions gets all RegionInfo from regionMap.
func (bc *BasicCluster) GetRegions() []*RegionInfo {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetRegions()
}

// GetMetaRegions gets a set of metapb.Region from regionMap.
func (bc *BasicCluster) GetMetaRegions() []*metapb.Region {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetMetaRegions()
}

// GetStoreRegions gets all RegionInfo with a given storeID.
func (bc *BasicCluster) GetStoreRegions(storeID uint64) []*RegionInfo {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetStoreRegions(storeID)
}

// GetLeaderStoreByRegionID returns the leader store of the given region.
func (bc *BasicCluster) GetLeaderStoreByRegionID(regionID uint64) *StoreInfo {
	bc.Regions.mu.RLock()
	region := bc.Regions.GetRegion(regionID)
	if region == nil || region.GetLeader() == nil {
		bc.Regions.mu.RUnlock()
		return nil
	}
	bc.Regions.mu.RUnlock()

	bc.Stores.mu.RLock()
	defer bc.Stores.mu.RUnlock()
	return bc.Stores.GetStore(region.GetLeader().GetStoreId())
}

// GetAdjacentRegions returns region's info that is adjacent with specific region.
func (bc *BasicCluster) GetAdjacentRegions(region *RegionInfo) (*RegionInfo, *RegionInfo) {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetAdjacentRegions(region)
}

// GetRangeHoles returns all range holes, i.e the key ranges without any region info.
func (bc *BasicCluster) GetRangeHoles() [][]string {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetRangeHoles()
}

const randomRegionMaxRetry = 10

// RandFollowerRegion returns a random region that has a follower on the store.
func (bc *BasicCluster) RandFollowerRegion(storeID uint64, ranges []KeyRange, opts ...RegionOption) *RegionInfo {
	bc.Regions.mu.RLock()
	regions := bc.Regions.RandFollowerRegions(storeID, ranges, randomRegionMaxRetry)
	bc.Regions.mu.RUnlock()
	return bc.selectRegion(regions, opts...)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (bc *BasicCluster) RandLeaderRegion(storeID uint64, ranges []KeyRange, opts ...RegionOption) *RegionInfo {
	bc.Regions.mu.RLock()
	regions := bc.Regions.RandLeaderRegions(storeID, ranges, randomRegionMaxRetry)
	bc.Regions.mu.RUnlock()
	return bc.selectRegion(regions, opts...)
}

// RandPendingRegion returns a random region that has a pending peer on the store.
func (bc *BasicCluster) RandPendingRegion(storeID uint64, ranges []KeyRange, opts ...RegionOption) *RegionInfo {
	bc.Regions.mu.RLock()
	regions := bc.Regions.RandPendingRegions(storeID, ranges, randomRegionMaxRetry)
	bc.Regions.mu.RUnlock()
	return bc.selectRegion(regions, opts...)
}

// RandLearnerRegion returns a random region that has a learner peer on the store.
func (bc *BasicCluster) RandLearnerRegion(storeID uint64, ranges []KeyRange, opts ...RegionOption) *RegionInfo {
	bc.Regions.mu.RLock()
	regions := bc.Regions.RandLearnerRegions(storeID, ranges, randomRegionMaxRetry)
	bc.Regions.mu.RUnlock()
	return bc.selectRegion(regions, opts...)
}

func (bc *BasicCluster) selectRegion(regions []*RegionInfo, opts ...RegionOption) *RegionInfo {
	for _, r := range regions {
		if r == nil {
			break
		}
		if slice.AllOf(opts, func(i int) bool { return opts[i](r) }) {
			return r
		}
	}
	return nil
}

// GetRegionCount gets the total count of RegionInfo of regionMap.
func (bc *BasicCluster) GetRegionCount() int {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetRegionCount()
}

// GetStoreRegionCount gets the total count of a store's leader and follower RegionInfo by storeID.
func (bc *BasicCluster) GetStoreRegionCount(storeID uint64) int {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetStoreLeaderCount(storeID) + bc.Regions.GetStoreFollowerCount(storeID) + bc.Regions.GetStoreLearnerCount(storeID)
}

// GetStoreLeaderCount get the total count of a store's leader RegionInfo.
func (bc *BasicCluster) GetStoreLeaderCount(storeID uint64) int {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetStoreLeaderCount(storeID)
}

// GetStoreFollowerCount get the total count of a store's follower RegionInfo.
func (bc *BasicCluster) GetStoreFollowerCount(storeID uint64) int {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetStoreFollowerCount(storeID)
}

// GetStorePendingPeerCount gets the total count of a store's region that includes pending peer.
func (bc *BasicCluster) GetStorePendingPeerCount(storeID uint64) int {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetStorePendingPeerCount(storeID)
}

// GetStoreLeaderRegionSize get total size of store's leader regions.
func (bc *BasicCluster) GetStoreLeaderRegionSize(storeID uint64) int64 {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetStoreLeaderRegionSize(storeID)
}

// GetStoreRegionSize get total size of store's regions.
func (bc *BasicCluster) GetStoreRegionSize(storeID uint64) int64 {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetStoreRegionSize(storeID)
}

// GetAverageRegionSize returns the average region approximate size.
func (bc *BasicCluster) GetAverageRegionSize() int64 {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetAverageRegionSize()
}

// GetRegionByKey searches RegionInfo from regionTree.
func (bc *BasicCluster) GetRegionByKey(regionKey []byte) *RegionInfo {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetRegionByKey(regionKey)
}

// GetPrevRegionByKey searches previous RegionInfo from regionTree.
func (bc *BasicCluster) GetPrevRegionByKey(regionKey []byte) *RegionInfo {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetPrevRegionByKey(regionKey)
}

// ScanRange scans regions intersecting [start key, end key), returns at most
// `limit` regions. limit <= 0 means no limit.
func (bc *BasicCluster) ScanRange(startKey, endKey []byte, limit int) []*RegionInfo {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.ScanRange(startKey, endKey, limit)
}

// GetOverlaps returns the regions which are overlapped with the specified region range.
func (bc *BasicCluster) GetOverlaps(region *RegionInfo) []*RegionInfo {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetOverlaps(region)
}

// GetRegionSizeByRange scans regions intersecting [start key, end key), returns the total region size of this range.
func (bc *BasicCluster) GetRegionSizeByRange(startKey, endKey []byte) int64 {
	var size int64
	for {
		bc.Regions.mu.RLock()
		var cnt int
		bc.Regions.tree.scanRange(startKey, func(region *RegionInfo) bool {
			if len(endKey) > 0 && bytes.Compare(region.GetStartKey(), endKey) >= 0 {
				return false
			}
			if cnt >= scanRegionLimit {
				return false
			}
			cnt++
			startKey = region.GetEndKey()
			size += region.GetApproximateSize()
			return true
		})
		bc.Regions.mu.RUnlock()
		if cnt == 0 {
			break
		}
		if len(startKey) == 0 {
			break
		}
	}

	return size
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
		bc.Regions.mu.RLock()
		bytesRate, keysRate := f(id)
		bc.Regions.mu.RUnlock()
		bytesRates = append(bytesRates, bytesRate)
		keysRates = append(keysRates, keysRate)
	}
	return
}

// GetRangeCount returns the number of regions that overlap with the range [startKey, endKey).
func (bc *BasicCluster) GetRangeCount(startKey, endKey []byte) int {
	bc.Regions.mu.RLock()
	defer bc.Regions.mu.RUnlock()
	return bc.Regions.GetRangeCount(startKey, endKey)
}

// GetStoresLeaderWriteRate get total write rate of each store's leaders.
func (bc *BasicCluster) GetStoresLeaderWriteRate() (storeIDs []uint64, bytesRates, keysRates []float64) {
	return bc.getWriteRate(bc.Regions.GetStoreLeaderWriteRate)
}

// GetStoresWriteRate get total write rate of each store's regions.
func (bc *BasicCluster) GetStoresWriteRate() (storeIDs []uint64, bytesRates, keysRates []float64) {
	return bc.getWriteRate(bc.Regions.GetStoreWriteRate)
}

func (bc *BasicCluster) getRelevantRegionsLocked(region *RegionInfo) (origin *RegionInfo, overlaps []*RegionInfo) {
	origin = bc.Regions.GetRegion(region.GetID())
	if origin == nil || !bytes.Equal(origin.GetStartKey(), region.GetStartKey()) || !bytes.Equal(origin.GetEndKey(), region.GetEndKey()) {
		overlaps = bc.Regions.GetOverlaps(region)
	}
	return
}

/* Regions write operations */

// PreCheckPutRegion checks if the region is valid to put.
func (bc *BasicCluster) PreCheckPutRegion(region *RegionInfo) (*RegionInfo, error) {
	bc.Regions.mu.RLock()
	origin, overlaps := bc.getRelevantRegionsLocked(region)
	bc.Regions.mu.RUnlock()
	return bc.check(region, origin, overlaps)
}

func (bc *BasicCluster) check(region, origin *RegionInfo, overlaps []*RegionInfo) (*RegionInfo, error) {
	for _, item := range overlaps {
		// PD ignores stale regions' heartbeats, unless it is recreated recently by unsafe recover operation.
		if region.GetRegionEpoch().GetVersion() < item.GetRegionEpoch().GetVersion() && !region.isRegionRecreated() {
			return nil, errRegionIsStale(region.GetMeta(), item.GetMeta())
		}
	}
	if origin == nil {
		return nil, nil
	}

	r := region.GetRegionEpoch()
	o := origin.GetRegionEpoch()
	// TiKV reports term after v3.0
	isTermBehind := region.GetTerm() > 0 && region.GetTerm() < origin.GetTerm()
	// Region meta is stale, return an error.
	if (isTermBehind || r.GetVersion() < o.GetVersion() || r.GetConfVer() < o.GetConfVer()) && !region.isRegionRecreated() {
		return origin, errRegionIsStale(region.GetMeta(), origin.GetMeta())
	}

	return origin, nil
}

// CheckAndPutRegion checks if the region is valid to put, if valid then put.
func (bc *BasicCluster) CheckAndPutRegion(region *RegionInfo) []*RegionInfo {
	origin, err := bc.PreCheckPutRegion(region)
	if err != nil {
		log.Debug("region is stale", zap.Stringer("origin", origin.GetMeta()), errs.ZapError(err))
		// return the state region to delete.
		return []*RegionInfo{region}
	}
	return bc.PutRegion(region)
}

// AtomicCheckAndPutRegion checks if the region is valid to put, if valid then put.
func (bc *BasicCluster) AtomicCheckAndPutRegion(region *RegionInfo) ([]*RegionInfo, error) {
	bc.Regions.mu.Lock()
	defer bc.Regions.mu.Unlock()
	origin, overlaps := bc.getRelevantRegionsLocked(region)
	_, err := bc.check(region, origin, overlaps)
	if err != nil {
		return nil, err
	}
	return bc.Regions.SetRegion(region), nil
}

// PutRegion put a region.
func (bc *BasicCluster) PutRegion(region *RegionInfo) []*RegionInfo {
	bc.Regions.mu.Lock()
	defer bc.Regions.mu.Unlock()
	return bc.Regions.SetRegion(region)
}

// RemoveRegionIfExist removes RegionInfo from regionTree and regionMap if exists.
func (bc *BasicCluster) RemoveRegionIfExist(id uint64) {
	bc.Regions.mu.Lock()
	defer bc.Regions.mu.Unlock()
	if r := bc.Regions.GetRegion(id); r != nil {
		bc.Regions.RemoveRegion(r)
	}
}

// ResetRegionCache drops all region cache.
func (bc *BasicCluster) ResetRegionCache() {
	bc.Regions.mu.Lock()
	defer bc.Regions.mu.Unlock()
	bc.Regions.RegionsInfo = NewRegionsInfo()
}

// RemoveRegion removes RegionInfo from regionTree and regionMap.
func (bc *BasicCluster) RemoveRegion(region *RegionInfo) {
	bc.Regions.mu.Lock()
	defer bc.Regions.mu.Unlock()
	bc.Regions.RemoveRegion(region)
}

// RegionSetInformer provides access to a shared informer of regions.
type RegionSetInformer interface {
	GetRegionCount() int
	RandFollowerRegion(storeID uint64, ranges []KeyRange, opts ...RegionOption) *RegionInfo
	RandLeaderRegion(storeID uint64, ranges []KeyRange, opts ...RegionOption) *RegionInfo
	RandLearnerRegion(storeID uint64, ranges []KeyRange, opts ...RegionOption) *RegionInfo
	RandPendingRegion(storeID uint64, ranges []KeyRange, opts ...RegionOption) *RegionInfo
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
	GetFollowerStores(region *RegionInfo) []*StoreInfo
	GetLeaderStore(region *RegionInfo) *StoreInfo
}

// StoreSetController is used to control stores' status.
type StoreSetController interface {
	PauseLeaderTransfer(id uint64) error
	ResumeLeaderTransfer(id uint64)

	SlowStoreEvicted(id uint64) error
	SlowStoreRecovered(id uint64)
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
