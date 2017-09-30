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

package schedulers

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/cache"
)

type mockCluster struct {
	id      *core.MockIDAllocator
  stores  *core.StoresInfo
  regions *core.RegionsInfo
  writeStatistics cache.Cache
	readStatistics  cache.Cache
}


func NewMockCluster(id *core.MockIDAllocator) *mockCluster{
	return &mockCluster{
    id:              id,
		stores:          core.NewStoresInfo(),
		regions:         core.NewRegionsInfo(),
    readStatistics:  cache.NewCache(10, cache.TwoQueueCache),
    writeStatistics: cache.NewCache(10, cache.TwoQueueCache),
	}
}

// GetStores returns all stores in the cluster.
func (mc *mockCluster) GetStores() []*core.StoreInfo {
	return mc.stores.GetStores()
}

// GetStore searches for a store by ID.
func (mc *mockCluster) GetStore(storeID uint64) *core.StoreInfo {
	return mc.stores.GetStore(storeID)
}

// GetRegions searches for a region by ID.
func (mc *mockCluster) GetRegion(regionID uint64) *core.RegionInfo {
	return mc.regions.GetRegion(regionID)
}

// GetRegionStores returns all stores that contains the region's peer.
func (mc *mockCluster) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	var stores []*core.StoreInfo
	for id := range region.GetStoreIds() {
		if store := mc.stores.GetStore(id); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// GetFollowerStores returns all stores that contains the region's follower peer.
func (mc *mockCluster) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	var stores []*core.StoreInfo
	for id := range region.GetFollowers() {
		if store := mc.stores.GetStore(id); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// GetLeaderStore returns all stores that contains the region's leader peer.
func (mc *mockCluster) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	return mc.stores.GetStore(region.Leader.GetStoreId())
}

// BlockStore stops balancer from selecting the store.
func (mc *mockCluster) BlockStore(storeID uint64) error {
	return errors.Trace(mc.stores.BlockStore(storeID))
}

// UnblockStore allows balancer to select the store.
func (mc *mockCluster) UnblockStore(storeID uint64) {
	mc.stores.UnblockStore(storeID)
}

// RandFollowerRegion returns a random region that has a follower on the store.
func (mc *mockCluster) RandFollowerRegion(storeID uint64) *core.RegionInfo {
	return mc.regions.RandFollowerRegion(storeID)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (mc *mockCluster) RandLeaderRegion(storeID uint64) *core.RegionInfo {
	return mc.regions.RandLeaderRegion(storeID)
}

// IsRegionHot checks if a region is in hot state.
func (mc *mockCluster) IsRegionHot(id uint64) bool {
	if stat, ok := mc.writeStatistics.Peek(id); ok {
		return stat.(*core.RegionStat).HotDegree >= hotRegionLowThreshold
	}
	return false
}


// RegionWriteStats returns hot region's write stats.
func (mc *mockCluster) RegionWriteStats() []*core.RegionStat {
	elements := mc.writeStatistics.Elems()
	stats := make([]*core.RegionStat, len(elements))
	for i := range elements {
		stats[i] = elements[i].Value.(*core.RegionStat)
	}
	return stats
}

// RegionReadStats returns hot region's read stats.
func (mc *mockCluster) RegionReadStats() []*core.RegionStat {
	elements := mc.readStatistics.Elems()
	stats := make([]*core.RegionStat, len(elements))
	for i := range elements {
		stats[i] = elements[i].Value.(*core.RegionStat)
	}
	return stats
}

func (mc *mockCluster) allocID() (uint64, error) {
	return mc.id.Alloc()
}

// AllocPeer allocs a new peer on a store.
func (mc *mockCluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := mc.allocID()
	if err != nil {
		log.Errorf("failed to alloc peer: %v", err)
		return nil, errors.Trace(err)
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

func (mc *mockCluster) putStore(store *core.StoreInfo) error {
	mc.stores.SetStore(store)
	return nil
}

func (mc *mockCluster) putRegion(region *core.RegionInfo) error {
	mc.regions.SetRegion(region)
	return nil
}

func (mc *mockCluster) setStoreUp(storeID uint64) {
	store := mc.GetStore(storeID)
	store.State = metapb.StoreState_Up
	store.LastHeartbeatTS = time.Now()
	mc.putStore(store)
}

func (mc *mockCluster) setStoreDown(storeID uint64) {
	store := mc.GetStore(storeID)
	store.State = metapb.StoreState_Up
	store.LastHeartbeatTS = time.Time{}
	mc.putStore(store)
}

func (mc *mockCluster) setStoreOffline(storeID uint64) {
	store := mc.GetStore(storeID)
	store.State = metapb.StoreState_Offline
	mc.putStore(store)
}

func (mc *mockCluster) setStoreBusy(storeID uint64, busy bool) {
	store := mc.GetStore(storeID)
	store.Stats.IsBusy = busy
	store.LastHeartbeatTS = time.Now()
	mc.putStore(store)
}

func (mc *mockCluster) addLeaderStore(storeID uint64, leaderCount int) {
	store := core.NewStoreInfo(&metapb.Store{Id: storeID})
	store.Stats = &pdpb.StoreStats{}
	store.LastHeartbeatTS = time.Now()
	store.LeaderCount = leaderCount
	mc.putStore(store)
}

func (mc *mockCluster) addRegionStore(storeID uint64, regionCount int) {
	store := core.NewStoreInfo(&metapb.Store{Id: storeID})
	store.Stats = &pdpb.StoreStats{}
	store.LastHeartbeatTS = time.Now()
	store.RegionCount = regionCount
	store.Stats.Capacity = uint64(1024)
	store.Stats.Available = store.Stats.Capacity
	mc.putStore(store)
}

func (mc *mockCluster) updateStoreLeaderWeight(storeID uint64, weight float64) {
	store := mc.GetStore(storeID)
	store.LeaderWeight = weight
	mc.putStore(store)
}

func (mc *mockCluster) updateStoreRegionWeight(storeID uint64, weight float64) {
	store := mc.GetStore(storeID)
	store.RegionWeight = weight
	mc.putStore(store)
}

func (mc *mockCluster) addLabelsStore(storeID uint64, regionCount int, labels map[string]string) {
	mc.addRegionStore(storeID, regionCount)
	store := mc.GetStore(storeID)
	for k, v := range labels {
		store.Labels = append(store.Labels, &metapb.StoreLabel{Key: k, Value: v})
	}
	mc.putStore(store)
}

func (mc *mockCluster) addLeaderRegion(regionID uint64, leaderID uint64, followerIds ...uint64) {
	region := &metapb.Region{Id: regionID}
	leader, _ := mc.AllocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := mc.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	mc.putRegion(core.NewRegionInfo(region, leader))
}

func (mc *mockCluster) LoadRegion(regionID uint64, followerIds ...uint64) {
	//  regions load from etcd will have no leader
	region := &metapb.Region{Id: regionID}
	region.Peers = []*metapb.Peer{}
	for _, id := range followerIds {
		peer, _ := mc.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	mc.putRegion(core.NewRegionInfo(region, nil))
}

func (mc *mockCluster) addLeaderRegionWithWriteInfo(regionID uint64, leaderID uint64, writtenBytes uint64, followerIds ...uint64) {
	region := &metapb.Region{Id: regionID}
	leader, _ := mc.AllocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := mc.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	r := core.NewRegionInfo(region, leader)
	r.WrittenBytes = writtenBytes
	mc.updateWriteStatus(r)
	mc.putRegion(r)
}

func (mc *mockCluster) updateLeaderCount(storeID uint64, leaderCount int) {
	store := mc.GetStore(storeID)
	store.LeaderCount = leaderCount
	mc.putStore(store)
}

func (mc *mockCluster) updateRegionCount(storeID uint64, regionCount int) {
	store := mc.GetStore(storeID)
	store.RegionCount = regionCount
	mc.putStore(store)
}

func (mc *mockCluster) updateSnapshotCount(storeID uint64, snapshotCount int) {
	store := mc.GetStore(storeID)
	store.Stats.ApplyingSnapCount = uint32(snapshotCount)
	mc.putStore(store)
}

func (mc *mockCluster) updateStorageRatio(storeID uint64, usedRatio, availableRatio float64) {
	store := mc.GetStore(storeID)
	store.Stats.Capacity = uint64(1024)
	store.Stats.UsedSize = uint64(float64(store.Stats.Capacity) * usedRatio)
	store.Stats.Available = uint64(float64(store.Stats.Capacity) * availableRatio)
	mc.putStore(store)
}

func (mc *mockCluster) updateStorageWrittenBytes(storeID uint64, BytesWritten uint64) {
	store := mc.GetStore(storeID)
	store.Stats.BytesWritten = BytesWritten
	mc.putStore(store)
}

const (
	runSchedulerCheckInterval = 3 * time.Second
	collectFactor             = 0.8
	historiesCacheSize        = 1000
	eventsCacheSize           = 1000
	maxScheduleRetries        = 10
	scheduleIntervalFactor    = 1.3

	statCacheMaxLen               = 1000
	hotRegionMinFlowRate          = 16 * 1024
	regionHeartBeatReportInterval = 60
	regionheartbeatSendChanCap    = 1024
	storeHeartBeatReportInterval  = 10
	minHotRegionReportInterval    = 3
	hotRegionAntiCount            = 1
	hotRegionScheduleName         = "balance-hot-region-scheduler"
)


func (mc *mockCluster) updateWriteStatus(region *core.RegionInfo) {
	var WrittenBytesPerSec uint64
	v, isExist := mc.writeStatistics.Peek(region.GetId())
	if isExist {
		interval := time.Since(v.(*core.RegionStat).LastUpdateTime).Seconds()
		if interval < minHotRegionReportInterval {
			return
		}
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / interval)
	} else {
		WrittenBytesPerSec = uint64(float64(region.WrittenBytes) / float64(regionHeartBeatReportInterval))
	}
	region.WrittenBytes = WrittenBytesPerSec

	// hotRegionThreshold is use to pick hot region
	// suppose the number of the hot regions is statCacheMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot regions
	// divide 2 because the store reports data about two times than the region record write to rocksdb
	divisor := float64(statCacheMaxLen) * 2 * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(mc.stores.TotalWrittenBytes()) / divisor)

	if hotRegionThreshold < hotRegionMinFlowRate {
		hotRegionThreshold = hotRegionMinFlowRate
	}
	mc.updateWriteStatCache(region, hotRegionThreshold)
}

func (mc *mockCluster) updateWriteStatCache(region *core.RegionInfo, hotRegionThreshold uint64) {
	var v *core.RegionStat
	key := region.GetId()
	value, isExist := mc.writeStatistics.Peek(key)
	newItem := &core.RegionStat{
		RegionID:       region.GetId(),
		FlowBytes:      region.WrittenBytes,
		LastUpdateTime: time.Now(),
		StoreID:        region.Leader.GetStoreId(),
		Version:        region.GetRegionEpoch().GetVersion(),
		AntiCount:      hotRegionAntiCount,
	}

	if isExist {
		v = value.(*core.RegionStat)
		newItem.HotDegree = v.HotDegree + 1
	}

	if region.WrittenBytes < hotRegionThreshold {
		if !isExist {
			return
		}
		if v.AntiCount <= 0 {
			mc.writeStatistics.Remove(key)
			return
		}
		// eliminate some noise
		newItem.HotDegree = v.HotDegree - 1
		newItem.AntiCount = v.AntiCount - 1
		newItem.FlowBytes = v.FlowBytes
	}
	mc.writeStatistics.Put(key, newItem)
}

func (mc *mockCluster) updateReadStatus(region *core.RegionInfo) {
	var ReadBytesPerSec uint64
	v, isExist := mc.readStatistics.Peek(region.GetId())
	if isExist {
		interval := time.Now().Sub(v.(*core.RegionStat).LastUpdateTime).Seconds()
		if interval < minHotRegionReportInterval {
			return
		}
		ReadBytesPerSec = uint64(float64(region.ReadBytes) / interval)
	} else {
		ReadBytesPerSec = uint64(float64(region.ReadBytes) / float64(regionHeartBeatReportInterval))
	}
	region.ReadBytes = ReadBytesPerSec

	// hotRegionThreshold is use to pick hot region
	// suppose the number of the hot regions is statLRUMaxLen
	// and we use total written Bytes past storeHeartBeatReportInterval seconds to divide the number of hot regions
	divisor := float64(statCacheMaxLen) * storeHeartBeatReportInterval
	hotRegionThreshold := uint64(float64(mc.stores.TotalReadBytes()) / divisor)

	if hotRegionThreshold < hotRegionMinFlowRate {
		hotRegionThreshold = hotRegionMinFlowRate
	}
	mc.updateReadStatCache(region, hotRegionThreshold)
}

// updateReadStatCache updates statistic for a region if it's hot, or remove it from statistics if it cools down
func (mc *mockCluster) updateReadStatCache(region *core.RegionInfo, hotRegionThreshold uint64) {
	var v *core.RegionStat
	key := region.GetId()
	value, isExist := mc.readStatistics.Peek(key)
	newItem := &core.RegionStat{
		RegionID:       region.GetId(),
		FlowBytes:      region.ReadBytes,
		LastUpdateTime: time.Now(),
		StoreID:        region.Leader.GetStoreId(),
		Version:        region.GetRegionEpoch().GetVersion(),
		AntiCount:      hotRegionAntiCount,
	}

	if isExist {
		v = value.(*core.RegionStat)
		newItem.HotDegree = v.HotDegree + 1
	}

	if region.ReadBytes < hotRegionThreshold {
		if !isExist {
			return
		}
		if v.AntiCount <= 0 {
			mc.readStatistics.Remove(key)
			return
		}
		// eliminate some noise
		newItem.HotDegree = v.HotDegree - 1
		newItem.AntiCount = v.AntiCount - 1
		newItem.FlowBytes = v.FlowBytes
	}
	mc.readStatistics.Put(key, newItem)
}


func (mc *mockCluster) updateStorageReadBytes(storeID uint64, BytesRead uint64) {
	store := mc.GetStore(storeID)
	store.Stats.BytesRead = BytesRead
	mc.putStore(store)
}

func (mc *mockCluster) addLeaderRegionWithReadInfo(regionID uint64, leaderID uint64, readBytes uint64, followerIds ...uint64) {
	region := &metapb.Region{Id: regionID}
	leader, _ := mc.AllocPeer(leaderID)
	region.Peers = []*metapb.Peer{leader}
	for _, id := range followerIds {
		peer, _ := mc.AllocPeer(id)
		region.Peers = append(region.Peers, peer)
	}
	r := core.NewRegionInfo(region, leader)
	r.ReadBytes = readBytes
	mc.updateReadStatus(r)
	mc.putRegion(r)
}


const (
	defaultMaxReplicas          = 3
	defaultMaxSnapshotCount     = 3
	defaultMaxStoreDownTime     = time.Hour
	defaultLeaderScheduleLimit  = 64
	defaultRegionScheduleLimit  = 12
	defaultReplicaScheduleLimit = 16
  hotRegionLowThreshold       = 3
)

type MockSchedulerOptions struct {
  RegionScheduleLimit uint64
  LeaderScheduleLimit uint64
  MaxSnapshotCount uint64
  MaxStoreDownTime time.Duration
  MaxReplicas int
  LocationLabels []string
  HotRegionLowThreshold int
}

func newMockSchedulerOptions() *MockSchedulerOptions {
  mso := &MockSchedulerOptions{}
  mso.RegionScheduleLimit = defaultRegionScheduleLimit
  mso.LeaderScheduleLimit = defaultLeaderScheduleLimit
  mso.MaxSnapshotCount = defaultMaxSnapshotCount
  mso.MaxStoreDownTime = defaultMaxStoreDownTime
  mso.MaxReplicas = defaultMaxReplicas
  mso.HotRegionLowThreshold = hotRegionLowThreshold
  return mso
}

func (mso *MockSchedulerOptions) GetLeaderScheduleLimit() uint64 {
  return mso.LeaderScheduleLimit
}

func (mso *MockSchedulerOptions) GetRegionScheduleLimit() uint64 {
  return mso.RegionScheduleLimit
}

func (mso *MockSchedulerOptions) GetMaxSnapshotCount() uint64 {
  return mso.MaxSnapshotCount
}

func (mso *MockSchedulerOptions) GetMaxStoreDownTime() time.Duration {
  return mso.MaxStoreDownTime
}

func (mso *MockSchedulerOptions) GetMaxReplicas() int {
  return mso.MaxReplicas
}

func (mso *MockSchedulerOptions) GetLocationLabels() []string {
  return mso.LocationLabels
}

func (mso *MockSchedulerOptions) GetHotRegionLowThreshold() int {
  return mso.HotRegionLowThreshold
}

func (mso *MockSchedulerOptions) SetMaxReplicas(replicas int) {
  mso.MaxReplicas = replicas
}
