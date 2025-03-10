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

package mockcluster

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/docker/go-units"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockid"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
)

const (
	defaultStoreCapacity = 100 * units.GiB // 100GiB
	defaultRegionSize    = 96 * units.MiB  // 96MiB
)

// Cluster is used to mock a cluster for test purpose.
type Cluster struct {
	ctx context.Context
	*core.BasicCluster
	*mockid.IDAllocator
	*placement.RuleManager
	*labeler.RegionLabeler
	*statistics.HotStat
	*config.PersistOptions
	pendingProcessedRegions map[uint64]struct{}
	*buckets.HotBucketCache
	storage.Storage
}

// NewCluster creates a new Cluster
func NewCluster(ctx context.Context, opts *config.PersistOptions) *Cluster {
	bc := core.NewBasicCluster()
	c := &Cluster{
		ctx:                     ctx,
		BasicCluster:            bc,
		IDAllocator:             mockid.NewIDAllocator(),
		HotStat:                 statistics.NewHotStat(ctx, bc),
		HotBucketCache:          buckets.NewBucketsCache(ctx),
		PersistOptions:          opts,
		pendingProcessedRegions: map[uint64]struct{}{},
		Storage:                 storage.NewStorageWithMemoryBackend(),
	}
	if c.PersistOptions.GetReplicationConfig().EnablePlacementRules {
		c.initRuleManager()
	}
	// It should be updated to the latest feature version.
	c.PersistOptions.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.HotScheduleWithQuery))
	c.RegionLabeler, _ = labeler.NewRegionLabeler(ctx, c.Storage, time.Second*5)
	return c
}

// GetStoreConfig returns the store config.
func (mc *Cluster) GetStoreConfig() sc.StoreConfigProvider {
	return mc.PersistOptions.GetStoreConfig()
}

// SetRegionBucketEnabled sets the region bucket enabled.
func (mc *Cluster) SetRegionBucketEnabled(enabled bool) {
	cfg, ok := mc.GetStoreConfig().(*sc.StoreConfig)
	if !ok || cfg == nil {
		return
	}
	cfg.Coprocessor.EnableRegionBucket = enabled
	mc.SetStoreConfig(cfg)
}

// GetCheckerConfig returns the checker config.
func (mc *Cluster) GetCheckerConfig() sc.CheckerConfigProvider {
	return mc.PersistOptions
}

// GetSchedulerConfig returns the scheduler config.
func (mc *Cluster) GetSchedulerConfig() sc.SchedulerConfigProvider {
	return mc.PersistOptions
}

// GetSharedConfig returns the shared config.
func (mc *Cluster) GetSharedConfig() sc.SharedConfigProvider {
	return mc.PersistOptions
}

// GetStorage returns the storage.
func (mc *Cluster) GetStorage() storage.Storage {
	return mc.Storage
}

// AllocID returns a new unique ID.
func (mc *Cluster) AllocID(uint32) (uint64, uint32, error) {
	return mc.IDAllocator.Alloc(1)
}

// UpdateRegionsLabelLevelStats updates the label level stats for the regions.
func (*Cluster) UpdateRegionsLabelLevelStats(_ []*core.RegionInfo) {}

// LoadRegion puts region info without leader
func (mc *Cluster) LoadRegion(regionID uint64, peerStoreIDs ...uint64) {
	//  regions load from etcd will have no leader
	r := mc.newMockRegionInfo(regionID, 0, peerStoreIDs...).Clone(core.WithLeader(nil))
	mc.PutRegion(r)
}

// GetStoresLoads gets stores load statistics.
func (mc *Cluster) GetStoresLoads() map[uint64][]float64 {
	mc.HotStat.FilterUnhealthyStore(mc)
	return mc.HotStat.GetStoresLoads()
}

// IsRegionHot checks if the region is hot.
func (mc *Cluster) IsRegionHot(region *core.RegionInfo) bool {
	return mc.HotCache.IsRegionHot(region, mc.GetHotRegionCacheHitsThreshold())
}

// GetHotPeerStat returns hot peer stat with specified regionID and storeID.
func (mc *Cluster) GetHotPeerStat(rw utils.RWType, regionID, storeID uint64) *statistics.HotPeerStat {
	return mc.HotCache.GetHotPeerStat(rw, regionID, storeID)
}

// BucketsStats returns hot region's buckets stats.
func (mc *Cluster) BucketsStats(degree int, regions ...uint64) map[uint64][]*buckets.BucketStat {
	task := buckets.NewCollectBucketStatsTask(degree, regions...)
	if !mc.HotBucketCache.CheckAsync(task) {
		return nil
	}
	return task.WaitRet(mc.ctx)
}

// GetHotPeerStats returns the read or write statistics for hot regions.
// It returns a map where the keys are store IDs and the values are slices of HotPeerStat.
// The result only includes peers that are hot enough.
func (mc *Cluster) GetHotPeerStats(rw utils.RWType) map[uint64][]*statistics.HotPeerStat {
	return mc.HotCache.GetHotPeerStats(rw, mc.GetHotRegionCacheHitsThreshold())
}

// HotRegionsFromStore picks hot regions in specify store.
func (mc *Cluster) HotRegionsFromStore(store uint64, kind utils.RWType) []*core.RegionInfo {
	stats := hotRegionsFromStore(mc.HotCache, store, kind, mc.GetHotRegionCacheHitsThreshold())
	regions := make([]*core.RegionInfo, 0, len(stats))
	for _, stat := range stats {
		region := mc.GetRegion(stat.RegionID)
		if region != nil {
			regions = append(regions, region)
		}
	}
	return regions
}

// hotRegionsFromStore picks hot region in specify store.
func hotRegionsFromStore(w *statistics.HotCache, storeID uint64, kind utils.RWType, minHotDegree int) []*statistics.HotPeerStat {
	if stats, ok := w.GetHotPeerStats(kind, minHotDegree)[storeID]; ok && len(stats) > 0 {
		return stats
	}
	return nil
}

// AllocPeer allocs a new peer on a store.
func (mc *Cluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, _, err := mc.AllocID(1)
	if err != nil {
		log.Error("failed to alloc peer", errs.ZapError(err))
		return nil, err
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

func (mc *Cluster) initRuleManager() {
	if mc.RuleManager == nil {
		mc.RuleManager = placement.NewRuleManager(mc.ctx, mc.GetStorage(), mc, mc.GetSharedConfig())
		mc.RuleManager.Initialize(int(mc.GetReplicationConfig().MaxReplicas), mc.GetReplicationConfig().LocationLabels, mc.GetReplicationConfig().IsolationLevel, false)
	}
}

// GetRuleManager returns the ruleManager of the cluster.
func (mc *Cluster) GetRuleManager() *placement.RuleManager {
	return mc.RuleManager
}

// GetRegionLabeler returns the region labeler of the cluster.
func (mc *Cluster) GetRegionLabeler() *labeler.RegionLabeler {
	return mc.RegionLabeler
}

// SetStoreUp sets store state to be up.
func (mc *Cluster) SetStoreUp(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetStoreState(metapb.StoreState_Up),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(newStore)
}

// SetStoreDisconnect changes a store's state to disconnected.
func (mc *Cluster) SetStoreDisconnect(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetStoreState(metapb.StoreState_Up),
		core.SetLastHeartbeatTS(time.Now().Add(-time.Second*30)),
	)
	mc.PutStore(newStore)
}

// SetStoreDown sets store down.
func (mc *Cluster) SetStoreDown(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetStoreState(metapb.StoreState_Up),
		core.SetLastHeartbeatTS(typeutil.ZeroTime),
	)
	mc.PutStore(newStore)
}

// SetStoreOffline sets store state to be offline.
func (mc *Cluster) SetStoreOffline(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline, false))
	mc.PutStore(newStore)
}

// SetStoreBusy sets store busy.
func (mc *Cluster) SetStoreBusy(storeID uint64, busy bool) {
	store := mc.GetStore(storeID)
	newStats := typeutil.DeepClone(store.GetStoreStats(), core.StoreStatsFactory)
	newStats.IsBusy = busy
	newStore := store.Clone(
		core.SetStoreStats(newStats),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(newStore)
}

// BuryStore marks a store as tombstone in cluster.
func (mc *Cluster) BuryStore(storeID uint64, forceBury bool) error {
	store := mc.GetStore(storeID)
	if store.IsUp() {
		if !forceBury {
			return errs.ErrStoreIsUp.FastGenByArgs()
		} else if !store.IsDisconnected() {
			return errors.Errorf("The store %v is not offline nor disconnected", storeID)
		}
	}

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	mc.PutStore(newStore)
	return nil
}

// AddLeaderStore adds store with specified count of leader.
func (mc *Cluster) AddLeaderStore(storeID uint64, leaderCount int, leaderSizes ...int64) {
	stats := &pdpb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.UsedSize = uint64(leaderCount) * defaultRegionSize
	stats.Available = stats.Capacity - uint64(leaderCount)*defaultRegionSize
	var leaderSize int64
	if len(leaderSizes) != 0 {
		leaderSize = leaderSizes[0]
	} else {
		leaderSize = int64(leaderCount) * defaultRegionSize / units.MiB
	}

	store := core.NewStoreInfo(
		&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(leaderSize),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	mc.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	mc.PutStore(store)
}

// AddRegionStore adds store with specified count of region.
func (mc *Cluster) AddRegionStore(storeID uint64, regionCount int, regionSizes ...uint64) {
	var regionSize uint64
	if len(regionSizes) == 0 {
		regionSize = uint64(int64(regionCount) * defaultRegionSize / units.MiB)
	} else {
		regionSize = regionSizes[0]
	}
	stats := &pdpb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.UsedSize = uint64(regionCount) * defaultRegionSize
	stats.Available = stats.Capacity - uint64(regionCount)*defaultRegionSize
	store := core.NewStoreInfo(
		&metapb.Store{Id: storeID, Labels: []*metapb.StoreLabel{
			{
				Key:   "ID",
				Value: fmt.Sprintf("%v", storeID),
			},
		}},
		core.SetStoreStats(stats),
		core.SetRegionCount(regionCount),
		core.SetLastHeartbeatTS(time.Now()),
		core.SetRegionSize(int64(regionSize)),
	)
	mc.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	mc.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	mc.PutStore(store)
}

// AddWitnessStore adds store with specified count of witness.
func (mc *Cluster) AddWitnessStore(storeID uint64, witnessCount int) {
	stats := &pdpb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.UsedSize = 0
	stats.Available = stats.Capacity
	store := core.NewStoreInfo(
		&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetWitnessCount(witnessCount),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	mc.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	mc.PutStore(store)
}

// AddRegionStoreWithLeader adds store with specified count of region and leader.
func (mc *Cluster) AddRegionStoreWithLeader(storeID uint64, regionCount int, leaderCounts ...int) {
	leaderCount := regionCount
	if len(leaderCounts) != 0 {
		leaderCount = leaderCounts[0]
	}
	mc.AddRegionStore(storeID, regionCount)
	for range leaderCount {
		id, _, _ := mc.AllocID(1)
		mc.AddLeaderRegion(id, storeID)
	}
}

// AddLabelsStore adds store with specified count of region and labels.
func (mc *Cluster) AddLabelsStore(storeID uint64, regionCount int, labels map[string]string) {
	newLabels := make([]*metapb.StoreLabel, 0, len(labels))
	for k, v := range labels {
		newLabels = append(newLabels, &metapb.StoreLabel{Key: k, Value: v})
	}
	stats := &pdpb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.Available = stats.Capacity - uint64(regionCount)*defaultRegionSize
	stats.UsedSize = uint64(regionCount) * defaultRegionSize
	store := core.NewStoreInfo(
		&metapb.Store{
			Id:     storeID,
			Labels: newLabels,
		},
		core.SetStoreStats(stats),
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionCount)*defaultRegionSize/units.MiB),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	mc.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	mc.PutStore(store)
}

// AddLabersStoreWithLearnerCount adds store with specified count of region, learner and labels.
func (mc *Cluster) AddLabersStoreWithLearnerCount(storeID uint64, regionCount int, learnerCount int, labels map[string]string) {
	mc.AddLabelsStore(storeID, regionCount, labels)
	store := mc.GetStore(storeID).Clone(core.SetLearnerCount(learnerCount))
	mc.PutStore(store)
}

// AddLeaderRegion adds region with specified leader and followers.
func (mc *Cluster) AddLeaderRegion(regionID uint64, leaderStoreID uint64, otherPeerStoreIDs ...uint64) *core.RegionInfo {
	origin := mc.newMockRegionInfo(regionID, leaderStoreID, otherPeerStoreIDs...)
	region := origin.Clone(core.SetApproximateSize(defaultRegionSize/units.MiB), core.SetApproximateKeys(10))
	mc.PutRegion(region)
	return region
}

// AddLeaderRegionWithWitness adds region with specified leader and followers and witness.
func (mc *Cluster) AddLeaderRegionWithWitness(regionID uint64, leaderStoreID uint64, otherPeerStoreIDs []uint64, witnessStoreID uint64) *core.RegionInfo {
	origin := mc.newMockRegionInfo(regionID, leaderStoreID, otherPeerStoreIDs...)
	region := origin.Clone(core.SetApproximateSize(defaultRegionSize/units.MiB), core.SetApproximateKeys(10), core.WithWitness(origin.GetStorePeer(witnessStoreID).Id))
	mc.PutRegion(region)
	return region
}

// AddLightWeightLeaderRegion adds a light-wight region with specified leader and followers.
func (mc *Cluster) AddLightWeightLeaderRegion(regionID uint64, leaderStoreID uint64, otherPeerStoreIDs ...uint64) *core.RegionInfo {
	region := mc.newMockRegionInfo(regionID, leaderStoreID, otherPeerStoreIDs...)
	mc.PutRegion(region)
	return region
}

// AddNoLeaderRegion adds region with specified replicas, no leader.
func (mc *Cluster) AddNoLeaderRegion(regionID uint64, otherPeerStoreIDs ...uint64) *core.RegionInfo {
	origin := mc.newMockRegionInfo(regionID, 0, otherPeerStoreIDs...)
	region := origin.Clone(core.SetApproximateSize(defaultRegionSize/units.MiB), core.SetApproximateKeys(10))
	mc.PutRegion(region)
	return region
}

// AddRegionWithLearner adds region with specified leader, followers and learners.
func (mc *Cluster) AddRegionWithLearner(regionID uint64, leaderStoreID uint64, followerStoreIDs, learnerStoreIDs []uint64) *core.RegionInfo {
	origin := mc.MockRegionInfo(regionID, leaderStoreID, followerStoreIDs, learnerStoreIDs, nil)
	region := origin.Clone(core.SetApproximateSize(defaultRegionSize/units.MiB), core.SetApproximateKeys(10))
	mc.PutRegion(region)
	return region
}

// AddLeaderRegionWithRange adds region with specified leader, followers and key range.
func (mc *Cluster) AddLeaderRegionWithRange(regionID uint64, startKey string, endKey string, leaderStoreID uint64, otherPeerStoreIDs ...uint64) {
	o := mc.newMockRegionInfo(regionID, leaderStoreID, otherPeerStoreIDs...)
	r := o.Clone(
		core.WithStartKey([]byte(startKey)),
		core.WithEndKey([]byte(endKey)),
	)
	mc.PutRegion(r)
}

// AddRegionWithReadInfo adds region with specified leader, followers and read info.
func (mc *Cluster) AddRegionWithReadInfo(
	regionID uint64, leaderStoreID uint64,
	readBytes, readKeys, readQuery uint64,
	reportInterval uint64,
	otherPeerStoreIDs []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockRegionInfo(regionID, leaderStoreID, otherPeerStoreIDs...)
	r = r.Clone(core.SetReadBytes(readBytes))
	r = r.Clone(core.SetReadKeys(readKeys))
	r = r.Clone(core.SetReportInterval(0, reportInterval))
	r = r.Clone(core.SetReadQuery(readQuery))
	filledNum := utils.DefaultAotSize
	if len(filledNums) > 0 {
		filledNum = filledNums[0]
	}

	var items []*statistics.HotPeerStat
	for range filledNum {
		items = mc.CheckRegionRead(r)
		for _, item := range items {
			mc.HotCache.Update(item, utils.Read)
		}
	}
	mc.PutRegion(r)
	return items
}

// AddRegionWithPeerReadInfo adds region with specified peer read info.
func (mc *Cluster) AddRegionWithPeerReadInfo(regionID, leaderStoreID, targetStoreID, readBytes, readKeys, reportInterval uint64,
	otherPeerStoreIDs []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockRegionInfo(regionID, leaderStoreID, otherPeerStoreIDs...)
	r = r.Clone(core.SetReadBytes(readBytes), core.SetReadKeys(readKeys), core.SetReportInterval(0, reportInterval))
	filledNum := utils.DefaultAotSize
	if len(filledNums) > 0 {
		filledNum = filledNums[0]
	}
	var items []*statistics.HotPeerStat
	for range filledNum {
		items = mc.CheckRegionRead(r)
		for _, item := range items {
			if item.StoreID == targetStoreID {
				mc.HotCache.Update(item, utils.Read)
			}
		}
	}
	mc.PutRegion(r)
	return items
}

// AddRegionLeaderWithReadInfo add region leader read info
func (mc *Cluster) AddRegionLeaderWithReadInfo(
	regionID uint64, leaderStoreID uint64,
	readBytes, readKeys, readQuery uint64,
	reportInterval uint64,
	otherPeerStoreIDs []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockRegionInfo(regionID, leaderStoreID, otherPeerStoreIDs...)
	r = r.Clone(core.SetReadBytes(readBytes))
	r = r.Clone(core.SetReadKeys(readKeys))
	r = r.Clone(core.SetReadQuery(readQuery))
	r = r.Clone(core.SetReportInterval(0, reportInterval))
	filledNum := utils.DefaultAotSize
	if len(filledNums) > 0 {
		filledNum = filledNums[0]
	}

	var items []*statistics.HotPeerStat
	for range filledNum {
		items = mc.CheckRegionLeaderRead(r)
		for _, item := range items {
			mc.HotCache.Update(item, utils.Read)
		}
	}
	mc.PutRegion(r)
	return items
}

// AddLeaderRegionWithWriteInfo adds region with specified leader and peers write info.
func (mc *Cluster) AddLeaderRegionWithWriteInfo(
	regionID uint64, leaderStoreID uint64,
	writtenBytes, writtenKeys, writtenQuery uint64,
	reportInterval uint64,
	otherPeerStoreIDs []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockRegionInfo(regionID, leaderStoreID, otherPeerStoreIDs...)
	r = r.Clone(core.SetWrittenBytes(writtenBytes))
	r = r.Clone(core.SetWrittenKeys(writtenKeys))
	r = r.Clone(core.SetReportInterval(0, reportInterval))
	r = r.Clone(core.SetWrittenQuery(writtenQuery))

	filledNum := utils.DefaultAotSize
	if len(filledNums) > 0 {
		filledNum = filledNums[0]
	}

	var items []*statistics.HotPeerStat
	for range filledNum {
		items = mc.CheckRegionWrite(r)
		for _, item := range items {
			mc.HotCache.Update(item, utils.Write)
		}
	}
	mc.PutRegion(r)
	return items
}

// UpdateStoreLeaderWeight updates store leader weight.
func (mc *Cluster) UpdateStoreLeaderWeight(storeID uint64, weight float64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetLeaderWeight(weight))
	mc.PutStore(newStore)
}

// SetStoreEvictLeader set store whether evict leader.
func (mc *Cluster) SetStoreEvictLeader(storeID uint64, enableEvictLeader bool) {
	store := mc.GetStore(storeID)
	if enableEvictLeader {
		mc.PutStore(store.Clone(core.PauseLeaderTransfer(constant.In)))
	} else {
		mc.PutStore(store.Clone(core.ResumeLeaderTransfer(constant.In)))
	}
}

// UpdateStoreRegionWeight updates store region weight.
func (mc *Cluster) UpdateStoreRegionWeight(storeID uint64, weight float64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetRegionWeight(weight))
	mc.PutStore(newStore)
}

// UpdateStoreLeaderSize updates store leader size.
func (mc *Cluster) UpdateStoreLeaderSize(storeID uint64, size int64) {
	store := mc.GetStore(storeID)
	newStats := typeutil.DeepClone(store.GetStoreStats(), core.StoreStatsFactory)
	newStats.Available = newStats.Capacity - uint64(store.GetLeaderSize())
	newStore := store.Clone(
		core.SetStoreStats(newStats),
		core.SetLeaderSize(size),
	)
	mc.PutStore(newStore)
}

// UpdateStoreRegionSize updates store region size.
func (mc *Cluster) UpdateStoreRegionSize(storeID uint64, size int64) {
	store := mc.GetStore(storeID)
	newStats := typeutil.DeepClone(store.GetStoreStats(), core.StoreStatsFactory)
	newStats.Available = newStats.Capacity - uint64(store.GetRegionSize())
	newStore := store.Clone(
		core.SetStoreStats(newStats),
		core.SetRegionSize(size),
	)
	mc.PutStore(newStore)
}

// UpdateLeaderCount updates store leader count.
func (mc *Cluster) UpdateLeaderCount(storeID uint64, leaderCount int) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(int64(leaderCount)*defaultRegionSize/units.MiB),
	)
	mc.PutStore(newStore)
}

// UpdateRegionCount updates store region count.
func (mc *Cluster) UpdateRegionCount(storeID uint64, regionCount int) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionCount)*defaultRegionSize/units.MiB),
	)
	mc.PutStore(newStore)
}

// UpdateSnapshotCount updates store snapshot count.
func (mc *Cluster) UpdateSnapshotCount(storeID uint64, snapshotCount int) {
	mc.updateStorageStatistics(storeID, func(newStats *pdpb.StoreStats) {
		newStats.ReceivingSnapCount = uint32(snapshotCount)
	})
}

// UpdatePendingPeerCount updates store pending peer count.
func (mc *Cluster) UpdatePendingPeerCount(storeID uint64, pendingPeerCount int) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetPendingPeerCount(pendingPeerCount))
	mc.PutStore(newStore)
}

// UpdateWitnessCount updates store witness count.
func (mc *Cluster) UpdateWitnessCount(storeID uint64, witnessCount int) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetWitnessCount(witnessCount),
	)
	mc.PutStore(newStore)
}

// UpdateStorageRatio updates store storage ratio count.
func (mc *Cluster) UpdateStorageRatio(storeID uint64, usedRatio, availableRatio float64) {
	mc.updateStorageStatistics(storeID, func(newStats *pdpb.StoreStats) {
		newStats.Capacity = defaultStoreCapacity
		newStats.UsedSize = uint64(float64(newStats.Capacity) * usedRatio)
		newStats.Available = uint64(float64(newStats.Capacity) * availableRatio)
	})
}

// UpdateStorageWrittenStats updates store written bytes.
func (mc *Cluster) UpdateStorageWrittenStats(storeID, bytesWritten, keysWritten uint64) {
	mc.updateStorageStatistics(storeID, func(newStats *pdpb.StoreStats) {
		newStats.BytesWritten = bytesWritten
		newStats.KeysWritten = keysWritten
	})
}

// UpdateStorageReadStats updates store written bytes.
func (mc *Cluster) UpdateStorageReadStats(storeID, bytesRead, keysRead uint64) {
	mc.updateStorageStatistics(storeID, func(newStats *pdpb.StoreStats) {
		newStats.BytesRead = bytesRead
		newStats.KeysRead = keysRead
	})
}

// UpdateStorageWrittenBytes updates store written bytes.
func (mc *Cluster) UpdateStorageWrittenBytes(storeID uint64, bytesWritten uint64) {
	mc.updateStorageStatistics(storeID, func(newStats *pdpb.StoreStats) {
		newStats.BytesWritten = bytesWritten
		newStats.KeysWritten = bytesWritten / 100
		newStats.QueryStats = &pdpb.QueryStats{
			Put: bytesWritten / 100,
		}
	})
}

// UpdateStorageReadBytes updates store read bytes.
func (mc *Cluster) UpdateStorageReadBytes(storeID uint64, bytesRead uint64) {
	mc.updateStorageStatistics(storeID, func(newStats *pdpb.StoreStats) {
		newStats.BytesRead = bytesRead
		newStats.KeysRead = bytesRead / 100
	})
}

// UpdateStorageWrittenKeys updates store written keys.
func (mc *Cluster) UpdateStorageWrittenKeys(storeID uint64, keysWritten uint64) {
	mc.updateStorageStatistics(storeID, func(newStats *pdpb.StoreStats) {
		newStats.KeysWritten = keysWritten
		newStats.BytesWritten = keysWritten * 100
	})
}

// UpdateStorageReadKeys updates store read bytes.
func (mc *Cluster) UpdateStorageReadKeys(storeID uint64, keysRead uint64) {
	mc.updateStorageStatistics(storeID, func(newStats *pdpb.StoreStats) {
		newStats.KeysRead = keysRead
		newStats.BytesRead = keysRead * 100
	})
}

// UpdateStorageReadQuery updates store read query.
func (mc *Cluster) UpdateStorageReadQuery(storeID uint64, queryRead uint64) {
	mc.updateStorageStatistics(storeID, func(newStats *pdpb.StoreStats) {
		newStats.QueryStats = core.RandomKindReadQuery(queryRead)
		newStats.BytesRead = queryRead * 100
	})
}

// UpdateStorageWriteQuery updates store write query.
func (mc *Cluster) UpdateStorageWriteQuery(storeID uint64, queryWrite uint64) {
	mc.updateStorageStatistics(storeID, func(newStats *pdpb.StoreStats) {
		newStats.QueryStats = core.RandomKindWriteQuery(queryWrite)
		newStats.BytesWritten = queryWrite * 100
	})
}

func (mc *Cluster) updateStorageStatistics(storeID uint64, update func(*pdpb.StoreStats)) {
	store := mc.GetStore(storeID)
	newStats := typeutil.DeepClone(store.GetStoreStats(), core.StoreStatsFactory)
	update(newStats)
	now := time.Now().Unix()
	interval := &pdpb.TimeInterval{StartTimestamp: uint64(now - utils.StoreHeartBeatReportInterval), EndTimestamp: uint64(now)}
	newStats.Interval = interval
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.Set(storeID, newStats)
	mc.PutStore(newStore)
}

// UpdateStoreStatus updates store status.
func (mc *Cluster) UpdateStoreStatus(id uint64) {
	leaderCount := mc.GetStoreLeaderCount(id)
	regionCount := mc.GetStoreRegionCount(id)
	pendingPeerCount := mc.GetStorePendingPeerCount(id)
	leaderSize := mc.GetStoreLeaderRegionSize(id)
	regionSize := mc.GetStoreRegionSize(id)
	store := mc.GetStore(id)
	stats := &pdpb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.Available = stats.Capacity - uint64(store.GetRegionSize()*units.MiB)
	stats.UsedSize = uint64(store.GetRegionSize() * units.MiB)
	newStore := store.Clone(
		core.SetStoreStats(stats),
		core.SetLeaderCount(leaderCount),
		core.SetRegionCount(regionCount),
		core.SetPendingPeerCount(pendingPeerCount),
		core.SetLeaderSize(leaderSize),
		core.SetRegionSize(regionSize),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(newStore)
}

func (mc *Cluster) newMockRegionInfo(regionID uint64, leaderStoreID uint64, otherPeerStoreIDs ...uint64) *core.RegionInfo {
	var followerStoreIDs []uint64
	var learnerStoreIDs []uint64
	for _, storeID := range otherPeerStoreIDs {
		if store := mc.GetStore(storeID); store != nil && store.IsTiFlash() {
			learnerStoreIDs = append(learnerStoreIDs, storeID)
		} else {
			followerStoreIDs = append(followerStoreIDs, storeID)
		}
	}
	return mc.MockRegionInfo(regionID, leaderStoreID, followerStoreIDs, learnerStoreIDs, nil)
}

// CheckLabelProperty checks label property.
func (mc *Cluster) CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool {
	for _, cfg := range mc.GetLabelPropertyConfig()[typ] {
		for _, l := range labels {
			if l.Key == cfg.Key && l.Value == cfg.Value {
				return true
			}
		}
	}
	return false
}

// PutRegionStores mocks method.
func (mc *Cluster) PutRegionStores(id uint64, stores ...uint64) {
	meta := &metapb.Region{
		Id:       id,
		StartKey: []byte(strconv.FormatUint(id, 10)),
		EndKey:   []byte(strconv.FormatUint(id+1, 10)),
	}
	for _, s := range stores {
		meta.Peers = append(meta.Peers, &metapb.Peer{StoreId: s})
	}
	mc.PutRegion(core.NewRegionInfo(meta, &metapb.Peer{StoreId: stores[0]}))
}

// PutStoreWithLabels mocks method.
func (mc *Cluster) PutStoreWithLabels(id uint64, labelPairs ...string) {
	labels := make(map[string]string)
	for i := 0; i < len(labelPairs); i += 2 {
		labels[labelPairs[i]] = labelPairs[i+1]
	}
	mc.AddLabelsStore(id, 0, labels)
}

// MockRegionInfo returns a mock region
// If leaderStoreID is zero, the regions would have no leader
func (mc *Cluster) MockRegionInfo(regionID uint64, leaderStoreID uint64,
	followerStoreIDs, learnerStoreIDs []uint64, epoch *metapb.RegionEpoch) *core.RegionInfo {
	region := &metapb.Region{
		Id:          regionID,
		StartKey:    []byte(fmt.Sprintf("%20d0", regionID)),
		EndKey:      []byte(fmt.Sprintf("%20d0", regionID+1)),
		RegionEpoch: epoch,
	}
	var leader *metapb.Peer
	if leaderStoreID != 0 {
		leader, _ = mc.AllocPeer(leaderStoreID)
		region.Peers = append(region.Peers, leader)
	}
	for _, storeID := range followerStoreIDs {
		peer, _ := mc.AllocPeer(storeID)
		region.Peers = append(region.Peers, peer)
	}
	for _, storeID := range learnerStoreIDs {
		peer, _ := mc.AllocPeer(storeID)
		peer.Role = metapb.PeerRole_Learner
		region.Peers = append(region.Peers, peer)
	}
	return core.NewRegionInfo(region, leader)
}

// SetStoreLabel set the labels to the target store
func (mc *Cluster) SetStoreLabel(storeID uint64, labels map[string]string) {
	store := mc.GetStore(storeID)
	newLabels := make([]*metapb.StoreLabel, 0, len(labels))
	for k, v := range labels {
		newLabels = append(newLabels, &metapb.StoreLabel{Key: k, Value: v})
	}
	newStore := store.Clone(core.SetStoreLabels(newLabels))
	mc.PutStore(newStore)
}

// AddPendingProcessedRegions mock method
func (mc *Cluster) AddPendingProcessedRegions(_ bool, ids ...uint64) {
	for _, id := range ids {
		mc.pendingProcessedRegions[id] = struct{}{}
	}
}

// CheckPendingProcessedRegions only used for unit test
func (mc *Cluster) CheckPendingProcessedRegions(id uint64) bool {
	_, ok := mc.pendingProcessedRegions[id]
	return ok
}

// ResetPendingProcessedRegions only used for unit test
func (mc *Cluster) ResetPendingProcessedRegions() {
	mc.pendingProcessedRegions = map[uint64]struct{}{}
}

// GetBasicCluster mock method
func (mc *Cluster) GetBasicCluster() *core.BasicCluster {
	return mc.BasicCluster
}

// GetRegionByKey get region by key
func (mc *Cluster) GetRegionByKey(regionKey []byte) *core.RegionInfo {
	return mc.BasicCluster.GetRegionByKey(regionKey)
}

// SetStoreLastHeartbeatInterval set the last heartbeat to the target store
func (mc *Cluster) SetStoreLastHeartbeatInterval(storeID uint64, interval time.Duration) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-interval)))
	mc.PutStore(newStore)
}

// CheckRegionRead checks region read info with all peers
func (mc *Cluster) CheckRegionRead(region *core.RegionInfo) []*statistics.HotPeerStat {
	items := make([]*statistics.HotPeerStat, 0)
	expiredItems := mc.HotCache.ExpiredReadItems(region)
	items = append(items, expiredItems...)
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	return append(items, mc.HotCache.CheckReadPeerSync(region, region.GetPeers(), region.GetLoads(), interval)...)
}

// CheckRegionWrite checks region write info with all peers
func (mc *Cluster) CheckRegionWrite(region *core.RegionInfo) []*statistics.HotPeerStat {
	items := make([]*statistics.HotPeerStat, 0)
	expiredItems := mc.HotCache.ExpiredWriteItems(region)
	items = append(items, expiredItems...)
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	return append(items, mc.HotCache.CheckWritePeerSync(region, region.GetPeers(), region.GetLoads(), interval)...)
}

// CheckRegionLeaderRead checks region read info with leader peer
func (mc *Cluster) CheckRegionLeaderRead(region *core.RegionInfo) []*statistics.HotPeerStat {
	items := make([]*statistics.HotPeerStat, 0)
	expiredItems := mc.HotCache.ExpiredReadItems(region)
	items = append(items, expiredItems...)
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	return append(items, mc.HotCache.CheckReadPeerSync(region, []*metapb.Peer{region.GetLeader()}, region.GetLoads(), interval)...)
}

// ObserveRegionsStats records the current stores stats from region stats.
func (mc *Cluster) ObserveRegionsStats() {
	storeIDs, writeBytesRates, writeKeysRates := mc.BasicCluster.GetStoresWriteRate()
	mc.HotStat.ObserveRegionsStats(storeIDs, writeBytesRates, writeKeysRates)
}
