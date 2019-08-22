// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/statistics"
	"go.uber.org/zap"
)

type clusterInfo struct {
	sync.RWMutex
	core            *core.BasicCluster
	id              core.IDAllocator
	kv              *core.KV
	meta            *metapb.Cluster
	opt             *scheduleOption
	regionStats     *statistics.RegionStatistics
	labelLevelStats *statistics.LabelLevelStatistics
	storesStats     *statistics.StoresStats
	prepareChecker  *prepareChecker
	changedRegions  chan *core.RegionInfo
	hotSpotCache    *statistics.HotSpotCache
}

var defaultChangedRegionsLimit = 10000

func newClusterInfo(id core.IDAllocator, opt *scheduleOption, kv *core.KV) *clusterInfo {
	return &clusterInfo{
		core:            core.NewBasicCluster(),
		id:              id,
		opt:             opt,
		kv:              kv,
		labelLevelStats: statistics.NewLabelLevelStatistics(),
		storesStats:     statistics.NewStoresStats(),
		prepareChecker:  newPrepareChecker(),
		changedRegions:  make(chan *core.RegionInfo, defaultChangedRegionsLimit),
		hotSpotCache:    statistics.NewHotSpotCache(),
	}
}

// Return nil if cluster is not bootstrapped.
func loadClusterInfo(id core.IDAllocator, kv *core.KV, opt *scheduleOption) (*clusterInfo, error) {
	c := newClusterInfo(id, opt, kv)

	c.meta = &metapb.Cluster{}
	ok, err := kv.LoadMeta(c.meta)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	start := time.Now()
	if err := kv.LoadStores(c.core.Stores); err != nil {
		return nil, err
	}
	log.Info("load stores",
		zap.Int("count", c.core.Stores.GetStoreCount()),
		zap.Duration("cost", time.Since(start)),
	)

	start = time.Now()
	if err := kv.LoadRegions(c.core.Regions); err != nil {
		return nil, err
	}
	log.Info("load regions",
		zap.Int("count", c.core.Regions.GetRegionCount()),
		zap.Duration("cost", time.Since(start)),
	)
	for _, store := range c.core.Stores.GetStores() {
		c.storesStats.CreateRollingStoreStats(store.GetID())
	}
	return c, nil
}

func (c *clusterInfo) OnStoreVersionChange() {
	var (
		minVersion     *semver.Version
		clusterVersion semver.Version
	)

	clusterVersion = c.opt.loadClusterVersion()
	stores := c.GetStores()
	for _, s := range stores {
		if s.IsTombstone() {
			continue
		}
		v := MustParseVersion(s.GetVersion())

		if minVersion == nil || v.LessThan(*minVersion) {
			minVersion = v
		}
	}
	// If the cluster version of PD is less than the minimum version of all stores,
	// it will update the cluster version.
	if clusterVersion.LessThan(*minVersion) {
		c.opt.SetClusterVersion(*minVersion)
		err := c.opt.persist(c.kv)
		if err != nil {
			log.Error("persist cluster version meet error", zap.Error(err))
		}
		log.Info("cluster version changed",
			zap.Stringer("old-cluster-version", clusterVersion),
			zap.Stringer("new-cluster-version", minVersion))
		CheckPDVersion(c.opt)
	}
}

func (c *clusterInfo) changedRegionNotifier() <-chan *core.RegionInfo {
	return c.changedRegions
}

// IsFeatureSupported checks if the feature is supported by current cluster.
func (c *clusterInfo) IsFeatureSupported(f Feature) bool {
	clusterVersion := c.opt.loadClusterVersion()
	minSupportVersion := MinSupportedVersion(f)
	return !clusterVersion.LessThan(minSupportVersion)
}

func (c *clusterInfo) allocID() (uint64, error) {
	return c.id.Alloc()
}

// AllocPeer allocs a new peer on a store.
func (c *clusterInfo) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := c.allocID()
	if err != nil {
		log.Error("failed to alloc peer", zap.Error(err))
		return nil, err
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

func (c *clusterInfo) getClusterID() uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.meta.GetId()
}

func (c *clusterInfo) getMeta() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()
	return proto.Clone(c.meta).(*metapb.Cluster)
}

func (c *clusterInfo) putMeta(meta *metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()
	return c.putMetaLocked(proto.Clone(meta).(*metapb.Cluster))
}

func (c *clusterInfo) putMetaLocked(meta *metapb.Cluster) error {
	if c.kv != nil {
		if err := c.kv.SaveMeta(meta); err != nil {
			return err
		}
	}
	c.meta = meta
	return nil
}

// GetStore searches for a store by ID.
func (c *clusterInfo) GetStore(storeID uint64) *core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetStore(storeID)
}

func (c *clusterInfo) putStore(store *core.StoreInfo) error {
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(store)
}

func (c *clusterInfo) putStoreLocked(store *core.StoreInfo) error {
	if c.kv != nil {
		if err := c.kv.SaveStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.PutStore(store)
	c.storesStats.CreateRollingStoreStats(store.GetID())
	return nil
}

func (c *clusterInfo) deleteStore(store *core.StoreInfo) error {
	c.Lock()
	defer c.Unlock()
	return c.deleteStoreLocked(store)
}

func (c *clusterInfo) deleteStoreLocked(store *core.StoreInfo) error {
	if c.kv != nil {
		if err := c.kv.DeleteStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.DeleteStore(store)
	c.storesStats.RemoveRollingStoreStats(store.GetID())
	return nil
}

// BlockStore stops balancer from selecting the store.
func (c *clusterInfo) BlockStore(storeID uint64) error {
	c.Lock()
	defer c.Unlock()
	return c.core.BlockStore(storeID)
}

// UnblockStore allows balancer to select the store.
func (c *clusterInfo) UnblockStore(storeID uint64) {
	c.Lock()
	defer c.Unlock()
	c.core.UnblockStore(storeID)
}

// AttachOverloadStatus attaches the overload status to a store.
func (c *clusterInfo) AttachOverloadStatus(storeID uint64, f func() bool) {
	c.Lock()
	defer c.Unlock()
	c.core.AttachOverloadStatus(storeID, f)
}

// GetStores returns all stores in the cluster.
func (c *clusterInfo) GetStores() []*core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetStores()
}

func (c *clusterInfo) getMetaStores() []*metapb.Store {
	c.RLock()
	defer c.RUnlock()
	return c.core.Stores.GetMetaStores()
}

func (c *clusterInfo) getStoreCount() int {
	c.RLock()
	defer c.RUnlock()
	return c.core.Stores.GetStoreCount()
}

func (c *clusterInfo) getStoresBytesWriteStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresBytesWriteStat()
}

func (c *clusterInfo) getStoresBytesReadStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresBytesReadStat()
}

func (c *clusterInfo) getStoresKeysWriteStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresKeysWriteStat()
}

func (c *clusterInfo) getStoresKeysReadStat() map[uint64]uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresKeysReadStat()
}

// ScanRegions scans region with start key, until number greater than limit.
func (c *clusterInfo) ScanRegions(startKey, endKey []byte, limit int) []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.ScanRange(startKey, endKey, limit)
}

// GetAdjacentRegions returns region's info that is adjacent with specific region
func (c *clusterInfo) GetAdjacentRegions(region *core.RegionInfo) (*core.RegionInfo, *core.RegionInfo) {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetAdjacentRegions(region)
}

// GetRegion searches for a region by ID.
func (c *clusterInfo) GetRegion(regionID uint64) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetRegion(regionID)
}

// IsRegionHot checks if a region is in hot state.
func (c *clusterInfo) IsRegionHot(id uint64) bool {
	c.RLock()
	defer c.RUnlock()
	return c.hotSpotCache.IsRegionHot(id, c.GetHotRegionCacheHitsThreshold())
}

// RandHotRegionFromStore randomly picks a hot region in specified store.
func (c *clusterInfo) RandHotRegionFromStore(store uint64, kind statistics.FlowKind) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	r := c.hotSpotCache.RandHotRegionFromStore(store, kind, c.GetHotRegionCacheHitsThreshold())
	if r == nil {
		return nil
	}
	return c.core.GetRegion(r.RegionID)
}

func (c *clusterInfo) searchRegion(regionKey []byte) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.SearchRegion(regionKey)
}

func (c *clusterInfo) searchPrevRegion(regionKey []byte) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.SearchPrevRegion(regionKey)
}

func (c *clusterInfo) putRegion(region *core.RegionInfo) error {
	c.Lock()
	defer c.Unlock()
	return c.putRegionLocked(region)
}

func (c *clusterInfo) putRegionLocked(region *core.RegionInfo) error {
	if c.kv != nil {
		if err := c.kv.SaveRegion(region.GetMeta()); err != nil {
			return err
		}
	}
	c.core.PutRegion(region)
	return nil
}

func (c *clusterInfo) getRegions() []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.GetRegions()
}

func (c *clusterInfo) getStoreRegions(storeID uint64) []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.GetStoreRegions(storeID)
}

func (c *clusterInfo) getMetaRegions() []*metapb.Region {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.GetMetaRegions()
}

func (c *clusterInfo) getRegionCount() int {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.GetRegionCount()
}

func (c *clusterInfo) getRegionStats(startKey, endKey []byte) *statistics.RegionStats {
	c.RLock()
	defer c.RUnlock()
	return statistics.GetRegionStats(c.core.Regions, startKey, endKey)
}

func (c *clusterInfo) dropRegion(id uint64) {
	c.Lock()
	defer c.Unlock()
	if region := c.core.GetRegion(id); region != nil {
		c.core.Regions.RemoveRegion(region)
	}
}

func (c *clusterInfo) GetStoreRegionCount(storeID uint64) int {
	c.RLock()
	defer c.RUnlock()
	return c.core.Regions.GetStoreRegionCount(storeID)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (c *clusterInfo) RandLeaderRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.RandLeaderRegion(storeID, opts...)
}

// RandFollowerRegion returns a random region that has a follower on the store.
func (c *clusterInfo) RandFollowerRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.RandFollowerRegion(storeID, opts...)
}

// RandPendingRegion returns a random region that has a pending peer on the store.
func (c *clusterInfo) RandPendingRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.RandPendingRegion(storeID, opts...)
}

// GetAverageRegionSize returns the average region approximate size.
func (c *clusterInfo) GetAverageRegionSize() int64 {
	c.RLock()
	defer c.RUnlock()
	return c.core.GetAverageRegionSize()
}

// GetRegionStores returns all stores that contains the region's peer.
func (c *clusterInfo) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	return c.getRegionStoresLocked(region)
}

func (c *clusterInfo) getRegionStoresLocked(region *core.RegionInfo) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := c.core.Stores.GetStore(p.StoreId); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

func (c *clusterInfo) takeRegionStoresLocked(region *core.RegionInfo) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := c.core.Stores.TakeStore(p.StoreId); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// GetLeaderStore returns all stores that contains the region's leader peer.
func (c *clusterInfo) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	return c.core.Stores.GetStore(region.GetLeader().GetStoreId())
}

// GetFollowerStores returns all stores that contains the region's follower peer.
func (c *clusterInfo) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	c.RLock()
	defer c.RUnlock()
	var stores []*core.StoreInfo
	for id := range region.GetFollowers() {
		if store := c.core.Stores.GetStore(id); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// isPrepared if the cluster information is collected
func (c *clusterInfo) isPrepared() bool {
	c.RLock()
	defer c.RUnlock()
	return c.prepareChecker.check(c)
}

// handleStoreHeartbeat updates the store status.
func (c *clusterInfo) handleStoreHeartbeat(stats *pdpb.StoreStats) error {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreId()
	store := c.core.Stores.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}
	newStore := store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(time.Now()))
	c.core.Stores.SetStore(newStore)
	c.storesStats.Observe(newStore.GetID(), newStore.GetStoreStats())
	c.storesStats.UpdateTotalBytesRate(c.core.Stores)
	return nil
}

func (c *clusterInfo) updateStoreStatusLocked(id uint64) {
	leaderCount := c.core.Regions.GetStoreLeaderCount(id)
	regionCount := c.core.Regions.GetStoreRegionCount(id)
	pendingPeerCount := c.core.Regions.GetStorePendingPeerCount(id)
	leaderRegionSize := c.core.Regions.GetStoreLeaderRegionSize(id)
	regionSize := c.core.Regions.GetStoreRegionSize(id)
	c.core.Stores.UpdateStoreStatusLocked(id, leaderCount, regionCount, pendingPeerCount, leaderRegionSize, regionSize)
}

// handleRegionHeartbeat updates the region information.
func (c *clusterInfo) handleRegionHeartbeat(region *core.RegionInfo) error {
	c.RLock()
	origin := c.core.Regions.GetRegion(region.GetID())
	if origin == nil {
		for _, item := range c.core.Regions.GetOverlaps(region) {
			if region.GetRegionEpoch().GetVersion() < item.GetRegionEpoch().GetVersion() {
				c.RUnlock()
				return ErrRegionIsStale(region.GetMeta(), item)
			}
		}
	}
	isWriteUpdate, writeItem := c.CheckWriteStatus(region)
	isReadUpdate, readItem := c.CheckReadStatus(region)
	c.RUnlock()

	// Save to KV if meta is updated.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	// Mark isNew if the region in cache does not have leader.
	var saveKV, saveCache, isNew bool
	if origin == nil {
		log.Debug("insert new region",
			zap.Uint64("region-id", region.GetID()),
			zap.Stringer("meta-region", core.RegionToHexMeta(region.GetMeta())),
		)
		saveKV, saveCache, isNew = true, true, true
	} else {
		r := region.GetRegionEpoch()
		o := origin.GetRegionEpoch()
		// Region meta is stale, return an error.
		if r.GetVersion() < o.GetVersion() || r.GetConfVer() < o.GetConfVer() {
			return ErrRegionIsStale(region.GetMeta(), origin.GetMeta())
		}
		if r.GetVersion() > o.GetVersion() {
			log.Info("region Version changed",
				zap.Uint64("region-id", region.GetID()),
				zap.String("detail", core.DiffRegionKeyInfo(origin, region)),
				zap.Uint64("old-version", o.GetVersion()),
				zap.Uint64("new-version", r.GetVersion()),
			)
			saveKV, saveCache = true, true
		}
		if r.GetConfVer() > o.GetConfVer() {
			log.Info("region ConfVer changed",
				zap.Uint64("region-id", region.GetID()),
				zap.String("detail", core.DiffRegionPeersInfo(origin, region)),
				zap.Uint64("old-confver", o.GetConfVer()),
				zap.Uint64("new-confver", r.GetConfVer()),
			)
			saveKV, saveCache = true, true
		}
		if region.GetLeader().GetId() != origin.GetLeader().GetId() {
			if origin.GetLeader().GetId() == 0 {
				isNew = true
			} else {
				log.Info("leader changed",
					zap.Uint64("region-id", region.GetID()),
					zap.Uint64("from", origin.GetLeader().GetStoreId()),
					zap.Uint64("to", region.GetLeader().GetStoreId()),
				)
			}
			saveCache = true
		}
		if len(region.GetDownPeers()) > 0 || len(region.GetPendingPeers()) > 0 {
			saveCache = true
		}
		if len(origin.GetDownPeers()) > 0 || len(origin.GetPendingPeers()) > 0 {
			saveCache = true
		}
		if len(region.GetPeers()) != len(origin.GetPeers()) {
			saveKV, saveCache = true, true
		}
		if region.GetApproximateSize() != origin.GetApproximateSize() {
			saveCache = true
		}
		if region.GetApproximateKeys() != origin.GetApproximateKeys() {
			saveCache = true
		}
	}

	if saveKV && c.kv != nil {
		if err := c.kv.SaveRegion(region.GetMeta()); err != nil {
			// Not successfully saved to kv is not fatal, it only leads to longer warm-up
			// after restart. Here we only log the error then go on updating cache.
			log.Error("fail to save region to kv",
				zap.Uint64("region-id", region.GetID()),
				zap.Stringer("region-meta", core.RegionToHexMeta(region.GetMeta())),
				zap.Error(err))
		}
		select {
		case c.changedRegions <- region:
		default:
		}
	}
	if !isWriteUpdate && !isReadUpdate && !saveCache && !isNew {
		return nil
	}

	c.Lock()
	defer c.Unlock()
	if isNew {
		c.prepareChecker.collect(region)
	}

	if saveCache {
		overlaps := c.core.Regions.SetRegion(region)
		if c.kv != nil {
			for _, item := range overlaps {
				if err := c.kv.DeleteRegion(item); err != nil {
					log.Error("fail to delete region from kv",
						zap.Uint64("region-id", item.GetId()),
						zap.Stringer("region-meta", core.RegionToHexMeta(item)),
						zap.Error(err))
				}
			}
		}
		for _, item := range overlaps {
			if c.regionStats != nil {
				c.regionStats.ClearDefunctRegion(item.GetId())
			}
			c.labelLevelStats.ClearDefunctRegion(item.GetId(), c.GetLocationLabels())
		}

		// Update related stores.
		if origin != nil {
			for _, p := range origin.GetPeers() {
				c.updateStoreStatusLocked(p.GetStoreId())
			}
		}
		for _, p := range region.GetPeers() {
			c.updateStoreStatusLocked(p.GetStoreId())
		}
	}

	if c.regionStats != nil {
		c.regionStats.Observe(region, c.takeRegionStoresLocked(region))
	}

	key := region.GetID()
	if isWriteUpdate {
		c.hotSpotCache.Update(key, writeItem, statistics.WriteFlow)
	}
	if isReadUpdate {
		c.hotSpotCache.Update(key, readItem, statistics.ReadFlow)
	}
	return nil
}

func (c *clusterInfo) updateRegionsLabelLevelStats(regions []*core.RegionInfo) {
	c.Lock()
	defer c.Unlock()
	for _, region := range regions {
		c.labelLevelStats.Observe(region, c.takeRegionStoresLocked(region), c.GetLocationLabels())
	}
}

func (c *clusterInfo) collectMetrics() {
	if c.regionStats == nil {
		return
	}
	c.RLock()
	defer c.RUnlock()
	c.regionStats.Collect()
	c.labelLevelStats.Collect()
	// collect hot cache metrics
	c.hotSpotCache.CollectMetrics(c.storesStats)
}

func (c *clusterInfo) resetClusterMetrics() {
	if c.regionStats == nil {
		return
	}
	c.RLock()
	defer c.RUnlock()
	c.regionStats.Reset()
	c.labelLevelStats.Reset()
	// reset hot spot cache metrics
	c.hotSpotCache.ResetMetrics()
}

func (c *clusterInfo) GetRegionStatsByType(typ statistics.RegionStatisticType) []*core.RegionInfo {
	if c.regionStats == nil {
		return nil
	}
	c.RLock()
	defer c.RUnlock()
	return c.regionStats.GetRegionStatsByType(typ)
}

func (c *clusterInfo) GetOpt() namespace.ScheduleOptions {
	return c.opt
}

func (c *clusterInfo) GetLeaderScheduleLimit() uint64 {
	return c.opt.GetLeaderScheduleLimit(namespace.DefaultNamespace)
}

func (c *clusterInfo) GetRegionScheduleLimit() uint64 {
	return c.opt.GetRegionScheduleLimit(namespace.DefaultNamespace)
}

func (c *clusterInfo) GetReplicaScheduleLimit() uint64 {
	return c.opt.GetReplicaScheduleLimit(namespace.DefaultNamespace)
}

func (c *clusterInfo) GetMergeScheduleLimit() uint64 {
	return c.opt.GetMergeScheduleLimit(namespace.DefaultNamespace)
}

func (c *clusterInfo) GetHotRegionScheduleLimit() uint64 {
	return c.opt.GetHotRegionScheduleLimit(namespace.DefaultNamespace)
}

func (c *clusterInfo) GetStoreBalanceRate() float64 {
	return c.opt.GetStoreBalanceRate()
}

func (c *clusterInfo) GetTolerantSizeRatio() float64 {
	return c.opt.GetTolerantSizeRatio()
}

func (c *clusterInfo) GetLowSpaceRatio() float64 {
	return c.opt.GetLowSpaceRatio()
}

func (c *clusterInfo) GetHighSpaceRatio() float64 {
	return c.opt.GetHighSpaceRatio()
}

func (c *clusterInfo) GetSchedulerMaxWaitingOperator() uint64 {
	return c.opt.GetSchedulerMaxWaitingOperator()
}

func (c *clusterInfo) GetMaxSnapshotCount() uint64 {
	return c.opt.GetMaxSnapshotCount()
}

func (c *clusterInfo) GetMaxPendingPeerCount() uint64 {
	return c.opt.GetMaxPendingPeerCount()
}

func (c *clusterInfo) GetMaxMergeRegionSize() uint64 {
	return c.opt.GetMaxMergeRegionSize()
}

func (c *clusterInfo) GetMaxMergeRegionKeys() uint64 {
	return c.opt.GetMaxMergeRegionKeys()
}

func (c *clusterInfo) GetSplitMergeInterval() time.Duration {
	return c.opt.GetSplitMergeInterval()
}

func (c *clusterInfo) GetEnableOneWayMerge() bool {
	return c.opt.GetEnableOneWayMerge()
}

func (c *clusterInfo) GetPatrolRegionInterval() time.Duration {
	return c.opt.GetPatrolRegionInterval()
}

func (c *clusterInfo) GetMaxStoreDownTime() time.Duration {
	return c.opt.GetMaxStoreDownTime()
}

func (c *clusterInfo) GetMaxReplicas() int {
	return c.opt.GetMaxReplicas(namespace.DefaultNamespace)
}

func (c *clusterInfo) GetLocationLabels() []string {
	return c.opt.GetLocationLabels()
}

func (c *clusterInfo) GetStrictlyMatchLabel() bool {
	return c.opt.rep.GetStrictlyMatchLabel()
}

func (c *clusterInfo) GetHotRegionCacheHitsThreshold() int {
	return c.opt.GetHotRegionCacheHitsThreshold()
}

func (c *clusterInfo) IsRaftLearnerEnabled() bool {
	if !c.IsFeatureSupported(RaftLearner) {
		return false
	}
	return c.opt.IsRaftLearnerEnabled()
}

func (c *clusterInfo) IsRemoveDownReplicaEnabled() bool {
	return c.opt.IsRemoveDownReplicaEnabled()
}

func (c *clusterInfo) IsReplaceOfflineReplicaEnabled() bool {
	return c.opt.IsReplaceOfflineReplicaEnabled()
}

func (c *clusterInfo) IsMakeUpReplicaEnabled() bool {
	return c.opt.IsMakeUpReplicaEnabled()
}

func (c *clusterInfo) IsRemoveExtraReplicaEnabled() bool {
	return c.opt.IsRemoveExtraReplicaEnabled()
}

func (c *clusterInfo) IsLocationReplacementEnabled() bool {
	return c.opt.IsLocationReplacementEnabled()
}

func (c *clusterInfo) IsNamespaceRelocationEnabled() bool {
	return c.opt.IsNamespaceRelocationEnabled()
}

func (c *clusterInfo) CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool {
	return c.opt.CheckLabelProperty(typ, labels)
}

// RegionReadStats returns hot region's read stats.
func (c *clusterInfo) RegionReadStats() []*statistics.RegionStat {
	// RegionStats is a thread-safe method
	return c.hotSpotCache.RegionStats(statistics.ReadFlow)
}

// RegionWriteStats returns hot region's write stats.
func (c *clusterInfo) RegionWriteStats() []*statistics.RegionStat {
	// RegionStats is a thread-safe method
	return c.hotSpotCache.RegionStats(statistics.WriteFlow)
}

// CheckWriteStatus checks the write status, returns whether need update statistics and item.
func (c *clusterInfo) CheckWriteStatus(region *core.RegionInfo) (bool, *statistics.RegionStat) {
	return c.hotSpotCache.CheckWrite(region, c.storesStats)
}

// CheckReadStatus checks the read status, returns whether need update statistics and item.
func (c *clusterInfo) CheckReadStatus(region *core.RegionInfo) (bool, *statistics.RegionStat) {
	return c.hotSpotCache.CheckRead(region, c.storesStats)
}

type prepareChecker struct {
	reactiveRegions map[uint64]int
	start           time.Time
	sum             int
	isPrepared      bool
}

func newPrepareChecker() *prepareChecker {
	return &prepareChecker{
		start:           time.Now(),
		reactiveRegions: make(map[uint64]int),
	}
}

// Before starting up the scheduler, we need to take the proportion of the regions on each store into consideration.
func (checker *prepareChecker) check(c *clusterInfo) bool {
	if checker.isPrepared || time.Since(checker.start) > collectTimeout {
		return true
	}
	// The number of active regions should be more than total region of all stores * collectFactor
	if float64(c.core.Regions.Length())*collectFactor > float64(checker.sum) {
		return false
	}
	for _, store := range c.core.GetStores() {
		if !store.IsUp() {
			continue
		}
		storeID := store.GetID()
		// For each store, the number of active regions should be more than total region of the store * collectFactor
		if float64(c.core.Regions.GetStoreRegionCount(storeID))*collectFactor > float64(checker.reactiveRegions[storeID]) {
			return false
		}
	}
	checker.isPrepared = true
	return true
}

func (checker *prepareChecker) collect(region *core.RegionInfo) {
	for _, p := range region.GetPeers() {
		checker.reactiveRegions[p.GetStoreId()]++
	}
	checker.sum++
}
