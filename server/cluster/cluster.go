// Copyright 2016 TiKV Project Authors.
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

package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/component"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/keyutil"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/core/storelimit"
	"github.com/tikv/pd/server/id"
	syncer "github.com/tikv/pd/server/region_syncer"
	"github.com/tikv/pd/server/replication"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/checker"
	"github.com/tikv/pd/server/schedule/hbstream"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/versioninfo"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

var backgroundJobInterval = 10 * time.Second

const (
	clientTimeout              = 3 * time.Second
	defaultChangedRegionsLimit = 10000
)

// Server is the interface for cluster.
type Server interface {
	GetAllocator() *id.AllocatorImpl
	GetConfig() *config.Config
	GetPersistOptions() *config.PersistOptions
	GetStorage() *core.Storage
	GetHBStreams() *hbstream.HeartbeatStreams
	GetRaftCluster() *RaftCluster
	GetBasicCluster() *core.BasicCluster
	ReplicateFileToAllMembers(ctx context.Context, name string, data []byte) error
}

// RaftCluster is used for cluster config management.
// Raft cluster key format:
// cluster 1 -> /1/raft, value is metapb.Cluster
// cluster 2 -> /2/raft
// For cluster 1
// store 1 -> /1/raft/s/1, value is metapb.Store
// region 1 -> /1/raft/r/1, value is metapb.Region
type RaftCluster struct {
	sync.RWMutex
	ctx context.Context

	running bool

	clusterID   uint64
	clusterRoot string

	// cached cluster info
	core    *core.BasicCluster
	meta    *metapb.Cluster
	opt     *config.PersistOptions
	storage *core.Storage
	id      id.Allocator
	limiter *StoreLimiter

	prepareChecker *prepareChecker
	changedRegions chan *core.RegionInfo

	labelLevelStats *statistics.LabelStatistics
	regionStats     *statistics.RegionStatistics
	storesStats     *statistics.StoresStats
	hotSpotCache    *statistics.HotCache

	coordinator      *coordinator
	suspectRegions   *cache.TTLUint64 // suspectRegions are regions that may need fix
	suspectKeyRanges *cache.TTLString // suspect key-range regions that may need fix

	wg           sync.WaitGroup
	quit         chan struct{}
	regionSyncer *syncer.RegionSyncer

	ruleManager *placement.RuleManager
	etcdClient  *clientv3.Client
	httpClient  *http.Client

	replicationMode *replication.ModeManager
	traceRegionFlow bool

	// It's used to manage components.
	componentManager *component.Manager
}

// Status saves some state information.
type Status struct {
	RaftBootstrapTime time.Time `json:"raft_bootstrap_time,omitempty"`
	IsInitialized     bool      `json:"is_initialized"`
	ReplicationStatus string    `json:"replication_status"`
}

// NewRaftCluster create a new cluster.
func NewRaftCluster(ctx context.Context, root string, clusterID uint64, regionSyncer *syncer.RegionSyncer, etcdClient *clientv3.Client, httpClient *http.Client) *RaftCluster {
	return &RaftCluster{
		ctx:          ctx,
		running:      false,
		clusterID:    clusterID,
		clusterRoot:  root,
		regionSyncer: regionSyncer,
		httpClient:   httpClient,
		etcdClient:   etcdClient,
	}
}

// LoadClusterStatus loads the cluster status.
func (c *RaftCluster) LoadClusterStatus() (*Status, error) {
	bootstrapTime, err := c.loadBootstrapTime()
	if err != nil {
		return nil, err
	}
	var isInitialized bool
	if bootstrapTime != typeutil.ZeroTime {
		isInitialized = c.isInitialized()
	}
	var replicationStatus string
	if c.replicationMode != nil {
		replicationStatus = c.replicationMode.GetReplicationStatus().String()
	}
	return &Status{
		RaftBootstrapTime: bootstrapTime,
		IsInitialized:     isInitialized,
		ReplicationStatus: replicationStatus,
	}, nil
}

func (c *RaftCluster) isInitialized() bool {
	if c.core.GetRegionCount() > 1 {
		return true
	}
	region := c.core.SearchRegion(nil)
	return region != nil &&
		len(region.GetVoters()) >= int(c.GetReplicationConfig().MaxReplicas) &&
		len(region.GetPendingPeers()) == 0
}

// GetReplicationConfig get the replication config.
func (c *RaftCluster) GetReplicationConfig() *config.ReplicationConfig {
	cfg := &config.ReplicationConfig{}
	*cfg = *c.opt.GetReplicationConfig()
	return cfg
}

// loadBootstrapTime loads the saved bootstrap time from etcd. It returns zero
// value of time.Time when there is error or the cluster is not bootstrapped
// yet.
func (c *RaftCluster) loadBootstrapTime() (time.Time, error) {
	var t time.Time
	data, err := c.storage.Load(c.storage.ClusterStatePath("raft_bootstrap_time"))
	if err != nil {
		return t, err
	}
	if data == "" {
		return t, nil
	}
	return typeutil.ParseTimestamp([]byte(data))
}

// InitCluster initializes the raft cluster.
func (c *RaftCluster) InitCluster(id id.Allocator, opt *config.PersistOptions, storage *core.Storage, basicCluster *core.BasicCluster) {
	c.core = basicCluster
	c.opt = opt
	c.storage = storage
	c.id = id
	c.labelLevelStats = statistics.NewLabelStatistics()
	c.storesStats = statistics.NewStoresStats()
	c.prepareChecker = newPrepareChecker()
	c.changedRegions = make(chan *core.RegionInfo, defaultChangedRegionsLimit)
	c.hotSpotCache = statistics.NewHotCache()
	c.suspectRegions = cache.NewIDTTL(c.ctx, time.Minute, 3*time.Minute)
	c.suspectKeyRanges = cache.NewStringTTL(c.ctx, time.Minute, 3*time.Minute)
	c.traceRegionFlow = opt.GetPDServerConfig().TraceRegionFlow
}

// Start starts a cluster.
func (c *RaftCluster) Start(s Server) error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}

	c.InitCluster(s.GetAllocator(), s.GetPersistOptions(), s.GetStorage(), s.GetBasicCluster())
	cluster, err := c.LoadClusterInfo()
	if err != nil {
		return err
	}
	if cluster == nil {
		return nil
	}

	c.ruleManager = placement.NewRuleManager(c.storage)
	if c.opt.IsPlacementRulesEnabled() {
		err = c.ruleManager.Initialize(c.opt.GetMaxReplicas(), c.opt.GetLocationLabels())
		if err != nil {
			return err
		}
	}

	c.componentManager = component.NewManager(c.storage)
	_, err = c.storage.LoadComponent(&c.componentManager)
	if err != nil {
		return err
	}

	c.replicationMode, err = replication.NewReplicationModeManager(s.GetConfig().ReplicationMode, s.GetStorage(), cluster, s)
	if err != nil {
		return err
	}

	c.coordinator = newCoordinator(c.ctx, cluster, s.GetHBStreams())
	c.regionStats = statistics.NewRegionStatistics(c.opt, c.ruleManager)
	c.limiter = NewStoreLimiter(s.GetPersistOptions())
	c.quit = make(chan struct{})

	c.wg.Add(4)
	go c.runCoordinator()
	failpoint.Inject("highFrequencyClusterJobs", func() {
		backgroundJobInterval = 100 * time.Microsecond
	})
	go c.runBackgroundJobs(backgroundJobInterval)
	go c.syncRegions()
	go c.runReplicationMode()
	c.running = true

	return nil
}

// LoadClusterInfo loads cluster related info.
func (c *RaftCluster) LoadClusterInfo() (*RaftCluster, error) {
	c.meta = &metapb.Cluster{}
	ok, err := c.storage.LoadMeta(c.meta)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	start := time.Now()
	if err := c.storage.LoadStores(c.core.PutStore); err != nil {
		return nil, err
	}
	log.Info("load stores",
		zap.Int("count", c.GetStoreCount()),
		zap.Duration("cost", time.Since(start)),
	)

	start = time.Now()

	// used to load region from kv storage to cache storage.
	if err := c.storage.LoadRegionsOnce(c.core.CheckAndPutRegion); err != nil {
		return nil, err
	}
	log.Info("load regions",
		zap.Int("count", c.core.GetRegionCount()),
		zap.Duration("cost", time.Since(start)),
	)
	for _, store := range c.GetStores() {
		c.storesStats.CreateRollingStoreStats(store.GetID())
	}
	return c, nil
}

func (c *RaftCluster) runBackgroundJobs(interval time.Duration) {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			log.Info("metrics are reset")
			c.resetMetrics()
			log.Info("background jobs has been stopped")
			return
		case <-ticker.C:
			c.checkStores()
			c.collectMetrics()
			c.coordinator.opController.PruneHistory()
		}
	}
}

func (c *RaftCluster) runCoordinator() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	defer func() {
		c.coordinator.wg.Wait()
		log.Info("coordinator has been stopped")
	}()
	c.coordinator.run()
	<-c.coordinator.ctx.Done()
	log.Info("coordinator is stopping")
}

func (c *RaftCluster) syncRegions() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	c.regionSyncer.RunServer(c.changedRegionNotifier(), c.quit)
}

func (c *RaftCluster) runReplicationMode() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	c.replicationMode.Run(c.quit)
}

// Stop stops the cluster.
func (c *RaftCluster) Stop() {
	c.Lock()

	if !c.running {
		c.Unlock()
		return
	}

	c.running = false
	close(c.quit)
	c.coordinator.stop()
	c.Unlock()
	c.wg.Wait()
}

// IsRunning return if the cluster is running.
func (c *RaftCluster) IsRunning() bool {
	c.RLock()
	defer c.RUnlock()
	return c.running
}

// GetOperatorController returns the operator controller.
func (c *RaftCluster) GetOperatorController() *schedule.OperatorController {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.opController
}

// GetRegionScatter returns the region scatter.
func (c *RaftCluster) GetRegionScatter() *schedule.RegionScatterer {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.regionScatterer
}

// GetHeartbeatStreams returns the heartbeat streams.
func (c *RaftCluster) GetHeartbeatStreams() *hbstream.HeartbeatStreams {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.hbStreams
}

// GetCoordinator returns the coordinator.
func (c *RaftCluster) GetCoordinator() *coordinator {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator
}

// GetRegionSyncer returns the region syncer.
func (c *RaftCluster) GetRegionSyncer() *syncer.RegionSyncer {
	c.RLock()
	defer c.RUnlock()
	return c.regionSyncer
}

// GetReplicationMode returns the ReplicationMode.
func (c *RaftCluster) GetReplicationMode() *replication.ModeManager {
	c.RLock()
	defer c.RUnlock()
	return c.replicationMode
}

// GetStorage returns the storage.
func (c *RaftCluster) GetStorage() *core.Storage {
	c.RLock()
	defer c.RUnlock()
	return c.storage
}

// SetStorage set the storage for test purpose.
func (c *RaftCluster) SetStorage(s *core.Storage) {
	c.Lock()
	defer c.Unlock()
	c.storage = s
}

// GetOpts returns cluster's configuration.
func (c *RaftCluster) GetOpts() *config.PersistOptions {
	return c.opt
}

// AddSuspectRegions adds regions to suspect list.
func (c *RaftCluster) AddSuspectRegions(regionIDs ...uint64) {
	c.Lock()
	defer c.Unlock()
	for _, regionID := range regionIDs {
		c.suspectRegions.Put(regionID, nil)
	}
}

// GetSuspectRegions gets all suspect regions.
func (c *RaftCluster) GetSuspectRegions() []uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.suspectRegions.GetAllID()
}

// RemoveSuspectRegion removes region from suspect list.
func (c *RaftCluster) RemoveSuspectRegion(id uint64) {
	c.Lock()
	defer c.Unlock()
	c.suspectRegions.Remove(id)
}

// AddSuspectKeyRange adds the key range with the its ruleID as the key
// The instance of each keyRange is like following format:
// [2][]byte: start key/end key
func (c *RaftCluster) AddSuspectKeyRange(start, end []byte) {
	c.Lock()
	defer c.Unlock()
	c.suspectKeyRanges.Put(keyutil.BuildKeyRangeKey(start, end), [2][]byte{start, end})
}

// PopOneSuspectKeyRange gets one suspect keyRange group.
// it would return value and true if pop success, or return empty [][2][]byte and false
// if suspectKeyRanges couldn't pop keyRange group.
func (c *RaftCluster) PopOneSuspectKeyRange() ([2][]byte, bool) {
	c.Lock()
	defer c.Unlock()
	_, value, success := c.suspectKeyRanges.Pop()
	if !success {
		return [2][]byte{}, false
	}
	v, ok := value.([2][]byte)
	if !ok {
		return [2][]byte{}, false
	}
	return v, true
}

// ClearSuspectKeyRanges clears the suspect keyRanges, only for unit test
func (c *RaftCluster) ClearSuspectKeyRanges() {
	c.Lock()
	defer c.Unlock()
	c.suspectKeyRanges.Clear()
}

// HandleStoreHeartbeat updates the store status.
func (c *RaftCluster) HandleStoreHeartbeat(stats *pdpb.StoreStats) error {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreId()
	store := c.GetStore(storeID)
	if store == nil {
		return errors.Errorf("store %v not found", storeID)
	}
	newStore := store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(time.Now()))
	if newStore.IsLowSpace(c.opt.GetLowSpaceRatio()) {
		log.Warn("store does not have enough disk space",
			zap.Uint64("store-id", newStore.GetID()),
			zap.Uint64("capacity", newStore.GetCapacity()),
			zap.Uint64("available", newStore.GetAvailable()))
	}
	if newStore.NeedPersist() && c.storage != nil {
		if err := c.storage.SaveStore(newStore.GetMeta()); err != nil {
			log.Error("failed to persist store", zap.Uint64("store-id", newStore.GetID()), errs.ZapError(err))
		} else {
			newStore = newStore.Clone(core.SetLastPersistTime(time.Now()))
		}
	}
	c.core.PutStore(newStore)
	c.storesStats.Observe(newStore.GetID(), newStore.GetStoreStats())
	c.storesStats.UpdateTotalBytesRate(c.core.GetStores)
	c.storesStats.UpdateTotalKeysRate(c.core.GetStores)
	c.storesStats.FilterUnhealthyStore(c)

	// c.limiter is nil before "start" is called
	if c.limiter != nil && c.opt.GetStoreLimitMode() == "auto" {
		c.limiter.Collect(newStore.GetStoreStats())
	}

	return nil
}

// processRegionHeartbeat updates the region information.
func (c *RaftCluster) processRegionHeartbeat(region *core.RegionInfo) error {
	c.RLock()
	origin, err := c.core.PreCheckPutRegion(region)
	if err != nil {
		c.RUnlock()
		return err
	}
	writeItems := c.CheckWriteStatus(region)
	readItems := c.CheckReadStatus(region)
	c.RUnlock()

	// Save to storage if meta is updated.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	// Mark isNew if the region in cache does not have leader.
	var saveKV, saveCache, isNew, needSync bool
	if origin == nil {
		log.Debug("insert new region",
			zap.Uint64("region-id", region.GetID()),
			logutil.ZapRedactStringer("meta-region", core.RegionToHexMeta(region.GetMeta())))
		saveKV, saveCache, isNew = true, true, true
	} else {
		r := region.GetRegionEpoch()
		o := origin.GetRegionEpoch()
		if r.GetVersion() > o.GetVersion() {
			log.Info("region Version changed",
				zap.Uint64("region-id", region.GetID()),
				logutil.ZapRedactString("detail", core.DiffRegionKeyInfo(origin, region)),
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
			saveCache, needSync = true, true
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

		if region.GetApproximateSize() != origin.GetApproximateSize() ||
			region.GetApproximateKeys() != origin.GetApproximateKeys() {
			saveCache = true
		}

		if c.traceRegionFlow && (region.GetBytesWritten() != origin.GetBytesWritten() ||
			region.GetBytesRead() != origin.GetBytesRead() ||
			region.GetKeysWritten() != origin.GetKeysWritten() ||
			region.GetKeysRead() != origin.GetKeysRead()) {
			saveCache, needSync = true, true
		}

		if region.GetReplicationStatus().GetState() != replication_modepb.RegionReplicationState_UNKNOWN &&
			(region.GetReplicationStatus().GetState() != origin.GetReplicationStatus().GetState() ||
				region.GetReplicationStatus().GetStateId() != origin.GetReplicationStatus().GetStateId()) {
			saveCache = true
		}
	}

	if len(writeItems) == 0 && len(readItems) == 0 && !saveKV && !saveCache && !isNew {
		return nil
	}

	failpoint.Inject("concurrentRegionHeartbeat", func() {
		time.Sleep(500 * time.Millisecond)
	})

	c.Lock()
	if saveCache {
		// To prevent a concurrent heartbeat of another region from overriding the up-to-date region info by a stale one,
		// check its validation again here.
		//
		// However it can't solve the race condition of concurrent heartbeats from the same region.
		if _, err := c.core.PreCheckPutRegion(region); err != nil {
			c.Unlock()
			return err
		}
		overlaps := c.core.PutRegion(region)
		if c.storage != nil {
			for _, item := range overlaps {
				if err := c.storage.DeleteRegion(item.GetMeta()); err != nil {
					log.Error("failed to delete region from storage",
						zap.Uint64("region-id", item.GetID()),
						logutil.ZapRedactStringer("region-meta", core.RegionToHexMeta(item.GetMeta())),
						errs.ZapError(err))
				}
			}
		}
		for _, item := range overlaps {
			if c.regionStats != nil {
				c.regionStats.ClearDefunctRegion(item.GetID())
			}
			c.labelLevelStats.ClearDefunctRegion(item.GetID(), c.opt.GetLocationLabels())
		}

		// Update related stores.
		storeMap := make(map[uint64]struct{})
		for _, p := range region.GetPeers() {
			storeMap[p.GetStoreId()] = struct{}{}
		}
		if origin != nil {
			for _, p := range origin.GetPeers() {
				storeMap[p.GetStoreId()] = struct{}{}
			}
		}
		for key := range storeMap {
			c.updateStoreStatusLocked(key)
		}
		regionEventCounter.WithLabelValues("update_cache").Inc()
	}

	if isNew {
		c.prepareChecker.collect(region)
	}

	if c.regionStats != nil {
		c.regionStats.Observe(region, c.takeRegionStoresLocked(region))
	}

	for _, writeItem := range writeItems {
		c.hotSpotCache.Update(writeItem)
	}
	for _, readItem := range readItems {
		c.hotSpotCache.Update(readItem)
	}
	c.Unlock()

	// If there are concurrent heartbeats from the same region, the last write will win even if
	// writes to storage in the critical area. So don't use mutex to protect it.
	if saveKV && c.storage != nil {
		if err := c.storage.SaveRegion(region.GetMeta()); err != nil {
			// Not successfully saved to storage is not fatal, it only leads to longer warm-up
			// after restart. Here we only log the error then go on updating cache.
			log.Error("failed to save region to storage",
				zap.Uint64("region-id", region.GetID()),
				logutil.ZapRedactStringer("region-meta", core.RegionToHexMeta(region.GetMeta())),
				errs.ZapError(err))
		}
		regionEventCounter.WithLabelValues("update_kv").Inc()
	}
	if saveKV || needSync {
		select {
		case c.changedRegions <- region:
		default:
		}
	}

	return nil
}

func (c *RaftCluster) updateStoreStatusLocked(id uint64) {
	leaderCount := c.core.GetStoreLeaderCount(id)
	regionCount := c.core.GetStoreRegionCount(id)
	pendingPeerCount := c.core.GetStorePendingPeerCount(id)
	leaderRegionSize := c.core.GetStoreLeaderRegionSize(id)
	regionSize := c.core.GetStoreRegionSize(id)
	c.core.UpdateStoreStatus(id, leaderCount, regionCount, pendingPeerCount, leaderRegionSize, regionSize)
}

//nolint:unused
func (c *RaftCluster) getClusterID() uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.meta.GetId()
}

func (c *RaftCluster) putMetaLocked(meta *metapb.Cluster) error {
	if c.storage != nil {
		if err := c.storage.SaveMeta(meta); err != nil {
			return err
		}
	}
	c.meta = meta
	return nil
}

// GetRegionByKey gets regionInfo by region key from cluster.
func (c *RaftCluster) GetRegionByKey(regionKey []byte) *core.RegionInfo {
	return c.core.SearchRegion(regionKey)
}

// GetPrevRegionByKey gets previous region and leader peer by the region key from cluster.
func (c *RaftCluster) GetPrevRegionByKey(regionKey []byte) *core.RegionInfo {
	return c.core.SearchPrevRegion(regionKey)
}

// ScanRegions scans region with start key, until the region contains endKey, or
// total number greater than limit.
func (c *RaftCluster) ScanRegions(startKey, endKey []byte, limit int) []*core.RegionInfo {
	return c.core.ScanRange(startKey, endKey, limit)
}

// GetRegion searches for a region by ID.
func (c *RaftCluster) GetRegion(regionID uint64) *core.RegionInfo {
	return c.core.GetRegion(regionID)
}

// GetMetaRegions gets regions from cluster.
func (c *RaftCluster) GetMetaRegions() []*metapb.Region {
	return c.core.GetMetaRegions()
}

// GetRegions returns all regions' information in detail.
func (c *RaftCluster) GetRegions() []*core.RegionInfo {
	return c.core.GetRegions()
}

// GetRegionCount returns total count of regions
func (c *RaftCluster) GetRegionCount() int {
	return c.core.GetRegionCount()
}

// GetStoreRegions returns all regions' information with a given storeID.
func (c *RaftCluster) GetStoreRegions(storeID uint64) []*core.RegionInfo {
	return c.core.GetStoreRegions(storeID)
}

// RandLeaderRegion returns a random region that has leader on the store.
func (c *RaftCluster) RandLeaderRegion(storeID uint64, ranges []core.KeyRange, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandLeaderRegion(storeID, ranges, opts...)
}

// RandFollowerRegion returns a random region that has a follower on the store.
func (c *RaftCluster) RandFollowerRegion(storeID uint64, ranges []core.KeyRange, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandFollowerRegion(storeID, ranges, opts...)
}

// RandPendingRegion returns a random region that has a pending peer on the store.
func (c *RaftCluster) RandPendingRegion(storeID uint64, ranges []core.KeyRange, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandPendingRegion(storeID, ranges, opts...)
}

// RandLearnerRegion returns a random region that has a learner peer on the store.
func (c *RaftCluster) RandLearnerRegion(storeID uint64, ranges []core.KeyRange, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandLearnerRegion(storeID, ranges, opts...)
}

// RandHotRegionFromStore randomly picks a hot region in specified store.
func (c *RaftCluster) RandHotRegionFromStore(store uint64, kind statistics.FlowKind) *core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	r := c.hotSpotCache.RandHotRegionFromStore(store, kind, c.opt.GetHotRegionCacheHitsThreshold())
	if r == nil {
		return nil
	}
	return c.GetRegion(r.RegionID)
}

// GetLeaderStore returns all stores that contains the region's leader peer.
func (c *RaftCluster) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	return c.core.GetLeaderStore(region)
}

// GetFollowerStores returns all stores that contains the region's follower peer.
func (c *RaftCluster) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	return c.core.GetFollowerStores(region)
}

// GetRegionStores returns all stores that contains the region's peer.
func (c *RaftCluster) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	return c.core.GetRegionStores(region)
}

// GetStoreCount returns the count of stores.
func (c *RaftCluster) GetStoreCount() int {
	return c.core.GetStoreCount()
}

// GetStoreRegionCount returns the number of regions for a given store.
func (c *RaftCluster) GetStoreRegionCount(storeID uint64) int {
	return c.core.GetStoreRegionCount(storeID)
}

// GetAverageRegionSize returns the average region approximate size.
func (c *RaftCluster) GetAverageRegionSize() int64 {
	return c.core.GetAverageRegionSize()
}

// GetRegionStats returns region statistics from cluster.
func (c *RaftCluster) GetRegionStats(startKey, endKey []byte) *statistics.RegionStats {
	c.RLock()
	defer c.RUnlock()
	return statistics.GetRegionStats(c.core.ScanRange(startKey, endKey, -1))
}

// GetStoresStats returns stores' statistics from cluster.
// And it will be unnecessary to filter unhealthy store, because it has been solved in process heartbeat
func (c *RaftCluster) GetStoresStats() *statistics.StoresStats {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats
}

// DropCacheRegion removes a region from the cache.
func (c *RaftCluster) DropCacheRegion(id uint64) {
	c.RLock()
	defer c.RUnlock()
	if region := c.GetRegion(id); region != nil {
		c.core.RemoveRegion(region)
	}
}

// GetCacheCluster gets the cached cluster.
func (c *RaftCluster) GetCacheCluster() *core.BasicCluster {
	c.RLock()
	defer c.RUnlock()
	return c.core
}

// GetMetaStores gets stores from cluster.
func (c *RaftCluster) GetMetaStores() []*metapb.Store {
	return c.core.GetMetaStores()
}

// GetStores returns all stores in the cluster.
func (c *RaftCluster) GetStores() []*core.StoreInfo {
	return c.core.GetStores()
}

// GetStore gets store from cluster.
func (c *RaftCluster) GetStore(storeID uint64) *core.StoreInfo {
	return c.core.GetStore(storeID)
}

// IsRegionHot checks if a region is in hot state.
func (c *RaftCluster) IsRegionHot(region *core.RegionInfo) bool {
	c.RLock()
	defer c.RUnlock()
	return c.hotSpotCache.IsRegionHot(region, c.opt.GetHotRegionCacheHitsThreshold())
}

// GetAdjacentRegions returns regions' information that are adjacent with the specific region ID.
func (c *RaftCluster) GetAdjacentRegions(region *core.RegionInfo) (*core.RegionInfo, *core.RegionInfo) {
	return c.core.GetAdjacentRegions(region)
}

// UpdateStoreLabels updates a store's location labels
// If 'force' is true, then update the store's labels forcibly.
func (c *RaftCluster) UpdateStoreLabels(storeID uint64, labels []*metapb.StoreLabel, force bool) error {
	store := c.GetStore(storeID)
	if store == nil {
		return errors.Errorf("invalid store ID %d, not found", storeID)
	}
	newStore := proto.Clone(store.GetMeta()).(*metapb.Store)
	newStore.Labels = labels
	// PutStore will perform label merge.
	err := c.PutStore(newStore, force)
	return err
}

// PutStore puts a store.
// If 'force' is true, then overwrite the store's labels.
func (c *RaftCluster) PutStore(store *metapb.Store, force bool) error {
	c.Lock()
	defer c.Unlock()

	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	v, err := versioninfo.ParseVersion(store.GetVersion())
	if err != nil {
		return errors.Errorf("invalid put store %v, error: %s", store, err)
	}
	clusterVersion := *c.opt.GetClusterVersion()
	if !versioninfo.IsCompatible(clusterVersion, *v) {
		return errors.Errorf("version should compatible with version  %s, got %s", clusterVersion, v)
	}

	// Store address can not be the same as other stores.
	for _, s := range c.GetStores() {
		// It's OK to start a new store on the same address if the old store has been removed.
		if s.IsTombstone() {
			continue
		}
		if s.GetID() != store.GetId() && s.GetAddress() == store.GetAddress() {
			return errors.Errorf("duplicated store address: %v, already registered by %v", store, s.GetMeta())
		}
	}

	s := c.GetStore(store.GetId())
	if s == nil {
		// Add a new store.
		s = core.NewStoreInfo(store)
	} else {
		// Use the given labels to update the store.
		labels := store.GetLabels()
		if !force {
			// If 'force' isn't set, the given labels will merge into those labels which already existed in the store.
			labels = s.MergeLabels(labels)
		}
		// Update an existed store.
		s = s.Clone(
			core.SetStoreAddress(store.Address, store.StatusAddress, store.PeerAddress),
			core.SetStoreVersion(store.GitHash, store.Version),
			core.SetStoreLabels(labels),
			core.SetStoreStartTime(store.StartTimestamp),
			core.SetStoreDeployPath(store.DeployPath),
		)
	}
	if err = c.checkStoreLabels(s); err != nil {
		return err
	}
	return c.putStoreLocked(s)
}

func (c *RaftCluster) checkStoreLabels(s *core.StoreInfo) error {
	if c.opt.IsPlacementRulesEnabled() {
		return nil
	}
	keysSet := make(map[string]struct{})
	for _, k := range c.opt.GetLocationLabels() {
		keysSet[k] = struct{}{}
		if v := s.GetLabelValue(k); len(v) == 0 {
			log.Warn("label configuration is incorrect",
				zap.Stringer("store", s.GetMeta()),
				zap.String("label-key", k))
			if c.opt.GetStrictlyMatchLabel() {
				return errors.Errorf("label configuration is incorrect, need to specify the key: %s ", k)
			}
		}
	}
	for _, label := range s.GetLabels() {
		key := label.GetKey()
		if _, ok := keysSet[key]; !ok {
			log.Warn("not found the key match with the store label",
				zap.Stringer("store", s.GetMeta()),
				zap.String("label-key", key))
			if c.opt.GetStrictlyMatchLabel() {
				return errors.Errorf("key matching the label was not found in the PD, store label key: %s ", key)
			}
		}
	}
	return nil
}

// RemoveStore marks a store as offline in cluster.
// State transition: Up -> Offline.
func (c *RaftCluster) RemoveStore(storeID uint64) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}

	// Remove an offline store should be OK, nothing to do.
	if store.IsOffline() {
		return nil
	}

	if store.IsTombstone() {
		return errs.ErrStoreTombstone.FastGenByArgs(storeID)
	}

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline))
	log.Warn("store has been offline",
		zap.Uint64("store-id", newStore.GetID()),
		zap.String("store-address", newStore.GetAddress()))
	err := c.putStoreLocked(newStore)
	if err == nil {
		c.SetStoreLimit(storeID, storelimit.RemovePeer, storelimit.Unlimited)
	}
	return err
}

// BuryStore marks a store as tombstone in cluster.
// State transition:
// Case 1: Up -> Tombstone (if force is true);
// Case 2: Offline -> Tombstone.
func (c *RaftCluster) BuryStore(storeID uint64, force bool) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}

	// Bury a tombstone store should be OK, nothing to do.
	if store.IsTombstone() {
		return nil
	}

	if store.IsUp() {
		if !force {
			return errs.ErrStoreIsUp.FastGenByArgs()
		}
		log.Warn("force bury store", zap.Stringer("store", store.GetMeta()))
	}

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	log.Warn("store has been Tombstone",
		zap.Uint64("store-id", newStore.GetID()),
		zap.String("store-address", newStore.GetAddress()))
	err := c.putStoreLocked(newStore)
	if err == nil {
		c.RemoveStoreLimit(storeID)
	}
	return err
}

// PauseLeaderTransfer prevents the store from been selected as source or
// target store of TransferLeader.
func (c *RaftCluster) PauseLeaderTransfer(storeID uint64) error {
	return c.core.PauseLeaderTransfer(storeID)
}

// ResumeLeaderTransfer cleans a store's pause state. The store can be selected
// as source or target of TransferLeader again.
func (c *RaftCluster) ResumeLeaderTransfer(storeID uint64) {
	c.core.ResumeLeaderTransfer(storeID)
}

// AttachAvailableFunc attaches an available function to a specific store.
func (c *RaftCluster) AttachAvailableFunc(storeID uint64, limitType storelimit.Type, f func() bool) {
	c.core.AttachAvailableFunc(storeID, limitType, f)
}

// SetStoreState sets up a store's state.
func (c *RaftCluster) SetStoreState(storeID uint64, state metapb.StoreState) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}

	newStore := store.Clone(core.SetStoreState(state))
	log.Warn("store update state",
		zap.Uint64("store-id", storeID),
		zap.Stringer("new-state", state))
	return c.putStoreLocked(newStore)
}

// SetStoreWeight sets up a store's leader/region balance weight.
func (c *RaftCluster) SetStoreWeight(storeID uint64, leaderWeight, regionWeight float64) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}

	if err := c.storage.SaveStoreWeight(storeID, leaderWeight, regionWeight); err != nil {
		return err
	}

	newStore := store.Clone(
		core.SetLeaderWeight(leaderWeight),
		core.SetRegionWeight(regionWeight),
	)

	return c.putStoreLocked(newStore)
}

func (c *RaftCluster) putStoreLocked(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.SaveStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.PutStore(store)
	c.storesStats.CreateRollingStoreStats(store.GetID())
	return nil
}

func (c *RaftCluster) checkStores() {
	var offlineStores []*metapb.Store
	var upStoreCount int
	stores := c.GetStores()
	for _, store := range stores {
		// the store has already been tombstone
		if store.IsTombstone() {
			continue
		}

		if store.IsUp() {
			if !store.IsLowSpace(c.opt.GetLowSpaceRatio()) {
				upStoreCount++
			}
			continue
		}

		offlineStore := store.GetMeta()
		// If the store is empty, it can be buried.
		regionCount := c.core.GetStoreRegionCount(offlineStore.GetId())
		if regionCount == 0 {
			if err := c.BuryStore(offlineStore.GetId(), false); err != nil {
				log.Error("bury store failed",
					zap.Stringer("store", offlineStore),
					errs.ZapError(err))
			}
		} else {
			offlineStores = append(offlineStores, offlineStore)
		}
	}

	if len(offlineStores) == 0 {
		return
	}

	// When placement rules feature is enabled. It is hard to determine required replica count precisely.
	if !c.opt.IsPlacementRulesEnabled() && upStoreCount < c.opt.GetMaxReplicas() {
		for _, offlineStore := range offlineStores {
			log.Warn("store may not turn into Tombstone, there are no extra up store has enough space to accommodate the extra replica", zap.Stringer("store", offlineStore))
		}
	}
}

// RemoveTombStoneRecords removes the tombStone Records.
func (c *RaftCluster) RemoveTombStoneRecords() error {
	c.Lock()
	defer c.Unlock()

	for _, store := range c.GetStores() {
		if store.IsTombstone() {
			// the store has already been tombstone
			err := c.deleteStoreLocked(store)
			if err != nil {
				log.Error("delete store failed",
					zap.Stringer("store", store.GetMeta()),
					errs.ZapError(err))
				return err
			}
			c.RemoveStoreLimit(store.GetID())
			log.Info("delete store succeeded",
				zap.Stringer("store", store.GetMeta()))
		}
	}
	return nil
}

func (c *RaftCluster) deleteStoreLocked(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.DeleteStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.DeleteStore(store)
	c.storesStats.RemoveRollingStoreStats(store.GetID())
	return nil
}

func (c *RaftCluster) collectMetrics() {
	statsMap := statistics.NewStoreStatisticsMap(c.opt)
	stores := c.GetStores()
	for _, s := range stores {
		statsMap.Observe(s, c.storesStats)
	}
	statsMap.Collect()

	c.coordinator.collectSchedulerMetrics()
	c.coordinator.collectHotSpotMetrics()
	c.coordinator.opController.CollectStoreLimitMetrics()
	c.collectClusterMetrics()
	c.collectHealthStatus()
}

func (c *RaftCluster) resetMetrics() {
	statsMap := statistics.NewStoreStatisticsMap(c.opt)
	statsMap.Reset()

	c.coordinator.resetSchedulerMetrics()
	c.coordinator.resetHotSpotMetrics()
	c.resetClusterMetrics()
	c.resetHealthStatus()
}

func (c *RaftCluster) collectClusterMetrics() {
	c.RLock()
	defer c.RUnlock()
	if c.regionStats == nil {
		return
	}
	c.regionStats.Collect()
	c.labelLevelStats.Collect()
	// collect hot cache metrics
	c.hotSpotCache.CollectMetrics()
}

func (c *RaftCluster) resetClusterMetrics() {
	c.RLock()
	defer c.RUnlock()
	if c.regionStats == nil {
		return
	}
	c.regionStats.Reset()
	c.labelLevelStats.Reset()
	// reset hot cache metrics
	c.hotSpotCache.ResetMetrics()
}

func (c *RaftCluster) collectHealthStatus() {
	members, err := GetMembers(c.etcdClient)
	if err != nil {
		log.Error("get members error", errs.ZapError(err))
	}
	healthy := CheckHealth(c.httpClient, members)
	for _, member := range members {
		var v float64
		if _, ok := healthy[member.GetMemberId()]; ok {
			v = 1
		}
		healthStatusGauge.WithLabelValues(member.GetName()).Set(v)
	}
}

func (c *RaftCluster) resetHealthStatus() {
	healthStatusGauge.Reset()
}

// GetRegionStatsByType gets the status of the region by types.
func (c *RaftCluster) GetRegionStatsByType(typ statistics.RegionStatisticType) []*core.RegionInfo {
	c.RLock()
	defer c.RUnlock()
	if c.regionStats == nil {
		return nil
	}
	return c.regionStats.GetRegionStatsByType(typ)
}

func (c *RaftCluster) updateRegionsLabelLevelStats(regions []*core.RegionInfo) {
	c.Lock()
	defer c.Unlock()
	for _, region := range regions {
		c.labelLevelStats.Observe(region, c.takeRegionStoresLocked(region), c.opt.GetLocationLabels())
	}
}

func (c *RaftCluster) takeRegionStoresLocked(region *core.RegionInfo) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := c.core.TakeStore(p.StoreId); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

// AllocID allocs ID.
func (c *RaftCluster) AllocID() (uint64, error) {
	return c.id.Alloc()
}

// OnStoreVersionChange changes the version of the cluster when needed.
func (c *RaftCluster) OnStoreVersionChange() {
	c.RLock()
	defer c.RUnlock()
	var minVersion *semver.Version
	stores := c.GetStores()
	for _, s := range stores {
		if s.IsTombstone() {
			continue
		}
		v := versioninfo.MustParseVersion(s.GetVersion())

		if minVersion == nil || v.LessThan(*minVersion) {
			minVersion = v
		}
	}
	clusterVersion := c.opt.GetClusterVersion()
	// If the cluster version of PD is less than the minimum version of all stores,
	// it will update the cluster version.
	failpoint.Inject("versionChangeConcurrency", func() {
		time.Sleep(500 * time.Millisecond)
	})

	if minVersion != nil && clusterVersion.LessThan(*minVersion) {
		if !c.opt.CASClusterVersion(clusterVersion, minVersion) {
			log.Error("cluster version changed by API at the same time")
		}
		err := c.opt.Persist(c.storage)
		if err != nil {
			log.Error("persist cluster version meet error", errs.ZapError(err))
		}
		log.Info("cluster version changed",
			zap.Stringer("old-cluster-version", clusterVersion),
			zap.Stringer("new-cluster-version", minVersion))
	}
}

func (c *RaftCluster) changedRegionNotifier() <-chan *core.RegionInfo {
	return c.changedRegions
}

// IsFeatureSupported checks if the feature is supported by current cluster.
func (c *RaftCluster) IsFeatureSupported(f versioninfo.Feature) bool {
	clusterVersion := *c.opt.GetClusterVersion()
	minSupportVersion := *versioninfo.MinSupportedVersion(f)
	// For features before version 5.0 (such as BatchSplit), strict version checks are performed according to the
	// original logic. But according to Semantic Versioning, specify a version MAJOR.MINOR.PATCH, PATCH is used when you
	// make backwards compatible bug fixes. In version 5.0 and later, we need to strictly comply.
	if versioninfo.IsCompatible(minSupportVersion, *versioninfo.MinSupportedVersion(versioninfo.Version4_0)) {
		return !clusterVersion.LessThan(minSupportVersion)
	}
	return versioninfo.IsCompatible(minSupportVersion, clusterVersion)
}

// GetConfig gets config from cluster.
func (c *RaftCluster) GetConfig() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()
	return proto.Clone(c.meta).(*metapb.Cluster)
}

// PutConfig puts config into cluster.
func (c *RaftCluster) PutConfig(meta *metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()
	if meta.GetId() != c.clusterID {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, c.clusterID)
	}
	return c.putMetaLocked(proto.Clone(meta).(*metapb.Cluster))
}

// GetMergeChecker returns merge checker.
func (c *RaftCluster) GetMergeChecker() *checker.MergeChecker {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.checkers.GetMergeChecker()
}

// GetComponentManager returns component manager.
func (c *RaftCluster) GetComponentManager() *component.Manager {
	c.RLock()
	defer c.RUnlock()
	return c.componentManager
}

// isPrepared if the cluster information is collected
func (c *RaftCluster) isPrepared() bool {
	c.RLock()
	defer c.RUnlock()
	return c.prepareChecker.check(c)
}

// GetStoresBytesWriteStat returns the bytes write stat of all StoreInfo.
func (c *RaftCluster) GetStoresBytesWriteStat() map[uint64]float64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresBytesWriteStat()
}

// GetStoresBytesReadStat returns the bytes read stat of all StoreInfo.
func (c *RaftCluster) GetStoresBytesReadStat() map[uint64]float64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresBytesReadStat()
}

// GetStoresKeysWriteStat returns the bytes write stat of all StoreInfo.
func (c *RaftCluster) GetStoresKeysWriteStat() map[uint64]float64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresKeysWriteStat()
}

// GetStoresKeysReadStat returns the bytes read stat of all StoreInfo.
func (c *RaftCluster) GetStoresKeysReadStat() map[uint64]float64 {
	c.RLock()
	defer c.RUnlock()
	return c.storesStats.GetStoresKeysReadStat()
}

// RegionReadStats returns hot region's read stats.
func (c *RaftCluster) RegionReadStats() map[uint64][]*statistics.HotPeerStat {
	// RegionStats is a thread-safe method
	return c.hotSpotCache.RegionStats(statistics.ReadFlow)
}

// RegionWriteStats returns hot region's write stats.
func (c *RaftCluster) RegionWriteStats() map[uint64][]*statistics.HotPeerStat {
	// RegionStats is a thread-safe method
	return c.hotSpotCache.RegionStats(statistics.WriteFlow)
}

// CheckWriteStatus checks the write status, returns whether need update statistics and item.
func (c *RaftCluster) CheckWriteStatus(region *core.RegionInfo) []*statistics.HotPeerStat {
	return c.hotSpotCache.CheckWrite(region, c.storesStats)
}

// CheckReadStatus checks the read status, returns whether need update statistics and item.
func (c *RaftCluster) CheckReadStatus(region *core.RegionInfo) []*statistics.HotPeerStat {
	return c.hotSpotCache.CheckRead(region, c.storesStats)
}

// TODO: remove me.
// only used in test.
//nolint:unused
func (c *RaftCluster) putRegion(region *core.RegionInfo) error {
	c.Lock()
	defer c.Unlock()
	if c.storage != nil {
		if err := c.storage.SaveRegion(region.GetMeta()); err != nil {
			return err
		}
	}
	c.core.PutRegion(region)
	return nil
}

// GetRuleManager returns the rule manager reference.
func (c *RaftCluster) GetRuleManager() *placement.RuleManager {
	c.RLock()
	defer c.RUnlock()
	return c.ruleManager
}

// FitRegion tries to fit the region with placement rules.
func (c *RaftCluster) FitRegion(region *core.RegionInfo) *placement.RegionFit {
	return c.GetRuleManager().FitRegion(c, region)
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
func (checker *prepareChecker) check(c *RaftCluster) bool {
	if checker.isPrepared || time.Since(checker.start) > collectTimeout {
		return true
	}
	// The number of active regions should be more than total region of all stores * collectFactor
	if float64(c.core.GetRegionCount())*collectFactor > float64(checker.sum) {
		return false
	}
	for _, store := range c.GetStores() {
		if !store.IsUp() {
			continue
		}
		storeID := store.GetID()
		// For each store, the number of active regions should be more than total region of the store * collectFactor
		if float64(c.core.GetStoreRegionCount(storeID))*collectFactor > float64(checker.reactiveRegions[storeID]) {
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

// GetHotWriteRegions gets hot write regions' info.
func (c *RaftCluster) GetHotWriteRegions() *statistics.StoreHotPeersInfos {
	c.RLock()
	co := c.coordinator
	c.RUnlock()
	return co.getHotWriteRegions()
}

// GetHotReadRegions gets hot read regions' info.
func (c *RaftCluster) GetHotReadRegions() *statistics.StoreHotPeersInfos {
	c.RLock()
	co := c.coordinator
	c.RUnlock()
	return co.getHotReadRegions()
}

// GetSchedulers gets all schedulers.
func (c *RaftCluster) GetSchedulers() []string {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.getSchedulers()
}

// GetSchedulerHandlers gets all scheduler handlers.
func (c *RaftCluster) GetSchedulerHandlers() map[string]http.Handler {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.getSchedulerHandlers()
}

// AddScheduler adds a scheduler.
func (c *RaftCluster) AddScheduler(scheduler schedule.Scheduler, args ...string) error {
	c.Lock()
	defer c.Unlock()
	return c.coordinator.addScheduler(scheduler, args...)
}

// RemoveScheduler removes a scheduler.
func (c *RaftCluster) RemoveScheduler(name string) error {
	c.Lock()
	defer c.Unlock()
	return c.coordinator.removeScheduler(name)
}

// PauseOrResumeScheduler pauses or resumes a scheduler.
func (c *RaftCluster) PauseOrResumeScheduler(name string, t int64) error {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.pauseOrResumeScheduler(name, t)
}

// IsSchedulerPaused checks if a scheduler is paused.
func (c *RaftCluster) IsSchedulerPaused(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.isSchedulerPaused(name)
}

// GetStoreLimiter returns the dynamic adjusting limiter
func (c *RaftCluster) GetStoreLimiter() *StoreLimiter {
	return c.limiter
}

// GetStoreLimitByType returns the store limit for a given store ID and type.
func (c *RaftCluster) GetStoreLimitByType(storeID uint64, typ storelimit.Type) float64 {
	return c.opt.GetStoreLimitByType(storeID, typ)
}

// GetAllStoresLimit returns all store limit
func (c *RaftCluster) GetAllStoresLimit() map[uint64]config.StoreLimitConfig {
	return c.opt.GetAllStoresLimit()
}

// AddStoreLimit add a store limit for a given store ID.
func (c *RaftCluster) AddStoreLimit(store *metapb.Store) {
	cfg := c.opt.GetScheduleConfig().Clone()
	sc := config.StoreLimitConfig{
		AddPeer:    config.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer),
		RemovePeer: config.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer),
	}
	if core.IsTiFlashStore(store) {
		sc = config.StoreLimitConfig{
			AddPeer:    config.DefaultTiFlashStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer),
			RemovePeer: config.DefaultTiFlashStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer),
		}
	}
	storeID := store.GetId()
	cfg.StoreLimit[storeID] = sc
	c.opt.SetScheduleConfig(cfg)
}

// RemoveStoreLimit remove a store limit for a given store ID.
func (c *RaftCluster) RemoveStoreLimit(storeID uint64) {
	cfg := c.opt.GetScheduleConfig().Clone()
	for _, limitType := range storelimit.TypeNameValue {
		c.AttachAvailableFunc(storeID, limitType, nil)
	}
	delete(cfg.StoreLimit, storeID)
	c.opt.SetScheduleConfig(cfg)
}

// SetStoreLimit sets a store limit for a given type and rate.
func (c *RaftCluster) SetStoreLimit(storeID uint64, typ storelimit.Type, ratePerMin float64) {
	c.opt.SetStoreLimit(storeID, typ, ratePerMin)
}

// SetAllStoresLimit sets all store limit for a given type and rate.
func (c *RaftCluster) SetAllStoresLimit(typ storelimit.Type, ratePerMin float64) {
	c.opt.SetAllStoresLimit(typ, ratePerMin)
}

// GetClusterVersion returns the current cluster version.
func (c *RaftCluster) GetClusterVersion() string {
	return c.opt.GetClusterVersion().String()
}

// GetEtcdClient returns the current etcd client
func (c *RaftCluster) GetEtcdClient() *clientv3.Client {
	return c.etcdClient
}

var healthURL = "/pd/api/v1/ping"

// CheckHealth checks if members are healthy.
func CheckHealth(client *http.Client, members []*pdpb.Member) map[uint64]*pdpb.Member {
	healthMembers := make(map[uint64]*pdpb.Member)
	for _, member := range members {
		for _, cURL := range member.ClientUrls {
			ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s%s", cURL, healthURL), nil)
			if err != nil {
				log.Error("failed to new request", errs.ZapError(errs.ErrNewHTTPRequest, err))
				cancel()
				continue
			}

			resp, err := client.Do(req)
			if resp != nil {
				resp.Body.Close()
			}
			cancel()
			if err == nil && resp.StatusCode == http.StatusOK {
				healthMembers[member.GetMemberId()] = member
				break
			}
		}
	}
	return healthMembers
}

// GetMembers return a slice of Members.
func GetMembers(etcdClient *clientv3.Client) ([]*pdpb.Member, error) {
	listResp, err := etcdutil.ListEtcdMembers(etcdClient)
	if err != nil {
		return nil, err
	}

	members := make([]*pdpb.Member, 0, len(listResp.Members))
	for _, m := range listResp.Members {
		info := &pdpb.Member{
			Name:       m.Name,
			MemberId:   m.ID,
			ClientUrls: m.ClientURLs,
			PeerUrls:   m.PeerURLs,
		}
		members = append(members, info)
	}

	return members, nil
}

// IsClientURL returns whether addr is a ClientUrl of any member.
func IsClientURL(addr string, etcdClient *clientv3.Client) bool {
	members, err := GetMembers(etcdClient)
	if err != nil {
		return false
	}
	for _, member := range members {
		for _, u := range member.GetClientUrls() {
			if u == addr {
				return true
			}
		}
	}
	return false
}
