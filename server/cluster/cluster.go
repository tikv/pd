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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/netutil"
	"github.com/tikv/pd/pkg/progress"
	"github.com/tikv/pd/pkg/syncutil"
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
	"github.com/tikv/pd/server/schedule/labeler"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/schedulers"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/statistics/buckets"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/versioninfo"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

var (
	// DefaultMinResolvedTSPersistenceInterval is the default value of min resolved ts persistence interval.
	DefaultMinResolvedTSPersistenceInterval = 10 * time.Second
)

// regionLabelGCInterval is the interval to run region-label's GC work.
const regionLabelGCInterval = time.Hour

const (
	// nodeStateCheckJobInterval is the interval to run node state check job.
	nodeStateCheckJobInterval = 10 * time.Second
	// metricsCollectionJobInterval is the interval to run metrics collection job.
	metricsCollectionJobInterval = 10 * time.Second
	clientTimeout                = 3 * time.Second
	defaultChangedRegionsLimit   = 10000
	// persistLimitRetryTimes is used to reduce the probability of the persistent error
	// since the once the store is add or remove, we shouldn't return an error even if the store limit is failed to persist.
	persistLimitRetryTimes = 5
	persistLimitWaitTime   = 100 * time.Millisecond
	removingAction         = "removing"
	preparingAction        = "preparing"
)

// Server is the interface for cluster.
type Server interface {
	GetAllocator() id.Allocator
	GetConfig() *config.Config
	GetPersistOptions() *config.PersistOptions
	GetStorage() storage.Storage
	GetHBStreams() *hbstream.HeartbeatStreams
	GetRaftCluster() *RaftCluster
	GetBasicCluster() *core.BasicCluster
	GetMembers() ([]*pdpb.Member, error)
	ReplicateFileToMember(ctx context.Context, member *pdpb.Member, name string, data []byte) error
}

// RaftCluster is used for cluster config management.
// Raft cluster key format:
// cluster 1 -> /1/raft, value is metapb.Cluster
// cluster 2 -> /2/raft
// For cluster 1
// store 1 -> /1/raft/s/1, value is metapb.Store
// region 1 -> /1/raft/r/1, value is metapb.Region
type RaftCluster struct {
	syncutil.RWMutex
	wg sync.WaitGroup

	serverCtx context.Context
	ctx       context.Context
	cancel    context.CancelFunc

	etcdClient *clientv3.Client
	httpClient *http.Client

	running            bool
	meta               *metapb.Cluster
	storeConfigManager *config.StoreConfigManager
	storage            storage.Storage
	minResolvedTS      uint64
	// Keep the previous store limit settings when removing a store.
	prevStoreLimit map[uint64]map[storelimit.Type]float64

	// This below fields are all read-only, we cannot update itself after the raft cluster starts.
	clusterID                uint64
	id                       id.Allocator
	core                     *core.BasicCluster // cached cluster info
	opt                      *config.PersistOptions
	limiter                  *StoreLimiter
	coordinator              *coordinator
	labelLevelStats          *statistics.LabelStatistics
	regionStats              *statistics.RegionStatistics
	hotStat                  *statistics.HotStat
	hotBuckets               *buckets.HotBucketCache
	ruleManager              *placement.RuleManager
	regionLabeler            *labeler.RegionLabeler
	replicationMode          *replication.ModeManager
	unsafeRecoveryController *unsafeRecoveryController
	progressManager          *progress.Manager
	regionSyncer             *syncer.RegionSyncer
	changedRegions           chan *core.RegionInfo
}

// Status saves some state information.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type Status struct {
	RaftBootstrapTime time.Time `json:"raft_bootstrap_time,omitempty"`
	IsInitialized     bool      `json:"is_initialized"`
	ReplicationStatus string    `json:"replication_status"`
}

// NewRaftCluster create a new cluster.
func NewRaftCluster(ctx context.Context, clusterID uint64, regionSyncer *syncer.RegionSyncer, etcdClient *clientv3.Client,
	httpClient *http.Client) *RaftCluster {
	return &RaftCluster{
		serverCtx:    ctx,
		running:      false,
		clusterID:    clusterID,
		regionSyncer: regionSyncer,
		httpClient:   httpClient,
		etcdClient:   etcdClient,
	}
}

// GetStoreConfig returns the store config.
func (c *RaftCluster) GetStoreConfig() *config.StoreConfig {
	return c.storeConfigManager.GetStoreConfig()
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
	region := c.core.GetRegionByKey(nil)
	return region != nil &&
		len(region.GetVoters()) >= int(c.opt.GetReplicationConfig().MaxReplicas) &&
		len(region.GetPendingPeers()) == 0
}

// loadBootstrapTime loads the saved bootstrap time from etcd. It returns zero
// value of time.Time when there is error or the cluster is not bootstrapped yet.
func (c *RaftCluster) loadBootstrapTime() (time.Time, error) {
	var t time.Time
	data, err := c.storage.Load(endpoint.ClusterBootstrapTimeKey())
	if err != nil {
		return t, err
	}
	if data == "" {
		return t, nil
	}
	return typeutil.ParseTimestamp([]byte(data))
}

// InitCluster initializes the raft cluster.
func (c *RaftCluster) InitCluster(
	id id.Allocator,
	opt *config.PersistOptions,
	storage storage.Storage,
	basicCluster *core.BasicCluster) {
	c.core, c.opt, c.storage, c.id = basicCluster, opt, storage, id
	c.ctx, c.cancel = context.WithCancel(c.serverCtx)
	c.labelLevelStats = statistics.NewLabelStatistics()
	c.hotStat = statistics.NewHotStat(c.ctx)
	c.hotBuckets = buckets.NewBucketsCache(c.ctx)
	c.progressManager = progress.NewManager()
	c.changedRegions = make(chan *core.RegionInfo, defaultChangedRegionsLimit)
	c.prevStoreLimit = make(map[uint64]map[storelimit.Type]float64)
	c.unsafeRecoveryController = newUnsafeRecoveryController(c)
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

	c.ruleManager = placement.NewRuleManager(c.storage, c, c.GetOpts())
	if c.opt.IsPlacementRulesEnabled() {
		err = c.ruleManager.Initialize(c.opt.GetMaxReplicas(), c.opt.GetLocationLabels())
		if err != nil {
			return err
		}
	}

	c.regionLabeler, err = labeler.NewRegionLabeler(c.ctx, c.storage, regionLabelGCInterval)
	if err != nil {
		return err
	}

	c.replicationMode, err = replication.NewReplicationModeManager(s.GetConfig().ReplicationMode, c.storage, cluster, s)
	if err != nil {
		return err
	}
	c.storeConfigManager = config.NewStoreConfigManager(c.httpClient)
	c.coordinator = newCoordinator(c.ctx, cluster, s.GetHBStreams())
	c.regionStats = statistics.NewRegionStatistics(c.opt, c.ruleManager, c.storeConfigManager)
	c.limiter = NewStoreLimiter(s.GetPersistOptions())

	c.wg.Add(8)
	go c.runCoordinator()
	go c.runMetricsCollectionJob()
	go c.runNodeStateCheckJob()
	go c.runStatsBackgroundJobs()
	go c.syncRegions()
	go c.runReplicationMode()
	go c.runMinResolvedTSJob()
	go c.runSyncConfig()
	c.running = true

	return nil
}

// runSyncConfig runs the job to sync tikv config.
func (c *RaftCluster) runSyncConfig() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	stores := c.GetStores()

	syncConfig(c.storeConfigManager, stores)
	for {
		select {
		case <-c.ctx.Done():
			log.Info("sync store config job is stopped")
			return
		case <-ticker.C:
			if !syncConfig(c.storeConfigManager, stores) {
				stores = c.GetStores()
			}
		}
	}
}

func syncConfig(manager *config.StoreConfigManager, stores []*core.StoreInfo) bool {
	for index := 0; index < len(stores); index++ {
		// filter out the stores that are tiflash
		store := stores[index]
		if core.IsStoreContainLabel(store.GetMeta(), core.EngineKey, core.EngineTiFlash) {
			continue
		}

		// filter out the stores that are not up.
		if !(store.IsPreparing() || store.IsServing()) {
			continue
		}
		// it will try next store if the current store is failed.
		address := netutil.ResolveLoopBackAddr(stores[index].GetStatusAddress(), stores[index].GetAddress())
		if err := manager.ObserveConfig(address); err != nil {
			storeSyncConfigEvent.WithLabelValues(address, "fail").Inc()
			log.Debug("sync store config failed, it will try next store", zap.Error(err))
			continue
		}
		storeSyncConfigEvent.WithLabelValues(address, "succ").Inc()
		// it will only try one store.
		return true
	}
	return false
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

	c.core.ResetStores()
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
	if err := c.storage.LoadRegionsOnce(c.ctx, c.core.CheckAndPutRegion); err != nil {
		return nil, err
	}
	log.Info("load regions",
		zap.Int("count", c.core.GetRegionCount()),
		zap.Duration("cost", time.Since(start)),
	)
	for _, store := range c.GetStores() {
		storeID := store.GetID()
		c.hotStat.GetOrCreateRollingStoreStats(storeID)
	}
	return c, nil
}

func (c *RaftCluster) runMetricsCollectionJob() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(metricsCollectionJobInterval)
	failpoint.Inject("highFrequencyClusterJobs", func() {
		ticker = time.NewTicker(time.Microsecond)
	})

	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Info("metrics are reset")
			c.resetMetrics()
			log.Info("metrics collection job has been stopped")
			return
		case <-ticker.C:
			c.collectMetrics()
		}
	}
}

func (c *RaftCluster) runNodeStateCheckJob() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(nodeStateCheckJobInterval)
	failpoint.Inject("highFrequencyClusterJobs", func() {
		ticker = time.NewTicker(2 * time.Second)
	})
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Info("node state check job has been stopped")
			return
		case <-ticker.C:
			c.checkStores()
		}
	}
}

func (c *RaftCluster) runStatsBackgroundJobs() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(statistics.RegionsStatsObserveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Info("statistics background jobs has been stopped")
			return
		case <-ticker.C:
			c.hotStat.ObserveRegionsStats(c.core.GetStoresWriteRate())
		}
	}
}

func (c *RaftCluster) runCoordinator() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	c.coordinator.runUntilStop()
}

func (c *RaftCluster) syncRegions() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	c.regionSyncer.RunServer(c.ctx, c.changedRegionNotifier())
}

func (c *RaftCluster) runReplicationMode() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	c.replicationMode.Run(c.ctx)
}

// Stop stops the cluster.
func (c *RaftCluster) Stop() {
	c.Lock()

	if !c.running {
		c.Unlock()
		return
	}

	c.running = false
	c.coordinator.stop()
	c.cancel()
	c.Unlock()
	c.wg.Wait()
	log.Info("raftcluster is stopped")
}

// IsRunning return if the cluster is running.
func (c *RaftCluster) IsRunning() bool {
	c.RLock()
	defer c.RUnlock()
	return c.running
}

// Context returns the context of RaftCluster.
func (c *RaftCluster) Context() context.Context {
	c.RLock()
	defer c.RUnlock()
	if c.running {
		return c.ctx
	}
	return nil
}

// GetCoordinator returns the coordinator.
func (c *RaftCluster) GetCoordinator() *coordinator {
	return c.coordinator
}

// GetOperatorController returns the operator controller.
func (c *RaftCluster) GetOperatorController() *schedule.OperatorController {
	return c.coordinator.opController
}

// SetPrepared set the prepare check to prepared. Only for test purpose.
func (c *RaftCluster) SetPrepared() {
	c.coordinator.prepareChecker.Lock()
	defer c.coordinator.prepareChecker.Unlock()
	c.coordinator.prepareChecker.prepared = true
}

// GetRegionScatter returns the region scatter.
func (c *RaftCluster) GetRegionScatter() *schedule.RegionScatterer {
	return c.coordinator.regionScatterer
}

// GetRegionSplitter returns the region splitter
func (c *RaftCluster) GetRegionSplitter() *schedule.RegionSplitter {
	return c.coordinator.regionSplitter
}

// GetMergeChecker returns merge checker.
func (c *RaftCluster) GetMergeChecker() *checker.MergeChecker {
	return c.coordinator.checkers.GetMergeChecker()
}

// GetSchedulers gets all schedulers.
func (c *RaftCluster) GetSchedulers() []string {
	return c.coordinator.getSchedulers()
}

// GetSchedulerHandlers gets all scheduler handlers.
func (c *RaftCluster) GetSchedulerHandlers() map[string]http.Handler {
	return c.coordinator.getSchedulerHandlers()
}

// AddScheduler adds a scheduler.
func (c *RaftCluster) AddScheduler(scheduler schedule.Scheduler, args ...string) error {
	return c.coordinator.addScheduler(scheduler, args...)
}

// RemoveScheduler removes a scheduler.
func (c *RaftCluster) RemoveScheduler(name string) error {
	return c.coordinator.removeScheduler(name)
}

// PauseOrResumeScheduler pauses or resumes a scheduler.
func (c *RaftCluster) PauseOrResumeScheduler(name string, t int64) error {
	return c.coordinator.pauseOrResumeScheduler(name, t)
}

// IsSchedulerPaused checks if a scheduler is paused.
func (c *RaftCluster) IsSchedulerPaused(name string) (bool, error) {
	return c.coordinator.isSchedulerPaused(name)
}

// IsSchedulerDisabled checks if a scheduler is disabled.
func (c *RaftCluster) IsSchedulerDisabled(name string) (bool, error) {
	return c.coordinator.isSchedulerDisabled(name)
}

// IsSchedulerAllowed checks if a scheduler is allowed.
func (c *RaftCluster) IsSchedulerAllowed(name string) (bool, error) {
	return c.coordinator.isSchedulerAllowed(name)
}

// IsSchedulerExisted checks if a scheduler is existed.
func (c *RaftCluster) IsSchedulerExisted(name string) (bool, error) {
	return c.coordinator.isSchedulerExisted(name)
}

// PauseOrResumeChecker pauses or resumes checker.
func (c *RaftCluster) PauseOrResumeChecker(name string, t int64) error {
	return c.coordinator.pauseOrResumeChecker(name, t)
}

// IsCheckerPaused returns if checker is paused
func (c *RaftCluster) IsCheckerPaused(name string) (bool, error) {
	return c.coordinator.isCheckerPaused(name)
}

// GetAllocator returns cluster's id allocator.
func (c *RaftCluster) GetAllocator() id.Allocator {
	return c.id
}

// GetRegionSyncer returns the region syncer.
func (c *RaftCluster) GetRegionSyncer() *syncer.RegionSyncer {
	return c.regionSyncer
}

// GetReplicationMode returns the ReplicationMode.
func (c *RaftCluster) GetReplicationMode() *replication.ModeManager {
	return c.replicationMode
}

// GetRuleManager returns the rule manager reference.
func (c *RaftCluster) GetRuleManager() *placement.RuleManager {
	return c.ruleManager
}

// GetRegionLabeler returns the region labeler.
func (c *RaftCluster) GetRegionLabeler() *labeler.RegionLabeler {
	return c.regionLabeler
}

// GetStorage returns the storage.
func (c *RaftCluster) GetStorage() storage.Storage {
	c.RLock()
	defer c.RUnlock()
	return c.storage
}

// SetStorage set the storage for test purpose.
func (c *RaftCluster) SetStorage(s storage.Storage) {
	c.Lock()
	defer c.Unlock()
	c.storage = s
}

// GetOpts returns cluster's configuration.
// There is no need a lock since it won't changed.
func (c *RaftCluster) GetOpts() *config.PersistOptions {
	return c.opt
}

// AddSuspectRegions adds regions to suspect list.
func (c *RaftCluster) AddSuspectRegions(regionIDs ...uint64) {
	c.coordinator.checkers.AddSuspectRegions(regionIDs...)
}

// GetSuspectRegions gets all suspect regions.
func (c *RaftCluster) GetSuspectRegions() []uint64 {
	return c.coordinator.checkers.GetSuspectRegions()
}

// GetHotStat gets hot stat for test.
func (c *RaftCluster) GetHotStat() *statistics.HotStat {
	return c.hotStat
}

// RemoveSuspectRegion removes region from suspect list.
func (c *RaftCluster) RemoveSuspectRegion(id uint64) {
	c.coordinator.checkers.RemoveSuspectRegion(id)
}

// GetUnsafeRecoveryController returns the unsafe recovery controller.
func (c *RaftCluster) GetUnsafeRecoveryController() *unsafeRecoveryController {
	return c.unsafeRecoveryController
}

// AddSuspectKeyRange adds the key range with the its ruleID as the key
// The instance of each keyRange is like following format:
// [2][]byte: start key/end key
func (c *RaftCluster) AddSuspectKeyRange(start, end []byte) {
	c.coordinator.checkers.AddSuspectKeyRange(start, end)
}

// PopOneSuspectKeyRange gets one suspect keyRange group.
// it would return value and true if pop success, or return empty [][2][]byte and false
// if suspectKeyRanges couldn't pop keyRange group.
func (c *RaftCluster) PopOneSuspectKeyRange() ([2][]byte, bool) {
	return c.coordinator.checkers.PopOneSuspectKeyRange()
}

// ClearSuspectKeyRanges clears the suspect keyRanges, only for unit test
func (c *RaftCluster) ClearSuspectKeyRanges() {
	c.coordinator.checkers.ClearSuspectKeyRanges()
}

// HandleStoreHeartbeat updates the store status.
func (c *RaftCluster) HandleStoreHeartbeat(stats *pdpb.StoreStats) error {
	storeID := stats.GetStoreId()
	c.Lock()
	defer c.Unlock()
	store := c.GetStore(storeID)
	if store == nil {
		return errors.Errorf("store %v not found", storeID)
	}
	newStore := store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(time.Now()))
	if newStore.IsLowSpace(c.opt.GetLowSpaceRatio()) {
		log.Warn("store does not have enough disk space",
			zap.Uint64("store-id", storeID),
			zap.Uint64("capacity", newStore.GetCapacity()),
			zap.Uint64("available", newStore.GetAvailable()))
	}
	if newStore.NeedPersist() && c.storage != nil {
		if err := c.storage.SaveStore(newStore.GetMeta()); err != nil {
			log.Error("failed to persist store", zap.Uint64("store-id", storeID), errs.ZapError(err))
		} else {
			newStore = newStore.Clone(core.SetLastPersistTime(time.Now()))
		}
	}
	if store := c.core.GetStore(storeID); store != nil {
		statistics.UpdateStoreHeartbeatMetrics(store)
	}
	c.core.PutStore(newStore)
	c.hotStat.Observe(storeID, newStore.GetStoreStats())
	c.hotStat.FilterUnhealthyStore(c)
	reportInterval := stats.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()

	// c.limiter is nil before "start" is called
	if c.limiter != nil && c.opt.GetStoreLimitMode() == "auto" {
		c.limiter.Collect(newStore.GetStoreStats())
	}

	regions := make(map[uint64]*core.RegionInfo, len(stats.GetPeerStats()))
	for _, peerStat := range stats.GetPeerStats() {
		regionID := peerStat.GetRegionId()
		region := c.GetRegion(regionID)
		regions[regionID] = region
		if region == nil {
			log.Warn("discard hot peer stat for unknown region",
				zap.Uint64("region-id", regionID),
				zap.Uint64("store-id", storeID))
			continue
		}
		peer := region.GetStorePeer(storeID)
		if peer == nil {
			log.Warn("discard hot peer stat for unknown region peer",
				zap.Uint64("region-id", regionID),
				zap.Uint64("store-id", storeID))
			continue
		}
		readQueryNum := core.GetReadQueryNum(peerStat.GetQueryStats())
		loads := []float64{
			statistics.RegionReadBytes:  float64(peerStat.GetReadBytes()),
			statistics.RegionReadKeys:   float64(peerStat.GetReadKeys()),
			statistics.RegionReadQuery:  float64(readQueryNum),
			statistics.RegionWriteBytes: 0,
			statistics.RegionWriteKeys:  0,
			statistics.RegionWriteQuery: 0,
		}
		peerInfo := core.NewPeerInfo(peer, loads, interval)
		c.hotStat.CheckReadAsync(statistics.NewCheckPeerTask(peerInfo, region))
	}
	// Here we will compare the reported regions with the previous hot peers to decide if it is still hot.
	c.hotStat.CheckReadAsync(statistics.NewCollectUnReportedPeerTask(storeID, regions, interval))
	return nil
}

// processReportBuckets update the bucket information.
func (c *RaftCluster) processReportBuckets(buckets *metapb.Buckets) error {
	region := c.core.GetRegion(buckets.GetRegionId())
	if region == nil {
		bucketEventCounter.WithLabelValues("region_cache_miss").Inc()
		return errors.Errorf("region %v not found", buckets.GetRegionId())
	}
	// use CAS to update the bucket information.
	// the two request(A:3,B:2) get the same region and need to update the buckets.
	// the A will pass the check and set the version to 3, the B will fail because the region.bucket has changed.
	// the retry should keep the old version and the new version will be set to the region.bucket, like two requests (A:2,B:3).
	for retry := 0; retry < 3; retry++ {
		old := region.GetBuckets()
		// region should not update if the version of the buckets is less than the old one.
		if old != nil && buckets.GetVersion() <= old.GetVersion() {
			bucketEventCounter.WithLabelValues("version_not_match").Inc()
			return nil
		}
		failpoint.Inject("concurrentBucketHeartbeat", func() {
			time.Sleep(500 * time.Millisecond)
		})
		if ok := region.UpdateBuckets(buckets, old); ok {
			return nil
		}
	}
	bucketEventCounter.WithLabelValues("update_failed").Inc()
	return nil
}

// IsPrepared return true if the prepare checker is ready.
func (c *RaftCluster) IsPrepared() bool {
	return c.coordinator.prepareChecker.isPrepared()
}

var regionGuide = core.GenerateRegionGuideFunc(true)

// processRegionHeartbeat updates the region information.
func (c *RaftCluster) processRegionHeartbeat(region *core.RegionInfo) error {
	origin, err := c.core.PreCheckPutRegion(region)
	if err != nil {
		return err
	}
	region.Inherit(origin, c.storeConfigManager.GetStoreConfig().IsEnableRegionBucket())

	c.hotStat.CheckWriteAsync(statistics.NewCheckExpiredItemTask(region))
	c.hotStat.CheckReadAsync(statistics.NewCheckExpiredItemTask(region))
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	for _, peer := range region.GetPeers() {
		peerInfo := core.NewPeerInfo(peer, region.GetWriteLoads(), interval)
		c.hotStat.CheckWriteAsync(statistics.NewCheckPeerTask(peerInfo, region))
	}

	// Save to storage if meta is updated.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	// Mark isNew if the region in cache does not have leader.
	isNew, saveKV, saveCache, needSync := regionGuide(region, origin)
	if !saveKV && !saveCache && !isNew {
		// Due to some config changes need to update the region stats as well,
		// so we do some extra checks here.
		if c.regionStats != nil && c.regionStats.RegionStatsNeedUpdate(region) {
			c.regionStats.Observe(region, c.getRegionStoresLocked(region))
		}
		return nil
	}

	failpoint.Inject("concurrentRegionHeartbeat", func() {
		time.Sleep(500 * time.Millisecond)
	})

	var overlaps []*core.RegionInfo
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
		overlaps = c.core.PutRegion(region)
		for _, item := range overlaps {
			if c.regionStats != nil {
				c.regionStats.ClearDefunctRegion(item.GetID())
			}
			c.labelLevelStats.ClearDefunctRegion(item.GetID())
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

	if !c.IsPrepared() && isNew {
		c.coordinator.prepareChecker.collect(region)
	}

	if c.regionStats != nil {
		c.regionStats.Observe(region, c.getRegionStoresLocked(region))
	}

	changedRegions := c.changedRegions
	c.Unlock()

	if c.storage != nil {
		// If there are concurrent heartbeats from the same region, the last write will win even if
		// writes to storage in the critical area. So don't use mutex to protect it.
		// Not successfully saved to storage is not fatal, it only leads to longer warm-up
		// after restart. Here we only log the error then go on updating cache.
		for _, item := range overlaps {
			if err := c.storage.DeleteRegion(item.GetMeta()); err != nil {
				log.Error("failed to delete region from storage",
					zap.Uint64("region-id", item.GetID()),
					logutil.ZapRedactStringer("region-meta", core.RegionToHexMeta(item.GetMeta())),
					errs.ZapError(err))
			}
		}
		if saveKV {
			if err := c.storage.SaveRegion(region.GetMeta()); err != nil {
				log.Error("failed to save region to storage",
					zap.Uint64("region-id", region.GetID()),
					logutil.ZapRedactStringer("region-meta", core.RegionToHexMeta(region.GetMeta())),
					errs.ZapError(err))
			}
			regionEventCounter.WithLabelValues("update_kv").Inc()
		}
	}

	if saveKV || needSync {
		select {
		case changedRegions <- region:
		default:
		}
	}

	return nil
}

func (c *RaftCluster) updateStoreStatusLocked(id uint64) {
	leaderCount := c.core.GetStoreLeaderCount(id)
	regionCount := c.core.GetStoreRegionCount(id)
	witnessCount := c.core.GetStoreWitnessCount(id)
	pendingPeerCount := c.core.GetStorePendingPeerCount(id)
	leaderRegionSize := c.core.GetStoreLeaderRegionSize(id)
	regionSize := c.core.GetStoreRegionSize(id)
	c.core.UpdateStoreStatus(id, leaderCount, regionCount, pendingPeerCount, leaderRegionSize, regionSize, witnessCount)
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

// GetBasicCluster returns the basic cluster.
func (c *RaftCluster) GetBasicCluster() *core.BasicCluster {
	return c.core
}

// GetRegionByKey gets regionInfo by region key from cluster.
func (c *RaftCluster) GetRegionByKey(regionKey []byte) *core.RegionInfo {
	return c.core.GetRegionByKey(regionKey)
}

// GetPrevRegionByKey gets previous region and leader peer by the region key from cluster.
func (c *RaftCluster) GetPrevRegionByKey(regionKey []byte) *core.RegionInfo {
	return c.core.GetPrevRegionByKey(regionKey)
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

// DropCacheRegion removes a region from the cache.
func (c *RaftCluster) DropCacheRegion(id uint64) {
	c.core.RemoveRegionIfExist(id)
}

// DropCacheAllRegion removes all regions from the cache.
func (c *RaftCluster) DropCacheAllRegion() {
	c.core.ResetRegionCache()
}

// GetMetaStores gets stores from cluster.
func (c *RaftCluster) GetMetaStores() []*metapb.Store {
	return c.core.GetMetaStores()
}

// GetStores returns all stores in the cluster.
func (c *RaftCluster) GetStores() []*core.StoreInfo {
	return c.core.GetStores()
}

// GetLeaderStoreByRegionID returns the leader store of the given region.
func (c *RaftCluster) GetLeaderStoreByRegionID(regionID uint64) *core.StoreInfo {
	return c.core.GetLeaderStoreByRegionID(regionID)
}

// GetStore gets store from cluster.
func (c *RaftCluster) GetStore(storeID uint64) *core.StoreInfo {
	return c.core.GetStore(storeID)
}

// GetAdjacentRegions returns regions' information that are adjacent with the specific region ID.
func (c *RaftCluster) GetAdjacentRegions(region *core.RegionInfo) (*core.RegionInfo, *core.RegionInfo) {
	return c.core.GetAdjacentRegions(region)
}

// GetRangeHoles returns all range holes, i.e the key ranges without any region info.
func (c *RaftCluster) GetRangeHoles() [][]string {
	return c.core.GetRangeHoles()
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
	return c.putStoreImpl(newStore, force)
}

// PutStore puts a store.
func (c *RaftCluster) PutStore(store *metapb.Store) error {
	if err := c.putStoreImpl(store, false); err != nil {
		return err
	}
	c.OnStoreVersionChange()
	c.AddStoreLimit(store)
	return nil
}

// putStoreImpl puts a store.
// If 'force' is true, then overwrite the store's labels.
func (c *RaftCluster) putStoreImpl(store *metapb.Store, force bool) error {
	c.Lock()
	defer c.Unlock()

	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	if err := c.checkStoreVersion(store); err != nil {
		return err
	}

	// Store address can not be the same as other stores.
	for _, s := range c.GetStores() {
		// It's OK to start a new store on the same address if the old store has been removed or physically destroyed.
		if s.IsRemoved() || s.IsPhysicallyDestroyed() {
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
	if err := c.checkStoreLabels(s); err != nil {
		return err
	}
	return c.putStoreLocked(s)
}

func (c *RaftCluster) checkStoreVersion(store *metapb.Store) error {
	v, err := versioninfo.ParseVersion(store.GetVersion())
	if err != nil {
		return errors.Errorf("invalid put store %v, error: %s", store, err)
	}
	clusterVersion := *c.opt.GetClusterVersion()
	if !versioninfo.IsCompatible(clusterVersion, *v) {
		return errors.Errorf("version should compatible with version  %s, got %s", clusterVersion, v)
	}
	return nil
}

func (c *RaftCluster) checkStoreLabels(s *core.StoreInfo) error {
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
func (c *RaftCluster) RemoveStore(storeID uint64, physicallyDestroyed bool) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}

	// Remove an offline store should be OK, nothing to do.
	if store.IsRemoving() && store.IsPhysicallyDestroyed() == physicallyDestroyed {
		return nil
	}

	if store.IsRemoved() {
		return errs.ErrStoreRemoved.FastGenByArgs(storeID)
	}

	if store.IsPhysicallyDestroyed() {
		return errs.ErrStoreDestroyed.FastGenByArgs(storeID)
	}

	if (store.IsPreparing() || store.IsServing()) && !physicallyDestroyed {
		if err := c.checkReplicaBeforeOfflineStore(storeID); err != nil {
			return err
		}
	}

	newStore := store.Clone(core.OfflineStore(physicallyDestroyed))
	log.Warn("store has been offline",
		zap.Uint64("store-id", storeID),
		zap.String("store-address", newStore.GetAddress()),
		zap.Bool("physically-destroyed", newStore.IsPhysicallyDestroyed()))
	err := c.putStoreLocked(newStore)
	if err == nil {
		regionSize := float64(c.core.GetStoreRegionSize(storeID))
		c.resetProgress(storeID, store.GetAddress())
		c.progressManager.AddProgress(encodeRemovingProgressKey(storeID), regionSize, regionSize, nodeStateCheckJobInterval)
		// record the current store limit in memory
		c.prevStoreLimit[storeID] = map[storelimit.Type]float64{
			storelimit.AddPeer:    c.GetStoreLimitByType(storeID, storelimit.AddPeer),
			storelimit.RemovePeer: c.GetStoreLimitByType(storeID, storelimit.RemovePeer),
		}
		// TODO: if the persist operation encounters error, the "Unlimited" will be rollback.
		// And considering the store state has changed, RemoveStore is actually successful.
		_ = c.SetStoreLimit(storeID, storelimit.RemovePeer, storelimit.Unlimited)
	}
	return err
}

func (c *RaftCluster) checkReplicaBeforeOfflineStore(storeID uint64) error {
	upStores := c.getUpStores()
	expectUpStoresNum := len(upStores) - 1
	if expectUpStoresNum < c.opt.GetMaxReplicas() {
		return errs.ErrStoresNotEnough.FastGenByArgs(storeID, expectUpStoresNum, c.opt.GetMaxReplicas())
	}

	// Check if there are extra up store to store the leaders of the regions.
	evictStores := c.getEvictLeaderStores()
	if len(evictStores) < expectUpStoresNum {
		return nil
	}

	expectUpstores := make(map[uint64]bool)
	for _, UpStoreID := range upStores {
		if UpStoreID == storeID {
			continue
		}
		expectUpstores[UpStoreID] = true
	}
	evictLeaderStoresNum := 0
	for _, evictStoreID := range evictStores {
		if _, ok := expectUpstores[evictStoreID]; ok {
			evictLeaderStoresNum++
		}
	}

	// returns error if there is no store for leader.
	if evictLeaderStoresNum == len(expectUpstores) {
		return errs.ErrNoStoreForRegionLeader.FastGenByArgs(storeID)
	}

	return nil
}

func (c *RaftCluster) getEvictLeaderStores() (evictStores []uint64) {
	if c.coordinator == nil {
		return nil
	}
	handler, ok := c.coordinator.getSchedulerHandlers()[schedulers.EvictLeaderName]
	if !ok {
		return
	}
	type evictLeaderHandler interface {
		EvictStoreIDs() []uint64
	}
	h, ok := handler.(evictLeaderHandler)
	if !ok {
		return
	}
	return h.EvictStoreIDs()
}

func (c *RaftCluster) getUpStores() []uint64 {
	upStores := make([]uint64, 0)
	for _, store := range c.GetStores() {
		if store.IsUp() {
			upStores = append(upStores, store.GetID())
		}
	}
	return upStores
}

// BuryStore marks a store as tombstone in cluster.
// If forceBury is false, the store should be offlined and emptied before calling this func.
func (c *RaftCluster) BuryStore(storeID uint64, forceBury bool) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}

	// Bury a tombstone store should be OK, nothing to do.
	if store.IsRemoved() {
		return nil
	}

	if store.IsUp() {
		if !forceBury {
			return errs.ErrStoreIsUp.FastGenByArgs()
		} else if !store.IsDisconnected() {
			return errors.Errorf("The store %v is not offline nor disconnected", storeID)
		}
	}

	newStore := store.Clone(core.TombstoneStore())
	log.Warn("store has been Tombstone",
		zap.Uint64("store-id", storeID),
		zap.String("store-address", newStore.GetAddress()),
		zap.String("state", store.GetState().String()),
		zap.Bool("physically-destroyed", store.IsPhysicallyDestroyed()))
	err := c.putStoreLocked(newStore)
	c.onStoreVersionChangeLocked()
	if err == nil {
		// clean up the residual information.
		delete(c.prevStoreLimit, storeID)
		c.RemoveStoreLimit(storeID)
		c.resetProgress(storeID, store.GetAddress())
		c.hotStat.RemoveRollingStoreStats(storeID)
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

// SlowStoreEvicted marks a store as a slow store and prevents transferring
// leader to the store
func (c *RaftCluster) SlowStoreEvicted(storeID uint64) error {
	return c.core.SlowStoreEvicted(storeID)
}

// SlowStoreRecovered cleans the evicted state of a store.
func (c *RaftCluster) SlowStoreRecovered(storeID uint64) {
	c.core.SlowStoreRecovered(storeID)
}

// UpStore up a store from offline
func (c *RaftCluster) UpStore(storeID uint64) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}

	if store.IsRemoved() {
		return errs.ErrStoreRemoved.FastGenByArgs(storeID)
	}

	if store.IsPhysicallyDestroyed() {
		return errs.ErrStoreDestroyed.FastGenByArgs(storeID)
	}

	if store.IsUp() {
		return nil
	}

	options := []core.StoreCreateOption{core.UpStore()}
	// get the previous store limit recorded in memory
	limiter, exist := c.prevStoreLimit[storeID]
	if exist {
		options = append(options,
			core.ResetStoreLimit(storelimit.AddPeer, limiter[storelimit.AddPeer]),
			core.ResetStoreLimit(storelimit.RemovePeer, limiter[storelimit.RemovePeer]),
		)
	}
	newStore := store.Clone(options...)
	log.Warn("store has been up",
		zap.Uint64("store-id", storeID),
		zap.String("store-address", newStore.GetAddress()))
	err := c.putStoreLocked(newStore)
	if err == nil {
		if exist {
			// persist the store limit
			_ = c.SetStoreLimit(storeID, storelimit.AddPeer, limiter[storelimit.AddPeer])
			_ = c.SetStoreLimit(storeID, storelimit.RemovePeer, limiter[storelimit.RemovePeer])
		}
		c.resetProgress(storeID, store.GetAddress())
	}
	return err
}

// ReadyToServe change store's node state to Serving.
func (c *RaftCluster) ReadyToServe(storeID uint64) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}

	if store.IsRemoved() {
		return errs.ErrStoreRemoved.FastGenByArgs(storeID)
	}

	if store.IsPhysicallyDestroyed() {
		return errs.ErrStoreDestroyed.FastGenByArgs(storeID)
	}

	if store.IsServing() {
		return errs.ErrStoreServing.FastGenByArgs(storeID)
	}

	newStore := store.Clone(core.UpStore())
	log.Info("store has changed to serving",
		zap.Uint64("store-id", storeID),
		zap.String("store-address", newStore.GetAddress()))
	err := c.putStoreLocked(newStore)
	if err == nil {
		c.resetProgress(storeID, store.GetAddress())
	}
	return err
}

// SetStoreWeight sets up a store's leader/region balance weight.
func (c *RaftCluster) SetStoreWeight(storeID uint64, leaderWeight, regionWeight float64) error {
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
	c.hotStat.GetOrCreateRollingStoreStats(store.GetID())
	return nil
}

func (c *RaftCluster) checkStores() {
	var offlineStores []*metapb.Store
	var upStoreCount int
	stores := c.GetStores()

	for _, store := range stores {
		// the store has already been tombstone
		if store.IsRemoved() {
			continue
		}

		storeID := store.GetID()
		if store.IsPreparing() {
			if store.GetUptime() >= c.opt.GetMaxStorePreparingTime() || c.GetRegionCount() < core.InitClusterRegionThreshold {
				if err := c.ReadyToServe(storeID); err != nil {
					log.Error("change store to serving failed",
						zap.Stringer("store", store.GetMeta()),
						errs.ZapError(err))
				}
			} else if c.IsPrepared() {
				threshold := c.getThreshold(stores, store)
				log.Debug("store serving threshold", zap.Uint64("store-id", storeID), zap.Float64("threshold", threshold))
				regionSize := float64(store.GetRegionSize())
				if regionSize >= threshold {
					if err := c.ReadyToServe(storeID); err != nil {
						log.Error("change store to serving failed",
							zap.Stringer("store", store.GetMeta()),
							errs.ZapError(err))
					}
				} else {
					remaining := threshold - regionSize
					// If we add multiple stores, the total will need to be changed.
					c.progressManager.UpdateProgressTotal(encodePreparingProgressKey(storeID), threshold)
					c.updateProgress(storeID, store.GetAddress(), preparingAction, regionSize, remaining, true /* inc */)
				}
			}
		}

		if store.IsUp() {
			if !store.IsLowSpace(c.opt.GetLowSpaceRatio()) {
				upStoreCount++
			}
			continue
		}

		offlineStore := store.GetMeta()
		id := offlineStore.GetId()
		regionSize := c.core.GetStoreRegionSize(id)
		if c.IsPrepared() {
			c.updateProgress(id, store.GetAddress(), removingAction, float64(regionSize), float64(regionSize), false /* dec */)
		}
		regionCount := c.core.GetStoreRegionCount(id)
		// If the store is empty, it can be buried.
		if regionCount == 0 {
			if err := c.BuryStore(id, false); err != nil {
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

func (c *RaftCluster) getThreshold(stores []*core.StoreInfo, store *core.StoreInfo) float64 {
	start := time.Now()
	if !c.opt.IsPlacementRulesEnabled() {
		regionSize := c.core.GetRegionSizeByRange([]byte(""), []byte("")) * int64(c.opt.GetMaxReplicas())
		weight := getStoreTopoWeight(store, stores, c.opt.GetLocationLabels())
		return float64(regionSize) * weight * 0.9
	}

	keys := c.ruleManager.GetSplitKeys([]byte(""), []byte(""))
	if len(keys) == 0 {
		return c.calculateRange(stores, store, []byte(""), []byte("")) * 0.9
	}

	storeSize := 0.0
	startKey := []byte("")
	for _, key := range keys {
		endKey := key
		storeSize += c.calculateRange(stores, store, startKey, endKey)
		startKey = endKey
	}
	// the range from the last split key to the last key
	storeSize += c.calculateRange(stores, store, startKey, []byte(""))
	log.Debug("threshold calculation time", zap.Duration("cost", time.Since(start)))
	return storeSize * 0.9
}

func (c *RaftCluster) calculateRange(stores []*core.StoreInfo, store *core.StoreInfo, startKey, endKey []byte) float64 {
	var storeSize float64
	rules := c.ruleManager.GetRulesForApplyRange(startKey, endKey)
	for _, rule := range rules {
		if !placement.MatchLabelConstraints(store, rule.LabelConstraints) {
			continue
		}

		var matchStores []*core.StoreInfo
		for _, s := range stores {
			if s.IsRemoving() || s.IsRemoved() {
				continue
			}
			if placement.MatchLabelConstraints(s, rule.LabelConstraints) {
				matchStores = append(matchStores, s)
			}
		}
		regionSize := c.core.GetRegionSizeByRange(startKey, endKey) * int64(rule.Count)
		weight := getStoreTopoWeight(store, matchStores, rule.LocationLabels)
		storeSize += float64(regionSize) * weight
		log.Debug("calculate range result",
			logutil.ZapRedactString("start-key", string(core.HexRegionKey(startKey))),
			logutil.ZapRedactString("end-key", string(core.HexRegionKey(endKey))),
			zap.Uint64("store-id", store.GetID()),
			zap.String("rule", rule.String()),
			zap.Int64("region-size", regionSize),
			zap.Float64("weight", weight),
			zap.Float64("store-size", storeSize),
		)
	}
	return storeSize
}

func getStoreTopoWeight(store *core.StoreInfo, stores []*core.StoreInfo, locationLabels []string) float64 {
	topology, sameLocationStoreNum := buildTopology(store, stores, locationLabels)
	weight := 1.0
	topo := topology
	storeLabels := getSortedLabels(store.GetLabels(), locationLabels)
	for _, label := range storeLabels {
		if _, ok := topo[label.Value]; ok {
			weight /= float64(len(topo))
			topo = topo[label.Value].(map[string]interface{})
		} else {
			break
		}
	}

	return weight / sameLocationStoreNum
}

func buildTopology(s *core.StoreInfo, stores []*core.StoreInfo, locationLabels []string) (map[string]interface{}, float64) {
	topology := make(map[string]interface{})
	sameLocationStoreNum := 1.0
	for _, store := range stores {
		if store.IsServing() || store.IsPreparing() {
			updateTopology(topology, getSortedLabels(store.GetLabels(), locationLabels))
		}

		if store.GetID() == s.GetID() {
			continue
		}

		if s.CompareLocation(store, locationLabels) == -1 {
			sameLocationStoreNum++
		}
	}

	return topology, sameLocationStoreNum
}

func getSortedLabels(storeLabels []*metapb.StoreLabel, locationLabels []string) []*metapb.StoreLabel {
	var sortedLabels []*metapb.StoreLabel
	for _, ll := range locationLabels {
		find := false
		for _, sl := range storeLabels {
			if ll == sl.Key {
				sortedLabels = append(sortedLabels, sl)
				find = true
				break
			}
		}
		// TODO: we need to improve this logic to make the label calculation more accurate if the user has the wrong label settings.
		if !find {
			sortedLabels = append(sortedLabels, &metapb.StoreLabel{Key: ll, Value: ""})
		}
	}
	return sortedLabels
}

// updateTopology records stores' topology in the `topology` variable.
func updateTopology(topology map[string]interface{}, sortedLabels []*metapb.StoreLabel) {
	if len(sortedLabels) == 0 {
		return
	}
	topo := topology
	for _, l := range sortedLabels {
		if _, exist := topo[l.Value]; !exist {
			topo[l.Value] = make(map[string]interface{})
		}
		topo = topo[l.Value].(map[string]interface{})
	}
}

func (c *RaftCluster) updateProgress(storeID uint64, storeAddress, action string, current, remaining float64, isInc bool) {
	storeLabel := strconv.FormatUint(storeID, 10)
	var progress string
	switch action {
	case removingAction:
		progress = encodeRemovingProgressKey(storeID)
	case preparingAction:
		progress = encodePreparingProgressKey(storeID)
	}

	if exist := c.progressManager.AddProgress(progress, current, remaining, nodeStateCheckJobInterval); !exist {
		return
	}
	c.progressManager.UpdateProgress(progress, current, remaining, isInc)
	process, ls, cs, err := c.progressManager.Status(progress)
	if err != nil {
		log.Error("get progress status failed", zap.String("progress", progress), zap.Float64("remaining", remaining), errs.ZapError(err))
		return
	}
	storesProgressGauge.WithLabelValues(storeAddress, storeLabel, action).Set(process)
	storesSpeedGauge.WithLabelValues(storeAddress, storeLabel, action).Set(cs)
	storesETAGauge.WithLabelValues(storeAddress, storeLabel, action).Set(ls)
}

func (c *RaftCluster) resetProgress(storeID uint64, storeAddress string) {
	storeLabel := strconv.FormatUint(storeID, 10)

	progress := encodePreparingProgressKey(storeID)
	if exist := c.progressManager.RemoveProgress(progress); exist {
		storesProgressGauge.DeleteLabelValues(storeAddress, storeLabel, preparingAction)
		storesSpeedGauge.DeleteLabelValues(storeAddress, storeLabel, preparingAction)
		storesETAGauge.DeleteLabelValues(storeAddress, storeLabel, preparingAction)
	}
	progress = encodeRemovingProgressKey(storeID)
	if exist := c.progressManager.RemoveProgress(progress); exist {
		storesProgressGauge.DeleteLabelValues(storeAddress, storeLabel, removingAction)
		storesSpeedGauge.DeleteLabelValues(storeAddress, storeLabel, removingAction)
		storesETAGauge.DeleteLabelValues(storeAddress, storeLabel, removingAction)
	}
}

func encodeRemovingProgressKey(storeID uint64) string {
	return fmt.Sprintf("%s-%d", removingAction, storeID)
}

func encodePreparingProgressKey(storeID uint64) string {
	return fmt.Sprintf("%s-%d", preparingAction, storeID)
}

// RemoveTombStoneRecords removes the tombStone Records.
func (c *RaftCluster) RemoveTombStoneRecords() error {
	c.Lock()
	defer c.Unlock()

	var failedStores []uint64
	for _, store := range c.GetStores() {
		if store.IsRemoved() {
			if c.core.GetStoreRegionCount(store.GetID()) > 0 {
				log.Warn("skip removing tombstone", zap.Stringer("store", store.GetMeta()))
				failedStores = append(failedStores, store.GetID())
				continue
			}
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
	var stores string
	if len(failedStores) != 0 {
		for i, storeID := range failedStores {
			stores += fmt.Sprintf("%d", storeID)
			if i != len(failedStores)-1 {
				stores += ", "
			}
		}
		return errors.Errorf("failed stores: %v", stores)
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
	return nil
}

func (c *RaftCluster) collectMetrics() {
	statsMap := statistics.NewStoreStatisticsMap(c.opt, c.storeConfigManager.GetStoreConfig())
	stores := c.GetStores()
	for _, s := range stores {
		statsMap.Observe(s, c.hotStat.StoresStats)
	}
	statsMap.Collect()

	c.coordinator.collectSchedulerMetrics()
	c.coordinator.collectHotSpotMetrics()
	c.collectClusterMetrics()
	c.collectHealthStatus()
}

func (c *RaftCluster) resetMetrics() {
	statsMap := statistics.NewStoreStatisticsMap(c.opt, c.storeConfigManager.GetStoreConfig())
	statsMap.Reset()

	c.coordinator.resetSchedulerMetrics()
	c.coordinator.resetHotSpotMetrics()
	c.resetClusterMetrics()
	c.resetHealthStatus()
	c.resetProgressIndicator()
}

func (c *RaftCluster) collectClusterMetrics() {
	if c.regionStats == nil {
		return
	}
	c.regionStats.Collect()
	c.labelLevelStats.Collect()
	// collect hot cache metrics
	c.hotStat.CollectMetrics()
}

func (c *RaftCluster) resetClusterMetrics() {
	if c.regionStats == nil {
		return
	}
	c.regionStats.Reset()
	c.labelLevelStats.Reset()
	// reset hot cache metrics
	c.hotStat.ResetMetrics()
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

func (c *RaftCluster) resetProgressIndicator() {
	c.progressManager.Reset()
	storesProgressGauge.Reset()
	storesSpeedGauge.Reset()
	storesETAGauge.Reset()
}

// GetRegionStatsByType gets the status of the region by types.
func (c *RaftCluster) GetRegionStatsByType(typ statistics.RegionStatisticType) []*core.RegionInfo {
	if c.regionStats == nil {
		return nil
	}
	return c.regionStats.GetRegionStatsByType(typ)
}

// GetOfflineRegionStatsByType gets the status of the offline region by types.
func (c *RaftCluster) GetOfflineRegionStatsByType(typ statistics.RegionStatisticType) []*core.RegionInfo {
	if c.regionStats == nil {
		return nil
	}
	return c.regionStats.GetOfflineRegionStatsByType(typ)
}

func (c *RaftCluster) updateRegionsLabelLevelStats(regions []*core.RegionInfo) {
	for _, region := range regions {
		c.labelLevelStats.Observe(region, c.getStoresWithoutLabelLocked(region, core.EngineKey, core.EngineTiFlash), c.opt.GetLocationLabels())
	}
}

func (c *RaftCluster) getRegionStoresLocked(region *core.RegionInfo) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := c.core.GetStore(p.StoreId); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

func (c *RaftCluster) getStoresWithoutLabelLocked(region *core.RegionInfo, key, value string) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := c.core.GetStore(p.StoreId); store != nil && !core.IsStoreContainLabel(store.GetMeta(), key, value) {
			stores = append(stores, store)
		}
	}
	return stores
}

// OnStoreVersionChange changes the version of the cluster when needed.
func (c *RaftCluster) OnStoreVersionChange() {
	c.RLock()
	defer c.RUnlock()
	c.onStoreVersionChangeLocked()
}

func (c *RaftCluster) onStoreVersionChangeLocked() {
	var minVersion *semver.Version
	stores := c.GetStores()
	for _, s := range stores {
		if s.IsRemoved() {
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
	if minVersion == nil || clusterVersion.Equal(*minVersion) {
		return
	}

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

func (c *RaftCluster) changedRegionNotifier() <-chan *core.RegionInfo {
	return c.changedRegions
}

// GetMetaCluster gets meta cluster.
func (c *RaftCluster) GetMetaCluster() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()
	return proto.Clone(c.meta).(*metapb.Cluster)
}

// PutMetaCluster puts meta cluster.
func (c *RaftCluster) PutMetaCluster(meta *metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()
	if meta.GetId() != c.clusterID {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, c.clusterID)
	}
	return c.putMetaLocked(proto.Clone(meta).(*metapb.Cluster))
}

// GetRegionStats returns region statistics from cluster.
func (c *RaftCluster) GetRegionStats(startKey, endKey []byte) *statistics.RegionStats {
	return statistics.GetRegionStats(c.core.ScanRange(startKey, endKey, -1))
}

// GetStoresStats returns stores' statistics from cluster.
// And it will be unnecessary to filter unhealthy store, because it has been solved in process heartbeat
func (c *RaftCluster) GetStoresStats() *statistics.StoresStats {
	return c.hotStat.StoresStats
}

// GetStoresLoads returns load stats of all stores.
func (c *RaftCluster) GetStoresLoads() map[uint64][]float64 {
	return c.hotStat.GetStoresLoads()
}

// IsRegionHot checks if a region is in hot state.
func (c *RaftCluster) IsRegionHot(region *core.RegionInfo) bool {
	return c.hotStat.IsRegionHot(region, c.opt.GetHotRegionCacheHitsThreshold())
}

// RegionReadStats returns hot region's read stats.
// The result only includes peers that are hot enough.
// RegionStats is a thread-safe method
func (c *RaftCluster) RegionReadStats() map[uint64][]*statistics.HotPeerStat {
	// As read stats are reported by store heartbeat, the threshold needs to be adjusted.
	threshold := c.GetOpts().GetHotRegionCacheHitsThreshold() *
		(statistics.RegionHeartBeatReportInterval / statistics.StoreHeartBeatReportInterval)
	return c.hotStat.RegionStats(statistics.Read, threshold)
}

// BucketsStats returns hot region's buckets stats.
func (c *RaftCluster) BucketsStats(degree int) map[uint64][]*buckets.BucketStat {
	task := buckets.NewCollectBucketStatsTask(degree)
	if !c.hotBuckets.CheckAsync(task) {
		return nil
	}
	return task.WaitRet(c.ctx)
}

// RegionWriteStats returns hot region's write stats.
// The result only includes peers that are hot enough.
func (c *RaftCluster) RegionWriteStats() map[uint64][]*statistics.HotPeerStat {
	// RegionStats is a thread-safe method
	return c.hotStat.RegionStats(statistics.Write, c.GetOpts().GetHotRegionCacheHitsThreshold())
}

// TODO: remove me.
// only used in test.
func (c *RaftCluster) putRegion(region *core.RegionInfo) error {
	if c.storage != nil {
		if err := c.storage.SaveRegion(region.GetMeta()); err != nil {
			return err
		}
	}
	c.core.PutRegion(region)
	return nil
}

// GetHotWriteRegions gets hot write regions' info.
func (c *RaftCluster) GetHotWriteRegions(storeIDs ...uint64) *statistics.StoreHotPeersInfos {
	hotWriteRegions := c.coordinator.getHotRegionsByType(statistics.Write)
	if len(storeIDs) > 0 && hotWriteRegions != nil {
		hotWriteRegions = getHotRegionsByStoreIDs(hotWriteRegions, storeIDs...)
	}
	return hotWriteRegions
}

// GetHotReadRegions gets hot read regions' info.
func (c *RaftCluster) GetHotReadRegions(storeIDs ...uint64) *statistics.StoreHotPeersInfos {
	hotReadRegions := c.coordinator.getHotRegionsByType(statistics.Read)
	if len(storeIDs) > 0 && hotReadRegions != nil {
		hotReadRegions = getHotRegionsByStoreIDs(hotReadRegions, storeIDs...)
	}
	return hotReadRegions
}

func getHotRegionsByStoreIDs(hotPeerInfos *statistics.StoreHotPeersInfos, storeIDs ...uint64) *statistics.StoreHotPeersInfos {
	asLeader := statistics.StoreHotPeersStat{}
	asPeer := statistics.StoreHotPeersStat{}
	for _, storeID := range storeIDs {
		asLeader[storeID] = hotPeerInfos.AsLeader[storeID]
		asPeer[storeID] = hotPeerInfos.AsPeer[storeID]
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
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
	storeID := store.GetId()
	cfg := c.opt.GetScheduleConfig().Clone()
	if _, ok := cfg.StoreLimit[storeID]; ok {
		return
	}

	sc := config.StoreLimitConfig{
		AddPeer:    config.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer),
		RemovePeer: config.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer),
	}
	if core.IsStoreContainLabel(store, core.EngineKey, core.EngineTiFlash) {
		sc = config.StoreLimitConfig{
			AddPeer:    config.DefaultTiFlashStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer),
			RemovePeer: config.DefaultTiFlashStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer),
		}
	}

	cfg.StoreLimit[storeID] = sc
	c.opt.SetScheduleConfig(cfg)
	var err error
	for i := 0; i < persistLimitRetryTimes; i++ {
		if err = c.opt.Persist(c.storage); err == nil {
			log.Info("store limit added", zap.Uint64("store-id", storeID))
			return
		}
		time.Sleep(persistLimitWaitTime)
	}
	log.Error("persist store limit meet error", errs.ZapError(err))
}

// RemoveStoreLimit remove a store limit for a given store ID.
func (c *RaftCluster) RemoveStoreLimit(storeID uint64) {
	cfg := c.opt.GetScheduleConfig().Clone()
	for _, limitType := range storelimit.TypeNameValue {
		c.core.ResetStoreLimit(storeID, limitType)
	}
	delete(cfg.StoreLimit, storeID)
	c.opt.SetScheduleConfig(cfg)
	var err error
	for i := 0; i < persistLimitRetryTimes; i++ {
		if err = c.opt.Persist(c.storage); err == nil {
			log.Info("store limit removed", zap.Uint64("store-id", storeID))
			id := strconv.FormatUint(storeID, 10)
			statistics.StoreLimitGauge.DeleteLabelValues(id, "add-peer")
			statistics.StoreLimitGauge.DeleteLabelValues(id, "remove-peer")
			return
		}
		time.Sleep(persistLimitWaitTime)
	}
	log.Error("persist store limit meet error", errs.ZapError(err))
}

// SetMinResolvedTS sets up a store with min resolved ts.
func (c *RaftCluster) SetMinResolvedTS(storeID, minResolvedTS uint64) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}

	newStore := store.Clone(
		core.SetMinResolvedTS(minResolvedTS),
	)

	return c.putStoreLocked(newStore)
}

func (c *RaftCluster) checkAndUpdateMinResolvedTS() (uint64, bool) {
	c.Lock()
	defer c.Unlock()

	if !c.isInitialized() {
		return math.MaxUint64, false
	}
	curMinResolvedTS := uint64(math.MaxUint64)
	for _, s := range c.GetStores() {
		if !core.IsAvailableForMinResolvedTS(s) {
			continue
		}
		if curMinResolvedTS > s.GetMinResolvedTS() {
			curMinResolvedTS = s.GetMinResolvedTS()
		}
	}
	if curMinResolvedTS == math.MaxUint64 || curMinResolvedTS <= c.minResolvedTS {
		return c.minResolvedTS, false
	}
	c.minResolvedTS = curMinResolvedTS
	return c.minResolvedTS, true
}

func (c *RaftCluster) runMinResolvedTSJob() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	interval := c.opt.GetMinResolvedTSPersistenceInterval()
	if interval == 0 {
		interval = DefaultMinResolvedTSPersistenceInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	c.loadMinResolvedTS()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("min resolved ts background jobs has been stopped")
			return
		case <-ticker.C:
			interval = c.opt.GetMinResolvedTSPersistenceInterval()
			if interval != 0 {
				if current, needPersist := c.checkAndUpdateMinResolvedTS(); needPersist {
					c.storage.SaveMinResolvedTS(current)
				}
			} else {
				interval = DefaultMinResolvedTSPersistenceInterval
			}
			ticker.Reset(interval)
		}
	}
}

func (c *RaftCluster) loadMinResolvedTS() {
	// Use `c.GetStorage()` here to prevent from the data race in test.
	minResolvedTS, err := c.GetStorage().LoadMinResolvedTS()
	if err != nil {
		log.Error("load min resolved ts meet error", errs.ZapError(err))
		return
	}
	c.Lock()
	defer c.Unlock()
	c.minResolvedTS = minResolvedTS
}

// GetMinResolvedTS returns the min resolved ts of the cluster.
func (c *RaftCluster) GetMinResolvedTS() uint64 {
	c.RLock()
	defer c.RUnlock()
	if !c.isInitialized() {
		return math.MaxUint64
	}
	return c.minResolvedTS
}

// SetStoreLimit sets a store limit for a given type and rate.
func (c *RaftCluster) SetStoreLimit(storeID uint64, typ storelimit.Type, ratePerMin float64) error {
	old := c.opt.GetScheduleConfig().Clone()
	c.opt.SetStoreLimit(storeID, typ, ratePerMin)
	if err := c.opt.Persist(c.storage); err != nil {
		// roll back the store limit
		c.opt.SetScheduleConfig(old)
		log.Error("persist store limit meet error", errs.ZapError(err))
		return err
	}
	log.Info("store limit changed", zap.Uint64("store-id", storeID), zap.String("type", typ.String()), zap.Float64("rate-per-min", ratePerMin))
	return nil
}

// SetAllStoresLimit sets all store limit for a given type and rate.
func (c *RaftCluster) SetAllStoresLimit(typ storelimit.Type, ratePerMin float64) error {
	old := c.opt.GetScheduleConfig().Clone()
	oldAdd := config.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer)
	oldRemove := config.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer)
	c.opt.SetAllStoresLimit(typ, ratePerMin)
	if err := c.opt.Persist(c.storage); err != nil {
		// roll back the store limit
		c.opt.SetScheduleConfig(old)
		config.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, oldAdd)
		config.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, oldRemove)
		log.Error("persist store limit meet error", errs.ZapError(err))
		return err
	}
	log.Info("all store limit changed", zap.String("type", typ.String()), zap.Float64("rate-per-min", ratePerMin))
	return nil
}

// SetAllStoresLimitTTL sets all store limit for a given type and rate with ttl.
func (c *RaftCluster) SetAllStoresLimitTTL(typ storelimit.Type, ratePerMin float64, ttl time.Duration) {
	c.opt.SetAllStoresLimitTTL(c.ctx, c.etcdClient, typ, ratePerMin, ttl)
}

// GetClusterVersion returns the current cluster version.
func (c *RaftCluster) GetClusterVersion() string {
	return c.opt.GetClusterVersion().String()
}

// GetEtcdClient returns the current etcd client
func (c *RaftCluster) GetEtcdClient() *clientv3.Client {
	return c.etcdClient
}

// GetProgressByID returns the progress details for a given store ID.
func (c *RaftCluster) GetProgressByID(storeID string) (action string, process, ls, cs float64, err error) {
	filter := func(progress string) bool {
		s := strings.Split(progress, "-")
		return len(s) == 2 && s[1] == storeID
	}
	progress := c.progressManager.GetProgresses(filter)
	if len(progress) != 0 {
		process, ls, cs, err = c.progressManager.Status(progress[0])
		if err != nil {
			return
		}
		if strings.HasPrefix(progress[0], removingAction) {
			action = removingAction
		} else if strings.HasPrefix(progress[0], preparingAction) {
			action = preparingAction
		}
		return
	}
	return "", 0, 0, 0, errs.ErrProgressNotFound.FastGenByArgs(fmt.Sprintf("the given store ID: %s", storeID))
}

// GetProgressByAction returns the progress details for a given action.
func (c *RaftCluster) GetProgressByAction(action string) (process, ls, cs float64, err error) {
	filter := func(progress string) bool {
		return strings.HasPrefix(progress, action)
	}

	progresses := c.progressManager.GetProgresses(filter)
	if len(progresses) == 0 {
		return 0, 0, 0, errs.ErrProgressNotFound.FastGenByArgs(fmt.Sprintf("the action: %s", action))
	}
	var p, l, s float64
	for _, progress := range progresses {
		p, l, s, err = c.progressManager.Status(progress)
		if err != nil {
			return
		}
		process += p
		ls += l
		cs += s
	}
	num := float64(len(progresses))
	process /= num
	cs /= num
	ls /= num
	// handle the special cases
	if math.IsNaN(ls) || math.IsInf(ls, 0) {
		ls = math.MaxFloat64
	}
	return
}

var healthURL = "/pd/api/v1/ping"

// CheckHealth checks if members are healthy.
func CheckHealth(client *http.Client, members []*pdpb.Member) map[uint64]*pdpb.Member {
	healthMembers := make(map[uint64]*pdpb.Member)
	for _, member := range members {
		for _, cURL := range member.ClientUrls {
			ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s%s", cURL, healthURL), nil)
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

// cacheCluster include cache info to improve the performance.
type cacheCluster struct {
	*RaftCluster
	stores []*core.StoreInfo
}

// GetStores returns store infos from cache
func (c *cacheCluster) GetStores() []*core.StoreInfo {
	return c.stores
}

// newCacheCluster constructor for cache
func newCacheCluster(c *RaftCluster) *cacheCluster {
	return &cacheCluster{
		RaftCluster: c,
		stores:      c.GetStores(),
	}
}

// GetPausedSchedulerDelayAt returns DelayAt of a paused scheduler
func (c *RaftCluster) GetPausedSchedulerDelayAt(name string) (int64, error) {
	return c.coordinator.getPausedSchedulerDelayAt(name)
}

// GetPausedSchedulerDelayUntil returns DelayUntil of a paused scheduler
func (c *RaftCluster) GetPausedSchedulerDelayUntil(name string) (int64, error) {
	return c.coordinator.getPausedSchedulerDelayUntil(name)
}
