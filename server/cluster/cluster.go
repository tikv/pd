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
	"encoding/json"
	errorspkg "errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cluster"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/gc"
	"github.com/tikv/pd/pkg/gctuner"
	"github.com/tikv/pd/pkg/id"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/memory"
	"github.com/tikv/pd/pkg/progress"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/replication"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/syncer"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/unsaferecovery"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/netutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	// DefaultMinResolvedTSPersistenceInterval is the default value of min resolved ts persistence interval.
	// If interval in config is zero, it means not to persist resolved ts and check config with this DefaultMinResolvedTSPersistenceInterval
	DefaultMinResolvedTSPersistenceInterval = config.DefaultMinResolvedTSPersistenceInterval
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	regionUpdateCacheEventCounter = regionEventCounter.WithLabelValues("update_cache")
	regionUpdateKVEventCounter    = regionEventCounter.WithLabelValues("update_kv")
	regionCacheMissCounter        = bucketEventCounter.WithLabelValues("region_cache_miss")
	versionNotMatchCounter        = bucketEventCounter.WithLabelValues("version_not_match")
	updateFailedCounter           = bucketEventCounter.WithLabelValues("update_failed")
)

// regionLabelGCInterval is the interval to run region-label's GC work.
const regionLabelGCInterval = time.Hour

const (
	// nodeStateCheckJobInterval is the interval to run node state check job.
	nodeStateCheckJobInterval = 10 * time.Second
	// metricsCollectionJobInterval is the interval to run metrics collection job.
	metricsCollectionJobInterval   = 10 * time.Second
	updateStoreStatsInterval       = 9 * time.Millisecond
	clientTimeout                  = 3 * time.Second
	defaultChangedRegionsLimit     = 10000
	gcTombstoneInterval            = 30 * 24 * time.Hour
	schedulingServiceCheckInterval = 10 * time.Second
	tsoServiceCheckInterval        = 100 * time.Millisecond
	// persistLimitRetryTimes is used to reduce the probability of the persistent error
	// since the once the store is added or removed, we shouldn't return an error even if the store limit is failed to persist.
	persistLimitRetryTimes  = 5
	persistLimitWaitTime    = 100 * time.Millisecond
	removingAction          = "removing"
	preparingAction         = "preparing"
	gcTunerCheckCfgInterval = 10 * time.Second

	// minSnapshotDurationSec is the minimum duration that a store can tolerate.
	// It should enlarge the limiter if the snapshot's duration is less than this value.
	minSnapshotDurationSec = 5

	// heartbeat relative const
	heartbeatTaskRunner = "heartbeat-async"
	miscTaskRunner      = "misc-async"
	logTaskRunner       = "log-async"
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
	GetKeyspaceGroupManager() *keyspace.GroupManager
	IsAPIServiceMode() bool
	GetSafePointV2Manager() *gc.SafePointV2Manager
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

	*core.BasicCluster // cached cluster info
	member             *member.EmbeddedEtcdMember

	etcdClient *clientv3.Client
	httpClient *http.Client

	running          bool
	isAPIServiceMode bool
	meta             *metapb.Cluster
	storage          storage.Storage
	minResolvedTS    atomic.Value // Store as uint64
	externalTS       atomic.Value // Store as uint64

	// Keep the previous store limit settings when removing a store.
	prevStoreLimit map[uint64]map[storelimit.Type]float64

	// This below fields are all read-only, we cannot update itself after the raft cluster starts.
	id  id.Allocator
	opt *config.PersistOptions
	*schedulingController
	ruleManager              *placement.RuleManager
	regionLabeler            *labeler.RegionLabeler
	replicationMode          *replication.ModeManager
	unsafeRecoveryController *unsaferecovery.Controller
	progressManager          *progress.Manager
	regionSyncer             *syncer.RegionSyncer
	changedRegions           chan *core.RegionInfo
	keyspaceGroupManager     *keyspace.GroupManager
	independentServices      sync.Map
	hbstreams                *hbstream.HeartbeatStreams
	tsoAllocator             *tso.AllocatorManager

	// heartbeatRunner is used to process the subtree update task asynchronously.
	heartbeatRunner ratelimit.Runner
	// miscRunner is used to process the statistics and persistent tasks asynchronously.
	miscRunner ratelimit.Runner
	// logRunner is used to process the log asynchronously.
	logRunner ratelimit.Runner
}

// Status saves some state information.
// NOTE:
// - This type is exported by HTTP API. Please pay more attention when modifying it.
// - Need to sync with client/http/types.go#ClusterStatus
type Status struct {
	RaftBootstrapTime time.Time `json:"raft_bootstrap_time,omitempty"`
	IsInitialized     bool      `json:"is_initialized"`
	ReplicationStatus string    `json:"replication_status"`
}

// NewRaftCluster create a new cluster.
func NewRaftCluster(
	ctx context.Context,
	member *member.EmbeddedEtcdMember,
	basicCluster *core.BasicCluster,
	storage storage.Storage,
	regionSyncer *syncer.RegionSyncer,
	etcdClient *clientv3.Client,
	httpClient *http.Client,
	tsoAllocator *tso.AllocatorManager,
) *RaftCluster {
	return &RaftCluster{
		serverCtx:       ctx,
		member:          member,
		regionSyncer:    regionSyncer,
		httpClient:      httpClient,
		etcdClient:      etcdClient,
		BasicCluster:    basicCluster,
		storage:         storage,
		tsoAllocator:    tsoAllocator,
		heartbeatRunner: ratelimit.NewConcurrentRunner(heartbeatTaskRunner, ratelimit.NewConcurrencyLimiter(uint64(runtime.NumCPU()*2)), time.Minute),
		miscRunner:      ratelimit.NewConcurrentRunner(miscTaskRunner, ratelimit.NewConcurrencyLimiter(uint64(runtime.NumCPU()*2)), time.Minute),
		logRunner:       ratelimit.NewConcurrentRunner(logTaskRunner, ratelimit.NewConcurrencyLimiter(uint64(runtime.NumCPU()*2)), time.Minute),
	}
}

// GetStoreConfig returns the store config.
func (c *RaftCluster) GetStoreConfig() sc.StoreConfigProvider {
	return c.GetOpts()
}

// GetCheckerConfig returns the checker config.
func (c *RaftCluster) GetCheckerConfig() sc.CheckerConfigProvider {
	return c.GetOpts()
}

// GetSchedulerConfig returns the scheduler config.
func (c *RaftCluster) GetSchedulerConfig() sc.SchedulerConfigProvider {
	return c.GetOpts()
}

// GetSharedConfig returns the shared config.
func (c *RaftCluster) GetSharedConfig() sc.SharedConfigProvider {
	return c.GetOpts()
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
	if c.GetTotalRegionCount() > 1 {
		return true
	}
	region := c.GetRegionByKey(nil)
	return region != nil &&
		len(region.GetVoters()) >= int(c.opt.GetReplicationConfig().MaxReplicas) &&
		len(region.GetPendingPeers()) == 0
}

// loadBootstrapTime loads the saved bootstrap time from etcd. It returns zero
// value of time.Time when there is error or the cluster is not bootstrapped yet.
func (c *RaftCluster) loadBootstrapTime() (time.Time, error) {
	var t time.Time
	data, err := c.storage.Load(keypath.ClusterBootstrapTimeKey())
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
	opt sc.ConfProvider,
	hbstreams *hbstream.HeartbeatStreams,
	keyspaceGroupManager *keyspace.GroupManager) error {
	c.opt, c.id = opt.(*config.PersistOptions), id
	c.ctx, c.cancel = context.WithCancel(c.serverCtx)
	c.progressManager = progress.NewManager()
	c.changedRegions = make(chan *core.RegionInfo, defaultChangedRegionsLimit)
	c.prevStoreLimit = make(map[uint64]map[storelimit.Type]float64)
	c.unsafeRecoveryController = unsaferecovery.NewController(c)
	c.keyspaceGroupManager = keyspaceGroupManager
	c.hbstreams = hbstreams
	c.ruleManager = placement.NewRuleManager(c.ctx, c.storage, c, c.GetOpts())
	if c.opt.IsPlacementRulesEnabled() {
		err := c.ruleManager.Initialize(c.opt.GetMaxReplicas(), c.opt.GetLocationLabels(), c.opt.GetIsolationLevel())
		if err != nil {
			return err
		}
	}
	c.schedulingController = newSchedulingController(c.ctx, c.BasicCluster, c.opt, c.ruleManager)
	return nil
}

// Start starts a cluster.
func (c *RaftCluster) Start(s Server) error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}
	c.isAPIServiceMode = s.IsAPIServiceMode()
	err := c.InitCluster(s.GetAllocator(), s.GetPersistOptions(), s.GetHBStreams(), s.GetKeyspaceGroupManager())
	if err != nil {
		return err
	}
	c.checkTSOService()
	cluster, err := c.LoadClusterInfo()
	if err != nil {
		return err
	}
	if cluster == nil {
		log.Warn("cluster is not bootstrapped")
		return nil
	}

	c.regionLabeler, err = labeler.NewRegionLabeler(c.ctx, c.storage, regionLabelGCInterval)
	if err != nil {
		return err
	}

	if !c.IsServiceIndependent(constant.SchedulingServiceName) {
		for _, store := range c.GetStores() {
			storeID := store.GetID()
			c.slowStat.ObserveSlowStoreStatus(storeID, store.IsSlow())
		}
	}
	c.replicationMode, err = replication.NewReplicationModeManager(s.GetConfig().ReplicationMode, c.storage, cluster, s)
	if err != nil {
		return err
	}
	c.loadExternalTS()
	c.loadMinResolvedTS()

	if c.isAPIServiceMode {
		// bootstrap keyspace group manager after starting other parts successfully.
		// This order avoids a stuck goroutine in keyspaceGroupManager when it fails to create raftcluster.
		err = c.keyspaceGroupManager.Bootstrap(c.ctx)
		if err != nil {
			return err
		}
	}
	c.checkSchedulingService()
	c.wg.Add(9)
	go c.runServiceCheckJob()
	go c.runMetricsCollectionJob()
	go c.runNodeStateCheckJob()
	go c.syncRegions()
	go c.runReplicationMode()
	go c.runMinResolvedTSJob()
	go c.runStoreConfigSync()
	go c.runUpdateStoreStats()
	go c.startGCTuner()

	c.running = true
	c.heartbeatRunner.Start(c.ctx)
	c.miscRunner.Start(c.ctx)
	c.logRunner.Start(c.ctx)
	return nil
}

func (c *RaftCluster) checkSchedulingService() {
	if c.isAPIServiceMode {
		servers, err := discovery.Discover(c.etcdClient, constant.SchedulingServiceName)
		if c.opt.GetMicroServiceConfig().IsSchedulingFallbackEnabled() && (err != nil || len(servers) == 0) {
			c.startSchedulingJobs(c, c.hbstreams)
			c.UnsetServiceIndependent(constant.SchedulingServiceName)
		} else {
			if c.stopSchedulingJobs() || c.coordinator == nil {
				c.initCoordinator(c.ctx, c, c.hbstreams)
			}
			if !c.IsServiceIndependent(constant.SchedulingServiceName) {
				c.SetServiceIndependent(constant.SchedulingServiceName)
			}
		}
	} else {
		c.startSchedulingJobs(c, c.hbstreams)
		c.UnsetServiceIndependent(constant.SchedulingServiceName)
	}
}

// checkTSOService checks the TSO service.
func (c *RaftCluster) checkTSOService() {
	if c.isAPIServiceMode {
		if c.opt.GetMicroServiceConfig().IsTSODynamicSwitchingEnabled() {
			servers, err := discovery.Discover(c.etcdClient, constant.TSOServiceName)
			if err != nil || len(servers) == 0 {
				if err := c.startTSOJobsIfNeeded(); err != nil {
					log.Error("failed to start TSO jobs", errs.ZapError(err))
					return
				}
				if c.IsServiceIndependent(constant.TSOServiceName) {
					log.Info("TSO is provided by PD")
					c.UnsetServiceIndependent(constant.TSOServiceName)
				}
			} else {
				c.stopTSOJobsIfNeeded()
				if !c.IsServiceIndependent(constant.TSOServiceName) {
					log.Info("TSO is provided by TSO server")
					c.SetServiceIndependent(constant.TSOServiceName)
				}
			}
		}
		return
	}

	if err := c.startTSOJobsIfNeeded(); err != nil {
		log.Error("failed to start TSO jobs", errs.ZapError(err))
		return
	}
}

func (c *RaftCluster) runServiceCheckJob() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	schedulingTicker := time.NewTicker(schedulingServiceCheckInterval)
	failpoint.Inject("highFrequencyClusterJobs", func() {
		schedulingTicker.Reset(time.Millisecond)
	})
	defer schedulingTicker.Stop()
	tsoTicker := time.NewTicker(tsoServiceCheckInterval)
	defer tsoTicker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Info("service check job is stopped")
			return
		case <-schedulingTicker.C:
			// ensure raft cluster is running
			// avoid unexpected startSchedulingJobs when raft cluster is stopping
			c.RLock()
			if c.running {
				c.checkSchedulingService()
			}
			c.RUnlock()
		case <-tsoTicker.C:
			// ensure raft cluster is running
			// avoid unexpected startTSOJobsIfNeeded when raft cluster is stopping
			// ref: https://github.com/tikv/pd/issues/8781
			c.RLock()
			if c.running {
				c.checkTSOService()
			}
			c.RUnlock()
		}
	}
}

func (c *RaftCluster) startTSOJobsIfNeeded() error {
	allocator := c.tsoAllocator.GetAllocator()
	if !allocator.IsInitialize() {
		log.Info("initializing the global TSO allocator")
		if err := allocator.Initialize(0); err != nil {
			log.Error("failed to initialize the global TSO allocator", errs.ZapError(err))
			return err
		}
	} else if !c.running {
		// If the global TSO allocator is already initialized, but the running flag is false,
		// it means there maybe unexpected error happened before.
		log.Warn("the global TSO allocator is already initialized before, but the cluster is not running")
	}
	return nil
}

func (c *RaftCluster) stopTSOJobsIfNeeded() {
	allocator := c.tsoAllocator.GetAllocator()
	if !allocator.IsInitialize() {
		return
	}
	log.Info("closing the global TSO allocator")
	c.tsoAllocator.ResetAllocatorGroup(true)
	failpoint.Inject("updateAfterResetTSO", func() {
		allocator := c.tsoAllocator.GetAllocator()
		if err := allocator.UpdateTSO(); !errorspkg.Is(err, errs.ErrUpdateTimestamp) {
			log.Panic("the tso update after reset should return ErrUpdateTimestamp as expected", zap.Error(err))
		}
		if allocator.IsInitialize() {
			log.Panic("the allocator should be uninitialized after reset")
		}
	})
}

// startGCTuner
func (c *RaftCluster) startGCTuner() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	tick := time.NewTicker(gcTunerCheckCfgInterval)
	defer tick.Stop()
	totalMem, err := memory.MemTotal()
	if err != nil {
		log.Fatal("fail to get total memory:%s", zap.Error(err))
	}
	log.Info("memory info", zap.Uint64("total-mem", totalMem))
	cfg := c.opt.GetPDServerConfig()
	enableGCTuner := cfg.EnableGOGCTuner
	memoryLimitBytes := uint64(float64(totalMem) * cfg.ServerMemoryLimit)
	gcThresholdBytes := uint64(float64(memoryLimitBytes) * cfg.GCTunerThreshold)
	if memoryLimitBytes == 0 {
		gcThresholdBytes = uint64(float64(totalMem) * cfg.GCTunerThreshold)
	}
	memoryLimitGCTriggerRatio := cfg.ServerMemoryLimitGCTrigger
	memoryLimitGCTriggerBytes := uint64(float64(memoryLimitBytes) * memoryLimitGCTriggerRatio)
	updateGCTuner := func() {
		gctuner.Tuning(gcThresholdBytes)
		gctuner.EnableGOGCTuner.Store(enableGCTuner)
		log.Info("update gc tuner", zap.Bool("enable-gc-tuner", enableGCTuner),
			zap.Uint64("gc-threshold-bytes", gcThresholdBytes))
	}
	updateGCMemLimit := func() {
		memory.ServerMemoryLimit.Store(memoryLimitBytes)
		gctuner.GlobalMemoryLimitTuner.SetPercentage(memoryLimitGCTriggerRatio)
		gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
		log.Info("update gc memory limit", zap.Uint64("memory-limit-bytes", memoryLimitBytes),
			zap.Float64("memory-limit-gc-trigger-ratio", memoryLimitGCTriggerRatio))
	}
	updateGCTuner()
	updateGCMemLimit()
	checkAndUpdateIfCfgChange := func() {
		cfg := c.opt.GetPDServerConfig()
		newEnableGCTuner := cfg.EnableGOGCTuner
		newMemoryLimitBytes := uint64(float64(totalMem) * cfg.ServerMemoryLimit)
		newGCThresholdBytes := uint64(float64(newMemoryLimitBytes) * cfg.GCTunerThreshold)
		if newMemoryLimitBytes == 0 {
			newGCThresholdBytes = uint64(float64(totalMem) * cfg.GCTunerThreshold)
		}
		newMemoryLimitGCTriggerRatio := cfg.ServerMemoryLimitGCTrigger
		newMemoryLimitGCTriggerBytes := uint64(float64(newMemoryLimitBytes) * newMemoryLimitGCTriggerRatio)
		if newEnableGCTuner != enableGCTuner || newGCThresholdBytes != gcThresholdBytes {
			enableGCTuner = newEnableGCTuner
			gcThresholdBytes = newGCThresholdBytes
			updateGCTuner()
		}
		if newMemoryLimitBytes != memoryLimitBytes || newMemoryLimitGCTriggerBytes != memoryLimitGCTriggerBytes {
			memoryLimitBytes = newMemoryLimitBytes
			memoryLimitGCTriggerBytes = newMemoryLimitGCTriggerBytes
			memoryLimitGCTriggerRatio = newMemoryLimitGCTriggerRatio
			updateGCMemLimit()
		}
	}
	for {
		select {
		case <-c.ctx.Done():
			log.Info("gc tuner is stopped")
			return
		case <-tick.C:
			checkAndUpdateIfCfgChange()
		}
	}
}

// runStoreConfigSync runs the job to sync the store config from TiKV.
func (c *RaftCluster) runStoreConfigSync() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	// TODO: After we fix the atomic problem of config, we can remove this failpoint.
	failpoint.Inject("skipStoreConfigSync", func() {
		failpoint.Return()
	})

	var (
		synced, switchRaftV2Config, needPersist bool
		stores                                  = c.GetStores()
	)
	// Start the ticker with a second-level timer to accelerate
	// the bootstrap stage.
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		synced, switchRaftV2Config, needPersist = c.syncStoreConfig(stores)
		if switchRaftV2Config {
			if err := c.opt.SwitchRaftV2(c.GetStorage()); err != nil {
				log.Warn("store config persisted failed", zap.Error(err))
			}
		}
		// Update the stores if the synchronization is not completed.
		if !synced {
			stores = c.GetStores()
		}
		if needPersist {
			if err := c.opt.Persist(c.storage); err != nil {
				log.Warn("store config persisted failed", zap.Error(err))
			}
			log.Info("store config is updated")
		}
		select {
		case <-c.ctx.Done():
			log.Info("sync store config job is stopped")
			return
		case <-ticker.C:
		}
	}
}

// syncStoreConfig syncs the store config from TiKV.
//   - `synced` is true if sync config from one tikv.
//   - `switchRaftV2` is true if the config of tikv engine is change to raft-kv2.
func (c *RaftCluster) syncStoreConfig(stores []*core.StoreInfo) (synced bool, switchRaftV2 bool, needPersist bool) {
	var err error
	for index := 0; index < len(stores); index++ {
		select {
		case <-c.ctx.Done():
			log.Info("stop sync store config job due to server shutdown")
			return
		default:
		}
		// filter out the stores that are tiflash
		store := stores[index]
		if store.IsTiFlash() {
			continue
		}

		// filter out the stores that are not up.
		if !(store.IsPreparing() || store.IsServing()) {
			continue
		}
		// it will try next store if the current store is failed.
		address := netutil.ResolveLoopBackAddr(stores[index].GetStatusAddress(), stores[index].GetAddress())
		switchRaftV2, needPersist, err = c.observeStoreConfig(c.ctx, address)
		if err != nil {
			// delete the store if it is failed and retry next store.
			stores = append(stores[:index], stores[index+1:]...)
			index--
			storeSyncConfigEvent.WithLabelValues(address, "fail").Inc()
			log.Debug("sync store config failed, it will try next store", zap.Error(err))
			continue
		} else if switchRaftV2 {
			storeSyncConfigEvent.WithLabelValues(address, "raft-v2").Inc()
		}
		storeSyncConfigEvent.WithLabelValues(address, "succ").Inc()

		return true, switchRaftV2, needPersist
	}
	return false, false, needPersist
}

// observeStoreConfig is used to observe the store config changes and
// return whether if the new config changes the engine to raft-kv2.
func (c *RaftCluster) observeStoreConfig(ctx context.Context, address string) (switchRaftV2 bool, needPersist bool, err error) {
	cfg, err := c.fetchStoreConfigFromTiKV(ctx, address)
	if err != nil {
		return false, false, err
	}
	oldCfg := c.opt.GetStoreConfig()
	if cfg == nil || oldCfg.Equal(cfg) {
		return false, false, nil
	}
	log.Info("sync the store config successful",
		zap.String("store-address", address),
		zap.String("store-config", cfg.String()),
		zap.String("old-config", oldCfg.String()))
	return c.updateStoreConfig(oldCfg, cfg), true, nil
}

// updateStoreConfig updates the store config. This is extracted for testing.
func (c *RaftCluster) updateStoreConfig(oldCfg, cfg *sc.StoreConfig) (switchRaftV2 bool) {
	cfg.Adjust()
	c.opt.SetStoreConfig(cfg)
	switchRaftV2 = oldCfg.Storage.Engine != sc.RaftstoreV2 && cfg.Storage.Engine == sc.RaftstoreV2
	return
}

// fetchStoreConfigFromTiKV tries to fetch the config from the TiKV store URL.
func (c *RaftCluster) fetchStoreConfigFromTiKV(ctx context.Context, statusAddress string) (*sc.StoreConfig, error) {
	cfg := &sc.StoreConfig{}
	failpoint.Inject("mockFetchStoreConfigFromTiKV", func(val failpoint.Value) {
		if regionMaxSize, ok := val.(string); ok {
			cfg.RegionMaxSize = regionMaxSize
			cfg.Storage.Engine = sc.RaftstoreV2
		}
		failpoint.Return(cfg, nil)
	})
	if c.httpClient == nil {
		return nil, fmt.Errorf("failed to get store config due to nil client")
	}
	var url string
	if netutil.IsEnableHTTPS(c.httpClient) {
		url = fmt.Sprintf("%s://%s/config", "https", statusAddress)
	} else {
		url = fmt.Sprintf("%s://%s/config", "http", statusAddress)
	}
	ctx, cancel := context.WithTimeout(ctx, clientTimeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create store config http request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	cancel()
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(body, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
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

	c.ResetStores()
	start := time.Now()
	if err := c.storage.LoadStores(c.PutStore); err != nil {
		return nil, err
	}
	log.Info("load stores",
		zap.Int("count", c.GetStoreCount()),
		zap.Duration("cost", time.Since(start)),
	)

	start = time.Now()

	// used to load region from kv storage to cache storage.
	if err = storage.TryLoadRegionsOnce(c.ctx, c.storage, c.CheckAndPutRegion); err != nil {
		return nil, err
	}
	log.Info("load regions",
		zap.Int("count", c.GetTotalRegionCount()),
		zap.Duration("cost", time.Since(start)),
	)

	return c, nil
}

func (c *RaftCluster) runMetricsCollectionJob() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(metricsCollectionJobInterval)
	failpoint.Inject("highFrequencyClusterJobs", func() {
		ticker.Reset(time.Millisecond)
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
		ticker.Reset(2 * time.Second)
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

func (c *RaftCluster) runUpdateStoreStats() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(updateStoreStatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Info("update store stats background jobs has been stopped")
			return
		case <-ticker.C:
			// Update related stores.
			start := time.Now()
			c.UpdateAllStoreStatus()
			updateStoreStatsGauge.Set(time.Since(start).Seconds())
		}
	}
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
	// We need to try to stop tso jobs whatever the cluster is running or not.
	// Because we need to call checkTSOService as soon as possible while the cluster is starting,
	// which makes the cluster may not be running but the tso job has been started.
	// For example, the cluster meets an error when starting, such as cluster is not bootstrapped.
	// In this case, the `running` in `RaftCluster` is false, but the tso job has been started.
	// Ref: https://github.com/tikv/pd/issues/8836
	c.stopTSOJobsIfNeeded()
	if !c.running {
		c.Unlock()
		return
	}
	c.running = false
	c.cancel()
	if !c.IsServiceIndependent(constant.SchedulingServiceName) {
		c.stopSchedulingJobs()
	}
	c.heartbeatRunner.Stop()
	c.miscRunner.Stop()
	c.logRunner.Stop()
	c.Unlock()

	c.wg.Wait()
	log.Info("raft cluster is stopped")
}

// Wait blocks until the cluster is stopped. Only for test purpose.
func (c *RaftCluster) Wait() {
	c.wg.Wait()
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

// GetHeartbeatStreams returns the heartbeat streams.
func (c *RaftCluster) GetHeartbeatStreams() *hbstream.HeartbeatStreams {
	return c.hbstreams
}

// AllocID returns a global unique ID.
func (c *RaftCluster) AllocID() (uint64, error) {
	return c.id.Alloc()
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
	return c.storage
}

// GetOpts returns cluster's configuration.
// There is no need a lock since it won't changed.
func (c *RaftCluster) GetOpts() sc.ConfProvider {
	return c.opt
}

// GetScheduleConfig returns scheduling configurations.
func (c *RaftCluster) GetScheduleConfig() *sc.ScheduleConfig {
	return c.opt.GetScheduleConfig()
}

// SetScheduleConfig sets the PD scheduling configuration.
func (c *RaftCluster) SetScheduleConfig(cfg *sc.ScheduleConfig) {
	c.opt.SetScheduleConfig(cfg)
}

// GetReplicationConfig returns replication configurations.
func (c *RaftCluster) GetReplicationConfig() *sc.ReplicationConfig {
	return c.opt.GetReplicationConfig()
}

// GetPDServerConfig returns pd server configurations.
func (c *RaftCluster) GetPDServerConfig() *config.PDServerConfig {
	return c.opt.GetPDServerConfig()
}

// SetPDServerConfig sets the PD configuration.
func (c *RaftCluster) SetPDServerConfig(cfg *config.PDServerConfig) {
	c.opt.SetPDServerConfig(cfg)
}

// IsSchedulingHalted returns whether the scheduling is halted.
// Currently, the PD scheduling is halted when:
//   - The `HaltScheduling` persist option is set to true.
//   - Online unsafe recovery is running.
func (c *RaftCluster) IsSchedulingHalted() bool {
	return c.opt.IsSchedulingHalted() || c.unsafeRecoveryController.IsRunning()
}

// GetUnsafeRecoveryController returns the unsafe recovery controller.
func (c *RaftCluster) GetUnsafeRecoveryController() *unsaferecovery.Controller {
	return c.unsafeRecoveryController
}

// HandleStoreHeartbeat updates the store status.
func (c *RaftCluster) HandleStoreHeartbeat(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) error {
	stats := heartbeat.GetStats()
	storeID := stats.GetStoreId()
	store := c.GetStore(storeID)
	if store == nil {
		return errors.Errorf("store %v not found", storeID)
	}

	limit := store.GetStoreLimit()
	version := c.opt.GetStoreLimitVersion()
	var opt core.StoreCreateOption
	if limit == nil || limit.Version() != version {
		if version == storelimit.VersionV2 {
			limit = storelimit.NewSlidingWindows()
		} else {
			limit = storelimit.NewStoreRateLimit(0.0)
		}
		opt = core.SetStoreLimit(limit)
	}

	nowTime := time.Now()
	var newStore *core.StoreInfo
	// If this cluster has slow stores, we should awaken hibernated regions in other stores.
	if !c.IsServiceIndependent(constant.SchedulingServiceName) {
		if needAwaken, slowStoreIDs := c.NeedAwakenAllRegionsInStore(storeID); needAwaken {
			log.Info("forcely awaken hibernated regions", zap.Uint64("store-id", storeID), zap.Uint64s("slow-stores", slowStoreIDs))
			newStore = store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(nowTime), core.SetLastAwakenTime(nowTime), opt)
			resp.AwakenRegions = &pdpb.AwakenRegions{
				AbnormalStores: slowStoreIDs,
			}
		} else {
			newStore = store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(nowTime), opt)
		}
	} else {
		newStore = store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(nowTime), opt)
	}

	if newStore.IsLowSpace(c.opt.GetLowSpaceRatio()) {
		log.Warn("store does not have enough disk space",
			zap.Uint64("store-id", storeID),
			zap.Uint64("capacity", newStore.GetCapacity()),
			zap.Uint64("available", newStore.GetAvailable()))
	}
	if newStore.NeedPersist() && c.storage != nil {
		if err := c.storage.SaveStoreMeta(newStore.GetMeta()); err != nil {
			log.Error("failed to persist store", zap.Uint64("store-id", storeID), errs.ZapError(err))
		} else {
			newStore = newStore.Clone(core.SetLastPersistTime(nowTime))
		}
	}
	if store := c.GetStore(storeID); store != nil {
		statistics.UpdateStoreHeartbeatMetrics(store)
	}
	c.PutStore(newStore)
	var (
		regions  map[uint64]*core.RegionInfo
		interval uint64
	)
	if !c.IsServiceIndependent(constant.SchedulingServiceName) {
		c.hotStat.Observe(storeID, newStore.GetStoreStats())
		c.hotStat.FilterUnhealthyStore(c)
		c.slowStat.ObserveSlowStoreStatus(storeID, newStore.IsSlow())
		reportInterval := stats.GetInterval()
		interval = reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()

		regions = make(map[uint64]*core.RegionInfo, len(stats.GetPeerStats()))
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
				utils.RegionReadBytes:     float64(peerStat.GetReadBytes()),
				utils.RegionReadKeys:      float64(peerStat.GetReadKeys()),
				utils.RegionReadQueryNum:  float64(readQueryNum),
				utils.RegionWriteBytes:    0,
				utils.RegionWriteKeys:     0,
				utils.RegionWriteQueryNum: 0,
			}
			checkReadPeerTask := func(cache *statistics.HotPeerCache) {
				stats := cache.CheckPeerFlow(region, []*metapb.Peer{peer}, loads, interval)
				for _, stat := range stats {
					cache.UpdateStat(stat)
				}
			}
			c.hotStat.CheckReadAsync(checkReadPeerTask)
		}
	}
	for _, stat := range stats.GetSnapshotStats() {
		// the duration of snapshot is the sum between to send and generate snapshot.
		// notice: to enlarge the limit in time, we reset the executing duration when it less than the minSnapshotDurationSec.
		dur := stat.GetSendDurationSec() + stat.GetGenerateDurationSec()
		if dur < minSnapshotDurationSec {
			dur = minSnapshotDurationSec
		}
		// This error is the diff between the executing duration and the waiting duration.
		// The waiting duration is the total duration minus the executing duration.
		// so e=executing_duration-waiting_duration=executing_duration-(total_duration-executing_duration)=2*executing_duration-total_duration
		// Eg: the total duration is 20s, the executing duration is 10s, the error is 0s.
		// Eg: the total duration is 20s, the executing duration is 8s, the error is -4s.
		// Eg: the total duration is 10s, the executing duration is 12s, the error is 4s.
		// if error is positive, it means the most time cost in executing, pd should send more snapshot to this tikv.
		// if error is negative, it means the most time cost in waiting, pd should send less snapshot to this tikv.
		e := int64(dur)*2 - int64(stat.GetTotalDurationSec())
		store.Feedback(float64(e))
	}
	if !c.IsServiceIndependent(constant.SchedulingServiceName) {
		// Here we will compare the reported regions with the previous hot peers to decide if it is still hot.
		collectUnReportedPeerTask := func(cache *statistics.HotPeerCache) {
			stats := cache.CheckColdPeer(storeID, regions, interval)
			for _, stat := range stats {
				cache.UpdateStat(stat)
			}
		}
		c.hotStat.CheckReadAsync(collectUnReportedPeerTask)
	}
	return nil
}

// processReportBuckets update the bucket information.
func (c *RaftCluster) processReportBuckets(buckets *metapb.Buckets) error {
	region := c.GetRegion(buckets.GetRegionId())
	if region == nil {
		regionCacheMissCounter.Inc()
		return errors.Errorf("region %v not found", buckets.GetRegionId())
	}
	// use CAS to update the bucket information.
	// the two request(A:3,B:2) get the same region and need to update the buckets.
	// the A will pass the check and set the version to 3, the B will fail because the region.bucket has changed.
	// the retry should keep the old version and the new version will be set to the region.bucket, like two requests (A:2,B:3).
	for range 3 {
		old := region.GetBuckets()
		// region should not update if the version of the buckets is less than the old one.
		if old != nil && buckets.GetVersion() <= old.GetVersion() {
			versionNotMatchCounter.Inc()
			return nil
		}
		failpoint.Inject("concurrentBucketHeartbeat", func() {
			time.Sleep(500 * time.Millisecond)
		})
		if ok := region.UpdateBuckets(buckets, old); ok {
			return nil
		}
	}
	updateFailedCounter.Inc()
	return nil
}

var regionGuide = core.GenerateRegionGuideFunc(true)
var syncRunner = ratelimit.NewSyncRunner()

// processRegionHeartbeat updates the region information.
func (c *RaftCluster) processRegionHeartbeat(ctx *core.MetaProcessContext, region *core.RegionInfo) error {
	tracer := ctx.Tracer
	origin, _, err := c.PreCheckPutRegion(region)
	tracer.OnPreCheckFinished()
	if err != nil {
		return err
	}

	region.Inherit(origin, c.GetStoreConfig().IsEnableRegionBucket())

	if !c.IsServiceIndependent(constant.SchedulingServiceName) {
		cluster.HandleStatsAsync(c, region)
	}
	tracer.OnAsyncHotStatsFinished()
	hasRegionStats := c.regionStats != nil
	// Save to storage if meta is updated, except for flashback.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	saveKV, saveCache, needSync, retained := regionGuide(ctx, region, origin)
	tracer.OnRegionGuideFinished()
	regionID := region.GetID()
	if !saveKV && !saveCache {
		// Due to some config changes need to update the region stats as well,
		// so we do some extra checks here.
		// TODO: Due to the accuracy requirements of the API "/regions/check/xxx",
		// region stats needs to be collected in API mode.
		// We need to think of a better way to reduce this part of the cost in the future.
		if hasRegionStats && c.regionStats.RegionStatsNeedUpdate(region) {
			ctx.MiscRunner.RunTask(
				regionID,
				ratelimit.ObserveRegionStatsAsync,
				func(ctx context.Context) {
					cluster.Collect(ctx, c, region)
				},
			)
		}
		// region is not updated to the subtree.
		if origin.GetRef() < 2 {
			ctx.TaskRunner.RunTask(
				regionID,
				ratelimit.UpdateSubTree,
				func(context.Context) {
					c.CheckAndPutSubTree(region)
				},
				ratelimit.WithRetained(true),
			)
		}
		return nil
	}
	failpoint.Inject("concurrentRegionHeartbeat", func() {
		time.Sleep(500 * time.Millisecond)
	})
	tracer.OnSaveCacheBegin()
	var overlaps []*core.RegionInfo
	if saveCache {
		failpoint.Inject("decEpoch", func() {
			region = region.Clone(core.SetRegionConfVer(2), core.SetRegionVersion(2))
		})
		// To prevent a concurrent heartbeat of another region from overriding the up-to-date region info by a stale one,
		// check its validation again here.
		//
		// However, it can't solve the race condition of concurrent heartbeats from the same region.
		if overlaps, err = c.CheckAndPutRootTree(ctx, region); err != nil {
			tracer.OnSaveCacheFinished()
			return err
		}
		ctx.TaskRunner.RunTask(
			regionID,
			ratelimit.UpdateSubTree,
			func(context.Context) {
				c.CheckAndPutSubTree(region)
			},
			ratelimit.WithRetained(retained),
		)
		tracer.OnUpdateSubTreeFinished()

		if !c.IsServiceIndependent(constant.SchedulingServiceName) {
			ctx.MiscRunner.RunTask(
				regionID,
				ratelimit.HandleOverlaps,
				func(ctx context.Context) {
					cluster.HandleOverlaps(ctx, c, overlaps)
				},
			)
		}
		regionUpdateCacheEventCounter.Inc()
	}

	tracer.OnSaveCacheFinished()
	if hasRegionStats {
		// handle region stats
		ctx.MiscRunner.RunTask(
			regionID,
			ratelimit.CollectRegionStatsAsync,
			func(ctx context.Context) {
				// TODO: Due to the accuracy requirements of the API "/regions/check/xxx",
				// region stats needs to be collected in API mode.
				// We need to think of a better way to reduce this part of the cost in the future.
				cluster.Collect(ctx, c, region)
			},
		)
	}

	tracer.OnCollectRegionStatsFinished()
	if c.storage != nil {
		if saveKV {
			ctx.MiscRunner.RunTask(
				regionID,
				ratelimit.SaveRegionToKV,
				func(context.Context) {
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
					if err := c.storage.SaveRegion(region.GetMeta()); err != nil {
						log.Error("failed to save region to storage",
							zap.Uint64("region-id", region.GetID()),
							logutil.ZapRedactStringer("region-meta", core.RegionToHexMeta(region.GetMeta())),
							errs.ZapError(err))
					}
					regionUpdateKVEventCounter.Inc()
				},
			)
		}
	}

	if saveKV || needSync {
		select {
		case c.changedRegions <- region:
		default:
		}
	}
	return nil
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
	return c.BasicCluster
}

// UpdateStoreLabels updates a store's location labels
// If 'force' is true, the origin labels will be overwritten with the new one forcibly.
func (c *RaftCluster) UpdateStoreLabels(storeID uint64, labels []*metapb.StoreLabel, force bool) error {
	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrInvalidStoreID.FastGenByArgs(storeID)
	}
	newStore := typeutil.DeepClone(store.GetMeta(), core.StoreFactory)
	newStore.Labels = labels
	return c.putStoreImpl(newStore, force)
}

// DeleteStoreLabel updates a store's location labels
func (c *RaftCluster) DeleteStoreLabel(storeID uint64, labelKey string) error {
	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrInvalidStoreID.FastGenByArgs(storeID)
	}
	if len(store.GetLabels()) == 0 {
		return errors.Errorf("the label key %s does not exist", labelKey)
	}
	newStore := typeutil.DeepClone(store.GetMeta(), core.StoreFactory)
	labels := make([]*metapb.StoreLabel, 0, len(newStore.GetLabels())-1)
	for _, label := range newStore.GetLabels() {
		if label.Key == labelKey {
			continue
		}
		labels = append(labels, label)
	}
	if len(labels) == len(store.GetLabels()) {
		return errors.Errorf("the label key %s does not exist", labelKey)
	}
	newStore.Labels = labels
	return c.putStoreImpl(newStore, true)
}

// PutMetaStore puts a store.
func (c *RaftCluster) PutMetaStore(store *metapb.Store) error {
	if err := c.putStoreImpl(store, false); err != nil {
		return err
	}
	c.OnStoreVersionChange()
	c.AddStoreLimit(store)
	return nil
}

// putStoreImpl puts a store.
// If 'force' is true, the store's labels will overwrite those labels which already existed in the store.
// If 'force' is false, the store's labels will merge into those labels which already existed in the store.
func (c *RaftCluster) putStoreImpl(store *metapb.Store, force bool) error {
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
			labels = core.MergeLabels(s.GetLabels(), labels)
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
	return c.setStore(s)
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
		if key == core.EngineKey {
			continue
		}
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
	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline, physicallyDestroyed))
	log.Warn("store has been offline",
		zap.Uint64("store-id", storeID),
		zap.String("store-address", newStore.GetAddress()),
		zap.Bool("physically-destroyed", newStore.IsPhysicallyDestroyed()))

	if err := c.setStore(newStore); err != nil {
		return err
	}
	regionSize := float64(c.GetStoreRegionSize(storeID))
	c.resetProgress(storeID, store.GetAddress())
	c.progressManager.AddProgress(encodeRemovingProgressKey(storeID), regionSize, regionSize, nodeStateCheckJobInterval, progress.WindowDurationOption(c.GetCoordinator().GetPatrolRegionsDuration()))
	// record the current store limit in memory
	c.prevStoreLimit[storeID] = map[storelimit.Type]float64{
		storelimit.AddPeer:    c.GetStoreLimitByType(storeID, storelimit.AddPeer),
		storelimit.RemovePeer: c.GetStoreLimitByType(storeID, storelimit.RemovePeer),
	}
	// TODO: if the persist operation encounters error, the "Unlimited" will be rollback.
	// And considering the store state has changed, RemoveStore is actually successful.
	_ = c.SetStoreLimit(storeID, storelimit.RemovePeer, storelimit.Unlimited)
	return nil
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

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	log.Warn("store has been Tombstone",
		zap.Uint64("store-id", storeID),
		zap.String("store-address", newStore.GetAddress()),
		zap.String("state", store.GetState().String()),
		zap.Bool("physically-destroyed", store.IsPhysicallyDestroyed()))
	err := c.setStore(newStore)
	c.OnStoreVersionChange()
	if err == nil {
		// clean up the residual information.
		delete(c.prevStoreLimit, storeID)
		c.RemoveStoreLimit(storeID)
		addr := store.GetAddress()
		c.resetProgress(storeID, addr)
		storeIDStr := strconv.FormatUint(storeID, 10)
		statistics.ResetStoreStatistics(addr, storeIDStr)
		if !c.IsServiceIndependent(constant.SchedulingServiceName) {
			c.removeStoreStatistics(storeID)
		}
	}
	return err
}

// NeedAwakenAllRegionsInStore checks whether we should do AwakenRegions operation.
func (c *RaftCluster) NeedAwakenAllRegionsInStore(storeID uint64) (needAwaken bool, slowStoreIDs []uint64) {
	store := c.GetStore(storeID)

	// If there did no exist slow stores, following checking can be skipped.
	if !c.slowStat.ExistsSlowStores() {
		return false, nil
	}
	// We just return AwakenRegions messages to those Serving stores which need to be awaken.
	if store.IsSlow() || !store.NeedAwakenStore() {
		return false, nil
	}

	needAwaken = false
	for _, store := range c.GetStores() {
		if store.IsRemoved() {
			continue
		}

		// We will filter out heartbeat requests from slowStores.
		if (store.IsUp() || store.IsRemoving()) && store.IsSlow() &&
			store.GetStoreStats().GetStoreId() != storeID {
			needAwaken = true
			slowStoreIDs = append(slowStoreIDs, store.GetID())
		}
	}
	return needAwaken, slowStoreIDs
}

// UpStore up a store from offline
func (c *RaftCluster) UpStore(storeID uint64) error {
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

	options := []core.StoreCreateOption{core.SetStoreState(metapb.StoreState_Up)}
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
	err := c.setStore(newStore)
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

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Up))
	log.Info("store has changed to serving",
		zap.Uint64("store-id", storeID),
		zap.String("store-address", newStore.GetAddress()))
	err := c.setStore(newStore)
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

	return c.setStore(newStore)
}

func (c *RaftCluster) setStore(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.SaveStoreMeta(store.GetMeta()); err != nil {
			return err
		}
	}
	c.PutStore(store)
	if !c.IsServiceIndependent(constant.SchedulingServiceName) {
		c.updateStoreStatistics(store.GetID(), store.IsSlow())
	}
	return nil
}

func (c *RaftCluster) isStorePrepared() bool {
	for _, store := range c.GetStores() {
		if !store.IsPreparing() && !store.IsServing() {
			continue
		}
		storeID := store.GetID()
		if !c.IsStorePrepared(storeID) {
			return false
		}
	}
	return true
}

func (c *RaftCluster) checkStores() {
	var offlineStores []*metapb.Store
	var upStoreCount int
	stores := c.GetStores()

	for _, store := range stores {
		// the store has already been tombstone
		if store.IsRemoved() {
			if store.DownTime() > gcTombstoneInterval {
				err := c.deleteStore(store)
				if err != nil {
					log.Error("auto gc the tombstone store failed",
						zap.Stringer("store", store.GetMeta()),
						zap.Duration("down-time", store.DownTime()),
						errs.ZapError(err))
				} else {
					log.Info("auto gc the tombstone store success", zap.Stringer("store", store.GetMeta()), zap.Duration("down-time", store.DownTime()))
				}
			}
			continue
		}

		storeID := store.GetID()
		if store.IsPreparing() {
			if store.GetUptime() >= c.opt.GetMaxStorePreparingTime() || c.GetTotalRegionCount() < core.InitClusterRegionThreshold {
				if err := c.ReadyToServe(storeID); err != nil {
					log.Error("change store to serving failed",
						zap.Stringer("store", store.GetMeta()),
						zap.Int("region-count", c.GetTotalRegionCount()),
						errs.ZapError(err))
				}
			} else if c.IsPrepared() || (c.IsServiceIndependent(constant.SchedulingServiceName) && c.isStorePrepared()) {
				threshold := c.getThreshold(stores, store)
				regionSize := float64(store.GetRegionSize())
				log.Debug("store serving threshold", zap.Uint64("store-id", storeID), zap.Float64("threshold", threshold), zap.Float64("region-size", regionSize))
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
		regionSize := c.GetStoreRegionSize(id)
		if c.IsPrepared() {
			c.updateProgress(id, store.GetAddress(), removingAction, float64(regionSize), float64(regionSize), false /* dec */)
		}
		// If the store is empty, it can be buried.
		needBury := c.GetStoreRegionCount(id) == 0
		failpoint.Inject("doNotBuryStore", func(_ failpoint.Value) {
			needBury = false
		})
		if needBury {
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
		regionSize := c.GetRegionSizeByRange([]byte(""), []byte("")) * int64(c.opt.GetMaxReplicas())
		weight := getStoreTopoWeight(store, stores, c.opt.GetLocationLabels(), c.opt.GetMaxReplicas())
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
		regionSize := c.GetRegionSizeByRange(startKey, endKey) * int64(rule.Count)
		weight := getStoreTopoWeight(store, matchStores, rule.LocationLabels, rule.Count)
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

func getStoreTopoWeight(store *core.StoreInfo, stores []*core.StoreInfo, locationLabels []string, count int) float64 {
	topology, validLabels, sameLocationStoreNum, isMatch := buildTopology(store, stores, locationLabels, count)
	weight := 1.0
	topo := topology
	if isMatch {
		return weight / float64(count) / sameLocationStoreNum
	}

	storeLabels := getSortedLabels(store.GetLabels(), locationLabels)
	for _, label := range storeLabels {
		if _, ok := topo[label.Value]; ok {
			if slice.Contains(validLabels, label.Key) {
				weight /= float64(len(topo))
			}
			topo = topo[label.Value].(map[string]any)
		} else {
			break
		}
	}

	return weight / sameLocationStoreNum
}

func buildTopology(s *core.StoreInfo, stores []*core.StoreInfo, locationLabels []string, count int) (map[string]any, []string, float64, bool) {
	topology := make(map[string]any)
	sameLocationStoreNum := 1.0
	totalLabelCount := make([]int, len(locationLabels))
	for _, store := range stores {
		if store.IsServing() || store.IsPreparing() {
			labelCount := updateTopology(topology, getSortedLabels(store.GetLabels(), locationLabels))
			for i, c := range labelCount {
				totalLabelCount[i] += c
			}
		}
	}

	validLabels := locationLabels
	var isMatch bool
	for i, c := range totalLabelCount {
		if count/c == 0 {
			validLabels = validLabels[:i]
			break
		}
		if count/c == 1 && count%c == 0 {
			validLabels = validLabels[:i+1]
			isMatch = true
			break
		}
	}
	for _, store := range stores {
		if store.GetID() == s.GetID() {
			continue
		}
		if s.CompareLocation(store, validLabels) == -1 {
			sameLocationStoreNum++
		}
	}

	return topology, validLabels, sameLocationStoreNum, isMatch
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
func updateTopology(topology map[string]any, sortedLabels []*metapb.StoreLabel) []int {
	labelCount := make([]int, len(sortedLabels))
	if len(sortedLabels) == 0 {
		return labelCount
	}
	topo := topology
	for i, l := range sortedLabels {
		if _, exist := topo[l.Value]; !exist {
			topo[l.Value] = make(map[string]any)
			labelCount[i] += 1
		}
		topo = topo[l.Value].(map[string]any)
	}
	return labelCount
}

func (c *RaftCluster) updateProgress(storeID uint64, storeAddress, action string, current, remaining float64, isInc bool) {
	storeLabel := strconv.FormatUint(storeID, 10)
	var progressName string
	var opts []progress.Option
	switch action {
	case removingAction:
		progressName = encodeRemovingProgressKey(storeID)
		opts = []progress.Option{progress.WindowDurationOption(c.GetCoordinator().GetPatrolRegionsDuration())}
	case preparingAction:
		progressName = encodePreparingProgressKey(storeID)
	}

	if exist := c.progressManager.AddProgress(progressName, current, remaining, nodeStateCheckJobInterval, opts...); !exist {
		return
	}
	c.progressManager.UpdateProgress(progressName, current, remaining, isInc, opts...)
	progress, leftSeconds, currentSpeed, err := c.progressManager.Status(progressName)
	if err != nil {
		log.Error("get progress status failed", zap.String("progress", progressName), zap.Float64("remaining", remaining), errs.ZapError(err))
		return
	}
	storesProgressGauge.WithLabelValues(storeAddress, storeLabel, action).Set(progress)
	storesSpeedGauge.WithLabelValues(storeAddress, storeLabel, action).Set(currentSpeed)
	storesETAGauge.WithLabelValues(storeAddress, storeLabel, action).Set(leftSeconds)
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
	var failedStores []uint64
	for _, store := range c.GetStores() {
		if store.IsRemoved() {
			if c.GetStoreRegionCount(store.GetID()) > 0 {
				log.Warn("skip removing tombstone", zap.Stringer("store", store.GetMeta()))
				failedStores = append(failedStores, store.GetID())
				continue
			}
			// the store has already been tombstone
			err := c.deleteStore(store)
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

// deleteStore deletes the store from the cluster. it's concurrent safe.
func (c *RaftCluster) deleteStore(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.DeleteStoreMeta(store.GetMeta()); err != nil {
			return err
		}
	}
	c.DeleteStore(store)
	return nil
}

func (c *RaftCluster) collectMetrics() {
	c.collectHealthStatus()
}

func (c *RaftCluster) resetMetrics() {
	c.resetHealthStatus()
	c.resetProgressIndicator()
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

func (*RaftCluster) resetHealthStatus() {
	healthStatusGauge.Reset()
}

func (c *RaftCluster) resetProgressIndicator() {
	c.progressManager.Reset()
	storesProgressGauge.Reset()
	storesSpeedGauge.Reset()
	storesETAGauge.Reset()
}

// OnStoreVersionChange changes the version of the cluster when needed.
func (c *RaftCluster) OnStoreVersionChange() {
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
	return typeutil.DeepClone(c.meta, core.ClusterFactory)
}

// PutMetaCluster puts meta cluster.
func (c *RaftCluster) PutMetaCluster(meta *metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()
	if meta.GetId() != keypath.ClusterID() {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, keypath.ClusterID())
	}
	return c.putMetaLocked(typeutil.DeepClone(meta, core.ClusterFactory))
}

// GetRegionStatsByRange returns region statistics from cluster.
func (c *RaftCluster) GetRegionStatsByRange(startKey, endKey []byte) *statistics.RegionStats {
	return statistics.GetRegionStats(c.ScanRegions(startKey, endKey, -1))
}

// GetRegionStatsCount returns the number of regions in the range.
func (c *RaftCluster) GetRegionStatsCount(startKey, endKey []byte) *statistics.RegionStats {
	stats := &statistics.RegionStats{}
	stats.Count = c.GetRegionCount(startKey, endKey)
	return stats
}

// TODO: remove me.
// only used in test.
func (c *RaftCluster) putRegion(region *core.RegionInfo) error {
	if c.storage != nil {
		if err := c.storage.SaveRegion(region.GetMeta()); err != nil {
			return err
		}
	}
	c.PutRegion(region)
	return nil
}

// GetStoreLimitByType returns the store limit for a given store ID and type.
func (c *RaftCluster) GetStoreLimitByType(storeID uint64, typ storelimit.Type) float64 {
	return c.opt.GetStoreLimitByType(storeID, typ)
}

// GetAllStoresLimit returns all store limit
func (c *RaftCluster) GetAllStoresLimit() map[uint64]sc.StoreLimitConfig {
	return c.opt.GetAllStoresLimit()
}

// AddStoreLimit add a store limit for a given store ID.
func (c *RaftCluster) AddStoreLimit(store *metapb.Store) {
	storeID := store.GetId()
	cfg := c.opt.GetScheduleConfig().Clone()
	if _, ok := cfg.StoreLimit[storeID]; ok {
		return
	}

	slc := sc.StoreLimitConfig{
		AddPeer:    sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer),
		RemovePeer: sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer),
	}
	if core.IsStoreContainLabel(store, core.EngineKey, core.EngineTiFlash) {
		slc = sc.StoreLimitConfig{
			AddPeer:    sc.DefaultTiFlashStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer),
			RemovePeer: sc.DefaultTiFlashStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer),
		}
	}

	cfg.StoreLimit[storeID] = slc
	c.opt.SetScheduleConfig(cfg)
	var err error
	for range persistLimitRetryTimes {
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
		c.ResetStoreLimit(storeID, limitType)
	}
	delete(cfg.StoreLimit, storeID)
	c.opt.SetScheduleConfig(cfg)
	var err error
	for range persistLimitRetryTimes {
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
	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}

	newStore := store.Clone(core.SetMinResolvedTS(minResolvedTS))
	c.PutStore(newStore)
	return nil
}

// CheckAndUpdateMinResolvedTS checks and updates the min resolved ts of the cluster.
// It only be called by the background job runMinResolvedTSJob.
// This is exported for testing purpose.
func (c *RaftCluster) CheckAndUpdateMinResolvedTS() (uint64, bool) {
	if !c.isInitialized() {
		return math.MaxUint64, false
	}
	newMinResolvedTS := uint64(math.MaxUint64)
	for _, s := range c.GetStores() {
		if !core.IsAvailableForMinResolvedTS(s) {
			continue
		}
		if newMinResolvedTS > s.GetMinResolvedTS() {
			newMinResolvedTS = s.GetMinResolvedTS()
		}
	}
	oldMinResolvedTS := c.minResolvedTS.Load().(uint64)
	if newMinResolvedTS == math.MaxUint64 || newMinResolvedTS <= oldMinResolvedTS {
		return oldMinResolvedTS, false
	}
	c.minResolvedTS.Store(newMinResolvedTS)
	return newMinResolvedTS, true
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

	for {
		select {
		case <-c.ctx.Done():
			log.Info("min resolved ts background jobs has been stopped")
			return
		case <-ticker.C:
			interval = c.opt.GetMinResolvedTSPersistenceInterval()
			if interval != 0 {
				if current, needPersist := c.CheckAndUpdateMinResolvedTS(); needPersist {
					if err := c.storage.SaveMinResolvedTS(current); err != nil {
						log.Error("persist min resolved ts meet error", errs.ZapError(err))
					}
				}
			} else {
				// If interval in config is zero, it means not to persist resolved ts and check config with this interval
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
	c.minResolvedTS.Store(minResolvedTS)
}

// GetMinResolvedTS returns the min resolved ts of the cluster.
func (c *RaftCluster) GetMinResolvedTS() uint64 {
	if !c.isInitialized() {
		return math.MaxUint64
	}
	return c.minResolvedTS.Load().(uint64)
}

// GetStoreMinResolvedTS returns the min resolved ts of the store.
func (c *RaftCluster) GetStoreMinResolvedTS(storeID uint64) uint64 {
	store := c.GetStore(storeID)
	if store == nil {
		return math.MaxUint64
	}
	if !c.isInitialized() || !core.IsAvailableForMinResolvedTS(store) {
		return math.MaxUint64
	}
	return store.GetMinResolvedTS()
}

// GetMinResolvedTSByStoreIDs returns the min_resolved_ts for each store
// and returns the min_resolved_ts for all given store lists.
func (c *RaftCluster) GetMinResolvedTSByStoreIDs(ids []uint64) (uint64, map[uint64]uint64) {
	minResolvedTS := uint64(math.MaxUint64)
	storesMinResolvedTS := make(map[uint64]uint64)
	for _, storeID := range ids {
		storeTS := c.GetStoreMinResolvedTS(storeID)
		storesMinResolvedTS[storeID] = storeTS
		if minResolvedTS > storeTS {
			minResolvedTS = storeTS
		}
	}
	return minResolvedTS, storesMinResolvedTS
}

// GetExternalTS returns the external timestamp.
func (c *RaftCluster) GetExternalTS() uint64 {
	if !c.isInitialized() {
		return math.MaxUint64
	}
	return c.externalTS.Load().(uint64)
}

// SetExternalTS sets the external timestamp.
func (c *RaftCluster) SetExternalTS(timestamp uint64) error {
	c.externalTS.Store(timestamp)
	return c.storage.SaveExternalTS(timestamp)
}

func (c *RaftCluster) loadExternalTS() {
	// Use `c.GetStorage()` here to prevent from the data race in test.
	externalTS, err := c.GetStorage().LoadExternalTS()
	if err != nil {
		log.Error("load external ts meet error", errs.ZapError(err))
		return
	}
	c.externalTS.Store(externalTS)
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
	oldAdd := sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.AddPeer)
	oldRemove := sc.DefaultStoreLimit.GetDefaultStoreLimit(storelimit.RemovePeer)
	c.opt.SetAllStoresLimit(typ, ratePerMin)
	if err := c.opt.Persist(c.storage); err != nil {
		// roll back the store limit
		c.opt.SetScheduleConfig(old)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.AddPeer, oldAdd)
		sc.DefaultStoreLimit.SetDefaultStoreLimit(storelimit.RemovePeer, oldRemove)
		log.Error("persist store limit meet error", errs.ZapError(err))
		return err
	}
	log.Info("all store limit changed", zap.String("type", typ.String()), zap.Float64("rate-per-min", ratePerMin))
	return nil
}

// SetAllStoresLimitTTL sets all store limit for a given type and rate with ttl.
func (c *RaftCluster) SetAllStoresLimitTTL(typ storelimit.Type, ratePerMin float64, ttl time.Duration) error {
	return c.opt.SetAllStoresLimitTTL(c.ctx, c.etcdClient, typ, ratePerMin, ttl)
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
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s%s", cURL, healthURL), http.NoBody)
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
	listResp, err := etcdutil.ListEtcdMembers(etcdClient.Ctx(), etcdClient)
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

// IsServiceIndependent returns whether the service is independent.
func (c *RaftCluster) IsServiceIndependent(name string) bool {
	_, exist := c.independentServices.Load(name)
	return exist
}

// SetServiceIndependent sets the service to be independent.
func (c *RaftCluster) SetServiceIndependent(name string) {
	c.independentServices.Store(name, struct{}{})
}

// UnsetServiceIndependent unsets the service to be independent.
func (c *RaftCluster) UnsetServiceIndependent(name string) {
	c.independentServices.Delete(name)
}
