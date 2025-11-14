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
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/cluster"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/gctuner"
	"github.com/tikv/pd/pkg/id"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/memory"
	"github.com/tikv/pd/pkg/metering"
	"github.com/tikv/pd/pkg/progress"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/replication"
	"github.com/tikv/pd/pkg/schedule/affinity"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/keyrange"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/syncer"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/unsaferecovery"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/netutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"github.com/tikv/pd/server/config"
)

var (
	// DefaultMinResolvedTSPersistenceInterval is the default value of min resolved ts persistence interval.
	// If interval in config is zero, it means not to persist resolved ts and check config with this DefaultMinResolvedTSPersistenceInterval
	DefaultMinResolvedTSPersistenceInterval = config.DefaultMinResolvedTSPersistenceInterval
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	regionUpdateCacheEventCounter = regionEventCounter.WithLabelValues("update_cache")
	regionUpdateKVEventCounter    = regionEventCounter.WithLabelValues("update_kv")
	regionCacheMissCounter        = bucketEventCounter.WithLabelValues("region_cache_miss")
	versionStaleCounter           = bucketEventCounter.WithLabelValues("version_stale")
	versionNotChangeCounter       = bucketEventCounter.WithLabelValues("version_no_change")
	updateFailedCounter           = bucketEventCounter.WithLabelValues("update_failed")
	updateSuccessCounter          = bucketEventCounter.WithLabelValues("update_success")
)

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
	gcTunerCheckCfgInterval = 10 * time.Second
	// regionLabelGCInterval is the interval to run region-label's GC work.
	regionLabelGCInterval = time.Hour
	// storageSizeCollectorInterval is the interval to run storage size collector.
	storageSizeCollectorInterval = time.Minute
	// dfsStatsCollectorInterval is the interval to run store stats collector.
	dfsStatsCollectorInterval = time.Minute

	// minSnapshotDurationSec is the minimum duration that a store can tolerate.
	// It should enlarge the limiter if the snapshot's duration is less than this value.
	minSnapshotDurationSec = 5

	// heartbeat relative const
	heartbeatTaskRunner  = "heartbeat-async"
	miscTaskRunner       = "misc-async"
	logTaskRunner        = "log-async"
	syncRegionTaskRunner = "sync-region-async"
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
	GetKeyspaceManager() *keyspace.Manager
	GetKeyspaceGroupManager() *keyspace.GroupManager
	IsKeyspaceGroupEnabled() bool
	GetMeteringWriter() *metering.Writer
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
	storeStateLock *syncutil.LockGroup
	wg             sync.WaitGroup

	serverCtx context.Context
	ctx       context.Context
	cancel    context.CancelFunc

	*core.BasicCluster // cached cluster info
	member             *member.Member

	etcdClient *clientv3.Client
	httpClient *http.Client

	running                bool
	isKeyspaceGroupEnabled bool
	meta                   *metapb.Cluster
	storage                storage.Storage
	minResolvedTS          atomic.Value // Store as uint64
	externalTS             atomic.Value // Store as uint64

	// Keep the previous store limit settings when removing a store.
	prevStoreLimit sync.Map // map[uint64]map[storelimit.Type]float64

	// This below fields are all read-only, we cannot update itself after the raft cluster starts.
	id  id.Allocator
	opt *config.PersistOptions
	*schedulingController
	affinityManager          *affinity.Manager
	ruleManager              *placement.RuleManager
	keyRangeManager          *keyrange.Manager
	regionLabeler            *labeler.RegionLabeler
	replicationMode          *replication.ModeManager
	unsafeRecoveryController *unsaferecovery.Controller
	progressManager          *progress.Manager
	regionSyncer             *syncer.RegionSyncer
	changedRegions           chan *core.RegionInfo
	keyspaceGroupManager     *keyspace.GroupManager
	independentServices      sync.Map
	hbstreams                *hbstream.HeartbeatStreams
	tsoAllocator             *tso.Allocator

	// heartbeatRunner is used to process the subtree update task asynchronously.
	heartbeatRunner ratelimit.Runner
	// miscRunner is used to process the statistics and persistent tasks asynchronously.
	miscRunner ratelimit.Runner
	// logRunner is used to process the log asynchronously.
	logRunner ratelimit.Runner
	// syncRegionRunner is used to sync region asynchronously.
	syncRegionRunner ratelimit.Runner
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
	member *member.Member,
	basicCluster *core.BasicCluster,
	storage storage.Storage,
	regionSyncer *syncer.RegionSyncer,
	etcdClient *clientv3.Client,
	httpClient *http.Client,
	tsoAllocator *tso.Allocator,
) *RaftCluster {
	return &RaftCluster{
		serverCtx:      ctx,
		storeStateLock: syncutil.NewLockGroup(syncutil.WithRemoveEntryOnUnlock(true)),
		member:         member,
		regionSyncer:   regionSyncer,
		httpClient:     httpClient,
		etcdClient:     etcdClient,
		BasicCluster:   basicCluster,
		storage:        storage,
		tsoAllocator:   tsoAllocator,
		heartbeatRunner: ratelimit.NewConcurrentRunner(heartbeatTaskRunner,
			ratelimit.NewConcurrencyLimiter(uint64(runtime.NumCPU()*2)), time.Minute),
		miscRunner: ratelimit.NewConcurrentRunner(miscTaskRunner,
			ratelimit.NewConcurrencyLimiter(uint64(runtime.NumCPU()*2)), time.Minute),
		logRunner: ratelimit.NewConcurrentRunner(logTaskRunner,
			ratelimit.NewConcurrencyLimiter(uint64(runtime.NumCPU()*2)), time.Minute),
		syncRegionRunner: ratelimit.NewConcurrentRunner(syncRegionTaskRunner,
			ratelimit.NewConcurrencyLimiter(1), time.Minute),
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
	data, err := c.storage.Load(keypath.ClusterBootstrapTimePath())
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
	c.changedRegions = make(chan *core.RegionInfo, defaultChangedRegionsLimit)
	failpoint.Inject("syncRegionChannelFull", func() {
		c.changedRegions = make(chan *core.RegionInfo, 100)
	})
	c.unsafeRecoveryController = unsaferecovery.NewController(c)
	c.keyspaceGroupManager = keyspaceGroupManager
	c.hbstreams = hbstreams
	c.ruleManager = placement.NewRuleManager(c.ctx, c.storage, c, c.GetOpts())
	c.affinityManager = affinity.NewManager(c.ctx, c.storage, c, c.GetOpts())
	c.keyRangeManager = keyrange.NewManager()
	c.schedulingController = newSchedulingController(c.ctx, c.BasicCluster, c.opt, c.ruleManager) // TODO: pass affinity manager
	return nil
}

// Start starts a cluster.
func (c *RaftCluster) Start(s Server, bootstrap bool) (err error) {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}
	c.isKeyspaceGroupEnabled = s.IsKeyspaceGroupEnabled()
	err = c.InitCluster(s.GetAllocator(), s.GetPersistOptions(), s.GetHBStreams(), s.GetKeyspaceGroupManager())
	if err != nil {
		return err
	}
	// We should not manage tso service when bootstrap try to start raft cluster.
	// It only is controlled by leader election.
	// Ref: https://github.com/tikv/pd/issues/8836
	if !bootstrap {
		c.checkTSOService()
	}
	defer func() {
		if !bootstrap && err != nil {
			c.stopTSOJobsIfNeeded()
		}
	}()
	failpoint.Inject("raftClusterReturn", func(val failpoint.Value) {
		if val, ok := val.(bool); (ok && val) || !ok {
			err = errors.New("raftClusterReturn")
		} else {
			err = nil
		}
		failpoint.Return(err)
	})
	cluster, err := c.LoadClusterInfo()
	if err != nil {
		return err
	}
	if cluster == nil {
		log.Warn("cluster is not bootstrapped")
		return nil
	}
	if c.opt.IsPlacementRulesEnabled() {
		err := c.ruleManager.Initialize(c.opt.GetMaxReplicas(), c.opt.GetLocationLabels(), c.opt.GetIsolationLevel(), false)
		if err != nil {
			return err
		}
	}

	c.regionLabeler, err = labeler.NewRegionLabeler(c.ctx, c.storage, regionLabelGCInterval)
	if err != nil {
		return err
	}

	err = c.affinityManager.Initialize()
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

	if c.isKeyspaceGroupEnabled {
		// bootstrap keyspace group manager after starting other parts successfully.
		// This order avoids a stuck goroutine in keyspaceGroupManager when it fails to create raftcluster.
		err = c.keyspaceGroupManager.Bootstrap(c.ctx)
		if err != nil {
			return err
		}
	}
	c.checkSchedulingService()
	c.wg.Add(12)
	go c.runServiceCheckJob()
	go c.runMetricsCollectionJob()
	go c.runNodeStateCheckJob()
	go c.syncRegions()
	go c.runReplicationMode()
	go c.runMinResolvedTSJob()
	go c.runStoreConfigSync()
	go c.runUpdateStoreStats()
	go c.startGCTuner()
	go c.startProgressGC()
	go c.runStorageSizeCollector(s.GetMeteringWriter(), c.regionLabeler, s.GetKeyspaceManager())
	go c.runDFSStatsCollector(s.GetMeteringWriter(), s.GetKeyspaceManager())

	c.running = true
	c.heartbeatRunner.Start(c.ctx)
	c.miscRunner.Start(c.ctx)
	c.logRunner.Start(c.ctx)
	c.syncRegionRunner.Start(c.ctx)
	return nil
}

func (c *RaftCluster) checkSchedulingService() {
	if c.isKeyspaceGroupEnabled {
		servers, err := discovery.Discover(c.etcdClient, constant.SchedulingServiceName)
		if c.opt.GetMicroserviceConfig().IsSchedulingFallbackEnabled() && (err != nil || len(servers) == 0) {
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
	if c.progressManager == nil {
		c.progressManager = progress.NewManager(c.GetCoordinator().GetCheckerController(),
			nodeStateCheckJobInterval)
	} else {
		c.progressManager.SetPatrolRegionsDurationGetter(c.GetCoordinator().GetCheckerController())
	}
}

// checkTSOService checks the TSO service.
func (c *RaftCluster) checkTSOService() {
	if c.isKeyspaceGroupEnabled {
		if c.opt.GetMicroserviceConfig().IsTSODynamicSwitchingEnabled() {
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
	if !c.tsoAllocator.IsInitialize() {
		log.Info("initializing the TSO allocator")
		if err := c.tsoAllocator.Initialize(); err != nil {
			log.Error("failed to initialize the TSO allocator", errs.ZapError(err))
			return err
		}
	} else if !c.running {
		// If the TSO allocator is already initialized, but the running flag is false,
		// it means there maybe unexpected error happened before.
		log.Warn("the TSO allocator is already initialized before, but the cluster is not running")
	}
	return nil
}

func (c *RaftCluster) stopTSOJobsIfNeeded() {
	if !c.tsoAllocator.IsInitialize() {
		return
	}
	log.Info("closing the TSO allocator")
	c.tsoAllocator.Reset(false)
	failpoint.Inject("updateAfterResetTSO", func() {
		if err := c.tsoAllocator.UpdateTSO(); !errorspkg.Is(err, errs.ErrUpdateTimestamp) {
			log.Panic("the tso update after reset should return ErrUpdateTimestamp as expected", zap.Error(err))
		}
		if c.tsoAllocator.IsInitialize() {
			log.Panic("the tso allocator should be uninitialized after reset")
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
		log.Fatal("fail to get total memory", zap.Error(err))
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

func (c *RaftCluster) startProgressGC() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	c.progressManager.GC(c.ctx)
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
		if !store.IsTiKV() {
			continue
		}

		// filter out the stores that are not up.
		if !store.IsPreparing() && !store.IsServing() {
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
	switchRaftV2 = oldCfg.Engine != sc.RaftstoreV2 && cfg.Engine == sc.RaftstoreV2
	return
}

// fetchStoreConfigFromTiKV tries to fetch the config from the TiKV store URL.
func (c *RaftCluster) fetchStoreConfigFromTiKV(ctx context.Context, statusAddress string) (*sc.StoreConfig, error) {
	cfg := &sc.StoreConfig{}
	failpoint.Inject("mockFetchStoreConfigFromTiKV", func(val failpoint.Value) {
		if regionMaxSize, ok := val.(string); ok {
			cfg.RegionMaxSize = regionMaxSize
			cfg.Engine = sc.RaftstoreV2
		}
		failpoint.Return(cfg, nil)
	})
	if c.httpClient == nil {
		return nil, errors.New("failed to get store config due to nil client")
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
		ticker.Reset(100 * time.Millisecond)
	})
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Info("node state check job has been stopped")
			return
		case <-ticker.C:
			failpoint.InjectCall("blockCheckStores")
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
	c.syncRegionRunner.Stop()
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
func (c *RaftCluster) AllocID(uint32) (uint64, uint32, error) {
	return c.id.Alloc(1)
}

// GetPrepareRegionCount returns the count of regions that are in prepare state.
func (c *RaftCluster) GetPrepareRegionCount() (int, error) {
	return c.GetTotalRegionCount(), nil
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

// GetAffinityManager returns the affinity manager reference.
func (c *RaftCluster) GetAffinityManager() *affinity.Manager {
	return c.affinityManager
}

// GetKeyRangeManager returns the key range manager reference
func (c *RaftCluster) GetKeyRangeManager() *keyrange.Manager {
	return c.keyRangeManager
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
	opts := make([]core.StoreCreateOption, 0)
	if limit == nil || limit.Version() != version {
		if version == storelimit.VersionV2 {
			limit = storelimit.NewSlidingWindows()
		} else {
			limit = storelimit.NewStoreRateLimit(0.0)
		}
		opts = append(opts, core.SetStoreLimit(limit))
	}

	nowTime := time.Now()
	// If this cluster has slow stores, we should awaken hibernated regions in other stores.
	if !c.IsServiceIndependent(constant.SchedulingServiceName) {
		if needAwaken, slowStoreIDs := c.NeedAwakenAllRegionsInStore(storeID); needAwaken {
			log.Info("forcely awaken hibernated regions", zap.Uint64("store-id", storeID), zap.Uint64s("slow-stores", slowStoreIDs))
			opts = append(opts, core.SetLastAwakenTime(nowTime))
			resp.AwakenRegions = &pdpb.AwakenRegions{
				AbnormalStores: slowStoreIDs,
			}
		}
	}
	opts = append(opts, core.SetStoreStats(stats), core.SetLastHeartbeatTS(nowTime))

	newStore := store.Clone(opts...)

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
			opts = append(opts, core.SetLastPersistTime(nowTime))
		}
	}
	// Supply NodeState in the response to help the store handle special cases
	// more conveniently, such as avoiding calling `remove_peer` redundantly under
	// NodeState_Removing.
	resp.State = store.GetNodeState()
	c.PutStore(newStore, opts...)
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
	c.adjustNetworkSlowStore(storeID)
	return nil
}

// processRegionBuckets update the bucket information.
func (c *RaftCluster) processRegionBuckets(buckets *metapb.Buckets) error {
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
		if old != nil {
			reportVersion := buckets.GetVersion()
			if reportVersion < old.GetVersion() {
				versionStaleCounter.Inc()
				return nil
			} else if reportVersion == old.GetVersion() {
				versionNotChangeCounter.Inc()
				return nil
			}
		}
		failpoint.Inject("concurrentBucketHeartbeat", func() {
			time.Sleep(500 * time.Millisecond)
		})
		if ok := region.UpdateBuckets(buckets, old); ok {
			updateSuccessCounter.Inc()
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
		// region stats needs to be collected in microservice env.
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
			if len(overlaps) > 0 {
				ctx.MiscRunner.RunTask(
					regionID,
					ratelimit.HandleOverlaps,
					func(ctx context.Context) {
						cluster.HandleOverlaps(ctx, c, overlaps)
					},
				)
			}
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
				// region stats needs to be collected in microservice env.
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
		ctx.SyncRegionRunner.RunTask(
			regionID,
			ratelimit.SyncRegionToFollower,
			func(context.Context) {
				c.changedRegions <- region
			},
			ratelimit.WithRetained(true),
		)
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

	opts := make([]core.StoreCreateOption, 0)
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
		opts = append(opts,
			core.SetStoreAddress(store.Address, store.StatusAddress, store.PeerAddress),
			core.SetStoreVersion(store.GitHash, store.Version),
			core.SetStoreLabels(labels),
			core.SetStoreStartTime(store.StartTimestamp),
			core.SetStoreDeployPath(store.DeployPath))
		// Update an existed store.
		s = s.Clone(opts...)
	}
	if err := c.checkStoreLabels(s); err != nil {
		return err
	}
	return c.setStore(s, opts...)
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
		if key == core.EngineKey || key == core.EngineRoleKey {
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
	c.storeStateLock.Lock(uint32(storeID))
	defer c.storeStateLock.Unlock(uint32(storeID))
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

	log.Warn("store has been offline",
		zap.Uint64("store-id", storeID),
		zap.Int("region-count", store.GetRegionCount()),
		zap.String("store-address", store.GetAddress()),
		zap.Bool("physically-destroyed", physicallyDestroyed))
	if err := c.setStore(
		store.Clone(core.SetStoreState(metapb.StoreState_Offline, physicallyDestroyed)),
		core.SetStoreState(metapb.StoreState_Offline, physicallyDestroyed),
	); err != nil {
		return err
	}

	// record the current store limit in memory
	c.prevStoreLimit.Store(storeID, map[storelimit.Type]float64{
		storelimit.AddPeer:    c.GetStoreLimitByType(storeID, storelimit.AddPeer),
		storelimit.RemovePeer: c.GetStoreLimitByType(storeID, storelimit.RemovePeer),
	})
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
// It is used by unsafe recovery or other special cases.
func (c *RaftCluster) BuryStore(storeID uint64, forceBury bool) error {
	c.storeStateLock.Lock(uint32(storeID))
	defer c.storeStateLock.Unlock(uint32(storeID))
	return c.BuryStoreLocked(storeID, forceBury)
}

// BuryStoreLocked marks a store as tombstone in cluster.
// If forceBury is false, the store should be offlined and emptied before calling this func.
// It is used by cluster check stores.
func (c *RaftCluster) BuryStoreLocked(storeID uint64, forceBury bool) error {
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

	log.Warn("store has been Tombstone",
		zap.Uint64("store-id", storeID),
		zap.String("store-address", store.GetAddress()),
		zap.String("state", store.GetState().String()),
		zap.Bool("physically-destroyed", store.IsPhysicallyDestroyed()))
	err := c.setStore(
		store.Clone(core.SetStoreState(metapb.StoreState_Tombstone)),
		core.SetStoreState(metapb.StoreState_Tombstone),
	)
	c.OnStoreVersionChange()
	if err == nil {
		// clean up the residual information.
		c.prevStoreLimit.Delete(storeID)
		c.RemoveStoreLimit(storeID)
		addr := store.GetAddress()
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
	c.storeStateLock.Lock(uint32(storeID))
	defer c.storeStateLock.Unlock(uint32(storeID))
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
	var limiter map[storelimit.Type]float64
	var exist bool
	if value, ok := c.prevStoreLimit.Load(storeID); ok {
		limiter = value.(map[storelimit.Type]float64)
		exist = true
		options = append(options,
			core.ResetStoreLimit(storelimit.AddPeer, limiter[storelimit.AddPeer]),
			core.ResetStoreLimit(storelimit.RemovePeer, limiter[storelimit.RemovePeer]),
		)
	}
	newStore := store.Clone(options...)
	log.Warn("store has been up",
		zap.Uint64("store-id", storeID),
		zap.String("store-address", newStore.GetAddress()))

	if err := c.setStore(newStore, options...); err != nil {
		return err
	}
	if exist {
		// persist the store limit
		_ = c.SetStoreLimit(storeID, storelimit.AddPeer, limiter[storelimit.AddPeer])
		_ = c.SetStoreLimit(storeID, storelimit.RemovePeer, limiter[storelimit.RemovePeer])
	}
	return nil
}

// ReadyToServeLocked change store's node state to Serving.
func (c *RaftCluster) ReadyToServeLocked(storeID uint64) error {
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
	err := c.setStore(newStore, core.SetStoreState(metapb.StoreState_Up))
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

	return c.setStore(store,
		core.SetLeaderWeight(leaderWeight),
		core.SetRegionWeight(regionWeight),
	)
}

// The meta of StoreInfo should be the latest.
func (c *RaftCluster) setStore(store *core.StoreInfo, opts ...core.StoreCreateOption) error {
	if c.storage != nil {
		if err := c.storage.SaveStoreMeta(store.GetMeta()); err != nil {
			return err
		}
	}
	c.PutStore(store, opts...)
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
	var (
		offlineStores []*metapb.Store
		upStoreCount  int
		stores        = c.GetStores()
	)

	for _, store := range stores {
		storeID := store.GetID()
		isInUp, isInOffline := c.checkStore(storeID)
		if isInUp {
			upStoreCount++
		}
		if isInOffline {
			offlineStores = append(offlineStores, store.GetMeta())
		}
	}

	// When placement rules feature is enabled. It is hard to determine required replica count precisely.
	if !c.opt.IsPlacementRulesEnabled() && upStoreCount < c.opt.GetMaxReplicas() {
		for _, offlineStore := range offlineStores {
			log.Warn("store may not turn into Tombstone, there are no extra up store has enough space to accommodate the extra replica", zap.Stringer("store", offlineStore))
		}
	}
}

func (c *RaftCluster) checkStore(storeID uint64) (isInUp, isInOffline bool) {
	c.storeStateLock.Lock(uint32(storeID))
	defer c.storeStateLock.Unlock(uint32(storeID))

	store := c.GetStore(storeID)
	if store == nil {
		log.Warn("store not found when checking, may be deleted", zap.Uint64("store-id", storeID))
		return false, false
	}

	var (
		regionSize = float64(store.GetRegionSize())
		threshold  float64
	)
	switch store.GetNodeState() {
	case metapb.NodeState_Preparing:
		readyToServe := store.GetUptime() >= c.opt.GetMaxStorePreparingTime() ||
			c.GetTotalRegionCount() < core.InitClusterRegionThreshold
		if !readyToServe && (c.IsPrepared() || (c.IsServiceIndependent(constant.SchedulingServiceName) && c.isStorePrepared())) {
			kr := keyutil.NewKeyRange("", "")
			threshold = c.getThreshold(c.GetStores(), store, &kr)
			log.Debug("store preparing threshold", zap.Uint64("store-id", storeID),
				zap.Float64("threshold", threshold),
				zap.Float64("region-size", regionSize))
			readyToServe = regionSize >= threshold
		}
		if readyToServe {
			if err := c.ReadyToServeLocked(storeID); err != nil {
				log.Error("change store to serving failed",
					zap.Stringer("store", store.GetMeta()),
					zap.Int("region-count", c.GetTotalRegionCount()),
					errs.ZapError(err))
			}
		}
	case metapb.NodeState_Serving:
	case metapb.NodeState_Removing:
		// If the store is empty, it can be buried.
		needBury := regionSize == 0
		failpoint.Inject("doNotBuryStore", func(_ failpoint.Value) {
			needBury = false
		})
		if needBury {
			if err := c.BuryStoreLocked(storeID, false); err != nil {
				log.Error("bury store failed",
					zap.Stringer("store", store.GetMeta()),
					errs.ZapError(err))
			}
		} else {
			isInOffline = true
		}
	case metapb.NodeState_Removed:
		if store.DownTime() <= gcTombstoneInterval {
			break
		}
		// the store has already been tombstone
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
	c.progressManager.UpdateProgress(store, regionSize, threshold)
	// When store is preparing or serving, we think it is in up.
	// `checkStore` only may change store state from preparing to serving.
	// So we don't need to get store again.
	isInUp = store.IsUp() && !store.IsLowSpace(c.opt.GetLowSpaceRatio())
	return isInUp, isInOffline
}

func (c *RaftCluster) getThreshold(stores []*core.StoreInfo, store *core.StoreInfo, kr *keyutil.KeyRange) float64 {
	start := time.Now()
	if !c.opt.IsPlacementRulesEnabled() {
		regionSize := c.GetRegionSizeByRange(kr.StartKey, kr.EndKey) * int64(c.opt.GetMaxReplicas())
		weight := core.GetStoreTopoWeight(store, stores, c.opt.GetLocationLabels(), c.opt.GetMaxReplicas())
		return float64(regionSize) * weight * 0.9
	}

	keys := c.ruleManager.GetSplitKeys(kr.StartKey, kr.EndKey)
	if len(keys) == 0 {
		return c.calculateRange(stores, store, kr.StartKey, kr.EndKey) * 0.9
	}

	storeSize := 0.0
	startKey := kr.StartKey
	for _, key := range keys {
		endKey := key
		storeSize += c.calculateRange(stores, store, startKey, endKey)
		startKey = endKey
	}
	// the range from the last split key to the last key
	storeSize += c.calculateRange(stores, store, startKey, kr.EndKey)
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
		weight := core.GetStoreTopoWeight(store, matchStores, rule.LocationLabels, rule.Count)
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
			stores += strconv.FormatUint(storeID, 10)
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
	c.progressManager.Reset()
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

func (c *RaftCluster) getRegionStats(startKey, endKey []byte, useHot bool, opts ...statistics.GetRegionStatsOption) *statistics.RegionStats {
	stats := statistics.NewRegionStats()
	for {
		regions := c.ScanRegions(startKey, endKey, core.ScanRegionLimit)

		for _, region := range regions {
			if useHot {
				stats.Observe(region, c, opts...)
			} else {
				stats.Observe(region, nil, opts...)
			}
		}
		if len(regions) < core.ScanRegionLimit {
			break
		}

		startKey = regions[len(regions)-1].GetEndKey()
	}
	return stats
}

// GetHotRegionStatusByRange return region statistics from cluster with hot statistics.
func (c *RaftCluster) GetHotRegionStatusByRange(startKey, endKey []byte, engine string) *statistics.RegionStats {
	stores := c.GetStores()
	switch engine {
	case core.EngineTiKV:
		f := filter.NewEngineFilter("user", filter.NotSpecialEngines)
		stores = filter.SelectSourceStores(stores, []filter.Filter{f}, c.GetSchedulerConfig(), nil, nil)
	case core.EngineTiFlash:
		f := filter.NewEngineFilter("user", filter.SpecialEngines)
		stores = filter.SelectSourceStores(stores, []filter.Filter{f}, c.GetSchedulerConfig(), nil, nil)
	default:
	}

	storeMap := make(map[uint64]string, len(stores))
	for _, store := range stores {
		storeMap[store.GetID()] = store.GetLabelValue(core.EngineKey)
	}
	opt := statistics.WithStoreMapOption(storeMap)
	stats := c.getRegionStats(startKey, endKey, true, opt)
	// Fill in the hot write region statistics.
	for storeID, label := range storeMap {
		if _, ok := stats.StoreLeaderCount[storeID]; !ok {
			stats.StoreLeaderCount[storeID] = 0
			stats.StoreLeaderKeys[storeID] = 0
			stats.StoreLeaderSize[storeID] = 0
		}
		if _, ok := stats.StorePeerCount[storeID]; !ok {
			stats.StorePeerCount[storeID] = 0
			stats.StorePeerKeys[storeID] = 0
			stats.StorePeerSize[storeID] = 0
		}

		if _, ok := stats.StoreWriteQuery[storeID]; !ok {
			stats.StoreWriteBytes[storeID] = 0
			stats.StoreWriteKeys[storeID] = 0
			stats.StoreWriteQuery[storeID] = 0
		}

		if _, ok := stats.StoreLeaderReadQuery[storeID]; !ok {
			stats.StoreLeaderReadKeys[storeID] = 0
			stats.StoreLeaderReadBytes[storeID] = 0
			stats.StoreLeaderReadQuery[storeID] = 0
		}

		if _, ok := stats.StorePeerReadQuery[storeID]; !ok {
			stats.StorePeerReadKeys[storeID] = 0
			stats.StorePeerReadBytes[storeID] = 0
			stats.StorePeerReadQuery[storeID] = 0
		}
		if label == "" {
			storeMap[storeID] = core.EngineTiKV
		}
	}

	stats.StoreEngine = storeMap
	return stats
}

// GetRegionStatsByRange returns region statistics from cluster.
// if useHotFlow is true, the hot region statistics will be returned.
func (c *RaftCluster) GetRegionStatsByRange(startKey, endKey []byte,
	opts ...statistics.GetRegionStatsOption) *statistics.RegionStats {
	return c.getRegionStats(startKey, endKey, false, opts...)
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

	c.PutStore(store, core.SetMinResolvedTS(minResolvedTS))
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
	// Avoid panic when minResolvedTS is not initialized.
	oldMinResolvedTS, _ := c.minResolvedTS.Load().(uint64)
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
func (c *RaftCluster) GetProgressByID(storeID uint64) (*progress.Progress, error) {
	if c == nil || c.progressManager == nil {
		return nil, errs.ErrProgressNotFound.FastGenByArgs("progress manager is not initialized")
	}
	p := c.progressManager.GetProgressByStoreID(storeID)
	if p == nil {
		return nil, errs.ErrProgressNotFound.FastGenByArgs(fmt.Sprintf("the given store ID: %d", storeID))
	}
	return p, nil
}

// GetProgressByAction returns the progress details for a given action.
func (c *RaftCluster) GetProgressByAction(action string) (*progress.Progress, error) {
	if c == nil || c.progressManager == nil {
		return nil, errs.ErrProgressNotFound.FastGenByArgs("progress manager is not initialized")
	}
	p := c.progressManager.GetAverageProgressByAction(progress.Action(action))
	if p == nil {
		return nil, errs.ErrProgressNotFound.FastGenByArgs(fmt.Sprintf("the action: %s", action))
	}
	return p, nil
}

var healthURL = "/pd/api/v1/ping"

// CheckHealth checks if members are healthy.
func CheckHealth(client *http.Client, members []*pdpb.Member) map[uint64]*pdpb.Member {
	healthMembers := make(map[uint64]*pdpb.Member)
	for _, member := range members {
		for _, cURL := range member.ClientUrls {
			ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s%s", cURL, healthURL), http.NoBody)
			req.Header.Set(apiutil.PDAllowFollowerHandleHeader, "true")
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

// Why 99? If the network is normal within the 10-second store heartbeat, the
// score will drop from 100 to: 100 - (100/10(min)/(60s/10s)) = 100 - (10/6) < 99. In
// other words, if the network is completely normal within 10 seconds, the score will
// be less than 99.
const networkSlowStoreEvictThreshold = 99

func (c *RaftCluster) adjustNetworkSlowStore(storeID uint64) {
	if c.GetAvgNetworkSlowScore(storeID) >= networkSlowStoreEvictThreshold {
		c.TriggerNetworkSlowEvict(storeID)
		storeTriggerNetworkSlowEvict.WithLabelValues(strconv.FormatUint(storeID, 10)).Inc()
	}
	// Note: Currently, only one network slow store needs to be considered.
	// If multiple network slow stores need to be considered, the scores
	// of the existing network slow stores need to be eliminated.
	if c.GetAvgNetworkSlowScore(storeID) <= 1 {
		// If the node remains normal, it takes 10 minutes to go from 100 to 1
		c.ResetTriggerNetworkSlowEvict(storeID)
		storeTriggerNetworkSlowEvict.DeleteLabelValues(strconv.FormatUint(storeID, 10))
	}
}

// runStorageSizeCollector runs the storage size collector for the metering.
func (c *RaftCluster) runStorageSizeCollector(
	writer *metering.Writer,
	regionLabeler *labeler.RegionLabeler,
	keyspaceManager *keyspace.Manager,
) {
	defer logutil.LogPanic()
	defer c.wg.Done()

	if writer == nil {
		log.Info("no metering writer provided, the storage size collector will not be started")
		return
	}
	log.Info("running the storage size collector")
	// Init and register the collector before starting the loop.
	collector := newStorageSizeCollector()
	writer.RegisterCollector(collector)
	// Start the ticker to collect the storage size data periodically.
	ticker := time.NewTicker(storageSizeCollectorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("storage size collector has been stopped")
			return
		case <-ticker.C:
			storageSizeInfoList := c.collectStorageSize(regionLabeler, keyspaceManager)
			// Collect the storage size info list of all keyspaces.
			collector.Collect(storageSizeInfoList)
		}
	}
}

func (c *RaftCluster) collectStorageSize(
	regionLabeler *labeler.RegionLabeler,
	keyspaceManager *keyspace.Manager,
) []*storageSizeInfo {
	regionBoundsMap := make(map[string]*keyspace.RegionBound)
	start := time.Now()
	// Iterate the region labeler to get all keyspaces and their corresponding region ranges.
	regionLabeler.IterateLabelRules(func(rule *labeler.LabelRule) bool {
		// Try to parse the keyspace ID from the label rule.
		keyspaceID, ok := keyspace.ParseKeyspaceIDFromLabelRule(rule)
		if !ok {
			return true
		}
		keyspaceName, err := keyspaceManager.GetEnabledKeyspaceNameByID(keyspaceID)
		if err != nil {
			// TODO: improve the observability of this error.
			return true
		}
		// Make the region bounds.
		regionBoundsMap[keyspaceName] = keyspace.MakeRegionBound(keyspaceID)
		return true
	})
	log.Info("iterated the region bounds of all keyspaces",
		zap.Duration("cost", time.Since(start)),
		zap.Int("count", len(regionBoundsMap)))

	start = time.Now()
	storageSizeInfoList := make([]*storageSizeInfo, 0, len(regionBoundsMap))
	// Observe the region stats of each keyspace.
	for keyspaceName, regionBounds := range regionBoundsMap {
		regionStats := c.GetRegionStatsByRange(regionBounds.TxnLeftBound, regionBounds.TxnRightBound)
		storageSizeInfoList = append(storageSizeInfoList, &storageSizeInfo{
			keyspaceName: keyspaceName,
			// Use the user storage size to record the logical storage size.
			rowBasedStorageSize:    uint64(regionStats.UserStorageSize),
			columnBasedStorageSize: uint64(regionStats.UserColumnarStorageSize),
		})
	}
	log.Info("observed the region stats of all keyspaces",
		zap.Duration("cost", time.Since(start)),
		zap.Int("count", len(storageSizeInfoList)))
	return storageSizeInfoList
}

// runDFSStatsCollector runs the DFS (Distributed File System) stats collector for the metering.
func (c *RaftCluster) runDFSStatsCollector(
	writer *metering.Writer,
	keyspaceManager *keyspace.Manager,
) {
	defer logutil.LogPanic()
	defer c.wg.Done()

	if writer == nil {
		log.Info("no metering writer provided, the dfs stats collector will not be started")
		return
	}
	log.Info("running the dfs stats collector")
	// Init and register the collector before starting the loop.
	collector := newDfsStatsCollector()
	writer.RegisterCollector(collector)
	// Start the ticker to collect the DFS stats data periodically.
	ticker := time.NewTicker(dfsStatsCollectorInterval)
	defer ticker.Stop()

	var start time.Time
	for {
		select {
		case <-c.ctx.Done():
			log.Info("dfs stats collector has been stopped")
			return
		case <-ticker.C:
			start = time.Now()
			keyspaceDFSStats, storeCount := c.collectDFSStats(keyspaceManager)
			log.Info("collected the incremental dfs stats from all stores",
				zap.Duration("cost", time.Since(start)),
				zap.Int("keyspace-dfs-stats-count", len(keyspaceDFSStats)),
				zap.Int("collected-store-count", storeCount))
			collector.Collect(keyspaceDFSStats)
		}
	}
}

type keyspaceDFSStatsKey struct {
	keyspaceName string
	component    string
}

type keyspaceDFSStatsMap map[keyspaceDFSStatsKey]*core.DFSStats

func (c *RaftCluster) collectDFSStats(keyspaceManager *keyspace.Manager) (keyspaceDFSStatsMap, int) {
	var (
		keyspaceDFSStats = make(keyspaceDFSStatsMap, 0)
		storeCount       = 0
		keyspaceName     string
		err              error
	)
	for _, store := range c.GetStores() {
		scopedDFSStats := store.TakeScopedDFSStats()
		if len(scopedDFSStats) == 0 {
			continue
		}
		storeCount++
		log.Info("collected the scoped dfs stats from the store",
			zap.Uint64("store-id", store.GetID()),
			zap.Int("collected-store-count", storeCount),
			zap.Int("scoped-dfs-stats-count", len(scopedDFSStats)))
		// Merge into the keyspace DFS stats.
		for scope, stats := range scopedDFSStats {
			// Set the keyspace name to empty string for global scope.
			if scope.GetIsGlobal() {
				keyspaceName = ""
			} else {
				keyspaceName, err = keyspaceManager.GetEnabledKeyspaceNameByID(scope.GetKeyspaceId())
				if err != nil {
					continue
				}
			}
			key := keyspaceDFSStatsKey{
				keyspaceName: keyspaceName,
				component:    scope.GetComponent(),
			}
			dfsStats, ok := keyspaceDFSStats[key]
			if ok {
				dfsStats.WrittenBytes += stats.WrittenBytes
				dfsStats.WriteRequests += stats.WriteRequests
			} else {
				keyspaceDFSStats[key] = &core.DFSStats{
					WrittenBytes:  stats.WrittenBytes,
					WriteRequests: stats.WriteRequests,
				}
			}
		}
	}
	return keyspaceDFSStats, storeCount
}
