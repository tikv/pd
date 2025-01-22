// Copyright 2023 TiKV Project Authors.
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

package server

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/cluster"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/scheduling/server/config"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/schedule"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/scatter"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/splitter"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
)

// Cluster is used to manage all information for scheduling purpose.
type Cluster struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	*core.BasicCluster
	persistConfig     *config.PersistConfig
	ruleManager       *placement.RuleManager
	labelerManager    *labeler.RegionLabeler
	regionStats       *statistics.RegionStatistics
	labelStats        *statistics.LabelStatistics
	hotStat           *statistics.HotStat
	storage           storage.Storage
	coordinator       *schedule.Coordinator
	checkMembershipCh chan struct{}
	pdLeader          atomic.Value
	running           atomic.Bool

	// heartbeatRunner is used to process the subtree update task asynchronously.
	heartbeatRunner ratelimit.Runner
	// miscRunner is used to process the statistics and persistent tasks asynchronously.
	miscRunner ratelimit.Runner
	// logRunner is used to process the log asynchronously.
	logRunner ratelimit.Runner
}

const (
	regionLabelGCInterval = time.Hour
	requestTimeout        = 3 * time.Second
	collectWaitTime       = time.Minute

	// heartbeat relative const
	heartbeatTaskRunner = "heartbeat-task-runner"
	miscTaskRunner      = "misc-task-runner"
	logTaskRunner       = "log-task-runner"
)

var syncRunner = ratelimit.NewSyncRunner()

// NewCluster creates a new cluster.
func NewCluster(
	parentCtx context.Context,
	persistConfig *config.PersistConfig,
	storage storage.Storage,
	basicCluster *core.BasicCluster,
	hbStreams *hbstream.HeartbeatStreams,
	checkMembershipCh chan struct{},
) (*Cluster, error) {
	ctx, cancel := context.WithCancel(parentCtx)
	labelerManager, err := labeler.NewRegionLabeler(ctx, storage, regionLabelGCInterval)
	if err != nil {
		cancel()
		return nil, err
	}
	ruleManager := placement.NewRuleManager(ctx, storage, basicCluster, persistConfig)
	c := &Cluster{
		ctx:               ctx,
		cancel:            cancel,
		BasicCluster:      basicCluster,
		ruleManager:       ruleManager,
		labelerManager:    labelerManager,
		persistConfig:     persistConfig,
		hotStat:           statistics.NewHotStat(ctx, basicCluster),
		labelStats:        statistics.NewLabelStatistics(),
		regionStats:       statistics.NewRegionStatistics(basicCluster, persistConfig, ruleManager),
		storage:           storage,
		checkMembershipCh: checkMembershipCh,

		heartbeatRunner: ratelimit.NewConcurrentRunner(heartbeatTaskRunner, ratelimit.NewConcurrencyLimiter(uint64(runtime.NumCPU()*2)), time.Minute),
		miscRunner:      ratelimit.NewConcurrentRunner(miscTaskRunner, ratelimit.NewConcurrencyLimiter(uint64(runtime.NumCPU()*2)), time.Minute),
		logRunner:       ratelimit.NewConcurrentRunner(logTaskRunner, ratelimit.NewConcurrencyLimiter(uint64(runtime.NumCPU()*2)), time.Minute),
	}
	c.coordinator = schedule.NewCoordinator(ctx, c, hbStreams)
	err = c.ruleManager.Initialize(persistConfig.GetMaxReplicas(), persistConfig.GetLocationLabels(), persistConfig.GetIsolationLevel(), true)
	if err != nil {
		cancel()
		return nil, err
	}
	return c, nil
}

// GetCoordinator returns the coordinator
func (c *Cluster) GetCoordinator() *schedule.Coordinator {
	return c.coordinator
}

// GetHotStat gets hot stat.
func (c *Cluster) GetHotStat() *statistics.HotStat {
	return c.hotStat
}

// GetStoresStats returns stores' statistics from cluster.
// And it will be unnecessary to filter unhealthy store, because it has been solved in process heartbeat
func (c *Cluster) GetStoresStats() *statistics.StoresStats {
	return c.hotStat.StoresStats
}

// GetRegionStats gets region statistics.
func (c *Cluster) GetRegionStats() *statistics.RegionStatistics {
	return c.regionStats
}

// GetLabelStats gets label statistics.
func (c *Cluster) GetLabelStats() *statistics.LabelStatistics {
	return c.labelStats
}

// GetBasicCluster returns the basic cluster.
func (c *Cluster) GetBasicCluster() *core.BasicCluster {
	return c.BasicCluster
}

// GetSharedConfig returns the shared config.
func (c *Cluster) GetSharedConfig() sc.SharedConfigProvider {
	return c.persistConfig
}

// GetRuleManager returns the rule manager.
func (c *Cluster) GetRuleManager() *placement.RuleManager {
	return c.ruleManager
}

// GetRegionLabeler returns the region labeler.
func (c *Cluster) GetRegionLabeler() *labeler.RegionLabeler {
	return c.labelerManager
}

// GetRegionSplitter returns the region splitter.
func (c *Cluster) GetRegionSplitter() *splitter.RegionSplitter {
	return c.coordinator.GetRegionSplitter()
}

// GetRegionScatterer returns the region scatter.
func (c *Cluster) GetRegionScatterer() *scatter.RegionScatterer {
	return c.coordinator.GetRegionScatterer()
}

// GetStoresLoads returns load stats of all stores.
func (c *Cluster) GetStoresLoads() map[uint64][]float64 {
	return c.hotStat.GetStoresLoads()
}

// IsRegionHot checks if a region is in hot state.
func (c *Cluster) IsRegionHot(region *core.RegionInfo) bool {
	return c.hotStat.IsRegionHot(region, c.persistConfig.GetHotRegionCacheHitsThreshold())
}

// GetHotPeerStat returns hot peer stat with specified regionID and storeID.
func (c *Cluster) GetHotPeerStat(rw utils.RWType, regionID, storeID uint64) *statistics.HotPeerStat {
	return c.hotStat.GetHotPeerStat(rw, regionID, storeID)
}

// GetHotPeerStats returns the read or write statistics for hot regions.
// It returns a map where the keys are store IDs and the values are slices of HotPeerStat.
// The result only includes peers that are hot enough.
// GetHotPeerStats is a thread-safe method.
func (c *Cluster) GetHotPeerStats(rw utils.RWType) map[uint64][]*statistics.HotPeerStat {
	threshold := c.persistConfig.GetHotRegionCacheHitsThreshold()
	if rw == utils.Read {
		// As read stats are reported by store heartbeat, the threshold needs to be adjusted.
		threshold = c.persistConfig.GetHotRegionCacheHitsThreshold() *
			(utils.RegionHeartBeatReportInterval / utils.StoreHeartBeatReportInterval)
	}
	return c.hotStat.GetHotPeerStats(rw, threshold)
}

// BucketsStats returns hot region's buckets stats.
func (c *Cluster) BucketsStats(degree int, regionIDs ...uint64) map[uint64][]*buckets.BucketStat {
	return c.hotStat.BucketsStats(degree, regionIDs...)
}

// GetStorage returns the storage.
func (c *Cluster) GetStorage() storage.Storage {
	return c.storage
}

// GetCheckerConfig returns the checker config.
func (c *Cluster) GetCheckerConfig() sc.CheckerConfigProvider { return c.persistConfig }

// GetSchedulerConfig returns the scheduler config.
func (c *Cluster) GetSchedulerConfig() sc.SchedulerConfigProvider { return c.persistConfig }

// GetStoreConfig returns the store config.
func (c *Cluster) GetStoreConfig() sc.StoreConfigProvider { return c.persistConfig }

// AllocID allocates a new ID.
func (c *Cluster) AllocID() (uint64, error) {
	client, err := c.getPDLeaderClient()
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(c.ctx, requestTimeout)
	defer cancel()
	resp, err := client.AllocID(ctx, &pdpb.AllocIDRequest{Header: &pdpb.RequestHeader{ClusterId: keypath.ClusterID()}})
	if err != nil {
		c.triggerMembershipCheck()
		return 0, err
	}
	return resp.GetId(), nil
}

func (c *Cluster) getPDLeaderClient() (pdpb.PDClient, error) {
	cli := c.pdLeader.Load()
	if cli == nil {
		c.triggerMembershipCheck()
		return nil, errors.New("PD leader is not found")
	}
	return cli.(pdpb.PDClient), nil
}

func (c *Cluster) triggerMembershipCheck() {
	select {
	case c.checkMembershipCh <- struct{}{}:
	default: // avoid blocking
	}
}

// SwitchPDLeader switches the PD leader.
func (c *Cluster) SwitchPDLeader(new pdpb.PDClient) bool {
	old := c.pdLeader.Load()
	return c.pdLeader.CompareAndSwap(old, new)
}

func trySend(notifier chan struct{}) {
	select {
	case notifier <- struct{}{}:
	// If the channel is not empty, it means the check is triggered.
	default:
	}
}

// updateScheduler listens on the schedulers updating notifier and manage the scheduler creation and deletion.
func (c *Cluster) updateScheduler() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	// Make sure the coordinator has initialized all the existing schedulers.
	c.waitSchedulersInitialized()
	// Establish a notifier to listen the schedulers updating.
	notifier := make(chan struct{}, 1)
	// Make sure the check will be triggered once later.
	trySend(notifier)
	c.persistConfig.SetSchedulersUpdatingNotifier(notifier)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Info("cluster is closing, stop listening the schedulers updating notifier")
			return
		case <-notifier:
			// This is triggered by the watcher when the schedulers are updated.
		}

		if !c.running.Load() {
			select {
			case <-c.ctx.Done():
				log.Info("cluster is closing, stop listening the schedulers updating notifier")
				return
			case <-ticker.C:
				// retry
				trySend(notifier)
				continue
			}
		}

		log.Info("schedulers updating notifier is triggered, try to update the scheduler")
		var (
			schedulersController   = c.coordinator.GetSchedulersController()
			latestSchedulersConfig = c.persistConfig.GetScheduleConfig().Schedulers
		)
		// Create the newly added schedulers.
		for _, scheduler := range latestSchedulersConfig {
			schedulerType := types.ConvertOldStrToType[scheduler.Type]
			s, err := schedulers.CreateScheduler(
				schedulerType,
				c.coordinator.GetOperatorController(),
				c.storage,
				schedulers.ConfigSliceDecoder(schedulerType, scheduler.Args),
				schedulersController.RemoveScheduler,
			)
			if err != nil {
				log.Error("failed to create scheduler",
					zap.String("scheduler-type", scheduler.Type),
					zap.Strings("scheduler-args", scheduler.Args),
					errs.ZapError(err))
				continue
			}
			name := s.GetName()
			if existed, _ := schedulersController.IsSchedulerExisted(name); existed {
				log.Info("scheduler has already existed, skip adding it",
					zap.String("scheduler-name", name),
					zap.Strings("scheduler-args", scheduler.Args))
				continue
			}
			if err := schedulersController.AddScheduler(s, scheduler.Args...); err != nil {
				log.Error("failed to add scheduler",
					zap.String("scheduler-name", name),
					zap.Strings("scheduler-args", scheduler.Args),
					errs.ZapError(err))
				continue
			}
			log.Info("add scheduler successfully",
				zap.String("scheduler-name", name),
				zap.Strings("scheduler-args", scheduler.Args))
		}
		// Remove the deleted schedulers.
		for _, name := range schedulersController.GetSchedulerNames() {
			scheduler := schedulersController.GetScheduler(name)
			oldType := types.SchedulerTypeCompatibleMap[scheduler.GetType()]
			if slice.AnyOf(latestSchedulersConfig, func(i int) bool {
				return latestSchedulersConfig[i].Type == oldType
			}) {
				continue
			}
			if err := schedulersController.RemoveScheduler(name); err != nil {
				log.Error("failed to remove scheduler",
					zap.String("scheduler-name", name),
					errs.ZapError(err))
				continue
			}
			log.Info("remove scheduler successfully",
				zap.String("scheduler-name", name))
		}
	}
}

func (c *Cluster) waitSchedulersInitialized() {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	for {
		if c.coordinator.AreSchedulersInitialized() {
			return
		}
		select {
		case <-c.ctx.Done():
			log.Info("cluster is closing, stop waiting the schedulers initialization")
			return
		case <-ticker.C:
		}
	}
}

// UpdateRegionsLabelLevelStats updates the status of the region label level by types.
func (c *Cluster) UpdateRegionsLabelLevelStats(regions []*core.RegionInfo) {
	for _, region := range regions {
		c.labelStats.Observe(region, c.getStoresWithoutLabelLocked(region, core.EngineKey, core.EngineTiFlash), c.persistConfig.GetLocationLabels())
	}
	c.labelStats.ClearDefunctRegions()
}

func (c *Cluster) getStoresWithoutLabelLocked(region *core.RegionInfo, key, value string) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := c.GetStore(p.GetStoreId()); store != nil && !core.IsStoreContainLabel(store.GetMeta(), key, value) {
			stores = append(stores, store)
		}
	}
	return stores
}

// HandleStoreHeartbeat updates the store status.
func (c *Cluster) HandleStoreHeartbeat(heartbeat *schedulingpb.StoreHeartbeatRequest) error {
	stats := heartbeat.GetStats()
	storeID := stats.GetStoreId()
	store := c.GetStore(storeID)
	if store == nil {
		return errors.Errorf("store %v not found", storeID)
	}

	nowTime := time.Now()
	newStore := store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(nowTime))

	c.PutStore(newStore)
	c.hotStat.Observe(storeID, newStore.GetStoreStats())
	c.hotStat.FilterUnhealthyStore(c)
	reportInterval := stats.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()

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

	// Here we will compare the reported regions with the previous hot peers to decide if it is still hot.
	collectUnReportedPeerTask := func(cache *statistics.HotPeerCache) {
		stats := cache.CheckColdPeer(storeID, regions, interval)
		for _, stat := range stats {
			cache.UpdateStat(stat)
		}
	}
	c.hotStat.CheckReadAsync(collectUnReportedPeerTask)
	return nil
}

// runUpdateStoreStats updates store stats periodically.
func (c *Cluster) runUpdateStoreStats() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(9 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Info("update store stats background jobs has been stopped")
			return
		case <-ticker.C:
			c.UpdateAllStoreStatus()
		}
	}
}

// runCoordinator runs the main scheduling loop.
func (c *Cluster) runCoordinator() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	// force wait for 1 minute to make prepare checker won't be directly skipped
	runCollectWaitTime := collectWaitTime
	failpoint.Inject("changeRunCollectWaitTime", func() {
		runCollectWaitTime = 1 * time.Second
	})
	c.coordinator.RunUntilStop(runCollectWaitTime)
}

func (c *Cluster) runMetricsCollectionJob() {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Info("metrics are reset")
			resetMetrics()
			log.Info("metrics collection job has been stopped")
			return
		case <-ticker.C:
			c.collectMetrics()
		}
	}
}

func (c *Cluster) collectMetrics() {
	statsMap := statistics.NewStoreStatisticsMap(c.persistConfig)
	stores := c.GetStores()
	for _, s := range stores {
		statsMap.Observe(s)
		statistics.ObserveHotStat(s, c.hotStat.StoresStats)
	}
	statsMap.Collect()

	c.coordinator.GetSchedulersController().CollectSchedulerMetrics()
	c.coordinator.CollectHotSpotMetrics()
	if c.regionStats == nil {
		return
	}
	c.regionStats.Collect()
	c.labelStats.Collect()
	// collect hot cache metrics
	c.hotStat.CollectMetrics()
	// collect the lock metrics
	c.CollectWaitLockMetrics()
}

func resetMetrics() {
	statistics.Reset()
	schedulers.ResetSchedulerMetrics()
	schedule.ResetHotSpotMetrics()
}

// StartBackgroundJobs starts background jobs.
func (c *Cluster) StartBackgroundJobs() {
	c.wg.Add(4)
	go c.updateScheduler()
	go c.runUpdateStoreStats()
	go c.runCoordinator()
	go c.runMetricsCollectionJob()
	c.heartbeatRunner.Start(c.ctx)
	c.miscRunner.Start(c.ctx)
	c.logRunner.Start(c.ctx)
	c.running.Store(true)
}

// StopBackgroundJobs stops background jobs.
func (c *Cluster) StopBackgroundJobs() {
	if !c.running.Load() {
		return
	}
	c.running.Store(false)
	c.coordinator.Stop()
	c.heartbeatRunner.Stop()
	c.miscRunner.Stop()
	c.logRunner.Stop()
	c.cancel()
	c.wg.Wait()
}

// IsBackgroundJobsRunning returns whether the background jobs are running. Only for test purpose.
func (c *Cluster) IsBackgroundJobsRunning() bool {
	return c.running.Load()
}

// HandleRegionHeartbeat processes RegionInfo reports from client.
func (c *Cluster) HandleRegionHeartbeat(region *core.RegionInfo) error {
	tracer := core.NewNoopHeartbeatProcessTracer()
	if c.persistConfig.GetScheduleConfig().EnableHeartbeatBreakdownMetrics {
		tracer = core.NewHeartbeatProcessTracer()
	}
	var taskRunner, miscRunner, logRunner ratelimit.Runner
	taskRunner, miscRunner, logRunner = syncRunner, syncRunner, syncRunner
	if c.persistConfig.GetScheduleConfig().EnableHeartbeatConcurrentRunner {
		taskRunner = c.heartbeatRunner
		miscRunner = c.miscRunner
		logRunner = c.logRunner
	}
	ctx := &core.MetaProcessContext{
		Context:    c.ctx,
		Tracer:     tracer,
		TaskRunner: taskRunner,
		MiscRunner: miscRunner,
		LogRunner:  logRunner,
	}
	tracer.Begin()
	if err := c.processRegionHeartbeat(ctx, region); err != nil {
		tracer.OnAllStageFinished()
		return err
	}
	tracer.OnAllStageFinished()
	c.coordinator.GetOperatorController().Dispatch(region, operator.DispatchFromHeartBeat, c.coordinator.RecordOpStepWithTTL)
	return nil
}

// processRegionHeartbeat updates the region information.
func (c *Cluster) processRegionHeartbeat(ctx *core.MetaProcessContext, region *core.RegionInfo) error {
	tracer := ctx.Tracer
	origin, _, err := c.PreCheckPutRegion(region)
	tracer.OnPreCheckFinished()
	if err != nil {
		return err
	}
	region.Inherit(origin, c.GetStoreConfig().IsEnableRegionBucket())
	cluster.HandleStatsAsync(c, region)
	tracer.OnAsyncHotStatsFinished()
	hasRegionStats := c.regionStats != nil
	// Save to storage if meta is updated, except for flashback.
	// Save to cache if meta or leader is updated, or contains any down/pending peer.
	_, saveCache, _, retained := core.GenerateRegionGuideFunc(true)(ctx, region, origin)
	regionID := region.GetID()
	if !saveCache {
		// Due to some config changes need to update the region stats as well,
		// so we do some extra checks here.
		if hasRegionStats && c.regionStats.RegionStatsNeedUpdate(region) {
			ctx.TaskRunner.RunTask(
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
	tracer.OnSaveCacheBegin()
	var overlaps []*core.RegionInfo
	if saveCache {
		// To prevent a concurrent heartbeat of another region from overriding the up-to-date region info by a stale one,
		// check its validation again here.
		//
		// However, it can't solve the race condition of concurrent heartbeats from the same region.

		// Async task in next PR.
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
		ctx.TaskRunner.RunTask(
			regionID,
			ratelimit.HandleOverlaps,
			func(ctx context.Context) {
				cluster.HandleOverlaps(ctx, c, overlaps)
			},
		)
	}
	tracer.OnSaveCacheFinished()
	if hasRegionStats {
		// handle region stats
		ctx.TaskRunner.RunTask(
			regionID,
			ratelimit.CollectRegionStatsAsync,
			func(ctx context.Context) {
				cluster.Collect(ctx, c, region)
			},
		)
	}

	tracer.OnCollectRegionStatsFinished()
	return nil
}

// IsPrepared return true if the prepare checker is ready.
func (c *Cluster) IsPrepared() bool {
	return c.coordinator.GetPrepareChecker().IsPrepared()
}

// SetPrepared set the prepare check to prepared. Only for test purpose.
func (c *Cluster) SetPrepared() {
	c.coordinator.GetPrepareChecker().SetPrepared()
}

// IsSchedulingHalted returns whether the scheduling is halted.
// Currently, the microservice scheduling is halted when:
//   - The `HaltScheduling` persist option is set to true.
func (c *Cluster) IsSchedulingHalted() bool {
	return c.persistConfig.IsSchedulingHalted()
}
