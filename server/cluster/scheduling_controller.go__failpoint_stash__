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

package cluster

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/checker"
	sc "github.com/tikv/pd/pkg/schedule/config"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/scatter"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/splitter"
	"github.com/tikv/pd/pkg/schedule/types"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// schedulingController is used to manage all schedulers and checkers.
type schedulingController struct {
	parentCtx context.Context
	ctx       context.Context
	cancel    context.CancelFunc
	mu        syncutil.RWMutex
	wg        sync.WaitGroup
	*core.BasicCluster
	opt         sc.ConfProvider
	coordinator *schedule.Coordinator
	labelStats  *statistics.LabelStatistics
	regionStats *statistics.RegionStatistics
	hotStat     *statistics.HotStat
	slowStat    *statistics.SlowStat
	running     bool
}

// newSchedulingController creates a new scheduling controller.
func newSchedulingController(parentCtx context.Context, basicCluster *core.BasicCluster, opt sc.ConfProvider, ruleManager *placement.RuleManager) *schedulingController {
	ctx, cancel := context.WithCancel(parentCtx)
	return &schedulingController{
		parentCtx:    parentCtx,
		ctx:          ctx,
		cancel:       cancel,
		BasicCluster: basicCluster,
		opt:          opt,
		labelStats:   statistics.NewLabelStatistics(),
		hotStat:      statistics.NewHotStat(parentCtx, basicCluster),
		slowStat:     statistics.NewSlowStat(),
		regionStats:  statistics.NewRegionStatistics(basicCluster, opt, ruleManager),
	}
}

func (sc *schedulingController) stopSchedulingJobs() bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if !sc.running {
		return false
	}
	sc.coordinator.Stop()
	sc.cancel()
	sc.wg.Wait()
	sc.running = false
	log.Info("scheduling service is stopped")
	return true
}

func (sc *schedulingController) startSchedulingJobs(cluster sche.ClusterInformer, hbstreams *hbstream.HeartbeatStreams) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.running {
		return
	}
	sc.initCoordinatorLocked(sc.parentCtx, cluster, hbstreams)
	sc.wg.Add(3)
	go sc.runCoordinator()
	go sc.runStatsBackgroundJobs()
	go sc.runSchedulingMetricsCollectionJob()
	sc.running = true
	log.Info("scheduling service is started")
}

func (sc *schedulingController) initCoordinator(ctx context.Context, cluster sche.ClusterInformer, hbstreams *hbstream.HeartbeatStreams) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.initCoordinatorLocked(ctx, cluster, hbstreams)
	sc.coordinator.InitSchedulers(false)
}

func (sc *schedulingController) initCoordinatorLocked(ctx context.Context, cluster sche.ClusterInformer, hbstreams *hbstream.HeartbeatStreams) {
	sc.ctx, sc.cancel = context.WithCancel(ctx)
	sc.coordinator = schedule.NewCoordinator(sc.ctx, cluster, hbstreams)
}

// runCoordinator runs the main scheduling loop.
func (sc *schedulingController) runCoordinator() {
	defer logutil.LogPanic()
	defer sc.wg.Done()
	sc.coordinator.RunUntilStop()
}

func (sc *schedulingController) runStatsBackgroundJobs() {
	defer logutil.LogPanic()
	defer sc.wg.Done()

	ticker := time.NewTicker(statistics.RegionsStatsObserveInterval)
	defer ticker.Stop()

	for _, store := range sc.GetStores() {
		storeID := store.GetID()
		sc.hotStat.GetOrCreateRollingStoreStats(storeID)
	}
	for {
		select {
		case <-sc.ctx.Done():
			log.Info("statistics background jobs has been stopped")
			return
		case <-ticker.C:
			sc.hotStat.ObserveRegionsStats(sc.GetStoresWriteRate())
		}
	}
}

func (sc *schedulingController) runSchedulingMetricsCollectionJob() {
	defer logutil.LogPanic()
	defer sc.wg.Done()

	ticker := time.NewTicker(metricsCollectionJobInterval)
	failpoint.Inject("highFrequencyClusterJobs", func() {
		ticker.Reset(time.Millisecond)
	})
	defer ticker.Stop()

	for {
		select {
		case <-sc.ctx.Done():
			log.Info("scheduling metrics are reset")
			resetSchedulingMetrics()
			log.Info("scheduling metrics collection job has been stopped")
			return
		case <-ticker.C:
			sc.collectSchedulingMetrics()
		}
	}
}

func resetSchedulingMetrics() {
	statistics.Reset()
	schedulers.ResetSchedulerMetrics()
	schedule.ResetHotSpotMetrics()
	statistics.ResetRegionStatsMetrics()
	statistics.ResetLabelStatsMetrics()
	// reset hot cache metrics
	statistics.ResetHotCacheStatusMetrics()
}

func (sc *schedulingController) collectSchedulingMetrics() {
	statsMap := statistics.NewStoreStatisticsMap(sc.opt)
	stores := sc.GetStores()
	for _, s := range stores {
		statsMap.Observe(s)
		statistics.ObserveHotStat(s, sc.hotStat.StoresStats)
	}
	statsMap.Collect()
	sc.coordinator.GetSchedulersController().CollectSchedulerMetrics()
	sc.coordinator.CollectHotSpotMetrics()
	if sc.regionStats == nil {
		return
	}
	sc.regionStats.Collect()
	sc.labelStats.Collect()
	// collect hot cache metrics
	sc.hotStat.CollectMetrics()
	// collect the lock metrics
	sc.CollectWaitLockMetrics()
}

func (sc *schedulingController) removeStoreStatistics(storeID uint64) {
	sc.hotStat.RemoveRollingStoreStats(storeID)
	sc.slowStat.RemoveSlowStoreStatus(storeID)
}

func (sc *schedulingController) updateStoreStatistics(storeID uint64, isSlow bool) {
	sc.hotStat.GetOrCreateRollingStoreStats(storeID)
	sc.slowStat.ObserveSlowStoreStatus(storeID, isSlow)
}

// GetHotStat gets hot stat.
func (sc *schedulingController) GetHotStat() *statistics.HotStat {
	return sc.hotStat
}

// GetRegionStats gets region statistics.
func (sc *schedulingController) GetRegionStats() *statistics.RegionStatistics {
	return sc.regionStats
}

// GetLabelStats gets label statistics.
func (sc *schedulingController) GetLabelStats() *statistics.LabelStatistics {
	return sc.labelStats
}

// GetRegionStatsByType gets the status of the region by types.
func (sc *schedulingController) GetRegionStatsByType(typ statistics.RegionStatisticType) []*core.RegionInfo {
	if sc.regionStats == nil {
		return nil
	}
	return sc.regionStats.GetRegionStatsByType(typ)
}

// UpdateRegionsLabelLevelStats updates the status of the region label level by types.
func (sc *schedulingController) UpdateRegionsLabelLevelStats(regions []*core.RegionInfo) {
	for _, region := range regions {
		sc.labelStats.Observe(region, sc.getStoresWithoutLabelLocked(region, core.EngineKey, core.EngineTiFlash), sc.opt.GetLocationLabels())
	}
	sc.labelStats.ClearDefunctRegions()
}

func (sc *schedulingController) getStoresWithoutLabelLocked(region *core.RegionInfo, key, value string) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := sc.GetStore(p.StoreId); store != nil && !core.IsStoreContainLabel(store.GetMeta(), key, value) {
			stores = append(stores, store)
		}
	}
	return stores
}

// GetStoresStats returns stores' statistics from cluster.
// And it will be unnecessary to filter unhealthy store, because it has been solved in process heartbeat
func (sc *schedulingController) GetStoresStats() *statistics.StoresStats {
	return sc.hotStat.StoresStats
}

// GetStoresLoads returns load stats of all stores.
func (sc *schedulingController) GetStoresLoads() map[uint64][]float64 {
	return sc.hotStat.GetStoresLoads()
}

// IsRegionHot checks if a region is in hot state.
func (sc *schedulingController) IsRegionHot(region *core.RegionInfo) bool {
	return sc.hotStat.IsRegionHot(region, sc.opt.GetHotRegionCacheHitsThreshold())
}

// GetHotPeerStat returns hot peer stat with specified regionID and storeID.
func (sc *schedulingController) GetHotPeerStat(rw utils.RWType, regionID, storeID uint64) *statistics.HotPeerStat {
	return sc.hotStat.GetHotPeerStat(rw, regionID, storeID)
}

// GetHotPeerStats returns the read or write statistics for hot regions.
// It returns a map where the keys are store IDs and the values are slices of HotPeerStat.
// The result only includes peers that are hot enough.
func (sc *schedulingController) GetHotPeerStats(rw utils.RWType) map[uint64][]*statistics.HotPeerStat {
	// GetHotPeerStats is a thread-safe method
	threshold := sc.opt.GetHotRegionCacheHitsThreshold()
	if rw == utils.Read {
		threshold = sc.opt.GetHotRegionCacheHitsThreshold() *
			(utils.RegionHeartBeatReportInterval / utils.StoreHeartBeatReportInterval)
	}
	return sc.hotStat.GetHotPeerStats(rw, threshold)
}

// BucketsStats returns hot region's buckets stats.
func (sc *schedulingController) BucketsStats(degree int, regionIDs ...uint64) map[uint64][]*buckets.BucketStat {
	return sc.hotStat.BucketsStats(degree, regionIDs...)
}

// GetCoordinator returns the coordinator.
func (sc *schedulingController) GetCoordinator() *schedule.Coordinator {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator
}

// GetPausedSchedulerDelayAt returns DelayAt of a paused scheduler
func (sc *schedulingController) GetPausedSchedulerDelayAt(name string) (int64, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetSchedulersController().GetPausedSchedulerDelayAt(name)
}

// GetPausedSchedulerDelayUntil returns DelayUntil of a paused scheduler
func (sc *schedulingController) GetPausedSchedulerDelayUntil(name string) (int64, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetSchedulersController().GetPausedSchedulerDelayUntil(name)
}

// GetOperatorController returns the operator controller.
func (sc *schedulingController) GetOperatorController() *operator.Controller {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetOperatorController()
}

// GetRegionScatterer returns the region scatter.
func (sc *schedulingController) GetRegionScatterer() *scatter.RegionScatterer {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetRegionScatterer()
}

// GetRegionSplitter returns the region splitter
func (sc *schedulingController) GetRegionSplitter() *splitter.RegionSplitter {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetRegionSplitter()
}

// GetMergeChecker returns merge checker.
func (sc *schedulingController) GetMergeChecker() *checker.MergeChecker {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetMergeChecker()
}

// GetRuleChecker returns rule checker.
func (sc *schedulingController) GetRuleChecker() *checker.RuleChecker {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetRuleChecker()
}

// GetSchedulers gets all schedulers.
func (sc *schedulingController) GetSchedulers() []string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetSchedulersController().GetSchedulerNames()
}

// GetSchedulerHandlers gets all scheduler handlers.
func (sc *schedulingController) GetSchedulerHandlers() map[string]http.Handler {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetSchedulersController().GetSchedulerHandlers()
}

// AddSchedulerHandler adds a scheduler handler.
func (sc *schedulingController) AddSchedulerHandler(scheduler schedulers.Scheduler, args ...string) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetSchedulersController().AddSchedulerHandler(scheduler, args...)
}

// RemoveSchedulerHandler removes a scheduler handler.
func (sc *schedulingController) RemoveSchedulerHandler(name string) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetSchedulersController().RemoveSchedulerHandler(name)
}

// AddScheduler adds a scheduler.
func (sc *schedulingController) AddScheduler(scheduler schedulers.Scheduler, args ...string) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetSchedulersController().AddScheduler(scheduler, args...)
}

// RemoveScheduler removes a scheduler.
func (sc *schedulingController) RemoveScheduler(name string) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetSchedulersController().RemoveScheduler(name)
}

// PauseOrResumeScheduler pauses or resumes a scheduler.
func (sc *schedulingController) PauseOrResumeScheduler(name string, t int64) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetSchedulersController().PauseOrResumeScheduler(name, t)
}

// PauseOrResumeChecker pauses or resumes checker.
func (sc *schedulingController) PauseOrResumeChecker(name string, t int64) error {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.PauseOrResumeChecker(name, t)
}

// AddPendingProcessedRegions adds regions to suspect list.
func (sc *schedulingController) AddPendingProcessedRegions(needCheckLen bool, regionIDs ...uint64) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	sc.coordinator.GetCheckerController().AddPendingProcessedRegions(needCheckLen, regionIDs...)
}

// GetPendingProcessedRegions gets all suspect regions.
func (sc *schedulingController) GetPendingProcessedRegions() []uint64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetCheckerController().GetPendingProcessedRegions()
}

// RemovePendingProcessedRegion removes region from pending processed regions.
func (sc *schedulingController) RemovePendingProcessedRegion(id uint64) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	sc.coordinator.GetCheckerController().RemovePendingProcessedRegion(id)
}

// PopOneSuspectKeyRange gets one suspect keyRange group.
// it would return value and true if pop success, or return empty [][2][]byte and false
// if suspectKeyRanges couldn't pop keyRange group.
func (sc *schedulingController) PopOneSuspectKeyRange() ([2][]byte, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetCheckerController().PopOneSuspectKeyRange()
}

// ClearSuspectKeyRanges clears the suspect keyRanges, only for unit test
func (sc *schedulingController) ClearSuspectKeyRanges() {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	sc.coordinator.GetCheckerController().ClearSuspectKeyRanges()
}

// AddSuspectKeyRange adds the key range with the its ruleID as the key
// The instance of each keyRange is like following format:
// [2][]byte: start key/end key
func (sc *schedulingController) AddSuspectKeyRange(start, end []byte) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	sc.coordinator.GetCheckerController().AddSuspectKeyRange(start, end)
}

func (sc *schedulingController) getEvictLeaderStores() (evictStores []uint64) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	if sc.coordinator == nil {
		return nil
	}
	handler, ok := sc.coordinator.GetSchedulersController().GetSchedulerHandlers()[types.EvictLeaderScheduler.String()]
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

// IsPrepared return true if the prepare checker is ready.
func (sc *schedulingController) IsPrepared() bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.coordinator.GetPrepareChecker().IsPrepared()
}

// SetPrepared set the prepare check to prepared. Only for test purpose.
func (sc *schedulingController) SetPrepared() {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	sc.coordinator.GetPrepareChecker().SetPrepared()
}

// IsSchedulingControllerRunning returns whether the scheduling controller is running. Only for test purpose.
func (sc *schedulingController) IsSchedulingControllerRunning() bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.running
}
