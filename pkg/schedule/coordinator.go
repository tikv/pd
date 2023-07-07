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

package schedule

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/checker"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/diagnostic"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/scatter"
	"github.com/tikv/pd/pkg/schedule/schedulers"
	"github.com/tikv/pd/pkg/schedule/splitter"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/config"
	"go.uber.org/zap"
)

const (
	runSchedulerCheckInterval  = 3 * time.Second
	checkSuspectRangesInterval = 100 * time.Millisecond
	collectFactor              = 0.9
	collectTimeout             = 5 * time.Minute
	maxLoadConfigRetries       = 10
	// pushOperatorTickInterval is the interval try to push the operator.
	pushOperatorTickInterval = 500 * time.Millisecond

	patrolScanRegionLimit = 128 // It takes about 14 minutes to iterate 1 million regions.
	// PluginLoad means action for load plugin
	PluginLoad = "PluginLoad"
	// PluginUnload means action for unload plugin
	PluginUnload = "PluginUnload"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	waitingListGauge  = regionListGauge.WithLabelValues("waiting_list")
	priorityListGauge = regionListGauge.WithLabelValues("priority_list")
)

// Coordinator is used to manage all schedulers and checkers to decide if the region needs to be scheduled.
type Coordinator struct {
	syncutil.RWMutex

	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	cluster           sche.ClusterInformer
	prepareChecker    *prepareChecker
	checkers          *checker.Controller
	regionScatterer   *scatter.RegionScatterer
	regionSplitter    *splitter.RegionSplitter
	schedulers        map[string]*schedulers.ScheduleController
	opController      *operator.Controller
	hbStreams         *hbstream.HeartbeatStreams
	pluginInterface   *PluginInterface
	diagnosticManager *diagnostic.Manager
}

// NewCoordinator creates a new Coordinator.
func NewCoordinator(ctx context.Context, cluster sche.ClusterInformer, hbStreams *hbstream.HeartbeatStreams) *Coordinator {
	ctx, cancel := context.WithCancel(ctx)
	opController := operator.NewController(ctx, cluster.GetBasicCluster(), cluster.GetPersistOptions(), hbStreams)
	schedulers := make(map[string]*schedulers.ScheduleController)
	c := &Coordinator{
		ctx:             ctx,
		cancel:          cancel,
		cluster:         cluster,
		prepareChecker:  newPrepareChecker(),
		checkers:        checker.NewController(ctx, cluster, cluster.GetOpts(), cluster.GetRuleManager(), cluster.GetRegionLabeler(), opController),
		regionScatterer: scatter.NewRegionScatterer(ctx, cluster, opController),
		regionSplitter:  splitter.NewRegionSplitter(cluster, splitter.NewSplitRegionsHandler(cluster, opController)),
		schedulers:      schedulers,
		opController:    opController,
		hbStreams:       hbStreams,
		pluginInterface: NewPluginInterface(),
	}
	c.diagnosticManager = diagnostic.NewManager(schedulers, cluster.GetPersistOptions())
	return c
}

// GetWaitingRegions returns the regions in the waiting list.
func (c *Coordinator) GetWaitingRegions() []*cache.Item {
	return c.checkers.GetWaitingRegions()
}

// IsPendingRegion returns if the region is in the pending list.
func (c *Coordinator) IsPendingRegion(region uint64) bool {
	return c.checkers.IsPendingRegion(region)
}

// PatrolRegions is used to scan regions.
// The checkers will check these regions to decide if they need to do some operations.
// The function is exposed for test purpose.
func (c *Coordinator) PatrolRegions() {
	defer logutil.LogPanic()

	defer c.wg.Done()
	ticker := time.NewTicker(c.cluster.GetOpts().GetPatrolRegionInterval())
	defer ticker.Stop()

	log.Info("Coordinator starts patrol regions")
	start := time.Now()
	var (
		key     []byte
		regions []*core.RegionInfo
	)
	for {
		select {
		case <-ticker.C:
			// Note: we reset the ticker here to support updating configuration dynamically.
			ticker.Reset(c.cluster.GetOpts().GetPatrolRegionInterval())
		case <-c.ctx.Done():
			log.Info("patrol regions has been stopped")
			return
		}
		if c.isSchedulingHalted() {
			continue
		}

		// Check priority regions first.
		c.checkPriorityRegions()
		// Check suspect regions first.
		c.checkSuspectRegions()
		// Check regions in the waiting list
		c.checkWaitingRegions()

		key, regions = c.checkRegions(key)
		if len(regions) == 0 {
			continue
		}
		// Updates the label level isolation statistics.
		c.cluster.UpdateRegionsLabelLevelStats(regions)
		if len(key) == 0 {
			patrolCheckRegionsGauge.Set(time.Since(start).Seconds())
			start = time.Now()
		}
		failpoint.Inject("break-patrol", func() {
			failpoint.Break()
		})
	}
}

func (c *Coordinator) isSchedulingHalted() bool {
	return c.cluster.GetPersistOptions().IsSchedulingHalted()
}

func (c *Coordinator) checkRegions(startKey []byte) (key []byte, regions []*core.RegionInfo) {
	regions = c.cluster.ScanRegions(startKey, nil, patrolScanRegionLimit)
	if len(regions) == 0 {
		// Resets the scan key.
		key = nil
		return
	}

	for _, region := range regions {
		c.tryAddOperators(region)
		key = region.GetEndKey()
	}
	return
}

func (c *Coordinator) checkSuspectRegions() {
	for _, id := range c.checkers.GetSuspectRegions() {
		region := c.cluster.GetRegion(id)
		c.tryAddOperators(region)
	}
}

func (c *Coordinator) checkWaitingRegions() {
	items := c.checkers.GetWaitingRegions()
	waitingListGauge.Set(float64(len(items)))
	for _, item := range items {
		region := c.cluster.GetRegion(item.Key)
		c.tryAddOperators(region)
	}
}

// checkPriorityRegions checks priority regions
func (c *Coordinator) checkPriorityRegions() {
	items := c.checkers.GetPriorityRegions()
	removes := make([]uint64, 0)
	priorityListGauge.Set(float64(len(items)))
	for _, id := range items {
		region := c.cluster.GetRegion(id)
		if region == nil {
			removes = append(removes, id)
			continue
		}
		ops := c.checkers.CheckRegion(region)
		// it should skip if region needs to merge
		if len(ops) == 0 || ops[0].Kind()&operator.OpMerge != 0 {
			continue
		}
		if !c.opController.ExceedStoreLimit(ops...) {
			c.opController.AddWaitingOperator(ops...)
		}
	}
	for _, v := range removes {
		c.checkers.RemovePriorityRegions(v)
	}
}

// checkSuspectRanges would pop one suspect key range group
// The regions of new version key range and old version key range would be placed into
// the suspect regions map
func (c *Coordinator) checkSuspectRanges() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	log.Info("Coordinator begins to check suspect key ranges")
	ticker := time.NewTicker(checkSuspectRangesInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("check suspect key ranges has been stopped")
			return
		case <-ticker.C:
			keyRange, success := c.checkers.PopOneSuspectKeyRange()
			if !success {
				continue
			}
			limit := 1024
			regions := c.cluster.ScanRegions(keyRange[0], keyRange[1], limit)
			if len(regions) == 0 {
				continue
			}
			regionIDList := make([]uint64, 0, len(regions))
			for _, region := range regions {
				regionIDList = append(regionIDList, region.GetID())
			}

			// if the last region's end key is smaller the keyRange[1] which means there existed the remaining regions between
			// keyRange[0] and keyRange[1] after scan regions, so we put the end key and keyRange[1] into Suspect KeyRanges
			lastRegion := regions[len(regions)-1]
			if lastRegion.GetEndKey() != nil && bytes.Compare(lastRegion.GetEndKey(), keyRange[1]) < 0 {
				c.checkers.AddSuspectKeyRange(lastRegion.GetEndKey(), keyRange[1])
			}
			c.checkers.AddSuspectRegions(regionIDList...)
		}
	}
}

func (c *Coordinator) tryAddOperators(region *core.RegionInfo) {
	if region == nil {
		// the region could be recent split, continue to wait.
		return
	}
	id := region.GetID()
	if c.opController.GetOperator(id) != nil {
		c.checkers.RemoveWaitingRegion(id)
		c.checkers.RemoveSuspectRegion(id)
		return
	}
	ops := c.checkers.CheckRegion(region)
	if len(ops) == 0 {
		return
	}

	if !c.opController.ExceedStoreLimit(ops...) {
		c.opController.AddWaitingOperator(ops...)
		c.checkers.RemoveWaitingRegion(id)
		c.checkers.RemoveSuspectRegion(id)
	} else {
		c.checkers.AddWaitingRegion(region)
	}
}

// drivePushOperator is used to push the unfinished operator to the executor.
func (c *Coordinator) drivePushOperator() {
	defer logutil.LogPanic()

	defer c.wg.Done()
	log.Info("Coordinator begins to actively drive push operator")
	ticker := time.NewTicker(pushOperatorTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("drive push operator has been stopped")
			return
		case <-ticker.C:
			c.opController.PushOperators(c.RecordOpStepWithTTL)
		}
	}
}

// RunUntilStop runs the coordinator until receiving the stop signal.
func (c *Coordinator) RunUntilStop() {
	c.Run()
	<-c.ctx.Done()
	log.Info("Coordinator is stopping")
	c.wg.Wait()
	log.Info("Coordinator has been stopped")
}

// Run starts coordinator.
func (c *Coordinator) Run() {
	ticker := time.NewTicker(runSchedulerCheckInterval)
	failpoint.Inject("changeCoordinatorTicker", func() {
		ticker = time.NewTicker(100 * time.Millisecond)
	})
	defer ticker.Stop()
	log.Info("Coordinator starts to collect cluster information")
	for {
		if c.ShouldRun() {
			log.Info("Coordinator has finished cluster information preparation")
			break
		}
		select {
		case <-ticker.C:
		case <-c.ctx.Done():
			log.Info("Coordinator stops running")
			return
		}
	}
	log.Info("Coordinator starts to run schedulers")
	var (
		scheduleNames []string
		configs       []string
		err           error
	)
	for i := 0; i < maxLoadConfigRetries; i++ {
		scheduleNames, configs, err = c.cluster.GetStorage().LoadAllScheduleConfig()
		select {
		case <-c.ctx.Done():
			log.Info("Coordinator stops running")
			return
		default:
		}
		if err == nil {
			break
		}
		log.Error("cannot load schedulers' config", zap.Int("retry-times", i), errs.ZapError(err))
	}
	if err != nil {
		log.Fatal("cannot load schedulers' config", errs.ZapError(err))
	}

	scheduleCfg := c.cluster.GetPersistOptions().GetScheduleConfig().Clone()
	// The new way to create scheduler with the independent configuration.
	for i, name := range scheduleNames {
		data := configs[i]
		typ := schedulers.FindSchedulerTypeByName(name)
		var cfg config.SchedulerConfig
		for _, c := range scheduleCfg.Schedulers {
			if c.Type == typ {
				cfg = c
				break
			}
		}
		if len(cfg.Type) == 0 {
			log.Error("the scheduler type not found", zap.String("scheduler-name", name), errs.ZapError(errs.ErrSchedulerNotFound))
			continue
		}
		if cfg.Disable {
			log.Info("skip create scheduler with independent configuration", zap.String("scheduler-name", name), zap.String("scheduler-type", cfg.Type), zap.Strings("scheduler-args", cfg.Args))
			continue
		}
		s, err := schedulers.CreateScheduler(cfg.Type, c.opController, c.cluster.GetStorage(), schedulers.ConfigJSONDecoder([]byte(data)), c.RemoveScheduler)
		if err != nil {
			log.Error("can not create scheduler with independent configuration", zap.String("scheduler-name", name), zap.Strings("scheduler-args", cfg.Args), errs.ZapError(err))
			continue
		}
		log.Info("create scheduler with independent configuration", zap.String("scheduler-name", s.GetName()))
		if err = c.AddScheduler(s); err != nil {
			log.Error("can not add scheduler with independent configuration", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", cfg.Args), errs.ZapError(err))
		}
	}

	// The old way to create the scheduler.
	k := 0
	for _, schedulerCfg := range scheduleCfg.Schedulers {
		if schedulerCfg.Disable {
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
			log.Info("skip create scheduler", zap.String("scheduler-type", schedulerCfg.Type), zap.Strings("scheduler-args", schedulerCfg.Args))
			continue
		}

		s, err := schedulers.CreateScheduler(schedulerCfg.Type, c.opController, c.cluster.GetStorage(), schedulers.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args), c.RemoveScheduler)
		if err != nil {
			log.Error("can not create scheduler", zap.String("scheduler-type", schedulerCfg.Type), zap.Strings("scheduler-args", schedulerCfg.Args), errs.ZapError(err))
			continue
		}

		log.Info("create scheduler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", schedulerCfg.Args))
		if err = c.AddScheduler(s, schedulerCfg.Args...); err != nil && !errors.ErrorEqual(err, errs.ErrSchedulerExisted.FastGenByArgs()) {
			log.Error("can not add scheduler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", schedulerCfg.Args), errs.ZapError(err))
		} else {
			// Only records the valid scheduler config.
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
		}
	}

	// Removes the invalid scheduler config and persist.
	scheduleCfg.Schedulers = scheduleCfg.Schedulers[:k]
	c.cluster.GetPersistOptions().SetScheduleConfig(scheduleCfg)
	if err := c.cluster.GetPersistOptions().Persist(c.cluster.GetStorage()); err != nil {
		log.Error("cannot persist schedule config", errs.ZapError(err))
	}

	c.wg.Add(3)
	// Starts to patrol regions.
	go c.PatrolRegions()
	// Checks suspect key ranges
	go c.checkSuspectRanges()
	go c.drivePushOperator()
}

// LoadPlugin load user plugin
func (c *Coordinator) LoadPlugin(pluginPath string, ch chan string) {
	log.Info("load plugin", zap.String("plugin-path", pluginPath))
	// get func: SchedulerType from plugin
	SchedulerType, err := c.pluginInterface.GetFunction(pluginPath, "SchedulerType")
	if err != nil {
		log.Error("GetFunction SchedulerType error", errs.ZapError(err))
		return
	}
	schedulerType := SchedulerType.(func() string)
	// get func: SchedulerArgs from plugin
	SchedulerArgs, err := c.pluginInterface.GetFunction(pluginPath, "SchedulerArgs")
	if err != nil {
		log.Error("GetFunction SchedulerArgs error", errs.ZapError(err))
		return
	}
	schedulerArgs := SchedulerArgs.(func() []string)
	// create and add user scheduler
	s, err := schedulers.CreateScheduler(schedulerType(), c.opController, c.cluster.GetStorage(), schedulers.ConfigSliceDecoder(schedulerType(), schedulerArgs()), c.RemoveScheduler)
	if err != nil {
		log.Error("can not create scheduler", zap.String("scheduler-type", schedulerType()), errs.ZapError(err))
		return
	}
	log.Info("create scheduler", zap.String("scheduler-name", s.GetName()))
	if err = c.AddScheduler(s); err != nil {
		log.Error("can't add scheduler", zap.String("scheduler-name", s.GetName()), errs.ZapError(err))
		return
	}

	c.wg.Add(1)
	go c.waitPluginUnload(pluginPath, s.GetName(), ch)
}

func (c *Coordinator) waitPluginUnload(pluginPath, schedulerName string, ch chan string) {
	defer logutil.LogPanic()
	defer c.wg.Done()
	// Get signal from channel which means user unload the plugin
	for {
		select {
		case action := <-ch:
			if action == PluginUnload {
				err := c.RemoveScheduler(schedulerName)
				if err != nil {
					log.Error("can not remove scheduler", zap.String("scheduler-name", schedulerName), errs.ZapError(err))
				} else {
					log.Info("unload plugin", zap.String("plugin", pluginPath))
					return
				}
			} else {
				log.Error("unknown action", zap.String("action", action))
			}
		case <-c.ctx.Done():
			log.Info("unload plugin has been stopped")
			return
		}
	}
}

// Stop stops the coordinator.
func (c *Coordinator) Stop() {
	c.cancel()
}

// GetHotRegionsByType gets hot regions' statistics by RWType.
func (c *Coordinator) GetHotRegionsByType(typ statistics.RWType) *statistics.StoreHotPeersInfos {
	isTraceFlow := c.cluster.GetOpts().IsTraceRegionFlow()
	storeLoads := c.cluster.GetStoresLoads()
	stores := c.cluster.GetStores()
	var infos *statistics.StoreHotPeersInfos
	switch typ {
	case statistics.Write:
		regionStats := c.cluster.RegionWriteStats()
		infos = statistics.GetHotStatus(stores, storeLoads, regionStats, statistics.Write, isTraceFlow)
	case statistics.Read:
		regionStats := c.cluster.RegionReadStats()
		infos = statistics.GetHotStatus(stores, storeLoads, regionStats, statistics.Read, isTraceFlow)
	default:
	}
	// update params `IsLearner` and `LastUpdateTime`
	s := []statistics.StoreHotPeersStat{infos.AsLeader, infos.AsPeer}
	for i, stores := range s {
		for j, store := range stores {
			for k := range store.Stats {
				h := &s[i][j].Stats[k]
				region := c.cluster.GetRegion(h.RegionID)
				if region != nil {
					h.IsLearner = core.IsLearner(region.GetPeer(h.StoreID))
				}
				switch typ {
				case statistics.Write:
					if region != nil {
						h.LastUpdateTime = time.Unix(int64(region.GetInterval().GetEndTimestamp()), 0)
					}
				case statistics.Read:
					store := c.cluster.GetStore(h.StoreID)
					if store != nil {
						ts := store.GetMeta().GetLastHeartbeat()
						h.LastUpdateTime = time.Unix(ts/1e9, ts%1e9)
					}
				default:
				}
			}
		}
	}
	return infos
}

// GetWaitGroup returns the wait group. Only for test purpose.
func (c *Coordinator) GetWaitGroup() *sync.WaitGroup {
	return &c.wg
}

// GetSchedulers returns all names of schedulers.
func (c *Coordinator) GetSchedulers() []string {
	c.RLock()
	defer c.RUnlock()
	names := make([]string, 0, len(c.schedulers))
	for name := range c.schedulers {
		names = append(names, name)
	}
	return names
}

// GetSchedulerHandlers returns all handlers of schedulers.
func (c *Coordinator) GetSchedulerHandlers() map[string]http.Handler {
	c.RLock()
	defer c.RUnlock()
	handlers := make(map[string]http.Handler, len(c.schedulers))
	for name, scheduler := range c.schedulers {
		handlers[name] = scheduler.Scheduler
	}
	return handlers
}

// CollectSchedulerMetrics collects metrics of all schedulers.
func (c *Coordinator) CollectSchedulerMetrics() {
	c.RLock()
	defer c.RUnlock()
	for _, s := range c.schedulers {
		var allowScheduler float64
		// If the scheduler is not allowed to schedule, it will disappear in Grafana panel.
		// See issue #1341.
		if !s.IsPaused() && !c.isSchedulingHalted() {
			allowScheduler = 1
		}
		schedulerStatusGauge.WithLabelValues(s.Scheduler.GetName(), "allow").Set(allowScheduler)
	}
}

// ResetSchedulerMetrics resets metrics of all schedulers.
func (c *Coordinator) ResetSchedulerMetrics() {
	schedulerStatusGauge.Reset()
}

// CollectHotSpotMetrics collects hot spot metrics.
func (c *Coordinator) CollectHotSpotMetrics() {
	stores := c.cluster.GetStores()
	// Collects hot write region metrics.
	collectHotMetrics(c.cluster, stores, statistics.Write)
	// Collects hot read region metrics.
	collectHotMetrics(c.cluster, stores, statistics.Read)
}

func collectHotMetrics(cluster sche.ClusterInformer, stores []*core.StoreInfo, typ statistics.RWType) {
	var (
		kind        string
		regionStats map[uint64][]*statistics.HotPeerStat
	)

	switch typ {
	case statistics.Read:
		regionStats = cluster.RegionReadStats()
		kind = statistics.Read.String()
	case statistics.Write:
		regionStats = cluster.RegionWriteStats()
		kind = statistics.Write.String()
	}
	status := statistics.CollectHotPeerInfos(stores, regionStats) // only returns TotalBytesRate,TotalKeysRate,TotalQueryRate,Count

	for _, s := range stores {
		// TODO: pre-allocate gauge metrics
		storeAddress := s.GetAddress()
		storeID := s.GetID()
		storeLabel := strconv.FormatUint(storeID, 10)
		stat, hasHotLeader := status.AsLeader[storeID]
		if hasHotLeader {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_bytes_as_leader").Set(stat.TotalBytesRate)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_keys_as_leader").Set(stat.TotalKeysRate)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_query_as_leader").Set(stat.TotalQueryRate)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_"+kind+"_region_as_leader").Set(float64(stat.Count))
		} else {
			hotSpotStatusGauge.DeleteLabelValues(storeAddress, storeLabel, "total_"+kind+"_bytes_as_leader")
			hotSpotStatusGauge.DeleteLabelValues(storeAddress, storeLabel, "total_"+kind+"_keys_as_leader")
			hotSpotStatusGauge.DeleteLabelValues(storeAddress, storeLabel, "total_"+kind+"_query_as_leader")
			hotSpotStatusGauge.DeleteLabelValues(storeAddress, storeLabel, "hot_"+kind+"_region_as_leader")
		}

		stat, hasHotPeer := status.AsPeer[storeID]
		if hasHotPeer {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_bytes_as_peer").Set(stat.TotalBytesRate)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_keys_as_peer").Set(stat.TotalKeysRate)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_query_as_peer").Set(stat.TotalQueryRate)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_"+kind+"_region_as_peer").Set(float64(stat.Count))
		} else {
			hotSpotStatusGauge.DeleteLabelValues(storeAddress, storeLabel, "total_"+kind+"_bytes_as_peer")
			hotSpotStatusGauge.DeleteLabelValues(storeAddress, storeLabel, "total_"+kind+"_keys_as_peer")
			hotSpotStatusGauge.DeleteLabelValues(storeAddress, storeLabel, "total_"+kind+"_query_as_peer")
			hotSpotStatusGauge.DeleteLabelValues(storeAddress, storeLabel, "hot_"+kind+"_region_as_peer")
		}

		if !hasHotLeader && !hasHotPeer {
			statistics.ForeachRegionStats(func(rwTy statistics.RWType, dim int, _ statistics.RegionStatKind) {
				schedulers.HotPendingSum.DeleteLabelValues(storeLabel, rwTy.String(), statistics.DimToString(dim))
			})
		}
	}
}

// ResetHotSpotMetrics resets hot spot metrics.
func (c *Coordinator) ResetHotSpotMetrics() {
	hotSpotStatusGauge.Reset()
	schedulers.HotPendingSum.Reset()
}

// ShouldRun returns true if the coordinator should run.
func (c *Coordinator) ShouldRun() bool {
	return c.prepareChecker.check(c.cluster.GetBasicCluster())
}

// AddScheduler adds a scheduler.
func (c *Coordinator) AddScheduler(scheduler schedulers.Scheduler, args ...string) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.GetName()]; ok {
		return errs.ErrSchedulerExisted.FastGenByArgs()
	}

	s := schedulers.NewScheduleController(c.ctx, c.cluster, c.opController, scheduler)
	if err := s.Scheduler.Prepare(c.cluster); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.runScheduler(s)
	c.schedulers[s.Scheduler.GetName()] = s
	c.cluster.GetPersistOptions().AddSchedulerCfg(s.Scheduler.GetType(), args)
	return nil
}

// RemoveScheduler removes a scheduler by name.
func (c *Coordinator) RemoveScheduler(name string) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return errs.ErrSchedulerNotFound.FastGenByArgs()
	}

	opt := c.cluster.GetPersistOptions()
	if err := c.removeOptScheduler(opt, name); err != nil {
		log.Error("can not remove scheduler", zap.String("scheduler-name", name), errs.ZapError(err))
		return err
	}

	if err := opt.Persist(c.cluster.GetStorage()); err != nil {
		log.Error("the option can not persist scheduler config", errs.ZapError(err))
		return err
	}

	if err := c.cluster.GetStorage().RemoveScheduleConfig(name); err != nil {
		log.Error("can not remove the scheduler config", errs.ZapError(err))
		return err
	}

	s.Stop()
	schedulerStatusGauge.DeleteLabelValues(name, "allow")
	delete(c.schedulers, name)

	return nil
}

func (c *Coordinator) removeOptScheduler(o *config.PersistOptions, name string) error {
	v := o.GetScheduleConfig().Clone()
	for i, schedulerCfg := range v.Schedulers {
		// To create a temporary scheduler is just used to get scheduler's name
		decoder := schedulers.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args)
		tmp, err := schedulers.CreateScheduler(schedulerCfg.Type, c.opController, storage.NewStorageWithMemoryBackend(), decoder, c.RemoveScheduler)
		if err != nil {
			return err
		}
		if tmp.GetName() == name {
			if config.IsDefaultScheduler(tmp.GetType()) {
				schedulerCfg.Disable = true
				v.Schedulers[i] = schedulerCfg
			} else {
				v.Schedulers = append(v.Schedulers[:i], v.Schedulers[i+1:]...)
			}
			o.SetScheduleConfig(v)
			return nil
		}
	}
	return nil
}

// PauseOrResumeScheduler pauses or resumes a scheduler by name.
func (c *Coordinator) PauseOrResumeScheduler(name string, t int64) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return errs.ErrNotBootstrapped.FastGenByArgs()
	}
	var s []*schedulers.ScheduleController
	if name != "all" {
		sc, ok := c.schedulers[name]
		if !ok {
			return errs.ErrSchedulerNotFound.FastGenByArgs()
		}
		s = append(s, sc)
	} else {
		for _, sc := range c.schedulers {
			s = append(s, sc)
		}
	}
	var err error
	for _, sc := range s {
		var delayAt, delayUntil int64
		if t > 0 {
			delayAt = time.Now().Unix()
			delayUntil = delayAt + t
		}
		sc.SetDelay(delayAt, delayUntil)
	}
	return err
}

// IsSchedulerAllowed returns whether a scheduler is allowed to schedule, a scheduler is not allowed to schedule if it is paused or blocked by unsafe recovery.
func (c *Coordinator) IsSchedulerAllowed(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return false, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	return s.AllowSchedule(false), nil
}

// IsSchedulerPaused returns whether a scheduler is paused.
func (c *Coordinator) IsSchedulerPaused(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return false, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	return s.IsPaused(), nil
}

// IsSchedulerDisabled returns whether a scheduler is disabled.
func (c *Coordinator) IsSchedulerDisabled(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return false, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	t := s.Scheduler.GetType()
	scheduleConfig := c.cluster.GetPersistOptions().GetScheduleConfig()
	for _, s := range scheduleConfig.Schedulers {
		if t == s.Type {
			return s.Disable, nil
		}
	}
	return false, nil
}

// IsSchedulerExisted returns whether a scheduler is existed.
func (c *Coordinator) IsSchedulerExisted(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	_, ok := c.schedulers[name]
	if !ok {
		return false, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	return true, nil
}

func (c *Coordinator) runScheduler(s *schedulers.ScheduleController) {
	defer logutil.LogPanic()
	defer c.wg.Done()
	defer s.Scheduler.Cleanup(c.cluster)

	ticker := time.NewTicker(s.GetInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			diagnosable := s.IsDiagnosticAllowed()
			if !s.AllowSchedule(diagnosable) {
				continue
			}
			if op := s.Schedule(diagnosable); len(op) > 0 {
				added := c.opController.AddWaitingOperator(op...)
				log.Debug("add operator", zap.Int("added", added), zap.Int("total", len(op)), zap.String("scheduler", s.Scheduler.GetName()))
			}
			// Note: we reset the ticker here to support updating configuration dynamically.
			ticker.Reset(s.GetInterval())
		case <-s.Ctx().Done():
			log.Info("scheduler has been stopped",
				zap.String("scheduler-name", s.Scheduler.GetName()),
				errs.ZapError(s.Ctx().Err()))
			return
		}
	}
}

// PauseOrResumeChecker pauses or resumes a checker by name.
func (c *Coordinator) PauseOrResumeChecker(name string, t int64) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return errs.ErrNotBootstrapped.FastGenByArgs()
	}
	p, err := c.checkers.GetPauseController(name)
	if err != nil {
		return err
	}
	p.PauseOrResume(t)
	return nil
}

// IsCheckerPaused returns whether a checker is paused.
func (c *Coordinator) IsCheckerPaused(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	p, err := c.checkers.GetPauseController(name)
	if err != nil {
		return false, err
	}
	return p.IsPaused(), nil
}

// GetRegionScatterer returns the region scatterer.
func (c *Coordinator) GetRegionScatterer() *scatter.RegionScatterer {
	return c.regionScatterer
}

// GetRegionSplitter returns the region splitter.
func (c *Coordinator) GetRegionSplitter() *splitter.RegionSplitter {
	return c.regionSplitter
}

// GetOperatorController returns the operator controller.
func (c *Coordinator) GetOperatorController() *operator.Controller {
	return c.opController
}

// GetCheckerController returns the checker controller.
func (c *Coordinator) GetCheckerController() *checker.Controller {
	return c.checkers
}

// GetMergeChecker returns the merge checker.
func (c *Coordinator) GetMergeChecker() *checker.MergeChecker {
	return c.checkers.GetMergeChecker()
}

// GetRuleChecker returns the rule checker.
func (c *Coordinator) GetRuleChecker() *checker.RuleChecker {
	return c.checkers.GetRuleChecker()
}

// GetPrepareChecker returns the prepare checker.
func (c *Coordinator) GetPrepareChecker() *prepareChecker {
	return c.prepareChecker
}

// GetHeartbeatStreams returns the heartbeat streams. Only for test purpose.
func (c *Coordinator) GetHeartbeatStreams() *hbstream.HeartbeatStreams {
	return c.hbStreams
}

// GetCluster returns the cluster. Only for test purpose.
func (c *Coordinator) GetCluster() sche.ClusterInformer {
	return c.cluster
}

// GetDiagnosticResult returns the diagnostic result.
func (c *Coordinator) GetDiagnosticResult(name string) (*schedulers.DiagnosticResult, error) {
	c.RLock()
	defer c.RUnlock()
	return c.diagnosticManager.GetDiagnosticResult(name)
}

// RecordOpStepWithTTL records OpStep with TTL
func (c *Coordinator) RecordOpStepWithTTL(regionID uint64) {
	c.GetRuleChecker().RecordRegionPromoteToNonWitness(regionID)
}

// GetPausedSchedulerDelayAt returns paused timestamp of a paused scheduler
func (c *Coordinator) GetPausedSchedulerDelayAt(name string) (int64, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return -1, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return -1, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	return s.GetDelayAt(), nil
}

// GetPausedSchedulerDelayUntil returns the delay time until the scheduler is paused.
func (c *Coordinator) GetPausedSchedulerDelayUntil(name string) (int64, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return -1, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return -1, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	return s.GetDelayUntil(), nil
}

// CheckTransferWitnessLeader determines if transfer leader is required, then sends to the scheduler if needed
func (c *Coordinator) CheckTransferWitnessLeader(region *core.RegionInfo) {
	if core.NeedTransferWitnessLeader(region) {
		c.RLock()
		s, ok := c.schedulers[schedulers.TransferWitnessLeaderName]
		c.RUnlock()
		if ok {
			select {
			case schedulers.RecvRegionInfo(s.Scheduler) <- region:
			default:
				log.Warn("drop transfer witness leader due to recv region channel full", zap.Uint64("region-id", region.GetID()))
			}
		}
	}
}
