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
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/hbstream"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedulers"
	"github.com/tikv/pd/server/statistics"
	"go.uber.org/zap"
)

const (
	runSchedulerCheckInterval  = 3 * time.Second
	checkSuspectRangesInterval = 100 * time.Millisecond
	collectFactor              = 0.8
	collectTimeout             = 5 * time.Minute
	maxScheduleRetries         = 10
	maxLoadConfigRetries       = 10

	patrolScanRegionLimit = 128 // It takes about 14 minutes to iterate 1 million regions.
	// PluginLoad means action for load plugin
	PluginLoad = "PluginLoad"
	// PluginUnload means action for unload plugin
	PluginUnload = "PluginUnload"
)

// coordinator is used to manage all schedulers and checkers to decide if the region needs to be scheduled.
type coordinator struct {
	sync.RWMutex

	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	cluster         *RaftCluster
	checkers        *schedule.CheckerController
	regionScatterer *schedule.RegionScatterer
	regionSplitter  *schedule.RegionSplitter
	schedulers      map[string]*scheduleController
	opController    *schedule.OperatorController
	hbStreams       *hbstream.HeartbeatStreams
	pluginInterface *schedule.PluginInterface
}

// newCoordinator creates a new coordinator.
func newCoordinator(ctx context.Context, cluster *RaftCluster, hbStreams *hbstream.HeartbeatStreams) *coordinator {
	ctx, cancel := context.WithCancel(ctx)
	opController := schedule.NewOperatorController(ctx, cluster, hbStreams)
	return &coordinator{
		ctx:             ctx,
		cancel:          cancel,
		cluster:         cluster,
		checkers:        schedule.NewCheckerController(ctx, cluster, cluster.ruleManager, cluster.regionLabeler, opController),
		regionScatterer: schedule.NewRegionScatterer(ctx, cluster),
		regionSplitter:  schedule.NewRegionSplitter(cluster, schedule.NewSplitRegionsHandler(cluster, opController)),
		schedulers:      make(map[string]*scheduleController),
		opController:    opController,
		hbStreams:       hbStreams,
		pluginInterface: schedule.NewPluginInterface(),
	}
}

// patrolRegions is used to scan regions.
// The checkers will check these regions to decide if they need to do some operations.
func (c *coordinator) patrolRegions() {
	defer logutil.LogPanic()

	defer c.wg.Done()
	timer := time.NewTimer(c.cluster.GetOpts().GetPatrolRegionInterval())
	defer timer.Stop()

	log.Info("coordinator starts patrol regions")
	start := time.Now()
	var key []byte
	for {
		select {
		case <-timer.C:
			timer.Reset(c.cluster.GetOpts().GetPatrolRegionInterval())
		case <-c.ctx.Done():
			log.Info("patrol regions has been stopped")
			return
		}

		// Check priority regions first.
		c.checkPriorityRegions()
		// Check suspect regions first.
		c.checkSuspectRegions()
		// Check regions in the waiting list
		c.checkWaitingRegions()

		regions := c.cluster.ScanRegions(key, nil, patrolScanRegionLimit)
		if len(regions) == 0 {
			// Resets the scan key.
			key = nil
			continue
		}

		for _, region := range regions {
			// Skips the region if there is already a pending operator.
			if c.opController.GetOperator(region.GetID()) != nil {
				continue
			}

			ops := c.checkers.CheckRegion(region)

			key = region.GetEndKey()
			if len(ops) == 0 {
				continue
			}

			if !c.opController.ExceedStoreLimit(ops...) {
				c.opController.AddWaitingOperator(ops...)
				c.checkers.RemoveWaitingRegion(region.GetID())
				c.cluster.RemoveSuspectRegion(region.GetID())
			} else {
				c.checkers.AddWaitingRegion(region)
			}
		}
		// Updates the label level isolation statistics.
		c.cluster.updateRegionsLabelLevelStats(regions)
		if len(key) == 0 {
			patrolCheckRegionsGauge.Set(time.Since(start).Seconds())
			start = time.Now()
		}
		failpoint.Inject("break-patrol", func() {
			failpoint.Break()
		})
	}
}

// checkPriorityRegions checks priority regions
func (c *coordinator) checkPriorityRegions() {
	items := c.checkers.GetPriorityRegions()
	removes := make([]uint64, 0)
	regionListGauge.WithLabelValues("priority_list").Set(float64(len(items)))
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

func (c *coordinator) checkSuspectRegions() {
	for _, id := range c.cluster.GetSuspectRegions() {
		region := c.cluster.GetRegion(id)
		if region == nil {
			// the region could be recent split, continue to wait.
			continue
		}
		if c.opController.GetOperator(id) != nil {
			c.cluster.RemoveSuspectRegion(id)
			continue
		}
		ops := c.checkers.CheckRegion(region)
		if len(ops) == 0 {
			continue
		}

		if !c.opController.ExceedStoreLimit(ops...) {
			c.opController.AddWaitingOperator(ops...)
			c.cluster.RemoveSuspectRegion(region.GetID())
		}
	}
}

// checkSuspectRanges would pop one suspect key range group
// The regions of new version key range and old version key range would be placed into
// the suspect regions map
func (c *coordinator) checkSuspectRanges() {
	defer c.wg.Done()
	log.Info("coordinator begins to check suspect key ranges")
	ticker := time.NewTicker(checkSuspectRangesInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("check suspect key ranges has been stopped")
			return
		case <-ticker.C:
			keyRange, success := c.cluster.PopOneSuspectKeyRange()
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
				c.cluster.AddSuspectKeyRange(lastRegion.GetEndKey(), keyRange[1])
			}
			c.cluster.AddSuspectRegions(regionIDList...)
		}
	}
}

func (c *coordinator) checkWaitingRegions() {
	items := c.checkers.GetWaitingRegions()
	regionListGauge.WithLabelValues("waiting_list").Set(float64(len(items)))
	for _, item := range items {
		id := item.Key
		region := c.cluster.GetRegion(id)
		if region == nil {
			// the region could be recent split, continue to wait.
			continue
		}
		if c.opController.GetOperator(id) != nil {
			c.checkers.RemoveWaitingRegion(id)
			continue
		}
		ops := c.checkers.CheckRegion(region)
		if len(ops) == 0 {
			continue
		}

		if !c.opController.ExceedStoreLimit(ops...) {
			c.opController.AddWaitingOperator(ops...)
			c.checkers.RemoveWaitingRegion(region.GetID())
		}
	}
}

// drivePushOperator is used to push the unfinished operator to the executor.
func (c *coordinator) drivePushOperator() {
	defer logutil.LogPanic()

	defer c.wg.Done()
	log.Info("coordinator begins to actively drive push operator")
	ticker := time.NewTicker(schedule.PushOperatorTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("drive push operator has been stopped")
			return
		case <-ticker.C:
			c.opController.PushOperators()
		}
	}
}

func (c *coordinator) run() {
	ticker := time.NewTicker(runSchedulerCheckInterval)
	defer ticker.Stop()
	log.Info("coordinator starts to collect cluster information")
	for {
		if c.shouldRun() {
			log.Info("coordinator has finished cluster information preparation")
			break
		}
		select {
		case <-ticker.C:
		case <-c.ctx.Done():
			log.Info("coordinator stops running")
			return
		}
	}
	log.Info("coordinator starts to run schedulers")
	var (
		scheduleNames []string
		configs       []string
		err           error
	)
	for i := 0; i < maxLoadConfigRetries; i++ {
		scheduleNames, configs, err = c.cluster.storage.LoadAllScheduleConfig()
		select {
		case <-c.ctx.Done():
			log.Info("coordinator stops running")
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

	scheduleCfg := c.cluster.opt.GetScheduleConfig().Clone()
	// The new way to create scheduler with the independent configuration.
	for i, name := range scheduleNames {
		data := configs[i]
		typ := schedule.FindSchedulerTypeByName(name)
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
		s, err := schedule.CreateScheduler(cfg.Type, c.opController, c.cluster.storage, schedule.ConfigJSONDecoder([]byte(data)))
		if err != nil {
			log.Error("can not create scheduler with independent configuration", zap.String("scheduler-name", name), zap.Strings("scheduler-args", cfg.Args), errs.ZapError(err))
			continue
		}
		log.Info("create scheduler with independent configuration", zap.String("scheduler-name", s.GetName()))
		if err = c.addScheduler(s); err != nil {
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

		s, err := schedule.CreateScheduler(schedulerCfg.Type, c.opController, c.cluster.storage, schedule.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args))
		if err != nil {
			log.Error("can not create scheduler", zap.String("scheduler-type", schedulerCfg.Type), zap.Strings("scheduler-args", schedulerCfg.Args), errs.ZapError(err))
			continue
		}

		log.Info("create scheduler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", schedulerCfg.Args))
		if err = c.addScheduler(s, schedulerCfg.Args...); err != nil && !errors.ErrorEqual(err, errs.ErrSchedulerExisted.FastGenByArgs()) {
			log.Error("can not add scheduler", zap.String("scheduler-name", s.GetName()), zap.Strings("scheduler-args", schedulerCfg.Args), errs.ZapError(err))
		} else {
			// Only records the valid scheduler config.
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
		}
	}

	// Removes the invalid scheduler config and persist.
	scheduleCfg.Schedulers = scheduleCfg.Schedulers[:k]
	c.cluster.opt.SetScheduleConfig(scheduleCfg)
	if err := c.cluster.opt.Persist(c.cluster.storage); err != nil {
		log.Error("cannot persist schedule config", errs.ZapError(err))
	}

	c.wg.Add(3)
	// Starts to patrol regions.
	go c.patrolRegions()
	// Checks suspect key ranges
	go c.checkSuspectRanges()
	go c.drivePushOperator()
}

// LoadPlugin load user plugin
func (c *coordinator) LoadPlugin(pluginPath string, ch chan string) {
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
	s, err := schedule.CreateScheduler(schedulerType(), c.opController, c.cluster.storage, schedule.ConfigSliceDecoder(schedulerType(), schedulerArgs()))
	if err != nil {
		log.Error("can not create scheduler", zap.String("scheduler-type", schedulerType()), errs.ZapError(err))
		return
	}
	log.Info("create scheduler", zap.String("scheduler-name", s.GetName()))
	if err = c.addScheduler(s); err != nil {
		log.Error("can't add scheduler", zap.String("scheduler-name", s.GetName()), errs.ZapError(err))
		return
	}

	c.wg.Add(1)
	go c.waitPluginUnload(pluginPath, s.GetName(), ch)
}

func (c *coordinator) waitPluginUnload(pluginPath, schedulerName string, ch chan string) {
	defer logutil.LogPanic()
	defer c.wg.Done()
	// Get signal from channel which means user unload the plugin
	for {
		select {
		case action := <-ch:
			if action == PluginUnload {
				err := c.removeScheduler(schedulerName)
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

func (c *coordinator) stop() {
	c.cancel()
}

// Hack to retrieve info from scheduler.
// TODO: remove it.
type hasHotStatus interface {
	GetHotStatus(typ string) *statistics.StoreHotPeersInfos
	GetPendingInfluence() map[uint64]*schedulers.Influence
}

func (c *coordinator) getHotWriteRegions() *statistics.StoreHotPeersInfos {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[schedulers.HotRegionName]
	if !ok {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotStatus(schedulers.HotWriteRegionType)
	}
	return nil
}

func (c *coordinator) getHotReadRegions() *statistics.StoreHotPeersInfos {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[schedulers.HotRegionName]
	if !ok {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotStatus(schedulers.HotReadRegionType)
	}
	return nil
}

func (c *coordinator) getSchedulers() []string {
	c.RLock()
	defer c.RUnlock()
	names := make([]string, 0, len(c.schedulers))
	for name := range c.schedulers {
		names = append(names, name)
	}
	return names
}

func (c *coordinator) getSchedulerHandlers() map[string]http.Handler {
	c.RLock()
	defer c.RUnlock()
	handlers := make(map[string]http.Handler, len(c.schedulers))
	for name, scheduler := range c.schedulers {
		handlers[name] = scheduler.Scheduler
	}
	return handlers
}

func (c *coordinator) collectSchedulerMetrics() {
	c.RLock()
	defer c.RUnlock()
	for _, s := range c.schedulers {
		var allowScheduler float64
		// If the scheduler is not allowed to schedule, it will disappear in Grafana panel.
		// See issue #1341.
		if !s.IsPaused() {
			allowScheduler = 1
		}
		schedulerStatusGauge.WithLabelValues(s.GetName(), "allow").Set(allowScheduler)
	}
}

func (c *coordinator) resetSchedulerMetrics() {
	schedulerStatusGauge.Reset()
}

func (c *coordinator) collectHotSpotMetrics() {
	c.RLock()
	// Collects hot write region metrics.
	s, ok := c.schedulers[schedulers.HotRegionName]
	if !ok {
		c.RUnlock()
		return
	}
	c.RUnlock()
	stores := c.cluster.GetStores()
	// Collects hot write region metrics.
	collectHotMetrics(s, stores, schedulers.HotWriteRegionType)
	// Collects hot read region metrics.
	collectHotMetrics(s, stores, schedulers.HotReadRegionType)
	// Collects pending influence.
	collectPendingInfluence(s, stores)
}

func collectHotMetrics(s *scheduleController, stores []*core.StoreInfo, typ string) {
	status := s.Scheduler.(hasHotStatus).GetHotStatus(typ)
	var (
		kind                      string
		byteTyp, keyTyp, queryTyp statistics.RegionStatKind
	)

	switch typ {
	case schedulers.HotReadRegionType:
		kind, byteTyp, keyTyp, queryTyp = "read", statistics.RegionReadBytes, statistics.RegionReadKeys, statistics.RegionReadQuery
	case schedulers.HotWriteRegionType:
		kind, byteTyp, keyTyp, queryTyp = "write", statistics.RegionWriteBytes, statistics.RegionWriteKeys, statistics.RegionWriteQuery
	}
	for _, s := range stores {
		storeAddress := s.GetAddress()
		storeID := s.GetID()
		storeLabel := fmt.Sprintf("%d", storeID)
		stat, ok := status.AsLeader[storeID]
		if ok {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_bytes_as_leader").Set(stat.TotalLoads[byteTyp])
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_keys_as_leader").Set(stat.TotalLoads[keyTyp])
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_query_as_leader").Set(stat.TotalLoads[queryTyp])
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_"+kind+"_region_as_leader").Set(float64(stat.Count))
		} else {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_bytes_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_keys_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_query_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_"+kind+"_region_as_leader").Set(0)
		}

		stat, ok = status.AsPeer[storeID]
		if ok {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_bytes_as_peer").Set(stat.TotalLoads[byteTyp])
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_keys_as_peer").Set(stat.TotalLoads[keyTyp])
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_query_as_peer").Set(stat.TotalLoads[queryTyp])
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_"+kind+"_region_as_peer").Set(float64(stat.Count))
		} else {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_bytes_as_peer").Set(0)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_keys_as_peer").Set(0)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_"+kind+"_query_as_peer").Set(0)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_"+kind+"_region_as_peer").Set(0)
		}
	}
}

func collectPendingInfluence(s *scheduleController, stores []*core.StoreInfo) {
	pendings := s.Scheduler.(hasHotStatus).GetPendingInfluence()
	for _, s := range stores {
		storeAddress := s.GetAddress()
		storeID := s.GetID()
		storeLabel := fmt.Sprintf("%d", storeID)
		if infl := pendings[storeID]; infl != nil {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "pending_influence_byte_rate").Set(infl.Loads[statistics.ByteDim])
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "pending_influence_key_rate").Set(infl.Loads[statistics.KeyDim])
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "pending_influence_query_rate").Set(infl.Loads[statistics.QueryDim])
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "pending_influence_count").Set(infl.Count)
		}
	}
}

func (c *coordinator) resetHotSpotMetrics() {
	hotSpotStatusGauge.Reset()
}

func (c *coordinator) shouldRun() bool {
	return c.cluster.isPrepared()
}

func (c *coordinator) addScheduler(scheduler schedule.Scheduler, args ...string) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.GetName()]; ok {
		return errs.ErrSchedulerExisted.FastGenByArgs()
	}

	s := newScheduleController(c, scheduler)
	if err := s.Prepare(c.cluster); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.runScheduler(s)
	c.schedulers[s.GetName()] = s
	c.cluster.opt.AddSchedulerCfg(s.GetType(), args)
	return nil
}

func (c *coordinator) removeScheduler(name string) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return errs.ErrSchedulerNotFound.FastGenByArgs()
	}

	opt := c.cluster.opt
	if err := c.removeOptScheduler(opt, name); err != nil {
		log.Error("can not remove scheduler", zap.String("scheduler-name", name), errs.ZapError(err))
		return err
	}

	if err := opt.Persist(c.cluster.storage); err != nil {
		log.Error("the option can not persist scheduler config", errs.ZapError(err))
		return err
	}

	if err := c.cluster.storage.RemoveScheduleConfig(name); err != nil {
		log.Error("can not remove the scheduler config", errs.ZapError(err))
		return err
	}

	s.Stop()
	schedulerStatusGauge.WithLabelValues(name, "allow").Set(0)
	delete(c.schedulers, name)

	return nil
}

func (c *coordinator) removeOptScheduler(o *config.PersistOptions, name string) error {
	v := o.GetScheduleConfig().Clone()
	for i, schedulerCfg := range v.Schedulers {
		// To create a temporary scheduler is just used to get scheduler's name
		decoder := schedule.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args)
		tmp, err := schedule.CreateScheduler(schedulerCfg.Type, schedule.NewOperatorController(c.ctx, nil, nil), core.NewStorage(kv.NewMemoryKV()), decoder)
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

func (c *coordinator) pauseOrResumeScheduler(name string, t int64) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return errs.ErrNotBootstrapped.FastGenByArgs()
	}
	var s []*scheduleController
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
		var delayUntil int64
		if t > 0 {
			delayUntil = time.Now().Unix() + t
		}
		atomic.StoreInt64(&sc.delayUntil, delayUntil)
	}
	return err
}

func (c *coordinator) isSchedulerPaused(name string) (bool, error) {
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

func (c *coordinator) isSchedulerDisabled(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return false, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	t := s.GetType()
	scheduleConfig := c.cluster.GetOpts().GetScheduleConfig()
	for _, s := range scheduleConfig.Schedulers {
		if t == s.Type {
			return s.Disable, nil
		}
	}
	return false, nil
}

func (c *coordinator) isSchedulerExisted(name string) (bool, error) {
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

func (c *coordinator) runScheduler(s *scheduleController) {
	defer logutil.LogPanic()
	defer c.wg.Done()
	defer s.Cleanup(c.cluster)

	timer := time.NewTimer(s.GetInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(s.GetInterval())
			if !s.AllowSchedule() {
				continue
			}
			if op := s.Schedule(); len(op) > 0 {
				added := c.opController.AddWaitingOperator(op...)
				log.Debug("add operator", zap.Int("added", added), zap.Int("total", len(op)), zap.String("scheduler", s.GetName()))
			}

		case <-s.Ctx().Done():
			log.Info("scheduler has been stopped",
				zap.String("scheduler-name", s.GetName()),
				errs.ZapError(s.Ctx().Err()))
			return
		}
	}
}

func (c *coordinator) pauseOrResumeChecker(name string, t int64) error {
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

func (c *coordinator) isCheckerPaused(name string) (bool, error) {
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

// scheduleController is used to manage a scheduler to schedule.
type scheduleController struct {
	schedule.Scheduler
	cluster      *RaftCluster
	opController *schedule.OperatorController
	nextInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	delayUntil   int64
}

// newScheduleController creates a new scheduleController.
func newScheduleController(c *coordinator, s schedule.Scheduler) *scheduleController {
	ctx, cancel := context.WithCancel(c.ctx)
	return &scheduleController{
		Scheduler:    s,
		cluster:      c.cluster,
		opController: c.opController,
		nextInterval: s.GetMinInterval(),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (s *scheduleController) Ctx() context.Context {
	return s.ctx
}

func (s *scheduleController) Stop() {
	s.cancel()
}

func (s *scheduleController) Schedule() []*operator.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// no need to retry if schedule should stop to speed exit
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}
		cacheCluster := newCacheCluster(s.cluster)
		// If we have schedule, reset interval to the minimal interval.
		if op := s.Scheduler.Schedule(cacheCluster); op != nil {
			s.nextInterval = s.Scheduler.GetMinInterval()
			return op
		}
	}
	s.nextInterval = s.Scheduler.GetNextInterval(s.nextInterval)
	return nil
}

// GetInterval returns the interval of scheduling for a scheduler.
func (s *scheduleController) GetInterval() time.Duration {
	return s.nextInterval
}

// AllowSchedule returns if a scheduler is allowed to schedule.
func (s *scheduleController) AllowSchedule() bool {
	return s.Scheduler.IsScheduleAllowed(s.cluster) && !s.IsPaused()
}

// isPaused returns if a scheduler is paused.
func (s *scheduleController) IsPaused() bool {
	delayUntil := atomic.LoadInt64(&s.delayUntil)
	return time.Now().Unix() < delayUntil
}
