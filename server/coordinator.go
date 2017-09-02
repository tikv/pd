// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedulers"
	"golang.org/x/net/context"
)

const (
	runSchedulerCheckInterval = 3 * time.Second
	collectFactor             = 0.8
	historiesCacheSize        = 1000
	eventsCacheSize           = 1000
	maxScheduleRetries        = 10
	maxScheduleInterval       = time.Minute
	minScheduleInterval       = time.Millisecond * 10
	minSlowScheduleInterval   = time.Second * 3
	scheduleIntervalFactor    = 1.3

	writeStatLRUMaxLen            = 1000
	hotRegionMinWriteRate         = 16 * 1024
	regionHeartBeatReportInterval = 60
	regionheartbeatSendChanCap    = 1024
	storeHeartBeatReportInterval  = 10
	minHotRegionReportInterval    = 3
	hotRegionAntiCount            = 1
	hotRegionScheduleName         = "balance-hot-region-scheduler"
)

var (
	hotRegionLowThreshold = 3
	errSchedulerExisted   = errors.New("scheduler existed")
	errSchedulerNotFound  = errors.New("scheduler not found")
)

type coordinator struct {
	sync.RWMutex

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	cluster    *clusterInfo
	opt        *scheduleOption
	limiter    *scheduleLimiter
	checker    *schedule.ReplicaChecker
	operators  map[uint64]schedule.Operator
	schedulers map[string]*scheduleController

	histories *cache.LRU
	events    *cache.FIFO

	hbStreams *heartbeatStreams
}

func newCoordinator(cluster *clusterInfo, opt *scheduleOption, hbStreams *heartbeatStreams) *coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &coordinator{
		ctx:        ctx,
		cancel:     cancel,
		cluster:    cluster,
		opt:        opt,
		limiter:    newScheduleLimiter(),
		checker:    schedule.NewReplicaChecker(opt, cluster),
		operators:  make(map[uint64]schedule.Operator),
		schedulers: make(map[string]*scheduleController),
		histories:  cache.NewLRU(historiesCacheSize),
		events:     cache.NewFIFO(eventsCacheSize),
		hbStreams:  hbStreams,
	}
}

func (c *coordinator) dispatch(region *core.RegionInfo) {
	// Check existed operator.
	if op := c.getOperator(region.GetId()); op != nil {
		res, finished := op.Do(region)
		if !finished {
			collectOperatorCounterMetrics(op)
			if res != nil {
				c.hbStreams.sendMsg(region, res)
			}
			return
		}
		c.removeOperator(op)
	}

	// Check replica operator.
	if c.limiter.operatorCount(core.RegionKind) >= c.opt.GetReplicaScheduleLimit() {
		return
	}
	if op := c.checker.Check(region); op != nil {
		c.addOperator(op)
	}
}

func (c *coordinator) run() {
	ticker := time.NewTicker(runSchedulerCheckInterval)
	defer ticker.Stop()
	log.Info("coordinator: Start collect cluster information")
	for {
		if c.shouldRun() {
			log.Info("coordinator: Cluster information is prepared")
			break
		}
		select {
		case <-ticker.C:
		case <-c.ctx.Done():
			log.Info("coordinator: Stopped coordinator")
			return
		}
	}
	log.Info("coordinator: Run scheduler")
	c.addScheduler(schedulers.NewBalanceLeaderScheduler(c.opt), minScheduleInterval)
	c.addScheduler(schedulers.NewBalanceRegionScheduler(c.opt), minScheduleInterval)
	c.addScheduler(schedulers.NewBalanceHotRegionScheduler(c.opt), minSlowScheduleInterval)
}

func (c *coordinator) stop() {
	c.cancel()
	c.wg.Wait()
}

// Hack to retrive info from scheduler.
// TODO: remove it.
type hasHotStatus interface {
	GetStatus() *core.StoreHotRegionInfos
}

func (c *coordinator) getHotWriteRegions() *core.StoreHotRegionInfos {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[hotRegionScheduleName]
	if !ok {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetStatus()
	}
	return nil
}

func (c *coordinator) getSchedulers() []string {
	c.RLock()
	defer c.RUnlock()

	var names []string
	for name := range c.schedulers {
		names = append(names, name)
	}
	return names
}

func (c *coordinator) collectSchedulerMetrics() {
	c.RLock()
	defer c.RUnlock()
	for _, s := range c.schedulers {
		var allowScheduler float64
		if s.AllowSchedule() {
			allowScheduler = 1
		}
		limit := float64(s.GetResourceLimit())

		schedulerStatusGauge.WithLabelValues(s.GetName(), "allow").Set(allowScheduler)
		schedulerStatusGauge.WithLabelValues(s.GetName(), "limit").Set(limit)
	}
}

func (c *coordinator) collectHotSpotMetrics() {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[hotRegionScheduleName]
	if !ok {
		return
	}
	status := s.Scheduler.(hasHotStatus).GetStatus()
	for storeID, stat := range status.AsPeer {
		store := fmt.Sprintf("store_%d", storeID)
		totalWriteBytes := float64(stat.WrittenBytes)
		hotWriteRegionCount := float64(stat.RegionsCount)

		hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_peer").Set(totalWriteBytes)
		hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_peer").Set(hotWriteRegionCount)
	}
	for storeID, stat := range status.AsLeader {
		store := fmt.Sprintf("store_%d", storeID)
		totalWriteBytes := float64(stat.WrittenBytes)
		hotWriteRegionCount := float64(stat.RegionsCount)

		hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_leader").Set(totalWriteBytes)
		hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_leader").Set(hotWriteRegionCount)
	}
}

func (c *coordinator) shouldRun() bool {
	return c.cluster.isPrepared()
}

func (c *coordinator) addScheduler(scheduler schedule.Scheduler, interval time.Duration) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.GetName()]; ok {
		return errSchedulerExisted
	}

	s := newScheduleController(c, scheduler, interval)
	if err := s.Prepare(c.cluster); err != nil {
		return errors.Trace(err)
	}

	c.wg.Add(1)
	go c.runScheduler(s)
	c.schedulers[s.GetName()] = s
	return nil
}

func (c *coordinator) removeScheduler(name string) error {
	c.Lock()
	defer c.Unlock()

	s, ok := c.schedulers[name]
	if !ok {
		return errSchedulerNotFound
	}

	s.Stop()
	delete(c.schedulers, name)
	return nil
}

func (c *coordinator) runScheduler(s *scheduleController) {
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
			if op := s.Schedule(c.cluster); op != nil {
				c.addOperator(op)
			}

		case <-s.Ctx().Done():
			log.Infof("%v stopped: %v", s.GetName(), s.Ctx().Err())
			return
		}
	}
}

func (c *coordinator) addOperator(op schedule.Operator) bool {
	c.Lock()
	defer c.Unlock()
	regionID := op.GetRegionID()

	log.Infof("[region %v] add operator: %+v", regionID, op)

	if old, ok := c.operators[regionID]; ok {
		if !isHigherPriorityOperator(op, old) {
			log.Infof("[region %v] cancel add operator, old: %+v", regionID, old)
			return false
		}
		log.Infof("[region %v] replace old operator: %+v", regionID, old)
		old.SetState(schedule.OperatorReplaced)
		c.removeOperatorLocked(old)
	}

	c.histories.Put(regionID, op)
	c.limiter.addOperator(op)
	c.operators[regionID] = op

	if region := c.cluster.GetRegion(op.GetRegionID()); region != nil {
		if msg, _ := op.Do(region); msg != nil {
			c.hbStreams.sendMsg(region, msg)
		}
	}

	collectOperatorCounterMetrics(op)
	return true
}

func isHigherPriorityOperator(new, old schedule.Operator) bool {
	if new.GetResourceKind() == core.AdminKind {
		return true
	}
	if new.GetResourceKind() == core.PriorityKind && old.GetResourceKind() != core.PriorityKind {
		return true
	}
	return false
}

func (c *coordinator) removeOperator(op schedule.Operator) {
	c.Lock()
	defer c.Unlock()
	c.removeOperatorLocked(op)
}

func (c *coordinator) removeOperatorLocked(op schedule.Operator) {
	regionID := op.GetRegionID()
	c.limiter.removeOperator(op)
	delete(c.operators, regionID)

	c.histories.Put(regionID, op)
	collectOperatorCounterMetrics(op)
}

func (c *coordinator) getOperator(regionID uint64) schedule.Operator {
	c.RLock()
	defer c.RUnlock()
	return c.operators[regionID]
}

func (c *coordinator) getOperators() []schedule.Operator {
	c.RLock()
	defer c.RUnlock()

	var operators []schedule.Operator
	for _, op := range c.operators {
		operators = append(operators, op)
	}

	return operators
}

func (c *coordinator) getHistories() []schedule.Operator {
	c.RLock()
	defer c.RUnlock()

	var operators []schedule.Operator
	for _, elem := range c.histories.Elems() {
		operators = append(operators, elem.Value.(schedule.Operator))
	}

	return operators
}

func (c *coordinator) getHistoriesOfKind(kind core.ResourceKind) []schedule.Operator {
	c.RLock()
	defer c.RUnlock()

	var operators []schedule.Operator
	for _, elem := range c.histories.Elems() {
		op := elem.Value.(schedule.Operator)
		if op.GetResourceKind() == kind {
			operators = append(operators, op)
		}
	}

	return operators
}

type scheduleLimiter struct {
	sync.RWMutex
	counts map[core.ResourceKind]uint64
}

func newScheduleLimiter() *scheduleLimiter {
	return &scheduleLimiter{
		counts: make(map[core.ResourceKind]uint64),
	}
}

func (l *scheduleLimiter) addOperator(op schedule.Operator) {
	l.Lock()
	defer l.Unlock()
	l.counts[op.GetResourceKind()]++
}

func (l *scheduleLimiter) removeOperator(op schedule.Operator) {
	l.Lock()
	defer l.Unlock()
	l.counts[op.GetResourceKind()]--
}

func (l *scheduleLimiter) operatorCount(kind core.ResourceKind) uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.counts[kind]
}

type scheduleController struct {
	schedule.Scheduler
	opt          *scheduleOption
	limiter      *scheduleLimiter
	nextInterval time.Duration
	minInterval  time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
}

func newScheduleController(c *coordinator, s schedule.Scheduler, minInterval time.Duration) *scheduleController {
	ctx, cancel := context.WithCancel(c.ctx)
	return &scheduleController{
		Scheduler:    s,
		opt:          c.opt,
		limiter:      c.limiter,
		nextInterval: minInterval,
		minInterval:  minInterval,
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

func (s *scheduleController) Schedule(cluster *clusterInfo) schedule.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// If we have schedule, reset interval to the minimal interval.
		if op := s.Scheduler.Schedule(cluster); op != nil {
			s.nextInterval = s.minInterval
			return op
		}
	}

	// If we have no schedule, increase the interval exponentially.
	s.nextInterval = minDuration(time.Duration(float64(s.nextInterval)*scheduleIntervalFactor), maxScheduleInterval)

	return nil
}

func (s *scheduleController) GetInterval() time.Duration {
	return s.nextInterval
}

func (s *scheduleController) AllowSchedule() bool {
	return s.limiter.operatorCount(s.GetResourceKind()) < s.GetResourceLimit()
}

func collectOperatorCounterMetrics(op schedule.Operator) {
	regionOp, ok := op.(*schedule.RegionOperator)
	if !ok {
		return
	}
	for _, op := range regionOp.Ops {
		operatorCounter.WithLabelValues(op.GetName(), op.GetState().String()).Add(1)
	}
}
