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
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"golang.org/x/net/context"
)

const (
	historiesCacheSize = 1000
	eventsCacheSize    = 1000
	maxScheduleRetries = 10
)

type coordinator struct {
	sync.RWMutex

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	cluster    *clusterInfo
	opt        *scheduleOption
	limiter    *scheduleLimiter
	checker    *replicaChecker
	operators  map[uint64]Operator
	schedulers map[string]*scheduleController

	histories *lruCache
	events    *fifoCache
}

func newCoordinator(cluster *clusterInfo, opt *scheduleOption) *coordinator {
	ctx, cancel := context.WithCancel(context.TODO())
	return &coordinator{
		ctx:        ctx,
		cancel:     cancel,
		cluster:    cluster,
		opt:        opt,
		limiter:    newScheduleLimiter(),
		checker:    newReplicaChecker(opt, cluster),
		operators:  make(map[uint64]Operator),
		schedulers: make(map[string]*scheduleController),
		histories:  newLRUCache(historiesCacheSize),
		events:     newFifoCache(eventsCacheSize),
	}
}

func (c *coordinator) dispatch(region *regionInfo) *pdpb.RegionHeartbeatResponse {
	// Check existed operator.
	if op := c.getOperator(region.GetId()); op != nil {
		res, finished := op.Do(region)
		if !finished {
			return res
		}
		c.removeOperator(op)
	}

	// Check replica operator.
	if c.limiter.operatorCount(regionKind) >= c.opt.GetReplicaScheduleLimit() {
		return nil
	}
	if op := c.checker.Check(region); op != nil {
		if c.addOperator(op) {
			res, _ := op.Do(region)
			return res
		}
	}

	return nil
}

func (c *coordinator) run() {
	c.addScheduler(newBalanceLeaderScheduler(c.opt))
	c.addScheduler(newBalanceStorageScheduler(c.opt))
}

func (c *coordinator) stop() {
	c.cancel()
	c.wg.Wait()
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

func (c *coordinator) addScheduler(scheduler Scheduler) bool {
	c.Lock()
	defer c.Unlock()

	s := newScheduleController(c, scheduler)
	if _, ok := c.schedulers[s.GetName()]; ok {
		return false
	}

	c.wg.Add(1)
	go c.runScheduler(s)
	c.schedulers[s.GetName()] = s
	return true
}

func (c *coordinator) removeScheduler(name string) bool {
	c.Lock()
	defer c.Unlock()

	s, ok := c.schedulers[name]
	if !ok {
		return false
	}

	s.Stop()
	delete(c.schedulers, name)
	return true
}

func (c *coordinator) runScheduler(s *scheduleController) {
	defer c.wg.Done()

	timer := time.NewTimer(s.GetInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(s.GetInterval())
			if !s.AllowSchedule() {
				continue
			}
			for i := 0; i < maxScheduleRetries; i++ {
				op := s.Schedule(c.cluster)
				if op == nil {
					continue
				}
				if c.addOperator(op) {
					break
				}
			}
		case <-s.Ctx().Done():
			log.Infof("%v stopped: %v", s.GetName(), s.Ctx().Err())
			return
		}
	}
}

func (c *coordinator) addOperator(op Operator) bool {
	c.Lock()
	defer c.Unlock()

	regionID := op.GetRegionID()
	if _, ok := c.operators[regionID]; ok {
		return false
	}

	c.limiter.addOperator(op)
	c.operators[regionID] = op
	collectOperatorCounterMetrics(op)
	return true
}

func (c *coordinator) removeOperator(op Operator) {
	c.Lock()
	defer c.Unlock()

	regionID := op.GetRegionID()
	c.limiter.removeOperator(op)
	delete(c.operators, regionID)

	c.histories.add(regionID, op)
}

func (c *coordinator) getOperator(regionID uint64) Operator {
	c.RLock()
	defer c.RUnlock()
	return c.operators[regionID]
}

func (c *coordinator) getOperators() map[uint64]Operator {
	c.RLock()
	defer c.RUnlock()

	operators := make(map[uint64]Operator)
	for id, op := range c.operators {
		operators[id] = op
	}

	return operators
}

func (c *coordinator) getHistories() []Operator {
	c.RLock()
	defer c.RUnlock()

	var operators []Operator
	for _, elem := range c.histories.elems() {
		operators = append(operators, elem.value.(Operator))
	}

	return operators
}

type scheduleLimiter struct {
	sync.RWMutex
	counts map[ResourceKind]uint64
}

func newScheduleLimiter() *scheduleLimiter {
	return &scheduleLimiter{
		counts: make(map[ResourceKind]uint64),
	}
}

func (l *scheduleLimiter) addOperator(op Operator) {
	l.Lock()
	defer l.Unlock()
	l.counts[op.GetResourceKind()]++
}

func (l *scheduleLimiter) removeOperator(op Operator) {
	l.Lock()
	defer l.Unlock()
	l.counts[op.GetResourceKind()]--
}

func (l *scheduleLimiter) operatorCount(kind ResourceKind) uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.counts[kind]
}

type scheduleController struct {
	Scheduler
	opt     *scheduleOption
	limiter *scheduleLimiter
	ctx     context.Context
	cancel  context.CancelFunc
}

func newScheduleController(c *coordinator, s Scheduler) *scheduleController {
	ctx, cancel := context.WithCancel(c.ctx)
	return &scheduleController{
		Scheduler: s,
		opt:       c.opt,
		limiter:   c.limiter,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (s *scheduleController) Ctx() context.Context {
	return s.ctx
}

func (s *scheduleController) Stop() {
	s.cancel()
}

func (s *scheduleController) GetInterval() time.Duration {
	limit := s.GetResourceLimit()
	interval := s.opt.GetScheduleInterval().Nanoseconds()
	return time.Duration(uint64(interval)/limit) * time.Nanosecond
}

func (s *scheduleController) AllowSchedule() bool {
	return s.limiter.operatorCount(s.GetResourceKind()) < s.GetResourceLimit()
}

func collectOperatorCounterMetrics(op Operator) {
	metrics := make(map[string]uint64)
	for _, op := range op.(*regionOperator).Ops {
		switch o := op.(type) {
		case *changePeerOperator:
			metrics[o.Name]++
		case *transferLeaderOperator:
			metrics[o.Name]++
		}
	}

	for label, value := range metrics {
		operatorCounter.WithLabelValues(label).Add(float64(value))
	}
}
