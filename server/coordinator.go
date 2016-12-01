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
	"context"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	regionCacheTTL     = 2 * time.Minute
	historiesCacheSize = 1000
	eventsCacheSize    = 1000
)

type coordinator struct {
	sync.RWMutex

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	cluster *clusterInfo
	opt     *scheduleOption
	checker *replicaChecker

	schedulers  map[string]Scheduler
	controllers map[string]Controller

	operators map[ResourceKind]map[uint64]*balanceOperator

	regionCache *expireRegionCache
	histories   *lruCache
	events      *fifoCache
}

func newCoordinator(cluster *clusterInfo, opt *scheduleOption) *coordinator {
	c := &coordinator{
		cluster:     cluster,
		opt:         opt,
		checker:     newReplicaChecker(cluster, opt),
		schedulers:  make(map[string]Scheduler),
		controllers: make(map[string]Controller),
		regionCache: newExpireRegionCache(regionCacheTTL, regionCacheTTL),
		histories:   newLRUCache(historiesCacheSize),
		events:      newFifoCache(eventsCacheSize),
	}

	c.ctx, c.cancel = context.WithCancel(context.TODO())

	c.operators = make(map[ResourceKind]map[uint64]*balanceOperator)
	c.operators[leaderKind] = make(map[uint64]*balanceOperator)
	c.operators[storageKind] = make(map[uint64]*balanceOperator)

	return c
}

func (c *coordinator) dispatch(region *regionInfo) *pdpb.RegionHeartbeatResponse {
	op := c.getOperator(region.GetId())
	if op == nil {
		op = c.checker.Check(region)
	}
	if op == nil {
		return nil
	}

	ctx := newOpContext(c.hookStartEvent, c.hookEndEvent)
	finished, res, err := op.Do(ctx, region)
	if err != nil {
		log.Errorf("failed to do operator for region %v: %v", region, err)
	}
	if err != nil || finished {
		c.removeOperator(op)
	}
	return res
}

func (c *coordinator) run() {
	c.addScheduler(newLeaderBalancer(c.opt), newLeaderController(c))
	c.addScheduler(newStorageBalancer(c.opt), newStorageController(c))
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

func (c *coordinator) addScheduler(s Scheduler, ctrl Controller) bool {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[s.GetName()]; ok {
		return false
	}

	c.wg.Add(1)
	go c.runScheduler(s, ctrl)

	c.schedulers[s.GetName()] = s
	c.controllers[s.GetName()] = ctrl
	return true
}

func (c *coordinator) removeScheduler(name string) bool {
	c.Lock()
	defer c.Unlock()

	ctrl, ok := c.controllers[name]
	if !ok {
		return false
	}

	ctrl.Stop()
	delete(c.schedulers, name)
	delete(c.controllers, name)
	return true
}

func (c *coordinator) runScheduler(s Scheduler, ctrl Controller) {
	defer c.wg.Done()

	timer := time.NewTimer(ctrl.GetInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(ctrl.GetInterval())
			if !ctrl.AllowSchedule() {
				continue
			}
			if op := s.Schedule(c.cluster); op != nil {
				c.addOperator(s.GetResourceKind(), op)
			}
		case <-ctrl.Ctx().Done():
			log.Infof("%v stopped: %v", s.GetName(), ctrl.Ctx().Err())
			return
		}
	}
}

func (c *coordinator) addOperator(kind ResourceKind, op *balanceOperator) {
	c.Lock()
	defer c.Unlock()

	regionID := op.Region.GetId()
	if c.getOperatorLocked(regionID) != nil {
		return
	}
	if _, ok := c.regionCache.get(regionID); ok {
		return
	}

	collectOperatorCounterMetrics(op)
	c.operators[kind][op.Region.GetId()] = op
}

func (c *coordinator) setOperator(kind ResourceKind, op *balanceOperator) {
	c.Lock()
	defer c.Unlock()

	collectOperatorCounterMetrics(op)
	c.operators[kind][op.Region.GetId()] = op
}

func (c *coordinator) removeOperator(op *balanceOperator) {
	c.Lock()
	defer c.Unlock()

	regionID := op.Region.GetId()
	c.histories.add(regionID, op)
	c.regionCache.set(regionID, nil)

	for _, ops := range c.operators {
		delete(ops, regionID)
	}
}

func (c *coordinator) getOperator(regionID uint64) *balanceOperator {
	c.RLock()
	defer c.RUnlock()
	return c.getOperatorLocked(regionID)
}

func (c *coordinator) getOperatorLocked(regionID uint64) *balanceOperator {
	for _, ops := range c.operators {
		if op, ok := ops[regionID]; ok {
			return op
		}
	}
	return nil
}

func (c *coordinator) getOperators() map[uint64]Operator {
	c.RLock()
	defer c.RUnlock()

	operators := make(map[uint64]Operator)
	for _, ops := range c.operators {
		for id, op := range ops {
			operators[id] = op
		}
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

func (c *coordinator) getOperatorCount(kind ResourceKind) int {
	c.RLock()
	defer c.RUnlock()
	return len(c.operators[kind])
}

// Controller is an interface to control the speed of different schedulers.
type Controller interface {
	Ctx() context.Context
	Stop()
	GetInterval() time.Duration
	AllowSchedule() bool
}

type leaderController struct {
	c      *coordinator
	ctx    context.Context
	cancel context.CancelFunc
}

func newLeaderController(c *coordinator) *leaderController {
	l := &leaderController{c: c}
	l.ctx, l.cancel = context.WithCancel(c.ctx)
	return l
}

func (l *leaderController) Ctx() context.Context {
	return l.ctx
}

func (l *leaderController) Stop() {
	l.cancel()
}

func (l *leaderController) GetInterval() time.Duration {
	return l.c.opt.GetLeaderScheduleInterval()
}

func (l *leaderController) AllowSchedule() bool {
	return l.c.getOperatorCount(leaderKind) < int(l.c.opt.GetLeaderScheduleLimit())
}

type storageController struct {
	c      *coordinator
	ctx    context.Context
	cancel context.CancelFunc
}

func newStorageController(c *coordinator) *storageController {
	s := &storageController{c: c}
	s.ctx, s.cancel = context.WithCancel(c.ctx)
	return s
}

func (s *storageController) Ctx() context.Context {
	return s.ctx
}

func (s *storageController) Stop() {
	s.cancel()
}

func (s *storageController) GetInterval() time.Duration {
	return s.c.opt.GetStorageScheduleInterval()
}

func (s *storageController) AllowSchedule() bool {
	return s.c.getOperatorCount(storageKind) < int(s.c.opt.GetStorageScheduleLimit())
}

func collectOperatorCounterMetrics(bop *balanceOperator) {
	metrics := make(map[string]uint64)
	prefix := ""
	switch bop.Type {
	case adminOP:
		prefix = "admin_"
	case replicaOP:
		prefix = "replica_"
	case balanceOP:
		prefix = "balance_"
	}
	for _, op := range bop.Ops {
		if _, ok := op.(*onceOperator); ok {
			op = op.(*onceOperator).Op
		}
		switch o := op.(type) {
		case *changePeerOperator:
			metrics[prefix+o.Name]++
		case *transferLeaderOperator:
			metrics[prefix+o.Name]++
		}
	}

	for label, value := range metrics {
		operatorCounter.WithLabelValues(label).Add(float64(value))
	}
}
