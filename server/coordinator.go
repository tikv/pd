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

	cluster *clusterInfo
	opt     *scheduleOption
	checker *replicaCheckController

	schedulers map[string]*ScheduleController
	operators  map[ResourceKind]map[uint64]Operator

	histories *lruCache
	events    *fifoCache
}

func newCoordinator(cluster *clusterInfo, opt *scheduleOption) *coordinator {
	c := &coordinator{
		cluster:    cluster,
		opt:        opt,
		schedulers: make(map[string]*ScheduleController),
		histories:  newLRUCache(historiesCacheSize),
		events:     newFifoCache(eventsCacheSize),
	}

	c.ctx, c.cancel = context.WithCancel(context.TODO())

	c.checker = newReplicaCheckController(c)
	c.operators = make(map[ResourceKind]map[uint64]Operator)
	c.operators[leaderKind] = make(map[uint64]Operator)
	c.operators[storageKind] = make(map[uint64]Operator)

	return c
}

func (c *coordinator) dispatch(region *regionInfo) *pdpb.RegionHeartbeatResponse {
	c.checker.Check(region)

	op := c.getOperator(region.GetId())
	if op == nil {
		return nil
	}

	res, finished := op.Do(region)
	if finished {
		c.removeOperator(op)
	}
	return res
}

func (c *coordinator) run() {
	c.addScheduler(newLeaderScheduleController(c, newLeaderBalancer(c.opt)))
	c.addScheduler(newStorageScheduleController(c, newStorageBalancer(c.opt)))
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

func (c *coordinator) addScheduler(s *ScheduleController) bool {
	c.Lock()
	defer c.Unlock()

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

func (c *coordinator) runScheduler(s *ScheduleController) {
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
	if c.getOperatorLocked(regionID) != nil {
		return false
	}

	collectOperatorCounterMetrics(op)
	c.operators[op.GetResourceKind()][regionID] = op
	return true
}

func (c *coordinator) removeOperator(op Operator) {
	c.Lock()
	defer c.Unlock()

	regionID := op.GetRegionID()
	c.histories.add(regionID, op)

	for _, ops := range c.operators {
		delete(ops, regionID)
	}
}

func (c *coordinator) getOperator(regionID uint64) Operator {
	c.RLock()
	defer c.RUnlock()
	return c.getOperatorLocked(regionID)
}

func (c *coordinator) getOperatorLocked(regionID uint64) Operator {
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

// ScheduleController combines Scheduler with Controller.
type ScheduleController struct {
	Scheduler
	Controller
}

func newLeaderScheduleController(c *coordinator, s Scheduler) *ScheduleController {
	return &ScheduleController{
		Scheduler:  s,
		Controller: newLeaderController(c),
	}
}

func newStorageScheduleController(c *coordinator, s Scheduler) *ScheduleController {
	return &ScheduleController{
		Scheduler:  s,
		Controller: newStorageController(c),
	}
}

type replicaCheckController struct {
	c        *coordinator
	opt      *scheduleOption
	checker  *replicaChecker
	lastTime time.Time
}

func newReplicaCheckController(c *coordinator) *replicaCheckController {
	return &replicaCheckController{
		c:       c,
		opt:     c.opt,
		checker: newReplicaChecker(c.opt, c.cluster),
	}
}

func (r *replicaCheckController) Check(region *regionInfo) {
	// Check limit and interval.
	if time.Since(r.lastTime) < r.opt.GetReplicaScheduleInterval() {
		return
	}
	if r.c.getOperatorCount(storageKind) >= int(r.opt.GetReplicaScheduleLimit()) {
		return
	}

	op := r.checker.Check(region)
	if op == nil {
		return
	}

	if r.c.addOperator(op) {
		r.lastTime = time.Now()
	}
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
