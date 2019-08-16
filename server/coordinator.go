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
	"container/heap"
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

//
const (
	runSchedulerCheckInterval = 3 * time.Second
	collectFactor             = 0.8
	collectTimeout            = 5 * time.Minute
	historyKeepTime           = 5 * time.Minute
	maxScheduleRetries        = 10

	regionheartbeatSendChanCap = 1024
	hotRegionScheduleName      = "balance-hot-region-scheduler"

	patrolScanRegionLimit = 128 // It takes about 14 minutes to iterate 1 million regions.

	slowNotifyInterval = 5 * time.Second
	fastNotifyInterval = 2 * time.Second
	// PushOperatorTickInterval is the interval try to push the operator.
	PushOperatorTickInterval = 500 * time.Millisecond
)

// The source of dispatched region.
const (
	DispatchFromHeartBeat     = "heartbeat"
	DispatchFromNotifierQueue = "active push"
	DispatchFromCreate        = "create"
)

var (
	errSchedulerExisted  = errors.New("scheduler existed")
	errSchedulerNotFound = errors.New("scheduler not found")
)

type coordinator struct {
	sync.RWMutex

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	cluster          *clusterInfo
	limiter          *schedule.Limiter
	replicaChecker   *schedule.ReplicaChecker
	regionScatterer  *schedule.RegionScatterer
	namespaceChecker *schedule.NamespaceChecker
	mergeChecker     *schedule.MergeChecker
	operators        map[uint64]*schedule.Operator
	schedulers       map[string]*scheduleController
	classifier       namespace.Classifier
	histories        *list.List
	hbStreams        *heartbeatStreams
	opRecords        *OperatorRecords
	opNotifierQueue  operatorQueue
}

func newCoordinator(cluster *clusterInfo, hbStreams *heartbeatStreams, classifier namespace.Classifier) *coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &coordinator{
		ctx:              ctx,
		cancel:           cancel,
		cluster:          cluster,
		limiter:          schedule.NewLimiter(),
		replicaChecker:   schedule.NewReplicaChecker(cluster, classifier),
		regionScatterer:  schedule.NewRegionScatterer(cluster, classifier),
		namespaceChecker: schedule.NewNamespaceChecker(cluster, classifier),
		mergeChecker:     schedule.NewMergeChecker(cluster, classifier),
		operators:        make(map[uint64]*schedule.Operator),
		schedulers:       make(map[string]*scheduleController),
		classifier:       classifier,
		histories:        list.New(),
		hbStreams:        hbStreams,
		opRecords:        NewOperatorRecords(),
		opNotifierQueue:  make(operatorQueue, 0),
	}
}

func (c *coordinator) dispatch(region *core.RegionInfo, source string) {
	// Check existed operator.
	if op := c.getOperator(region.GetID()); op != nil {
		timeout := op.IsTimeout()
		if step := op.Check(region); step != nil && !timeout {
			operatorCounter.WithLabelValues(op.Desc(), "check").Inc()
			c.sendScheduleCommand(region, step, source)
			return
		}
		if op.IsFinish() {
			log.Info("operator finish", zap.Uint64("region-id", region.GetID()), zap.Reflect("operator", op))
			operatorCounter.WithLabelValues(op.Desc(), "finish").Inc()
			operatorDuration.WithLabelValues(op.Desc()).Observe(op.ElapsedTime().Seconds())
			c.pushHistory(op)
			c.opRecords.Put(op, pdpb.OperatorStatus_SUCCESS)
			c.removeOperator(op)
		} else if timeout {
			log.Info("operator timeout", zap.Uint64("region-id", region.GetID()), zap.Reflect("operator", op))
			operatorCounter.WithLabelValues(op.Desc(), "timeout").Inc()
			c.removeOperator(op)
			c.opRecords.Put(op, pdpb.OperatorStatus_TIMEOUT)
		}
	}
}

func (c *coordinator) patrolRegions() {
	defer logutil.LogPanic()

	defer c.wg.Done()
	timer := time.NewTimer(c.cluster.GetPatrolRegionInterval())
	defer timer.Stop()

	log.Info("coordinator starts patrol regions")
	start := time.Now()
	var key []byte
	for {
		select {
		case <-timer.C:
			timer.Reset(c.cluster.GetPatrolRegionInterval())
		case <-c.ctx.Done():
			log.Info("patrol regions has been stopped")
			return
		}

		regions := c.cluster.ScanRegions(key, patrolScanRegionLimit)
		if len(regions) == 0 {
			// reset scan key.
			key = nil
			continue
		}

		for _, region := range regions {
			// Skip the region if there is already a pending operator.
			if c.getOperator(region.GetID()) != nil {
				continue
			}

			key = region.GetEndKey()

			if c.checkRegion(region) {
				break
			}
		}
		// update label level isolation statistics.
		c.cluster.updateRegionsLabelLevelStats(regions)
		if len(key) == 0 {
			patrolCheckRegionsHistogram.Observe(time.Since(start).Seconds())
			start = time.Now()
		}
	}
}

// drivePushOperator is used to push the unfinished operator to the excutor.
func (c *coordinator) drivePushOperator() {
	defer logutil.LogPanic()

	defer c.wg.Done()
	log.Info("coordinator begins to actively drive push operator")
	ticker := time.NewTicker(PushOperatorTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("drive push operator has been stopped")
			return
		case <-ticker.C:
			c.PushOperators()
		}
	}
}

func (c *coordinator) checkRegion(region *core.RegionInfo) bool {
	// If PD has restarted, it need to check learners added before and promote them.
	// Don't check isRaftLearnerEnabled cause it may be disable learner feature but still some learners to promote.
	for _, p := range region.GetLearners() {
		if region.GetPendingLearner(p.GetId()) != nil {
			continue
		}
		step := schedule.PromoteLearner{
			ToStore: p.GetStoreId(),
			PeerID:  p.GetId(),
		}
		op := schedule.NewOperator("promoteLearner", region.GetID(), region.GetRegionEpoch(), schedule.OpRegion, step)
		if c.addOperator(op) {
			return true
		}
	}

	if c.limiter.OperatorCount(schedule.OpLeader) < c.cluster.GetLeaderScheduleLimit() &&
		c.limiter.OperatorCount(schedule.OpRegion) < c.cluster.GetRegionScheduleLimit() &&
		c.limiter.OperatorCount(schedule.OpReplica) < c.cluster.GetReplicaScheduleLimit() {
		if op := c.namespaceChecker.Check(region); op != nil {
			if c.addOperator(op) {
				return true
			}
		}
	}

	if c.limiter.OperatorCount(schedule.OpReplica) < c.cluster.GetReplicaScheduleLimit() {
		if op := c.replicaChecker.Check(region); op != nil {
			if c.addOperator(op) {
				return true
			}
		}
	}
	if c.cluster.IsFeatureSupported(RegionMerge) && c.limiter.OperatorCount(schedule.OpMerge) < c.cluster.GetMergeScheduleLimit() {
		if op1, op2 := c.mergeChecker.Check(region); op1 != nil && op2 != nil {
			// make sure two operators can add successfully altogether
			if c.addOperator(op1, op2) {
				return true
			}
		}
	}
	return false
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

	k := 0
	scheduleCfg := c.cluster.opt.load()
	for _, schedulerCfg := range scheduleCfg.Schedulers {
		if schedulerCfg.Disable {
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
			log.Info("skip create scheduler", zap.String("scheduler-type", schedulerCfg.Type))
			continue
		}
		s, err := schedule.CreateScheduler(schedulerCfg.Type, c.limiter, schedulerCfg.Args...)
		if err != nil {
			log.Fatal("can not create scheduler", zap.String("scheduler-type", schedulerCfg.Type), zap.Error(err))
		}
		log.Info("create scheduler", zap.String("scheduler-name", s.GetName()))
		if err = c.addScheduler(s, schedulerCfg.Args...); err != nil {
			log.Error("can not add scheduler", zap.String("scheduler-name", s.GetName()), zap.Error(err))
		}

		// only record valid scheduler config
		if err == nil {
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
		}
	}

	// remove invalid scheduler config and persist
	scheduleCfg.Schedulers = scheduleCfg.Schedulers[:k]
	if err := c.cluster.opt.persist(c.cluster.kv); err != nil {
		log.Error("cannot persist schedule config", zap.Error(err))
	}

	c.wg.Add(2)
	// Starts to patrol regions.
	go c.patrolRegions()
	go c.drivePushOperator()
}

func (c *coordinator) stop() {
	c.cancel()
}

// Hack to retrive info from scheduler.
// TODO: remove it.
type hasHotStatus interface {
	GetHotReadStatus() *core.StoreHotRegionInfos
	GetHotWriteStatus() *core.StoreHotRegionInfos
}

func (c *coordinator) getHotWriteRegions() *core.StoreHotRegionInfos {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[hotRegionScheduleName]
	if !ok {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotWriteStatus()
	}
	return nil
}

func (c *coordinator) getHotReadRegions() *core.StoreHotRegionInfos {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[hotRegionScheduleName]
	if !ok {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotReadStatus()
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

func (c *coordinator) collectSchedulerMetrics() {
	c.RLock()
	defer c.RUnlock()
	for _, s := range c.schedulers {
		var allowScheduler float64
		if s.AllowSchedule() {
			allowScheduler = 1
		}
		schedulerStatusGauge.WithLabelValues(s.GetName(), "allow").Set(allowScheduler)
	}
}

func (c *coordinator) collectHotSpotMetrics() {
	c.RLock()
	defer c.RUnlock()
	// collect hot write region metrics
	s, ok := c.schedulers[hotRegionScheduleName]
	if !ok {
		return
	}
	stores := c.cluster.GetStores()
	status := s.Scheduler.(hasHotStatus).GetHotWriteStatus()
	for _, s := range stores {
		store := fmt.Sprintf("store_%d", s.GetId())
		stat, ok := status.AsPeer[s.GetId()]
		if ok {
			totalWriteBytes := float64(stat.TotalFlowBytes)
			hotWriteRegionCount := float64(stat.RegionsCount)

			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_peer").Set(totalWriteBytes)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_peer").Set(hotWriteRegionCount)
		} else {
			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_peer").Set(0)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_peer").Set(0)
		}

		stat, ok = status.AsLeader[s.GetId()]
		if ok {
			totalWriteBytes := float64(stat.TotalFlowBytes)
			hotWriteRegionCount := float64(stat.RegionsCount)

			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_leader").Set(totalWriteBytes)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_leader").Set(hotWriteRegionCount)
		} else {
			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_leader").Set(0)
		}
	}

	// collect hot read region metrics
	status = s.Scheduler.(hasHotStatus).GetHotReadStatus()
	for _, s := range stores {
		store := fmt.Sprintf("store_%d", s.GetId())
		stat, ok := status.AsLeader[s.GetId()]
		if ok {
			totalReadBytes := float64(stat.TotalFlowBytes)
			hotReadRegionCount := float64(stat.RegionsCount)

			hotSpotStatusGauge.WithLabelValues(store, "total_read_bytes_as_leader").Set(totalReadBytes)
			hotSpotStatusGauge.WithLabelValues(store, "hot_read_region_as_leader").Set(hotReadRegionCount)
		} else {
			hotSpotStatusGauge.WithLabelValues(store, "total_read_bytes_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(store, "hot_read_region_as_leader").Set(0)
		}
	}

}

func (c *coordinator) shouldRun() bool {
	return c.cluster.isPrepared()
}

func (c *coordinator) addScheduler(scheduler schedule.Scheduler, args ...string) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.GetName()]; ok {
		return errSchedulerExisted
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

	s, ok := c.schedulers[name]
	if !ok {
		return errSchedulerNotFound
	}

	s.Stop()
	schedulerStatusGauge.WithLabelValues(name, "allow").Set(0)
	delete(c.schedulers, name)

	return c.cluster.opt.RemoveSchedulerCfg(name)
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
			opInfluence := schedule.NewOpInfluence(c.getOperators(), c.cluster)
			if op := s.Schedule(c.cluster, opInfluence); op != nil {
				c.addOperator(op...)
			}

		case <-s.Ctx().Done():
			log.Info("scheduler has been stopped",
				zap.String("scheduler-name", s.GetName()),
				zap.Error(s.Ctx().Err()))
			return
		}
	}
}

func (c *coordinator) addOperatorLocked(op *schedule.Operator) bool {
	regionID := op.RegionID()

	log.Info("add operator", zap.Uint64("region-id", regionID), zap.Reflect("operator", op))

	// If there is an old operator, replace it. The priority should be checked
	// already.
	if old, ok := c.operators[regionID]; ok {
		log.Info("replace old operator", zap.Uint64("region-id", regionID), zap.Reflect("operator", old))
		operatorCounter.WithLabelValues(old.Desc(), "replaced").Inc()
		c.opRecords.Put(old, pdpb.OperatorStatus_REPLACE)
		c.removeOperatorLocked(old)
	}

	c.operators[regionID] = op
	c.limiter.UpdateCounts(c.operators)

	var step schedule.OperatorStep
	if region := c.cluster.GetRegion(op.RegionID()); region != nil {
		if step = op.Check(region); step != nil {
			c.sendScheduleCommand(region, step, DispatchFromCreate)
		}
	}

	heap.Push(&c.opNotifierQueue, &operatorWithTime{op, c.getNextPushOperatorTime(step, time.Now())})
	operatorCounter.WithLabelValues(op.Desc(), "create").Inc()
	return true
}

func (c *coordinator) addOperator(ops ...*schedule.Operator) bool {
	c.Lock()
	defer c.Unlock()

	for _, op := range ops {
		if !c.checkAddOperator(op) {
			operatorCounter.WithLabelValues(op.Desc(), "canceled").Inc()
			c.opRecords.Put(op, pdpb.OperatorStatus_CANCEL)
			return false
		}
	}
	for _, op := range ops {
		c.addOperatorLocked(op)
	}

	return true
}

func (c *coordinator) checkAddOperator(op *schedule.Operator) bool {
	region := c.cluster.GetRegion(op.RegionID())
	if region == nil {
		log.Debug("region not found, cancel add operator", zap.Uint64("region-id", op.RegionID()))
		return false
	}
	if region.GetRegionEpoch().GetVersion() != op.RegionEpoch().GetVersion() || region.GetRegionEpoch().GetConfVer() != op.RegionEpoch().GetConfVer() {
		log.Debug("region epoch not match, cancel add operator", zap.Uint64("region-id", op.RegionID()), zap.Reflect("old", region.GetRegionEpoch()), zap.Reflect("new", op.RegionEpoch()))
		return false
	}
	if old := c.operators[op.RegionID()]; old != nil && !isHigherPriorityOperator(op, old) {
		log.Debug("already have operator, cancel add operator", zap.Uint64("region-id", op.RegionID()), zap.Reflect("old", old))
		return false
	}
	return true
}

func isHigherPriorityOperator(new, old *schedule.Operator) bool {
	return new.GetPriorityLevel() < old.GetPriorityLevel()
}

func (c *coordinator) pushHistory(op *schedule.Operator) {
	c.Lock()
	defer c.Unlock()
	for _, h := range op.History() {
		c.histories.PushFront(h)
	}
}

func (c *coordinator) pruneHistory() {
	c.Lock()
	defer c.Unlock()
	p := c.histories.Back()
	for p != nil && time.Since(p.Value.(schedule.OperatorHistory).FinishTime) > historyKeepTime {
		prev := p.Prev()
		c.histories.Remove(p)
		p = prev
	}
}

func (c *coordinator) removeOperator(op *schedule.Operator) {
	c.Lock()
	defer c.Unlock()
	c.removeOperatorLocked(op)
}

func (c *coordinator) removeOperatorLocked(op *schedule.Operator) {
	regionID := op.RegionID()
	delete(c.operators, regionID)
	c.limiter.UpdateCounts(c.operators)
	operatorCounter.WithLabelValues(op.Desc(), "remove").Inc()
}

func (c *coordinator) getOperator(regionID uint64) *schedule.Operator {
	c.RLock()
	defer c.RUnlock()
	return c.operators[regionID]
}

func (c *coordinator) getOperators() []*schedule.Operator {
	c.RLock()
	defer c.RUnlock()

	operators := make([]*schedule.Operator, 0, len(c.operators))
	for _, op := range c.operators {
		operators = append(operators, op)
	}

	return operators
}

func (c *coordinator) getHistory(start time.Time) []schedule.OperatorHistory {
	c.RLock()
	defer c.RUnlock()
	histories := make([]schedule.OperatorHistory, 0, c.histories.Len())
	for p := c.histories.Front(); p != nil; p = p.Next() {
		history := p.Value.(schedule.OperatorHistory)
		if history.FinishTime.Before(start) {
			break
		}
		histories = append(histories, history)
	}
	return histories
}

func (c *coordinator) sendScheduleCommand(region *core.RegionInfo, step schedule.OperatorStep, source string) {
	log.Info("send schedule command", zap.Uint64("region-id", region.GetID()), zap.Stringer("step", step), zap.String("source", source))
	switch s := step.(type) {
	case schedule.TransferLeader:
		cmd := &pdpb.RegionHeartbeatResponse{
			TransferLeader: &pdpb.TransferLeader{
				Peer: region.GetStorePeer(s.ToStore),
			},
		}
		c.hbStreams.sendMsg(region, cmd)
	case schedule.AddPeer:
		if region.GetStorePeer(s.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_AddNode,
				Peer: &metapb.Peer{
					Id:      s.PeerID,
					StoreId: s.ToStore,
				},
			},
		}
		c.hbStreams.sendMsg(region, cmd)
	case schedule.AddLearner:
		if region.GetStorePeer(s.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_AddLearnerNode,
				Peer: &metapb.Peer{
					Id:        s.PeerID,
					StoreId:   s.ToStore,
					IsLearner: true,
				},
			},
		}
		c.hbStreams.sendMsg(region, cmd)
	case schedule.PromoteLearner:
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				// reuse AddNode type
				ChangeType: eraftpb.ConfChangeType_AddNode,
				Peer: &metapb.Peer{
					Id:      s.PeerID,
					StoreId: s.ToStore,
				},
			},
		}
		c.hbStreams.sendMsg(region, cmd)
	case schedule.RemovePeer:
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_RemoveNode,
				Peer:       region.GetStorePeer(s.FromStore),
			},
		}
		c.hbStreams.sendMsg(region, cmd)
	case schedule.MergeRegion:
		if s.IsPassive {
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			Merge: &pdpb.Merge{
				Target: s.ToRegion,
			},
		}
		c.hbStreams.sendMsg(region, cmd)
	case schedule.SplitRegion:
		cmd := &pdpb.RegionHeartbeatResponse{
			SplitRegion: &pdpb.SplitRegion{
				Policy: s.Policy,
			},
		}
		c.hbStreams.sendMsg(region, cmd)
	default:
		log.Error("unknown operator step", zap.Reflect("step", step))
	}
}

// GetOperatorStatus gets the operator and its status with the specify id.
func (c *coordinator) GetOperatorStatus(id uint64) *OperatorWithStatus {
	c.Lock()
	defer c.Unlock()
	if op, ok := c.operators[id]; ok {
		return &OperatorWithStatus{
			Op:     op,
			Status: pdpb.OperatorStatus_RUNNING,
		}
	}
	return c.opRecords.Get(id)
}

func (c *coordinator) getNextPushOperatorTime(step schedule.OperatorStep, now time.Time) time.Time {
	nextTime := slowNotifyInterval
	switch step.(type) {
	case schedule.TransferLeader, schedule.PromoteLearner:
		nextTime = fastNotifyInterval
	}
	return now.Add(nextTime)
}

// pollNeedDispatchRegion returns the region need to dispatch,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (c *coordinator) pollNeedDispatchRegion() (r *core.RegionInfo, next bool) {
	c.Lock()
	defer c.Unlock()
	if c.opNotifierQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&c.opNotifierQueue).(*operatorWithTime)
	regionID := item.op.RegionID()
	op, ok := c.operators[regionID]
	if !ok || op == nil {
		return nil, true
	}
	r = c.cluster.GetRegion(regionID)
	if r == nil {
		return nil, true
	}
	step := op.Check(r)
	if step == nil {
		return nil, true
	}
	now := time.Now()
	if now.Before(item.time) {
		heap.Push(&c.opNotifierQueue, item)
		return nil, false
	}

	// pushes with new notify time.
	item.time = c.getNextPushOperatorTime(step, now)
	heap.Push(&c.opNotifierQueue, item)
	return r, true
}

// PushOperators periodically pushes the unfinished operator to the executor(TiKV).
func (c *coordinator) PushOperators() {
	for {
		r, next := c.pollNeedDispatchRegion()
		if !next {
			break
		}
		if r == nil {
			continue
		}

		c.dispatch(r, DispatchFromNotifierQueue)
	}
}

type scheduleController struct {
	schedule.Scheduler
	cluster      *clusterInfo
	limiter      *schedule.Limiter
	classifier   namespace.Classifier
	nextInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
}

func newScheduleController(c *coordinator, s schedule.Scheduler) *scheduleController {
	ctx, cancel := context.WithCancel(c.ctx)
	return &scheduleController{
		Scheduler:    s,
		cluster:      c.cluster,
		limiter:      c.limiter,
		nextInterval: s.GetMinInterval(),
		classifier:   c.classifier,
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

func (s *scheduleController) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) []*schedule.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// If we have schedule, reset interval to the minimal interval.
		if op := scheduleByNamespace(cluster, s.classifier, s.Scheduler, opInfluence); op != nil {
			s.nextInterval = s.Scheduler.GetMinInterval()
			return op
		}
	}
	s.nextInterval = s.Scheduler.GetNextInterval(s.nextInterval)
	return nil
}

func (s *scheduleController) GetInterval() time.Duration {
	return s.nextInterval
}

func (s *scheduleController) AllowSchedule() bool {
	return s.Scheduler.IsScheduleAllowed(s.cluster)
}

// OperatorWithStatus records the operator and its status.
type OperatorWithStatus struct {
	Op     *schedule.Operator
	Status pdpb.OperatorStatus
}

// OperatorRecords remains the operator and its status for a while.
type OperatorRecords struct {
	ttl *cache.TTL
}

const operatorStatusRemainTime = 10 * time.Minute

// NewOperatorRecords returns a OperatorRecords.
func NewOperatorRecords() *OperatorRecords {
	return &OperatorRecords{
		ttl: cache.NewTTL(time.Minute, operatorStatusRemainTime),
	}
}

// Get gets the operator and its status.
func (o *OperatorRecords) Get(id uint64) *OperatorWithStatus {
	v, exist := o.ttl.Get(id)
	if !exist {
		return nil
	}
	return v.(*OperatorWithStatus)
}

// Put puts the operator and its status.
func (o *OperatorRecords) Put(op *schedule.Operator, status pdpb.OperatorStatus) {
	id := op.RegionID()
	record := &OperatorWithStatus{
		Op:     op,
		Status: status,
	}
	o.ttl.Put(id, record)
}
