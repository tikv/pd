// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import (
	"container/heap"
	"container/list"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/juju/ratelimit"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"go.uber.org/zap"
)

// The source of dispatched region.
const (
	DispatchFromHeartBeat     = "heartbeat"
	DispatchFromNotifierQueue = "active push"
	DispatchFromCreate        = "create"
)

var (
	historyKeepTime    = 5 * time.Minute
	slowNotifyInterval = 5 * time.Second
	fastNotifyInterval = 2 * time.Second
	// PushOperatorTickInterval is the interval try to push the operator.
	PushOperatorTickInterval = 500 * time.Millisecond
	// StoreBalanceBaseTime represents the base time of balance rate.
	StoreBalanceBaseTime float64 = 60
)

// HeartbeatStreams is an interface of async region heartbeat.
type HeartbeatStreams interface {
	SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse)
}

// OperatorController is used to limit the speed of scheduling.
type OperatorController struct {
	sync.RWMutex
	cluster   Cluster
	operators map[uint64]*Operator
	hbStreams HeartbeatStreams
	histories *list.List
	counts    map[OperatorKind]uint64
	opRecords *OperatorRecords
	// TODO: Need to clean up the unused store ID.
	storesLimit     map[uint64]*ratelimit.Bucket
	wop             WaitingOperator
	wopStatus       *WaitingOperatorStatus
	opNotifierQueue operatorQueue
}

// NewOperatorController creates a OperatorController.
func NewOperatorController(cluster Cluster, hbStreams HeartbeatStreams) *OperatorController {
	return &OperatorController{
		cluster:         cluster,
		operators:       make(map[uint64]*Operator),
		hbStreams:       hbStreams,
		histories:       list.New(),
		counts:          make(map[OperatorKind]uint64),
		opRecords:       NewOperatorRecords(),
		storesLimit:     make(map[uint64]*ratelimit.Bucket),
		wop:             NewRandBuckets(),
		wopStatus:       NewWaitingOperatorStatus(),
		opNotifierQueue: make(operatorQueue, 0),
	}
}

// Dispatch is used to dispatch the operator of a region.
func (oc *OperatorController) Dispatch(region *core.RegionInfo, source string) {
	// Check existed operator.
	if op := oc.GetOperator(region.GetID()); op != nil {
		failpoint.Inject("concurrentRemoveOperator", func() {
			time.Sleep(500 * time.Millisecond)
		})
		timeout := op.IsTimeout()
		if step := op.Check(region); step != nil && !timeout {
			operatorCounter.WithLabelValues(op.Desc(), "check").Inc()
			oc.SendScheduleCommand(region, step, source)
			return
		}
		if op.IsFinish() && oc.RemoveOperator(op) {
			log.Info("operator finish", zap.Uint64("region-id", region.GetID()), zap.Reflect("operator", op))
			operatorCounter.WithLabelValues(op.Desc(), "finish").Inc()
			operatorDuration.WithLabelValues(op.Desc()).Observe(op.RunningTime().Seconds())
			oc.pushHistory(op)
			oc.opRecords.Put(op, pdpb.OperatorStatus_SUCCESS)
			oc.PromoteWaitingOperator()
		} else if timeout && oc.RemoveOperator(op) {
			log.Info("operator timeout", zap.Uint64("region-id", region.GetID()), zap.Reflect("operator", op))
			operatorCounter.WithLabelValues(op.Desc(), "timeout").Inc()
			oc.opRecords.Put(op, pdpb.OperatorStatus_TIMEOUT)
			oc.PromoteWaitingOperator()
		}
	}
}

func (oc *OperatorController) getNextPushOperatorTime(step OperatorStep, now time.Time) time.Time {
	nextTime := slowNotifyInterval
	switch step.(type) {
	case TransferLeader, PromoteLearner:
		nextTime = fastNotifyInterval
	}
	return now.Add(nextTime)
}

// pollNeedDispatchRegion returns the region need to dispatch,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *OperatorController) pollNeedDispatchRegion() (r *core.RegionInfo, next bool) {
	oc.Lock()
	defer oc.Unlock()
	if oc.opNotifierQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&oc.opNotifierQueue).(*operatorWithTime)
	regionID := item.op.regionID
	op, ok := oc.operators[regionID]
	if !ok || op == nil {
		return nil, true
	}
	r = oc.cluster.GetRegion(regionID)
	if r == nil {
		_ = oc.removeOperatorLocked(op)
		log.Debug("remove operator because region disappeared",
			zap.Uint64("region-id", op.RegionID()),
			zap.Stringer("operator", op))
		operatorCounter.WithLabelValues(op.Desc(), "disappear").Inc()
		oc.opRecords.Put(op, pdpb.OperatorStatus_CANCEL)
		return nil, true
	}
	step := op.Check(r)
	if step == nil {
		return r, true
	}
	now := time.Now()
	if now.Before(item.time) {
		heap.Push(&oc.opNotifierQueue, item)
		return nil, false
	}

	// pushes with new notify time.
	item.time = oc.getNextPushOperatorTime(step, now)
	heap.Push(&oc.opNotifierQueue, item)
	return r, true
}

// PushOperators periodically pushes the unfinished operator to the executor(TiKV).
func (oc *OperatorController) PushOperators() {
	for {
		r, next := oc.pollNeedDispatchRegion()
		if !next {
			break
		}
		if r == nil {
			continue
		}

		oc.Dispatch(r, DispatchFromNotifierQueue)
	}
}

// AddWaitingOperator adds operators to waiting operators.
func (oc *OperatorController) AddWaitingOperator(ops ...*Operator) bool {
	oc.Lock()

	if !oc.checkAddOperator(ops...) {
		for _, op := range ops {
			operatorWaitCounter.WithLabelValues(op.Desc(), "add_canceled").Inc()
			oc.opRecords.Put(op, pdpb.OperatorStatus_CANCEL)
		}
		oc.Unlock()
		return false
	}

	op := ops[0]
	desc := op.Desc()
	if oc.wopStatus.ops[desc] >= oc.cluster.GetSchedulerMaxWaitingOperator() {
		operatorWaitCounter.WithLabelValues(op.Desc(), "exceed_max").Inc()
		oc.Unlock()
		return false
	}
	oc.wop.PutOperator(op)
	operatorWaitCounter.WithLabelValues(op.Desc(), "put").Inc()
	// This step is especially for the merge operation.
	if len(ops) > 1 {
		oc.wop.PutOperator(ops[1])
	}
	oc.wopStatus.ops[desc]++
	oc.Unlock()
	oc.PromoteWaitingOperator()
	return true
}

// AddOperator adds operators to the running operators.
func (oc *OperatorController) AddOperator(ops ...*Operator) bool {
	oc.Lock()
	defer oc.Unlock()

	if oc.exceedStoreLimit(ops...) || !oc.checkAddOperator(ops...) {
		for _, op := range ops {
			operatorCounter.WithLabelValues(op.Desc(), "canceled").Inc()
			oc.opRecords.Put(op, pdpb.OperatorStatus_CANCEL)
		}
		return false
	}
	for _, op := range ops {
		oc.addOperatorLocked(op)
	}
	return true
}

// PromoteWaitingOperator promotes operators from waiting operators.
func (oc *OperatorController) PromoteWaitingOperator() {
	oc.Lock()
	defer oc.Unlock()
	var ops []*Operator
	for {
		ops = oc.wop.GetOperator()
		if ops == nil {
			return
		}
		operatorWaitCounter.WithLabelValues(ops[0].Desc(), "get").Inc()

		if oc.exceedStoreLimit(ops...) || !oc.checkAddOperator(ops...) {
			for _, op := range ops {
				operatorWaitCounter.WithLabelValues(op.Desc(), "promote_canceled").Inc()
				oc.opRecords.Put(op, pdpb.OperatorStatus_CANCEL)
			}
			oc.wopStatus.ops[ops[0].Desc()]--
			continue
		}
		oc.wopStatus.ops[ops[0].Desc()]--
		break
	}

	for _, op := range ops {
		oc.addOperatorLocked(op)
	}
}

// checkAddOperator checks if the operator can be added.
// There are several situations that cannot be added:
// - There is no such region in the cluster
// - The epoch of the operator and the epoch of the corresponding region are no longer consistent.
// - The region already has a higher priority or same priority operator.
func (oc *OperatorController) checkAddOperator(ops ...*Operator) bool {
	for _, op := range ops {
		region := oc.cluster.GetRegion(op.RegionID())
		if region == nil {
			log.Debug("region not found, cancel add operator", zap.Uint64("region-id", op.RegionID()))
			return false
		}
		if region.GetRegionEpoch().GetVersion() != op.RegionEpoch().GetVersion() || region.GetRegionEpoch().GetConfVer() != op.RegionEpoch().GetConfVer() {
			log.Debug("region epoch not match, cancel add operator", zap.Uint64("region-id", op.RegionID()), zap.Reflect("old", region.GetRegionEpoch()), zap.Reflect("new", op.RegionEpoch()))
			return false
		}
		if old := oc.operators[op.RegionID()]; old != nil && !isHigherPriorityOperator(op, old) {
			log.Debug("already have operator, cancel add operator", zap.Uint64("region-id", op.RegionID()), zap.Reflect("old", old))
			return false
		}
	}
	return true
}

func isHigherPriorityOperator(new, old *Operator) bool {
	return new.GetPriorityLevel() > old.GetPriorityLevel()
}

func (oc *OperatorController) addOperatorLocked(op *Operator) bool {
	regionID := op.RegionID()

	log.Info("add operator", zap.Uint64("region-id", regionID), zap.Reflect("operator", op))

	// If there is an old operator, replace it. The priority should be checked
	// already.
	if old, ok := oc.operators[regionID]; ok {
		_ = oc.removeOperatorLocked(old)
		log.Info("replace old operator", zap.Uint64("region-id", regionID), zap.Reflect("operator", old))
		operatorCounter.WithLabelValues(old.Desc(), "replaced").Inc()
		oc.opRecords.Put(old, pdpb.OperatorStatus_REPLACE)
	}

	oc.operators[regionID] = op
	op.startTime = time.Now()
	operatorCounter.WithLabelValues(op.Desc(), "start").Inc()
	operatorWaitDuration.WithLabelValues(op.Desc()).Observe(op.ElapsedTime().Seconds())
	opInfluence := NewTotalOpInfluence([]*Operator{op}, oc.cluster)
	for storeID := range opInfluence.storesInfluence {
		stepCost := opInfluence.GetStoreInfluence(storeID).StepCost
		if stepCost == 0 {
			continue
		}
		storeLimitGauge.WithLabelValues(strconv.FormatUint(storeID, 10), "take").Set(float64(stepCost) / float64(RegionInfluence))
		oc.storesLimit[storeID].Take(stepCost)
	}
	oc.updateCounts(oc.operators)

	var step OperatorStep
	if region := oc.cluster.GetRegion(op.RegionID()); region != nil {
		if step = op.Check(region); step != nil {
			oc.SendScheduleCommand(region, step, DispatchFromCreate)
		}
	}

	heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op, time: oc.getNextPushOperatorTime(step, time.Now())})
	operatorCounter.WithLabelValues(op.Desc(), "create").Inc()
	return true
}

// RemoveOperator removes a operator from the running operators.
func (oc *OperatorController) RemoveOperator(op *Operator) (found bool) {
	oc.Lock()
	defer oc.Unlock()
	return oc.removeOperatorLocked(op)
}

// GetOperatorStatus gets the operator and its status with the specify id.
func (oc *OperatorController) GetOperatorStatus(id uint64) *OperatorWithStatus {
	oc.Lock()
	defer oc.Unlock()
	if op, ok := oc.operators[id]; ok {
		return &OperatorWithStatus{
			Op:     op,
			Status: pdpb.OperatorStatus_RUNNING,
		}
	}
	return oc.opRecords.Get(id)
}

func (oc *OperatorController) removeOperatorLocked(op *Operator) bool {
	regionID := op.RegionID()
	if cur := oc.operators[regionID]; cur == op {
		delete(oc.operators, regionID)
		oc.updateCounts(oc.operators)
		operatorCounter.WithLabelValues(op.Desc(), "remove").Inc()
		return true
	}
	return false
}

// GetOperator gets a operator from the given region.
func (oc *OperatorController) GetOperator(regionID uint64) *Operator {
	oc.RLock()
	defer oc.RUnlock()
	return oc.operators[regionID]
}

// GetOperators gets operators from the running operators.
func (oc *OperatorController) GetOperators() []*Operator {
	oc.RLock()
	defer oc.RUnlock()

	operators := make([]*Operator, 0, len(oc.operators))
	for _, op := range oc.operators {
		operators = append(operators, op)
	}

	return operators
}

// GetWaitingOperators gets operators from the waiting operators.
func (oc *OperatorController) GetWaitingOperators() []*Operator {
	oc.RLock()
	defer oc.RUnlock()
	return oc.wop.ListOperator()
}

// SendScheduleCommand sends a command to the region.
func (oc *OperatorController) SendScheduleCommand(region *core.RegionInfo, step OperatorStep, source string) {
	log.Info("send schedule command", zap.Uint64("region-id", region.GetID()), zap.Stringer("step", step), zap.String("source", source))
	switch st := step.(type) {
	case TransferLeader:
		cmd := &pdpb.RegionHeartbeatResponse{
			TransferLeader: &pdpb.TransferLeader{
				Peer: region.GetStorePeer(st.ToStore),
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case AddPeer:
		if region.GetStorePeer(st.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_AddNode,
				Peer: &metapb.Peer{
					Id:      st.PeerID,
					StoreId: st.ToStore,
				},
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case AddLightPeer:
		if region.GetStorePeer(st.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_AddNode,
				Peer: &metapb.Peer{
					Id:      st.PeerID,
					StoreId: st.ToStore,
				},
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case AddLearner:
		if region.GetStorePeer(st.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_AddLearnerNode,
				Peer: &metapb.Peer{
					Id:        st.PeerID,
					StoreId:   st.ToStore,
					IsLearner: true,
				},
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case AddLightLearner:
		if region.GetStorePeer(st.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_AddLearnerNode,
				Peer: &metapb.Peer{
					Id:        st.PeerID,
					StoreId:   st.ToStore,
					IsLearner: true,
				},
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case PromoteLearner:
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				// reuse AddNode type
				ChangeType: eraftpb.ConfChangeType_AddNode,
				Peer: &metapb.Peer{
					Id:      st.PeerID,
					StoreId: st.ToStore,
				},
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case RemovePeer:
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_RemoveNode,
				Peer:       region.GetStorePeer(st.FromStore),
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case MergeRegion:
		if st.IsPassive {
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			Merge: &pdpb.Merge{
				Target: st.ToRegion,
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case SplitRegion:
		cmd := &pdpb.RegionHeartbeatResponse{
			SplitRegion: &pdpb.SplitRegion{
				Policy: st.Policy,
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	default:
		log.Error("unknown operator step", zap.Reflect("step", step))
	}
}

func (oc *OperatorController) pushHistory(op *Operator) {
	oc.Lock()
	defer oc.Unlock()
	for _, h := range op.History() {
		oc.histories.PushFront(h)
	}
}

// PruneHistory prunes a part of operators' history.
func (oc *OperatorController) PruneHistory() {
	oc.Lock()
	defer oc.Unlock()
	p := oc.histories.Back()
	for p != nil && time.Since(p.Value.(OperatorHistory).FinishTime) > historyKeepTime {
		prev := p.Prev()
		oc.histories.Remove(p)
		p = prev
	}
}

// GetHistory gets operators' history.
func (oc *OperatorController) GetHistory(start time.Time) []OperatorHistory {
	oc.RLock()
	defer oc.RUnlock()
	histories := make([]OperatorHistory, 0, oc.histories.Len())
	for p := oc.histories.Front(); p != nil; p = p.Next() {
		history := p.Value.(OperatorHistory)
		if history.FinishTime.Before(start) {
			break
		}
		histories = append(histories, history)
	}
	return histories
}

// updateCounts updates resource counts using current pending operators.
func (oc *OperatorController) updateCounts(operators map[uint64]*Operator) {
	for k := range oc.counts {
		delete(oc.counts, k)
	}
	for _, op := range operators {
		oc.counts[op.Kind()]++
	}
}

// OperatorCount gets the count of operators filtered by mask.
func (oc *OperatorController) OperatorCount(mask OperatorKind) uint64 {
	oc.RLock()
	defer oc.RUnlock()
	var total uint64
	for k, count := range oc.counts {
		if k&mask != 0 {
			total += count
		}
	}
	return total
}

// GetOpInfluence gets OpInfluence.
func (oc *OperatorController) GetOpInfluence(cluster Cluster) OpInfluence {
	oc.RLock()
	defer oc.RUnlock()

	var res []*Operator
	for _, op := range oc.operators {
		if !op.IsTimeout() && !op.IsFinish() {
			region := cluster.GetRegion(op.RegionID())
			if region != nil {
				res = append(res, op)
			}
		}
	}
	return NewUnfinishedOpInfluence(res, cluster)
}

// NewTotalOpInfluence creates a OpInfluence.
func NewTotalOpInfluence(operators []*Operator, cluster Cluster) OpInfluence {
	influence := OpInfluence{
		storesInfluence: make(map[uint64]*StoreInfluence),
	}

	for _, op := range operators {
		region := cluster.GetRegion(op.RegionID())
		if region != nil {
			op.TotalInfluence(influence, region)
		}
	}

	return influence
}

// NewUnfinishedOpInfluence creates a OpInfluence.
func NewUnfinishedOpInfluence(operators []*Operator, cluster Cluster) OpInfluence {
	influence := OpInfluence{
		storesInfluence: make(map[uint64]*StoreInfluence),
	}

	for _, op := range operators {
		if !op.IsTimeout() && !op.IsFinish() {
			region := cluster.GetRegion(op.RegionID())
			if region != nil {
				op.UnfinishedInfluence(influence, region)
			}
		}
	}

	return influence
}

// OpInfluence records the influence of the cluster.
type OpInfluence struct {
	storesInfluence map[uint64]*StoreInfluence
}

// GetStoreInfluence get storeInfluence of specific store.
func (m OpInfluence) GetStoreInfluence(id uint64) *StoreInfluence {
	storeInfluence, ok := m.storesInfluence[id]
	if !ok {
		storeInfluence = &StoreInfluence{}
		m.storesInfluence[id] = storeInfluence
	}
	return storeInfluence
}

// StoreInfluence records influences that pending operators will make.
type StoreInfluence struct {
	RegionSize  int64
	RegionCount int64
	LeaderSize  int64
	LeaderCount int64
	StepCost    int64
}

// ResourceSize returns delta size of leader/region by influence.
func (s StoreInfluence) ResourceSize(kind core.ResourceKind) int64 {
	switch kind {
	case core.LeaderKind:
		return s.LeaderSize
	case core.RegionKind:
		return s.RegionSize
	default:
		return 0
	}
}

// SetOperator is only used for test.
func (oc *OperatorController) SetOperator(op *Operator) {
	oc.Lock()
	defer oc.Unlock()
	oc.operators[op.RegionID()] = op
}

// OperatorWithStatus records the operator and its status.
type OperatorWithStatus struct {
	Op     *Operator
	Status pdpb.OperatorStatus
}

// MarshalJSON returns the status of operator as a JSON string
func (o *OperatorWithStatus) MarshalJSON() ([]byte, error) {
	return []byte(`"` + fmt.Sprintf("status: %s, operator: %s", o.Status.String(), o.Op.String()) + `"`), nil
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
func (o *OperatorRecords) Put(op *Operator, status pdpb.OperatorStatus) {
	id := op.regionID
	record := &OperatorWithStatus{
		Op:     op,
		Status: status,
	}
	o.ttl.Put(id, record)
}

// exceedStoreLimit returns true if the store exceeds the cost limit after adding the operator. Otherwise, returns false.
func (oc *OperatorController) exceedStoreLimit(ops ...*Operator) bool {
	opInfluence := NewTotalOpInfluence(ops, oc.cluster)
	for storeID := range opInfluence.storesInfluence {
		stepCost := opInfluence.GetStoreInfluence(storeID).StepCost
		if stepCost == 0 {
			continue
		}

		available := oc.getOrCreateStoreLimit(storeID).Available()
		storeLimitGauge.WithLabelValues(strconv.FormatUint(storeID, 10), "available").Set(float64(available) / float64(RegionInfluence))
		if available < stepCost {
			return true
		}
	}
	return false
}

// SetAllStoresLimit is used to set limit of all stores.
func (oc *OperatorController) SetAllStoresLimit(rate float64) {
	oc.Lock()
	defer oc.Unlock()
	for storeID := range oc.storesLimit {
		oc.newStoreLimit(storeID, rate)
	}
}

// SetStoreLimit is used to set the limit of a store.
func (oc *OperatorController) SetStoreLimit(storeID uint64, rate float64) {
	oc.Lock()
	defer oc.Unlock()
	oc.newStoreLimit(storeID, rate)
}

// newStoreLimit is used to create the limit of a store.
func (oc *OperatorController) newStoreLimit(storeID uint64, rate float64) {
	capacity := RegionInfluence
	if rate > 1 {
		capacity = int64(rate * float64(RegionInfluence))
	}
	rate *= float64(RegionInfluence)
	oc.storesLimit[storeID] = ratelimit.NewBucketWithRate(rate, capacity)
}

// getOrCreateStoreLimit is used to get or create the limit of a store.
func (oc *OperatorController) getOrCreateStoreLimit(storeID uint64) *ratelimit.Bucket {
	if oc.storesLimit[storeID] == nil {
		rate := oc.cluster.GetStoreBalanceRate() / StoreBalanceBaseTime
		oc.newStoreLimit(storeID, rate)
		oc.cluster.AttachOverloadStatus(storeID, func() bool {
			oc.RLock()
			defer oc.RUnlock()
			return oc.storesLimit[storeID].Available() < RegionInfluence
		})
	}
	return oc.storesLimit[storeID]
}

// GetAllStoresLimit is used to get limit of all stores.
func (oc *OperatorController) GetAllStoresLimit() map[uint64]float64 {
	oc.RLock()
	defer oc.RUnlock()
	ret := make(map[uint64]float64)
	for storeID, limit := range oc.storesLimit {
		store := oc.cluster.GetStore(storeID)
		if !store.IsTombstone() {
			ret[storeID] = limit.Rate() / float64(RegionInfluence)
		}
	}
	return ret
}

// RemoveStoreLimit removes the store limit for a given store ID.
func (oc *OperatorController) RemoveStoreLimit(storeID uint64) {
	oc.Lock()
	defer oc.Unlock()
	oc.cluster.AttachOverloadStatus(storeID, nil)
	delete(oc.storesLimit, storeID)
}
