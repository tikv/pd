// Copyright 2018 TiKV Project Authors.
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
	"container/heap"
	"container/list"
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/core/storelimit"
	"github.com/tikv/pd/server/schedule/hbstream"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
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
	// FastOperatorFinishTime min finish time, if finish duration less than it,op will be pushed to fast operator queue
	FastOperatorFinishTime = 10 * time.Second
)

// OperatorController is used to limit the speed of scheduling.
type OperatorController struct {
	sync.RWMutex
	ctx             context.Context
	cluster         opt.Cluster
	operators       map[uint64]*operator.Operator
	hbStreams       *hbstream.HeartbeatStreams
	fastOperators   *cache.TTLUint64
	histories       *list.List
	counts          map[operator.OpKind]uint64
	opRecords       *OperatorRecords
	wop             WaitingOperator
	wopStatus       *WaitingOperatorStatus
	opNotifierQueue operatorQueue
}

// NewOperatorController creates a OperatorController.
func NewOperatorController(ctx context.Context, cluster opt.Cluster, hbStreams *hbstream.HeartbeatStreams) *OperatorController {
	return &OperatorController{
		ctx:             ctx,
		cluster:         cluster,
		operators:       make(map[uint64]*operator.Operator),
		hbStreams:       hbStreams,
		histories:       list.New(),
		fastOperators:   cache.NewIDTTL(ctx, time.Minute, FastOperatorFinishTime),
		counts:          make(map[operator.OpKind]uint64),
		opRecords:       NewOperatorRecords(ctx),
		wop:             NewRandBuckets(),
		wopStatus:       NewWaitingOperatorStatus(),
		opNotifierQueue: make(operatorQueue, 0),
	}
}

// Ctx returns a context which will be canceled once RaftCluster is stopped.
// For now, it is only used to control the lifetime of TTL cache in schedulers.
func (oc *OperatorController) Ctx() context.Context {
	return oc.ctx
}

// GetCluster exports cluster to evict-scheduler for check store status.
func (oc *OperatorController) GetCluster() opt.Cluster {
	oc.RLock()
	defer oc.RUnlock()
	return oc.cluster
}

// Dispatch is used to dispatch the operator of a region.
func (oc *OperatorController) Dispatch(region *core.RegionInfo, source string) {
	// Check existed operator.
	if op := oc.GetOperator(region.GetID()); op != nil {
		failpoint.Inject("concurrentRemoveOperator", func() {
			time.Sleep(500 * time.Millisecond)
		})

		// Update operator status:
		// The operator status should be STARTED.
		// Check will call CheckSuccess and CheckTimeout.
		step := op.Check(region)

		switch op.Status() {
		case operator.STARTED:
			operatorCounter.WithLabelValues(op.Desc(), "check").Inc()
			if source == DispatchFromHeartBeat && oc.checkStaleOperator(op, step, region) {
				return
			}
			oc.SendScheduleCommand(region, step, source)
		case operator.SUCCESS:
			oc.pushHistory(op)
			if oc.RemoveOperator(op) {
				operatorWaitCounter.WithLabelValues(op.Desc(), "promote-success").Inc()
				oc.PromoteWaitingOperator()
			}
			if time.Since(op.GetStartTime()) < FastOperatorFinishTime {
				log.Debug("op finish duration less than 10s", zap.Uint64("region-id", op.RegionID()))
				oc.pushFastOperator(op)
			}
		case operator.TIMEOUT:
			if oc.RemoveOperator(op) {
				operatorCounter.WithLabelValues(op.Desc(), "promote-timeout").Inc()
				oc.PromoteWaitingOperator()
			}
		default:
			if oc.removeOperatorWithoutBury(op) {
				// CREATED, EXPIRED must not appear.
				// CANCELED, REPLACED must remove before transition.
				log.Error("dispatching operator with unexpected status",
					zap.Uint64("region-id", op.RegionID()),
					zap.String("status", operator.OpStatusToString(op.Status())),
					zap.Reflect("operator", op), errs.ZapError(errs.ErrUnexpectedOperatorStatus))
				operatorWaitCounter.WithLabelValues(op.Desc(), "unexpected").Inc()
				failpoint.Inject("unexpectedOperator", func() {
					panic(op)
				})
				_ = op.Cancel()
				oc.buryOperator(op)
				operatorWaitCounter.WithLabelValues(op.Desc(), "promote-unexpected").Inc()
				oc.PromoteWaitingOperator()
			}
		}
	}
}

func (oc *OperatorController) checkStaleOperator(op *operator.Operator, step operator.OpStep, region *core.RegionInfo) bool {
	err := step.CheckSafety(region)
	if err != nil {
		if oc.RemoveOperator(op, zap.String("reason", err.Error())) {
			operatorCounter.WithLabelValues(op.Desc(), "stale").Inc()
			operatorWaitCounter.WithLabelValues(op.Desc(), "promote-stale").Inc()
			oc.PromoteWaitingOperator()
			return true
		}
	}
	// When the "source" is heartbeat, the region may have a newer
	// confver than the region that the operator holds. In this case,
	// the operator is stale, and will not be executed even we would
	// have sent it to TiKV servers. Here, we just cancel it.
	origin := op.RegionEpoch()
	latest := region.GetRegionEpoch()
	changes := latest.GetConfVer() - origin.GetConfVer()
	if changes > op.ConfVerChanged(region) {
		if oc.RemoveOperator(
			op,
			zap.String("reason", "stale operator, confver does not meet expectations"),
			zap.Reflect("latest-epoch", region.GetRegionEpoch()),
			zap.Uint64("diff", changes),
		) {
			operatorCounter.WithLabelValues(op.Desc(), "stale").Inc()
			operatorWaitCounter.WithLabelValues(op.Desc(), "promote-stale").Inc()
			oc.PromoteWaitingOperator()
			return true
		}
	}

	return false
}

func (oc *OperatorController) getNextPushOperatorTime(step operator.OpStep, now time.Time) time.Time {
	nextTime := slowNotifyInterval
	switch step.(type) {
	case operator.TransferLeader, operator.PromoteLearner, operator.DemoteFollower, operator.ChangePeerV2Enter, operator.ChangePeerV2Leave:
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
	regionID := item.op.RegionID()
	op, ok := oc.operators[regionID]
	if !ok || op == nil {
		return nil, true
	}
	r = oc.cluster.GetRegion(regionID)
	if r == nil {
		_ = oc.removeOperatorLocked(op)
		if op.Cancel() {
			log.Warn("remove operator because region disappeared",
				zap.Uint64("region-id", op.RegionID()),
				zap.Stringer("operator", op))
			operatorCounter.WithLabelValues(op.Desc(), "disappear").Inc()
		}
		oc.buryOperator(op)
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
func (oc *OperatorController) AddWaitingOperator(ops ...*operator.Operator) int {
	oc.Lock()
	added := 0

	for i := 0; i < len(ops); i++ {
		op := ops[i]
		desc := op.Desc()
		isMerge := false
		if op.Kind()&operator.OpMerge != 0 {
			if i+1 >= len(ops) {
				// should not be here forever
				log.Error("orphan merge operators found", zap.String("desc", desc), errs.ZapError(errs.ErrMergeOperator.FastGenByArgs("orphan operator found")))
				oc.Unlock()
				return added
			}
			if ops[i+1].Kind()&operator.OpMerge == 0 {
				log.Error("merge operator should be paired", zap.String("desc",
					ops[i+1].Desc()), errs.ZapError(errs.ErrMergeOperator.FastGenByArgs("operator should be paired")))
				oc.Unlock()
				return added
			}
			isMerge = true
		}
		if !oc.checkAddOperator(op) {
			_ = op.Cancel()
			oc.buryOperator(op)
			if isMerge {
				// Merge operation have two operators, cancel them all
				next := ops[i+1]
				_ = next.Cancel()
				oc.buryOperator(next)
			}
			oc.Unlock()
			return added
		}
		oc.wop.PutOperator(op)
		if isMerge {
			// count two merge operators as one, so wopStatus.ops[desc] should
			// not be updated here
			i++
			added++
			oc.wop.PutOperator(ops[i])
		}
		operatorWaitCounter.WithLabelValues(desc, "put").Inc()
		oc.wopStatus.ops[desc]++
		added++
	}

	oc.Unlock()
	operatorWaitCounter.WithLabelValues(ops[0].Desc(), "promote-add").Inc()
	oc.PromoteWaitingOperator()
	return added
}

// AddOperator adds operators to the running operators.
func (oc *OperatorController) AddOperator(ops ...*operator.Operator) bool {
	oc.Lock()
	defer oc.Unlock()

	if oc.exceedStoreLimitLocked(ops...) || !oc.checkAddOperator(ops...) {
		for _, op := range ops {
			_ = op.Cancel()
			oc.buryOperator(op)
		}
		return false
	}
	for _, op := range ops {
		if !oc.addOperatorLocked(op) {
			return false
		}
	}
	return true
}

// PromoteWaitingOperator promotes operators from waiting operators.
func (oc *OperatorController) PromoteWaitingOperator() {
	oc.Lock()
	defer oc.Unlock()
	var ops []*operator.Operator
	for {
		// GetOperator returns one operator or two merge operators
		ops = oc.wop.GetOperator()
		if ops == nil {
			return
		}
		operatorWaitCounter.WithLabelValues(ops[0].Desc(), "get").Inc()

		if oc.exceedStoreLimitLocked(ops...) || !oc.checkAddOperator(ops...) {
			for _, op := range ops {
				operatorWaitCounter.WithLabelValues(op.Desc(), "promote-canceled").Inc()
				_ = op.Cancel()
				oc.buryOperator(op)
			}
			oc.wopStatus.ops[ops[0].Desc()]--
			continue
		}
		oc.wopStatus.ops[ops[0].Desc()]--
		break
	}

	for _, op := range ops {
		if !oc.addOperatorLocked(op) {
			break
		}
	}
}

// checkAddOperator checks if the operator can be added.
// There are several situations that cannot be added:
// - There is no such region in the cluster
// - The epoch of the operator and the epoch of the corresponding region are no longer consistent.
// - The region already has a higher priority or same priority operator.
// - Exceed the max number of waiting operators
// - At least one operator is expired.
func (oc *OperatorController) checkAddOperator(ops ...*operator.Operator) bool {
	for _, op := range ops {
		region := oc.cluster.GetRegion(op.RegionID())
		if region == nil {
			log.Debug("region not found, cancel add operator",
				zap.Uint64("region-id", op.RegionID()))
			operatorWaitCounter.WithLabelValues(op.Desc(), "not-found").Inc()
			return false
		}
		if region.GetRegionEpoch().GetVersion() != op.RegionEpoch().GetVersion() ||
			region.GetRegionEpoch().GetConfVer() != op.RegionEpoch().GetConfVer() {
			log.Debug("region epoch not match, cancel add operator",
				zap.Uint64("region-id", op.RegionID()),
				zap.Reflect("old", region.GetRegionEpoch()),
				zap.Reflect("new", op.RegionEpoch()))
			operatorWaitCounter.WithLabelValues(op.Desc(), "epoch-not-match").Inc()
			return false
		}
		if old := oc.operators[op.RegionID()]; old != nil && !isHigherPriorityOperator(op, old) {
			log.Debug("already have operator, cancel add operator",
				zap.Uint64("region-id", op.RegionID()),
				zap.Reflect("old", old))
			operatorWaitCounter.WithLabelValues(op.Desc(), "already-have").Inc()
			return false
		}
		if op.Status() != operator.CREATED {
			log.Error("trying to add operator with unexpected status",
				zap.Uint64("region-id", op.RegionID()),
				zap.String("status", operator.OpStatusToString(op.Status())),
				zap.Reflect("operator", op), errs.ZapError(errs.ErrUnexpectedOperatorStatus))
			failpoint.Inject("unexpectedOperator", func() {
				panic(op)
			})
			operatorWaitCounter.WithLabelValues(op.Desc(), "unexpected-status").Inc()
			return false
		}
		if oc.wopStatus.ops[op.Desc()] >= oc.cluster.GetOpts().GetSchedulerMaxWaitingOperator() {
			log.Debug("exceed max return false", zap.Uint64("waiting", oc.wopStatus.ops[op.Desc()]), zap.String("desc", op.Desc()), zap.Uint64("max", oc.cluster.GetOpts().GetSchedulerMaxWaitingOperator()))
			operatorWaitCounter.WithLabelValues(op.Desc(), "exceed-max").Inc()
			return false
		}
	}
	expired := false
	for _, op := range ops {
		if op.CheckExpired() {
			expired = true
			operatorWaitCounter.WithLabelValues(op.Desc(), "expired").Inc()
		}
	}
	return !expired
}

func isHigherPriorityOperator(new, old *operator.Operator) bool {
	return new.GetPriorityLevel() > old.GetPriorityLevel()
}

func (oc *OperatorController) addOperatorLocked(op *operator.Operator) bool {
	regionID := op.RegionID()

	log.Info("add operator",
		zap.Uint64("region-id", regionID),
		zap.Reflect("operator", op),
		zap.String("additional-info", op.GetAdditionalInfo()))

	// If there is an old operator, replace it. The priority should be checked
	// already.
	if old, ok := oc.operators[regionID]; ok {
		_ = oc.removeOperatorLocked(old)
		_ = old.Replace()
		oc.buryOperator(old)
	}

	if !op.Start() {
		log.Error("adding operator with unexpected status",
			zap.Uint64("region-id", regionID),
			zap.String("status", operator.OpStatusToString(op.Status())),
			zap.Reflect("operator", op), errs.ZapError(errs.ErrUnexpectedOperatorStatus))
		failpoint.Inject("unexpectedOperator", func() {
			panic(op)
		})
		operatorCounter.WithLabelValues(op.Desc(), "unexpected").Inc()
		return false
	}
	oc.operators[regionID] = op
	operatorCounter.WithLabelValues(op.Desc(), "start").Inc()
	operatorWaitDuration.WithLabelValues(op.Desc()).Observe(op.ElapsedTime().Seconds())
	opInfluence := NewTotalOpInfluence([]*operator.Operator{op}, oc.cluster)
	for storeID := range opInfluence.StoresInfluence {
		store := oc.cluster.GetStore(storeID)
		if store == nil {
			log.Error("invalid store ID", zap.Uint64("store-id", storeID))
			return false
		}
		for n, v := range storelimit.TypeNameValue {
			storeLimit := store.GetStoreLimit(v)
			if storeLimit == nil {
				continue
			}
			stepCost := opInfluence.GetStoreInfluence(storeID).GetStepCost(v)
			if stepCost == 0 {
				continue
			}
			storeLimit.Take(stepCost)
			storeLimitCostCounter.WithLabelValues(strconv.FormatUint(storeID, 10), n).Add(float64(stepCost) / float64(storelimit.RegionInfluence[v]))
		}
	}
	oc.updateCounts(oc.operators)

	var step operator.OpStep
	if region := oc.cluster.GetRegion(op.RegionID()); region != nil {
		if step = op.Check(region); step != nil {
			oc.SendScheduleCommand(region, step, DispatchFromCreate)
		}
	}

	heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op, time: oc.getNextPushOperatorTime(step, time.Now())})
	operatorCounter.WithLabelValues(op.Desc(), "create").Inc()
	for _, counter := range op.Counters {
		counter.Inc()
	}
	return true
}

// RemoveOperator removes a operator from the running operators.
func (oc *OperatorController) RemoveOperator(op *operator.Operator, extraFields ...zap.Field) bool {
	oc.Lock()
	removed := oc.removeOperatorLocked(op)
	oc.Unlock()
	if removed {
		if op.Cancel() {
			log.Info("operator removed",
				zap.Uint64("region-id", op.RegionID()),
				zap.Duration("takes", op.RunningTime()),
				zap.Reflect("operator", op))
		}
		oc.buryOperator(op, extraFields...)
	}
	return removed
}

func (oc *OperatorController) removeOperatorWithoutBury(op *operator.Operator) bool {
	oc.Lock()
	defer oc.Unlock()
	return oc.removeOperatorLocked(op)
}

func (oc *OperatorController) removeOperatorLocked(op *operator.Operator) bool {
	regionID := op.RegionID()
	if cur := oc.operators[regionID]; cur == op {
		delete(oc.operators, regionID)
		oc.updateCounts(oc.operators)
		operatorCounter.WithLabelValues(op.Desc(), "remove").Inc()
		return true
	}
	return false
}

func (oc *OperatorController) buryOperator(op *operator.Operator, extraFields ...zap.Field) {
	st := op.Status()

	if !operator.IsEndStatus(st) {
		log.Error("burying operator with non-end status",
			zap.Uint64("region-id", op.RegionID()),
			zap.String("status", operator.OpStatusToString(op.Status())),
			zap.Reflect("operator", op), errs.ZapError(errs.ErrUnexpectedOperatorStatus))
		failpoint.Inject("unexpectedOperator", func() {
			panic(op)
		})
		operatorCounter.WithLabelValues(op.Desc(), "unexpected").Inc()
		_ = op.Cancel()
	}

	switch st {
	case operator.SUCCESS:
		log.Info("operator finish",
			zap.Uint64("region-id", op.RegionID()),
			zap.Duration("takes", op.RunningTime()),
			zap.Reflect("operator", op),
			zap.String("additional-info", op.GetAdditionalInfo()))
		operatorCounter.WithLabelValues(op.Desc(), "finish").Inc()
		operatorDuration.WithLabelValues(op.Desc()).Observe(op.RunningTime().Seconds())
		for _, counter := range op.FinishedCounters {
			counter.Inc()
		}
	case operator.REPLACED:
		log.Info("replace old operator",
			zap.Uint64("region-id", op.RegionID()),
			zap.Duration("takes", op.RunningTime()),
			zap.Reflect("operator", op))
		operatorCounter.WithLabelValues(op.Desc(), "replace").Inc()
	case operator.EXPIRED:
		log.Info("operator expired",
			zap.Uint64("region-id", op.RegionID()),
			zap.Duration("lives", op.ElapsedTime()),
			zap.Reflect("operator", op))
		operatorCounter.WithLabelValues(op.Desc(), "expire").Inc()
	case operator.TIMEOUT:
		log.Info("operator timeout",
			zap.Uint64("region-id", op.RegionID()),
			zap.Duration("takes", op.RunningTime()),
			zap.Reflect("operator", op))
		operatorCounter.WithLabelValues(op.Desc(), "timeout").Inc()
	case operator.CANCELED:
		fields := []zap.Field{
			zap.Uint64("region-id", op.RegionID()),
			zap.Duration("takes", op.RunningTime()),
			zap.Reflect("operator", op),
		}
		fields = append(fields, extraFields...)
		log.Info("operator canceled",
			fields...,
		)
		operatorCounter.WithLabelValues(op.Desc(), "cancel").Inc()
	}

	oc.opRecords.Put(op)
}

// GetOperatorStatus gets the operator and its status with the specify id.
func (oc *OperatorController) GetOperatorStatus(id uint64) *OperatorWithStatus {
	oc.Lock()
	defer oc.Unlock()
	if op, ok := oc.operators[id]; ok {
		return NewOperatorWithStatus(op)
	}
	return oc.opRecords.Get(id)
}

// GetOperator gets a operator from the given region.
func (oc *OperatorController) GetOperator(regionID uint64) *operator.Operator {
	oc.RLock()
	defer oc.RUnlock()
	return oc.operators[regionID]
}

// GetOperators gets operators from the running operators.
func (oc *OperatorController) GetOperators() []*operator.Operator {
	oc.RLock()
	defer oc.RUnlock()

	operators := make([]*operator.Operator, 0, len(oc.operators))
	for _, op := range oc.operators {
		operators = append(operators, op)
	}

	return operators
}

// GetWaitingOperators gets operators from the waiting operators.
func (oc *OperatorController) GetWaitingOperators() []*operator.Operator {
	oc.RLock()
	defer oc.RUnlock()
	return oc.wop.ListOperator()
}

// SendScheduleCommand sends a command to the region.
func (oc *OperatorController) SendScheduleCommand(region *core.RegionInfo, step operator.OpStep, source string) {
	log.Info("send schedule command",
		zap.Uint64("region-id", region.GetID()),
		zap.Stringer("step", step),
		zap.String("source", source))

	var cmd *pdpb.RegionHeartbeatResponse
	switch st := step.(type) {
	case operator.TransferLeader:
		cmd = &pdpb.RegionHeartbeatResponse{
			TransferLeader: &pdpb.TransferLeader{
				Peer: region.GetStorePeer(st.ToStore),
			},
		}
	case operator.AddPeer:
		if region.GetStorePeer(st.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd = addNode(st.PeerID, st.ToStore)
	case operator.AddLearner:
		if region.GetStorePeer(st.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd = addLearnerNode(st.PeerID, st.ToStore)
	case operator.PromoteLearner:
		cmd = addNode(st.PeerID, st.ToStore)
	case operator.DemoteFollower:
		cmd = addLearnerNode(st.PeerID, st.ToStore)
	case operator.RemovePeer:
		cmd = &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_RemoveNode,
				Peer:       region.GetStorePeer(st.FromStore),
			},
		}
	case operator.MergeRegion:
		if st.IsPassive {
			return
		}
		cmd = &pdpb.RegionHeartbeatResponse{
			Merge: &pdpb.Merge{
				Target: st.ToRegion,
			},
		}
	case operator.SplitRegion:
		cmd = &pdpb.RegionHeartbeatResponse{
			SplitRegion: &pdpb.SplitRegion{
				Policy: st.Policy,
				Keys:   st.SplitKeys,
			},
		}
	case operator.ChangePeerV2Enter:
		cmd = &pdpb.RegionHeartbeatResponse{
			ChangePeerV2: st.GetRequest(),
		}
	case operator.ChangePeerV2Leave:
		cmd = &pdpb.RegionHeartbeatResponse{
			ChangePeerV2: &pdpb.ChangePeerV2{},
		}
	default:
		log.Error("unknown operator step", zap.Reflect("step", step), errs.ZapError(errs.ErrUnknownOperatorStep))
		return
	}
	oc.hbStreams.SendMsg(region, cmd)
}

func addNode(id, storeID uint64) *pdpb.RegionHeartbeatResponse {
	return &pdpb.RegionHeartbeatResponse{
		ChangePeer: &pdpb.ChangePeer{
			ChangeType: eraftpb.ConfChangeType_AddNode,
			Peer: &metapb.Peer{
				Id:      id,
				StoreId: storeID,
				Role:    metapb.PeerRole_Voter,
			},
		},
	}
}

func addLearnerNode(id, storeID uint64) *pdpb.RegionHeartbeatResponse {
	return &pdpb.RegionHeartbeatResponse{
		ChangePeer: &pdpb.ChangePeer{
			ChangeType: eraftpb.ConfChangeType_AddLearnerNode,
			Peer: &metapb.Peer{
				Id:      id,
				StoreId: storeID,
				Role:    metapb.PeerRole_Learner,
			},
		},
	}
}

func (oc *OperatorController) pushHistory(op *operator.Operator) {
	oc.Lock()
	defer oc.Unlock()
	for _, h := range op.History() {
		oc.histories.PushFront(h)
	}
}

func (oc *OperatorController) pushFastOperator(op *operator.Operator) {
	oc.fastOperators.Put(op.RegionID(), op)
}

// PruneHistory prunes a part of operators' history.
func (oc *OperatorController) PruneHistory() {
	oc.Lock()
	defer oc.Unlock()
	p := oc.histories.Back()
	for p != nil && time.Since(p.Value.(operator.OpHistory).FinishTime) > historyKeepTime {
		prev := p.Prev()
		oc.histories.Remove(p)
		p = prev
	}
}

// GetHistory gets operators' history.
func (oc *OperatorController) GetHistory(start time.Time) []operator.OpHistory {
	oc.RLock()
	defer oc.RUnlock()
	histories := make([]operator.OpHistory, 0, oc.histories.Len())
	for p := oc.histories.Front(); p != nil; p = p.Next() {
		history := p.Value.(operator.OpHistory)
		if history.FinishTime.Before(start) {
			break
		}
		histories = append(histories, history)
	}
	return histories
}

// updateCounts updates resource counts using current pending operators.
func (oc *OperatorController) updateCounts(operators map[uint64]*operator.Operator) {
	for k := range oc.counts {
		delete(oc.counts, k)
	}
	for _, op := range operators {
		oc.counts[op.SchedulerKind()]++
	}
}

// OperatorCount gets the count of operators filtered by kind.
// kind only has one OpKind.
func (oc *OperatorController) OperatorCount(kind operator.OpKind) uint64 {
	oc.RLock()
	defer oc.RUnlock()
	return oc.counts[kind]
}

// GetOpInfluence gets OpInfluence.
func (oc *OperatorController) GetOpInfluence(cluster opt.Cluster) operator.OpInfluence {
	influence := operator.OpInfluence{
		StoresInfluence: make(map[uint64]*operator.StoreInfluence),
	}
	oc.RLock()
	defer oc.RUnlock()
	for _, op := range oc.operators {
		if !op.CheckTimeout() && !op.CheckSuccess() {
			region := cluster.GetRegion(op.RegionID())
			if region != nil {
				op.UnfinishedInfluence(influence, region)
			}
		}
	}
	return influence
}

// GetFastOpInfluence get fast finish operator influence
func (oc *OperatorController) GetFastOpInfluence(cluster opt.Cluster, influence operator.OpInfluence) {
	for _, id := range oc.fastOperators.GetAllID() {
		value, ok := oc.fastOperators.Get(id)
		if !ok {
			continue
		}
		op, ok := value.(*operator.Operator)
		if !ok {
			continue
		}
		region := cluster.GetRegion(op.RegionID())
		if region != nil {
			log.Debug("op influence less than 10s", zap.Uint64("region-id", op.RegionID()))
			op.TotalInfluence(influence, region)
		}
	}
}

// NewTotalOpInfluence creates a OpInfluence.
func NewTotalOpInfluence(operators []*operator.Operator, cluster opt.Cluster) operator.OpInfluence {
	influence := operator.OpInfluence{
		StoresInfluence: make(map[uint64]*operator.StoreInfluence),
	}

	for _, op := range operators {
		region := cluster.GetRegion(op.RegionID())
		if region != nil {
			op.TotalInfluence(influence, region)
		}
	}

	return influence
}

// SetOperator is only used for test.
func (oc *OperatorController) SetOperator(op *operator.Operator) {
	oc.Lock()
	defer oc.Unlock()
	oc.operators[op.RegionID()] = op
	oc.updateCounts(oc.operators)
}

// OperatorWithStatus records the operator and its status.
type OperatorWithStatus struct {
	Op     *operator.Operator
	Status pdpb.OperatorStatus
}

// NewOperatorWithStatus creates an OperatorStatus from an operator.
func NewOperatorWithStatus(op *operator.Operator) *OperatorWithStatus {
	return &OperatorWithStatus{
		Op:     op,
		Status: operator.OpStatusToPDPB(op.Status()),
	}
}

// MarshalJSON returns the status of operator as a JSON string
func (o *OperatorWithStatus) MarshalJSON() ([]byte, error) {
	return []byte(`"` + fmt.Sprintf("status: %s, operator: %s", o.Status.String(), o.Op.String()) + `"`), nil
}

// OperatorRecords remains the operator and its status for a while.
type OperatorRecords struct {
	ttl *cache.TTLUint64
}

const operatorStatusRemainTime = 10 * time.Minute

// NewOperatorRecords returns a OperatorRecords.
func NewOperatorRecords(ctx context.Context) *OperatorRecords {
	return &OperatorRecords{
		ttl: cache.NewIDTTL(ctx, time.Minute, operatorStatusRemainTime),
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
func (o *OperatorRecords) Put(op *operator.Operator) {
	id := op.RegionID()
	record := NewOperatorWithStatus(op)
	o.ttl.Put(id, record)
}

// ExceedStoreLimit returns true if the store exceeds the cost limit after adding the operator. Otherwise, returns false.
func (oc *OperatorController) ExceedStoreLimit(ops ...*operator.Operator) bool {
	oc.Lock()
	defer oc.Unlock()
	return oc.exceedStoreLimitLocked(ops...)
}

// exceedStoreLimitLocked returns true if the store exceeds the cost limit after adding the operator. Otherwise, returns false.
func (oc *OperatorController) exceedStoreLimitLocked(ops ...*operator.Operator) bool {
	opInfluence := NewTotalOpInfluence(ops, oc.cluster)
	for storeID := range opInfluence.StoresInfluence {
		for _, v := range storelimit.TypeNameValue {
			stepCost := opInfluence.GetStoreInfluence(storeID).GetStepCost(v)
			if stepCost == 0 {
				continue
			}
			limiter := oc.getOrCreateStoreLimit(storeID, v)
			if limiter == nil {
				return false
			}
			if limiter.Available() < stepCost {
				return true
			}
		}
	}
	return false
}

// getOrCreateStoreLimit is used to get or create the limit of a store.
func (oc *OperatorController) getOrCreateStoreLimit(storeID uint64, limitType storelimit.Type) *storelimit.StoreLimit {
	ratePerSec := oc.cluster.GetOpts().GetStoreLimitByType(storeID, limitType) / StoreBalanceBaseTime
	s := oc.cluster.GetStore(storeID)
	if s == nil {
		log.Error("invalid store ID", zap.Uint64("store-id", storeID))
		return nil
	}
	if s.GetStoreLimit(limitType) == nil {
		oc.cluster.GetBasicCluster().ResetStoreLimit(storeID, limitType, ratePerSec)
	}
	if ratePerSec != s.GetStoreLimit(limitType).Rate() {
		oc.cluster.GetBasicCluster().ResetStoreLimit(storeID, limitType, ratePerSec)
	}
	return s.GetStoreLimit(limitType)
}
