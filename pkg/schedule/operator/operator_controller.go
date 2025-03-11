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

package operator

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/versioninfo"
)

// The source of dispatched region.
const (
	DispatchFromHeartBeat     = "heartbeat"
	DispatchFromNotifierQueue = "active push"
	DispatchFromCreate        = "create"
)

var (
	slowNotifyInterval = 5 * time.Second
	fastNotifyInterval = 2 * time.Second
	// StoreBalanceBaseTime represents the base time of balance rate.
	StoreBalanceBaseTime float64 = 60
	// FastOperatorFinishTime min finish time, if finish duration less than it, op will be pushed to fast operator queue
	FastOperatorFinishTime = 10 * time.Second
)

type opCounter struct {
	syncutil.RWMutex
	count map[OpKind]uint64
}

func (c *opCounter) inc(kind OpKind) {
	c.Lock()
	defer c.Unlock()
	c.count[kind]++
}

func (c *opCounter) dec(kind OpKind) {
	c.Lock()
	defer c.Unlock()
	if c.count[kind] > 0 {
		c.count[kind]--
	}
}

func (c *opCounter) getCountByKind(kind OpKind) uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.count[kind]
}

// Controller is used to limit the speed of scheduling.
type Controller struct {
	operators sync.Map
	ctx       context.Context
	config    config.SharedConfigProvider
	cluster   *core.BasicCluster
	hbStreams *hbstream.HeartbeatStreams

	// fast path, TTLUint64 is safe for concurrent.
	fastOperators *cache.TTLUint64

	// opNotifierQueue is a priority queue to notify the operator to be checked.
	// safe for concurrent.
	opNotifierQueue *concurrentHeapOpQueue

	// states
	records   *records // safe for concurrent
	wop       WaitingOperator
	wopStatus *waitingOperatorStatus
	counts    *opCounter
}

// NewController creates a Controller.
func NewController(ctx context.Context, cluster *core.BasicCluster, config config.SharedConfigProvider, hbStreams *hbstream.HeartbeatStreams) *Controller {
	return &Controller{
		ctx:             ctx,
		cluster:         cluster,
		config:          config,
		hbStreams:       hbStreams,
		fastOperators:   cache.NewIDTTL(ctx, time.Minute, FastOperatorFinishTime),
		opNotifierQueue: newConcurrentHeapOpQueue(),
		// states
		records:   newRecords(ctx),
		wop:       newRandBuckets(),
		wopStatus: newWaitingOperatorStatus(),
		counts:    &opCounter{count: make(map[OpKind]uint64)},
	}
}

// Ctx returns a context which will be canceled once RaftCluster is stopped.
// For now, it is only used to control the lifetime of TTL cache in schedulers.
func (oc *Controller) Ctx() context.Context {
	return oc.ctx
}

// GetCluster exports basic cluster to evict-scheduler for check store status.
func (oc *Controller) GetCluster() *core.BasicCluster {
	return oc.cluster
}

// GetHBStreams returns the heartbeat steams.
func (oc *Controller) GetHBStreams() *hbstream.HeartbeatStreams {
	return oc.hbStreams
}

// Dispatch is used to dispatch the operator of a region.
func (oc *Controller) Dispatch(region *core.RegionInfo, source string, recordOpStepWithTTL func(regionID uint64)) {
	// Check existed
	if op := oc.GetOperator(region.GetID()); op != nil {
		failpoint.Inject("concurrentRemoveOperator", func() {
			time.Sleep(500 * time.Millisecond)
		})
		// Update operator status:
		// The operator status should be STARTED.
		// Check will call CheckSuccess and CheckTimeout.
		step := op.Check(region)
		switch op.Status() {
		case STARTED:
			operatorCounter.WithLabelValues(op.Desc(), "check").Inc()
			if source == DispatchFromHeartBeat && oc.checkStaleOperator(op, step, region) {
				return
			}
			oc.SendScheduleCommand(region, step, source)
		case SUCCESS:
			if op.ContainNonWitnessStep() {
				recordOpStepWithTTL(op.RegionID())
			}
			if oc.RemoveOperator(op) {
				operatorCounter.WithLabelValues(op.Desc(), "promote-success").Inc()
				oc.PromoteWaitingOperator()
			}
			if time.Since(op.GetStartTime()) < FastOperatorFinishTime {
				log.Debug("op finish duration less than 10s", zap.Uint64("region-id", op.RegionID()))
				oc.pushFastOperator(op)
			}
		case TIMEOUT:
			if oc.RemoveOperator(op, Timeout) {
				operatorCounter.WithLabelValues(op.Desc(), "promote-timeout").Inc()
				oc.PromoteWaitingOperator()
			}
		default:
			if oc.removeOperatorWithoutBury(op) {
				// CREATED, EXPIRED must not appear.
				// CANCELED, REPLACED must remove before transition.
				log.Error("dispatching operator with unexpected status",
					zap.Uint64("region-id", op.RegionID()),
					zap.String("status", OpStatusToString(op.Status())),
					zap.Reflect("operator", op), errs.ZapError(errs.ErrUnexpectedOperatorStatus))
				failpoint.Inject("unexpectedOperator", func() {
					panic(op)
				})
				_ = op.Cancel(NotInRunningState)
				oc.buryOperator(op)
				operatorCounter.WithLabelValues(op.Desc(), "promote-unexpected").Inc()
				oc.PromoteWaitingOperator()
			}
		}
	}
}

func (oc *Controller) checkStaleOperator(op *Operator, step OpStep, region *core.RegionInfo) bool {
	err := step.CheckInProgress(oc.cluster, oc.config, region)
	if err != nil {
		log.Info("operator is stale", zap.Uint64("region-id", op.RegionID()), errs.ZapError(err))
		if oc.RemoveOperator(op, StaleStatus) {
			operatorCounter.WithLabelValues(op.Desc(), "promote-stale").Inc()
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
		log.Info("operator is stale",
			zap.Uint64("region-id", op.RegionID()),
			zap.Uint64("diff", changes),
			zap.Reflect("latest-epoch", region.GetRegionEpoch()))
		if oc.RemoveOperator(
			op,
			EpochNotMatch,
		) {
			operatorCounter.WithLabelValues(op.Desc(), "promote-stale").Inc()
			oc.PromoteWaitingOperator()
			return true
		}
	}

	return false
}

func getNextPushOperatorTime(step OpStep, now time.Time) time.Time {
	nextTime := slowNotifyInterval
	switch step.(type) {
	case TransferLeader, PromoteLearner, ChangePeerV2Enter, ChangePeerV2Leave:
		nextTime = fastNotifyInterval
	}
	return now.Add(nextTime)
}

// pollNeedDispatchRegion returns the region need to dispatch,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *Controller) pollNeedDispatchRegion() (r *core.RegionInfo, next bool) {
	if oc.opNotifierQueue.len() == 0 {
		return nil, false
	}
	item, _ := oc.opNotifierQueue.pop()
	regionID := item.op.RegionID()
	opi, ok := oc.operators.Load(regionID)
	if !ok || opi.(*Operator) == nil {
		return nil, true
	}
	op := opi.(*Operator)
	// Check the operator lightly. It cant't dispatch the op for some scenario.
	var reason CancelReasonType
	r, reason = oc.checkOperatorLightly(op)
	if len(reason) != 0 {
		_ = oc.removeOperatorInner(op)
		if op.Cancel(reason) {
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
		oc.opNotifierQueue.push(item)
		return nil, false
	}

	// pushes with new notify time.
	item.time = getNextPushOperatorTime(step, now)
	oc.opNotifierQueue.push(item)
	return r, true
}

// PushOperators periodically pushes the unfinished operator to the executor(TiKV).
func (oc *Controller) PushOperators(recordOpStepWithTTL func(regionID uint64)) {
	for {
		r, next := oc.pollNeedDispatchRegion()
		if !next {
			break
		}
		if r == nil {
			continue
		}

		oc.Dispatch(r, DispatchFromNotifierQueue, recordOpStepWithTTL)
	}
}

// AddWaitingOperator adds operators to waiting operators.
func (oc *Controller) AddWaitingOperator(ops ...*Operator) int {
	added := 0
	needPromoted := 0

	for i := 0; i < len(ops); i++ {
		op := ops[i]
		desc := op.Desc()
		isMerge := false
		if op.Kind()&OpMerge != 0 {
			if i+1 >= len(ops) {
				// should not be here forever
				log.Error("orphan merge operators found", zap.String("desc", desc), errs.ZapError(errs.ErrMergeOperator.FastGenByArgs("orphan operator found")))
				return added
			}
			if ops[i+1].Kind()&OpMerge == 0 {
				log.Error("merge operator should be paired", zap.String("desc",
					ops[i+1].Desc()), errs.ZapError(errs.ErrMergeOperator.FastGenByArgs("operator should be paired")))
				return added
			}
			isMerge = true
		}
		if pass, reason := oc.checkAddOperator(false, op); !pass {
			_ = op.Cancel(reason)
			oc.buryOperator(op)
			if isMerge {
				// Merge operation have two operators, cancel them all
				i++
				next := ops[i]
				_ = next.Cancel(reason)
				oc.buryOperator(next)
			}
			continue
		}

		if isMerge {
			// count two merge operators as one, so wopStatus.ops[desc] should
			// not be updated here
			// TODO: call checkAddOperator ...
			oc.wop.PutMergeOperators([]*Operator{op, ops[i+1]})
			i++
			added++
		} else {
			oc.wop.PutOperator(op)
		}
		operatorCounter.WithLabelValues(desc, "put").Inc()
		oc.wopStatus.incCount(desc)
		added++
		needPromoted++
	}
	operatorCounter.WithLabelValues(ops[0].Desc(), "promote-add").Add(float64(needPromoted))
	for range needPromoted {
		oc.PromoteWaitingOperator()
	}
	return added
}

// AddOperator adds operators to the running operators.
func (oc *Controller) AddOperator(ops ...*Operator) bool {
	// note: checkAddOperator uses false param for `isPromoting`.
	// This is used to keep check logic before fixing issue #4946,
	// but maybe user want to add operator when waiting queue is busy
	if oc.ExceedStoreLimit(ops...) {
		for _, op := range ops {
			operatorCounter.WithLabelValues(op.Desc(), "exceed-limit").Inc()
			_ = op.Cancel(ExceedStoreLimit)
			oc.buryOperator(op)
		}
		return false
	}
	if pass, reason := oc.checkAddOperator(false, ops...); !pass {
		for _, op := range ops {
			_ = op.Cancel(reason)
			oc.buryOperator(op)
		}
		return false
	}
	for _, op := range ops {
		if !oc.addOperatorInner(op) {
			return false
		}
	}
	return true
}

// PromoteWaitingOperator promotes operators from waiting operators.
func (oc *Controller) PromoteWaitingOperator() {
	var ops []*Operator
	for {
		// GetOperator returns one operator or two merge operators
		// need write lock
		ops = oc.wop.GetOperator()
		if ops == nil {
			return
		}
		operatorCounter.WithLabelValues(ops[0].Desc(), "get").Inc()
		if oc.ExceedStoreLimit(ops...) {
			for _, op := range ops {
				operatorCounter.WithLabelValues(op.Desc(), "exceed-limit").Inc()
				_ = op.Cancel(ExceedStoreLimit)
				oc.buryOperator(op)
			}
			oc.wopStatus.decCount(ops[0].Desc())
			continue
		}

		if pass, reason := oc.checkAddOperator(true, ops...); !pass {
			for _, op := range ops {
				operatorCounter.WithLabelValues(op.Desc(), "check-failed").Inc()
				_ = op.Cancel(reason)
				oc.buryOperator(op)
			}
			oc.wopStatus.decCount(ops[0].Desc())
			continue
		}
		oc.wopStatus.decCount(ops[0].Desc())
		break
	}

	for _, op := range ops {
		if !oc.addOperatorInner(op) {
			break
		}
	}
}

// checkAddOperator checks if the operator can be added.
// There are several situations that cannot be added:
// - There is no such region in the cluster
// - The epoch of the operator and the epoch of the corresponding region are no longer consistent.
// - The region already has a higher priority or same priority
// - Exceed the max number of waiting operators
// - At least one operator is expired.
func (oc *Controller) checkAddOperator(isPromoting bool, ops ...*Operator) (bool, CancelReasonType) {
	for _, op := range ops {
		region := oc.cluster.GetRegion(op.RegionID())
		if region == nil {
			log.Debug("region not found, cancel add operator",
				zap.Uint64("region-id", op.RegionID()))
			operatorCounter.WithLabelValues(op.Desc(), "not-found").Inc()
			return false, RegionNotFound
		}
		if region.GetRegionEpoch().GetVersion() != op.RegionEpoch().GetVersion() ||
			region.GetRegionEpoch().GetConfVer() != op.RegionEpoch().GetConfVer() {
			log.Debug("region epoch not match, cancel add operator",
				zap.Uint64("region-id", op.RegionID()),
				zap.Reflect("old", region.GetRegionEpoch()),
				zap.Reflect("new", op.RegionEpoch()))
			operatorCounter.WithLabelValues(op.Desc(), "epoch-not-match").Inc()
			return false, EpochNotMatch
		}
		if oldi, ok := oc.operators.Load(op.RegionID()); ok && oldi.(*Operator) != nil && !isHigherPriorityOperator(op, oldi.(*Operator)) {
			old := oldi.(*Operator)
			log.Debug("already have operator, cancel add operator",
				zap.Uint64("region-id", op.RegionID()),
				zap.Reflect("old", old))
			operatorCounter.WithLabelValues(op.Desc(), "already-have").Inc()
			return false, AlreadyExist
		}
		if op.Status() != CREATED {
			log.Error("trying to add operator with unexpected status",
				zap.Uint64("region-id", op.RegionID()),
				zap.String("status", OpStatusToString(op.Status())),
				zap.Reflect("operator", op), errs.ZapError(errs.ErrUnexpectedOperatorStatus))
			failpoint.Inject("unexpectedOperator", func() {
				panic(op)
			})
			operatorCounter.WithLabelValues(op.Desc(), "unexpected-status").Inc()
			return false, NotInCreateStatus
		}
		if !isPromoting && oc.wopStatus.getCount(op.Desc()) >= oc.config.GetSchedulerMaxWaitingOperator() {
			log.Debug("exceed max return false", zap.Uint64("waiting", oc.wopStatus.getCount(op.Desc())), zap.String("desc", op.Desc()), zap.Uint64("max", oc.config.GetSchedulerMaxWaitingOperator()))
			operatorCounter.WithLabelValues(op.Desc(), "exceed-max-waiting").Inc()
			return false, ExceedWaitLimit
		}

		if op.SchedulerKind() == OpAdmin || op.IsLeaveJointStateOperator() {
			continue
		}
	}
	var reason CancelReasonType
	for _, op := range ops {
		if op.CheckExpired() {
			reason = Expired
			operatorCounter.WithLabelValues(op.Desc(), "expired").Inc()
		}
	}
	return reason != Expired, reason
}

// checkOperatorLightly checks whether the ops can be dispatched in Controller::pollNeedDispatchRegion.
// The operators can't be dispatched for some scenarios, such as region disappeared, region changed ...
// `region` is the target region of `op`.
func (oc *Controller) checkOperatorLightly(op *Operator) (*core.RegionInfo, CancelReasonType) {
	region := oc.cluster.GetRegion(op.RegionID())
	if region == nil {
		operatorCounter.WithLabelValues(op.Desc(), "not-found").Inc()
		return nil, RegionNotFound
	}

	// It may be suitable for all kinds of operator but not merge-region.
	// But to be cautions, it only takes effect on merge-region currently.
	// If the version of epoch is changed, the region has been splitted or merged, and the key range has been changed.
	// The changing for conf_version of epoch doesn't modify the region key range, skip it.
	if (op.Kind()&OpMerge != 0) && region.GetRegionEpoch().GetVersion() > op.RegionEpoch().GetVersion() {
		operatorCounter.WithLabelValues(op.Desc(), "epoch-not-match").Inc()
		return nil, EpochNotMatch
	}
	return region, ""
}

func isHigherPriorityOperator(new, old *Operator) bool {
	return new.GetPriorityLevel() > old.GetPriorityLevel()
}

func (oc *Controller) addOperatorInner(op *Operator) bool {
	regionID := op.RegionID()
	log.Info("add operator",
		zap.Uint64("region-id", regionID),
		zap.Reflect("operator", op),
		zap.String("additional-info", op.LogAdditionalInfo()))

	// If there is an old operator, replace it. The priority should be checked
	// already.
	if oldi, ok := oc.operators.Load(regionID); ok {
		old := oldi.(*Operator)
		_ = oc.removeOperatorInner(old)
		_ = old.Replace()
		oc.buryOperator(old)
	}

	if !op.Start() {
		log.Error("adding operator with unexpected status",
			zap.Uint64("region-id", regionID),
			zap.String("status", OpStatusToString(op.Status())),
			zap.Reflect("operator", op), errs.ZapError(errs.ErrUnexpectedOperatorStatus))
		failpoint.Inject("unexpectedOperator", func() {
			panic(op)
		})
		operatorCounter.WithLabelValues(op.Desc(), "unexpected").Inc()
		return false
	}
	oc.operators.Store(regionID, op)
	oc.counts.inc(op.SchedulerKind())
	operatorCounter.WithLabelValues(op.Desc(), "start").Inc()
	operatorSizeHist.WithLabelValues(op.Desc()).Observe(float64(op.ApproximateSize))
	opInfluence := NewTotalOpInfluence([]*Operator{op}, oc.cluster)
	for storeID := range opInfluence.StoresInfluence {
		store := oc.cluster.GetStore(storeID)
		if store == nil {
			log.Info("missing store", zap.Uint64("store-id", storeID))
			continue
		}
		limit := store.GetStoreLimit()
		for n, v := range storelimit.TypeNameValue {
			stepCost := opInfluence.GetStoreInfluence(storeID).GetStepCost(v)
			if stepCost == 0 {
				continue
			}
			limit.Take(stepCost, v, op.GetPriorityLevel())
			storeLimitCostCounter.WithLabelValues(strconv.FormatUint(storeID, 10), n).Add(float64(stepCost) / float64(storelimit.RegionInfluence[v]))
		}
	}

	var step OpStep
	if region := oc.cluster.GetRegion(op.RegionID()); region != nil {
		if step = op.Check(region); step != nil {
			oc.SendScheduleCommand(region, step, DispatchFromCreate)
		}
	}

	oc.opNotifierQueue.push(&operatorWithTime{op: op, time: getNextPushOperatorTime(step, time.Now())})
	operatorCounter.WithLabelValues(op.Desc(), "create").Inc()
	for _, counter := range op.Counters {
		counter.Inc()
	}
	return true
}

func (oc *Controller) ack(op *Operator) {
	opInfluence := NewTotalOpInfluence([]*Operator{op}, oc.cluster)
	for storeID := range opInfluence.StoresInfluence {
		for _, v := range storelimit.TypeNameValue {
			limiter := oc.getOrCreateStoreLimit(storeID, v)
			if limiter == nil {
				return
			}
			cost := opInfluence.GetStoreInfluence(storeID).GetStepCost(v)
			limiter.Ack(cost, v)
		}
	}
}

// RemoveOperators removes all operators from the running operators.
func (oc *Controller) RemoveOperators(reasons ...CancelReasonType) {
	removed := oc.removeOperatorsInner()
	var cancelReason CancelReasonType
	if len(reasons) > 0 {
		cancelReason = reasons[0]
	}
	for _, op := range removed {
		if op.Cancel(cancelReason) {
			log.Info("operator removed",
				zap.Uint64("region-id", op.RegionID()),
				zap.Duration("takes", op.RunningTime()),
				zap.Reflect("operator", op))
		}
		oc.buryOperator(op)
	}
}

func (oc *Controller) removeOperatorsInner() []*Operator {
	var removed []*Operator
	oc.operators.Range(func(regionID, value any) bool {
		op := value.(*Operator)
		oc.operators.Delete(regionID)
		oc.counts.dec(op.SchedulerKind())
		operatorCounter.WithLabelValues(op.Desc(), "remove").Inc()
		oc.ack(op)
		if op.Kind()&OpMerge != 0 {
			oc.removeRelatedMergeOperator(op)
		}
		removed = append(removed, op)
		return true
	})
	return removed
}

// RemoveOperator removes an operator from the running operators.
func (oc *Controller) RemoveOperator(op *Operator, reasons ...CancelReasonType) bool {
	removed := oc.removeOperatorInner(op)
	var cancelReason CancelReasonType
	if len(reasons) > 0 {
		cancelReason = reasons[0]
	}
	if removed {
		if op.Cancel(cancelReason) {
			log.Info("operator removed",
				zap.Uint64("region-id", op.RegionID()),
				zap.Duration("takes", op.RunningTime()),
				zap.Reflect("operator", op))
		}
		oc.buryOperator(op)
	}
	return removed
}

func (oc *Controller) removeOperatorWithoutBury(op *Operator) bool {
	return oc.removeOperatorInner(op)
}

func (oc *Controller) removeOperatorInner(op *Operator) bool {
	regionID := op.RegionID()
	if cur, ok := oc.operators.Load(regionID); ok && cur.(*Operator) == op {
		oc.operators.Delete(regionID)
		oc.counts.dec(op.SchedulerKind())
		operatorCounter.WithLabelValues(op.Desc(), "remove").Inc()
		oc.ack(op)
		if op.Kind()&OpMerge != 0 {
			oc.removeRelatedMergeOperator(op)
		}
		return true
	}
	return false
}

func (oc *Controller) removeRelatedMergeOperator(op *Operator) {
	relatedID, _ := strconv.ParseUint(op.GetAdditionalInfo(string(RelatedMergeRegion)), 10, 64)
	relatedOpi, ok := oc.operators.Load(relatedID)
	if !ok {
		return
	}
	relatedOp := relatedOpi.(*Operator)
	if relatedOp != nil && relatedOp.Status() != CANCELED {
		log.Info("operator canceled related merge region",
			zap.Uint64("region-id", relatedOp.RegionID()),
			zap.String("additional-info", relatedOp.LogAdditionalInfo()),
			zap.Duration("takes", relatedOp.RunningTime()))
		oc.removeOperatorInner(relatedOp)
		relatedOp.Cancel(RelatedMergeRegion)
		oc.buryOperator(relatedOp)
	}
}

func (oc *Controller) buryOperator(op *Operator) {
	st := op.Status()

	if !IsEndStatus(st) {
		log.Error("burying operator with non-end status",
			zap.Uint64("region-id", op.RegionID()),
			zap.String("status", OpStatusToString(op.Status())),
			zap.Reflect("operator", op), errs.ZapError(errs.ErrUnexpectedOperatorStatus))
		failpoint.Inject("unexpectedOperator", func() {
			panic(op)
		})
		operatorCounter.WithLabelValues(op.Desc(), "unexpected").Inc()
		_ = op.Cancel(Unknown)
	}

	switch st {
	case SUCCESS:
		log.Info("operator finish",
			zap.Uint64("region-id", op.RegionID()),
			zap.Duration("takes", op.RunningTime()),
			zap.Reflect("operator", op),
			zap.String("additional-info", op.LogAdditionalInfo()))
		operatorCounter.WithLabelValues(op.Desc(), "finish").Inc()
		operatorDuration.WithLabelValues(op.Desc()).Observe(op.RunningTime().Seconds())
		for _, counter := range op.FinishedCounters {
			counter.Inc()
		}
	case REPLACED:
		log.Info("replace old operator",
			zap.Uint64("region-id", op.RegionID()),
			zap.Duration("takes", op.RunningTime()),
			zap.Reflect("operator", op),
			zap.String("additional-info", op.LogAdditionalInfo()))
		operatorCounter.WithLabelValues(op.Desc(), "replace").Inc()
	case EXPIRED:
		log.Info("operator expired",
			zap.Uint64("region-id", op.RegionID()),
			zap.Duration("lives", op.ElapsedTime()),
			zap.Reflect("operator", op))
		operatorCounter.WithLabelValues(op.Desc(), "expire").Inc()
	case TIMEOUT:
		log.Info("operator timeout",
			zap.Uint64("region-id", op.RegionID()),
			zap.Duration("takes", op.RunningTime()),
			zap.Reflect("operator", op),
			zap.String("additional-info", op.LogAdditionalInfo()))
		operatorCounter.WithLabelValues(op.Desc(), "timeout").Inc()
	case CANCELED:
		log.Info("operator canceled",
			zap.Uint64("region-id", op.RegionID()),
			zap.Duration("takes", op.RunningTime()),
			zap.Reflect("operator", op),
			zap.String("additional-info", op.LogAdditionalInfo()),
		)
		operatorCounter.WithLabelValues(op.Desc(), "cancel").Inc()
	}

	oc.records.Put(op)
}

// GetOperatorStatus gets the operator and its status with the specify id.
func (oc *Controller) GetOperatorStatus(id uint64) *OpWithStatus {
	if opi, ok := oc.operators.Load(id); ok && opi.(*Operator) != nil {
		op := opi.(*Operator)
		return NewOpWithStatus(op)
	}
	return oc.records.Get(id)
}

// GetOperator gets an operator from the given region.
func (oc *Controller) GetOperator(regionID uint64) *Operator {
	if v, ok := oc.operators.Load(regionID); ok {
		return v.(*Operator)
	}
	return nil
}

// GetOperators gets operators from the running operators.
func (oc *Controller) GetOperators() []*Operator {
	operators := make([]*Operator, 0, oc.opNotifierQueue.len())
	oc.operators.Range(
		func(_, value any) bool {
			operators = append(operators, value.(*Operator))
			return true
		})
	return operators
}

// GetWaitingOperators gets operators from the waiting operators.
func (oc *Controller) GetWaitingOperators() []*Operator {
	return oc.wop.ListOperator()
}

// GetOperatorsOfKind returns the running operators of the kind.
func (oc *Controller) GetOperatorsOfKind(mask OpKind) []*Operator {
	operators := make([]*Operator, 0, oc.opNotifierQueue.len())
	oc.operators.Range(
		func(_, value any) bool {
			op := value.(*Operator)
			if op.Kind()&mask != 0 {
				operators = append(operators, value.(*Operator))
			}
			return true
		})
	return operators
}

// SendScheduleCommand sends a command to the region.
func (oc *Controller) SendScheduleCommand(region *core.RegionInfo, step OpStep, source string) {
	log.Info("send schedule command",
		zap.Uint64("region-id", region.GetID()),
		zap.Stringer("step", step),
		zap.String("source", source))

	useConfChangeV2 := versioninfo.IsFeatureSupported(oc.config.GetClusterVersion(), versioninfo.ConfChangeV2)
	cmd := step.GetCmd(region, useConfChangeV2)
	if cmd == nil {
		return
	}
	oc.hbStreams.SendMsg(region, cmd)
}

func (oc *Controller) pushFastOperator(op *Operator) {
	oc.fastOperators.Put(op.RegionID(), op)
}

// GetRecords gets operators' records.
func (oc *Controller) GetRecords(from time.Time) []*OpRecord {
	records := make([]*OpRecord, 0, oc.records.ttl.Len())
	for _, id := range oc.records.ttl.GetAllID() {
		op := oc.records.Get(id)
		if op == nil || op.FinishTime.Before(from) {
			continue
		}
		records = append(records, op.Record(op.FinishTime))
	}
	return records
}

// GetHistory gets operators' history.
func (oc *Controller) GetHistory(start time.Time) []OpHistory {
	history := make([]OpHistory, 0, oc.records.ttl.Len())
	for _, id := range oc.records.ttl.GetAllID() {
		op := oc.records.Get(id)
		if op == nil || op.FinishTime.Before(start) {
			continue
		}
		history = append(history, op.History()...)
	}
	return history
}

// OperatorCount gets the count of operators filtered by kind.
// kind only has one OpKind.
func (oc *Controller) OperatorCount(kind OpKind) uint64 {
	return oc.counts.getCountByKind(kind)
}

// GetOpInfluence gets OpInfluence.
func (oc *Controller) GetOpInfluence(cluster *core.BasicCluster) OpInfluence {
	influence := OpInfluence{
		StoresInfluence: make(map[uint64]*StoreInfluence),
	}
	oc.operators.Range(
		func(_, value any) bool {
			op := value.(*Operator)
			if !op.CheckTimeout() && !op.CheckSuccess() {
				region := cluster.GetRegion(op.RegionID())
				if region != nil {
					op.UnfinishedInfluence(influence, region)
				}
			}
			return true
		})
	return influence
}

// GetFastOpInfluence get fast finish operator influence
func (oc *Controller) GetFastOpInfluence(cluster *core.BasicCluster, influence OpInfluence) {
	for _, id := range oc.fastOperators.GetAllID() {
		value, ok := oc.fastOperators.Get(id)
		if !ok {
			continue
		}
		op, ok := value.(*Operator)
		if !ok {
			continue
		}
		AddOpInfluence(op, influence, cluster)
	}
}

// CleanAllOpRecords removes all operators' records.
// It is used in tests only.
func (oc *Controller) CleanAllOpRecords() {
	oc.records.ttl.Clear()
}

// AddOpInfluence add operator influence for cluster
func AddOpInfluence(op *Operator, influence OpInfluence, cluster *core.BasicCluster) {
	region := cluster.GetRegion(op.RegionID())
	op.TotalInfluence(influence, region)
}

// NewTotalOpInfluence creates a OpInfluence.
func NewTotalOpInfluence(operators []*Operator, cluster *core.BasicCluster) OpInfluence {
	influence := *NewOpInfluence()

	for _, op := range operators {
		AddOpInfluence(op, influence, cluster)
	}

	return influence
}

// SetOperator is only used for test.
func (oc *Controller) SetOperator(op *Operator) {
	oc.operators.Store(op.RegionID(), op)
	oc.counts.inc(op.SchedulerKind())
}

// OpWithStatus records the operator and its status.
type OpWithStatus struct {
	*Operator
	Status     pdpb.OperatorStatus
	FinishTime time.Time
}

// NewOpWithStatus creates an OpWithStatus from an operator.
func NewOpWithStatus(op *Operator) *OpWithStatus {
	return &OpWithStatus{
		Operator:   op,
		Status:     OpStatusToPDPB(op.Status()),
		FinishTime: time.Now(),
	}
}

// MarshalJSON returns the status of operator as a JSON string
func (o *OpWithStatus) MarshalJSON() ([]byte, error) {
	return []byte(`"` + fmt.Sprintf("status: %s, operator: %s", o.Status.String(), o.Operator.String()) + `"`), nil
}

// records remains the operator and its status for a while.
type records struct {
	ttl *cache.TTLUint64
}

const operatorStatusRemainTime = 10 * time.Minute

// newRecords returns a records.
func newRecords(ctx context.Context) *records {
	return &records{
		ttl: cache.NewIDTTL(ctx, time.Minute, operatorStatusRemainTime),
	}
}

// Get gets the operator and its status.
func (o *records) Get(id uint64) *OpWithStatus {
	v, exist := o.ttl.Get(id)
	if !exist {
		return nil
	}
	return v.(*OpWithStatus)
}

// Put puts the operator and its status.
func (o *records) Put(op *Operator) {
	id := op.RegionID()
	record := NewOpWithStatus(op)
	o.ttl.Put(id, record)
}

// ExceedStoreLimit returns true if the store exceeds the cost limit after adding the  Otherwise, returns false.
func (oc *Controller) ExceedStoreLimit(ops ...*Operator) bool {
	// The operator with Urgent priority, like admin operators, should ignore the store limit check.
	var desc string
	if len(ops) != 0 {
		desc = ops[0].Desc()
		if ops[0].GetPriorityLevel() == constant.Urgent {
			return false
		}
	}
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
			if !limiter.Available(stepCost, v, ops[0].GetPriorityLevel()) {
				OperatorExceededStoreLimitCounter.WithLabelValues(desc).Inc()
				return true
			}
		}
	}
	return false
}

// getOrCreateStoreLimit is used to get or create the limit of a store.
func (oc *Controller) getOrCreateStoreLimit(storeID uint64, limitType storelimit.Type) storelimit.StoreLimit {
	ratePerSec := oc.config.GetStoreLimitByType(storeID, limitType) / StoreBalanceBaseTime
	s := oc.cluster.GetStore(storeID)
	if s == nil {
		log.Error("invalid store ID", zap.Uint64("store-id", storeID))
		return nil
	}
	// The other limits do not need to update by config exclude StoreRateLimit.
	if limit, ok := s.GetStoreLimit().(*storelimit.StoreRateLimit); ok && limit.Rate(limitType) != ratePerSec {
		oc.cluster.ResetStoreLimit(storeID, limitType, ratePerSec)
	}
	return s.GetStoreLimit()
}
