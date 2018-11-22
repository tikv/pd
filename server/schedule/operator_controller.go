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
	"container/list"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

var historyKeepTime = 5 * time.Minute

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
}

// NewOperatorController creates a OperatorController.
func NewOperatorController(cluster Cluster, hbStreams HeartbeatStreams) *OperatorController {
	return &OperatorController{
		cluster:   cluster,
		operators: make(map[uint64]*Operator),
		hbStreams: hbStreams,
		histories: list.New(),
		counts:    make(map[OperatorKind]uint64),
	}
}

// Dispatch is used to dispatch the operator of a region.
func (oc *OperatorController) Dispatch(region *core.RegionInfo) {
	// Check existed operator.
	if op := oc.GetOperator(region.GetID()); op != nil {
		timeout := op.IsTimeout()
		if step := op.Check(region); step != nil && !timeout {
			operatorCounter.WithLabelValues(op.Desc(), "check").Inc()
			oc.SendScheduleCommand(region, step)
			return
		}
		if op.IsFinish() {
			log.Infof("[region %v] operator finish: %s", region.GetID(), op)
			operatorCounter.WithLabelValues(op.Desc(), "finish").Inc()
			operatorDuration.WithLabelValues(op.Desc()).Observe(op.ElapsedTime().Seconds())
			oc.pushHistory(op)
			oc.RemoveOperator(op)
		} else if timeout {
			log.Infof("[region %v] operator timeout: %s", region.GetID(), op)
			oc.RemoveOperator(op)
		}
	}
}

// AddOperator adds operators to the running operators.
func (oc *OperatorController) AddOperator(ops ...*Operator) bool {
	oc.Lock()
	defer oc.Unlock()

	for _, op := range ops {
		if !oc.checkAddOperator(op) {
			operatorCounter.WithLabelValues(op.Desc(), "canceled").Inc()
			return false
		}
	}
	for _, op := range ops {
		oc.addOperatorLocked(op)
	}

	return true
}

func (oc *OperatorController) checkAddOperator(op *Operator) bool {
	region := oc.cluster.GetRegion(op.RegionID())
	if region == nil {
		log.Debugf("[region %v] region not found, cancel add operator", op.RegionID())
		return false
	}
	if region.GetRegionEpoch().GetVersion() != op.RegionEpoch().GetVersion() || region.GetRegionEpoch().GetConfVer() != op.RegionEpoch().GetConfVer() {
		log.Debugf("[region %v] region epoch not match, %v vs %v, cancel add operator", op.RegionID(), region.GetRegionEpoch(), op.RegionEpoch())
		return false
	}
	if old := oc.operators[op.RegionID()]; old != nil && !isHigherPriorityOperator(op, old) {
		log.Debugf("[region %v] already have operator %s, cancel add operator", op.RegionID(), old)
		return false
	}
	return true
}

func isHigherPriorityOperator(new, old *Operator) bool {
	return new.GetPriorityLevel() < old.GetPriorityLevel()
}

func (oc *OperatorController) addOperatorLocked(op *Operator) bool {
	regionID := op.RegionID()

	log.Infof("[region %v] add operator: %s", regionID, op)

	// If there is an old operator, replace it. The priority should be checked
	// already.
	if old, ok := oc.operators[regionID]; ok {
		log.Infof("[region %v] replace old operator: %s", regionID, old)
		operatorCounter.WithLabelValues(old.Desc(), "replaced").Inc()
		oc.removeOperatorLocked(old)
	}

	oc.operators[regionID] = op
	oc.updateCounts(oc.operators)

	if region := oc.cluster.GetRegion(op.RegionID()); region != nil {
		if step := op.Check(region); step != nil {
			oc.SendScheduleCommand(region, step)
		}
	}

	operatorCounter.WithLabelValues(op.Desc(), "create").Inc()
	return true
}

// RemoveOperator removes a operator from the running operators.
func (oc *OperatorController) RemoveOperator(op *Operator) {
	oc.Lock()
	defer oc.Unlock()
	oc.removeOperatorLocked(op)
}

func (oc *OperatorController) removeOperatorLocked(op *Operator) {
	regionID := op.RegionID()
	delete(oc.operators, regionID)
	oc.updateCounts(oc.operators)
	operatorCounter.WithLabelValues(op.Desc(), "remove").Inc()
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

// SendScheduleCommand sends a command to the region.
func (oc *OperatorController) SendScheduleCommand(region *core.RegionInfo, step OperatorStep) {
	log.Infof("[region %v] send schedule command: %s", region.GetID(), step)
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
		log.Errorf("unknown operatorStep: %v", step)
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
	operators := oc.GetOperators()
	for _, op := range operators {
		if !op.IsTimeout() && !op.IsFinish() {
			region := cluster.GetRegion(op.RegionID())
			if region != nil {
				res = append(res, op)
			}
		}
	}
	return NewOpInfluence(res, cluster)
}

// NewOpInfluence creates a OpInfluence.
func NewOpInfluence(operators []*Operator, cluster Cluster) OpInfluence {
	influence := OpInfluence{
		storesInfluence:  make(map[uint64]*StoreInfluence),
		regionsInfluence: make(map[uint64]*Operator),
	}

	for _, op := range operators {
		if !op.IsTimeout() && !op.IsFinish() {
			region := cluster.GetRegion(op.RegionID())
			if region != nil {
				op.Influence(influence, region)
			}
		}
		influence.regionsInfluence[op.RegionID()] = op
	}

	return influence
}

// OpInfluence records the influence of the cluster.
type OpInfluence struct {
	storesInfluence  map[uint64]*StoreInfluence
	regionsInfluence map[uint64]*Operator
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

// GetRegionsInfluence gets regionInfluence of specific region.
func (m OpInfluence) GetRegionsInfluence() map[uint64]*Operator {
	return m.regionsInfluence
}

// StoreInfluence records influences that pending operators will make.
type StoreInfluence struct {
	RegionSize  int64
	RegionCount int64
	LeaderSize  int64
	LeaderCount int64
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

// SetOperator is only used for test
func (oc *OperatorController) SetOperator(op *Operator) {
	oc.Lock()
	defer oc.Unlock()
	oc.operators[op.RegionID()] = op
}
