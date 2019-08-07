// Copyright 2016 PingCAP, Inc.
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

package operator

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/statistics"
	"go.uber.org/zap"
)

const (
	// LeaderOperatorWaitTime is the duration that when a leader operator lives
	// longer than it, the operator will be considered timeout.
	LeaderOperatorWaitTime = 10 * time.Second
	// RegionOperatorWaitTime is the duration that when a region operator lives
	// longer than it, the operator will be considered timeout.
	RegionOperatorWaitTime = 10 * time.Minute
	// RegionInfluence represents the influence of a operator step, which is used by ratelimit.
	RegionInfluence int64 = 1000
	// smallRegionInfluence represents the influence of a operator step
	// when the region size is smaller than smallRegionThreshold, which is used by ratelimit.
	smallRegionInfluence int64 = 200
	// smallRegionThreshold is used to represent a region which can be regarded as a small region once the size is small than it.
	smallRegionThreshold int64 = 20
)

// Cluster provides an overview of a cluster's regions distribution.
type Cluster interface {
	core.RegionSetInformer
	core.StoreSetInformer
	core.StoreSetController

	statistics.RegionStatInformer
	opt.Options

	// TODO: it should be removed. Schedulers don't need to know anything
	// about peers.
	AllocPeer(storeID uint64) (*metapb.Peer, error)
}

// OpInfluence records the influence of the cluster.
type OpInfluence struct {
	StoresInfluence map[uint64]*StoreInfluence
}

// GetStoreInfluence get storeInfluence of specific store.
func (m OpInfluence) GetStoreInfluence(id uint64) *StoreInfluence {
	storeInfluence, ok := m.StoresInfluence[id]
	if !ok {
		storeInfluence = &StoreInfluence{}
		m.StoresInfluence[id] = storeInfluence
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

// OpStep describes the basic scheduling steps that can not be subdivided.
type OpStep interface {
	fmt.Stringer
	IsFinish(region *core.RegionInfo) bool
	Influence(opInfluence OpInfluence, region *core.RegionInfo)
}

// TransferLeader is an OpStep that transfers a region's leader.
type TransferLeader struct {
	FromStore, ToStore uint64
}

func (tl TransferLeader) String() string {
	return fmt.Sprintf("transfer leader from store %v to store %v", tl.FromStore, tl.ToStore)
}

// IsFinish checks if current step is finished.
func (tl TransferLeader) IsFinish(region *core.RegionInfo) bool {
	return region.GetLeader().GetStoreId() == tl.ToStore
}

// Influence calculates the store difference that current step makes.
func (tl TransferLeader) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	from := opInfluence.GetStoreInfluence(tl.FromStore)
	to := opInfluence.GetStoreInfluence(tl.ToStore)

	from.LeaderSize -= region.GetApproximateSize()
	from.LeaderCount--
	to.LeaderSize += region.GetApproximateSize()
	to.LeaderCount++
}

// AddPeer is an OpStep that adds a region peer.
type AddPeer struct {
	ToStore, PeerID uint64
}

func (ap AddPeer) String() string {
	return fmt.Sprintf("add peer %v on store %v", ap.PeerID, ap.ToStore)
}

// IsFinish checks if current step is finished.
func (ap AddPeer) IsFinish(region *core.RegionInfo) bool {
	if p := region.GetStoreVoter(ap.ToStore); p != nil {
		if p.GetId() != ap.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", ap.String()), zap.Uint64("obtain-voter", p.GetId()))
			return false
		}
		return region.GetPendingVoter(p.GetId()) == nil
	}
	return false
}

// Influence calculates the store difference that current step makes.
func (ap AddPeer) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	to := opInfluence.GetStoreInfluence(ap.ToStore)

	regionSize := region.GetApproximateSize()
	to.RegionSize += regionSize
	to.RegionCount++
	if regionSize > smallRegionThreshold {
		to.StepCost += RegionInfluence
	} else if regionSize <= smallRegionThreshold && regionSize > core.EmptyRegionApproximateSize {
		to.StepCost += smallRegionInfluence
	}
}

// AddLearner is an OpStep that adds a region learner peer.
type AddLearner struct {
	ToStore, PeerID uint64
}

func (al AddLearner) String() string {
	return fmt.Sprintf("add learner peer %v on store %v", al.PeerID, al.ToStore)
}

// IsFinish checks if current step is finished.
func (al AddLearner) IsFinish(region *core.RegionInfo) bool {
	if p := region.GetStoreLearner(al.ToStore); p != nil {
		if p.GetId() != al.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", al.String()), zap.Uint64("obtain-learner", p.GetId()))
			return false
		}
		return region.GetPendingLearner(p.GetId()) == nil
	}
	return false
}

// Influence calculates the store difference that current step makes.
func (al AddLearner) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	to := opInfluence.GetStoreInfluence(al.ToStore)

	regionSize := region.GetApproximateSize()
	to.RegionSize += regionSize
	to.RegionCount++
	if regionSize > smallRegionThreshold {
		to.StepCost += RegionInfluence
	} else if regionSize <= smallRegionThreshold && regionSize > core.EmptyRegionApproximateSize {
		to.StepCost += smallRegionInfluence
	}
}

// PromoteLearner is an OpStep that promotes a region learner peer to normal voter.
type PromoteLearner struct {
	ToStore, PeerID uint64
}

func (pl PromoteLearner) String() string {
	return fmt.Sprintf("promote learner peer %v on store %v to voter", pl.PeerID, pl.ToStore)
}

// IsFinish checks if current step is finished.
func (pl PromoteLearner) IsFinish(region *core.RegionInfo) bool {
	if p := region.GetStoreVoter(pl.ToStore); p != nil {
		if p.GetId() != pl.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", pl.String()), zap.Uint64("obtain-voter", p.GetId()))
		}
		return p.GetId() == pl.PeerID
	}
	return false
}

// Influence calculates the store difference that current step makes.
func (pl PromoteLearner) Influence(opInfluence OpInfluence, region *core.RegionInfo) {}

// RemovePeer is an OpStep that removes a region peer.
type RemovePeer struct {
	FromStore uint64
}

func (rp RemovePeer) String() string {
	return fmt.Sprintf("remove peer on store %v", rp.FromStore)
}

// IsFinish checks if current step is finished.
func (rp RemovePeer) IsFinish(region *core.RegionInfo) bool {
	return region.GetStorePeer(rp.FromStore) == nil
}

// Influence calculates the store difference that current step makes.
func (rp RemovePeer) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	from := opInfluence.GetStoreInfluence(rp.FromStore)

	from.RegionSize -= region.GetApproximateSize()
	from.RegionCount--
}

// MergeRegion is an OpStep that merge two regions.
type MergeRegion struct {
	FromRegion *metapb.Region
	ToRegion   *metapb.Region
	// there are two regions involved in merge process,
	// so to keep them from other scheduler,
	// both of them should add MerRegion operatorStep.
	// But actually, TiKV just needs the region want to be merged to get the merge request,
	// thus use a IsPassive mark to indicate that
	// this region doesn't need to send merge request to TiKV.
	IsPassive bool
}

func (mr MergeRegion) String() string {
	return fmt.Sprintf("merge region %v into region %v", mr.FromRegion.GetId(), mr.ToRegion.GetId())
}

// IsFinish checks if current step is finished.
func (mr MergeRegion) IsFinish(region *core.RegionInfo) bool {
	if mr.IsPassive {
		return !bytes.Equal(region.GetStartKey(), mr.ToRegion.StartKey) || !bytes.Equal(region.GetEndKey(), mr.ToRegion.EndKey)
	}
	return false
}

// Influence calculates the store difference that current step makes.
func (mr MergeRegion) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	if mr.IsPassive {
		for _, p := range region.GetPeers() {
			o := opInfluence.GetStoreInfluence(p.GetStoreId())
			o.RegionCount--
			if region.GetLeader().GetId() == p.GetId() {
				o.LeaderCount--
			}
		}
	}
}

// SplitRegion is an OpStep that splits a region.
type SplitRegion struct {
	StartKey, EndKey []byte
	Policy           pdpb.CheckPolicy
}

func (sr SplitRegion) String() string {
	return fmt.Sprintf("split region with policy %s", sr.Policy.String())
}

// IsFinish checks if current step is finished.
func (sr SplitRegion) IsFinish(region *core.RegionInfo) bool {
	return !bytes.Equal(region.GetStartKey(), sr.StartKey) || !bytes.Equal(region.GetEndKey(), sr.EndKey)
}

// Influence calculates the store difference that current step makes.
func (sr SplitRegion) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	for _, p := range region.GetPeers() {
		inf := opInfluence.GetStoreInfluence(p.GetStoreId())
		inf.RegionCount++
		if region.GetLeader().GetId() == p.GetId() {
			inf.LeaderCount++
		}
	}
}

// AddLightPeer is an OpStep that adds a region peer without considering the influence.
type AddLightPeer struct {
	ToStore, PeerID uint64
}

func (ap AddLightPeer) String() string {
	return fmt.Sprintf("add peer %v on store %v", ap.PeerID, ap.ToStore)
}

// IsFinish checks if current step is finished.
func (ap AddLightPeer) IsFinish(region *core.RegionInfo) bool {
	if p := region.GetStoreVoter(ap.ToStore); p != nil {
		if p.GetId() != ap.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", ap.String()), zap.Uint64("obtain-voter", p.GetId()))
			return false
		}
		return region.GetPendingVoter(p.GetId()) == nil
	}
	return false
}

// Influence calculates the store difference that current step makes.
func (ap AddLightPeer) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	to := opInfluence.GetStoreInfluence(ap.ToStore)

	to.RegionSize += region.GetApproximateSize()
	to.RegionCount++
}

// AddLightLearner is an OpStep that adds a region learner peer without considering the influence.
type AddLightLearner struct {
	ToStore, PeerID uint64
}

func (al AddLightLearner) String() string {
	return fmt.Sprintf("add learner peer %v on store %v", al.PeerID, al.ToStore)
}

// IsFinish checks if current step is finished.
func (al AddLightLearner) IsFinish(region *core.RegionInfo) bool {
	if p := region.GetStoreLearner(al.ToStore); p != nil {
		if p.GetId() != al.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", al.String()), zap.Uint64("obtain-learner", p.GetId()))
			return false
		}
		return region.GetPendingLearner(p.GetId()) == nil
	}
	return false
}

// Influence calculates the store difference that current step makes.
func (al AddLightLearner) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	to := opInfluence.GetStoreInfluence(al.ToStore)

	to.RegionSize += region.GetApproximateSize()
	to.RegionCount++
}

// Operator contains execution steps generated by scheduler.
type Operator struct {
	desc        string
	regionID    uint64
	regionEpoch *metapb.RegionEpoch
	kind        OpKind
	steps       []OpStep
	currentStep int32
	createTime  time.Time
	// startTime is used to record the start time of an operator which is added into running operators.
	startTime time.Time
	stepTime  int64
	level     core.PriorityLevel
}

// NewOperator creates a new operator.
func NewOperator(desc string, regionID uint64, regionEpoch *metapb.RegionEpoch, kind OpKind, steps ...OpStep) *Operator {
	return &Operator{
		desc:        desc,
		regionID:    regionID,
		regionEpoch: regionEpoch,
		kind:        kind,
		steps:       steps,
		createTime:  time.Now(),
		stepTime:    time.Now().UnixNano(),
		level:       core.NormalPriority,
	}
}

func (o *Operator) String() string {
	s := fmt.Sprintf("%s (kind:%s, region:%v(%v,%v), createAt:%s, startAt:%s, currentStep:%v, steps:%+v) ", o.desc, o.kind, o.regionID, o.regionEpoch.GetVersion(), o.regionEpoch.GetConfVer(), o.createTime, o.startTime, atomic.LoadInt32(&o.currentStep), o.steps)
	if o.IsTimeout() {
		s = s + "timeout"
	}
	if o.IsFinish() {
		s = s + "finished"
	}
	return s
}

// MarshalJSON serializes custom types to JSON.
func (o *Operator) MarshalJSON() ([]byte, error) {
	return []byte(`"` + o.String() + `"`), nil
}

// Desc returns the operator's short description.
func (o *Operator) Desc() string {
	return o.desc
}

// SetDesc sets the description for the operator.
func (o *Operator) SetDesc(desc string) {
	o.desc = desc
}

// AttachKind attaches an operator kind for the operator.
func (o *Operator) AttachKind(kind OpKind) {
	o.kind |= kind
}

// RegionID returns the region that operator is targeted.
func (o *Operator) RegionID() uint64 {
	return o.regionID
}

// RegionEpoch returns the region's epoch that is attached to the operator.
func (o *Operator) RegionEpoch() *metapb.RegionEpoch {
	return o.regionEpoch
}

// Kind returns operator's kind.
func (o *Operator) Kind() OpKind {
	return o.kind
}

// ElapsedTime returns duration since it was created.
func (o *Operator) ElapsedTime() time.Duration {
	return time.Since(o.createTime)
}

// RunningTime returns duration since it was promoted.
func (o *Operator) RunningTime() time.Duration {
	return time.Since(o.startTime)
}

// SetStartTime sets the start time for operator.
func (o *Operator) SetStartTime(t time.Time) {
	o.startTime = t
}

// GetStartTime ges the start time for operator.
func (o *Operator) GetStartTime() time.Time {
	return o.startTime
}

// Len returns the operator's steps count.
func (o *Operator) Len() int {
	return len(o.steps)
}

// Step returns the i-th step.
func (o *Operator) Step(i int) OpStep {
	if i >= 0 && i < len(o.steps) {
		return o.steps[i]
	}
	return nil
}

// Check checks if current step is finished, returns next step to take action.
// It's safe to be called by multiple goroutine concurrently.
func (o *Operator) Check(region *core.RegionInfo) OpStep {
	for step := atomic.LoadInt32(&o.currentStep); int(step) < len(o.steps); step++ {
		if o.steps[int(step)].IsFinish(region) {
			operatorStepDuration.WithLabelValues(reflect.TypeOf(o.steps[int(step)]).Name()).
				Observe(time.Since(time.Unix(0, atomic.LoadInt64(&o.stepTime))).Seconds())
			atomic.StoreInt32(&o.currentStep, step+1)
			atomic.StoreInt64(&o.stepTime, time.Now().UnixNano())
		} else {
			return o.steps[int(step)]
		}
	}
	return nil
}

// SetPriorityLevel sets the priority level for operator.
func (o *Operator) SetPriorityLevel(level core.PriorityLevel) {
	o.level = level
}

// GetPriorityLevel gets the priority level.
func (o *Operator) GetPriorityLevel() core.PriorityLevel {
	return o.level
}

// IsFinish checks if all steps are finished.
func (o *Operator) IsFinish() bool {
	return atomic.LoadInt32(&o.currentStep) >= int32(len(o.steps))
}

// IsTimeout checks the operator's create time and determines if it is timeout.
func (o *Operator) IsTimeout() bool {
	var timeout bool
	if o.IsFinish() {
		return false
	}
	if o.startTime.IsZero() {
		return false
	}
	if o.kind&OpRegion != 0 {
		timeout = time.Since(o.startTime) > RegionOperatorWaitTime
	} else {
		timeout = time.Since(o.startTime) > LeaderOperatorWaitTime
	}
	if timeout {
		return true
	}
	return false
}

// UnfinishedInfluence calculates the store difference which unfinished operator steps make.
func (o *Operator) UnfinishedInfluence(opInfluence OpInfluence, region *core.RegionInfo) {
	for step := atomic.LoadInt32(&o.currentStep); int(step) < len(o.steps); step++ {
		if !o.steps[int(step)].IsFinish(region) {
			o.steps[int(step)].Influence(opInfluence, region)
		}
	}
}

// TotalInfluence calculates the store difference which whole operator steps make.
func (o *Operator) TotalInfluence(opInfluence OpInfluence, region *core.RegionInfo) {
	for step := 0; step < len(o.steps); step++ {
		o.steps[int(step)].Influence(opInfluence, region)
	}
}

// OpHistory is used to log and visualize completed operators.
type OpHistory struct {
	FinishTime time.Time
	From, To   uint64
	Kind       core.ResourceKind
}

// History transfers the operator's steps to operator histories.
func (o *Operator) History() []OpHistory {
	now := time.Now()
	var histories []OpHistory
	var addPeerStores, removePeerStores []uint64
	for _, step := range o.steps {
		switch s := step.(type) {
		case TransferLeader:
			histories = append(histories, OpHistory{
				FinishTime: now,
				From:       s.FromStore,
				To:         s.ToStore,
				Kind:       core.LeaderKind,
			})
		case AddPeer:
			addPeerStores = append(addPeerStores, s.ToStore)
		case AddLightPeer:
			addPeerStores = append(addPeerStores, s.ToStore)
		case AddLearner:
			addPeerStores = append(addPeerStores, s.ToStore)
		case AddLightLearner:
			addPeerStores = append(addPeerStores, s.ToStore)
		case RemovePeer:
			removePeerStores = append(removePeerStores, s.FromStore)
		}
	}
	for i := range addPeerStores {
		if i < len(removePeerStores) {
			histories = append(histories, OpHistory{
				FinishTime: now,
				From:       removePeerStores[i],
				To:         addPeerStores[i],
				Kind:       core.RegionKind,
			})
		}
	}
	return histories
}

// CreateAddPeerOperator creates an operator that adds a new peer.
func CreateAddPeerOperator(desc string, cluster Cluster, region *core.RegionInfo, peerID uint64, toStoreID uint64, kind OpKind) *Operator {
	steps := CreateAddPeerSteps(cluster, toStoreID, peerID)
	return NewOperator(desc, region.GetID(), region.GetRegionEpoch(), kind|OpRegion, steps...)
}

// CreateAddLearnerOperator creates an operator that adds a new learner.
func CreateAddLearnerOperator(desc string, cluster Cluster, region *core.RegionInfo, peerID uint64, toStoreID uint64, kind OpKind) *Operator {
	step := AddLearner{ToStore: toStoreID, PeerID: peerID}
	return NewOperator(desc, region.GetID(), region.GetRegionEpoch(), kind|OpRegion, step)
}

// CreatePromoteLearnerOperator creates an operator that promotes a learner.
func CreatePromoteLearnerOperator(desc string, region *core.RegionInfo, peer *metapb.Peer) *Operator {
	step := PromoteLearner{
		ToStore: peer.GetStoreId(),
		PeerID:  peer.GetId(),
	}
	return NewOperator(desc, region.GetID(), region.GetRegionEpoch(), OpRegion, step)
}

// CreateRemovePeerOperator creates an operator that removes a peer from region.
func CreateRemovePeerOperator(desc string, cluster Cluster, kind OpKind, region *core.RegionInfo, storeID uint64) (*Operator, error) {
	removeKind, steps, err := removePeerSteps(cluster, region, storeID, getRegionFollowerIDs(region))
	if err != nil {
		return nil, err
	}
	return NewOperator(desc, region.GetID(), region.GetRegionEpoch(), removeKind|kind, steps...), nil
}

// CreateAddPeerSteps creates an OpStep list that add a new peer.
func CreateAddPeerSteps(cluster Cluster, newStore uint64, peerID uint64) []OpStep {
	var st []OpStep
	if cluster.IsRaftLearnerEnabled() {
		st = []OpStep{
			AddLearner{ToStore: newStore, PeerID: peerID},
			PromoteLearner{ToStore: newStore, PeerID: peerID},
		}
	} else {
		st = []OpStep{
			AddPeer{ToStore: newStore, PeerID: peerID},
		}
	}
	return st
}

// CreateAddLightPeerSteps creates an OpStep list that add a new peer without considering the influence.
func CreateAddLightPeerSteps(cluster Cluster, newStore uint64, peerID uint64) []OpStep {
	var st []OpStep
	if cluster.IsRaftLearnerEnabled() {
		st = []OpStep{
			AddLightLearner{ToStore: newStore, PeerID: peerID},
			PromoteLearner{ToStore: newStore, PeerID: peerID},
		}
	} else {
		st = []OpStep{
			AddLightPeer{ToStore: newStore, PeerID: peerID},
		}
	}
	return st
}

// CreateTransferLeaderOperator creates an operator that transfers the leader from a source store to a target store.
func CreateTransferLeaderOperator(desc string, region *core.RegionInfo, sourceStoreID uint64, targetStoreID uint64, kind OpKind) *Operator {
	step := TransferLeader{FromStore: sourceStoreID, ToStore: targetStoreID}
	return NewOperator(desc, region.GetID(), region.GetRegionEpoch(), kind|OpLeader, step)
}

// CreateMoveRegionOperator creates an operator that moves a region to specified stores.
func CreateMoveRegionOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, storeIDs []uint64) (*Operator, error) {
	mvkind, steps, err := moveRegionSteps(cluster, region, storeIDs)
	if err != nil {
		return nil, err
	}
	kind |= mvkind
	return NewOperator(desc, region.GetID(), region.GetRegionEpoch(), kind, steps...), nil
}

// moveRegionSteps reorders the storeIDs then calls orderedMoveRegionSteps.
//
// After reordering, the first store will not have RejectLeader label.
// If all of the stores have RejectLeader label, it returns an error.
func moveRegionSteps(cluster Cluster, region *core.RegionInfo, storeIDs []uint64) (OpKind, []OpStep, error) {
	i, _ := findNoLabelProperty(cluster, opt.RejectLeader, storeIDs)
	if i < 0 {
		return 0, nil, errors.New("all of the stores have RejectLeader label")
	}
	storeIDs[0], storeIDs[i] = storeIDs[i], storeIDs[0]
	return orderedMoveRegionSteps(cluster, region, storeIDs)
}

// orderedMoveRegionSteps returns steps to move peers of a region to specific stores in order.
//
// If the current leader is not in storeIDs, it will be transferred to a follower which
//  do not need to move if there is one, otherwise the first suitable new added follower.
// New peers will be added in the same order in storeIDs.
func orderedMoveRegionSteps(cluster Cluster, region *core.RegionInfo, storeIDs []uint64) (OpKind, []OpStep, error) {
	var kind OpKind

	oldStores := make([]uint64, 0, len(storeIDs))
	newStores := make([]uint64, 0, len(storeIDs))

	sourceStores := region.GetStoreIds()
	targetStores := make(map[uint64]struct{}, len(storeIDs))

	// Add missing peers.
	var addPeerSteps [][]OpStep
	for _, id := range storeIDs {
		targetStores[id] = struct{}{}
		if _, ok := sourceStores[id]; ok {
			oldStores = append(oldStores, id)
			continue
		}
		newStores = append(newStores, id)
		peer, err := cluster.AllocPeer(id)
		if err != nil {
			log.Debug("peer alloc failed", zap.Error(err))
			return kind, nil, err
		}
		addPeerSteps = append(addPeerSteps, CreateAddPeerSteps(cluster, id, peer.Id))
		kind |= OpRegion
	}

	// Transferring leader to a new added follower may be refused by TiKV.
	// Ref: https://github.com/tikv/tikv/issues/3819
	// So, the new leader should be a follower that do not need to move if there is one,
	// otherwise the first suitable new added follower.
	storeIDs = append(oldStores, newStores...)

	// Remove redundant peers.
	var rmPeerSteps []OpStep
	var mvLeaderSteps []OpStep
	for _, peer := range region.GetPeers() {
		id := peer.GetStoreId()
		if _, ok := targetStores[id]; ok {
			continue
		}
		if region.GetLeader().GetStoreId() == id {
			tlkind, tlsteps, err := transferLeaderToAnySteps(cluster, id, storeIDs)
			if err != nil {
				log.Debug("move region to stores failed", zap.Uint64("region id", region.GetID()), zap.Uint64s("store IDs", storeIDs), zap.Error(err))
				return kind, nil, err
			}
			mvLeaderSteps = append(tlsteps, RemovePeer{FromStore: id})
			kind |= tlkind
		} else {
			rmPeerSteps = append(rmPeerSteps, RemovePeer{FromStore: id})
		}
		kind |= OpRegion
	}

	// The following 3 'for' loop interleave addPeers and rmPeers.
	// This makes the operator add and remove peers one by one, so that there won't have
	//  too many additional peers if the operator fails in the half.
	var steps = make([]OpStep, 0, len(addPeerSteps)*2+len(rmPeerSteps)+len(mvLeaderSteps))
	i, j := 0, 0
	for ; i < len(addPeerSteps) && j < len(rmPeerSteps); i, j = i+1, j+1 {
		steps = append(steps, addPeerSteps[i]...)
		steps = append(steps, rmPeerSteps[j])
	}
	for ; i < len(addPeerSteps); i++ {
		steps = append(steps, addPeerSteps[i]...)
	}
	for ; j < len(rmPeerSteps); j++ {
		steps = append(steps, rmPeerSteps[j])
	}

	steps = append(steps, mvLeaderSteps...)

	return kind, steps, nil
}

// CreateMovePeerOperator creates an operator that replaces an old peer with a new peer.
func CreateMovePeerOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, oldStore, newStore uint64, peerID uint64) (*Operator, error) {
	removeKind, steps, err := removePeerSteps(cluster, region, oldStore, append(getRegionFollowerIDs(region), newStore))
	if err != nil {
		return nil, err
	}
	st := CreateAddPeerSteps(cluster, newStore, peerID)
	steps = append(st, steps...)
	return NewOperator(desc, region.GetID(), region.GetRegionEpoch(), removeKind|kind|OpRegion, steps...), nil
}

// CreateMoveLeaderOperator creates an operator that replaces an old leader with a new leader.
func CreateMoveLeaderOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, oldStore, newStore uint64, peerID uint64) (*Operator, error) {
	removeKind, steps, err := removePeerSteps(cluster, region, oldStore, []uint64{newStore})
	if err != nil {
		return nil, err
	}
	st := CreateAddPeerSteps(cluster, newStore, peerID)
	st = append(st, TransferLeader{ToStore: newStore, FromStore: oldStore})
	steps = append(st, steps...)
	return NewOperator(desc, region.GetID(), region.GetRegionEpoch(), removeKind|kind|OpLeader|OpRegion, steps...), nil
}

// CreateSplitRegionOperator creates an operator that splits a region.
func CreateSplitRegionOperator(desc string, region *core.RegionInfo, kind OpKind, policy string) *Operator {
	step := SplitRegion{
		StartKey: region.GetStartKey(),
		EndKey:   region.GetEndKey(),
		Policy:   pdpb.CheckPolicy(pdpb.CheckPolicy_value[strings.ToUpper(policy)]),
	}
	return NewOperator(desc, region.GetID(), region.GetRegionEpoch(), kind, step)
}

func getRegionFollowerIDs(region *core.RegionInfo) []uint64 {
	var ids []uint64
	for id := range region.GetFollowers() {
		ids = append(ids, id)
	}
	return ids
}

// removePeerSteps returns the steps to safely remove a peer. It prevents removing leader by transfer its leadership first.
func removePeerSteps(cluster Cluster, region *core.RegionInfo, storeID uint64, followerIDs []uint64) (kind OpKind, steps []OpStep, err error) {
	if region.GetLeader() != nil && region.GetLeader().GetStoreId() == storeID {
		kind, steps, err = transferLeaderToAnySteps(cluster, storeID, followerIDs)
		if err != nil {
			log.Debug("fail to create remove peer operator", zap.Uint64("region-id", region.GetID()), zap.Error(err))
			return
		}
	}
	steps = append(steps, RemovePeer{FromStore: storeID})
	kind |= OpRegion
	return
}

// findNoLabelProperty finds the first store without given label property.
func findNoLabelProperty(cluster Cluster, prop string, storeIDs []uint64) (int, uint64) {
	for i, id := range storeIDs {
		store := cluster.GetStore(id)
		if store != nil {
			if !cluster.CheckLabelProperty(prop, store.GetLabels()) {
				return i, id
			}
		} else {
			log.Debug("nil store", zap.Uint64("store id", id))
		}
	}
	return -1, 0
}

// transferLeaderToAnySteps returns the first suitable store to become region leader.
//   Return an error if there is no suitable store.
func transferLeaderToAnySteps(cluster Cluster, leaderID uint64, storeIDs []uint64) (OpKind, []OpStep, error) {
	_, id := findNoLabelProperty(cluster, opt.RejectLeader, storeIDs)
	if id != 0 {
		return OpLeader, []OpStep{TransferLeader{FromStore: leaderID, ToStore: id}}, nil
	}
	return 0, nil, errors.New("no suitable store to become region leader")
}

// CreateMergeRegionOperator creates an operator that merge two region into one.
func CreateMergeRegionOperator(desc string, cluster Cluster, source *core.RegionInfo, target *core.RegionInfo, kind OpKind) ([]*Operator, error) {
	kinds, steps, err := matchPeerSteps(cluster, source, target)
	if err != nil {
		return nil, err
	}

	steps = append(steps, MergeRegion{
		FromRegion: source.GetMeta(),
		ToRegion:   target.GetMeta(),
		IsPassive:  false,
	})

	op1 := NewOperator(desc, source.GetID(), source.GetRegionEpoch(), kinds|kind|OpMerge, steps...)
	op2 := NewOperator(desc, target.GetID(), target.GetRegionEpoch(), kinds|kind|OpMerge, MergeRegion{
		FromRegion: source.GetMeta(),
		ToRegion:   target.GetMeta(),
		IsPassive:  true,
	})

	return []*Operator{op1, op2}, nil
}

// matchPeerSteps returns the steps to match the location of peer stores of source region with target's.
func matchPeerSteps(cluster Cluster, source *core.RegionInfo, target *core.RegionInfo) (OpKind, []OpStep, error) {
	var kind OpKind

	sourcePeers := source.GetPeers()
	targetPeers := target.GetPeers()

	// make sure the peer count is same
	if len(sourcePeers) != len(targetPeers) {
		return kind, nil, errors.New("mismatch count of peer")
	}

	targetStores := make([]uint64, len(targetPeers))

	for i, peer := range targetPeers {
		targetStores[i] = peer.GetStoreId()
	}

	return moveRegionSteps(cluster, source, targetStores)
}

// CreateScatterRegionOperator creates an operator that scatters the specified region.
func CreateScatterRegionOperator(desc string, cluster Cluster, origin *core.RegionInfo, replacedPeers, targetPeers []*metapb.Peer) *Operator {
	// Randomly pick a leader
	i := rand.Intn(len(targetPeers))
	targetLeaderPeer := targetPeers[i]
	originLeaderStoreID := origin.GetLeader().GetStoreId()

	originStoreIDs := origin.GetStoreIds()
	steps := make([]OpStep, 0, len(targetPeers)*3+1)
	// deferSteps will append to the end of the steps
	deferSteps := make([]OpStep, 0, 5)
	var kind OpKind
	sameLeader := targetLeaderPeer.GetStoreId() == originLeaderStoreID
	// No need to do anything
	if sameLeader {
		isSame := true
		for _, peer := range targetPeers {
			if _, ok := originStoreIDs[peer.GetStoreId()]; !ok {
				isSame = false
				break
			}
		}
		if isSame {
			return nil
		}
	}

	// Creates the first step
	if _, ok := originStoreIDs[targetLeaderPeer.GetStoreId()]; !ok {
		st := CreateAddLightPeerSteps(cluster, targetLeaderPeer.GetStoreId(), targetLeaderPeer.GetId())
		steps = append(steps, st...)
		// Do not transfer leader to the newly added peer
		// Ref: https://github.com/tikv/tikv/issues/3819
		deferSteps = append(deferSteps, TransferLeader{FromStore: originLeaderStoreID, ToStore: targetLeaderPeer.GetStoreId()})
		deferSteps = append(deferSteps, RemovePeer{FromStore: replacedPeers[i].GetStoreId()})
		kind |= OpLeader
		kind |= OpRegion
	} else {
		if !sameLeader {
			steps = append(steps, TransferLeader{FromStore: originLeaderStoreID, ToStore: targetLeaderPeer.GetStoreId()})
			kind |= OpLeader
		}
	}

	// For the other steps
	for j, peer := range targetPeers {
		if peer.GetId() == targetLeaderPeer.GetId() {
			continue
		}
		if _, ok := originStoreIDs[peer.GetStoreId()]; ok {
			continue
		}
		if replacedPeers[j].GetStoreId() == originLeaderStoreID {
			st := CreateAddLightPeerSteps(cluster, peer.GetStoreId(), peer.GetId())
			st = append(st, RemovePeer{FromStore: replacedPeers[j].GetStoreId()})
			deferSteps = append(deferSteps, st...)
			kind |= OpRegion | OpLeader
			continue
		}
		st := CreateAddLightPeerSteps(cluster, peer.GetStoreId(), peer.GetId())
		steps = append(steps, st...)
		steps = append(steps, RemovePeer{FromStore: replacedPeers[j].GetStoreId()})
		kind |= OpRegion
	}

	steps = append(steps, deferSteps...)
	op := NewOperator(desc, origin.GetID(), origin.GetRegionEpoch(), kind, steps...)
	return op
}

// CheckOperatorValid checks if the operator is valid.
func CheckOperatorValid(op *Operator) bool {
	removeStores := []uint64{}
	for _, step := range op.steps {
		if tr, ok := step.(TransferLeader); ok {
			for _, store := range removeStores {
				if store == tr.FromStore {
					return false
				}
				if store == tr.ToStore {
					return false
				}
			}
		}
		if rp, ok := step.(RemovePeer); ok {
			removeStores = append(removeStores, rp.FromStore)
		}
	}
	return true
}
