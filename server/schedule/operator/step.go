// Copyright 2019 TiKV Project Authors.
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
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/core/storelimit"
	"go.uber.org/zap"
)

const (
	// DefaultSlowExecutorRate is the fast rate of the step executor.
	// default: 6 s/Mb
	DefaultSlowExecutorRate = 6
	// DefaultFastExecutorRate is the slow rate of the step executor.
	// default:  0.6 s/Mb
	DefaultFastExecutorRate = 0.6
	// FastStepWaitTime is the duration that the OpStep may take.
	// there are some steps that may take a short time, such as transfer leader, remove peer etc.
	// It should consider the latency of handling region heartbeat especially big cluster.
	// The update duration of region heartbeat should be less than the region heartbeat interval(default 60s).
	FastStepWaitTime = 60 * time.Second
	// SlowStepWaitTime is the duration that the OpStep may take.
	// there are some steps that may take a long time, such as add peer, merge region etc.
	SlowStepWaitTime = 10 * time.Minute
)

// OpStep describes the basic scheduling steps that can not be subdivided.
type OpStep interface {
	fmt.Stringer
	ConfVerChanged(region *core.RegionInfo) uint64
	IsFinish(region *core.RegionInfo) bool
	CheckInProgress(ci ClusterInformer, region *core.RegionInfo) error
	Influence(opInfluence OpInfluence, region *core.RegionInfo)
	Timeout(regionSize int64) time.Duration
	GetCmd(region *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse
}

// TransferLeader is an OpStep that transfers a region's leader.
type TransferLeader struct {
	// Compatible with old TiKV's TransferLeader.
	FromStore, ToStore uint64
	// Multi-target transfer leader.
	ToStores []uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (tl TransferLeader) ConfVerChanged(_ *core.RegionInfo) uint64 {
	return 0 // transfer leader never change the conf version
}

func (tl TransferLeader) String() string {
	return fmt.Sprintf("transfer leader from store %v to store %v", tl.FromStore, tl.ToStore)
}

// IsFinish checks if current step is finished.
func (tl TransferLeader) IsFinish(region *core.RegionInfo) bool {
	for _, storeID := range tl.ToStores {
		if region.GetLeader().GetStoreId() == storeID {
			return true
		}
	}
	return region.GetLeader().GetStoreId() == tl.ToStore
}

// CheckInProgress checks if the step is in the progress of advancing.
func (tl TransferLeader) CheckInProgress(ci ClusterInformer, region *core.RegionInfo) error {
	errList := make([]error, 0, len(tl.ToStores)+1)
	for _, storeID := range append(tl.ToStores, tl.ToStore) {
		peer := region.GetStorePeer(tl.ToStore)
		if peer == nil {
			errList = append(errList, errors.New("peer does not existed"))
			continue
		}
		if core.IsLearner(peer) {
			errList = append(errList, errors.New("peer already is a learner"))
			continue
		}
		if err := validateStore(ci, storeID); err != nil {
			errList = append(errList, err)
			continue
		}
		return nil
	}
	return errors.Errorf("%v", errList)
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

// Timeout returns duration that current step may take.
func (tl TransferLeader) Timeout(regionSize int64) time.Duration {
	return fastStepWaitDuration(regionSize)
}

// GetCmd returns the schedule command for heartbeat response.
func (tl TransferLeader) GetCmd(region *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	peers := make([]*metapb.Peer, 0, len(tl.ToStores))
	for _, storeID := range tl.ToStores {
		peers = append(peers, region.GetStorePeer(storeID))
	}
	return &pdpb.RegionHeartbeatResponse{
		TransferLeader: &pdpb.TransferLeader{
			Peer:  region.GetStorePeer(tl.ToStore),
			Peers: peers,
		},
	}
}

// AddPeer is an OpStep that adds a region peer.
type AddPeer struct {
	ToStore, PeerID uint64
	IsLightWeight   bool
	IsWitness       bool
}

// ConfVerChanged returns the delta value for version increased by this step.
func (ap AddPeer) ConfVerChanged(region *core.RegionInfo) uint64 {
	peer := region.GetStoreVoter(ap.ToStore)
	return typeutil.BoolToUint64(peer.GetId() == ap.PeerID)
}

func (ap AddPeer) String() string {
	return fmt.Sprintf("add peer %v on store %v", ap.PeerID, ap.ToStore)
}

// IsFinish checks if current step is finished.
func (ap AddPeer) IsFinish(region *core.RegionInfo) bool {
	if peer := region.GetStoreVoter(ap.ToStore); peer != nil {
		if peer.GetId() != ap.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", ap.String()), zap.Uint64("obtain-voter", peer.GetId()))
			return false
		}
		if peer.GetIsWitness() != ap.IsWitness {
			return false
		}
		return region.GetPendingVoter(peer.GetId()) == nil
	}
	return false
}

// Influence calculates the store difference that current step makes.
func (ap AddPeer) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	to := opInfluence.GetStoreInfluence(ap.ToStore)

	regionSize := region.GetApproximateSize()
	to.RegionSize += regionSize
	to.RegionCount++
	if ap.IsLightWeight {
		return
	}
	to.AdjustStepCost(storelimit.AddPeer, regionSize)
}

// CheckInProgress checks if the step is in the progress of advancing.
func (ap AddPeer) CheckInProgress(ci ClusterInformer, region *core.RegionInfo) error {
	if err := validateStore(ci, ap.ToStore); err != nil {
		return err
	}
	peer := region.GetStorePeer(ap.ToStore)
	if peer != nil && peer.GetId() != ap.PeerID {
		return errors.Errorf("peer %d has already existed in store %d, the timeout is trying to add peer %d on the same store", peer.GetId(), ap.ToStore, ap.PeerID)
	}
	return nil
}

// Timeout returns duration that current step may take.
func (ap AddPeer) Timeout(regionSize int64) time.Duration {
	return slowStepWaitDuration(regionSize)
}

// GetCmd returns the schedule command for heartbeat response.
func (ap AddPeer) GetCmd(region *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	peer := region.GetStorePeer(ap.ToStore)
	if peer != nil {
		// The newly added peer is pending.
		return nil
	}
	return createResponse(addNode(ap.PeerID, ap.ToStore, ap.IsWitness), useConfChangeV2)
}

// BecomeWitness is an OpStep that makes a peer become a witness.
type BecomeWitness struct {
	PeerID, StoreID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (bw BecomeWitness) ConfVerChanged(region *core.RegionInfo) uint64 {
	return 0
}

func (bw BecomeWitness) String() string {
	return fmt.Sprintf("convert peer %v on store %v to witness", bw.PeerID, bw.StoreID)
}

// IsFinish checks if current step is finished.
func (bw BecomeWitness) IsFinish(region *core.RegionInfo) bool {
	if peer := region.GetStorePeer(bw.StoreID); peer != nil {
		if peer.GetId() != bw.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", bw.String()), zap.Uint64("obtain-learner", peer.GetId()))
			return false
		}
		return peer.IsWitness
	}
	return false
}

// CheckInProgress checks if the step is in the progress of advancing.
func (bw BecomeWitness) CheckInProgress(ci ClusterInformer, region *core.RegionInfo) error {
	if err := validateStore(ci, bw.StoreID); err != nil {
		return err
	}
	peer := region.GetStorePeer(bw.StoreID)
	if peer == nil || peer.GetId() != bw.PeerID {
		return errors.New("peer does not exist")
	}
	return nil
}

// Influence calculates the store difference that current step makes.
func (bw BecomeWitness) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	to := opInfluence.GetStoreInfluence(bw.StoreID)

	regionSize := region.GetApproximateSize()
	to.RegionSize -= regionSize
	to.AdjustStepCost(storelimit.RemovePeer, regionSize)
}

// Timeout returns duration that current step may take.
func (bw BecomeWitness) Timeout(regionSize int64) time.Duration {
	return fastStepWaitDuration(regionSize)
}

// GetCmd returns the schedule command for heartbeat response.
func (bw BecomeWitness) GetCmd(_ *core.RegionInfo, _ bool) *pdpb.RegionHeartbeatResponse {
	return switchWitness(bw.PeerID, true)
}

// BecomeNonWitness is an OpStep that makes a peer become a non-witness.
type BecomeNonWitness struct {
	PeerID, StoreID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (bn BecomeNonWitness) ConfVerChanged(region *core.RegionInfo) uint64 {
	return 0
}

func (bn BecomeNonWitness) String() string {
	return fmt.Sprintf("convert peer %v on store %v to non-witness", bn.PeerID, bn.StoreID)
}

// IsFinish checks if current step is finished.
func (bn BecomeNonWitness) IsFinish(region *core.RegionInfo) bool {
	if peer := region.GetStorePeer(bn.StoreID); peer != nil {
		if peer.GetId() != bn.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", bn.String()), zap.Uint64("obtain-non-witness", peer.GetId()))
			return false
		}
		return region.GetPendingPeer(peer.GetId()) == nil && !peer.IsWitness
	}
	return false
}

// CheckInProgress checks if the step is in the progress of advancing.
func (bn BecomeNonWitness) CheckInProgress(ci ClusterInformer, region *core.RegionInfo) error {
	if err := validateStore(ci, bn.StoreID); err != nil {
		return err
	}
	peer := region.GetStorePeer(bn.StoreID)
	if peer == nil || peer.GetId() != bn.PeerID {
		return errors.New("peer does not exist")
	}
	return nil
}

// Influence calculates the store difference that current step makes.
func (bn BecomeNonWitness) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	to := opInfluence.GetStoreInfluence(bn.StoreID)

	regionSize := region.GetApproximateSize()
	to.AdjustStepCost(storelimit.AddPeer, regionSize)
}

// Timeout returns duration that current step may take.
func (bn BecomeNonWitness) Timeout(regionSize int64) time.Duration {
	return slowStepWaitDuration(regionSize)
}

// GetCmd returns the schedule command for heartbeat response.
func (bn BecomeNonWitness) GetCmd(region *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	return switchWitness(bn.PeerID, false)
}

type BatchSwitchWitness struct {
	toWitnesses    []BecomeWitness
	toNonWitnesses []BecomeNonWitness
}

func (bsw BatchSwitchWitness) String() string {
	b := &strings.Builder{}
	_, _ = b.WriteString("batch switch witness")
	for _, w := range bsw.toWitnesses {
		_, _ = fmt.Fprintf(b, ", switch peer %v on store %v to witness", w.PeerID, w.StoreID)
	}
	for _, nw := range bsw.toNonWitnesses {
		_, _ = fmt.Fprintf(b, ", switch peer %v on store %v to non witness", nw.PeerID, nw.StoreID)
	}
	return b.String()
}

// ConfVerChanged returns the delta value for version increased by this step.
func (bsw BatchSwitchWitness) ConfVerChanged(region *core.RegionInfo) uint64 {
	return 0 // switch witness never change the conf version
}

// IsFinish checks if current step is finished.
func (bsw BatchSwitchWitness) IsFinish(region *core.RegionInfo) bool {
	for _, w := range bsw.toWitnesses {
		if !w.IsFinish(region) {
			return false
		}
	}
	for _, nw := range bsw.toNonWitnesses {
		if !nw.IsFinish(region) {
			return false
		}
	}
	return true
}

func (bsw BatchSwitchWitness) CheckInProgress(ci ClusterInformer, region *core.RegionInfo) error {
	for _, w := range bsw.toWitnesses {
		if err := w.CheckInProgress(ci, region); err != nil {
			return err
		}
	}
	for _, nw := range bsw.toNonWitnesses {
		if err := nw.CheckInProgress(ci, region); err != nil {
			return err
		}
	}
	return nil
}

func (bsw BatchSwitchWitness) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	for _, w := range bsw.toWitnesses {
		w.Influence(opInfluence, region)
	}
	for _, nw := range bsw.toNonWitnesses {
		nw.Influence(opInfluence, region)
	}
}

func (bsw BatchSwitchWitness) Timeout(regionSize int64) time.Duration {
	count := uint64(len(bsw.toWitnesses)+len(bsw.toNonWitnesses)) + 1
	return fastStepWaitDuration(regionSize) * time.Duration(count)
}

func (bsw BatchSwitchWitness) GetCmd(region *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	switches := make([]*pdpb.SwitchWitness, 0, len(bsw.toWitnesses)+len(bsw.toNonWitnesses))
	for _, w := range bsw.toWitnesses {
		switches = append(switches, w.GetCmd(region, useConfChangeV2).SwitchWitnesses.SwitchWitnesses...)
	}
	for _, nw := range bsw.toNonWitnesses {
		switches = append(switches, nw.GetCmd(region, useConfChangeV2).SwitchWitnesses.SwitchWitnesses...)
	}
	return &pdpb.RegionHeartbeatResponse{
		SwitchWitnesses: &pdpb.BatchSwitchWitness{
			SwitchWitnesses: switches,
		},
	}
}

// AddLearner is an OpStep that adds a region learner peer.
type AddLearner struct {
	ToStore, PeerID uint64
	IsLightWeight   bool
	IsWitness       bool
}

// ConfVerChanged returns the delta value for version increased by this step.
func (al AddLearner) ConfVerChanged(region *core.RegionInfo) uint64 {
	peer := region.GetStorePeer(al.ToStore)
	return typeutil.BoolToUint64(peer.GetId() == al.PeerID)
}

func (al AddLearner) String() string {
	return fmt.Sprintf("add learner peer %v on store %v", al.PeerID, al.ToStore)
}

// IsFinish checks if current step is finished.
func (al AddLearner) IsFinish(region *core.RegionInfo) bool {
	if peer := region.GetStoreLearner(al.ToStore); peer != nil {
		if peer.GetId() != al.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", al.String()), zap.Uint64("obtain-learner", peer.GetId()))
			return false
		}
		if peer.GetIsWitness() != al.IsWitness {
			return false
		}
		return region.GetPendingLearner(peer.GetId()) == nil
	}
	return false
}

// CheckInProgress checks if the step is in the progress of advancing.
func (al AddLearner) CheckInProgress(ci ClusterInformer, region *core.RegionInfo) error {
	if err := validateStore(ci, al.ToStore); err != nil {
		return err
	}
	peer := region.GetStorePeer(al.ToStore)
	if peer == nil {
		return nil
	}
	if peer.GetId() != al.PeerID {
		return errors.Errorf("peer %d has already existed in store %d, the timeout is trying to add peer %d on the same store", peer.GetId(), al.ToStore, al.PeerID)
	}
	if !core.IsLearner(peer) {
		return errors.New("peer already is a voter")
	}
	return nil
}

// Influence calculates the store difference that current step makes.
func (al AddLearner) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	to := opInfluence.GetStoreInfluence(al.ToStore)

	regionSize := region.GetApproximateSize()
	to.RegionSize += regionSize
	to.RegionCount++
	if al.IsLightWeight {
		return
	}
	to.AdjustStepCost(storelimit.AddPeer, regionSize)
}

// Timeout returns duration that current step may take.
func (al AddLearner) Timeout(regionSize int64) time.Duration {
	return slowStepWaitDuration(regionSize)
}

// GetCmd returns the schedule command for heartbeat response.
func (al AddLearner) GetCmd(region *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	if region.GetStorePeer(al.ToStore) != nil {
		// The newly added peer is pending.
		return nil
	}
	return createResponse(addLearnerNode(al.PeerID, al.ToStore, al.IsWitness), useConfChangeV2)
}

// PromoteLearner is an OpStep that promotes a region learner peer to normal voter.
type PromoteLearner struct {
	ToStore, PeerID uint64
	IsWitness       bool
}

// ConfVerChanged returns the delta value for version increased by this step.
// It is also used by ChangePeerV2Leave. Since there are currently four roles,
// we need to confirm whether it is a Voter, not a DemotingVoter, etc.
func (pl PromoteLearner) ConfVerChanged(region *core.RegionInfo) uint64 {
	peer := region.GetStoreVoter(pl.ToStore)
	return typeutil.BoolToUint64(peer.GetId() == pl.PeerID && peer.GetRole() == metapb.PeerRole_Voter)
}

func (pl PromoteLearner) String() string {
	return fmt.Sprintf("promote learner peer %v on store %v to voter", pl.PeerID, pl.ToStore)
}

// IsFinish checks if current step is finished. It is also used by ChangePeerV2Leave.
func (pl PromoteLearner) IsFinish(region *core.RegionInfo) bool {
	if peer := region.GetStoreVoter(pl.ToStore); peer != nil {
		if peer.GetId() != pl.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", pl.String()), zap.Uint64("obtain-voter", peer.GetId()))
		}
		return peer.GetId() == pl.PeerID && peer.GetRole() == metapb.PeerRole_Voter
	}
	return false
}

// CheckInProgress checks if the step is in the progress of advancing.
func (pl PromoteLearner) CheckInProgress(_ ClusterInformer, region *core.RegionInfo) error {
	peer := region.GetStorePeer(pl.ToStore)
	if peer.GetId() != pl.PeerID {
		return errors.New("peer does not exist")
	}
	return nil
}

// Influence calculates the store difference that current step makes.
func (pl PromoteLearner) Influence(_ OpInfluence, _ *core.RegionInfo) {}

// Timeout returns duration that current step may take.
func (pl PromoteLearner) Timeout(regionSize int64) time.Duration {
	return fastStepWaitDuration(regionSize)
}

// GetCmd returns the schedule command for heartbeat response.
func (pl PromoteLearner) GetCmd(_ *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	return createResponse(addNode(pl.PeerID, pl.ToStore, pl.IsWitness), useConfChangeV2)
}

// RemovePeer is an OpStep that removes a region peer.
type RemovePeer struct {
	FromStore, PeerID uint64
	IsDownStore       bool
}

// ConfVerChanged returns the delta value for version increased by this step.
func (rp RemovePeer) ConfVerChanged(region *core.RegionInfo) uint64 {
	id := region.GetStorePeer(rp.FromStore).GetId()
	// 1. id == 0 -> The peer does not exist, it needs to return 1.
	// 2. id != 0 && rp.PeerId == 0 -> No rp.PeerID is specified, and there is a Peer on the Store, it needs to return 0.
	// 3. id != 0 && rp.PeerID != 0 && id == rp.PeerID -> The peer still exists, it needs to return 0.
	// 4. id != 0 && rp.PeerID != 0 && id != rp.PeerID -> The rp.PeerID is specified,
	//     and although there is a Peer on the Store, but the Id has changed, it should return 1.
	//     This is for the following case:
	//     If DemoteFollower step is not allowed, it will be split into RemovePeer and AddLearner.
	//     After the AddLearner step, ConfVerChanged of RemovePeer should still return 1.
	return typeutil.BoolToUint64(id == 0 || (rp.PeerID != 0 && id != rp.PeerID))
}

func (rp RemovePeer) String() string {
	return fmt.Sprintf("remove peer on store %v", rp.FromStore)
}

// IsFinish checks if current step is finished.
func (rp RemovePeer) IsFinish(region *core.RegionInfo) bool {
	return region.GetStorePeer(rp.FromStore) == nil
}

// CheckInProgress checks if the step is in the progress of advancing.
func (rp RemovePeer) CheckInProgress(_ ClusterInformer, region *core.RegionInfo) error {
	if rp.FromStore == region.GetLeader().GetStoreId() {
		return errors.New("cannot remove leader peer")
	}
	return nil
}

// Influence calculates the store difference that current step makes.
func (rp RemovePeer) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	from := opInfluence.GetStoreInfluence(rp.FromStore)

	regionSize := region.GetApproximateSize()
	from.RegionSize -= regionSize
	from.RegionCount--

	if rp.IsDownStore && regionSize > storelimit.SmallRegionThreshold {
		regionSize = storelimit.SmallRegionThreshold
	}
	from.AdjustStepCost(storelimit.RemovePeer, regionSize)
}

// Timeout returns duration that current step may take.
func (rp RemovePeer) Timeout(regionSize int64) time.Duration {
	return fastStepWaitDuration(regionSize)
}

// GetCmd returns the schedule command for heartbeat response.
func (rp RemovePeer) GetCmd(region *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	return createResponse(&pdpb.ChangePeer{
		ChangeType: eraftpb.ConfChangeType_RemoveNode,
		Peer:       region.GetStorePeer(rp.FromStore),
	}, useConfChangeV2)
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

// ConfVerChanged returns the delta value for version increased by this step.
func (mr MergeRegion) ConfVerChanged(_ *core.RegionInfo) uint64 {
	return 0
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

// CheckInProgress checks if the step is in the progress of advancing.
func (mr MergeRegion) CheckInProgress(_ ClusterInformer, _ *core.RegionInfo) error {
	return nil
}

// Influence calculates the store difference that current step makes.
func (mr MergeRegion) Influence(opInfluence OpInfluence, region *core.RegionInfo) {
	if mr.IsPassive {
		for _, peer := range region.GetPeers() {
			o := opInfluence.GetStoreInfluence(peer.GetStoreId())
			o.RegionCount--
			if region.GetLeader().GetId() == peer.GetId() {
				o.LeaderCount--
			}
		}
	}
}

// Timeout returns duration that current step may take.
// The merge step need more time to finish but less than slow step.
func (mr MergeRegion) Timeout(regionSize int64) time.Duration {
	return fastStepWaitDuration(regionSize) * 10
}

// GetCmd returns the schedule command for heartbeat response.
func (mr MergeRegion) GetCmd(region *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	if mr.IsPassive {
		return nil
	}
	return &pdpb.RegionHeartbeatResponse{
		Merge: &pdpb.Merge{
			Target: mr.ToRegion,
		},
	}
}

// SplitRegion is an OpStep that splits a region.
type SplitRegion struct {
	StartKey, EndKey []byte
	Policy           pdpb.CheckPolicy
	SplitKeys        [][]byte
}

// ConfVerChanged returns the delta value for version increased by this step.
func (sr SplitRegion) ConfVerChanged(_ *core.RegionInfo) uint64 {
	return 0
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
	for _, peer := range region.GetPeers() {
		inf := opInfluence.GetStoreInfluence(peer.GetStoreId())
		inf.RegionCount++
		if region.GetLeader().GetId() == peer.GetId() {
			inf.LeaderCount++
		}
	}
}

// CheckInProgress checks if the step is in the progress of advancing.
func (sr SplitRegion) CheckInProgress(_ ClusterInformer, _ *core.RegionInfo) error {
	return nil
}

// Timeout returns duration that current step may take.
func (sr SplitRegion) Timeout(regionSize int64) time.Duration {
	return fastStepWaitDuration(regionSize)
}

// GetCmd returns the schedule command for heartbeat response.
func (sr SplitRegion) GetCmd(region *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	return &pdpb.RegionHeartbeatResponse{
		SplitRegion: &pdpb.SplitRegion{
			Policy: sr.Policy,
			Keys:   sr.SplitKeys,
		},
	}
}

// DemoteVoter is very similar to DemoteFollower. But it allows Demote Leader.
// Note: It is not an OpStep, only a sub step in ChangePeerV2Enter and ChangePeerV2Leave.
type DemoteVoter struct {
	ToStore, PeerID uint64
	IsWitness       bool
}

func (dv DemoteVoter) String() string {
	return fmt.Sprintf("demote voter peer %v on store %v to learner", dv.PeerID, dv.ToStore)
}

// ConfVerChanged returns the delta value for version increased by this step.
func (dv DemoteVoter) ConfVerChanged(region *core.RegionInfo) uint64 {
	peer := region.GetStorePeer(dv.ToStore)
	// the demoting peer may be removed later.
	return typeutil.BoolToUint64(peer == nil || (peer.GetId() == dv.PeerID && peer.GetRole() == metapb.PeerRole_Learner))
}

// IsFinish checks if current step is finished.
func (dv DemoteVoter) IsFinish(region *core.RegionInfo) bool {
	if peer := region.GetStoreLearner(dv.ToStore); peer != nil {
		if peer.GetId() != dv.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", dv.String()), zap.Uint64("obtain-learner", peer.GetId()))
			return false
		}
		return region.GetPendingLearner(peer.GetId()) == nil
	}
	return false
}

// Timeout returns duration that current step may take.
func (dv DemoteVoter) Timeout(regionSize int64) time.Duration {
	return fastStepWaitDuration(regionSize)
}

// GetCmd returns the schedule command for heartbeat response.
func (dv DemoteVoter) GetCmd(_ *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	return createResponse(addLearnerNode(dv.PeerID, dv.ToStore, dv.IsWitness), useConfChangeV2)
}

// ChangePeerV2Enter is an OpStep that uses joint consensus to request all PromoteLearner and DemoteVoter.
type ChangePeerV2Enter struct {
	PromoteLearners []PromoteLearner
	DemoteVoters    []DemoteVoter
}

func (cpe ChangePeerV2Enter) String() string {
	b := &strings.Builder{}
	_, _ = b.WriteString("use joint consensus")
	for _, pl := range cpe.PromoteLearners {
		_, _ = fmt.Fprintf(b, ", promote learner peer %v on store %v to voter", pl.PeerID, pl.ToStore)
	}
	for _, dv := range cpe.DemoteVoters {
		_, _ = fmt.Fprintf(b, ", demote voter peer %v on store %v to learner", dv.PeerID, dv.ToStore)
	}
	return b.String()
}

// ConfVerChanged returns the delta value for version increased by this step.
func (cpe ChangePeerV2Enter) ConfVerChanged(region *core.RegionInfo) uint64 {
	for _, pl := range cpe.PromoteLearners {
		peer := region.GetStoreVoter(pl.ToStore)
		if peer.GetId() != pl.PeerID || !core.IsVoterOrIncomingVoter(peer) {
			return 0
		}
	}
	for _, dv := range cpe.DemoteVoters {
		peer := region.GetStorePeer(dv.ToStore)
		// the demoting peer may be removed later.
		if peer != nil && (peer.GetId() != dv.PeerID || !core.IsLearnerOrDemotingVoter(peer)) {
			return 0
		}
	}
	return uint64(len(cpe.PromoteLearners) + len(cpe.DemoteVoters))
}

// IsFinish checks if current step is finished.
func (cpe ChangePeerV2Enter) IsFinish(region *core.RegionInfo) bool {
	for _, pl := range cpe.PromoteLearners {
		peer := region.GetStoreVoter(pl.ToStore)
		if peer != nil && peer.GetId() != pl.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", pl.String()), zap.Uint64("obtain-voter", peer.GetId()))
		}
		if peer.GetId() != pl.PeerID || !core.IsVoterOrIncomingVoter(peer) {
			return false
		}
	}
	for _, dv := range cpe.DemoteVoters {
		peer := region.GetStorePeer(dv.ToStore)
		if peer != nil && peer.GetId() != dv.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", dv.String()), zap.Uint64("obtain-learner", peer.GetId()))
		}
		if peer.GetId() != dv.PeerID || !core.IsLearnerOrDemotingVoter(peer) {
			return false
		}
	}
	return true
}

// CheckInProgress checks if the step is in the progress of advancing.
func (cpe ChangePeerV2Enter) CheckInProgress(_ ClusterInformer, region *core.RegionInfo) error {
	inJointState, notInJointState := false, false
	for _, pl := range cpe.PromoteLearners {
		peer := region.GetStorePeer(pl.ToStore)
		if peer.GetId() != pl.PeerID {
			return errors.New("peer does not exist")
		}
		switch peer.GetRole() {
		case metapb.PeerRole_Learner, metapb.PeerRole_Voter:
			notInJointState = true
		case metapb.PeerRole_IncomingVoter:
			inJointState = true
		case metapb.PeerRole_DemotingVoter:
			return errors.New("cannot promote a demoting voter")
		default:
			return errors.New("unexpected peer role")
		}
	}
	for _, dv := range cpe.DemoteVoters {
		peer := region.GetStorePeer(dv.ToStore)
		if peer.GetId() != dv.PeerID {
			return errors.New("peer does not exist")
		}
		switch peer.GetRole() {
		case metapb.PeerRole_Voter, metapb.PeerRole_Learner:
			notInJointState = true
		case metapb.PeerRole_DemotingVoter:
			inJointState = true
		case metapb.PeerRole_IncomingVoter:
			return errors.New("cannot demote a incoming voter")
		default:
			return errors.New("unexpected peer role")
		}
	}

	switch count := core.CountInJointState(region.GetPeers()...); {
	case notInJointState && inJointState:
		return errors.New("non-atomic joint consensus")
	case notInJointState && count != 0:
		return errors.New("some other peers are in joint state, when the region is in joint state")
	case inJointState && count != len(cpe.PromoteLearners)+len(cpe.DemoteVoters):
		return errors.New("some other peers are in joint state, when the region is not in joint state")
	}

	return nil
}

// Influence calculates the store difference that current step makes.
func (cpe ChangePeerV2Enter) Influence(_ OpInfluence, _ *core.RegionInfo) {}

// Timeout returns duration that current step may take.
func (cpe ChangePeerV2Enter) Timeout(regionSize int64) time.Duration {
	count := uint64(len(cpe.PromoteLearners)+len(cpe.DemoteVoters)) + 1
	return fastStepWaitDuration(regionSize) * time.Duration(count)
}

// GetCmd returns the schedule command for heartbeat response.
func (cpe ChangePeerV2Enter) GetCmd(region *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	if !useConfChangeV2 {
		// only supported in ChangePeerV2
		return nil
	}
	changes := make([]*pdpb.ChangePeer, 0, len(cpe.PromoteLearners)+len(cpe.DemoteVoters))
	for _, pl := range cpe.PromoteLearners {
		changes = append(changes, pl.GetCmd(region, useConfChangeV2).ChangePeerV2.Changes...)
	}
	for _, dv := range cpe.DemoteVoters {
		changes = append(changes, dv.GetCmd(region, useConfChangeV2).ChangePeerV2.Changes...)
	}
	return &pdpb.RegionHeartbeatResponse{
		ChangePeerV2: &pdpb.ChangePeerV2{
			Changes: changes,
		},
	}
}

// ChangePeerV2Leave is an OpStep that leaves the joint state.
type ChangePeerV2Leave struct {
	PromoteLearners []PromoteLearner
	DemoteVoters    []DemoteVoter
}

func (cpl ChangePeerV2Leave) String() string {
	b := &strings.Builder{}
	_, _ = b.WriteString("leave joint state")
	for _, pl := range cpl.PromoteLearners {
		_, _ = fmt.Fprintf(b, ", promote learner peer %v on store %v to voter", pl.PeerID, pl.ToStore)
	}
	for _, dv := range cpl.DemoteVoters {
		_, _ = fmt.Fprintf(b, ", demote voter peer %v on store %v to learner", dv.PeerID, dv.ToStore)
	}
	return b.String()
}

// ConfVerChanged returns the delta value for version increased by this step.
func (cpl ChangePeerV2Leave) ConfVerChanged(region *core.RegionInfo) uint64 {
	for _, pl := range cpl.PromoteLearners {
		if pl.ConfVerChanged(region) == 0 {
			return 0
		}
	}
	for _, dv := range cpl.DemoteVoters {
		if dv.ConfVerChanged(region) == 0 {
			return 0
		}
	}
	return uint64(len(cpl.PromoteLearners) + len(cpl.DemoteVoters))
}

// IsFinish checks if current step is finished.
func (cpl ChangePeerV2Leave) IsFinish(region *core.RegionInfo) bool {
	for _, pl := range cpl.PromoteLearners {
		if !pl.IsFinish(region) {
			return false
		}
	}
	for _, dv := range cpl.DemoteVoters {
		if !dv.IsFinish(region) {
			return false
		}
	}
	if core.IsInJointState(region.GetPeers()...) {
		log.Warn("region is still in the joint state", zap.Uint64("region-id", region.GetID()))
		return false
	}
	return true
}

// CheckInProgress checks if the step is in the progress of advancing.
func (cpl ChangePeerV2Leave) CheckInProgress(_ ClusterInformer, region *core.RegionInfo) error {
	inJointState, notInJointState, demoteLeader := false, false, false
	leaderStoreID := region.GetLeader().GetStoreId()

	for _, pl := range cpl.PromoteLearners {
		peer := region.GetStorePeer(pl.ToStore)
		if peer.GetId() != pl.PeerID {
			return errors.New("peer does not exist")
		}
		switch peer.GetRole() {
		case metapb.PeerRole_Voter:
			notInJointState = true
		case metapb.PeerRole_IncomingVoter:
			inJointState = true
		case metapb.PeerRole_Learner:
			return errors.New("peer is still a learner")
		case metapb.PeerRole_DemotingVoter:
			return errors.New("cannot promote a demoting voter")
		default:
			return errors.New("unexpected peer role")
		}
	}
	for _, dv := range cpl.DemoteVoters {
		peer := region.GetStorePeer(dv.ToStore)
		if peer.GetId() != dv.PeerID {
			return errors.New("peer does not exist")
		}
		switch peer.GetRole() {
		case metapb.PeerRole_Learner:
			notInJointState = true
		case metapb.PeerRole_DemotingVoter:
			inJointState = true
			if peer.GetStoreId() == leaderStoreID {
				demoteLeader = true
			}
		case metapb.PeerRole_Voter:
			return errors.New("peer is still a voter")
		case metapb.PeerRole_IncomingVoter:
			return errors.New("cannot demote a incoming voter")
		default:
			return errors.New("unexpected peer role")
		}
	}

	switch count := core.CountInJointState(region.GetPeers()...); {
	case notInJointState && inJointState:
		return errors.New("non-atomic joint consensus")
	case notInJointState && count != 0:
		return errors.New("some other peers are in joint state, when the region is in joint state")
	case inJointState && count != len(cpl.PromoteLearners)+len(cpl.DemoteVoters):
		return errors.New("some other peers are in joint state, when the region is not in joint state")
	case demoteLeader:
		return errors.New("cannot demote leader peer")
	}

	return nil
}

// Influence calculates the store difference that current step makes.
func (cpl ChangePeerV2Leave) Influence(_ OpInfluence, _ *core.RegionInfo) {}

// Timeout returns duration that current step may take.
func (cpl ChangePeerV2Leave) Timeout(regionSize int64) time.Duration {
	count := uint64(len(cpl.PromoteLearners)+len(cpl.DemoteVoters)) + 1
	return fastStepWaitDuration(regionSize) * time.Duration(count)
}

// GetCmd returns the schedule command for heartbeat response.
func (cpl ChangePeerV2Leave) GetCmd(region *core.RegionInfo, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	if !useConfChangeV2 {
		// only supported in ChangePeerV2
		return nil
	}
	return &pdpb.RegionHeartbeatResponse{
		ChangePeerV2: &pdpb.ChangePeerV2{},
	}
}

func validateStore(ci ClusterInformer, id uint64) error {
	store := ci.GetBasicCluster().GetStore(id)
	if store == nil {
		return errors.New("target store does not exist")
	}
	if store.DownTime() > ci.GetOpts().GetMaxStoreDownTime() {
		return errors.New("target store is down")
	}
	return nil
}

func slowStepWaitDuration(regionSize int64) time.Duration {
	seconds := DefaultSlowExecutorRate * regionSize
	wait := time.Duration(seconds) * time.Second
	if wait < SlowStepWaitTime {
		wait = SlowStepWaitTime
	}
	return wait
}

func fastStepWaitDuration(regionSize int64) time.Duration {
	seconds := int64(DefaultFastExecutorRate * float64(regionSize))
	wait := time.Duration(seconds) * time.Second
	if wait < FastStepWaitTime {
		wait = FastStepWaitTime
	}
	return wait
}

func addNode(id, storeID uint64, isWitness bool) *pdpb.ChangePeer {
	return &pdpb.ChangePeer{
		ChangeType: eraftpb.ConfChangeType_AddNode,
		Peer: &metapb.Peer{
			Id:        id,
			StoreId:   storeID,
			Role:      metapb.PeerRole_Voter,
			IsWitness: isWitness,
		},
	}
}

func addLearnerNode(id, storeID uint64, isWitness bool) *pdpb.ChangePeer {
	return &pdpb.ChangePeer{
		ChangeType: eraftpb.ConfChangeType_AddLearnerNode,
		Peer: &metapb.Peer{
			Id:        id,
			StoreId:   storeID,
			Role:      metapb.PeerRole_Learner,
			IsWitness: isWitness,
		},
	}
}

func createResponse(change *pdpb.ChangePeer, useConfChangeV2 bool) *pdpb.RegionHeartbeatResponse {
	if useConfChangeV2 {
		return &pdpb.RegionHeartbeatResponse{
			ChangePeerV2: &pdpb.ChangePeerV2{
				Changes: []*pdpb.ChangePeer{change},
			},
		}
	}
	return &pdpb.RegionHeartbeatResponse{
		ChangePeer: change,
	}
}

func switchWitness(peerID uint64, isWitness bool) *pdpb.RegionHeartbeatResponse {
	return &pdpb.RegionHeartbeatResponse{
		SwitchWitnesses: &pdpb.BatchSwitchWitness{
			SwitchWitnesses: []*pdpb.SwitchWitness{&pdpb.SwitchWitness{PeerId: peerID, IsWitness: isWitness}},
		},
	}
}
