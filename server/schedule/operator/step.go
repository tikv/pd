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

// OpStep describes the basic scheduling steps that can not be subdivided.
type OpStep interface {
	fmt.Stringer
	ConfVerChanged(region *core.RegionInfo) uint64
	IsFinish(region *core.RegionInfo) bool
	CheckSafety(region *core.RegionInfo) error
	Influence(opInfluence OpInfluence, region *core.RegionInfo)
}

// TransferLeader is an OpStep that transfers a region's leader.
type TransferLeader struct {
	FromStore, ToStore uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (tl TransferLeader) ConfVerChanged(region *core.RegionInfo) uint64 {
	return 0 // transfer leader never change the conf version
}

func (tl TransferLeader) String() string {
	return fmt.Sprintf("transfer leader from store %v to store %v", tl.FromStore, tl.ToStore)
}

// IsFinish checks if current step is finished.
func (tl TransferLeader) IsFinish(region *core.RegionInfo) bool {
	return region.GetLeader().GetStoreId() == tl.ToStore
}

// CheckSafety checks if the step meets the safety properties.
func (tl TransferLeader) CheckSafety(region *core.RegionInfo) error {
	peer := region.GetStorePeer(tl.ToStore)
	if peer == nil {
		return errors.New("peer does not existed")
	}
	if core.IsLearner(peer) {
		return errors.New("peer already is a learner")
	}
	return nil
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
	IsLightWeight   bool
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

// CheckSafety checks if the step meets the safety properties.
func (ap AddPeer) CheckSafety(region *core.RegionInfo) error {
	peer := region.GetStorePeer(ap.ToStore)
	if peer != nil && peer.GetId() != ap.PeerID {
		return errors.Errorf("peer %d has already existed in store %d, the operator is trying to add peer %d on the same store", peer.GetId(), ap.ToStore, ap.PeerID)
	}
	return nil
}

// AddLearner is an OpStep that adds a region learner peer.
type AddLearner struct {
	ToStore, PeerID uint64
	IsLightWeight   bool
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
		return region.GetPendingLearner(peer.GetId()) == nil
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (al AddLearner) CheckSafety(region *core.RegionInfo) error {
	peer := region.GetStorePeer(al.ToStore)
	if peer == nil {
		return nil
	}
	if peer.GetId() != al.PeerID {
		return errors.Errorf("peer %d has already existed in store %d, the operator is trying to add peer %d on the same store", peer.GetId(), al.ToStore, al.PeerID)
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

// PromoteLearner is an OpStep that promotes a region learner peer to normal voter.
type PromoteLearner struct {
	ToStore, PeerID uint64
}

// ConfVerChanged returns the delta value for version increased by this step.
func (pl PromoteLearner) ConfVerChanged(region *core.RegionInfo) uint64 {
	peer := region.GetStoreVoter(pl.ToStore)
	return typeutil.BoolToUint64(peer.GetId() == pl.PeerID)
}

func (pl PromoteLearner) String() string {
	return fmt.Sprintf("promote learner peer %v on store %v to voter", pl.PeerID, pl.ToStore)
}

// IsFinish checks if current step is finished.
func (pl PromoteLearner) IsFinish(region *core.RegionInfo) bool {
	if peer := region.GetStoreVoter(pl.ToStore); peer != nil {
		if peer.GetId() != pl.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", pl.String()), zap.Uint64("obtain-voter", peer.GetId()))
		}
		return peer.GetId() == pl.PeerID
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (pl PromoteLearner) CheckSafety(region *core.RegionInfo) error {
	peer := region.GetStorePeer(pl.ToStore)
	if peer.GetId() != pl.PeerID {
		return errors.New("peer does not exist")
	}
	return nil
}

// Influence calculates the store difference that current step makes.
func (pl PromoteLearner) Influence(opInfluence OpInfluence, region *core.RegionInfo) {}

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

// CheckSafety checks if the step meets the safety properties.
func (rp RemovePeer) CheckSafety(region *core.RegionInfo) error {
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
func (mr MergeRegion) ConfVerChanged(region *core.RegionInfo) uint64 {
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

// CheckSafety checks if the step meets the safety properties.
func (mr MergeRegion) CheckSafety(region *core.RegionInfo) error {
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

// SplitRegion is an OpStep that splits a region.
type SplitRegion struct {
	StartKey, EndKey []byte
	Policy           pdpb.CheckPolicy
	SplitKeys        [][]byte
}

// ConfVerChanged returns the delta value for version increased by this step.
func (sr SplitRegion) ConfVerChanged(region *core.RegionInfo) uint64 {
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

// CheckSafety checks if the step meets the safety properties.
func (sr SplitRegion) CheckSafety(region *core.RegionInfo) error {
	return nil
}

// DemoteFollower is an OpStep that demotes a region follower peer to learner.
type DemoteFollower struct {
	ToStore, PeerID uint64
}

func (df DemoteFollower) String() string {
	return fmt.Sprintf("demote follower peer %v on store %v to learner", df.PeerID, df.ToStore)
}

// ConfVerChanged returns the delta value for version increased by this step.
func (df DemoteFollower) ConfVerChanged(region *core.RegionInfo) uint64 {
	peer := region.GetStoreLearner(df.ToStore)
	return typeutil.BoolToUint64(peer.GetId() == df.PeerID)
}

// IsFinish checks if current step is finished.
func (df DemoteFollower) IsFinish(region *core.RegionInfo) bool {
	if peer := region.GetStoreLearner(df.ToStore); peer != nil {
		if peer.GetId() != df.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", df.String()), zap.Uint64("obtain-learner", peer.GetId()))
		}
		return peer.GetId() == df.PeerID
	}
	return false
}

// CheckSafety checks if the step meets the safety properties.
func (df DemoteFollower) CheckSafety(region *core.RegionInfo) error {
	peer := region.GetStorePeer(df.ToStore)
	if peer.GetId() != df.PeerID {
		return errors.New("peer does not exist")
	}
	if peer.GetId() == region.GetLeader().GetId() {
		return errors.New("cannot demote leader peer")
	}
	return nil
}

// Influence calculates the store difference that current step makes.
func (df DemoteFollower) Influence(opInfluence OpInfluence, region *core.RegionInfo) {}

// DemoteVoter is very similar to DemoteFollower. But it allows Demote Leader.
// Note: It is not an OpStep, only a sub step in ChangePeerV2Enter and ChangePeerV2Leave.
type DemoteVoter struct {
	ToStore, PeerID uint64
}

func (dv DemoteVoter) String() string {
	return fmt.Sprintf("demote voter peer %v on store %v to learner", dv.PeerID, dv.ToStore)
}

// ConfVerChanged returns the delta value for version increased by this step.
func (dv DemoteVoter) ConfVerChanged(region *core.RegionInfo) bool {
	peer := region.GetStoreLearner(dv.ToStore)
	return peer.GetId() == dv.PeerID
}

// IsFinish checks if current step is finished.
func (dv DemoteVoter) IsFinish(region *core.RegionInfo) bool {
	if peer := region.GetStoreLearner(dv.ToStore); peer != nil {
		if peer.GetId() != dv.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", dv.String()), zap.Uint64("obtain-learner", peer.GetId()))
		}
		return peer.GetId() == dv.PeerID
	}
	return false
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
		peer := region.GetStoreVoter(dv.ToStore)
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
		if peer.GetId() != pl.PeerID || peer.GetRole() != metapb.PeerRole_IncomingVoter {
			return false
		}
	}
	for _, dv := range cpe.DemoteVoters {
		peer := region.GetStoreVoter(dv.ToStore)
		if peer != nil && peer.GetId() != dv.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", dv.String()), zap.Uint64("obtain-learner", peer.GetId()))
		}
		if peer.GetId() != dv.PeerID || peer.GetRole() != metapb.PeerRole_DemotingVoter {
			return false
		}
	}
	return true
}

// CheckSafety checks if the step meets the safety properties.
func (cpe ChangePeerV2Enter) CheckSafety(region *core.RegionInfo) error {
	inJointState, notInJointState := false, false
	for _, pl := range cpe.PromoteLearners {
		peer := region.GetStorePeer(pl.ToStore)
		if peer.GetId() != pl.PeerID {
			return errors.New("peer does not exist")
		}
		switch peer.GetRole() {
		case metapb.PeerRole_Learner:
			notInJointState = true
		case metapb.PeerRole_IncomingVoter:
			inJointState = true
		case metapb.PeerRole_Voter:
			return errors.New("peer already is a voter")
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
		case metapb.PeerRole_Voter:
			notInJointState = true
		case metapb.PeerRole_DemotingVoter:
			inJointState = true
		case metapb.PeerRole_Learner:
			return errors.New("peer already is a learner")
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
func (cpe ChangePeerV2Enter) Influence(opInfluence OpInfluence, region *core.RegionInfo) {}

// GetRequest get the ChangePeerV2 request
func (cpe ChangePeerV2Enter) GetRequest() *pdpb.ChangePeerV2 {
	changes := make([]*pdpb.ChangePeer, 0, len(cpe.PromoteLearners)+len(cpe.DemoteVoters))
	for _, pl := range cpe.PromoteLearners {
		changes = append(changes, &pdpb.ChangePeer{
			ChangeType: eraftpb.ConfChangeType_AddNode,
			Peer: &metapb.Peer{
				Id:      pl.PeerID,
				StoreId: pl.ToStore,
				Role:    metapb.PeerRole_Voter,
			},
		})
	}
	for _, dv := range cpe.DemoteVoters {
		changes = append(changes, &pdpb.ChangePeer{
			ChangeType: eraftpb.ConfChangeType_AddLearnerNode,
			Peer: &metapb.Peer{
				Id:      dv.PeerID,
				StoreId: dv.ToStore,
				Role:    metapb.PeerRole_Learner,
			},
		})
	}
	return &pdpb.ChangePeerV2{
		Changes: changes,
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
		peer := region.GetStoreVoter(pl.ToStore)
		if peer.GetId() != pl.PeerID || peer.GetRole() != metapb.PeerRole_Voter {
			return 0
		}
	}
	for _, dv := range cpl.DemoteVoters {
		if region.GetStorePeer(dv.PeerID) != nil && !dv.ConfVerChanged(region) {
			return 0
		}
	}
	return uint64(len(cpl.PromoteLearners) + len(cpl.DemoteVoters))
}

// IsFinish checks if current step is finished.
func (cpl ChangePeerV2Leave) IsFinish(region *core.RegionInfo) bool {
	for _, pl := range cpl.PromoteLearners {
		peer := region.GetStoreVoter(pl.ToStore)
		if peer != nil && peer.GetId() != pl.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", pl.String()), zap.Uint64("obtain-voter", peer.GetId()))
		}
		if peer.GetId() != pl.PeerID || peer.GetRole() != metapb.PeerRole_Voter {
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

// CheckSafety checks if the step meets the safety properties.
func (cpl ChangePeerV2Leave) CheckSafety(region *core.RegionInfo) error {
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
func (cpl ChangePeerV2Leave) Influence(opInfluence OpInfluence, region *core.RegionInfo) {}
