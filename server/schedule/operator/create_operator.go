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
	"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
	"go.uber.org/zap"
)

// CreateAddPeerOperator creates an operator that adds a new peer.
func CreateAddPeerOperator(desc string, ci ClusterInformer, region *core.RegionInfo, peer *metapb.Peer, kind OpKind) (*Operator, error) {
	return NewBuilder(desc, ci, region).
		AddPeer(peer).
		Build(kind)
}

// CreateDemoteVoterOperator creates an operator that demotes a voter
func CreateDemoteVoterOperator(desc string, ci ClusterInformer, region *core.RegionInfo, peer *metapb.Peer) (*Operator, error) {
	return NewBuilder(desc, ci, region).
		DemoteVoter(peer.GetStoreId()).
		Build(0)
}

// CreatePromoteLearnerOperator creates an operator that promotes a learner.
func CreatePromoteLearnerOperator(desc string, ci ClusterInformer, region *core.RegionInfo, peer *metapb.Peer) (*Operator, error) {
	return NewBuilder(desc, ci, region).
		PromoteLearner(peer.GetStoreId()).
		Build(0)
}

// CreateRemovePeerOperator creates an operator that removes a peer from region.
func CreateRemovePeerOperator(desc string, ci ClusterInformer, kind OpKind, region *core.RegionInfo, storeID uint64) (*Operator, error) {
	return NewBuilder(desc, ci, region).
		RemovePeer(storeID).
		Build(kind)
}

// CreateTransferLeaderOperator creates an operator that transfers the leader from a source store to a target store.
func CreateTransferLeaderOperator(desc string, ci ClusterInformer, region *core.RegionInfo, sourceStoreID uint64, targetStoreID uint64, targetStoreIDs []uint64, kind OpKind) (*Operator, error) {
	return NewBuilder(desc, ci, region, SkipOriginJointStateCheck).
		SetLeader(targetStoreID).
		SetLeaders(targetStoreIDs).
		Build(kind)
}

// CreateForceTransferLeaderOperator creates an operator that transfers the leader from a source store to a target store forcible.
func CreateForceTransferLeaderOperator(desc string, ci ClusterInformer, region *core.RegionInfo, sourceStoreID uint64, targetStoreID uint64, kind OpKind) (*Operator, error) {
	return NewBuilder(desc, ci, region, SkipOriginJointStateCheck).
		SetLeader(targetStoreID).
		EnableForceTargetLeader().
		Build(kind)
}

// CreateMoveRegionOperator creates an operator that moves a region to specified stores.
func CreateMoveRegionOperator(desc string, ci ClusterInformer, region *core.RegionInfo, kind OpKind, roles map[uint64]placement.PeerRoleType) (*Operator, error) {
	// construct the peers from roles
	peers := make(map[uint64]*metapb.Peer)
	for storeID, role := range roles {
		peers[storeID] = &metapb.Peer{
			StoreId: storeID,
			Role:    role.MetaPeerRole(),
		}
	}
	builder := NewBuilder(desc, ci, region).SetPeers(peers).SetExpectedRoles(roles)
	return builder.Build(kind)
}

// CreateMovePeerOperator creates an operator that replaces an old peer with a new peer.
func CreateMovePeerOperator(desc string, ci ClusterInformer, region *core.RegionInfo, kind OpKind, oldStore uint64, peer *metapb.Peer) (*Operator, error) {
	return NewBuilder(desc, ci, region).
		RemovePeer(oldStore).
		AddPeer(peer).
		Build(kind)
}

// CreateReplaceLeaderPeerOperator creates an operator that replaces an old peer with a new peer, and move leader from old store firstly.
func CreateReplaceLeaderPeerOperator(desc string, ci ClusterInformer, region *core.RegionInfo, kind OpKind, oldStore uint64, peer *metapb.Peer, leader *metapb.Peer) (*Operator, error) {
	return NewBuilder(desc, ci, region).
		RemovePeer(oldStore).
		AddPeer(peer).
		SetLeader(leader.GetStoreId()).
		Build(kind)
}

// CreateMoveLeaderOperator creates an operator that replaces an old leader with a new leader.
func CreateMoveLeaderOperator(desc string, ci ClusterInformer, region *core.RegionInfo, kind OpKind, oldStore uint64, peer *metapb.Peer) (*Operator, error) {
	return NewBuilder(desc, ci, region).
		RemovePeer(oldStore).
		AddPeer(peer).
		SetLeader(peer.GetStoreId()).
		Build(kind)
}

// CreateSplitRegionOperator creates an operator that splits a region.
func CreateSplitRegionOperator(desc string, region *core.RegionInfo, kind OpKind, policy pdpb.CheckPolicy, keys [][]byte) (*Operator, error) {
	if core.IsInJointState(region.GetPeers()...) {
		return nil, errors.Errorf("cannot split region which is in joint state")
	}

	step := SplitRegion{
		StartKey:  region.GetStartKey(),
		EndKey:    region.GetEndKey(),
		Policy:    policy,
		SplitKeys: keys,
	}
	brief := fmt.Sprintf("split: region %v use policy %s", region.GetID(), policy)
	if len(keys) > 0 {
		hexKeys := make([]string, len(keys))
		for i := range keys {
			hexKeys[i] = core.HexRegionKeyStr(logutil.RedactBytes(keys[i]))
		}
		brief += fmt.Sprintf(" and keys %v", hexKeys)
	}
	op := NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), kind|OpSplit, region.GetApproximateSize(), step)
	op.AdditionalInfos["region-start-key"] = core.HexRegionKeyStr(logutil.RedactBytes(region.GetStartKey()))
	op.AdditionalInfos["region-end-key"] = core.HexRegionKeyStr(logutil.RedactBytes(region.GetEndKey()))
	return op, nil
}

// CreateMergeRegionOperator creates an operator that merge two region into one.
func CreateMergeRegionOperator(desc string, ci ClusterInformer, source *core.RegionInfo, target *core.RegionInfo, kind OpKind) ([]*Operator, error) {
	if core.IsInJointState(source.GetPeers()...) || core.IsInJointState(target.GetPeers()...) {
		return nil, errors.Errorf("cannot merge regions which are in joint state")
	}

	peers := make(map[uint64]*metapb.Peer)
	for _, pa := range source.GetPeers() {
		pb := target.GetStorePeer(pa.GetStoreId())
		if pb != nil {
			peers[pa.GetStoreId()] = &metapb.Peer{
				StoreId: pa.GetStoreId(),
				Role:    pa.GetRole(),
			}
		}
	}

	var sourceSteps, targetSteps []OpStep
	sourceOriginPeers := make([]*metapb.Peer, 0, len(source.GetPeers())-len(peers))
	for _, peer := range source.GetPeers() {
		if _, ok := peers[peer.GetStoreId()]; !ok {
			sourceOriginPeers = append(sourceOriginPeers, peer)
		}
	}
	sort.Slice(sourceOriginPeers, func(i, j int) bool {
		if sourceOriginPeers[i].GetRole() == sourceOriginPeers[j].GetRole() {
			return sourceOriginPeers[i].GetStoreId() < sourceOriginPeers[j].GetStoreId()
		}
		return sourceOriginPeers[i].GetRole() < sourceOriginPeers[j].GetRole()
	})
	targetOriginPeers := make([]*metapb.Peer, 0, len(target.GetPeers())-len(peers))
	for _, peer := range target.GetPeers() {
		if _, ok := peers[peer.GetStoreId()]; !ok {
			targetOriginPeers = append(targetOriginPeers, peer)
		}
	}
	sort.Slice(targetOriginPeers, func(i, j int) bool {
		if targetOriginPeers[i].GetRole() == targetOriginPeers[j].GetRole() {
			return targetOriginPeers[i].GetStoreId() < targetOriginPeers[j].GetStoreId()
		}
		return targetOriginPeers[i].GetRole() < targetOriginPeers[j].GetRole()
	})
	if len(targetOriginPeers) != len(sourceOriginPeers) {
		return nil, errors.Errorf("the amount of peers is not matched")
	}
	index := 0
	for index < len(targetOriginPeers) {
		stores := make([]*core.StoreInfo, 0)
		isTargetStore := make(map[uint64]struct{})
		for {
			if targetOriginPeers[index].GetRole() != sourceOriginPeers[index].GetRole() {
				return nil, errors.Errorf("the peer's role is not matched")
			}
			if targetOriginPeers[index].GetStoreId() == sourceOriginPeers[index].GetStoreId() {
				peers[targetOriginPeers[index].GetStoreId()] = targetOriginPeers[index]
			}
			store1 := ci.GetBasicCluster().GetStore(targetOriginPeers[index].GetStoreId()).Clone()
			stores = append(stores, store1)
			isTargetStore[store1.GetID()] = struct{}{}
			store2 := ci.GetBasicCluster().GetStore(sourceOriginPeers[index].GetStoreId()).Clone()
			stores = append(stores, store2)
			if index+1 < len(targetOriginPeers) && targetOriginPeers[index+1].GetRole() == targetOriginPeers[index].GetRole() {
				index++
			} else {
				break
			}
		}
		sort.Slice(stores, func(i, j int) bool {
			iOp := int64(0)
			iDelta := 0.
			if _, ok := isTargetStore[stores[i].GetID()]; ok {
				iOp, iDelta = source.GetApproximateSize(), 0.1
			}
			jOp := int64(0)
			jDelta := 0.
			if _, ok := isTargetStore[stores[j].GetID()]; ok {
				jOp, jDelta = source.GetApproximateSize(), 0.1
			}
			return stores[i].RegionScore(ci.GetOpts().GetRegionScoreFormulaVersion(), ci.GetOpts().GetHighSpaceRatio(), ci.GetOpts().GetLowSpaceRatio(), iOp)-iDelta <
				stores[j].RegionScore(ci.GetOpts().GetRegionScoreFormulaVersion(), ci.GetOpts().GetHighSpaceRatio(), ci.GetOpts().GetLowSpaceRatio(), jOp)-jDelta
		})
		storesIndex := 0
		for len(peers) < index {
			peers[stores[storesIndex].GetID()] = &metapb.Peer{
				StoreId: stores[storesIndex].GetID(),
				Role:    targetOriginPeers[index].GetRole(),
			}
			storesIndex++
		}

		index++
	}
	sourceMatchOp, err := NewBuilder("", ci, source).
		SetPeers(peers).
		Build(kind)
	if err != nil {
		return nil, err
	}
	sourceSteps = append(sourceSteps, sourceMatchOp.steps...)
	sourceKind := sourceMatchOp.Kind()

	targetMatchOp, err := NewBuilder("", ci, target).
		SetPeers(peers).
		Build(kind)
	if err != nil {
		return nil, err
	}
	targetSteps = append(targetSteps, targetMatchOp.steps...)
	targetKind := targetMatchOp.Kind()

	sourceSteps = append(sourceSteps, MergeRegion{
		FromRegion: source.GetMeta(),
		ToRegion:   target.GetMeta(),
		Peers:      peers,
		IsPassive:  false,
	})
	targetSteps = append(targetSteps, MergeRegion{
		FromRegion: source.GetMeta(),
		ToRegion:   target.GetMeta(),
		Peers:      peers,
		IsPassive:  true,
	})
	brief := fmt.Sprintf("merge: region %v to %v", source.GetID(), target.GetID())
	opSource := NewOperator(desc, brief, source.GetID(), source.GetRegionEpoch(), sourceKind|OpMerge, source.GetApproximateSize(), sourceSteps...)
	opTarget := NewOperator(desc, brief, target.GetID(), target.GetRegionEpoch(), targetKind|OpMerge, target.GetApproximateSize(), targetSteps...)

	return []*Operator{opTarget, opSource}, nil
}

// CreateScatterRegionOperator creates an operator that scatters the specified region.
func CreateScatterRegionOperator(desc string, ci ClusterInformer, origin *core.RegionInfo, targetPeers map[uint64]*metapb.Peer, targetLeader uint64) (*Operator, error) {
	// randomly pick a leader.
	var ids []uint64
	for id, peer := range targetPeers {
		if !core.IsLearner(peer) {
			ids = append(ids, id)
		}
	}
	var leader uint64
	if len(ids) > 0 {
		leader = ids[rand.Intn(len(ids))]
	}
	if targetLeader != 0 {
		leader = targetLeader
	}
	return NewBuilder(desc, ci, origin).
		SetPeers(targetPeers).
		SetLeader(leader).
		EnableLightWeight().
		// EnableForceTargetLeader in order to ignore the leader schedule limit
		EnableForceTargetLeader().
		Build(0)
}

// OpDescLeaveJointState is the expected desc for LeaveJointStateOperator.
const OpDescLeaveJointState = "leave-joint-state"

// CreateLeaveJointStateOperator creates an operator that let region leave joint state.
func CreateLeaveJointStateOperator(desc string, ci ClusterInformer, origin *core.RegionInfo) (*Operator, error) {
	b := NewBuilder(desc, ci, origin, SkipOriginJointStateCheck)

	if b.err == nil && !core.IsInJointState(origin.GetPeers()...) {
		b.err = errors.Errorf("cannot build leave joint state operator for region which is not in joint state")
	}

	if b.err != nil {
		return nil, b.err
	}

	// prepareBuild
	b.toDemote = newPeersMap()
	b.toPromote = newPeersMap()
	for _, o := range b.originPeers {
		switch o.GetRole() {
		case metapb.PeerRole_IncomingVoter:
			b.toPromote.Set(o)
		case metapb.PeerRole_DemotingVoter:
			b.toDemote.Set(o)
		}
	}

	leader := b.originPeers[b.originLeaderStoreID]
	if leader == nil || !b.allowLeader(leader, true) {
		b.targetLeaderStoreID = 0
	} else {
		b.targetLeaderStoreID = b.originLeaderStoreID
	}

	b.currentPeers, b.currentLeaderStoreID = b.originPeers.Copy(), b.originLeaderStoreID
	b.peerAddStep = make(map[uint64]int)
	brief := b.brief()

	// buildStepsWithJointConsensus
	var kind OpKind

	b.setTargetLeaderIfNotExist()
	if b.targetLeaderStoreID == 0 {
		// Because the demote leader will be rejected by TiKV,
		// when the target leader cannot be found, we need to force a target to be found.
		b.forceTargetLeader = true
		b.setTargetLeaderIfNotExist()
	}

	if b.targetLeaderStoreID == 0 {
		log.Error(
			"unable to find target leader",
			zap.Reflect("region", origin),
			errs.ZapError(errs.ErrCreateOperator.FastGenByArgs("no target leader")))
		b.originLeaderStoreID = 0
	} else if b.originLeaderStoreID != b.targetLeaderStoreID {
		kind |= OpLeader
	}

	b.execChangePeerV2(false, true)
	return NewOperator(b.desc, brief, b.regionID, b.regionEpoch, kind, origin.GetApproximateSize(), b.steps...), nil
}
