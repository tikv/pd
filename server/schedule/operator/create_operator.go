// Copyright 2019 PingCAP, Inc.
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
	"errors"
	"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule/opt"
	"go.uber.org/zap"
)

// CreateAddPeerOperator creates an operator that adds a new peer.
func CreateAddPeerOperator(desc string, region *core.RegionInfo, peer *metapb.Peer, kind OpKind) *Operator {
	steps := CreateAddPeerSteps(peer)
	brief := fmt.Sprintf("add peer: store %v", peer.StoreId)
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), kind|OpRegion, steps...)
}

// CreatePromoteLearnerOperator creates an operator that promotes a learner.
func CreatePromoteLearnerOperator(desc string, region *core.RegionInfo, peer *metapb.Peer) *Operator {
	step := PromoteLearner{
		ToStore: peer.GetStoreId(),
		PeerID:  peer.GetId(),
	}
	brief := fmt.Sprintf("promote learner: store %v", peer.GetStoreId())
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), OpRegion, step)
}

// CreateRemovePeerOperator creates an operator that removes a peer from region.
func CreateRemovePeerOperator(desc string, cluster Cluster, kind OpKind, region *core.RegionInfo, storeID uint64) (*Operator, error) {
	removeKind, steps, err := removePeerSteps(cluster, region, storeID, getRegionFollowerIDs(region))
	if err != nil {
		return nil, err
	}
	brief := fmt.Sprintf("rm peer: store %v", storeID)
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), removeKind|kind, steps...), nil
}

// CreateAddPeerSteps creates an OpStep list that add a new peer.
func CreateAddPeerSteps(newPeer *metapb.Peer) []OpStep {
	if newPeer.IsLearner {
		return []OpStep{AddLearner{ToStore: newPeer.StoreId, PeerID: newPeer.Id}}
	}
	return []OpStep{
		AddLearner{ToStore: newPeer.StoreId, PeerID: newPeer.Id},
		PromoteLearner{ToStore: newPeer.StoreId, PeerID: newPeer.Id},
	}
}

// CreateAddLightPeerSteps creates an OpStep list that add a new peer without considering the influence.
func CreateAddLightPeerSteps(newStore uint64, peerID uint64) []OpStep {
	st := []OpStep{
		AddLightLearner{ToStore: newStore, PeerID: peerID},
		PromoteLearner{ToStore: newStore, PeerID: peerID},
	}
	return st
}

// CreateTransferLeaderOperator creates an operator that transfers the leader from a source store to a target store.
func CreateTransferLeaderOperator(desc string, region *core.RegionInfo, sourceStoreID uint64, targetStoreID uint64, kind OpKind) *Operator {
	step := TransferLeader{FromStore: sourceStoreID, ToStore: targetStoreID}
	brief := fmt.Sprintf("transfer leader: store %v to %v", sourceStoreID, targetStoreID)
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), kind|OpLeader, step)
}

// CreateMoveRegionOperator creates an operator that moves a region to specified stores.
func CreateMoveRegionOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, storeIDs map[uint64]struct{}) (*Operator, error) {
	mvkind, steps, err := moveRegionSteps(cluster, region, storeIDs)
	if err != nil {
		return nil, err
	}
	kind |= mvkind
	brief := fmt.Sprintf("mv region: stores %v to %v", u64Set(region.GetStoreIds()), u64Set(storeIDs))
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), kind, steps...), nil
}

// moveRegionSteps returns steps to move a region to specific stores.
//
// The first store in the slice will not have RejectLeader label.
// If all of the stores have RejectLeader label, it returns an error.
func moveRegionSteps(cluster Cluster, region *core.RegionInfo, stores map[uint64]struct{}) (OpKind, []OpStep, error) {
	storeIDs := make([]uint64, 0, len(stores))
	for id := range stores {
		storeIDs = append(storeIDs, id)
	}

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
// do not need to move if there is one, otherwise the first suitable new added follower.
// New peers will be added in the same order in storeIDs.
// NOTE: orderedMoveRegionSteps does NOT check duplicate stores.
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
		addPeerSteps = append(addPeerSteps, CreateAddPeerSteps(peer))
		kind |= OpRegion
	}

	// Transferring leader to a new added follower may be refused by TiKV.
	// Ref: https://github.com/tikv/tikv/issues/3819
	// So, the new leader should be a follower that do not need to move if there is one,
	// otherwise the first suitable new added follower.
	orderedStoreIDs := append(oldStores, newStores...)

	// Remove redundant peers.
	var rmPeerSteps [][]OpStep
	// Transfer leader as late as possible to prevent transferring to a new added follower.
	var mvLeaderSteps []OpStep
	for _, peer := range region.GetPeers() {
		id := peer.GetStoreId()
		if _, ok := targetStores[id]; ok {
			continue
		}
		if region.GetLeader().GetStoreId() == id {
			tlkind, tlsteps, err := transferLeaderToSuitableSteps(cluster, id, orderedStoreIDs)
			if err != nil {
				log.Debug("move region to stores failed", zap.Uint64("region-id", region.GetID()), zap.Uint64s("store-ids", orderedStoreIDs), zap.Error(err))
				return kind, nil, err
			}
			mvLeaderSteps = append(tlsteps, RemovePeer{FromStore: id})
			kind |= tlkind
		} else {
			rmPeerSteps = append(rmPeerSteps, []OpStep{RemovePeer{FromStore: id}})
		}
		kind |= OpRegion
	}

	// Interleaving makes the operator add and remove peers one by one, so that there won't have
	// too many additional peers if the operator fails in the half.
	hint := len(addPeerSteps)*2 + len(rmPeerSteps) + len(mvLeaderSteps)
	steps := interleaveStepGroups(addPeerSteps, rmPeerSteps, hint)

	steps = append(steps, mvLeaderSteps...)

	return kind, steps, nil
}

// interleaveStepGroups interleaves two slice of step groups. For example:
//
//  a = [[opA1, opA2], [opA3], [opA4, opA5, opA6]]
//  b = [[opB1], [opB2], [opB3, opB4], [opB5, opB6]]
//  c = interleaveStepGroups(a, b, 0)
//  c == [opA1, opA2, opB1, opA3, opB2, opA4, opA5, opA6, opB3, opB4, opB5, opB6]
//
// sizeHint is a hint for the capacity of returned slice.
func interleaveStepGroups(a, b [][]OpStep, sizeHint int) []OpStep {
	steps := make([]OpStep, 0, sizeHint)
	i, j := 0, 0
	for ; i < len(a) && j < len(b); i, j = i+1, j+1 {
		steps = append(steps, a[i]...)
		steps = append(steps, b[j]...)
	}
	for ; i < len(a); i++ {
		steps = append(steps, a[i]...)
	}
	for ; j < len(b); j++ {
		steps = append(steps, b[j]...)
	}
	return steps
}

// CreateMovePeerOperator creates an operator that replaces an old peer with a new peer.
func CreateMovePeerOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, oldStore uint64, peer *metapb.Peer) (*Operator, error) {
	removeKind, steps, err := removePeerSteps(cluster, region, oldStore, append(getRegionFollowerIDs(region), peer.StoreId))
	if err != nil {
		return nil, err
	}
	st := CreateAddPeerSteps(peer)
	steps = append(st, steps...)
	brief := fmt.Sprintf("mv peer: store %v to %v", oldStore, peer.StoreId)
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), removeKind|kind|OpRegion, steps...), nil
}

// CreateOfflinePeerOperator creates an operator that replaces an old peer with a new peer when offline a store.
func CreateOfflinePeerOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, oldStore uint64, peer *metapb.Peer) (*Operator, error) {
	k, steps, err := transferLeaderStep(cluster, region, oldStore, getRegionFollowerIDs(region))
	if err != nil {
		return nil, err
	}
	kind |= k
	st := CreateAddPeerSteps(peer)
	steps = append(steps, st...)
	steps = append(steps, RemovePeer{FromStore: oldStore})
	brief := fmt.Sprintf("mv peer: store %v to %v", oldStore, peer.StoreId)
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), kind|OpRegion, steps...), nil
}

// CreateMoveLeaderOperator creates an operator that replaces an old leader with a new leader.
func CreateMoveLeaderOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, oldStore uint64, peer *metapb.Peer) (*Operator, error) {
	removeKind, steps, err := removePeerSteps(cluster, region, oldStore, []uint64{peer.StoreId})
	if err != nil {
		return nil, err
	}
	st := CreateAddPeerSteps(peer)
	steps = append(st, steps...)
	brief := fmt.Sprintf("mv leader: store %v to %v", oldStore, peer.StoreId)
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), removeKind|kind|OpLeader|OpRegion, steps...), nil
}

// CreateSplitRegionOperator creates an operator that splits a region.
func CreateSplitRegionOperator(desc string, region *core.RegionInfo, kind OpKind, policy pdpb.CheckPolicy, keys [][]byte) *Operator {
	step := SplitRegion{
		StartKey:  region.GetStartKey(),
		EndKey:    region.GetEndKey(),
		Policy:    policy,
		SplitKeys: keys,
	}
	brief := fmt.Sprintf("split: region %v", region.GetID())
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), kind, step)
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
	kind, steps, err = transferLeaderStep(cluster, region, storeID, followerIDs)
	if err != nil {
		return
	}

	steps = append(steps, RemovePeer{FromStore: storeID})
	kind |= OpRegion
	return
}

func transferLeaderStep(cluster Cluster, region *core.RegionInfo, storeID uint64, followerIDs []uint64) (kind OpKind, steps []OpStep, err error) {
	if region.GetLeader() != nil && region.GetLeader().GetStoreId() == storeID {
		kind, steps, err = transferLeaderToSuitableSteps(cluster, storeID, followerIDs)
		if err != nil {
			log.Debug("failed to create transfer leader step", zap.Uint64("region-id", region.GetID()), zap.Error(err))
			return
		}
	}
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
			log.Debug("nil store", zap.Uint64("store-id", id))
		}
	}
	return -1, 0
}

// transferLeaderToSuitableSteps returns the first suitable store to become region leader.
// Returns an error if there is no suitable store.
func transferLeaderToSuitableSteps(cluster Cluster, leaderID uint64, storeIDs []uint64) (OpKind, []OpStep, error) {
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

	brief := fmt.Sprintf("merge: region %v to %v", source.GetID(), target.GetID())
	op1 := NewOperator(desc, brief, source.GetID(), source.GetRegionEpoch(), kinds|kind|OpMerge, steps...)
	op2 := NewOperator(desc, brief, target.GetID(), target.GetRegionEpoch(), kinds|kind|OpMerge, MergeRegion{
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

	targetLeader := target.GetLeader().GetStoreId()
	if targetLeader == 0 {
		return kind, nil, errors.New("target does not have a leader")
	}

	// The target leader store must not have RejectLeader.
	targetStores := make([]uint64, 1, len(targetPeers))
	targetStores[0] = targetLeader

	for _, peer := range targetPeers {
		id := peer.GetStoreId()
		if id != targetLeader {
			targetStores = append(targetStores, id)
		}
	}

	return orderedMoveRegionSteps(cluster, source, targetStores)
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
		st := CreateAddLightPeerSteps(targetLeaderPeer.GetStoreId(), targetLeaderPeer.GetId())
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
			st := CreateAddLightPeerSteps(peer.GetStoreId(), peer.GetId())
			st = append(st, RemovePeer{FromStore: replacedPeers[j].GetStoreId()})
			deferSteps = append(deferSteps, st...)
			kind |= OpRegion | OpLeader
			continue
		}
		st := CreateAddLightPeerSteps(peer.GetStoreId(), peer.GetId())
		steps = append(steps, st...)
		steps = append(steps, RemovePeer{FromStore: replacedPeers[j].GetStoreId()})
		kind |= OpRegion
	}

	steps = append(steps, deferSteps...)

	targetStores := make([]uint64, len(targetPeers))
	for i := range targetPeers {
		targetStores[i] = targetPeers[i].GetStoreId()
	}
	sort.Sort(u64Slice(targetStores))
	brief := fmt.Sprintf("scatter region: stores %v to %v", u64Set(origin.GetStoreIds()), targetStores)
	op := NewOperator(desc, brief, origin.GetID(), origin.GetRegionEpoch(), kind, steps...)
	return op
}

type u64Slice []uint64

func (s u64Slice) Len() int {
	return len(s)
}

func (s u64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s u64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

type u64Set map[uint64]struct{}

func (s u64Set) String() string {
	v := make([]uint64, 0, len(s))
	for x := range s {
		v = append(v, x)
	}
	sort.Sort(u64Slice(v))
	return fmt.Sprintf("%v", v)
}
