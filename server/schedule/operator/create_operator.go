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
	"encoding/hex"
	"fmt"
	"math/rand"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/v4/server/core"
)

// CreateAddPeerOperator creates an operator that adds a new peer.
func CreateAddPeerOperator(desc string, cluster Cluster, region *core.RegionInfo, peer *metapb.Peer, kind OpKind) (*Operator, error) {
	return NewBuilder(desc, cluster, region).
		AddPeer(peer).
		Build(kind)
}

// CreatePromoteLearnerOperator creates an operator that promotes a learner.
func CreatePromoteLearnerOperator(desc string, cluster Cluster, region *core.RegionInfo, peer *metapb.Peer) (*Operator, error) {
	return NewBuilder(desc, cluster, region).
		PromoteLearner(peer.GetStoreId()).
		Build(0)
}

// CreateRemovePeerOperator creates an operator that removes a peer from region.
func CreateRemovePeerOperator(desc string, cluster Cluster, kind OpKind, region *core.RegionInfo, storeID uint64) (*Operator, error) {
	return NewBuilder(desc, cluster, region).
		RemovePeer(storeID).
		Build(kind)
}

// CreateTransferLeaderOperator creates an operator that transfers the leader from a source store to a target store.
func CreateTransferLeaderOperator(desc string, cluster Cluster, region *core.RegionInfo, sourceStoreID uint64, targetStoreID uint64, kind OpKind) (*Operator, error) {
	return NewBuilder(desc, cluster, region).
		SetLeader(targetStoreID).
		Build(kind)
}

// CreateMoveRegionOperator creates an operator that moves a region to specified stores.
func CreateMoveRegionOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, peers map[uint64]*metapb.Peer) (*Operator, error) {
	return NewBuilder(desc, cluster, region).
		SetPeers(peers).
		Build(kind)
}

// CreateMovePeerOperator creates an operator that replaces an old peer with a new peer.
func CreateMovePeerOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, oldStore uint64, peer *metapb.Peer) (*Operator, error) {
	return NewBuilder(desc, cluster, region).
		RemovePeer(oldStore).
		AddPeer(peer).
		Build(kind)
}

// CreateMoveLeaderOperator creates an operator that replaces an old leader with a new leader.
func CreateMoveLeaderOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, oldStore uint64, peer *metapb.Peer) (*Operator, error) {
	return NewBuilder(desc, cluster, region).
		RemovePeer(oldStore).
		AddPeer(peer).
		SetLeader(peer.GetStoreId()).
		Build(kind)
}

// CreateSplitRegionOperator creates an operator that splits a region.
func CreateSplitRegionOperator(desc string, region *core.RegionInfo, kind OpKind, policy pdpb.CheckPolicy, keys [][]byte) *Operator {
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
			hexKeys[i] = hex.EncodeToString(keys[i])
		}
		brief += fmt.Sprintf(" and keys %v", hexKeys)
	}
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), kind|OpSplit, step)
}

// CreateMergeRegionOperator creates an operator that merge two region into one.
func CreateMergeRegionOperator(desc string, cluster Cluster, source *core.RegionInfo, target *core.RegionInfo, kind OpKind) ([]*Operator, error) {
	var steps []OpStep
	if !isRegionMatch(source, target) {
		peers := make(map[uint64]*metapb.Peer)
		for _, p := range target.GetPeers() {
			peers[p.GetStoreId()] = &metapb.Peer{
				StoreId:   p.GetStoreId(),
				IsLearner: p.GetIsLearner(),
			}
		}
		matchOp, err := NewBuilder("", cluster, source).
			SetPeers(peers).
			Build(kind)
		if err != nil {
			return nil, err
		}

		steps = append(steps, matchOp.steps...)
		kind = matchOp.Kind()
	}

	steps = append(steps, MergeRegion{
		FromRegion: source.GetMeta(),
		ToRegion:   target.GetMeta(),
		IsPassive:  false,
	})

	brief := fmt.Sprintf("merge: region %v to %v", source.GetID(), target.GetID())
	op1 := NewOperator(desc, brief, source.GetID(), source.GetRegionEpoch(), kind|OpMerge, steps...)
	op2 := NewOperator(desc, brief, target.GetID(), target.GetRegionEpoch(), kind|OpMerge, MergeRegion{
		FromRegion: source.GetMeta(),
		ToRegion:   target.GetMeta(),
		IsPassive:  true,
	})

	return []*Operator{op1, op2}, nil
}

func isRegionMatch(a, b *core.RegionInfo) bool {
	if len(a.GetPeers()) != len(b.GetPeers()) {
		return false
	}
	for _, pa := range a.GetPeers() {
		pb := b.GetStorePeer(pa.GetStoreId())
		if pb == nil || pb.GetIsLearner() != pa.GetIsLearner() {
			return false
		}
	}
	return true
}

// CreateScatterRegionOperator creates an operator that scatters the specified region.
func CreateScatterRegionOperator(desc string, cluster Cluster, origin *core.RegionInfo, targetPeers map[uint64]*metapb.Peer) (*Operator, error) {
	// randomly pick a leader.
	var ids []uint64
	for id := range targetPeers {
		ids = append(ids, id)
	}
	var leader uint64
	if len(ids) > 0 {
		leader = ids[rand.Intn(len(ids))]
	}
	return NewBuilder(desc, cluster, origin).
		SetPeers(targetPeers).
		SetLeader(leader).
		SetLightWeight().
		Build(0)
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
