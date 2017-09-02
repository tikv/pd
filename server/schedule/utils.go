// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

// CreateRemovePeerOperator creates a RegionOperator that removes a peer from
// region. It prevents removing leader by tranfer its leadership first.
func CreateRemovePeerOperator(region *core.RegionInfo, peer *metapb.Peer) Operator {
	removePeer := NewRemovePeerOperator(region.GetId(), peer)
	if region.Leader != nil && region.Leader.GetId() == peer.GetId() {
		if follower := region.GetFollower(); follower != nil {
			transferLeader := NewTransferLeaderOperator(region.GetId(), region.Leader, follower)
			return NewRegionOperator(region, core.RegionKind, transferLeader, removePeer)
		}
		return nil
	}
	return NewRegionOperator(region, core.RegionKind, removePeer)
}

// CreateAddPeerOperator creates a RegionOperator that adds a peer to region.
func CreateAddPeerOperator(region *core.RegionInfo, peer *metapb.Peer) Operator {
	addPeer := NewAddPeerOperator(region.GetId(), peer)
	return NewRegionOperator(region, core.RegionKind, addPeer)
}

// CreateMovePeerOperator creates a RegionOperator that replaces an old peer with
// a new peer. It prevents removing leader by transfer its leadership first.
func CreateMovePeerOperator(region *core.RegionInfo, kind core.ResourceKind, oldPeer, newPeer *metapb.Peer) Operator {
	addPeer := NewAddPeerOperator(region.GetId(), newPeer)
	removePeer := NewRemovePeerOperator(region.GetId(), oldPeer)
	if region.Leader != nil && region.Leader.GetId() == oldPeer.GetId() {
		newLeader := newPeer
		if follower := region.GetFollower(); follower != nil {
			newLeader = follower
		}
		transferLeader := NewTransferLeaderOperator(region.GetId(), region.Leader, newLeader)
		return NewRegionOperator(region, kind, addPeer, transferLeader, removePeer)
	}
	return NewRegionOperator(region, kind, addPeer, removePeer)
}
