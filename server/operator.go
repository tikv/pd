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

package server

import (
	"fmt"
	"time"

	"github.com/ngaut/log"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	maxOperatorWaitTime = 5 * time.Minute
)

// Operator is an interface to schedule region.
type Operator interface {
	GetRegionID() uint64
	GetResourceKind() ResourceKind
	Do(region *regionInfo) (*pdpb.RegionHeartbeatResponse, bool)
}

type regionOperator struct {
	Region *regionInfo `json:"region"`
	Start  time.Time   `json:"start"`
	End    time.Time   `json:"end"`
	Index  int         `json:"index"`
	Ops    []Operator  `json:"operators"`
}

func newRegionOperator(region *regionInfo, ops ...Operator) *regionOperator {
	// Do some check here, just fatal because it must be bug.
	if len(ops) == 0 {
		log.Fatal("new region operator with no ops")
	}
	kind := ops[0].GetResourceKind()
	for _, op := range ops {
		if op.GetResourceKind() != kind {
			log.Fatalf("new region operator with ops of different kinds %v and %v", op.GetResourceKind(), kind)
		}
	}

	return &regionOperator{
		Region: region,
		Start:  time.Now(),
		Ops:    ops,
	}
}

func (op *regionOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *regionOperator) GetRegionID() uint64 {
	return op.Region.GetId()
}

func (op *regionOperator) GetResourceKind() ResourceKind {
	return op.Ops[0].GetResourceKind()
}

func (op *regionOperator) Do(region *regionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	if time.Since(op.Start) > maxOperatorWaitTime {
		log.Errorf("%s : operator timeout", op)
		return nil, true
	}

	// Update region.
	op.Region = region.clone()

	// If an operator is not finished, do it.
	for ; op.Index < len(op.Ops); op.Index++ {
		if res, finished := op.Ops[op.Index].Do(region); !finished {
			return res, false
		}
	}

	op.End = time.Now()
	return nil, true
}

type changePeerOperator struct {
	Name       string           `json:"name"`
	RegionID   uint64           `json:"region_id"`
	ChangePeer *pdpb.ChangePeer `json:"change_peer"`
}

func newAddPeerOperator(regionID uint64, peer *metapb.Peer) *changePeerOperator {
	return &changePeerOperator{
		Name:     "add_peer",
		RegionID: regionID,
		ChangePeer: &pdpb.ChangePeer{
			ChangeType: raftpb.ConfChangeType_AddNode.Enum(),
			Peer:       peer,
		},
	}
}

func newRemovePeerOperator(regionID uint64, peer *metapb.Peer) *changePeerOperator {
	return &changePeerOperator{
		Name:     "remove_peer",
		RegionID: regionID,
		ChangePeer: &pdpb.ChangePeer{
			ChangeType: raftpb.ConfChangeType_RemoveNode.Enum(),
			Peer:       peer,
		},
	}
}

func (op *changePeerOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *changePeerOperator) GetRegionID() uint64 {
	return op.RegionID
}

func (op *changePeerOperator) GetResourceKind() ResourceKind {
	return storageKind
}

func (op *changePeerOperator) Do(region *regionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	// Check if operator is finished.
	peer := op.ChangePeer.GetPeer()
	switch op.ChangePeer.GetChangeType() {
	case raftpb.ConfChangeType_AddNode:
		if region.GetPendingPeer(peer.GetId()) != nil {
			// Peer is added but not finished.
			return nil, false
		}
		if region.GetPeer(peer.GetId()) != nil {
			// Peer is added and finished.
			return nil, true
		}
	case raftpb.ConfChangeType_RemoveNode:
		if region.GetPeer(peer.GetId()) == nil {
			// Peer is removed.
			return nil, true
		}
	}

	log.Infof("%s %s", op, region)

	res := &pdpb.RegionHeartbeatResponse{
		ChangePeer: op.ChangePeer,
	}
	return res, false
}

type transferLeaderOperator struct {
	Name      string       `json:"name"`
	RegionID  uint64       `json:"region_id"`
	OldLeader *metapb.Peer `json:"old_leader"`
	NewLeader *metapb.Peer `json:"new_leader"`
}

func newTransferLeaderOperator(regionID uint64, oldLeader, newLeader *metapb.Peer) *transferLeaderOperator {
	return &transferLeaderOperator{
		Name:      "transfer_leader",
		RegionID:  regionID,
		OldLeader: oldLeader,
		NewLeader: newLeader,
	}
}

func (op *transferLeaderOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *transferLeaderOperator) GetRegionID() uint64 {
	return op.RegionID
}

func (op *transferLeaderOperator) GetResourceKind() ResourceKind {
	return leaderKind
}

func (op *transferLeaderOperator) Do(region *regionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	// Check if operator is finished.
	if region.Leader.GetId() == op.NewLeader.GetId() {
		return nil, true
	}

	log.Infof("%s %v", op, region)

	res := &pdpb.RegionHeartbeatResponse{
		TransferLeader: &pdpb.TransferLeader{
			Peer: op.NewLeader,
		},
	}
	return res, false
}
