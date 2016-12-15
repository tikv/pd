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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	maxOperatorWaitTime = 5 * time.Minute
)

var (
	errOperatorTimeout = errors.New("operator timeout")
)

// Operator is an interface to schedule region.
type Operator interface {
	GetRegionID() uint64
	GetResourceKind() ResourceKind
	Do(region *regionInfo) (bool, *pdpb.RegionHeartbeatResponse, error)
}

type regionOperator struct {
	Region *regionInfo `json:"region"`
	Index  int         `json:"index"`
	Start  time.Time   `json:"start"`
	End    time.Time   `json:"end"`
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
			log.Fatal("new region operator with ops of different kinds")
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

func (op *regionOperator) Do(region *regionInfo) (bool, *pdpb.RegionHeartbeatResponse, error) {
	if time.Since(op.Start) > maxOperatorWaitTime {
		return false, nil, errors.Trace(errOperatorTimeout)
	}
	if err := checkStaleRegion(op.Region.Region, region.Region); err != nil {
		return false, nil, errors.Trace(err)
	}
	op.Region = region.clone()

	// If an operator is not finished, do it.
	for ; op.Index < len(op.Ops); op.Index++ {
		finished, res, err := op.Ops[op.Index].Do(region)
		if !finished {
			return finished, res, err
		}
	}

	return true, nil, nil
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

func (op *changePeerOperator) Do(region *regionInfo) (bool, *pdpb.RegionHeartbeatResponse, error) {
	// Check if operator is finished.
	peer := op.ChangePeer.GetPeer()
	switch op.ChangePeer.GetChangeType() {
	case raftpb.ConfChangeType_AddNode:
		if region.GetPendingPeer(peer.GetId()) != nil {
			// Peer is added but not finished.
			return false, nil, nil
		}
		if region.GetPeer(peer.GetId()) != nil {
			// Peer is added and finished.
			return true, nil, nil
		}
	case raftpb.ConfChangeType_RemoveNode:
		if region.GetPeer(peer.GetId()) == nil {
			// Peer is removed.
			return true, nil, nil
		}
	}

	log.Infof("%s %s", op, region)

	res := &pdpb.RegionHeartbeatResponse{
		ChangePeer: op.ChangePeer,
	}
	return false, res, nil
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

func (op *transferLeaderOperator) Do(region *regionInfo) (bool, *pdpb.RegionHeartbeatResponse, error) {
	// Check if operator is finished.
	if region.Leader.GetId() == op.NewLeader.GetId() {
		return true, nil, nil
	}

	log.Infof("%s %v", op, region)

	res := &pdpb.RegionHeartbeatResponse{
		TransferLeader: &pdpb.TransferLeader{
			Peer: op.NewLeader,
		},
	}
	return false, res, nil
}
