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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raftpb"
)

// Operator is the interface to do some operations.
type Operator interface {
	// Do does the operator, if finished then return true.
	Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.RegionHeartbeatResponse, error)
	// Check checks whether the operator has been finished.
	Check(region *metapb.Region, leader *metapb.Peer) (bool, error)
}

// BalanceOperator is used to do region balance.
type BalanceOperator struct {
	index  int
	ops    []Operator
	region *metapb.Region
}

func newBalanceOperator(region *metapb.Region, ops ...Operator) *BalanceOperator {
	return &BalanceOperator{
		index:  0,
		ops:    ops,
		region: region,
	}
}

// Check implements Operator.Check interface.
func (bo *BalanceOperator) Check(region *metapb.Region, leader *metapb.Peer) (bool, error) {
	if bo.index >= len(bo.ops) {
		return true, nil
	}

	err := checkStaleRegion(bo.region, region)
	if err != nil {
		return false, errors.Trace(err)
	}

	bo.region = cloneRegion(region)

	return false, nil
}

// Do implements Operator.Do interface.
func (bo *BalanceOperator) Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.RegionHeartbeatResponse, error) {
	ok, err := bo.Check(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		return true, nil, nil
	}

	finished, res, err := bo.ops[bo.index].Do(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if !finished {
		return false, res, nil
	}

	bo.index++

	ok, err = bo.Check(region, leader)
	return ok, nil, errors.Trace(err)
}

// ChangePeerOperator is used to do peer change.
type ChangePeerOperator struct {
	changePeer *pdpb.ChangePeer
}

func newAddPeerOperator(peer *metapb.Peer) *ChangePeerOperator {
	return &ChangePeerOperator{
		changePeer: &pdpb.ChangePeer{
			ChangeType: raftpb.ConfChangeType_AddNode.Enum(),
			Peer:       peer,
		},
	}
}

func newRemovePeerOperator(peer *metapb.Peer) *ChangePeerOperator {
	return &ChangePeerOperator{
		changePeer: &pdpb.ChangePeer{
			ChangeType: raftpb.ConfChangeType_RemoveNode.Enum(),
			Peer:       peer,
		},
	}
}

// Check implements Operator.Check interface.
func (co *ChangePeerOperator) Check(region *metapb.Region, leader *metapb.Peer) (bool, error) {
	if region == nil {
		return false, errors.New("invalid region")
	}
	if leader == nil {
		return false, errors.New("invalid leader peer")
	}

	if co.changePeer.GetChangeType() == raftpb.ConfChangeType_AddNode {
		if containPeer(region, co.changePeer.GetPeer()) {
			return true, nil
		}
	} else if co.changePeer.GetChangeType() == raftpb.ConfChangeType_RemoveNode {
		if !containPeer(region, co.changePeer.GetPeer()) {
			return true, nil
		}
	}

	return false, nil
}

// Do implements Operator.Do interface.
func (co *ChangePeerOperator) Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.RegionHeartbeatResponse, error) {
	ok, err := co.Check(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		return true, nil, nil
	}

	res := &pdpb.RegionHeartbeatResponse{
		ChangePeer: co.changePeer,
	}
	return false, res, nil
}

// TransferLeaderOperator is used to do leader transfer.
type TransferLeaderOperator struct {
	oldLeader *metapb.Peer
	newLeader *metapb.Peer
}

func newTransferLeaderOperator(oldLeader, newLeader *metapb.Peer) *TransferLeaderOperator {
	return &TransferLeaderOperator{
		oldLeader: oldLeader,
		newLeader: newLeader,
	}
}

// Check implements Operator.Check interface.
func (lto *TransferLeaderOperator) Check(region *metapb.Region, leader *metapb.Peer) (bool, error) {
	if leader == nil {
		return false, errors.New("invalid leader peer")
	}

	// If the leader has already been changed to new leader, we finish it.
	if leader.GetId() == lto.newLeader.GetId() {
		return true, nil
	}

	// If the old leader has been changed but not be new leader, we also finish it.
	if leader.GetId() != lto.oldLeader.GetId() {
		log.Warnf("old leader %v has changed to %v, but not %v", lto.oldLeader, leader, lto.newLeader)
		return true, nil
	}

	return false, nil
}

// Do implements Operator.Doop interface.
func (lto *TransferLeaderOperator) Do(region *metapb.Region, leader *metapb.Peer) (bool, *pdpb.RegionHeartbeatResponse, error) {
	ok, err := lto.Check(region, leader)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		return true, nil, nil
	}

	res := &pdpb.RegionHeartbeatResponse{
		TransferLeader: &pdpb.TransferLeader{
			Peer: lto.newLeader,
		},
	}
	return false, res, nil
}
