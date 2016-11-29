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
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	maxTransferLeaderWaitCount = 3
)

var baseID uint64

type callback func(op Operator)

type opContext struct {
	start callback
	end   callback
}

func newOpContext(start callback, end callback) *opContext {
	return &opContext{
		start: start,
		end:   end,
	}
}

func doCallback(f callback, op Operator) {
	if f != nil {
		f(op)
	}
}

// Operator is the interface to do some operations.
type Operator interface {
	// Do does the operator, if finished then return true.
	Do(ctx *opContext, region *regionInfo) (bool, *pdpb.RegionHeartbeatResponse, error)
}

type optype int

// Priority: adminOP > replicaOP > balanceOP
const (
	balanceOP optype = iota + 1
	replicaOP
	adminOP
)

// balanceOperator is used to do region balance.
type balanceOperator struct {
	ID     uint64      `json:"id"`
	Type   optype      `json:"type"`
	Index  int         `json:"index"`
	Start  time.Time   `json:"start"`
	End    time.Time   `json:"end"`
	Ops    []Operator  `json:"operators"`
	Region *regionInfo `json:"region"`
}

func newBalanceOperator(region *regionInfo, optype optype, ops ...Operator) *balanceOperator {
	return &balanceOperator{
		ID:     atomic.AddUint64(&baseID, 1),
		Type:   optype,
		Ops:    ops,
		Region: region,
	}
}

func (bo *balanceOperator) String() string {
	ret := fmt.Sprintf("[balanceOperator]id: %d, index: %d, start: %s, end: %s, region: %v, ops:",
		bo.ID, bo.Index, bo.Start, bo.End, bo.Region)

	for i := range bo.Ops {
		ret += fmt.Sprintf(" [%d - %v] ", i, bo.Ops[i])
	}

	return ret
}

// Check checks whether operator already finished or not.
func (bo *balanceOperator) check(region *regionInfo) (bool, error) {
	if bo.Index >= len(bo.Ops) {
		bo.End = time.Now()
		return true, nil
	}

	err := checkStaleRegion(bo.Region.Region, region.Region)
	if err != nil {
		return false, errors.Trace(err)
	}

	bo.Region = region.clone()

	return false, nil
}

// Do implements Operator.Do interface.
func (bo *balanceOperator) Do(ctx *opContext, region *regionInfo) (bool, *pdpb.RegionHeartbeatResponse, error) {
	ok, err := bo.check(region)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		return true, nil, nil
	}

	if bo.Start.IsZero() {
		bo.Start = time.Now()
	}

	finished, res, err := bo.Ops[bo.Index].Do(ctx, region)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if !finished {
		return false, res, nil
	}

	bo.Index++

	if bo.Index >= len(bo.Ops) {
		bo.End = time.Now()
	}
	return !bo.End.IsZero(), res, nil
}

// getRegionID returns the region id which the operator for balance.
func (bo *balanceOperator) getRegionID() uint64 {
	return bo.Region.GetId()
}

// onceOperator is the operator wrapping another operator
// and can be called only once. It will return finished every time.
type onceOperator struct {
	Op       Operator `json:"operator"`
	Finished bool     `json:"finished"`
}

func newOnceOperator(op Operator) *onceOperator {
	return &onceOperator{
		Op:       op,
		Finished: false,
	}
}

func (op *onceOperator) String() string {
	return fmt.Sprintf("[onceOperator]op: %v, finished: %v", op.Op, op.Finished)
}

// Do implements Operator.Do interface.
func (op *onceOperator) Do(ctx *opContext, region *regionInfo) (bool, *pdpb.RegionHeartbeatResponse, error) {
	if op.Finished {
		return true, nil, nil
	}

	op.Finished = true
	_, resp, err := op.Op.Do(ctx, region)
	return true, resp, errors.Trace(err)
}

// changePeerOperator is used to do peer change.
type changePeerOperator struct {
	ChangePeer *pdpb.ChangePeer `json:"operator"`
	RegionID   uint64           `json:"regionid"`
	Name       string           `json:"name"`
	firstCheck bool
}

func newAddPeerOperator(regionID uint64, peer *metapb.Peer) *changePeerOperator {
	return &changePeerOperator{
		ChangePeer: &pdpb.ChangePeer{
			ChangeType: raftpb.ConfChangeType_AddNode.Enum(),
			Peer:       peer,
		},
		RegionID:   regionID,
		Name:       "add_peer",
		firstCheck: true,
	}
}

func newRemovePeerOperator(regionID uint64, peer *metapb.Peer) *changePeerOperator {
	return &changePeerOperator{
		ChangePeer: &pdpb.ChangePeer{
			ChangeType: raftpb.ConfChangeType_RemoveNode.Enum(),
			Peer:       peer,
		},
		RegionID:   regionID,
		Name:       "remove_peer",
		firstCheck: true,
	}
}

func (co *changePeerOperator) String() string {
	return fmt.Sprintf("[changePeerOperator]regionID: %d, changePeer: %v", co.RegionID, co.ChangePeer)
}

// check checks whether operator already finished or not.
func (co *changePeerOperator) check(region *regionInfo) (bool, error) {
	if co.ChangePeer.GetChangeType() == raftpb.ConfChangeType_AddNode {
		if region.ContainsPeer(co.ChangePeer.GetPeer().GetId()) {
			return true, nil
		}
		log.Infof("balance [%s], try to add peer %s", region, co.ChangePeer.GetPeer())
	} else if co.ChangePeer.GetChangeType() == raftpb.ConfChangeType_RemoveNode {
		if !region.ContainsPeer(co.ChangePeer.GetPeer().GetId()) {
			return true, nil
		}
		log.Infof("balance [%s], try to remove peer %s", region, co.ChangePeer.GetPeer())
	}

	return false, nil
}

// Do implements Operator.Do interface.
func (co *changePeerOperator) Do(ctx *opContext, region *regionInfo) (bool, *pdpb.RegionHeartbeatResponse, error) {
	ok, err := co.check(region)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		return true, nil, nil
	}

	if co.firstCheck {
		doCallback(ctx.start, co)
		co.firstCheck = false
	}

	res := &pdpb.RegionHeartbeatResponse{
		ChangePeer: co.ChangePeer,
	}
	return false, res, nil
}

// transferLeaderOperator is used to do leader transfer.
type transferLeaderOperator struct {
	Count int `json:"count"`

	OldLeader *metapb.Peer `json:"old_leader"`
	NewLeader *metapb.Peer `json:"new_leader"`

	RegionID uint64 `json:"regionid"`
	Name     string `json:"name"`

	firstCheck    bool
	startCallback func(op Operator)
	endCallback   func(op Operator)
}

func newTransferLeaderOperator(regionID uint64, oldLeader *metapb.Peer, newLeader *metapb.Peer) *transferLeaderOperator {
	return &transferLeaderOperator{
		OldLeader:  oldLeader,
		NewLeader:  newLeader,
		RegionID:   regionID,
		Name:       "transfer_leader",
		firstCheck: true,
	}
}

func (tlo *transferLeaderOperator) String() string {
	return fmt.Sprintf("[transferLeaderOperator]count: %d,  oldLeader: %v, newLeader: %v",
		tlo.Count, tlo.OldLeader, tlo.NewLeader)
}

// check checks whether operator already finished or not.
func (tlo *transferLeaderOperator) check(region *regionInfo) (bool, error) {
	if region.Leader == nil {
		return false, errors.New("invalid leader peer")
	}

	// If the leader has already been changed to new leader, we finish it.
	if region.Leader.GetId() == tlo.NewLeader.GetId() {
		return true, nil
	}

	log.Infof("balance [%d][%s], try to transfer leader from %s to %s", tlo.Count, region, tlo.OldLeader, tlo.NewLeader)
	return false, nil
}

// Do implements Operator.Do interface.
func (tlo *transferLeaderOperator) Do(ctx *opContext, region *regionInfo) (bool, *pdpb.RegionHeartbeatResponse, error) {
	ok, err := tlo.check(region)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	if ok {
		doCallback(ctx.end, tlo)
		return true, nil, nil
	}

	if tlo.firstCheck {
		doCallback(ctx.start, tlo)
		tlo.firstCheck = false
	}

	// If tlo.count is greater than 0, then we should check whether it exceeds the maxTransferLeaderWaitCount.
	if tlo.Count > 0 {
		if tlo.Count >= maxTransferLeaderWaitCount {
			return false, nil, errors.Errorf("transfer leader operator called %d times but still be unsucceessful - %v", tlo.Count, tlo)
		}

		tlo.Count++
		return false, nil, nil
	}

	res := &pdpb.RegionHeartbeatResponse{
		TransferLeader: &pdpb.TransferLeader{
			Peer: tlo.NewLeader,
		},
	}
	tlo.Count++
	return false, res, nil
}

// splitOperator is used to do region split, only for history operator mark.
type splitOperator struct {
	Origin *metapb.Region `json:"origin"`
	Left   *metapb.Region `json:"left"`
	Right  *metapb.Region `json:"right"`

	Name string `json:"name"`
}

func newSplitOperator(origin *metapb.Region, left *metapb.Region, right *metapb.Region) *splitOperator {
	return &splitOperator{
		Origin: origin,
		Left:   left,
		Right:  right,
		Name:   "split",
	}
}

// Do implements Operator.Do interface.
func (so *splitOperator) Do(ctx *opContext, region *regionInfo) (bool, *pdpb.RegionHeartbeatResponse, error) {
	return true, nil, nil
}
