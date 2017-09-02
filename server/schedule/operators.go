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

package schedule

import (
	"encoding/json"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
)

// AdminOperator is the operator created manually using API.
type AdminOperator struct {
	sync.RWMutex `json:"-"`
	Name         string           `json:"name"`
	Start        time.Time        `json:"start"`
	Region       *core.RegionInfo `json:"region"`
	Ops          []Operator       `json:"ops"`
	State        OperatorState    `json:"state"`
}

// NewAdminOperator creates an AdminOperator with detailed operators.
func NewAdminOperator(region *core.RegionInfo, ops ...Operator) *AdminOperator {
	return &AdminOperator{
		Name:   "admin_operator",
		Start:  time.Now(),
		Region: region,
		Ops:    ops,
		State:  OperatorWaiting,
	}
}

func (op *AdminOperator) String() string {
	op.RLock()
	defer op.RUnlock()
	return jsonString(op)
}

// GetRegionID returns the operated region ID.
func (op *AdminOperator) GetRegionID() uint64 {
	op.RLock()
	defer op.RUnlock()
	return op.Region.GetId()
}

// GetResourceKind returns AdminKind.
func (op *AdminOperator) GetResourceKind() core.ResourceKind {
	return core.AdminKind
}

// GetState returns the operator's state.
func (op *AdminOperator) GetState() OperatorState {
	op.RLock()
	defer op.RUnlock()
	return op.State
}

// SetState updates operator's state.
func (op *AdminOperator) SetState(state OperatorState) {
	op.Lock()
	defer op.Unlock()
	op.State = state
	for _, o := range op.Ops {
		o.SetState(state)
	}
}

// GetName returns operator's name.
func (op *AdminOperator) GetName() string {
	return op.Name
}

// Do checks operator's process, returns next action if need.
func (op *AdminOperator) Do(region *core.RegionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	op.Lock()
	defer op.Unlock()
	// Update region.
	op.Region = region.Clone()

	// Do all operators in order.
	for i := 0; i < len(op.Ops); i++ {
		if res, finished := op.Ops[i].Do(region); !finished {
			op.State = OperatorRunning
			return res, false
		}
	}

	// Admin operator never ends, remove it from the API.
	op.State = OperatorFinished
	return nil, false
}

// RegionOperator is the operator contains several leader or region movements.
type RegionOperator struct {
	sync.RWMutex `json:"-"`
	Name         string            `json:"name"`
	Start        time.Time         `json:"start"`
	Region       *core.RegionInfo  `json:"region"`
	End          time.Time         `json:"end"`
	Index        int               `json:"index"`
	Ops          []Operator        `json:"ops"`
	Kind         core.ResourceKind `json:"kind"`
	State        OperatorState     `json:"state"`
}

// NewRegionOperator creates an RegionOperator with detailed operators.
func NewRegionOperator(region *core.RegionInfo, kind core.ResourceKind, ops ...Operator) *RegionOperator {
	// Do some check here, just fatal because it must be bug.
	if len(ops) == 0 {
		log.Fatalf("[region %d] new region operator with no ops", region.GetId())
	}

	return &RegionOperator{
		Name:   "region_operator",
		Start:  time.Now(),
		Region: region,
		Ops:    ops,
		Kind:   kind,
		State:  OperatorWaiting,
	}
}

func (op *RegionOperator) String() string {
	op.RLock()
	defer op.RUnlock()
	return jsonString(op)
}

// GetRegionID returns the operated region ID.
func (op *RegionOperator) GetRegionID() uint64 {
	op.RLock()
	defer op.RUnlock()
	return op.Region.GetId()
}

// GetResourceKind returns the resource type to be scheduled.
func (op *RegionOperator) GetResourceKind() core.ResourceKind {
	return op.Kind
}

// GetState returns the operator's state.
func (op *RegionOperator) GetState() OperatorState {
	op.RLock()
	defer op.RUnlock()
	return op.State
}

// SetState updates operator's state.
func (op *RegionOperator) SetState(state OperatorState) {
	op.Lock()
	defer op.Unlock()
	if op.State == OperatorFinished || op.State == OperatorTimeOut {
		return
	}
	op.State = state
	for _, o := range op.Ops {
		o.SetState(state)
	}
}

// GetName returns operator's name.
func (op *RegionOperator) GetName() string {
	return op.Name
}

// Do checks operator's process, returns next action if need.
func (op *RegionOperator) Do(region *core.RegionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	op.Lock()
	defer op.Unlock()
	if time.Since(op.Start) > MaxOperatorWaitTime {
		log.Errorf("[region %d] Operator timeout:%s", region.GetId(), jsonString(op))
		op.State = OperatorTimeOut
		return nil, true
	}

	// Update region.
	op.Region = region.Clone()

	// If an operator is not finished, do it.
	for ; op.Index < len(op.Ops); op.Index++ {
		if res, finished := op.Ops[op.Index].Do(region); !finished {
			op.State = OperatorRunning
			return res, false
		}
	}

	op.End = time.Now()
	op.State = OperatorFinished
	return nil, true
}

// ChangePeerOperator is the operator adds or removes a peer.
type ChangePeerOperator struct {
	sync.RWMutex `json:"-"`
	Name         string           `json:"name"`
	RegionID     uint64           `json:"region_id"`
	ChangePeer   *pdpb.ChangePeer `json:"change_peer"`
	State        OperatorState    `json:"state"`
}

// NewAddPeerOperator creates a ChangePeerOperator with the peer to add.
func NewAddPeerOperator(regionID uint64, peer *metapb.Peer) *ChangePeerOperator {
	return &ChangePeerOperator{
		Name:     "add_peer",
		RegionID: regionID,
		ChangePeer: &pdpb.ChangePeer{
			// FIXME: replace with actual ConfChangeType once eraftpb uses proto3.
			ChangeType: pdpb.ConfChangeType_AddNode,
			Peer:       peer,
		},
		State: OperatorWaiting,
	}
}

// NewRemovePeerOperator creates a ChangePeerOperator with the peer to remove.
func NewRemovePeerOperator(regionID uint64, peer *metapb.Peer) *ChangePeerOperator {
	return &ChangePeerOperator{
		Name:     "remove_peer",
		RegionID: regionID,
		ChangePeer: &pdpb.ChangePeer{
			// FIXME: replace with actual ConfChangeType once eraftpb uses proto3.
			ChangeType: pdpb.ConfChangeType_RemoveNode,
			Peer:       peer,
		},
		State: OperatorWaiting,
	}
}

func (op *ChangePeerOperator) String() string {
	op.RLock()
	defer op.RUnlock()
	return jsonString(op)
}

// GetRegionID returns the operated region ID.
func (op *ChangePeerOperator) GetRegionID() uint64 {
	return op.RegionID
}

// GetResourceKind returns RegionKind.
func (op *ChangePeerOperator) GetResourceKind() core.ResourceKind {
	return core.RegionKind
}

// GetState returns the operator's state.
func (op *ChangePeerOperator) GetState() OperatorState {
	op.RLock()
	defer op.RUnlock()
	return op.State
}

// SetState updates operator's state.
func (op *ChangePeerOperator) SetState(state OperatorState) {
	op.Lock()
	defer op.Unlock()
	if op.State == OperatorFinished {
		return
	}
	op.State = state
}

// GetName returns operator's name.
func (op *ChangePeerOperator) GetName() string {
	return op.Name
}

// Do checks operator's process, returns next action if need.
func (op *ChangePeerOperator) Do(region *core.RegionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	op.Lock()
	defer op.Unlock()
	// Check if operator is finished.
	peer := op.ChangePeer.GetPeer()
	switch op.ChangePeer.GetChangeType() {
	case pdpb.ConfChangeType_AddNode:
		if region.GetPendingPeer(peer.GetId()) != nil {
			// Peer is added but not finished.
			return nil, false
		}
		if region.GetPeer(peer.GetId()) != nil {
			// Peer is added and finished.
			op.State = OperatorFinished
			return nil, true
		}
	case pdpb.ConfChangeType_RemoveNode:
		if region.GetPeer(peer.GetId()) == nil {
			// Peer is removed.
			op.State = OperatorFinished
			return nil, true
		}
	}

	log.Infof("[region %d] Do operator %s {%v}", region.GetId(), op.Name, op.ChangePeer.GetPeer())

	op.State = OperatorRunning
	res := &pdpb.RegionHeartbeatResponse{
		ChangePeer: op.ChangePeer,
	}
	return res, false
}

// TransferLeaderOperator is the operator transfers leadership of a region.
type TransferLeaderOperator struct {
	sync.RWMutex `json:"-"`
	Name         string        `json:"name"`
	RegionID     uint64        `json:"region_id"`
	OldLeader    *metapb.Peer  `json:"old_leader"`
	NewLeader    *metapb.Peer  `json:"new_leader"`
	State        OperatorState `json:"state"`
}

// NewTransferLeaderOperator creates an TransferLeaderOperator with the old and
// new leader.
func NewTransferLeaderOperator(regionID uint64, oldLeader, newLeader *metapb.Peer) *TransferLeaderOperator {
	return &TransferLeaderOperator{
		Name:      "transfer_leader",
		RegionID:  regionID,
		OldLeader: oldLeader,
		NewLeader: newLeader,
		State:     OperatorWaiting,
	}
}

func (op *TransferLeaderOperator) String() string {
	op.RLock()
	defer op.RUnlock()
	return jsonString(op)
}

// GetRegionID returns the operated region ID.
func (op *TransferLeaderOperator) GetRegionID() uint64 {
	return op.RegionID
}

// GetResourceKind returns RegionKind.
func (op *TransferLeaderOperator) GetResourceKind() core.ResourceKind {
	return core.LeaderKind
}

// GetState returns the operator's state.
func (op *TransferLeaderOperator) GetState() OperatorState {
	op.RLock()
	defer op.RUnlock()
	return op.State
}

// SetState updates operator's state.
func (op *TransferLeaderOperator) SetState(state OperatorState) {
	op.Lock()
	defer op.Unlock()
	if op.State == OperatorFinished {
		return
	}
	op.State = state
}

// GetName returns operator's name.
func (op *TransferLeaderOperator) GetName() string {
	return op.Name
}

// Do checks operator's process, returns next action if need.
func (op *TransferLeaderOperator) Do(region *core.RegionInfo) (*pdpb.RegionHeartbeatResponse, bool) {
	op.Lock()
	defer op.Unlock()
	// Check if operator is finished.
	if region.Leader.GetId() == op.NewLeader.GetId() {
		op.State = OperatorFinished
		return nil, true
	}

	log.Infof("[region %d] Do operator %s,from peer:{%v} to peer:{%v}", region.GetId(), op.Name, op.OldLeader, op.NewLeader)
	op.State = OperatorRunning
	res := &pdpb.RegionHeartbeatResponse{
		TransferLeader: &pdpb.TransferLeader{
			Peer: op.NewLeader,
		},
	}
	return res, false
}

func jsonString(v interface{}) string {
	data, _ := json.Marshal(v)
	return string(data)
}
