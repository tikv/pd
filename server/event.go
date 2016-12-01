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
	"sync/atomic"

	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
)

type statusType byte

const (
	evtStart statusType = iota + 1
	evtEnd
)

type msgType byte

const (
	msgSplit msgType = iota + 1
	msgTransferLeader
	msgAddReplica
	msgRemoveReplica
)

// LogEvent is operator log event.
type LogEvent struct {
	ID     uint64     `json:"id"`
	Code   msgType    `json:"code"`
	Status statusType `json:"status"`

	SplitEvent struct {
		Region uint64 `json:"region"`
		Left   uint64 `json:"left"`
		Right  uint64 `json:"right"`
	} `json:"split_event,omitempty"`

	AddReplicaEvent struct {
		Region uint64 `json:"region"`
		Store  uint64 `json:"store"`
	} `json:"add_replica_event,omitempty"`

	RemoveReplicaEvent struct {
		Region uint64 `json:"region"`
		Store  uint64 `json:"store"`
	} `json:"remove_replica_event,omitempty"`

	TransferLeaderEvent struct {
		Region    uint64 `json:"region"`
		StoreFrom uint64 `json:"store_from"`
		StoreTo   uint64 `json:"store_to"`
	} `json:"transfer_leader_event,omitempty"`
}

func (c *coordinator) innerPostEvent(evt LogEvent) {
	key := atomic.AddUint64(&baseID, 1)
	evt.ID = key
	c.events.add(key, evt)
}

func (c *coordinator) postEvent(op Operator, status statusType) {
	var evt LogEvent
	evt.Status = status

	switch e := op.(type) {
	case *splitOperator:
		evt.Code = msgSplit
		evt.SplitEvent.Region = e.Origin.GetId()
		evt.SplitEvent.Left = e.Left.GetId()
		evt.SplitEvent.Right = e.Right.GetId()
		c.innerPostEvent(evt)
	case *transferLeaderOperator:
		evt.Code = msgTransferLeader
		evt.TransferLeaderEvent.Region = e.RegionID
		evt.TransferLeaderEvent.StoreFrom = e.OldLeader.GetStoreId()
		evt.TransferLeaderEvent.StoreTo = e.NewLeader.GetStoreId()
		c.innerPostEvent(evt)
	case *changePeerOperator:
		if e.ChangePeer.GetChangeType() == raftpb.ConfChangeType_AddNode {
			evt.Code = msgAddReplica
			evt.AddReplicaEvent.Region = e.RegionID
			evt.AddReplicaEvent.Store = e.ChangePeer.Peer.GetStoreId()
			c.innerPostEvent(evt)
		} else {
			evt.Code = msgRemoveReplica
			evt.RemoveReplicaEvent.Region = e.RegionID
			evt.RemoveReplicaEvent.Store = e.ChangePeer.Peer.GetStoreId()
			c.innerPostEvent(evt)
		}
	}
}

func (c *coordinator) fetchEvents(key uint64, all bool) []LogEvent {
	var elems []*cacheItem
	if all {
		elems = c.events.elems()
	} else {
		elems = c.events.fromElems(key)
	}

	evts := make([]LogEvent, 0, len(elems))
	for _, ele := range elems {
		evts = append(evts, ele.value.(LogEvent))
	}

	return evts
}

func (c *coordinator) hookStartEvent(op Operator) {
	c.postEvent(op, evtStart)
}

func (c *coordinator) hookEndEvent(op Operator) {
	c.postEvent(op, evtEnd)
}
