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
	"github.com/pingcap/kvproto/pkg/raftpb"
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
	Code msgType

	SplitEvent struct {
		Region uint64
		Left   uint64
		Right  uint64
	} `json:",omitempty"`

	AddReplicaEvent struct {
		Region uint64
	} `json:",omitempty"`

	RemoveReplicaEvent struct {
		Region uint64
	} `json:",omitempty"`

	TransferLeaderEvent struct {
		Region    uint64
		StoreFrom uint64
		StoreTo   uint64
	} `json:",omitempty"`
}

func (bw *balancerWorker) noneBlockPostEvent(evt LogEvent) {
	select {
	case bw.eventCh <- evt:
	default:
	}
}

func (bw *balancerWorker) postEvent(op Operator) {
	switch e := op.(type) {
	case *splitOperator:
		var evt LogEvent
		evt.Code = msgSplit
		evt.SplitEvent.Region = e.Origin.GetId()
		evt.SplitEvent.Left = e.Left.GetId()
		evt.SplitEvent.Right = e.Right.GetId()
		bw.noneBlockPostEvent(evt)
	case *balanceOperator:
		regionID := e.Region.GetId()
		for _, o := range e.Ops {
			var evt LogEvent
			switch eo := o.(type) {
			case *transferLeaderOperator:
				evt.Code = msgTransferLeader
				evt.TransferLeaderEvent.Region = regionID
				evt.TransferLeaderEvent.StoreFrom = eo.OldLeader.GetStoreId()
				evt.TransferLeaderEvent.StoreTo = eo.NewLeader.GetStoreId()
				bw.noneBlockPostEvent(evt)
			case *changePeerOperator:
				if eo.ChangePeer.GetChangeType() == raftpb.ConfChangeType_AddNode {
					evt.Code = msgAddReplica
					evt.AddReplicaEvent.Region = eo.RegionID
					bw.noneBlockPostEvent(evt)
				} else {
					evt.Code = msgRemoveReplica
					evt.RemoveReplicaEvent.Region = eo.RegionID
					bw.noneBlockPostEvent(evt)
				}
			}
		}
	case *onceOperator:
		var evt LogEvent
		switch eo := e.Op.(type) {
		case *changePeerOperator:
			if eo.ChangePeer.GetChangeType() == raftpb.ConfChangeType_AddNode {
				evt.Code = msgAddReplica
				evt.AddReplicaEvent.Region = eo.RegionID
				bw.noneBlockPostEvent(evt)
			} else {
				evt.Code = msgRemoveReplica
				evt.RemoveReplicaEvent.Region = eo.RegionID
				bw.noneBlockPostEvent(evt)
			}
		}
	}
}

func (bw *balancerWorker) fetchEvents(count int64) []LogEvent {
	if count <= 0 {
		return nil
	}

	evts := make([]LogEvent, 0, count)

LOOP:
	for {
		select {
		case evt := <-bw.eventCh:
			evts = append(evts, evt)
			count--

			if count <= 0 {
				break LOOP
			}
		default:
			break LOOP
		}
	}

	return evts
}
