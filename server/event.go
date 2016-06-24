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
	"math/rand"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
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
	Code msgType `json:"code"`

	SplitEvent struct {
		Region uint64 `json:"region"`
		Left   uint64 `json:"left"`
		Right  uint64 `json:"right"`
	} `json:"split_event,omitempty"`

	AddReplicaEvent struct {
		Region uint64 `json:"region"`
	} `json:"add_replica_event,omitempty"`

	RemoveReplicaEvent struct {
		Region uint64 `json:"region"`
	} `json:"remove_replica_event,omitempty"`

	TransferLeaderEvent struct {
		Region    uint64 `json:"region"`
		StoreFrom uint64 `json:"store_from"`
		StoreTo   uint64 `json:"store_to"`
	} `json:"transfer_leader_event,omitempty"`
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

func randIDs(ids []int, n int) []uint64 {
	l := len(ids)
	m := map[int]bool{}
	s := make([]uint64, l)

	for i := 0; i < n; {
		idx := rand.Intn(l)
		if m[idx] {
			continue
		}

		s[i] = uint64(ids[idx])
		m[idx] = true
		i++
	}

	return s
}

func (bw *balancerWorker) mockBalanceOperator() {
	rids := rand.Perm(100000)
	ids := randIDs(rids, 9)

	oldLeaderPeer := &metapb.Peer{
		Id:      proto.Uint64(uint64(ids[0])),
		StoreId: proto.Uint64(uint64(ids[1])),
	}
	newLeaderPeer := &metapb.Peer{
		Id:      proto.Uint64(uint64(ids[2])),
		StoreId: proto.Uint64(uint64(ids[3])),
	}
	followerPeer := &metapb.Peer{
		Id:      proto.Uint64(uint64(ids[4])),
		StoreId: proto.Uint64(uint64(ids[5])),
	}
	newPeer := &metapb.Peer{
		Id:      proto.Uint64(uint64(ids[6])),
		StoreId: proto.Uint64(uint64(ids[7])),
	}

	region := &metapb.Region{
		Id:       proto.Uint64(uint64(ids[8])),
		StartKey: []byte("aaaa"),
		EndKey:   []byte("zzzz"),
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: proto.Uint64(1),
			Version: proto.Uint64(2),
		},
		Peers: []*metapb.Peer{
			oldLeaderPeer, newLeaderPeer, followerPeer,
		},
	}

	op1 := newTransferLeaderOperator(oldLeaderPeer, newLeaderPeer, maxWaitCount)
	op2 := newAddPeerOperator(region.GetId(), newPeer)
	op3 := newRemovePeerOperator(region.GetId(), oldLeaderPeer)
	op := newBalanceOperator(region, op1, op2, op3)
	bw.addBalanceOperator(region.GetId(), op)
}
