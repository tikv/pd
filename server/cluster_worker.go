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

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/util"
)

func (c *raftCluster) handleChangePeerReq(region *metapb.Region, leader *metapb.Peer) (*pdpb.RegionHeartbeatResponse, error) {
	balancer := newDefaultBalancer(region, leader)
	balanceOperator, err := balancer.Balance(c)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if balanceOperator == nil {
		return nil, nil
	}

	_, res, err := balanceOperator.Do(region, leader)
	return res, errors.Trace(err)
}

func (c *raftCluster) HandleAskSplit(request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	reqRegion := request.GetRegion()
	startKey := reqRegion.GetStartKey()
	region, err := c.GetRegion(startKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// If the request epoch is less than current region epoch, then returns an error.
	reqRegionEpoch := reqRegion.GetRegionEpoch()
	regionEpoch := region.GetRegionEpoch()
	if reqRegionEpoch.GetVersion() < regionEpoch.GetVersion() ||
		reqRegionEpoch.GetConfVer() < regionEpoch.GetConfVer() {
		return nil, errors.Errorf("invalid region epoch, request: %v, currenrt: %v", reqRegionEpoch, regionEpoch)
	}

	newRegionID, err := c.s.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	peerIDs := make([]uint64, len(request.Region.Peers))
	for i := 0; i < len(peerIDs); i++ {
		if peerIDs[i], err = c.s.idAlloc.Alloc(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	split := &pdpb.AskSplitResponse{
		NewRegionId: proto.Uint64(newRegionID),
		NewPeerIds:  peerIDs,
	}

	return split, nil
}

func (c *raftCluster) callCommand(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	storeID := request.Header.Peer.GetStoreId()
	store, err := c.GetStore(storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	nc, err := c.storeConns.GetConn(store.GetAddress())
	if err != nil {
		return nil, errors.Trace(err)
	}

	msg := &msgpb.Message{
		MsgType: msgpb.MessageType_Cmd.Enum(),
		CmdReq:  request,
	}

	msgID := atomic.AddUint64(&c.s.msgID, 1)
	if err = util.WriteMessage(nc.conn, msgID, msg); err != nil {
		c.storeConns.RemoveConn(store.GetAddress())
		return nil, errors.Trace(err)
	}

	msg.Reset()
	if _, err = util.ReadMessage(nc.conn, msg); err != nil {
		c.storeConns.RemoveConn(store.GetAddress())
		return nil, errors.Trace(err)
	}

	if msg.CmdResp == nil {
		// This is a very serious bug, should we panic here?
		return nil, errors.Errorf("invalid command response message but %v", msg)
	}

	return msg.CmdResp, nil
}
