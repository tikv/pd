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
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

func (c *raftCluster) handleDefaultBalancer(region *metapb.Region, leader *metapb.Peer) (*pdpb.RegionHeartbeatResponse, error) {
	// TODO: we must consider max allowed running default balance regions too.
	// E,g, max peer count is 3, if we have only two store 1, 2 and add 3 later,
	// if we don't limit, we will allow many regions do ConfChange add peer.
	balancer := newDefaultBalancer(region, leader)
	balanceOperator, err := balancer.Balance(c.cachedCluster)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if balanceOperator == nil {
		return nil, nil
	}

	_, res, err := balanceOperator.Do(region, leader)
	return res, errors.Trace(err)
}

func (c *raftCluster) HandleRegionHeartbeat(region *metapb.Region, leader *metapb.Peer) (*pdpb.RegionHeartbeatResponse, error) {
	regionID := region.GetId()
	balanceOperator := c.balanceWorker.getBalanceOperator(regionID)
	if balanceOperator == nil {
		return c.handleDefaultBalancer(region, leader)
	}

	ret, res, err := balanceOperator.Do(region, leader)
	if err != nil {
		log.Errorf("do balance for region %d failed %s", regionID, err)
		c.balanceWorker.removeBalanceOperator(regionID)
	}

	if ret {
		// Do finished, remove it.
		c.balanceWorker.removeBalanceOperator(regionID)
	}

	return res, nil
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
