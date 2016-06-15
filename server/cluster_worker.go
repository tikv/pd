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

func (c *raftCluster) addDefaultBalanceOperator(region *metapb.Region, leader *metapb.Peer) (*balanceOperator, error) {
	if !c.balancerWorker.allowBalance() {
		return nil, nil
	}

	balancer := newDefaultBalancer(region, leader)
	balanceOperator, err := balancer.Balance(c.cachedCluster)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if balanceOperator == nil {
		return nil, nil
	}

	if c.balancerWorker.addBalanceOperator(balanceOperator.getRegionID(), balanceOperator) {
		return balanceOperator, nil
	}

	// Em, the balance worker may have already added a BalanceOperator.
	return c.balancerWorker.getBalanceOperator(region.GetId()), nil
}

func (c *raftCluster) handleRegionHeartbeat(region *metapb.Region, leader *metapb.Peer) (*pdpb.RegionHeartbeatResponse, error) {
	// If the region peer count is 0, then we should not handle this.
	if len(region.GetPeers()) == 0 {
		log.Warnf("invalid region, zero region peer count - %v", region)
		return nil, errors.Errorf("invalid region, zero region peer count - %v", region)
	}

	regionID := region.GetId()
	balanceOperator := c.balancerWorker.getBalanceOperator(regionID)
	var err error
	if balanceOperator == nil {
		balanceOperator, err = c.addDefaultBalanceOperator(region, leader)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if balanceOperator == nil {
			return nil, nil
		}
	}

	finished, res, err := balanceOperator.Do(region, leader)
	if err != nil {
		// Do balance failed, remove it.
		log.Errorf("do balance for region %d failed %s", regionID, err)
		c.balancerWorker.removeBalanceOperator(regionID)
		c.balancerWorker.removeRegionCache(regionID)
	}
	if finished {
		// Do finished, remove it.
		c.balancerWorker.removeBalanceOperator(regionID)
	}

	return res, nil
}

func (c *raftCluster) handleAskSplit(request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	reqRegion := request.GetRegion()
	startKey := reqRegion.GetStartKey()
	region, err := c.getRegion(startKey)
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
