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
	"bytes"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

func (c *RaftCluster) checkRegionMerge(region *regionInfo) (*pdpb.RegionHeartbeatResponse, error) {
	cluster := c.cachedCluster

	fromRegion, intoRegion := cluster.getMergingRegions(region.GetId())
	if fromRegion == nil || intoRegion == nil {
		return nil, nil
	}

	// Tell region to do nothing.
	none := &pdpb.RegionHeartbeatResponse{}

	for _, peer := range intoRegion.GetPeers() {
		storeID := peer.GetStoreId()
		if fromRegion.GetStorePeer(storeID) == nil {
			// Into region do nothing if peers are not match.
			if region.GetId() == intoRegion.GetId() {
				return none, nil
			}
			newPeer, err := cluster.allocPeer(storeID)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return &pdpb.RegionHeartbeatResponse{
				ChangePeer: &pdpb.ChangePeer{
					ChangeType: raftpb.ConfChangeType_AddNode.Enum(),
					Peer:       newPeer,
				},
			}, nil
		}
	}

	for _, peer := range fromRegion.GetPeers() {
		storeID := peer.GetStoreId()
		if intoRegion.GetStorePeer(storeID) == nil {
			// Into region do nothing if peers are not match.
			if region.GetId() == intoRegion.GetId() {
				return none, nil
			}
			return &pdpb.RegionHeartbeatResponse{
				ChangePeer: &pdpb.ChangePeer{
					ChangeType: raftpb.ConfChangeType_RemoveNode.Enum(),
					Peer:       peer,
				},
			}, nil
		}
	}

	// If we reach here, from region and into region peers are match.

	// From region do nothing if peers are match.
	if region.GetId() == fromRegion.GetId() {
		return none, nil
	}

	// Into region begin to merge if peers are match.
	return &pdpb.RegionHeartbeatResponse{
		RegionMerge: &pdpb.RegionMerge{
			FromRegion: fromRegion.Region,
		},
	}, nil
}

func (c *RaftCluster) handleRegionHeartbeat(region *regionInfo) (*pdpb.RegionHeartbeatResponse, error) {
	// We must not do balance if region is merging.
	res, err := c.checkRegionMerge(region)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if res != nil {
		return res, nil
	}

	// If the region peer count is 0, then we should not handle this.
	if len(region.GetPeers()) == 0 {
		log.Warnf("invalid region, zero region peer count - %v", region)
		return nil, errors.Errorf("invalid region, zero region peer count - %v", region)
	}

	bw := c.balancerWorker
	regionID := region.GetId()

	err = bw.checkReplicas(region)
	if err != nil {
		return nil, errors.Trace(err)
	}

	op := bw.getBalanceOperator(regionID)
	if op == nil {
		return nil, nil
	}

	ctx := newOpContext(bw.hookStartEvent, bw.hookEndEvent)
	finished, res, err := op.Do(ctx, region)
	if err != nil {
		// Do balance failed, remove it.
		log.Errorf("do balance for region %d failed %s", regionID, err)
		bw.removeBalanceOperator(regionID)
		bw.removeRegionCache(regionID)
	}
	if finished {
		// Do finished, remove it.
		bw.removeBalanceOperator(regionID)
	}

	return res, nil
}

func (c *RaftCluster) handleAskSplit(request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	region := request.GetRegion()

	newRegionID, newPeerIDs, err := c.cachedCluster.handleRegionSplit(region)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.AskSplitResponse{
		NewRegionId: newRegionID,
		NewPeerIds:  newPeerIDs,
	}, nil
}

func (c *RaftCluster) handleAskMerge(request *pdpb.AskMergeRequest) (*pdpb.AskMergeResponse, error) {
	fromRegion := request.GetFromRegion()

	intoRegion, err := c.cachedCluster.handleRegionMerge(fromRegion)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.AskMergeResponse{
		IntoRegion: intoRegion,
	}, nil
}

func (c *RaftCluster) checkSplitRegion(left *metapb.Region, right *metapb.Region) error {
	if left == nil || right == nil {
		return errors.New("invalid split region")
	}

	if !bytes.Equal(left.GetEndKey(), right.GetStartKey()) {
		return errors.New("invalid split region")
	}

	if len(right.GetEndKey()) == 0 || bytes.Compare(left.GetStartKey(), right.GetEndKey()) < 0 {
		return nil
	}

	return errors.New("invalid split region")
}

func (c *RaftCluster) handleReportSplit(request *pdpb.ReportSplitRequest) (*pdpb.ReportSplitResponse, error) {
	left := request.GetLeft()
	right := request.GetRight()

	err := c.checkSplitRegion(left, right)
	if err != nil {
		log.Warnf("report split region is invalid - %v, %v", request, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}

	// Build origin region by using left and right.
	originRegion := proto.Clone(left).(*metapb.Region)
	originRegion.RegionEpoch = nil
	originRegion.EndKey = right.GetEndKey()

	// Wrap report split as an Operator, and add it into history cache.
	op := newSplitOperator(originRegion, left, right)
	c.balancerWorker.historyOperators.add(originRegion.GetId(), op)

	c.balancerWorker.postEvent(op, evtEnd)

	return &pdpb.ReportSplitResponse{}, nil
}
