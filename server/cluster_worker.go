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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

// HandleRegionHeartbeat processes RegionInfo reports from client.
func (c *RaftCluster) HandleRegionHeartbeat(region *core.RegionInfo) error {
	if err := c.cachedCluster.handleRegionHeartbeat(region); err != nil {
		return errors.Trace(err)
	}

	// If the region peer count is 0, then we should not handle this.
	if len(region.GetPeers()) == 0 {
		log.Warnf("invalid region, zero region peer count - %v", region)
		return errors.Errorf("invalid region, zero region peer count - %v", region)
	}

	c.coordinator.dispatch(region)
	return nil
}

func (c *RaftCluster) handleAskSplit(request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	reqRegion := request.GetRegion()
	err := c.validRequestRegion(reqRegion)
	if err != nil {
		return nil, errors.Trace(err)
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

	// Disable merge for the 2 regions in a period of time.
	c.coordinator.mergeChecker.RecordRegionSplit(reqRegion.GetId())
	c.coordinator.mergeChecker.RecordRegionSplit(newRegionID)

	split := &pdpb.AskSplitResponse{
		NewRegionId: newRegionID,
		NewPeerIds:  peerIDs,
	}

	return split, nil
}

func (c *RaftCluster) validRequestRegion(reqRegion *metapb.Region) error {
	startKey := reqRegion.GetStartKey()
	region, _ := c.GetRegionByKey(startKey)
	if region == nil {
		return errors.Errorf("region not found, request region: %v", reqRegion)
	}
	// If the request epoch is less than current region epoch, then returns an error.
	reqRegionEpoch := reqRegion.GetRegionEpoch()
	regionEpoch := region.GetRegionEpoch()
	if reqRegionEpoch.GetVersion() < regionEpoch.GetVersion() ||
		reqRegionEpoch.GetConfVer() < regionEpoch.GetConfVer() {
		return errors.Errorf("invalid region epoch, request: %v, currenrt: %v", reqRegionEpoch, regionEpoch)
	}
	return nil
}

func (c *RaftCluster) handleAskBatchSplit(request *pdpb.AskBatchSplitRequest) (*pdpb.AskBatchSplitResponse, error) {
	reqRegion := request.GetRegion()
	splitCount := request.GetSplitCount()
	err := c.validRequestRegion(reqRegion)
	if err != nil {
		return nil, errors.Trace(err)
	}
	splitIDs := make([]*pdpb.SplitID, 0, splitCount)

	// Disable merge the regions in a period of time.
	c.coordinator.mergeChecker.RecordRegionSplit(reqRegion.GetId())
	for i := 0; i < int(splitCount); i++ {
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

		c.coordinator.mergeChecker.RecordRegionSplit(newRegionID)
		splitIDs = append(splitIDs, &pdpb.SplitID{
			NewRegionId: newRegionID,
			NewPeerIds:  peerIDs,
		})
	}

	resp := &pdpb.AskBatchSplitResponse{Ids: splitIDs}

	return resp, nil
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

func (c *RaftCluster) checkSplitRegions(regions []*metapb.Region) error {
	if len(regions) <= 1 {
		return errors.New("invalid split region")
	}

	for i := 1; i < len(regions); i++ {
		left := regions[i-1]
		right := regions[i]
		if !bytes.Equal(left.GetEndKey(), right.GetStartKey()) {
			return errors.New("invalid split region")
		}
		if len(right.GetEndKey()) != 0 && bytes.Compare(left.GetStartKey(), right.GetEndKey()) >= 0 {
			return errors.New("invalid split region")
		}
	}
	return nil
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
	originRegion := proto.Clone(right).(*metapb.Region)
	originRegion.RegionEpoch = nil
	originRegion.StartKey = left.GetStartKey()
	log.Infof("[region %d] region split, generate new region: %v", originRegion.GetId(), left)
	return &pdpb.ReportSplitResponse{}, nil
}

func (c *RaftCluster) handleBatchReportSplit(request *pdpb.ReportBatchSplitRequest) (*pdpb.ReportBatchSplitResponse, error) {
	regions := request.GetRegions()

	err := c.checkSplitRegions(regions)
	if err != nil {
		log.Warnf("report batch split region is invalid - %v, %v", request, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}
	last := len(regions) - 1
	originRegion := proto.Clone(regions[last]).(*metapb.Region)
	log.Infof("[region %d] region split, generate %d new regions: %v", originRegion.GetId(), last, regions[:last])
	return &pdpb.ReportBatchSplitResponse{}, nil
}
