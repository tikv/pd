// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"bytes"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.uber.org/zap"
)

// store doesn't pick balance leader source if the split region is bigger than maxSplitThreshold.
const maxSplitThreshold = 10

// HandleRegionHeartbeat processes RegionInfo reports from client.
func (c *RaftCluster) HandleRegionHeartbeat(region *core.RegionInfo) error {
	if err := c.processRegionHeartbeat(region); err != nil {
		return err
	}

	c.coordinator.GetOperatorController().Dispatch(region, operator.DispatchFromHeartBeat, c.coordinator.RecordOpStepWithTTL)
	return nil
}

// ProcessRegionSplit to process split region into region cache.
// it's different with the region heartbeat, it's only fill some new region into the region cache.
// so it doesn't consider the leader and hot statistics.
func (c *RaftCluster) ProcessRegionSplit(regions []*metapb.Region) []error {
	if err := c.checkSplitRegions(regions); err != nil {
		return []error{err}
	}
	total := len(regions) - 1
	regions[0], regions[total] = regions[total], regions[0]
	leaderStoreID := uint64(0)
	if r := c.core.GetRegion(regions[0].GetId()); r != nil {
		leaderStoreID = r.GetLeader().GetStoreId()
	}
	if leaderStoreID == 0 {
		return []error{errors.New("origin region no leader")}
	}
	leaderStore := c.GetStore(leaderStoreID)
	if leaderStore == nil {
		return []error{errors.New("leader store not found")}
	}
	errList := make([]error, 0, total)
	for _, region := range regions {
		if len(region.GetPeers()) == 0 {
			errList = append(errList, errors.New(fmt.Sprintf("region:%d has no peer", region.GetId())))
			continue
		}
		// region split initiator store will be leader with a high probability
		leader := region.Peers[0]
		if leaderStoreID > 0 {
			for _, peer := range region.GetPeers() {
				if peer.GetStoreId() == leaderStoreID {
					leader = peer
					break
				}
			}
		}
		region := core.NewRegionInfo(region, leader)
		changed := &core.RegionChanged{
			IsNew: true, SaveKV: true, SaveCache: true, NeedSync: true,
		}
		if err := c.SaveRegion(region, changed); err != nil {
			errList = append(errList, err)
		}
	}
	// If the number of regions exceeds the threshold, update the last split time.
	if len(regions) >= maxSplitThreshold {
		newStore := leaderStore.Clone(core.SetRecentlySplitRegionsTime(time.Now()))
		c.core.PutStore(newStore)
	}
	return errList
}

// HandleAskSplit handles the split request.
func (c *RaftCluster) HandleAskSplit(request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	if c.isSchedulingHalted() {
		return nil, errs.ErrSchedulingIsHalted.FastGenByArgs()
	}
	if !c.opt.IsTikvRegionSplitEnabled() {
		return nil, errs.ErrSchedulerTiKVSplitDisabled.FastGenByArgs()
	}
	reqRegion := request.GetRegion()
	err := c.ValidRequestRegion(reqRegion)
	if err != nil {
		return nil, err
	}

	if repMode := c.GetReplicationMode(); repMode != nil && repMode.IsRegionSplitPaused() {
		return nil, errors.New("region split is paused by replication mode")
	}

	newRegionID, err := c.id.Alloc()
	if err != nil {
		return nil, err
	}

	peerIDs := make([]uint64, len(request.Region.Peers))
	for i := 0; i < len(peerIDs); i++ {
		if peerIDs[i], err = c.id.Alloc(); err != nil {
			return nil, err
		}
	}

	if versioninfo.IsFeatureSupported(c.GetOpts().GetClusterVersion(), versioninfo.RegionMerge) {
		// Disable merge for the 2 regions in a period of time.
		c.GetMergeChecker().RecordRegionSplit([]uint64{reqRegion.GetId(), newRegionID})
	}

	split := &pdpb.AskSplitResponse{
		NewRegionId: newRegionID,
		NewPeerIds:  peerIDs,
	}

	log.Info("alloc ids for region split", zap.Uint64("region-id", newRegionID), zap.Uint64s("peer-ids", peerIDs))

	return split, nil
}

func (c *RaftCluster) isSchedulingHalted() bool {
	return c.opt.IsSchedulingHalted()
}

// ValidRequestRegion is used to decide if the region is valid.
func (c *RaftCluster) ValidRequestRegion(reqRegion *metapb.Region) error {
	startKey := reqRegion.GetStartKey()
	region := c.GetRegionByKey(startKey)
	if region == nil {
		return errors.Errorf("region not found, request region: %v", logutil.RedactStringer(core.RegionToHexMeta(reqRegion)))
	}
	// If the request epoch is less than current region epoch, then returns an error.
	reqRegionEpoch := reqRegion.GetRegionEpoch()
	regionEpoch := region.GetMeta().GetRegionEpoch()
	if reqRegionEpoch.GetVersion() < regionEpoch.GetVersion() ||
		reqRegionEpoch.GetConfVer() < regionEpoch.GetConfVer() {
		return errors.Errorf("invalid region epoch, request: %v, current: %v", reqRegionEpoch, regionEpoch)
	}
	return nil
}

// HandleAskBatchSplit handles the batch split request.
func (c *RaftCluster) HandleAskBatchSplit(request *pdpb.AskBatchSplitRequest) (*pdpb.AskBatchSplitResponse, error) {
	if c.isSchedulingHalted() {
		return nil, errs.ErrSchedulingIsHalted.FastGenByArgs()
	}
	if !c.opt.IsTikvRegionSplitEnabled() {
		return nil, errs.ErrSchedulerTiKVSplitDisabled.FastGenByArgs()
	}
	reqRegion := request.GetRegion()
	splitCount := request.GetSplitCount()
	err := c.ValidRequestRegion(reqRegion)
	if err != nil {
		return nil, err
	}
	if repMode := c.GetReplicationMode(); repMode != nil && repMode.IsRegionSplitPaused() {
		return nil, errors.New("region split is paused by replication mode")
	}
	splitIDs := make([]*pdpb.SplitID, 0, splitCount)
	recordRegions := make([]uint64, 0, splitCount+1)

	for i := 0; i < int(splitCount); i++ {
		newRegionID, err := c.id.Alloc()
		if err != nil {
			return nil, errs.ErrSchedulerNotFound.FastGenByArgs()
		}

		peerIDs := make([]uint64, len(request.Region.Peers))
		for i := 0; i < len(peerIDs); i++ {
			if peerIDs[i], err = c.id.Alloc(); err != nil {
				return nil, err
			}
		}

		recordRegions = append(recordRegions, newRegionID)
		splitIDs = append(splitIDs, &pdpb.SplitID{
			NewRegionId: newRegionID,
			NewPeerIds:  peerIDs,
		})

		log.Info("alloc ids for region split", zap.Uint64("region-id", newRegionID), zap.Uint64s("peer-ids", peerIDs))
	}

	recordRegions = append(recordRegions, reqRegion.GetId())
	if versioninfo.IsFeatureSupported(c.GetOpts().GetClusterVersion(), versioninfo.RegionMerge) {
		// Disable merge the regions in a period of time.
		c.GetMergeChecker().RecordRegionSplit(recordRegions)
	}

	// If region splits during the scheduling process, regions with abnormal
	// status may be left, and these regions need to be checked with higher
	// priority.
	c.AddSuspectRegions(recordRegions...)

	resp := &pdpb.AskBatchSplitResponse{Ids: splitIDs}

	return resp, nil
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

// HandleReportSplit handles the report split request.
func (c *RaftCluster) HandleReportSplit(request *pdpb.ReportSplitRequest) (*pdpb.ReportSplitResponse, error) {
	left := request.GetLeft()
	right := request.GetRight()

	if errs := c.ProcessRegionSplit([]*metapb.Region{left, right}); len(errs) > 0 {
		log.Warn("report split region is invalid",
			logutil.ZapRedactStringer("left-region", core.RegionToHexMeta(left)),
			logutil.ZapRedactStringer("right-region", core.RegionToHexMeta(right)),
			zap.Errors("errs", errs),
		)
		// error[0] may be checker error, others are ignored.
		return nil, errs[0]
	}

	log.Info("region split, generate new region",
		zap.Uint64("region-id", right.GetId()),
		logutil.ZapRedactStringer("region-meta", core.RegionToHexMeta(left)))
	return &pdpb.ReportSplitResponse{}, nil
}

// HandleBatchReportSplit handles the batch report split request.
func (c *RaftCluster) HandleBatchReportSplit(request *pdpb.ReportBatchSplitRequest) (*pdpb.ReportBatchSplitResponse, error) {
	regions := request.GetRegions()
	hrm := core.RegionsToHexMeta(regions)
	if errs := c.ProcessRegionSplit(regions); len(errs) > 0 {
		log.Warn("report batch split region is invalid",
			zap.Stringer("region-meta", hrm),
			zap.Errors("errs", errs))
		// error[0] may be checker error, others are ignored.
		return nil, errs[0]
	}
	last := len(regions) - 1
	originRegionID := regions[last].GetId()
	log.Info("region batch split, generate new regions",
		zap.Uint64("region-id", originRegionID),
		zap.Stringer("new-peer", hrm[:last]),
		zap.Int("total", last))
	return &pdpb.ReportBatchSplitResponse{}, nil
}

// HandleReportBuckets processes buckets reports from client
func (c *RaftCluster) HandleReportBuckets(b *metapb.Buckets) error {
	if err := c.processReportBuckets(b); err != nil {
		return err
	}
	c.hotStat.CheckAsync(buckets.NewCheckPeerTask(b))
	return nil
}
