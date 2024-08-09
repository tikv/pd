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

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.uber.org/zap"
)

// HandleRegionHeartbeat processes RegionInfo reports from client.
func (c *RaftCluster) HandleRegionHeartbeat(region *core.RegionInfo) error {
	tracer := core.NewNoopHeartbeatProcessTracer()
	if c.GetScheduleConfig().EnableHeartbeatBreakdownMetrics {
		tracer = core.NewHeartbeatProcessTracer()
	}
	defer tracer.Release()
	var taskRunner, miscRunner, logRunner ratelimit.Runner
	taskRunner, miscRunner, logRunner = syncRunner, syncRunner, syncRunner
	if c.GetScheduleConfig().EnableHeartbeatConcurrentRunner {
		taskRunner = c.heartbeatRunner
		miscRunner = c.miscRunner
		logRunner = c.logRunner
	}

	ctx := &core.MetaProcessContext{
		Context:    c.ctx,
		Tracer:     tracer,
		TaskRunner: taskRunner,
		MiscRunner: miscRunner,
		LogRunner:  logRunner,
	}
	tracer.Begin()
	if err := c.processRegionHeartbeat(ctx, region); err != nil {
		tracer.OnAllStageFinished()
		return err
	}
	tracer.OnAllStageFinished()

	if c.IsServiceIndependent(constant.SchedulingServiceName) {
		return nil
	}
	c.coordinator.GetOperatorController().Dispatch(region, operator.DispatchFromHeartBeat, c.coordinator.RecordOpStepWithTTL)
	return nil
}

// HandleAskSplit handles the split request.
func (c *RaftCluster) HandleAskSplit(request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	if c.IsSchedulingHalted() {
		return nil, errs.ErrSchedulingIsHalted.FastGenByArgs()
	}
	if !c.opt.IsTikvRegionSplitEnabled() {
		return nil, errs.ErrSchedulerTiKVSplitDisabled.FastGenByArgs()
	}
	reqRegion := request.GetRegion()
	err := c.ValidRegion(reqRegion)
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

// HandleAskBatchSplit handles the batch split request.
func (c *RaftCluster) HandleAskBatchSplit(request *pdpb.AskBatchSplitRequest) (*pdpb.AskBatchSplitResponse, error) {
	if c.IsSchedulingHalted() {
		return nil, errs.ErrSchedulingIsHalted.FastGenByArgs()
	}
	if !c.opt.IsTikvRegionSplitEnabled() {
		return nil, errs.ErrSchedulerTiKVSplitDisabled.FastGenByArgs()
	}
	reqRegion := request.GetRegion()
	splitCount := request.GetSplitCount()
	err := c.ValidRegion(reqRegion)
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
	c.AddPendingProcessedRegions(false, recordRegions...)

	resp := &pdpb.AskBatchSplitResponse{Ids: splitIDs}

	return resp, nil
}

func checkSplitRegion(left *metapb.Region, right *metapb.Region) error {
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

func checkSplitRegions(regions []*metapb.Region) error {
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
func (*RaftCluster) HandleReportSplit(request *pdpb.ReportSplitRequest) (*pdpb.ReportSplitResponse, error) {
	left := request.GetLeft()
	right := request.GetRight()

	err := checkSplitRegion(left, right)
	if err != nil {
		log.Warn("report split region is invalid",
			logutil.ZapRedactStringer("left-region", core.RegionToHexMeta(left)),
			logutil.ZapRedactStringer("right-region", core.RegionToHexMeta(right)),
			errs.ZapError(err))
		return nil, err
	}

	// Build origin region by using left and right.
	originRegion := typeutil.DeepClone(right, core.RegionFactory)
	originRegion.RegionEpoch = nil
	originRegion.StartKey = left.GetStartKey()
	log.Info("region split, generate new region",
		zap.Uint64("region-id", originRegion.GetId()),
		logutil.ZapRedactStringer("region-meta", core.RegionToHexMeta(left)))
	return &pdpb.ReportSplitResponse{}, nil
}

// HandleBatchReportSplit handles the batch report split request.
func (*RaftCluster) HandleBatchReportSplit(request *pdpb.ReportBatchSplitRequest) (*pdpb.ReportBatchSplitResponse, error) {
	regions := request.GetRegions()

	hrm := core.RegionsToHexMeta(regions)
	err := checkSplitRegions(regions)
	if err != nil {
		log.Warn("report batch split region is invalid",
			logutil.ZapRedactStringer("region-meta", hrm),
			errs.ZapError(err))
		return nil, err
	}
	last := len(regions) - 1
	originRegion := typeutil.DeepClone(regions[last], core.RegionFactory)
	hrm = core.RegionsToHexMeta(regions[:last])
	log.Info("region batch split, generate new regions",
		zap.Uint64("region-id", originRegion.GetId()),
		logutil.ZapRedactStringer("origin", hrm),
		zap.Int("total", last))
	return &pdpb.ReportBatchSplitResponse{}, nil
}

// HandleReportBuckets processes buckets reports from client
func (c *RaftCluster) HandleReportBuckets(b *metapb.Buckets) error {
	if err := c.processReportBuckets(b); err != nil {
		return err
	}
	if !c.IsServiceIndependent(constant.SchedulingServiceName) {
		c.hotStat.CheckAsync(buckets.NewCheckPeerTask(b))
	}
	return nil
}
