// Copyright 2023 TiKV Project Authors.
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
	"context"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/statistics"
)

// Cluster provides an overview of a cluster's basic information.
type Cluster interface {
	GetHotStat() *statistics.HotStat
	GetRegionStats() *statistics.RegionStatistics
	GetLabelStats() *statistics.LabelStatistics
	GetCoordinator() *schedule.Coordinator
	GetRuleManager() *placement.RuleManager
	GetBasicCluster() *core.BasicCluster
}

// HandleStatsAsync handles the flow asynchronously.
func HandleStatsAsync(c Cluster, region *core.RegionInfo) {
	checkWritePeerTask := func(cache *statistics.HotPeerCache) {
		reportInterval := region.GetInterval()
		interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
		stats := cache.CheckPeerFlow(region, region.GetPeers(), region.GetWriteLoads(), interval)
		for _, stat := range stats {
			cache.UpdateStat(stat)
		}
	}

	checkExpiredTask := func(cache *statistics.HotPeerCache) {
		expiredStats := cache.CollectExpiredItems(region)
		for _, stat := range expiredStats {
			cache.UpdateStat(stat)
		}
	}

	c.GetHotStat().CheckWriteAsync(checkExpiredTask)
	c.GetHotStat().CheckReadAsync(checkExpiredTask)
	c.GetHotStat().CheckWriteAsync(checkWritePeerTask)
	c.GetCoordinator().GetSchedulersController().CheckTransferWitnessLeader(region)
}

// HandleOverlaps handles the overlap regions.
func HandleOverlaps(ctx context.Context, c Cluster, overlaps []*core.RegionInfo) {
	for _, item := range overlaps {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if c.GetRegionStats() != nil {
			c.GetRegionStats().ClearDefunctRegion(item.GetID())
		}
		c.GetLabelStats().MarkDefunctRegion(item.GetID())
		c.GetRuleManager().InvalidCache(item.GetID())
	}
}

// Collect collects the cluster information.
func Collect(ctx context.Context, c Cluster, region *core.RegionInfo) {
	// get region again from root tree. make sure the observed region is the latest.
	bc := c.GetBasicCluster()
	if bc == nil {
		return
	}
	region = bc.GetRegion(region.GetID())
	if region == nil {
		return
	}
	select {
	case <-ctx.Done():
		return
	default:
	}
	c.GetRegionStats().Observe(region, c.GetBasicCluster().GetRegionStores(region))
}
