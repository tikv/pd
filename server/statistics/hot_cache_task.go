// Copyright 2021 TiKV Project Authors.
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

package statistics

import "github.com/tikv/pd/server/core"

type flowItemTaskKind uint32

const (
	CheckPeerTaskType flowItemTaskKind = iota
	CheckExpiredTaskType
	CollectUnReportedPeerTaskType
	CollectRegionStatsTaskType
)

type FlowItemTask interface {
	taskType() flowItemTaskKind
	runTask(flow *hotPeerCache)
}

type checkPeerTask struct {
	peerInfo   *core.PeerInfo
	regionInfo *core.RegionInfo
}

// NewCheckPeerTask ...
func NewCheckPeerTask(peerInfo *core.PeerInfo, regionInfo *core.RegionInfo) FlowItemTask {
	return &checkPeerTask{
		peerInfo:   peerInfo,
		regionInfo: regionInfo,
	}
}

func (t *checkPeerTask) taskType() flowItemTaskKind {
	return CheckPeerTaskType
}

func (t *checkPeerTask) runTask(flow *hotPeerCache) {
	stat := flow.CheckPeerFlow(t.peerInfo, t.regionInfo)
	if stat != nil {
		update(stat, flow)
	}
}

type checkExpiredTask struct {
	region *core.RegionInfo
}

// NewCheckExpiredItemTask ...
func NewCheckExpiredItemTask(region *core.RegionInfo) FlowItemTask {
	return &checkExpiredTask{
		region: region,
	}
}

func (t *checkExpiredTask) taskType() flowItemTaskKind {
	return CheckExpiredTaskType
}

func (t *checkExpiredTask) runTask(flow *hotPeerCache) {
	expiredStats := flow.CollectExpiredItems(t.region)
	for _, stat := range expiredStats {
		update(stat, flow)
	}
}

type collectUnReportedPeerTask struct {
	storeID   uint64
	regionIDs map[uint64]struct{}
	interval  uint64
}

// NewCollectUnReportedPeerTask ...
func NewCollectUnReportedPeerTask(storeID uint64, regionIDs map[uint64]struct{}, interval uint64) FlowItemTask {
	return &collectUnReportedPeerTask{
		storeID:   storeID,
		regionIDs: regionIDs,
		interval:  interval,
	}
}

func (t *collectUnReportedPeerTask) taskType() flowItemTaskKind {
	return CollectUnReportedPeerTaskType
}

func (t *collectUnReportedPeerTask) runTask(flow *hotPeerCache) {
	stats := flow.CheckColdPeer(t.storeID, t.regionIDs, t.interval)
	for _, stat := range stats {
		update(stat, flow)
	}
}

type collectRegionStatsTask struct {
	minDegree int
	ret       chan map[uint64][]*HotPeerStat
}

func newCollectRegionStatsTask(minDegree int) FlowItemTask {
	return &collectRegionStatsTask{
		minDegree: minDegree,
		ret:       make(chan map[uint64][]*HotPeerStat),
	}
}

func (t *collectRegionStatsTask) taskType() flowItemTaskKind {
	return CollectRegionStatsTaskType
}

func (t *collectRegionStatsTask) runTask(flow *hotPeerCache) {
	t.ret <- flow.RegionStats(t.minDegree)
}
