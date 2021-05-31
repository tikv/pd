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
	checkPeerTaskType flowItemTaskKind = iota
	checkExpiredTaskType
	collectUnReportedPeerTaskType
	collectRegionStatsTaskType
	isRegionHotTaskType
	collectMetricsTaskType
)

// FlowItemTask indicates the task in flowItem queue
type FlowItemTask interface {
	taskType() flowItemTaskKind
	runTask(flow *hotPeerCache)
}

type checkPeerTask struct {
	peerInfo   *core.PeerInfo
	regionInfo *core.RegionInfo
}

// NewCheckPeerTask creates task to update peerInfo
func NewCheckPeerTask(peerInfo *core.PeerInfo, regionInfo *core.RegionInfo) FlowItemTask {
	return &checkPeerTask{
		peerInfo:   peerInfo,
		regionInfo: regionInfo,
	}
}

func (t *checkPeerTask) taskType() flowItemTaskKind {
	return checkPeerTaskType
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

// NewCheckExpiredItemTask creates task to collect expired items
func NewCheckExpiredItemTask(region *core.RegionInfo) FlowItemTask {
	return &checkExpiredTask{
		region: region,
	}
}

func (t *checkExpiredTask) taskType() flowItemTaskKind {
	return checkExpiredTaskType
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

// NewCollectUnReportedPeerTask creates task to collect unreported peers
func NewCollectUnReportedPeerTask(storeID uint64, regionIDs map[uint64]struct{}, interval uint64) FlowItemTask {
	return &collectUnReportedPeerTask{
		storeID:   storeID,
		regionIDs: regionIDs,
		interval:  interval,
	}
}

func (t *collectUnReportedPeerTask) taskType() flowItemTaskKind {
	return collectUnReportedPeerTaskType
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

func newCollectRegionStatsTask(minDegree int) *collectRegionStatsTask {
	return &collectRegionStatsTask{
		minDegree: minDegree,
		ret:       make(chan map[uint64][]*HotPeerStat),
	}
}

func (t *collectRegionStatsTask) taskType() flowItemTaskKind {
	return collectRegionStatsTaskType
}

func (t *collectRegionStatsTask) runTask(flow *hotPeerCache) {
	t.ret <- flow.RegionStats(t.minDegree)
}

type isRegionHotTask struct {
	region       *core.RegionInfo
	minHotDegree int
	ret          chan bool
}

func newIsRegionHotTask(region *core.RegionInfo, minDegree int) *isRegionHotTask {
	return &isRegionHotTask{
		region:       region,
		minHotDegree: minDegree,
		ret:          make(chan bool),
	}
}

func (t *isRegionHotTask) taskType() flowItemTaskKind {
	return isRegionHotTaskType
}

func (t *isRegionHotTask) runTask(flow *hotPeerCache) {
	t.ret <- flow.isRegionHotWithAnyPeers(t.region, t.minHotDegree)
}

type collectMetricsTask struct {
	typ  string
	done chan struct{}
}

func newCollectMetricsTask(typ string) *collectMetricsTask {
	return &collectMetricsTask{
		typ:  typ,
		done: make(chan struct{}),
	}
}

func (t *collectMetricsTask) taskType() flowItemTaskKind {
	return collectMetricsTaskType
}

func (t *collectMetricsTask) runTask(flow *hotPeerCache) {
	flow.CollectMetrics(t.typ)
	t.done <- struct{}{}
}
