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

import (
	"context"
	"time"

	"github.com/tikv/pd/server/core"
)

// FlowItemTask indicates the task in flowItem queue
type FlowItemTask interface {
	taskType() string
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

func (t *checkPeerTask) taskType() string {
	return "checkPeerTask"
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

func (t *checkExpiredTask) taskType() string {
	return "checkExpiredTask"
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

func (t *collectUnReportedPeerTask) taskType() string {
	return "collectUnReportedPeerTask"
}

func (t *collectUnReportedPeerTask) runTask(flow *hotPeerCache) {
	stats := flow.CheckColdPeer(t.storeID, t.regionIDs, t.interval)
	for _, stat := range stats {
		update(stat, flow)
	}
}

type collectRegionStatsTask struct {
	rw        string
	minDegree int
	ret       chan map[uint64][]*HotPeerStat
}

func newCollectRegionStatsTask(minDegree int, rw string) *collectRegionStatsTask {
	return &collectRegionStatsTask{
		rw:        rw,
		minDegree: minDegree,
		ret:       make(chan map[uint64][]*HotPeerStat, 1),
	}
}

func (t *collectRegionStatsTask) taskType() string {
	return "collectRegionStatsTask"
}

func (t *collectRegionStatsTask) runTask(flow *hotPeerCache) {
	t.ret <- flow.RegionStats(t.minDegree)
}

// TODO: do we need a wait-return timeout?
func (t *collectRegionStatsTask) waitRet(ctx context.Context, quit <-chan struct{}) map[uint64][]*HotPeerStat {
	start := time.Now()
	defer func() {
		hotCacheFlowTaskWaitDurationHist.WithLabelValues(t.taskType(), t.rw).Observe(time.Since(start).Seconds())
	}()
	select {
	case <-ctx.Done():
		return nil
	case <-quit:
		return nil
	case ret := <-t.ret:
		return ret
	}
}

type isRegionHotTask struct {
	rw           string
	region       *core.RegionInfo
	minHotDegree int
	ret          chan bool
}

func newIsRegionHotTask(region *core.RegionInfo, minDegree int, rw string) *isRegionHotTask {
	return &isRegionHotTask{
		rw:           rw,
		region:       region,
		minHotDegree: minDegree,
		ret:          make(chan bool, 1),
	}
}

func (t *isRegionHotTask) taskType() string {
	return "isRegionHotTask"
}

func (t *isRegionHotTask) runTask(flow *hotPeerCache) {
	t.ret <- flow.isRegionHotWithAnyPeers(t.region, t.minHotDegree)
}

// TODO: do we need a wait-return timeout?
func (t *isRegionHotTask) waitRet(ctx context.Context, quit <-chan struct{}) bool {
	start := time.Now()
	defer func() {
		hotCacheFlowTaskWaitDurationHist.WithLabelValues(t.taskType(), t.rw).Observe(time.Since(start).Seconds())
	}()
	select {
	case <-ctx.Done():
		return false
	case <-quit:
		return false
	case r := <-t.ret:
		return r
	}
}

type collectMetricsTask struct {
	rw string
}

func newCollectMetricsTask(rw string) *collectMetricsTask {
	return &collectMetricsTask{
		rw: rw,
	}
}

func (t *collectMetricsTask) taskType() string {
	return "collectMetricsTask"
}

func (t *collectMetricsTask) runTask(flow *hotPeerCache) {
	flow.CollectMetrics(t.rw)
}
