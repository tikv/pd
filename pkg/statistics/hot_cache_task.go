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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"context"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
)

// FlowItemTask indicates the task in flowItem queue
type FlowItemTask interface {
	runTask(cache *hotPeerCache)
}

type checkReadPeerTask struct {
	regionInfo *core.RegionInfo
	peers      []*metapb.Peer
	loads      []float64
	interval   uint64
}

// NewCheckReadPeerTask creates task to update peerInfo
func NewCheckReadPeerTask(regionInfo *core.RegionInfo, peers []*metapb.Peer, loads []float64, interval uint64) FlowItemTask {
	return &checkReadPeerTask{
		regionInfo: regionInfo,
		peers:      peers,
		loads:      loads,
		interval:   interval,
	}
}

func (t *checkReadPeerTask) runTask(cache *hotPeerCache) {
	stats := cache.checkPeerFlow(t.regionInfo, t.peers, t.loads, t.interval)
	for _, stat := range stats {
		cache.updateStat(stat)
	}
}

type checkWritePeerTask struct {
	region *core.RegionInfo
}

// NewCheckWritePeerTask creates task to update peerInfo
func NewCheckWritePeerTask(region *core.RegionInfo) FlowItemTask {
	return &checkWritePeerTask{
		region: region,
	}
}

func (t *checkWritePeerTask) runTask(cache *hotPeerCache) {
	stats := cache.checkPeerFlow(t.region, nil, nil, 0)
	for _, stat := range stats {
		cache.updateStat(stat)
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

func (t *checkExpiredTask) runTask(cache *hotPeerCache) {
	expiredStats := cache.collectExpiredItems(t.region)
	for _, stat := range expiredStats {
		cache.updateStat(stat)
	}
}

type collectUnReportedPeerTask struct {
	storeID  uint64
	regions  map[uint64]*core.RegionInfo
	interval uint64
}

// NewCollectUnReportedPeerTask creates task to collect unreported peers
func NewCollectUnReportedPeerTask(storeID uint64, regions map[uint64]*core.RegionInfo, interval uint64) FlowItemTask {
	return &collectUnReportedPeerTask{
		storeID:  storeID,
		regions:  regions,
		interval: interval,
	}
}

func (t *collectUnReportedPeerTask) runTask(cache *hotPeerCache) {
	stats := cache.checkColdPeer(t.storeID, t.regions, t.interval)
	for _, stat := range stats {
		cache.updateStat(stat)
	}
}

type collectRegionStatsTask struct {
	minDegree int
	ret       chan map[uint64][]*HotPeerStat
}

func newCollectRegionStatsTask(minDegree int) *collectRegionStatsTask {
	return &collectRegionStatsTask{
		minDegree: minDegree,
		ret:       make(chan map[uint64][]*HotPeerStat, 1),
	}
}

func (t *collectRegionStatsTask) runTask(cache *hotPeerCache) {
	t.ret <- cache.RegionStats(t.minDegree)
}

// TODO: do we need a wait-return timeout?
func (t *collectRegionStatsTask) waitRet(ctx context.Context) map[uint64][]*HotPeerStat {
	select {
	case <-ctx.Done():
		return nil
	case ret := <-t.ret:
		return ret
	}
}

type checkRegionHotTask struct {
	region       *core.RegionInfo
	minHotDegree int
	ret          chan bool
}

func newCheckRegionHotTask(region *core.RegionInfo, minDegree int) *checkRegionHotTask {
	return &checkRegionHotTask{
		region:       region,
		minHotDegree: minDegree,
		ret:          make(chan bool, 1),
	}
}

func (t *checkRegionHotTask) runTask(cache *hotPeerCache) {
	t.ret <- cache.isRegionHotWithAnyPeers(t.region, t.minHotDegree)
}

// TODO: do we need a wait-return timeout?
func (t *checkRegionHotTask) waitRet(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case r := <-t.ret:
		return r
	}
}

type collectMetricsTask struct {
}

func newCollectMetricsTask() *collectMetricsTask {
	return &collectMetricsTask{}
}

func (*collectMetricsTask) runTask(cache *hotPeerCache) {
	cache.collectMetrics()
}

type getHotPeerStatTask struct {
	regionID uint64
	storeID  uint64
	ret      chan *HotPeerStat
}

func newGetHotPeerStatTask(regionID, storeID uint64) *getHotPeerStatTask {
	return &getHotPeerStatTask{
		regionID: regionID,
		storeID:  storeID,
		ret:      make(chan *HotPeerStat, 1),
	}
}

func (t *getHotPeerStatTask) runTask(cache *hotPeerCache) {
	t.ret <- cache.getHotPeerStat(t.regionID, t.storeID)
}

// TODO: do we need a wait-return timeout?
func (t *getHotPeerStatTask) waitRet(ctx context.Context) *HotPeerStat {
	select {
	case <-ctx.Done():
		return nil
	case r := <-t.ret:
		return r
	}
}
