// Copyright 2026 TiKV Project Authors.
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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
)

func newWriteTransferTestRegion(id, writtenBytes, interval uint64) *core.RegionInfo {
	peers := make([]*metapb.Peer, 3)
	for i := range peers {
		peers[i] = &metapb.Peer{Id: id*10 + uint64(i) + 1, StoreId: (id+uint64(i))%5 + 1}
	}
	return core.NewRegionInfo(&metapb.Region{
		Id: id, Peers: peers,
		StartKey: []byte(fmt.Sprintf("%08d", id)),
		EndKey:   []byte(fmt.Sprintf("%08d", id+1)),
	}, peers[0], core.SetWrittenBytes(writtenBytes), core.SetReportInterval(0, interval))
}

func newWriteTransferTestCache(t *testing.T) (*HotCache, *core.BasicCluster) {
	t.Helper()
	cluster := core.NewBasicCluster()
	for id := uint64(1); id <= 5; id++ {
		cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: id}))
	}
	return NewHotCache(t.Context(), cluster), cluster
}

func TestColdRegionLeaderTransferDoesNotRaiseWriteThreshold(t *testing.T) {
	for _, historicalBytes := range []uint64{0, 2 << 20} {
		t.Run(fmt.Sprintf("historical-bytes-%d", historicalBytes), func(t *testing.T) {
			re := require.New(t)
			cache, cluster := newWriteTransferTestCache(t)
			// Both heartbeat handlers submit statistics before publishing the region.
			report := func(region *core.RegionInfo) {
				cache.CheckRegionFlowAsync(region)
				cluster.PutRegion(region)
			}
			countSteadyPeers := func() int {
				count := 0
				for _, peers := range cache.GetHotPeerStats(utils.Write, 3) {
					for _, peer := range peers {
						if peer.RegionID >= 10000 {
							count++
						}
					}
				}
				return count
			}
			steady := make([]*core.RegionInfo, 32)
			for i := range steady {
				steady[i] = newWriteTransferTestRegion(uint64(10000+i), 2400*60, 60)
			}
			for range 12 {
				for _, region := range steady {
					report(region)
				}
			}
			re.Equal(96, countSteadyPeers())

			cold := make([]*core.RegionInfo, 128)
			for i := range cold {
				cold[i] = newWriteTransferTestRegion(uint64(i+1), 0, 60)
				report(cold[i])
			}
			re.Equal(96, countSteadyPeers())
			for _, region := range cold {
				// A returning leader reports writes accumulated before the idle period.
				report(region.Clone(core.WithLeader(region.GetPeers()[1]), core.SetWrittenBytes(historicalBytes), core.SetReportInterval(0, 500)))
			}
			thresholds := make(chan map[uint64]float64, 1)
			re.True(cache.CheckWriteAsync(func(writeCache *HotPeerCache) {
				// Let the usual threshold refresh occur before another cold report.
				for _, threshold := range writeCache.thresholdsOfStore {
					threshold.updatedTime = time.Now().Add(-2 * ThresholdsUpdateInterval)
				}
				rates := make(map[uint64]float64)
				for id := uint64(1); id <= 5; id++ {
					rates[id] = writeCache.calcHotThresholds(id)[utils.ByteDim]
				}
				thresholds <- rates
			}))
			rates := <-thresholds
			t.Logf("historical_bytes=%d thresholds=%v", historicalBytes, rates)
			for _, region := range steady {
				report(region)
			}
			re.Equal(96, countSteadyPeers(), "inactive regions must not cool continuously hot peers after a leader transfer")
			for _, rate := range rates {
				re.Equal(1024.0, rate)
			}
		})
	}
}

func TestWriteRegionLeaderTransferSample(t *testing.T) {
	for _, tc := range []struct {
		name                                     string
		known, unknownLeader, cached, sameLeader bool
	}{
		{name: "cold-transfer", known: true},
		{name: "hot-transfer", known: true, cached: true},
		{name: "new-region"},
		{name: "unknown-leader", known: true, unknownLeader: true},
		{name: "unchanged-leader", known: true, sameLeader: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cache, cluster := newWriteTransferTestCache(t)
			region := newWriteTransferTestRegion(1, 8000*60, 60)
			if tc.known {
				origin := region
				if tc.unknownLeader {
					origin = origin.Clone(core.WithLeader(nil))
				}
				cluster.PutRegion(origin)
			}
			if tc.cached {
				for range 3 {
					cache.CheckRegionFlowAsync(region)
				}
			}
			old := cache.GetHotPeerStat(utils.Write, region.GetID(), region.GetPeers()[0].GetStoreId())
			oldDegree := 0
			if old != nil {
				oldDegree = old.HotDegree
			}

			// Publish the new leader before the queued sample runs. Transfer
			// detection must use the heartbeat's original metadata snapshot.
			entered, release := make(chan struct{}), make(chan struct{})
			re.True(cache.CheckWriteAsync(func(*HotPeerCache) {
				close(entered)
				select {
				case <-release:
				case <-t.Context().Done():
				}
			}))
			<-entered
			moved := region
			if !tc.sameLeader {
				moved = moved.Clone(core.WithLeader(region.GetPeers()[1]))
			}
			moved = moved.Clone(core.SetWrittenBytes(2<<20), core.SetReportInterval(0, 500))
			cache.CheckRegionFlowAsync(moved)
			cluster.PutRegion(moved)
			close(release)

			skipped := tc.known && !tc.unknownLeader && !tc.sameLeader
			for _, peer := range moved.GetPeers() {
				stat := cache.GetHotPeerStat(utils.Write, moved.GetID(), peer.GetStoreId())
				switch {
				case skipped && !tc.cached:
					re.Nil(stat, "a stale first sample must not create a cache entry")
				case skipped:
					re.NotNil(stat)
					re.Equal(oldDegree, stat.HotDegree)
					re.Equal(8000.0, stat.GetLoad(utils.ByteDim))
					re.Equal(peer.GetStoreId() == moved.GetLeader().GetStoreId(), stat.IsLeader())
				default:
					re.NotNil(stat, "valid new hot regions must still be admitted")
				}
			}
			fresh := moved.Clone(core.SetWrittenBytes(8000*60), core.SetReportInterval(500, 560))
			cache.CheckRegionFlowAsync(fresh)
			cluster.PutRegion(fresh)
			stat := cache.GetHotPeerStat(utils.Write, fresh.GetID(), fresh.GetLeader().GetStoreId())
			re.NotNil(stat)
			if tc.cached {
				re.Equal(oldDegree+1, stat.HotDegree, "the second sample must not be skipped again")
			}
			for range 3 {
				cache.CheckRegionFlowAsync(fresh)
			}
			re.Len(cache.GetHotPeerStats(utils.Write, 3), 3, "fresh sustained writes must become schedulable")
		})
	}
}
