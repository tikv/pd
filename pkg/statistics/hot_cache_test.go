// Copyright 2024 TiKV Project Authors.
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
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sasha-s/go-deadlock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	// Pooled deadlock timers cannot cross synctest bubble boundaries.
	deadlock.Opts.TimerPool = deadlock.TimerPoolDisabled
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func newReadProjectionTestCache(t *testing.T) (*HotCache, *core.BasicCluster, *core.RegionInfo, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cluster := core.NewBasicCluster()
	region := newBenchmarkRegion(1, 3).Clone(core.WithStartKey([]byte("a")), core.WithEndKey([]byte("b")))
	for _, peer := range region.GetPeers() {
		cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: peer.GetStoreId()}))
	}
	cluster.PutRegion(region)
	return NewHotCache(ctx, cluster), cluster, region, cancel
}

func reportReadProjectionSample(t *testing.T, cache *HotCache, region *core.RegionInfo, peer *metapb.Peer, byteRate float64) {
	t.Helper()
	interval := uint64(utils.StoreHeartBeatReportInterval)
	loads := make([]float64, utils.RegionStatCount)
	loads[utils.RegionReadBytes] = byteRate * float64(interval)
	loads[utils.RegionReadKeys] = 1024 * float64(interval)
	require.True(t, cache.CheckReadPeerAsync(region, peer, loads, interval))
}

func seedReadProjectionSamples(t *testing.T, cache *HotCache, region *core.RegionInfo, peers ...*metapb.Peer) {
	t.Helper()
	if len(peers) == 0 {
		peers = region.GetPeers()
	}
	for range 5 {
		for _, peer := range peers {
			reportReadProjectionSample(t, cache, region, peer, float64(peer.GetStoreId())*(1<<20))
		}
	}
}

func readProjectionRawPeer(t *testing.T, cache *HotCache, regionID, storeID uint64) *HotPeerStat {
	t.Helper()
	result := make(chan *HotPeerStat, 1)
	require.True(t, cache.CheckReadAsync(func(cache *HotPeerCache) {
		result <- cache.getHotPeerStat(regionID, storeID)
	}))
	return <-result
}

func TestReadHotPeerSnapshotSurvivesRemoval(t *testing.T) {
	for _, clearAll := range []bool{false, true} {
		t.Run(fmt.Sprintf("clear-all-%t", clearAll), func(t *testing.T) {
			re := require.New(t)
			cache, _, region, cancel := newReadProjectionTestCache(t)
			defer cancel()
			leader := region.GetLeader()
			seedReadProjectionSamples(t, cache, region, leader)
			snapshot := readProjectionRawPeer(t, cache, region.GetID(), leader.GetStoreId())
			re.NotNil(snapshot)
			before := *snapshot
			re.Same(snapshot, cache.GetHotPeerStat(utils.Read, region.GetID(), leader.GetStoreId()),
				"an unchanged role must not clone the sample")

			removed := make(chan []*HotPeerStat, 1)
			re.True(cache.CheckReadAsync(func(owner *HotPeerCache) {
				if clearAll {
					owner.removeAllItem()
					removed <- nil
					return
				}
				items := owner.CollectExpiredItems(region.Clone(core.WithRemoveStorePeer(leader.GetStoreId())))
				for _, item := range items {
					owner.UpdateStat(item)
				}
				removed <- items
			}))
			items := <-removed
			re.Nil(readProjectionRawPeer(t, cache, region.GetID(), leader.GetStoreId()))
			re.Equal(before, *snapshot, "cache removal must not mutate a snapshot already handed to a reader")
			if !clearAll {
				re.Len(items, 1)
				re.NotSame(snapshot, items[0])
				re.Equal(utils.Remove, items[0].GetActionType())
				re.Same(snapshot.rollingLoads[0], items[0].rollingLoads[0], "removal only copies the sample header")
				re.Same(&snapshot.Loads[0], &items[0].Loads[0])
			}
		})
	}
}

func TestReadHotPeerStatsUseCurrentRegionLeader(t *testing.T) {
	re := require.New(t)
	cache, cluster, region, cancel := newReadProjectionTestCache(t)
	defer cancel()
	seedReadProjectionSamples(t, cache, region)
	before := cache.GetHotPeerStats(utils.Read, 3)
	for _, peer := range region.GetPeers() {
		re.Len(before[peer.GetStoreId()], 1)
	}

	// Only region metadata changes; none of the stores has sent another sample.
	moved := region.Clone(core.WithLeader(region.GetPeers()[1]))
	cluster.PutRegion(moved)
	after := cache.GetHotPeerStats(utils.Read, 3)
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		re.Len(after[storeID], 1)
		old, current := before[storeID][0], after[storeID][0]
		re.Equal(storeID == moved.GetLeader().GetStoreId(), current.IsLeader())
		re.Equal(float64(storeID)*(1<<20), current.GetLoad(utils.ByteDim))
		re.Equal(old.GetLoads(), current.GetLoads(), "metadata must not move load between peers")
		re.Equal(old.HotDegree, current.HotDegree)
		re.Equal(old.AntiCount, current.AntiCount)
		re.Equal(storeID == region.GetLeader().GetStoreId(), old.IsLeader(), "an earlier result must retain its role")
		raw := readProjectionRawPeer(t, cache, region.GetID(), storeID)
		re.Equal(old.isLeader, raw.isLeader, "metadata must preserve the sampled role")
		re.Same(old.rollingLoads[0], raw.rollingLoads[0], "metadata only replaces the immutable header")
		re.Equal(current.IsLeader(), raw.IsLeader())
		re.True(raw.lastTransferLeaderTime.IsZero(), "querying must not change sample-driven cooldown")
		single := cache.GetHotPeerStat(utils.Read, region.GetID(), storeID)
		re.NotNil(single)
		re.Equal(current.IsLeader(), single.IsLeader())
		re.Equal(current.GetLoads(), single.GetLoads())
	}

	info := CollectHotPeerInfos(cluster.GetStores(), after)
	re.Zero(info.AsLeader[region.GetLeader().GetStoreId()].Count)
	re.Equal(1, info.AsLeader[moved.GetLeader().GetStoreId()].Count)
	re.Equal(before[moved.GetLeader().GetStoreId()][0].GetLoad(utils.ByteDim), info.AsLeader[moved.GetLeader().GetStoreId()].TotalBytesRate)
	for _, stat := range info.AsPeer {
		re.Equal(1, stat.Count)
	}
}

func TestReadHotPeerStatsUseLatestAcceptedNotification(t *testing.T) {
	re := require.New(t)
	cache, cluster, region, cancel := newReadProjectionTestCache(t)
	defer cancel()
	seedReadProjectionSamples(t, cache, region)
	before := cache.GetHotPeerStats(utils.Read, 3)

	// Several accepted publications can accumulate before any cache consumer
	// runs. The last snapshot must win, including removal and reinsertion.
	cluster.PutRegion(region.Clone(core.WithLeader(region.GetPeers()[1])))
	cluster.PutRegion(region)
	cluster.RemoveRegion(region)
	latest := region.Clone(core.WithLeader(region.GetPeers()[2]))
	cluster.PutRegion(latest)
	after := cache.GetHotPeerStats(utils.Read, 3)
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		re.Len(after[storeID], 1)
		re.Equal(storeID == latest.GetLeader().GetStoreId(), after[storeID][0].IsLeader())
		re.Equal(before[storeID][0].GetLoads(), after[storeID][0].GetLoads())
	}

	cluster.PutRegion(region)
	cluster.RemoveRegion(region)
	for _, peer := range region.GetPeers() {
		re.Nil(cache.GetHotPeerStat(utils.Read, region.GetID(), peer.GetStoreId()))
	}
}

func TestReadHotPeerStatsDoNotInventSamples(t *testing.T) {
	for _, name := range []string{"new-leader-without-sample", "unknown-leader", "removed-peer", "removed-region"} {
		t.Run(name, func(t *testing.T) {
			re := require.New(t)
			cache, cluster, region, cancel := newReadProjectionTestCache(t)
			defer cancel()
			oldLeader := region.GetLeader()
			seedReadProjectionSamples(t, cache, region, oldLeader)
			before := cache.GetHotPeerStat(utils.Read, region.GetID(), oldLeader.GetStoreId())
			re.NotNil(before)
			var current *core.RegionInfo
			switch name {
			case "new-leader-without-sample":
				current = region.Clone(core.WithLeader(region.GetPeers()[1]))
			case "unknown-leader":
				current = region.Clone(core.WithLeader(nil))
			case "removed-peer":
				current = region.Clone(core.WithLeader(region.GetPeers()[1]), core.WithRemoveStorePeer(oldLeader.GetStoreId()))
			case "removed-region":
				cluster.RemoveRegion(region)
			}
			if current != nil {
				cluster.PutRegion(current)
			}
			after := cache.GetHotPeerStats(utils.Read, 3)
			single := cache.GetHotPeerStat(utils.Read, region.GetID(), oldLeader.GetStoreId())
			if name == "removed-peer" || name == "removed-region" {
				re.Empty(after[oldLeader.GetStoreId()])
				re.Nil(single)
			} else {
				re.Len(after[oldLeader.GetStoreId()], 1)
				re.NotNil(single)
				re.False(single.IsLeader())
				re.Equal(before.GetLoads(), single.GetLoads())
			}
			for _, peer := range region.GetPeers()[1:] {
				re.Empty(after[peer.GetStoreId()])
				re.Nil(cache.GetHotPeerStat(utils.Read, region.GetID(), peer.GetStoreId()), "metadata cannot create a read sample")
			}
			for _, stat := range CollectHotPeerInfos(cluster.GetStores(), after).AsLeader {
				re.Zero(stat.Count)
			}
		})
	}
}

func TestReadHotPeerStatsRejectReplacedPeerSamples(t *testing.T) {
	for _, cold := range []bool{false, true} {
		t.Run(fmt.Sprintf("cold-%t", cold), func(t *testing.T) {
			re := require.New(t)
			cache, cluster, region, cancel := newReadProjectionTestCache(t)
			defer cancel()
			seedReadProjectionSamples(t, cache, region)
			if cold {
				re.True(cache.CheckColdPeerAsync(region.GetLeader().GetStoreId(), map[uint64]struct{}{}, utils.StoreHeartBeatReportInterval))
			}
			old := cache.GetHotPeerStat(utils.Read, region.GetID(), region.GetLeader().GetStoreId())
			re.NotNil(old)
			re.Greater(old.HotDegree, 1)

			replacement := &metapb.Peer{Id: region.GetLeader().GetId() + 100, StoreId: region.GetLeader().GetStoreId()}
			replaced := region.Clone(core.WithRemoveStorePeer(replacement.GetStoreId()), core.WithAddPeer(replacement), core.WithLeader(replacement))
			cluster.PutRegion(replaced)
			re.Nil(cache.GetHotPeerStat(utils.Read, region.GetID(), replacement.GetStoreId()))
			re.Empty(cache.GetHotPeerStats(utils.Read, 0)[replacement.GetStoreId()])
			invalid := readProjectionRawPeer(t, cache, region.GetID(), replacement.GetStoreId())
			re.Equal(readRoleInvalid, invalid.readRole)
			re.Equal(old.peerID, invalid.peerID, "retain physical provenance until the replacement sample arrives")
			re.Same(old.rollingLoads[0], invalid.rollingLoads[0])

			const newByteRate = 8 << 20
			reportReadProjectionSample(t, cache, replaced, replacement, newByteRate)
			fresh := cache.GetHotPeerStat(utils.Read, region.GetID(), replacement.GetStoreId())
			re.NotNil(fresh)
			re.Equal(1, fresh.HotDegree, "a replacement must earn its own hot degree")
			re.Equal(float64(newByteRate), fresh.Loads[utils.ByteDim])
			re.NotSame(old.rollingLoads[utils.ByteDim], fresh.rollingLoads[utils.ByteDim], "a replacement must not inherit the removed peer's history")
			re.Empty(cache.GetHotPeerStats(utils.Read, 3)[replacement.GetStoreId()])

			reportReadProjectionSample(t, cache, region, region.GetLeader(), 1<<20)
			afterDelayedSample := cache.GetHotPeerStat(utils.Read, region.GetID(), replacement.GetStoreId())
			re.Same(fresh, afterDelayedSample, "a delayed sample from the removed peer must not overwrite its replacement")
		})
	}
}

func TestReadHotPeerStatsPreserveSampleCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		cache, cluster, region, cancel := newReadProjectionTestCache(t)
		defer cancel()
		seedReadProjectionSamples(t, cache, region)
		oldLeaderID := region.GetLeader().GetStoreId()
		before := cache.GetHotPeerStat(utils.Read, region.GetID(), oldLeaderID)
		re.NotNil(before)
		re.False(before.IsNeedCoolDownTransferLeader(3, utils.Read))
		moved := region.Clone(core.WithLeader(region.GetPeers()[1]))
		cluster.PutRegion(moved)

		cooldown := time.Duration(3*utils.Read.ReportInterval()) * time.Second
		for range 2 {
			view := cache.GetHotPeerStat(utils.Read, region.GetID(), oldLeaderID)
			re.False(view.IsLeader())
			re.True(view.IsNeedCoolDownTransferLeader(3, utils.Read), "a mismatched sample must remain in cooldown until refreshed")
			re.False(view.IsNeedCoolDownTransferLeader(0, utils.Read), "a zero threshold still disables cooldown")
			re.Same(view, cache.GetHotPeerStat(utils.Read, region.GetID(), oldLeaderID), "a query must not copy unchanged metadata")
			re.True(readProjectionRawPeer(t, cache, region.GetID(), oldLeaderID).lastTransferLeaderTime.IsZero())
			time.Sleep(cooldown + time.Second)
			re.True(view.IsNeedCoolDownTransferLeader(3, utils.Read), "a held view must not age out of an unsampled role change")
		}

		// The old leader's next sample detects the transfer from its unchanged provenance.
		reportReadProjectionSample(t, cache, moved, moved.GetStorePeer(oldLeaderID), 1<<20)
		fresh := cache.GetHotPeerStat(utils.Read, region.GetID(), oldLeaderID)
		re.NotNil(fresh)
		re.False(fresh.IsLeader())
		re.False(fresh.lastTransferLeaderTime.IsZero())
		re.True(fresh.IsNeedCoolDownTransferLeader(3, utils.Read))
		started := fresh.lastTransferLeaderTime
		time.Sleep(cooldown + time.Second)
		after := cache.GetHotPeerStat(utils.Read, region.GetID(), oldLeaderID)
		re.NotNil(after)
		re.Equal(started, after.lastTransferLeaderTime)
		re.False(after.IsNeedCoolDownTransferLeader(3, utils.Read), "a matching sample query must not restart cooldown")
	})
}

func TestReadHotPeerStatsDoNotRenewSampleTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		cache, cluster, region, cancel := newReadProjectionTestCache(t)
		defer cancel()
		seedReadProjectionSamples(t, cache, region)
		re.Len(cache.GetHotPeerStats(utils.Read, 3)[region.GetLeader().GetStoreId()], 1)
		ttl := time.Duration(3*utils.Read.ReportInterval()) * time.Second
		time.Sleep(ttl - time.Second)
		cluster.PutRegion(region.Clone(core.WithLeader(region.GetPeers()[1])))
		for _, peer := range region.GetPeers() {
			re.NotNil(cache.GetHotPeerStat(utils.Read, region.GetID(), peer.GetStoreId()))
		}
		re.Len(cache.GetHotPeerStats(utils.Read, 3)[region.GetLeader().GetStoreId()], 1)
		time.Sleep(2 * time.Second)
		re.True(cache.CheckReadAsync(func(cache *HotPeerCache) { cache.gc() }))
		stats := cache.GetHotPeerStats(utils.Read, 0)
		for _, peer := range region.GetPeers() {
			re.Empty(stats[peer.GetStoreId()], "querying metadata must not extend sample lifetime")
			re.Nil(cache.GetHotPeerStat(utils.Read, region.GetID(), peer.GetStoreId()))
		}
	})
}

func TestReadHotPeerStatsReconcileDroppedNotifications(t *testing.T) {
	re := require.New(t)
	cache, cluster, region, cancel := newReadProjectionTestCache(t)
	defer cancel()
	seedReadProjectionSamples(t, cache, region)
	before := cache.GetHotPeerStats(utils.Read, 3)
	updates := cache.readCache.readMetadata
	for i := range cap(updates.regions) {
		updates.notify(uint64(i+1000), nil)
	}
	moved := region.Clone(core.WithLeader(region.GetPeers()[1]))
	cluster.PutRegion(moved)
	re.True(updates.dirty.Load(), "a full mailbox must record the dropped publication")
	after := cache.GetHotPeerStats(utils.Read, 3)
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		re.Len(after[storeID], 1)
		re.Equal(storeID == moved.GetLeader().GetStoreId(), after[storeID][0].IsLeader())
		re.Equal(before[storeID][0].GetLoads(), after[storeID][0].GetLoads())
	}
	re.False(updates.dirty.Load())
	re.Empty(updates.regions)

	cluster.ResetRegionCache()
	for _, peer := range region.GetPeers() {
		re.Nil(cache.GetHotPeerStat(utils.Read, region.GetID(), peer.GetStoreId()))
	}
	// Registration survives reset; reinserted metadata must also be observed.
	cluster.PutRegion(moved)
	re.True(cache.GetHotPeerStat(utils.Read, region.GetID(), moved.GetLeader().GetStoreId()).IsLeader())
}

func TestReadMetadataFallbackDoesNotRetainRemovedRegions(t *testing.T) {
	re := require.New(t)
	cache, cluster, region, cancel := newReadProjectionTestCache(t)
	defer cancel()
	seedReadProjectionSamples(t, cache, region)
	remaining := make(chan int, 1)
	re.True(cache.CheckReadAsync(func(owner *HotPeerCache) {
		for _, peer := range region.GetPeers() {
			removed := *owner.getHotPeerStat(region.GetID(), peer.GetStoreId())
			removed.actionType = utils.Remove
			owner.UpdateStat(&removed)
		}
		remaining <- len(owner.storesOfRegion)
	}))
	re.Zero(<-remaining, "a fallback scan must not retain historically hot Region IDs")

	updates := cache.readCache.readMetadata
	for i := range cap(updates.regions) + 1 {
		updates.notify(uint64(i+1000), nil)
	}
	cache.GetHotPeerStats(utils.Read, 0)
	re.False(updates.dirty.Load())
	re.Empty(updates.regions)

	// A later sample recreates the index and still observes accepted metadata.
	moved := region.Clone(core.WithLeader(region.GetPeers()[1]))
	cluster.PutRegion(moved)
	seedReadProjectionSamples(t, cache, moved)
	stats := cache.GetHotPeerStats(utils.Read, 3)
	for _, peer := range moved.GetPeers() {
		re.Len(stats[peer.GetStoreId()], 1)
		re.Equal(peer.GetStoreId() == moved.GetLeader().GetStoreId(), stats[peer.GetStoreId()][0].IsLeader())
	}
}

func TestReadHotPeerStatsRejectStaleMetadataFromQueuedSamples(t *testing.T) {
	for _, firstAdmission := range []bool{false, true} {
		t.Run(fmt.Sprintf("first-admission-%t", firstAdmission), func(t *testing.T) {
			re := require.New(t)
			cache, cluster, region, cancel := newReadProjectionTestCache(t)
			defer cancel()
			if !firstAdmission {
				seedReadProjectionSamples(t, cache, region)
				re.Len(cache.GetHotPeerStats(utils.Read, 3)[region.GetLeader().GetStoreId()], 1)
			}
			moved := region.Clone(core.WithLeader(region.GetPeers()[1]))
			cluster.PutRegion(moved)
			// Consume the notice before admitting a sample captured with the old
			// leader. With no existing entry, this also exercises a discarded notice.
			cache.GetHotPeerStats(utils.Read, 0)
			for _, peer := range region.GetPeers()[:2] {
				reportReadProjectionSample(t, cache, region, peer, float64(peer.GetStoreId())*(1<<20))
				view := cache.GetHotPeerStat(utils.Read, region.GetID(), peer.GetStoreId())
				re.NotNil(view)
				re.Equal(peer.GetStoreId() == moved.GetLeader().GetStoreId(), view.IsLeader())
				re.True(view.IsNeedCoolDownTransferLeader(3, utils.Read))
				raw := readProjectionRawPeer(t, cache, region.GetID(), peer.GetStoreId())
				re.Equal(peer.GetStoreId() == region.GetLeader().GetStoreId(), raw.isLeader)
			}
		})
	}
	for _, removed := range []bool{false, true} {
		t.Run(fmt.Sprintf("absent-first-sample-%t", removed), func(t *testing.T) {
			re := require.New(t)
			cache, cluster, region, cancel := newReadProjectionTestCache(t)
			defer cancel()
			if removed {
				cluster.RemoveRegion(region)
			} else {
				cluster.PutRegion(region.Clone(core.WithRemoveStorePeer(region.GetLeader().GetStoreId()), core.WithLeader(region.GetPeers()[1])))
			}
			cache.GetHotPeerStats(utils.Read, 0)
			reportReadProjectionSample(t, cache, region, region.GetLeader(), 1<<20)
			re.Nil(cache.GetHotPeerStat(utils.Read, region.GetID(), region.GetLeader().GetStoreId()))
			re.Nil(readProjectionRawPeer(t, cache, region.GetID(), region.GetLeader().GetStoreId()))
		})
	}
}

func TestReadHotPeerStatsIgnoreUnpublishedRegionMetadata(t *testing.T) {
	re := require.New(t)
	cache, _, region, cancel := newReadProjectionTestCache(t)
	defer cancel()
	seedReadProjectionSamples(t, cache, region)
	before := cache.GetHotPeerStat(utils.Read, region.GetID(), region.GetLeader().GetStoreId())
	unpublished := region.Clone(core.WithRemoveStorePeer(region.GetLeader().GetStoreId()), core.WithLeader(region.GetPeers()[1]))
	cache.CheckRegionFlowWithOriginAsync(unpublished, region)
	after := cache.GetHotPeerStat(utils.Read, region.GetID(), region.GetLeader().GetStoreId())
	re.Same(before, after, "pre-publication write processing must not invalidate accepted read samples")
}

func TestReadInvalidPeerDoesNotRenewTTLFromColdSamples(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		cache, cluster, region, cancel := newReadProjectionTestCache(t)
		defer cancel()
		seedReadProjectionSamples(t, cache, region)
		storeID := region.GetLeader().GetStoreId()
		before := cache.GetHotPeerStat(utils.Read, region.GetID(), storeID)
		cluster.PutRegion(region.Clone(core.WithRemoveStorePeer(storeID), core.WithLeader(region.GetPeers()[1])))
		time.Sleep(time.Duration(3*utils.Read.ReportInterval())*time.Second - time.Second)
		// No query consumes the notice before this cold sample.
		re.True(cache.CheckColdPeerAsync(storeID, map[uint64]struct{}{}, utils.StoreHeartBeatReportInterval))
		invalid := readProjectionRawPeer(t, cache, region.GetID(), storeID)
		re.Equal(readRoleInvalid, invalid.readRole)
		re.Equal(before.peerID, invalid.peerID)
		re.Equal(before.HotDegree, invalid.HotDegree, "an invalid peer must not receive a cold sample")
		time.Sleep(2 * time.Second)
		re.True(cache.CheckReadAsync(func(owner *HotPeerCache) { owner.gc() }))
		re.Nil(readProjectionRawPeer(t, cache, region.GetID(), storeID))
		re.True(before.IsLeader(), "invalidation must preserve previously returned headers")
	})
}

func TestReadInvalidPeerThresholdsMatchRemoval(t *testing.T) {
	for _, validCount := range []int{TopNN - 1, TopNN, TopNN + 7} {
		t.Run(fmt.Sprintf("valid-%d", validCount), func(t *testing.T) {
			re := require.New(t)
			cluster := core.NewBasicCluster()
			cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cache := NewHotCache(ctx, cluster)
			reference := NewHotPeerCache(ctx, cluster, utils.Read)
			var removed []*core.RegionInfo
			for i := range validCount + 20 {
				regionID := uint64(i + 1)
				region := newBenchmarkRegion(regionID, 1).Clone(
					core.WithStartKey([]byte(fmt.Sprintf("%08d", regionID))),
					core.WithEndKey([]byte(fmt.Sprintf("%08d", regionID+1))),
				)
				cluster.PutRegion(region)
				loads := make([]float64, utils.DimLen)
				for dim, kind := range utils.Read.RegionStats() {
					loads[dim] = utils.MinHotThresholds[kind] * float64(i+2) * 100
				}
				stat := &HotPeerStat{
					StoreID: 1, RegionID: regionID, peerID: region.GetLeader().GetId(), isLeader: true,
					HotDegree: 4, AntiCount: utils.Read.DefaultAntiCount(), Loads: loads, actionType: utils.Add,
				}
				cache.Update(stat, utils.Read)
				copy := *stat
				reference.putItem(&copy)
				if i >= validCount {
					removed = append(removed, region)
				}
			}
			re.Len(cache.GetHotPeerStats(utils.Read, 3)[1], validCount+20)
			previous := append([]float64(nil), cache.GetThresholds(utils.Read, 1)...)
			for _, region := range removed {
				cluster.RemoveRegion(region)
				reference.removeItem(reference.getOldHotPeerStat(region.GetID(), 1))
			}
			want := reference.calcHotThresholds(1)
			re.Greater(previous[utils.ByteDim], want[utils.ByteDim])

			probe := newBenchmarkRegion(1000, 1).Clone(core.WithStartKey([]byte("probe")), core.WithEndKey([]byte("probef")))
			cluster.PutRegion(probe)
			// Sampling must reconcile removals and expire the old threshold even
			// when no public query has consumed the notifications.
			re.True(cache.CheckReadPeerAsync(probe, probe.GetLeader(), make([]float64, utils.RegionStatCount), utils.StoreHeartBeatReportInterval))
			result := make(chan []float64, 1)
			re.True(cache.CheckReadAsync(func(owner *HotPeerCache) {
				result <- append([]float64(nil), owner.calcHotThresholds(1)...)
			}))
			re.Equal(want, <-result, "retained invalid descriptors must have the same threshold effect as removal")
			re.Len(cache.GetHotPeerStats(utils.Read, 3)[1], validCount)
		})
	}
}

func TestReadIsRegionHotRejectsReplacedPeer(t *testing.T) {
	re := require.New(t)
	cache, cluster, region, cancel := newReadProjectionTestCache(t)
	defer cancel()
	seedReadProjectionSamples(t, cache, region, region.GetLeader())
	re.True(cache.IsRegionHot(region, 3))
	peer := &metapb.Peer{Id: region.GetLeader().GetId() + 100, StoreId: region.GetLeader().GetStoreId()}
	replacement := region.Clone(core.WithRemoveStorePeer(peer.GetStoreId()), core.WithAddPeer(peer), core.WithLeader(peer))
	cluster.PutRegion(replacement)
	re.False(cache.IsRegionHot(replacement, 1), "the old physical peer's degree must not make its replacement hot")
	reportReadProjectionSample(t, cache, replacement, peer, 8<<20)
	re.True(cache.IsRegionHot(replacement, 1))
	re.False(cache.IsRegionHot(replacement, 3))
}

func TestIsHot(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	for i := range utils.RWTypeLen {
		cluster := core.NewBasicCluster()
		cache := NewHotCache(ctx, cluster)
		region, err := buildRegion(cluster, i, 3, 60)
		re.NoError(err)
		cluster.PutRegion(region)
		loads := make([]float64, utils.RegionStatCount)
		loads[utils.RegionReadBytes] = 100000000
		loads[utils.RegionReadKeys] = 1000
		loads[utils.RegionReadQueryNum] = 1000
		stats := cache.CheckReadPeerSync(region, region.GetPeers(), loads, 60)
		cache.Update(stats[0], i)
		for range 100 {
			re.True(cache.IsRegionHot(region, 1))
		}
	}
}

func TestRegionFlowTasksMatchOriginalForAnyPeerCount(t *testing.T) {
	for _, peerCount := range []int{0, 1, 2, 3, 5, 9} {
		t.Run(fmt.Sprintf("%d-peers", peerCount), func(t *testing.T) {
			ctx := t.Context()
			previousRegion := newBenchmarkRegion(1, peerCount+1)
			newCache := func() *HotPeerCache {
				cluster := core.NewBasicCluster()
				for _, peer := range previousRegion.GetPeers() {
					cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: peer.GetStoreId()}))
				}
				return NewHotPeerCache(ctx, cluster, utils.Write)
			}
			originalCache := newCache()
			compactCache := newCache()

			_, seedWrite := newOriginalRegionFlowTasks(previousRegion)
			seedWrite(originalCache)
			seedWrite(compactCache)
			originalStats := originalCache.GetHotPeerStats(0)
			compactStats := compactCache.GetHotPeerStats(0)
			require.Len(t, originalStats, peerCount+1)
			require.Len(t, compactStats, peerCount+1)
			for _, peer := range previousRegion.GetPeers() {
				require.NotEmpty(t, originalStats[peer.GetStoreId()])
				require.NotEmpty(t, compactStats[peer.GetStoreId()])
			}

			region := newBenchmarkRegion(1, peerCount)

			originalExpired, originalWrite := newOriginalRegionFlowTasks(region)
			compactExpired, compactWrite := newRegionFlowTasks(region, previousRegion)
			originalExpired(originalCache)
			originalWrite(originalCache)
			compactExpired(compactCache)
			compactWrite(compactCache)

			originalStats = originalCache.GetHotPeerStats(0)
			compactStats = compactCache.GetHotPeerStats(0)
			require.Equal(t, originalStats, compactStats)
			require.Equal(t, originalCache.storesOfRegion, compactCache.storesOfRegion)
			require.Equal(t, originalCache.regionsOfStore, compactCache.regionsOfStore)

			expiredStoreID := uint64(peerCount + 1)
			require.Empty(t, originalStats[expiredStoreID])
			require.Empty(t, compactStats[expiredStoreID])
			require.Nil(t, originalCache.getHotPeerStat(region.GetID(), expiredStoreID))
			require.Nil(t, compactCache.getHotPeerStat(region.GetID(), expiredStoreID))
			for _, peer := range region.GetPeers() {
				require.NotEmpty(t, originalStats[peer.GetStoreId()])
				require.NotEmpty(t, compactStats[peer.GetStoreId()])
			}
		})
	}
}

func BenchmarkPendingRegionHeartbeatTasks(b *testing.B) {
	for _, peerCount := range []int{1, 3, 5} {
		b.Run(fmt.Sprintf("%d-peers", peerCount), func(b *testing.B) {
			b.Run("retain-region-info", func(b *testing.B) {
				runPendingRegionHeartbeatTaskBenchmark(b, peerCount, newOriginalRegionFlowTasks)
			})
			b.Run("retain-region-meta", func(b *testing.B) {
				runPendingRegionHeartbeatTaskBenchmark(b, peerCount, func(region *core.RegionInfo) (func(*HotPeerCache), func(*HotPeerCache)) {
					return newRegionFlowTasks(region, nil)
				})
			})
		})
	}
}

func runPendingRegionHeartbeatTaskBenchmark(
	b *testing.B,
	peerCount int,
	newTasks func(*core.RegionInfo) (func(*HotPeerCache), func(*HotPeerCache)),
) {
	const maxPendingHeartbeats = 16 * 1024
	pending := make([][3]func(*HotPeerCache), min(b.N, maxPendingHeartbeats))
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		region := newBenchmarkRegion(uint64(i+1), peerCount)
		checkExpiredTask, checkWritePeerTask := newTasks(region)
		pending[i%len(pending)] = [3]func(*HotPeerCache){
			checkExpiredTask,
			checkExpiredTask,
			checkWritePeerTask,
		}
	}
	b.StopTimer()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	retainedBytes := max(int64(after.HeapAlloc)-int64(before.HeapAlloc), 0)
	b.ReportMetric(float64(retainedBytes)/float64(len(pending)), "retained-B/heartbeat")
	b.ReportMetric(float64(retainedBytes)/float64(len(pending)*3), "retained-B/task")
	runtime.KeepAlive(pending)
}

func newOriginalRegionFlowTasks(region *core.RegionInfo) (checkExpiredTask, checkWritePeerTask func(*HotPeerCache)) {
	checkExpiredTask = func(cache *HotPeerCache) {
		expiredStats := cache.CollectExpiredItems(region)
		for _, stat := range expiredStats {
			cache.UpdateStat(stat)
		}
	}
	checkWritePeerTask = func(cache *HotPeerCache) {
		reportInterval := region.GetInterval()
		interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
		stats := cache.CheckPeerFlow(region, region.GetPeers(), region.GetWriteLoads(), interval)
		for _, stat := range stats {
			cache.UpdateStat(stat)
		}
	}
	return checkExpiredTask, checkWritePeerTask
}

func newBenchmarkRegion(regionID uint64, peerCount int) *core.RegionInfo {
	peers := make([]*metapb.Peer, peerCount)
	for i := range peerCount {
		peers[i] = &metapb.Peer{Id: regionID*10 + uint64(i+1), StoreId: uint64(i + 1)}
	}
	var leader *metapb.Peer
	if len(peers) > 0 {
		leader = peers[0]
	}
	return core.NewRegionInfo(
		&metapb.Region{
			Id:          regionID,
			StartKey:    make([]byte, 32),
			EndKey:      make([]byte, 32),
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:       peers,
		},
		leader,
		core.SetWrittenBytes(1<<20),
		core.SetWrittenKeys(1024),
		core.SetReportInterval(0, utils.RegionHeartBeatReportInterval),
	)
}

func newWriteTransferTestRegion(id, writtenBytes, interval, term uint64) *core.RegionInfo {
	peers := make([]*metapb.Peer, 3)
	for i := range peers {
		peers[i] = &metapb.Peer{Id: id*10 + uint64(i) + 1, StoreId: (id+uint64(i))%5 + 1}
	}
	return core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
		Region: &metapb.Region{
			Id: id, Peers: peers,
			StartKey: []byte(fmt.Sprintf("%08d", id)),
			EndKey:   []byte(fmt.Sprintf("%08d", id+1)),
		},
		Leader: peers[0], Term: term, BytesWritten: writtenBytes,
		Interval: &pdpb.TimeInterval{EndTimestamp: interval},
	}, 0)
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
	for _, tc := range []struct {
		name              string
		historicalBytes   uint64
		returnToSameStore bool
	}{
		{name: "new-leader-idle"},
		{name: "new-leader-historical-writes", historicalBytes: 2 << 20},
		{name: "returning-leader-idle", returnToSameStore: true},
		{name: "returning-leader-historical-writes", historicalBytes: 2 << 20, returnToSameStore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cache, cluster := newWriteTransferTestCache(t)
			// Both heartbeat handlers submit statistics before publishing the region.
			report := func(region *core.RegionInfo) {
				cache.CheckRegionFlowWithOriginAsync(region, cluster.GetRegion(region.GetID()))
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
				steady[i] = newWriteTransferTestRegion(uint64(10000+i), 2400*60, 60, 10)
			}
			for range 12 {
				for _, region := range steady {
					report(region)
				}
			}
			re.Equal(96, countSteadyPeers())

			cold := make([]*core.RegionInfo, 128)
			for i := range cold {
				cold[i] = newWriteTransferTestRegion(uint64(i+1), 0, 60, 10)
				report(cold[i])
			}
			re.Equal(96, countSteadyPeers())
			for _, region := range cold {
				// A returning leader reports writes accumulated before the idle period.
				moved := newWriteTransferTestRegion(region.GetID(), tc.historicalBytes, 500, 12)
				if !tc.returnToSameStore {
					moved = moved.Clone(core.WithLeader(region.GetPeers()[1]))
				}
				report(moved)
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
			t.Logf("historical_bytes=%d thresholds=%v", tc.historicalBytes, rates)
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
		name                                                     string
		known, unknownLeader, cached, sameLeader, explicitOrigin bool
		skipSample                                               bool
		originTerm, term                                         uint64
	}{
		{name: "cold-transfer", known: true, skipSample: true},
		{name: "hot-transfer", known: true, cached: true, skipSample: true},
		{name: "new-region"},
		{name: "unknown-leader", known: true, unknownLeader: true},
		{name: "unchanged-leader", known: true, sameLeader: true},
		{name: "cold-transfer-with-origin", known: true, explicitOrigin: true, skipSample: true},
		{name: "hot-transfer-with-origin", known: true, cached: true, explicitOrigin: true, skipSample: true},
		{name: "new-region-with-origin", explicitOrigin: true},
		{name: "unknown-leader-with-origin", known: true, unknownLeader: true, explicitOrigin: true},
		{name: "unchanged-leader-with-origin", known: true, sameLeader: true, explicitOrigin: true},
		{name: "cold-returning-leader", known: true, sameLeader: true, originTerm: 10, term: 12, skipSample: true},
		{name: "hot-returning-leader", known: true, cached: true, sameLeader: true, originTerm: 10, term: 12, skipSample: true},
		{name: "cold-returning-leader-with-origin", known: true, sameLeader: true, explicitOrigin: true, originTerm: 10, term: 12, skipSample: true},
		{name: "hot-returning-leader-with-origin", known: true, cached: true, sameLeader: true, explicitOrigin: true, originTerm: 10, term: 12, skipSample: true},
		{name: "unchanged-term", known: true, sameLeader: true, originTerm: 10, term: 10},
		{name: "unknown-origin-term", known: true, sameLeader: true, term: 12},
		{name: "unknown-current-term", known: true, sameLeader: true, originTerm: 10},
		{name: "new-region-with-term", sameLeader: true, term: 12},
		{name: "unknown-leader-with-term", known: true, unknownLeader: true, sameLeader: true, originTerm: 10, term: 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cache, cluster := newWriteTransferTestCache(t)
			report := func(region *core.RegionInfo) {
				if tc.explicitOrigin {
					cache.CheckRegionFlowWithOriginAsync(region, cluster.GetRegion(region.GetID()))
				} else {
					cache.CheckRegionFlowAsync(region)
				}
			}
			region := newWriteTransferTestRegion(1, 8000*60, 60, tc.originTerm)
			if tc.known {
				origin := region
				if tc.unknownLeader {
					origin = origin.Clone(core.WithLeader(nil))
				}
				cluster.PutRegion(origin)
			}
			if tc.cached {
				for range 3 {
					report(region)
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
			moved := newWriteTransferTestRegion(1, 2<<20, 500, tc.term)
			if !tc.sameLeader {
				moved = moved.Clone(core.WithLeader(region.GetPeers()[1]))
			}
			report(moved)
			cluster.PutRegion(moved)
			close(release)

			for _, peer := range moved.GetPeers() {
				stat := cache.GetHotPeerStat(utils.Write, moved.GetID(), peer.GetStoreId())
				switch {
				case tc.skipSample && !tc.cached:
					re.Nil(stat, "a stale first sample must not create a cache entry")
				case tc.skipSample:
					re.NotNil(stat)
					re.Equal(oldDegree, stat.HotDegree)
					re.Equal(8000.0, stat.GetLoad(utils.ByteDim))
					re.Equal(peer.GetStoreId() == moved.GetLeader().GetStoreId(), stat.IsLeader())
				default:
					re.NotNil(stat, "valid new hot regions must still be admitted")
				}
			}
			fresh := moved.Clone(core.SetWrittenBytes(8000*60), core.SetReportInterval(500, 560))
			report(fresh)
			cluster.PutRegion(fresh)
			stat := cache.GetHotPeerStat(utils.Write, fresh.GetID(), fresh.GetLeader().GetStoreId())
			re.NotNil(stat)
			if tc.cached {
				re.Equal(oldDegree+1, stat.HotDegree, "the second sample must not be skipped again")
			}
			for range 3 {
				report(fresh)
			}
			re.Len(cache.GetHotPeerStats(utils.Write, 3), 3, "fresh sustained writes must become schedulable")
		})
	}
}

func TestWriteRegionTermChangeSkipsOnlyFirstSample(t *testing.T) {
	for _, writtenBytes := range []uint64{0, 8000 * 60} {
		t.Run(fmt.Sprintf("written-bytes-%d", writtenBytes), func(t *testing.T) {
			re := require.New(t)
			cache, cluster := newWriteTransferTestCache(t)
			guide := core.GenerateRegionGuideFunc(false)
			report := func(region *core.RegionInfo) {
				origin, _, err := cluster.PreCheckPutRegion(region)
				re.NoError(err)
				region.Inherit(origin, false)
				cache.CheckRegionFlowWithOriginAsync(region, origin)
				_, saveCache, _, _ := guide(core.ContextTODO(), region, origin)
				if saveCache {
					cluster.PutRegion(region)
				}
			}
			origin := newWriteTransferTestRegion(1, writtenBytes, 60, 10)
			report(origin)
			// Only the term changes. Neither the leader nor write flow forces
			// the heartbeat handler to publish another region snapshot.
			moved := newWriteTransferTestRegion(1, writtenBytes, 60, 12)
			report(moved)
			re.Equal(uint64(12), cluster.GetRegion(1).GetTerm())
			// Identical fresh samples must use the updated term and enter TopN.
			fresh := moved.Clone(core.SetWrittenBytes(8000 * 60))
			for range 4 {
				report(fresh)
			}
			re.Len(cache.GetHotPeerStats(utils.Write, 3), 3)
		})
	}
}
