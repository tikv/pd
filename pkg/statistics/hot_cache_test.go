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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sasha-s/go-deadlock"
	"github.com/smallnest/chanx"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	// Keep deadlock detection enabled without reusing timers across synctest bubbles.
	deadlock.Opts.TimerPool = deadlock.TimerPoolDisabled
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func TestIsHot(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	for i := range utils.RWTypeLen {
		cluster := core.NewBasicCluster()
		cache := NewHotCache(ctx, cluster)
		region, err := buildRegion(cluster, i, 3, 60)
		re.NoError(err)
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

func TestUpdateWaitsForCacheWorker(t *testing.T) {
	for kind := range utils.RWTypeLen {
		t.Run(kind.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, cache, region, _ := newHotReadCache(t, []uint64{1, 2})
				cache.CheckRegionFlowAsync(region)
				item := *cache.GetHotPeerStat(kind, region.GetID(), 1)
				item.HotDegree++
				release := blockHotCacheWorker(t, cache, kind)
				done := make(chan struct{})
				go func() {
					cache.Update(&item, kind)
					close(done)
				}()
				synctest.Wait()
				select {
				case <-done:
					t.Fatal("synchronous updates must wait for the owning worker")
				default:
				}
				release()
				<-done
				require.Equal(t, item.HotDegree, cache.GetHotPeerStat(kind, region.GetID(), 1).HotDegree)
			})
		})
	}
}

func TestSyncCacheHelpersFollowPendingMetadata(t *testing.T) {
	for _, operation := range []string{"sample", "expiry", "threshold"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, cache, region, loads := newHotReadCache(t, []uint64{1, 2, 3})
				release := blockHotReadWorker(t, cache)
				latest := region.Clone(core.WithRemoveStorePeer(3), core.WithIncConfVer())
				_, err := cache.UpdateRegion(core.ContextTODO(), latest)
				require.NoError(t, err)
				done := make(chan struct{})
				var expired []*HotPeerStat
				go func() {
					switch operation {
					case "sample":
						cache.CheckReadPeerSync(latest, latest.GetPeers(), loads, utils.StoreHeartBeatReportInterval)
					case "expiry":
						expired = cache.ExpiredReadItems(latest)
					case "threshold":
						cache.GetThresholds(utils.Read, 1)
					}
					close(done)
				}()
				synctest.Wait()
				select {
				case <-done:
					t.Fatal("synchronous helpers must observe earlier metadata on the cache owner")
				default:
				}
				release()
				<-done
				require.Empty(t, expired, "the accepted removal must be applied before expiry inspection")
				require.Empty(t, cache.GetHotPeerStats(utils.Read, 0)[3])
			})
		})
	}
}

func TestCleanCacheWaitsForPendingTasks(t *testing.T) {
	for kind := range utils.RWTypeLen {
		t.Run(kind.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, cache, region, _ := newHotReadCache(t, []uint64{1, 2})
				cache.CheckRegionFlowAsync(region)
				require.Len(t, cache.GetHotPeerStats(utils.Write, 0)[1], 1)
				release := blockHotCacheWorker(t, cache, kind)
				updated := region.Clone(core.WithLeader(region.GetStorePeer(2)))
				cache.CheckRegionFlowAsync(updated)
				_, err := cache.UpdateRegion(core.ContextTODO(), updated)
				require.NoError(t, err)
				if kind == utils.Read {
					_ = cache.GetHotPeerStats(utils.Write, 0)
				} else {
					_ = cache.GetHotPeerStats(utils.Read, 0)
				}
				done := make(chan struct{})
				go func() {
					cache.CleanCache()
					close(done)
				}()
				synctest.Wait()
				select {
				case <-done:
					t.Fatal("cleanup returned before pending cache tasks finished")
				default:
				}
				release()
				<-done
				for kind := range utils.RWTypeLen {
					for _, stats := range cache.GetHotPeerStats(kind, 0) {
						require.Empty(t, stats, "cleanup must remove statistics from earlier tasks")
					}
				}
				require.Empty(t, cache.readCache.readMetadata)
				cache.CleanCache()
			})
		})
	}
}

func TestCleanCacheCancellation(t *testing.T) {
	for _, stage := range []string{"write-input", "read-input", "execution"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				cache := NewHotCache(ctx, core.NewBasicCluster())
				if stage == "write-input" || stage == "read-input" {
					peerCache := cache.writeCache
					if stage == "read-input" {
						peerCache = cache.readCache
					}
					queue := peerCache.taskQueue
					peerCache.taskQueue = &chanx.UnboundedChan[func(*HotPeerCache)]{
						In:  make(chan func(*HotPeerCache)),
						Out: queue.Out,
					}
					defer func() { peerCache.taskQueue = queue }()
				} else {
					defer blockHotReadWorker(t, cache)()
				}
				done := make(chan struct{})
				go func() {
					cache.CleanCache()
					close(done)
				}()
				synctest.Wait()
				select {
				case <-done:
					t.Fatal("cleanup must wait for pending work or cancellation")
				default:
				}
				cancel()
				<-done
			})
		})
	}
}

func TestRegionHeartbeatUpdatesReadLeader(t *testing.T) {
	for _, tc := range []struct {
		name            string
		hotStores       []uint64
		removeOldLeader bool
		noLeader        bool
	}{
		{name: "first-hot-sample"},
		{name: "both-leaders-hot", hotStores: []uint64{1, 2, 3}},
		{name: "only-old-leader-hot", hotStores: []uint64{1, 3}},
		{name: "only-new-leader-hot", hotStores: []uint64{2, 3}},
		{name: "only-follower-hot", hotStores: []uint64{3}},
		{name: "old-leader-removed", hotStores: []uint64{1, 2, 3}, removeOldLeader: true},
		{name: "old-leader-removed-new-leader-cold", hotStores: []uint64{1, 3}, removeOldLeader: true},
		{name: "no-leader", hotStores: []uint64{1, 2, 3}, noLeader: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cluster, cache, region, loads := newHotReadCache(t, tc.hotStores)
			oldRegion := region
			before := cache.GetHotPeerStats(utils.Read, 0)
			for _, storeID := range tc.hotStores {
				re.Len(before[storeID], 1)
				re.Equal(storeID == 1, before[storeID][0].IsLeader())
			}

			leader := region.GetStorePeer(2)
			if tc.noLeader {
				leader = nil
			}
			region = region.Clone(core.WithLeader(leader))
			if tc.removeOldLeader {
				region = region.Clone(core.WithRemoveStorePeer(1))
			}
			cache.CheckRegionFlowAsync(region)
			_, err := cache.UpdateRegion(core.ContextTODO(), region)
			require.NoError(t, err)
			after := cache.GetHotPeerStats(utils.Read, 0)
			for _, storeID := range tc.hotStores {
				oldItem := before[storeID][0]
				re.Equal(storeID == 1, oldItem.IsLeader(), "previously returned stats must stay unchanged")
				re.False(oldItem.IsNeedCoolDownTransferLeader(3, utils.Read))
				if tc.removeOldLeader && storeID == 1 {
					re.Empty(after[storeID])
					continue
				}
				re.Len(after[storeID], 1)
				item := after[storeID][0]
				re.Equal(storeID == leader.GetStoreId(), item.IsLeader())
				re.Equal(oldItem.Loads, item.Loads)
				re.Equal(oldItem.HotDegree, item.HotDegree)
				re.Equal(oldItem.AntiCount, item.AntiCount)
				re.Same(oldItem.rollingLoads[0], item.rollingLoads[0])
				re.True(item.IsNeedCoolDownTransferLeader(3, utils.Read))
				re.Equal(after[3][0].lastTransferLeaderTime, item.lastTransferLeaderTime, "all replicas must share the transfer cooldown")
			}
			for _, peer := range region.GetPeers() {
				if before[peer.GetStoreId()] == nil {
					re.Empty(after[peer.GetStoreId()], "region heartbeats must not create read statistics")
				}
			}

			cache.CheckRegionFlowAsync(region)
			re.Equal(after, cache.GetHotPeerStats(utils.Read, 0), "unchanged heartbeats must preserve cooldown")
			for _, peer := range region.GetPeers() {
				storeID := peer.GetStoreId()
				re.True(cache.CheckReadPeerAsync(oldRegion, oldRegion.GetStorePeer(storeID), loads, utils.StoreHeartBeatReportInterval))
			}
			afterStore := cache.GetHotPeerStats(utils.Read, 0)
			for _, peer := range region.GetPeers() {
				storeID := peer.GetStoreId()
				re.Len(afterStore[storeID], 1)
				re.Equal(storeID == leader.GetStoreId(), afterStore[storeID][0].IsLeader(), "stale store snapshots must preserve observed leadership")
				if stats := after[storeID]; len(stats) > 0 {
					re.Equal(stats[0].lastTransferLeaderTime, afterStore[storeID][0].lastTransferLeaderTime)
					re.Equal(stats[0].HotDegree+1, afterStore[storeID][0].HotDegree, "the read sample must still be counted")
					re.Equal(storeID == leader.GetStoreId(), stats[0].IsLeader(), "returned metadata snapshots must stay unchanged")
				}
			}
			cluster.PutRegion(region)
			cache.CheckRegionFlowAsync(region)
			re.Equal(afterStore, cache.GetHotPeerStats(utils.Read, 0))
		})
	}
}

func TestRegionMetadataPressureDoesNotBlockHeartbeats(t *testing.T) {
	for _, stage := range []string{"input", "capacity"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, cache, region, loads := newHotReadCache(t, []uint64{1, 2, 3})
				cache.CheckRegionFlowAsync(region)
				before := cache.GetHotPeerStats(utils.Write, 0)
				_ = cache.GetHotPeerStats(utils.Read, 0)
				input := make(chan func(*HotPeerCache))
				if stage == "capacity" {
					// Leave a free input slot to prove rejection comes from the
					// queue length limit, rather than a blocked channel send.
					input = make(chan func(*HotPeerCache), chanMaxLength+2)
					for range chanMaxLength + 1 {
						input <- nil
					}
				}
				queue := cache.readCache.taskQueue
				cache.readCache.taskQueue = &chanx.UnboundedChan[func(*HotPeerCache)]{In: input, Out: queue.Out}
				defer func() { cache.readCache.taskQueue = queue }()
				updated := region.Clone(core.SetWrittenBytes(2 << 20))
				cache.CheckRegionFlowAsync(updated)
				// Both heartbeat implementations publish after handling statistics.
				_, err := cache.UpdateRegion(core.ContextTODO(), updated)
				require.NoError(t, err)
				synctest.Wait()
				require.Greater(t, cache.GetHotPeerStats(utils.Write, 0)[1][0].HotDegree, before[1][0].HotDegree)
				require.Same(t, updated, cluster.GetRegion(region.GetID()), "read pressure must not hold up region publication")
				require.False(t, cache.CheckReadPeerAsync(region, region.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
				// No sample tasks can enter, so publication must wake the read worker.
				transferred := updated.Clone(core.WithLeader(updated.GetStorePeer(2)))
				cache.CheckRegionFlowAsync(transferred)
				_, err = cache.UpdateRegion(core.ContextTODO(), transferred)
				require.NoError(t, err)
				synctest.Wait()
				after := cache.readCache.GetHotPeerStats(0)
				require.False(t, after[1][0].IsLeader())
				require.True(t, after[2][0].IsLeader())
				cache.readCache.taskQueue = queue
				require.True(t, cache.CheckReadPeerAsync(updated, updated.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
				after = cache.GetHotPeerStats(utils.Read, 0)
				require.False(t, after[1][0].IsLeader(), "a later stale snapshot must not undo delivered metadata")
				require.True(t, after[2][0].IsLeader())
			})
		})
	}
}

func TestReadMetadataLifetime(t *testing.T) {
	for _, freshSample := range []bool{false, true} {
		t.Run(fmt.Sprintf("fresh-sample=%t", freshSample), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, cache, old, loads := newHotReadCache(t, []uint64{1, 2})
				time.Sleep(20 * time.Second)
				current := old.Clone(core.WithLeader(old.GetStorePeer(2)))
				_, err := cache.UpdateRegion(core.ContextTODO(), current)
				require.NoError(t, err)
				if freshSample {
					require.True(t, cache.CheckReadPeerAsync(old, old.GetStorePeer(2), loads, utils.StoreHeartBeatReportInterval))
				}
				_ = cache.GetHotPeerStats(utils.Read, 0)
				time.Sleep(11 * time.Second)
				cache.CollectMetrics()
				after := cache.GetHotPeerStats(utils.Read, 0)
				require.Empty(t, after[1], "metadata must not renew read samples")
				if freshSample {
					require.Len(t, after[2], 1, "a fresh sample must renew its own TTL")
					require.True(t, after[2][0].IsLeader())
				} else {
					require.Empty(t, after[2])
				}
				time.Sleep(31 * time.Second)
				cache.CollectMetrics()
				_ = cache.GetHotPeerStats(utils.Read, 0)
				require.Empty(t, cache.readCache.readMetadata, "cold regions must release cached metadata")
				require.True(t, cache.CheckReadPeerAsync(old, old.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
				require.False(t, cache.GetHotPeerStat(utils.Read, old.GetID(), 1).IsLeader(), "old snapshots must use published leadership after GC")
			})
		})
	}
}

func TestCoalescedReadMetadataPreservesTransitions(t *testing.T) {
	for _, peerID := range []uint64{3, 100} {
		t.Run(fmt.Sprintf("readded-peer=%d", peerID), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, cache, initial, loads := newHotReadCache(t, []uint64{1, 2, 3})
				release := blockHotReadWorker(t, cache)
				if peerID != initial.GetStorePeer(3).GetId() {
					require.True(t, cache.CheckReadPeerAsync(initial, initial.GetStorePeer(3), loads, utils.StoreHeartBeatReportInterval))
				}
				region := initial.Clone(core.WithRemoveStorePeer(3), core.WithIncConfVer())
				_, err := cache.UpdateRegion(core.ContextTODO(), region)
				require.NoError(t, err)
				for i := range 1000 {
					region = region.Clone(core.WithLeader(region.GetStorePeer(uint64(i%2 + 1))))
					_, err = cache.UpdateRegion(core.ContextTODO(), region)
					require.NoError(t, err)
				}
				region = region.Clone(core.WithLeader(initial.GetStorePeer(1)), core.WithAddPeer(&metapb.Peer{Id: peerID, StoreId: 3}), core.WithIncConfVer())
				_, err = cache.UpdateRegion(core.ContextTODO(), region)
				require.NoError(t, err)
				require.Len(t, cache.regionMetaUpdates, 1)
				require.Len(t, cache.regionMetaUpdates[region.GetID()].retainedPeers, 2, "pending state must retain peer intersection, not transition history")
				release()
				after := cache.GetHotPeerStats(utils.Read, 0)
				require.True(t, after[1][0].IsLeader())
				for _, storeID := range []uint64{1, 2} {
					require.True(t, after[storeID][0].IsNeedCoolDownTransferLeader(3, utils.Read), "a transfer away and back must start cooldown")
					require.Equal(t, after[1][0].lastTransferLeaderTime, after[storeID][0].lastTransferLeaderTime)
				}
				require.Empty(t, after[3], "readding a peer must not restore removed samples")
				require.True(t, cache.CheckReadPeerAsync(region, region.GetStorePeer(3), loads, utils.StoreHeartBeatReportInterval))
				require.Len(t, cache.GetHotPeerStats(utils.Read, 0)[3], 1)
			})
		})
	}
}

func TestUnpublishedRegionMetadataPreservesReadCache(t *testing.T) {
	_, cache, initial, _ := newHotReadCache(t, []uint64{1, 2, 3})
	before := cache.GetHotPeerStats(utils.Read, 0)
	updated := initial.Clone(core.WithLeader(initial.GetStorePeer(2)), core.WithRemoveStorePeer(3),
		core.WithIncConfVer(), core.WithEndKey([]byte("z")), core.WithIncVersion())
	cache.CheckRegionFlowAsync(updated)
	require.Equal(t, before, cache.GetHotPeerStats(utils.Read, 0), "unpublished heartbeats must not change read roles, cooldowns or membership")
	_, err := cache.UpdateRegion(core.ContextTODO(), updated)
	require.NoError(t, err)
	after := cache.GetHotPeerStats(utils.Read, 0)
	require.False(t, after[1][0].IsLeader())
	require.True(t, after[2][0].IsLeader())
	require.Empty(t, after[3])
}

func TestRejectedRegionMetadataPreservesReadCache(t *testing.T) {
	for _, stale := range []string{"term", "confver", "version"} {
		t.Run(stale, func(t *testing.T) {
			cluster, cache, initial, loads := newHotReadCache(t, []uint64{1, 2, 3})
			initial = withHeartbeatTerm(initial, 10)
			cluster.PutRegion(initial)
			_, _, err := cluster.PreCheckPutRegion(initial)
			require.NoError(t, err)
			latest := withHeartbeatTerm(initial.Clone(core.WithLeader(initial.GetStorePeer(2)), core.WithRemoveStorePeer(3), core.WithIncConfVer(), core.WithIncVersion()), 11)
			if stale != "term" {
				initial = withHeartbeatTerm(initial, 12)
				if stale == "confver" {
					initial = initial.Clone(core.WithIncVersion())
				} else {
					initial = initial.Clone(core.WithIncConfVer())
				}
			}
			_, err = cache.UpdateRegion(core.ContextTODO(), latest)
			require.NoError(t, err)
			before := cache.GetHotPeerStats(utils.Read, 0)
			cache.CheckRegionFlowAsync(initial)
			_, err = cache.UpdateRegion(core.ContextTODO(), initial)
			require.Error(t, err)
			require.Equal(t, before, cache.GetHotPeerStats(utils.Read, 0))
			require.True(t, cache.CheckReadPeerAsync(initial, initial.GetStorePeer(3), loads, utils.StoreHeartBeatReportInterval))
			require.Empty(t, cache.GetHotPeerStats(utils.Read, 0)[3])
		})
	}
}

func TestOverlapRejectionPreservesReadMetadata(t *testing.T) {
	cluster, cache, initial, loads := newHotReadCache(t, []uint64{1, 2, 3},
		core.WithStartKey([]byte("a")), core.WithEndKey([]byte("b")))
	before := cache.GetHotPeerStats(utils.Read, 0)
	neighbor := newBenchmarkRegion(2, 3).Clone(core.SetRegionVersion(10), core.SetRegionConfVer(10),
		core.WithStartKey([]byte("b")), core.WithEndKey([]byte("d")))
	cluster.PutRegion(neighbor)
	merged := initial.Clone(core.WithEndKey([]byte("d")), core.WithIncVersion(),
		core.WithLeader(initial.GetStorePeer(2)), core.WithRemoveStorePeer(3), core.WithIncConfVer())
	_, _, err := cluster.PreCheckPutRegion(merged)
	require.NoError(t, err)
	cache.CheckRegionFlowAsync(merged)
	_, err = cluster.CheckAndPutRootTree(core.ContextTODO(), neighbor.Clone(core.SetRegionVersion(12)))
	require.NoError(t, err)
	_, err = cache.UpdateRegion(core.ContextTODO(), merged)
	require.Error(t, err)
	require.Same(t, initial, cluster.GetRegion(initial.GetID()))
	require.Equal(t, before, cache.GetHotPeerStats(utils.Read, 0), "rejected overlap metadata must preserve all read statistics")
	require.True(t, cache.GetHotPeerStat(utils.Read, initial.GetID(), 1).IsLeader())
	require.True(t, cache.CheckReadPeerAsync(initial, initial.GetStorePeer(3), loads, utils.StoreHeartBeatReportInterval))
	require.Len(t, cache.GetHotPeerStats(utils.Read, 0)[3], 1)
}

func TestAcceptedReadMetadataAcrossDetachedBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, cache, initial, _ := newHotReadCache(t, []uint64{1, 2, 3})
		started, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		fp := "github.com/tikv/pd/pkg/statistics/afterReadRegionMetadataBatchSwap"
		require.NoError(t, failpoint.EnableCall(fp, sync.OnceFunc(func() {
			close(started)
			<-release
		})))
		t.Cleanup(func() {
			unblock()
			require.NoError(t, failpoint.Disable(fp))
		})
		_, err := cache.UpdateRegion(core.ContextTODO(), initial.Clone(core.WithLeader(initial.GetStorePeer(2))))
		require.NoError(t, err)
		<-started
		latest := initial.Clone(core.WithLeader(initial.GetStorePeer(3)))
		_, err = cache.UpdateRegion(core.ContextTODO(), latest)
		require.NoError(t, err)
		unblock()
		after := cache.GetHotPeerStats(utils.Read, 0)
		for storeID, stats := range after {
			require.Equal(t, storeID == 3, stats[0].IsLeader(), "a detached batch must not overwrite a later accepted publication")
		}
	})
}

func TestReadSampleUsesAcceptedPeerBeforeSampling(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		t.Run(fmt.Sprintf("replaced=%t", replaced), func(t *testing.T) {
			cluster, cache, initial, loads := newHotReadCache(t, nil)
			latest := initial.Clone(core.WithRemoveStorePeer(3), core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 3}), core.WithIncConfVer())
			fp := "github.com/tikv/pd/pkg/statistics/beforeReadRegionMetadata"
			require.NoError(t, failpoint.EnableCall(fp, sync.OnceFunc(func() { cluster.PutRegion(latest) })))
			t.Cleanup(func() { require.NoError(t, failpoint.Disable(fp)) })
			peer := initial.GetStorePeer(3)
			if replaced {
				peer = latest.GetStorePeer(3)
			}
			require.True(t, cache.CheckReadPeerAsync(initial, peer, loads, utils.StoreHeartBeatReportInterval))
			after := cache.GetHotPeerStats(utils.Read, 0)
			require.Same(t, latest, cluster.GetRegion(initial.GetID()), "publication must occur before the sample reads accepted metadata")
			if replaced {
				require.Len(t, after[3], 1, "later metadata application must preserve the replacement's fresh sample")
			} else {
				require.Empty(t, after[3], "an obsolete peer must not be admitted")
			}
		})
	}
}

func TestReadMetadataRemovalAndRecovery(t *testing.T) {
	for _, change := range []string{"unknown-term", "recreated", "removed", "reset", "replaced-before-heartbeat"} {
		t.Run(change, func(t *testing.T) {
			cluster, cache, initial, loads := newHotReadCache(t, []uint64{1, 2, 3})
			initial = withHeartbeatTerm(initial, 100)
			cluster.PutRegion(initial)
			latest := initial.Clone(core.WithLeader(initial.GetStorePeer(2)))
			switch change {
			case "unknown-term":
				latest = withHeartbeatTerm(latest, 0)
			case "recreated":
				latest = withHeartbeatTerm(latest.Clone(core.SetRegionVersion(1), core.SetRegionConfVer(1)), 1)
			case "replaced-before-heartbeat":
				replaced := initial.Clone(core.WithRemoveStorePeer(3), core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 3}), core.WithIncConfVer())
				cluster.PutRegion(replaced)
				latest = replaced.Clone(core.WithLeader(replaced.GetStorePeer(2)))
			case "removed":
				cluster.RemoveRegion(initial)
			case "reset":
				cluster.ResetRegionCache()
			}
			if change == "removed" || change == "reset" {
				require.True(t, cache.CheckReadPeerAsync(initial, initial.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
				require.Empty(t, cache.GetHotPeerStats(utils.Read, 0)[1], "removed regions must not accept delayed samples")
				require.Empty(t, cache.readCache.readMetadata)
			} else {
				_, err := cache.UpdateRegion(core.ContextTODO(), latest)
				require.NoError(t, err)
				require.True(t, cache.GetHotPeerStat(utils.Read, latest.GetID(), 2).IsLeader())
				if change == "replaced-before-heartbeat" {
					require.Empty(t, cache.GetHotPeerStats(utils.Read, 0)[3], "later heartbeats must not hide an earlier physical peer replacement")
					require.True(t, cache.CheckReadPeerAsync(initial, initial.GetStorePeer(3), loads, utils.StoreHeartBeatReportInterval))
					require.Empty(t, cache.GetHotPeerStats(utils.Read, 0)[3], "old peer samples must not resurrect the replacement")
					require.True(t, cache.CheckReadPeerAsync(latest, latest.GetStorePeer(3), loads, utils.StoreHeartBeatReportInterval))
					require.Len(t, cache.GetHotPeerStats(utils.Read, 0)[3], 1, "the replacement must accept a fresh sample")
				}
			}
		})
	}
}

func blockHotReadWorker(t *testing.T, cache *HotCache) func() {
	t.Helper()
	return blockHotCacheWorker(t, cache, utils.Read)
}

func blockHotCacheWorker(t *testing.T, cache *HotCache, kind utils.RWType) func() {
	t.Helper()
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	check := cache.CheckReadAsync
	if kind == utils.Write {
		check = cache.CheckWriteAsync
	}
	require.True(t, check(func(*HotPeerCache) {
		close(started)
		<-release
	}))
	<-started
	return unblock
}

func withHeartbeatTerm(region *core.RegionInfo, term uint64) *core.RegionInfo {
	return core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
		Region:   region.GetMeta(),
		Leader:   region.GetLeader(),
		Term:     term,
		Interval: region.GetInterval(),
	}, 0)
}

func newHotReadCache(t *testing.T, hotStores []uint64, opts ...core.RegionCreateOption) (*core.BasicCluster, *HotCache, *core.RegionInfo, []float64) {
	t.Helper()
	cluster := core.NewBasicCluster()
	// Ordinary regions must not match core's epoch-(1,1) unsafe-recovery exception.
	region := newBenchmarkRegion(1, 3).Clone(core.SetRegionVersion(10), core.SetRegionConfVer(10))
	for _, opt := range opts {
		opt(region)
	}
	for _, peer := range region.GetPeers() {
		cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: peer.GetStoreId()}))
	}
	cluster.PutRegion(region)
	cache := NewHotCache(t.Context(), cluster)
	loads := make([]float64, utils.RegionStatCount)
	loads[utils.RegionReadBytes] = 1 << 20
	loads[utils.RegionReadKeys] = 1024
	loads[utils.RegionReadQueryNum] = 512
	for _, storeID := range hotStores {
		require.True(t, cache.CheckReadPeerAsync(region, region.GetStorePeer(storeID), loads, utils.StoreHeartBeatReportInterval))
	}
	before := cache.GetHotPeerStats(utils.Read, 0)
	for _, storeID := range hotStores {
		require.Len(t, before[storeID], 1)
	}
	return cluster, cache, region, loads
}

func TestRegionHeartbeatUpdatesWriteLeader(t *testing.T) {
	re := require.New(t)
	cluster := core.NewBasicCluster()
	region := newBenchmarkRegion(1, 3)
	for _, peer := range region.GetPeers() {
		cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: peer.GetStoreId()}))
	}
	cache := NewHotCache(t.Context(), cluster)
	cache.CheckRegionFlowAsync(region)
	before := cache.GetHotPeerStats(utils.Write, 0)
	region = region.Clone(core.WithLeader(region.GetStorePeer(2)))
	cache.CheckRegionFlowAsync(region)
	after := cache.GetHotPeerStats(utils.Write, 0)
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		re.Len(before[storeID], 1)
		re.Len(after[storeID], 1)
		item := after[storeID][0]
		re.Equal(storeID == 2, item.IsLeader())
		re.True(item.IsNeedCoolDownTransferLeader(3, utils.Write))
		re.Equal(before[storeID][0].HotDegree, item.HotDegree, "the first write sample after a leader transfer must be skipped")
	}
	cache.CheckRegionFlowAsync(region)
	for storeID, stats := range cache.GetHotPeerStats(utils.Write, 0) {
		re.Len(stats, 1)
		re.Equal(after[storeID][0].HotDegree+1, stats[0].HotDegree)
		re.Equal(after[storeID][0].lastTransferLeaderTime, stats[0].lastTransferLeaderTime)
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

func BenchmarkReadRegionPublication(b *testing.B) {
	for _, transfer := range []bool{false, true} {
		b.Run(fmt.Sprintf("transfer=%t", transfer), func(b *testing.B) {
			cluster := core.NewBasicCluster()
			first := newBenchmarkRegion(1, 3)
			for _, peer := range first.GetPeers() {
				cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: peer.GetStoreId()}))
			}
			cluster.PutRegion(first)
			cache := NewHotCache(b.Context(), cluster)
			loads := make([]float64, utils.RegionStatCount)
			loads[utils.RegionReadBytes] = 1 << 20
			for _, peer := range first.GetPeers() {
				cache.CheckReadPeerAsync(first, peer, loads, utils.StoreHeartBeatReportInterval)
			}
			_ = cache.GetHotPeerStats(utils.Read, 0)
			regions := [2]*core.RegionInfo{first, first}
			if transfer {
				regions[1] = first.Clone(core.WithLeader(first.GetStorePeer(2)))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				region := regions[i%len(regions)]
				origin, _, err := cluster.PreCheckPutRegion(region)
				if err != nil {
					b.Fatal(err)
				}
				cache.CheckRegionFlowWithOriginAsync(region, origin)
				if _, err := cache.UpdateRegion(core.ContextTODO(), region); err != nil {
					b.Fatal(err)
				}
				if i%64 == 0 {
					_ = cache.GetHotPeerStats(utils.Read, 0)
					_ = cache.GetHotPeerStats(utils.Write, 0)
				}
			}
			_ = cache.GetHotPeerStats(utils.Read, 0)
			_ = cache.GetHotPeerStats(utils.Write, 0)
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
