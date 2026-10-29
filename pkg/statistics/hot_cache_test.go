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

func TestCleanCacheWaitsForPendingTasks(t *testing.T) {
	for kind := range utils.RWTypeLen {
		t.Run(kind.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, cache, region, _ := newHotReadCache(t, []uint64{1, 2})
				cache.CheckRegionFlowAsync(region)
				require.Len(t, cache.GetHotPeerStats(utils.Write, 0)[1], 1)
				started, release := make(chan struct{}), make(chan struct{})
				t.Cleanup(func() {
					select {
					case <-release:
					default:
						close(release)
					}
				})
				block := func(*HotPeerCache) {
					close(started)
					<-release
				}
				if kind == utils.Read {
					require.True(t, cache.CheckReadAsync(block))
				} else {
					require.True(t, cache.CheckWriteAsync(block))
				}
				<-started
				cache.CheckRegionFlowAsync(region.Clone(core.WithLeader(region.GetStorePeer(2))))
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
				close(release)
				<-done
				for kind := range utils.RWTypeLen {
					for _, stats := range cache.GetHotPeerStats(kind, 0) {
						require.Empty(t, stats, "cleanup must remove statistics from earlier tasks")
					}
				}
				require.Empty(t, cache.readCache.pendingReadLeaders)
				cache.CleanCache()
			})
		})
	}
}

func TestCleanCacheCancellation(t *testing.T) {
	for _, stage := range []string{"write-input", "read-input", "read-capacity", "execution"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				cache := NewHotCache(ctx, core.NewBasicCluster())
				cache.readTaskSlots = make(chan struct{}, 1)
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
					started, release := make(chan struct{}), make(chan struct{})
					defer close(release)
					require.True(t, cache.CheckReadAsync(func(*HotPeerCache) {
						close(started)
						<-release
					}))
					<-started
					if stage == "read-capacity" {
						require.True(t, cache.CheckReadAsync(nil))
					}
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
				if stage == "read-input" {
					require.Empty(t, cache.readTaskSlots, "canceled admission must release its reservation")
				}
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
		{name: "both-leaders-hot", hotStores: []uint64{1, 2, 3}},
		{name: "only-old-leader-hot", hotStores: []uint64{1, 3}},
		{name: "only-new-leader-hot", hotStores: []uint64{2, 3}},
		{name: "old-leader-removed", hotStores: []uint64{1, 2, 3}, removeOldLeader: true},
		{name: "no-leader", hotStores: []uint64{1, 2, 3}, noLeader: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cluster := core.NewBasicCluster()
			region := newBenchmarkRegion(1, 3)
			for _, peer := range region.GetPeers() {
				cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: peer.GetStoreId()}))
			}
			cache := NewHotCache(t.Context(), cluster)
			loads := make([]float64, utils.RegionStatCount)
			loads[utils.RegionReadBytes] = 1 << 20
			loads[utils.RegionReadKeys] = 1024
			loads[utils.RegionReadQueryNum] = 512
			for _, storeID := range tc.hotStores {
				re.True(cache.CheckReadPeerAsync(region, region.GetStorePeer(storeID), loads, utils.StoreHeartBeatReportInterval))
			}
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
			after := cache.GetHotPeerStats(utils.Read, 0)
			for _, storeID := range tc.hotStores {
				oldItem := before[storeID][0]
				re.Equal(storeID == 1, oldItem.IsLeader(), "previously returned stats must stay unchanged")
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
			}
			for _, peer := range region.GetPeers() {
				if before[peer.GetStoreId()] == nil {
					re.Empty(after[peer.GetStoreId()], "region heartbeats must not create read statistics")
				}
			}

			cache.CheckRegionFlowAsync(region)
			re.Equal(after, cache.GetHotPeerStats(utils.Read, 0), "unchanged heartbeats must preserve cooldown")
			for _, storeID := range tc.hotStores {
				if region.GetStorePeer(storeID) == nil {
					continue
				}
				re.True(cache.CheckReadPeerAsync(region, region.GetStorePeer(storeID), loads, utils.StoreHeartBeatReportInterval))
			}
			for storeID, stats := range cache.GetHotPeerStats(utils.Read, 0) {
				if len(stats) > 0 {
					re.Equal(after[storeID][0].lastTransferLeaderTime, stats[0].lastTransferLeaderTime)
				}
			}
		})
	}
}

func TestRegionMetadataPreservesReadSampleTTL(t *testing.T) {
	for _, transfers := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("transfers=%d", transfers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, cache, region, _ := newHotReadCache(t, []uint64{1, 2, 3})
				time.Sleep(20 * time.Second)
				for i := range transfers {
					region = region.Clone(core.WithLeader(region.GetStorePeer(uint64(i + 2))))
					cache.CheckRegionFlowAsync(region)
					_ = cache.GetHotPeerStats(utils.Read, 0)
				}
				cluster.PutRegion(region)
				time.Sleep(11 * time.Second)
				cache.CollectMetrics()
				for _, stats := range cache.GetHotPeerStats(utils.Read, 0) {
					require.Empty(t, stats, "region metadata must not extend the read sample TTL")
				}
			})
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
				queue := cache.readCache.taskQueue
				var input chan func(*HotPeerCache)
				var release chan struct{}
				if stage == "input" {
					input = make(chan func(*HotPeerCache))
					cache.readCache.taskQueue = &chanx.UnboundedChan[func(*HotPeerCache)]{In: input, Out: queue.Out}
				} else {
					cache.readTaskSlots = make(chan struct{}, 1)
					started := make(chan struct{})
					release = make(chan struct{})
					require.True(t, cache.CheckReadAsync(func(*HotPeerCache) {
						close(started)
						<-release
					}))
					<-started
					require.True(t, cache.CheckReadAsync(nil))
				}
				updated := region.Clone(core.SetWrittenBytes(2 << 20))
				done := make(chan struct{})
				t.Cleanup(func() {
					if stage == "input" {
						select {
						case task := <-input:
							cache.runReadTask(task)
						case <-done:
						}
						<-done
						cache.readCache.taskQueue = queue
					} else {
						select {
						case <-release:
						default:
							close(release)
						}
						<-done
					}
				})
				go func() {
					cache.CheckRegionFlowAsync(updated)
					// Both heartbeat implementations publish the region after handling statistics.
					cluster.PutRegion(updated)
					close(done)
				}()
				synctest.Wait()
				afterWrite := cache.GetHotPeerStats(utils.Write, 0)
				require.Greater(t, afterWrite[1][0].HotDegree, before[1][0].HotDegree,
					"read pressure must not delay healthy write sampling")
				require.Same(t, updated, cluster.GetRegion(region.GetID()),
					"read pressure must not hold up region publication")
				select {
				case <-done:
				default:
					t.Fatal("read metadata admission blocked the heartbeat caller")
				}
				require.False(t, cache.CheckReadPeerAsync(region, region.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval),
					"ordinary samples must still respect read queue pressure")
				// Keep the root at leader 1 to prove delivery, rather than root-tree recovery.
				cache.CheckRegionFlowAsync(updated.Clone(core.WithLeader(updated.GetStorePeer(2))))
				synctest.Wait()
				if stage == "input" {
					// The metadata wakeup must work even without any accepted read tasks.
					after := cache.readCache.GetHotPeerStats(0)
					require.False(t, after[1][0].IsLeader())
					require.True(t, after[2][0].IsLeader())
					cache.readCache.taskQueue = queue
				} else {
					close(release)
					synctest.Wait()
				}
				require.True(t, cache.CheckReadPeerAsync(updated, updated.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
				synctest.Wait()
				after := cache.GetHotPeerStats(utils.Read, 0)
				require.False(t, after[1][0].IsLeader(), "a later stale snapshot must not undo delivered metadata")
				require.True(t, after[2][0].IsLeader())
			})
		})
	}
}

func TestCoalescedReadMetadataPreservesTransferCooldown(t *testing.T) {
	for _, transferAway := range []bool{false, true} {
		t.Run(fmt.Sprintf("transfer-away=%t", transferAway), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, cache, region, loads := newHotReadCache(t, []uint64{1, 2, 3})
				region = withHeartbeatTerm(region, 10)
				cluster.PutRegion(region)
				cache.CheckRegionFlowAsync(region)
				_ = cache.GetHotPeerStats(utils.Read, 0)
				release := blockHotReadWorker(t, cache)
				intermediate := region
				if transferAway {
					intermediate = region.Clone(core.WithLeader(region.GetStorePeer(2)))
				}
				cache.CheckRegionFlowAsync(withHeartbeatTerm(intermediate, 11))
				cache.CheckRegionFlowAsync(withHeartbeatTerm(region, 12))
				stale := withHeartbeatTerm(region.Clone(core.WithLeader(region.GetStorePeer(2))), 11)
				cache.CheckRegionFlowAsync(stale)
				release()
				synctest.Wait()
				after := cache.GetHotPeerStats(utils.Read, 0)
				for storeID, stats := range after {
					require.Len(t, stats, 1)
					require.Equal(t, storeID == 1, stats[0].IsLeader(), "older metadata must not replace the coalesced newer term")
					require.Equal(t, transferAway, stats[0].IsNeedCoolDownTransferLeader(3, utils.Read),
						"a coalesced transfer away and back must cool down unchanged followers")
					require.Equal(t, after[1][0].lastTransferLeaderTime, stats[0].lastTransferLeaderTime)
				}
				cache.CheckRegionFlowAsync(stale)
				require.True(t, cache.CheckReadPeerAsync(region, region.GetStorePeer(3), loads, utils.StoreHeartBeatReportInterval))
				later := cache.GetHotPeerStats(utils.Read, 0)
				for storeID, stats := range later {
					require.Equal(t, storeID == 1, stats[0].IsLeader())
					require.Equal(t, after[storeID][0].lastTransferLeaderTime, stats[0].lastTransferLeaderTime)
				}
			})
		})
	}
}

func TestCoalescedReadMetadataRejectsStaleTransfers(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(fmt.Sprintf("published=%t", published), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, cache, region, _ := newHotReadCache(t, []uint64{1, 2, 3})
				if published {
					cluster.PutRegion(withHeartbeatTerm(region, 12))
				} else {
					cluster.PutRegion(withHeartbeatTerm(region, 10))
				}
				cache.CheckRegionFlowAsync(withHeartbeatTerm(region, 12))
				before := cache.GetHotPeerStats(utils.Read, 0)
				release := blockHotReadWorker(t, cache)
				cache.CheckRegionFlowAsync(withHeartbeatTerm(region.Clone(core.WithLeader(region.GetStorePeer(2))), 11))
				cache.CheckRegionFlowAsync(withHeartbeatTerm(region, 12))
				release()
				synctest.Wait()
				require.Equal(t, before, cache.GetHotPeerStats(utils.Read, 0),
					"coalescing rejected metadata must not start a transfer cooldown")
			})
		})
	}
}

func TestCoalescedReadMetadataPreservesUnknownTerm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, cache, region, _ := newHotReadCache(t, []uint64{1, 2, 3})
		cache.CheckRegionFlowAsync(withHeartbeatTerm(region, 12))
		_ = cache.GetHotPeerStats(utils.Read, 0)
		release := blockHotReadWorker(t, cache)
		cache.CheckRegionFlowAsync(region)
		cache.CheckRegionFlowAsync(withHeartbeatTerm(region.Clone(core.WithLeader(region.GetStorePeer(2))), 11))
		release()
		synctest.Wait()
		after := cache.GetHotPeerStats(utils.Read, 0)
		require.False(t, after[1][0].IsLeader())
		require.True(t, after[2][0].IsLeader(), "an unknown term must retain the original version-reset semantics")
	})
}

func TestCoalescedReadMetadataPreservesPeerRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, cache, region, _ := newHotReadCache(t, []uint64{1, 2, 3})
		cache.CheckRegionFlowAsync(region)
		_ = cache.GetHotPeerStats(utils.Read, 0)
		release := blockHotReadWorker(t, cache)
		cache.CheckRegionFlowAsync(region.Clone(core.WithRemoveStorePeer(3), core.WithIncConfVer()))
		cache.CheckRegionFlowAsync(region.Clone(core.SetRegionConfVer(region.GetRegionEpoch().GetConfVer() + 2)))
		release()
		synctest.Wait()
		after := cache.GetHotPeerStats(utils.Read, 0)
		require.Len(t, after[1], 1)
		require.Len(t, after[2], 1)
		require.Empty(t, after[3], "readding a peer must not resurrect its removed read sample")
	})
}

func blockHotReadWorker(t *testing.T, cache *HotCache) func() {
	t.Helper()
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	require.True(t, cache.CheckReadAsync(func(*HotPeerCache) {
		close(started)
		<-release
	}))
	<-started
	return unblock
}

func TestRegionTransferCoolsDownAllReadPeers(t *testing.T) {
	for _, hotStores := range [][]uint64{{1, 2, 3}, {1, 3}, {2, 3}, {3}} {
		t.Run(fmt.Sprint(hotStores), func(t *testing.T) {
			_, cache, region, loads := newHotReadCache(t, hotStores)
			cache.CheckRegionFlowAsync(region)
			before := cache.GetHotPeerStats(utils.Read, 0)
			region = region.Clone(core.WithLeader(region.GetStorePeer(2)))
			cache.CheckRegionFlowAsync(region)
			after := cache.GetHotPeerStats(utils.Read, 0)
			var transferTime time.Time
			for storeID, stats := range after {
				require.Len(t, stats, 1)
				stat := stats[0]
				require.True(t, stat.IsNeedCoolDownTransferLeader(3, utils.Read), "every cached replica must observe the transfer cooldown")
				if transferTime.IsZero() {
					transferTime = stat.lastTransferLeaderTime
				}
				require.Equal(t, transferTime, stat.lastTransferLeaderTime)
				require.False(t, before[storeID][0].IsNeedCoolDownTransferLeader(3, utils.Read))
				require.True(t, cache.CheckReadPeerAsync(region, region.GetStorePeer(storeID), loads, utils.StoreHeartBeatReportInterval))
			}
			cache.CheckRegionFlowAsync(region)
			for _, stats := range cache.GetHotPeerStats(utils.Read, 0) {
				require.Len(t, stats, 1)
				require.Equal(t, transferTime, stats[0].lastTransferLeaderTime, "later samples and unchanged metadata must preserve cooldown")
			}
		})
	}
}

func TestRemovedReadLeaderPreservesFollowerCooldown(t *testing.T) {
	_, cache, region, _ := newHotReadCache(t, []uint64{1, 3})
	region = region.Clone(core.WithLeader(region.GetStorePeer(2)), core.WithRemoveStorePeer(1))
	cache.CheckRegionFlowAsync(region)
	after := cache.GetHotPeerStats(utils.Read, 0)
	require.Empty(t, after[1])
	require.Len(t, after[3], 1)
	require.True(t, after[3][0].IsNeedCoolDownTransferLeader(3, utils.Read))
}

func TestReadSampleRefreshesTTLAfterLeaderTransfer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, cache, region, loads := newHotReadCache(t, []uint64{1, 2})
		time.Sleep(20 * time.Second)
		region = region.Clone(core.WithLeader(region.GetStorePeer(2)))
		cache.CheckRegionFlowAsync(region)
		require.True(t, cache.CheckReadPeerAsync(region, region.GetStorePeer(2), loads, utils.StoreHeartBeatReportInterval))
		_ = cache.GetHotPeerStats(utils.Read, 0)
		time.Sleep(11 * time.Second)
		cache.CollectMetrics()
		after := cache.GetHotPeerStats(utils.Read, 0)
		require.Empty(t, after[1])
		require.Len(t, after[2], 1, "a new read sample must refresh its own TTL")
		time.Sleep(31 * time.Second)
		cache.CollectMetrics()
		require.Empty(t, cache.GetHotPeerStats(utils.Read, 0)[2])
	})
}

func TestReviewExpiredStatsDoNotEraseLeaderKnowledge(t *testing.T) {
	for _, publishBeforeGC := range []bool{false, true} {
		t.Run(fmt.Sprintf("publish-before-gc=%t", publishBeforeGC), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, cache, oldRegion, loads := newHotReadCache(t, []uint64{1, 2})
				time.Sleep(31 * time.Second)
				region := oldRegion.Clone(core.WithLeader(oldRegion.GetStorePeer(2)))
				cache.CheckRegionFlowAsync(region)
				_ = cache.GetHotPeerStats(utils.Read, 0)
				storeSnapshot := cluster.GetRegion(region.GetID())
				if publishBeforeGC {
					cluster.PutRegion(region)
				}
				cache.CollectMetrics()
				for _, stats := range cache.GetHotPeerStats(utils.Read, 0) {
					require.Empty(t, stats, "leader metadata must not keep expired samples alive")
				}
				require.True(t, cache.CheckReadPeerAsync(storeSnapshot, storeSnapshot.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
				after := cache.GetHotPeerStats(utils.Read, 0)
				require.Len(t, after[1], 1)
				require.False(t, after[1][0].IsLeader(), "sample expiry must not erase observed leadership")
				cluster.PutRegion(region)
				time.Sleep(31 * time.Second)
				cache.CollectMetrics()
				require.Empty(t, cache.GetHotPeerStats(utils.Read, 0)[1])
				require.Empty(t, cache.readCache.pendingReadLeaders, "published leadership must not leave pending metadata behind")
				require.True(t, cache.CheckReadPeerAsync(storeSnapshot, storeSnapshot.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
				after = cache.GetHotPeerStats(utils.Read, 0)
				require.Len(t, after[1], 1)
				require.False(t, after[1][0].IsLeader(), "an old snapshot must also be safe after pending metadata is reclaimed")
			})
		})
	}
}

func TestRegionMetadataBeforeFirstHotRead(t *testing.T) {
	_, cache, region, loads := newHotReadCache(t, nil)
	newRegion := region.Clone(core.WithLeader(region.GetStorePeer(2)))
	cache.CheckRegionFlowAsync(newRegion)
	require.Empty(t, cache.GetHotPeerStats(utils.Read, 0))
	require.True(t, cache.CheckReadPeerAsync(region, region.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
	after := cache.GetHotPeerStats(utils.Read, 0)
	require.Len(t, after[1], 1)
	require.False(t, after[1][0].IsLeader())
}

func TestPendingReadLeaderCleanupAfterRegionRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cluster, cache, region, _ := newHotReadCache(t, []uint64{1, 2})
		cache.CheckRegionFlowAsync(region.Clone(core.WithLeader(region.GetStorePeer(2))))
		_ = cache.GetHotPeerStats(utils.Read, 0)
		cluster.RemoveRegion(region)
		time.Sleep(31 * time.Second)
		cache.CollectMetrics()
		_ = cache.GetHotPeerStats(utils.Read, 0)
		require.Empty(t, cache.readCache.pendingReadLeaders)
	})
}

func TestStaleReadSnapshotPreservesRegionLeader(t *testing.T) {
	for _, hotStores := range [][]uint64{{1, 2, 3}, {1}, {2}, {3}} {
		for _, storeID := range []uint64{1, 2, 3} {
			for _, leaderID := range []uint64{0, 2} {
				t.Run(fmt.Sprintf("hot=%v/store=%d/leader=%d", hotStores, storeID, leaderID), func(t *testing.T) {
					cluster, cache, oldRegion, loads := newHotReadCache(t, hotStores)
					region := oldRegion.Clone(core.WithLeader(oldRegion.GetStorePeer(leaderID)))
					cache.CheckRegionFlowAsync(region)
					afterMetadata := cache.GetHotPeerStats(utils.Read, 0)
					// The region task can run before the root tree publishes the new leader.
					storeSnapshot := cluster.GetRegion(region.GetID())
					require.True(t, cache.CheckReadPeerAsync(storeSnapshot, storeSnapshot.GetStorePeer(storeID), loads, utils.StoreHeartBeatReportInterval))
					afterStore := cache.GetHotPeerStats(utils.Read, 0)
					require.Len(t, afterStore[storeID], 1)
					for id, stats := range afterStore {
						for _, stat := range stats {
							require.Equal(t, id == leaderID, stat.IsLeader(), "a stale read snapshot must not overwrite region leadership")
							if before := afterMetadata[id]; len(before) > 0 {
								require.Equal(t, before[0].lastTransferLeaderTime, stat.lastTransferLeaderTime)
								if id == storeID {
									require.Equal(t, before[0].HotDegree+1, stat.HotDegree, "the read sample must still be counted")
								}
							}
						}
						for _, stat := range afterMetadata[id] {
							require.Equal(t, id == leaderID, stat.IsLeader(), "previously returned stats must stay unchanged")
						}
					}
					cluster.PutRegion(region)
					cache.CheckRegionFlowAsync(region)
					require.Equal(t, afterStore, cache.GetHotPeerStats(utils.Read, 0))
				})
			}
		}
	}
}

func TestStaleRegionHeartbeatPreservesReadLeader(t *testing.T) {
	for _, published := range []bool{false, true} {
		for _, expired := range []bool{false, true} {
			t.Run(fmt.Sprintf("published=%t/expired=%t", published, expired), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					cluster, cache, region, loads := newHotReadCache(t, []uint64{1, 2, 3})
					oldRegion := withHeartbeatTerm(region, 10)
					cluster.PutRegion(oldRegion)
					newRegion := withHeartbeatTerm(region.Clone(core.WithLeader(region.GetStorePeer(2))), 11)
					cache.CheckRegionFlowAsync(newRegion)
					before := cache.GetHotPeerStats(utils.Read, 0)
					if published {
						cluster.PutRegion(newRegion)
					}
					if expired {
						time.Sleep(31 * time.Second)
						cache.CollectMetrics()
						for _, stats := range cache.GetHotPeerStats(utils.Read, 0) {
							require.Empty(t, stats)
						}
					}
					// The old heartbeat passed precheck before the new heartbeat,
					// but was delayed before admitting its metadata task.
					cache.CheckRegionFlowAsync(oldRegion)
					after := cache.GetHotPeerStats(utils.Read, 0)
					if !expired {
						require.Equal(t, before, after, "older metadata must preserve leader roles and cooldowns")
					}
					require.True(t, cache.CheckReadPeerAsync(oldRegion, oldRegion.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
					after = cache.GetHotPeerStats(utils.Read, 0)
					require.Len(t, after[1], 1)
					require.False(t, after[1][0].IsLeader(), "older metadata must not override the observed leader")
				})
			})
		}
	}
}

func TestPendingReadLeaderPreservesTerm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cluster, cache, region, loads := newHotReadCache(t, []uint64{1, 2})
		cluster.PutRegion(withHeartbeatTerm(region, 10))
		cache.CheckRegionFlowAsync(withHeartbeatTerm(region, 12))
		_ = cache.GetHotPeerStats(utils.Read, 0)
		time.Sleep(31 * time.Second)
		cache.CollectMetrics()
		for _, stats := range cache.GetHotPeerStats(utils.Read, 0) {
			require.Empty(t, stats)
		}
		staleRegion := withHeartbeatTerm(region.Clone(core.WithLeader(region.GetStorePeer(2))), 11)
		cache.CheckRegionFlowAsync(staleRegion)
		require.True(t, cache.CheckReadPeerAsync(region, region.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
		after := cache.GetHotPeerStats(utils.Read, 0)
		require.Len(t, after[1], 1)
		require.True(t, after[1][0].IsLeader(), "sample expiry must not erase the newer term with an unchanged leader")
		cluster.PutRegion(withHeartbeatTerm(region, 12))
		time.Sleep(31 * time.Second)
		cache.CollectMetrics()
		_ = cache.GetHotPeerStats(utils.Read, 0)
		require.Empty(t, cache.readCache.pendingReadLeaders, "published terms must release pending leader metadata")
	})
}

func TestRegionMetadataWithUnknownTerm(t *testing.T) {
	cluster, cache, region, _ := newHotReadCache(t, []uint64{1, 2})
	cluster.PutRegion(withHeartbeatTerm(region, 10))
	cache.CheckRegionFlowAsync(withHeartbeatTerm(region, 10))
	_ = cache.GetHotPeerStats(utils.Read, 0)
	cache.CheckRegionFlowAsync(region.Clone(core.WithLeader(region.GetStorePeer(2))))
	after := cache.GetHotPeerStats(utils.Read, 0)
	require.False(t, after[1][0].IsLeader())
	require.True(t, after[2][0].IsLeader(), "term zero must retain core heartbeat compatibility")
}

func withHeartbeatTerm(region *core.RegionInfo, term uint64) *core.RegionInfo {
	return core.RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
		Region:   region.GetMeta(),
		Leader:   region.GetLeader(),
		Term:     term,
		Interval: region.GetInterval(),
	}, 0)
}

func newHotReadCache(t *testing.T, hotStores []uint64) (*core.BasicCluster, *HotCache, *core.RegionInfo, []float64) {
	t.Helper()
	cluster := core.NewBasicCluster()
	region := newBenchmarkRegion(1, 3)
	for _, peer := range region.GetPeers() {
		cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: peer.GetStoreId()}))
	}
	cluster.PutRegion(region)
	cache := NewHotCache(t.Context(), cluster)
	loads := make([]float64, utils.RegionStatCount)
	loads[utils.RegionReadBytes] = 1 << 20
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
			compactExpired, compactWrite := newRegionFlowTasks(region)
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
				runPendingRegionHeartbeatTaskBenchmark(b, peerCount, newRegionFlowTasks)
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
