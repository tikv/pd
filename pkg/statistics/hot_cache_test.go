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

func TestCleanCacheWaitsForPendingTasks(t *testing.T) {
	for kind := range utils.RWTypeLen {
		t.Run(kind.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, cache, region, _ := newHotReadCache(t, []uint64{1, 2})
				cache.CheckRegionFlowAsync(region)
				require.Len(t, cache.GetHotPeerStats(utils.Write, 0)[1], 1)
				release := blockHotCacheWorker(t, cache, kind)
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
		observed        bool
	}{
		{name: "first-hot-sample"},
		{name: "both-leaders-hot", hotStores: []uint64{1, 2, 3}},
		{name: "only-old-leader-hot", hotStores: []uint64{1, 3}},
		{name: "only-new-leader-hot", hotStores: []uint64{2, 3}},
		{name: "only-follower-hot", hotStores: []uint64{3}, observed: true},
		{name: "both-leaders-hot-with-metadata", hotStores: []uint64{1, 2, 3}, observed: true},
		{name: "only-old-leader-hot-with-metadata", hotStores: []uint64{1, 3}, observed: true},
		{name: "only-new-leader-hot-with-metadata", hotStores: []uint64{2, 3}, observed: true},
		{name: "old-leader-removed", hotStores: []uint64{1, 2, 3}, removeOldLeader: true},
		{name: "old-leader-removed-new-leader-cold", hotStores: []uint64{1, 3}, removeOldLeader: true},
		{name: "no-leader", hotStores: []uint64{1, 2, 3}, noLeader: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			cluster, cache, region, loads := newHotReadCache(t, tc.hotStores)
			oldRegion := region
			if tc.observed {
				cache.CheckRegionFlowAsync(region)
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
				cluster.PutRegion(updated)
				synctest.Wait()
				require.Greater(t, cache.GetHotPeerStats(utils.Write, 0)[1][0].HotDegree, before[1][0].HotDegree)
				require.Same(t, updated, cluster.GetRegion(region.GetID()), "read pressure must not hold up region publication")
				require.False(t, cache.CheckReadPeerAsync(region, region.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
				// Keep the root at leader 1 and admit no read tasks, so only the
				// independent metadata wakeup can deliver this transfer.
				cache.CheckRegionFlowAsync(updated.Clone(core.WithLeader(updated.GetStorePeer(2))))
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

func TestCoalescedReadMetadataPreservesTransferCooldown(t *testing.T) {
	for _, published := range []bool{false, true} {
		for _, transferAway := range []bool{false, true} {
			t.Run(fmt.Sprintf("published=%t/transfer-away=%t", published, transferAway), func(t *testing.T) {
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
					intermediate = withHeartbeatTerm(intermediate, 11)
					cache.CheckRegionFlowAsync(intermediate)
					if published {
						cluster.PutRegion(intermediate)
					}
					latest := withHeartbeatTerm(region, 12)
					cache.CheckRegionFlowAsync(latest)
					if published {
						cluster.PutRegion(latest)
					}
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

func TestReadMetadataPreservesUnknownTerm(t *testing.T) {
	for _, coalesced := range []bool{false, true} {
		t.Run(fmt.Sprintf("coalesced=%t", coalesced), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, cache, region, _ := newHotReadCache(t, []uint64{1, 2, 3})
				cluster.PutRegion(withHeartbeatTerm(region, 10))
				cache.CheckRegionFlowAsync(withHeartbeatTerm(region, 12))
				_ = cache.GetHotPeerStats(utils.Read, 0)
				var release func()
				if coalesced {
					release = blockHotReadWorker(t, cache)
				}
				cache.CheckRegionFlowAsync(region.Clone(core.WithLeader(region.GetStorePeer(2))))
				if !coalesced {
					after := cache.GetHotPeerStats(utils.Read, 0)
					require.False(t, after[1][0].IsLeader())
					require.True(t, after[2][0].IsLeader(), "term zero must retain core heartbeat compatibility")
				}
				cache.CheckRegionFlowAsync(withHeartbeatTerm(region.Clone(core.WithLeader(region.GetStorePeer(2))), 11))
				if coalesced {
					release()
					synctest.Wait()
				}
				after := cache.GetHotPeerStats(utils.Read, 0)
				require.False(t, after[1][0].IsLeader())
				require.True(t, after[2][0].IsLeader(), "an unknown term must reset the processed term guard")
			})
		})
	}
}

func TestCoalescedReadMetadataPreservesPeerRemoval(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(fmt.Sprintf("published=%t", published), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, cache, region, _ := newHotReadCache(t, []uint64{1, 2, 3})
				region = withHeartbeatTerm(region, 10)
				cluster.PutRegion(region)
				cache.CheckRegionFlowAsync(region)
				_ = cache.GetHotPeerStats(utils.Read, 0)
				release := blockHotReadWorker(t, cache)
				removed := withHeartbeatTerm(region.Clone(core.WithRemoveStorePeer(3), core.WithIncConfVer()), 11)
				cache.CheckRegionFlowAsync(removed)
				if published {
					cluster.PutRegion(removed)
				}
				readded := withHeartbeatTerm(removed.Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 3}), core.WithIncConfVer()), 12)
				cache.CheckRegionFlowAsync(readded)
				if published {
					cluster.PutRegion(readded)
				}
				release()
				synctest.Wait()
				after := cache.GetHotPeerStats(utils.Read, 0)
				require.Len(t, after[1], 1)
				require.Len(t, after[2], 1)
				require.Empty(t, after[3], "readding a peer must not resurrect its removed read sample")
			})
		})
	}
}

func TestQueuedReadSampleDoesNotResurrectRemovedPeer(t *testing.T) {
	for _, published := range []bool{false, true} {
		for _, replacement := range []bool{false, true} {
			for _, hotStores := range [][]uint64{nil, {1, 2, 3}} {
				t.Run(fmt.Sprintf("published=%t/replacement=%t/hot=%t", published, replacement, len(hotStores) > 0), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						cluster, cache, region, loads := newHotReadCache(t, hotStores)
						cache.CheckRegionFlowAsync(region)
						_ = cache.GetHotPeerStats(utils.Read, 0)
						release := blockHotReadWorker(t, cache)
						require.True(t, cache.CheckReadPeerAsync(region, region.GetStorePeer(3), loads, utils.StoreHeartBeatReportInterval))
						removed := region.Clone(core.WithRemoveStorePeer(3), core.WithIncConfVer())
						if replacement {
							cache.CheckRegionFlowAsync(removed)
							removed = removed.Clone(core.WithAddPeer(&metapb.Peer{Id: 100, StoreId: 3}), core.WithIncConfVer())
						}
						cache.CheckRegionFlowAsync(removed)
						if published {
							cluster.PutRegion(removed)
						}
						release()
						synctest.Wait()
						require.Empty(t, cache.GetHotPeerStats(utils.Read, 0)[3], "an obsolete sample must not restore a removed or replaced peer")
						if replacement {
							require.True(t, cache.CheckReadPeerAsync(removed, removed.GetStorePeer(3), loads, utils.StoreHeartBeatReportInterval))
							require.Len(t, cache.GetHotPeerStats(utils.Read, 0)[3], 1, "the replacement peer must accept its own read sample")
						}
					})
				})
			}
		}
	}
}

func TestRecreatedRegionResetsReadLeader(t *testing.T) {
	for _, published := range []bool{false, true} {
		for _, coalesced := range []bool{false, true} {
			t.Run(fmt.Sprintf("published=%t/coalesced=%t", published, coalesced), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					cluster, cache, region, loads := newHotReadCache(t, []uint64{1, 2, 3})
					region = withHeartbeatTerm(region, 100)
					cluster.PutRegion(region)
					var release func()
					if coalesced {
						release = blockHotReadWorker(t, cache)
					}
					cache.CheckRegionFlowAsync(region)
					if !coalesced {
						_ = cache.GetHotPeerStats(utils.Read, 0)
					}
					recreated := withHeartbeatTerm(region.Clone(core.SetRegionVersion(1), core.SetRegionConfVer(1), core.WithLeader(region.GetStorePeer(2))), 2)
					_, _, err := cluster.PreCheckPutRegion(recreated)
					require.NoError(t, err)
					cache.CheckRegionFlowAsync(recreated)
					publish := func() {
						_, err := cluster.CheckAndPutRootTree(&core.MetaProcessContext{Tracer: core.NewNoopHeartbeatProcessTracer()}, recreated)
						require.NoError(t, err, "core must accept online unsafe recovery with a lower term")
					}
					if published {
						publish()
					}
					if coalesced {
						release()
						synctest.Wait()
					}
					after := cache.GetHotPeerStats(utils.Read, 0)
					require.False(t, after[1][0].IsLeader())
					require.True(t, after[2][0].IsLeader(), "recovery must reset old terms even before root publication")
					if !published {
						publish()
					}
					for range 3 {
						cache.CheckRegionFlowAsync(recreated)
						for _, peer := range recreated.GetPeers() {
							require.True(t, cache.CheckReadPeerAsync(recreated, peer, loads, utils.StoreHeartBeatReportInterval))
						}
					}
					for storeID, stats := range cache.GetHotPeerStats(utils.Read, 0) {
						require.Len(t, stats, 1)
						require.Equal(t, storeID == 2, stats[0].IsLeader())
					}
				})
			})
		}
	}
}

func TestConcurrentReadMetadataFollowsAcceptedRoot(t *testing.T) {
	for _, epoch := range []string{"confver", "version"} {
		for _, coalesced := range []bool{false, true} {
			t.Run(fmt.Sprintf("epoch=%s/coalesced=%t", epoch, coalesced), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					cluster, cache, initial, loads := newHotReadCache(t, []uint64{1, 2, 3})
					initial = withHeartbeatTerm(initial, 10)
					cluster.PutRegion(initial)
					cache.CheckRegionFlowAsync(initial)
					_ = cache.GetHotPeerStats(utils.Read, 0)
					cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 4}))
					increment := core.WithIncConfVer()
					if epoch == "version" {
						increment = core.WithIncVersion()
					}
					higherTerm := withHeartbeatTerm(initial, 12)
					accepted := withHeartbeatTerm(initial.Clone(core.WithLeader(initial.GetStorePeer(2)),
						core.WithRemoveStorePeer(3), core.WithAddPeer(&metapb.Peer{Id: 104, StoreId: 4}), increment), 11)
					for _, region := range []*core.RegionInfo{higherTerm, accepted} {
						_, _, err := cluster.PreCheckPutRegion(region)
						require.NoError(t, err)
					}
					var release func()
					if coalesced {
						release = blockHotReadWorker(t, cache)
					}
					cache.CheckRegionFlowAsync(higherTerm)
					if !coalesced {
						_ = cache.GetHotPeerStats(utils.Read, 0)
					}
					cache.CheckRegionFlowAsync(accepted)
					_, err := cluster.CheckAndPutRootTree(core.ContextTODO(), accepted)
					require.NoError(t, err)
					_, err = cluster.CheckAndPutRootTree(core.ContextTODO(), higherTerm)
					require.Error(t, err, "a higher term does not make an obsolete epoch valid")
					if coalesced {
						release()
					}
					// Queries must converge even before any new read samples arrive.
					stat := cache.GetHotPeerStat(utils.Read, initial.GetID(), 1)
					require.NotNil(t, stat)
					require.False(t, stat.IsLeader())
					require.Empty(t, cache.GetHotPeerStats(utils.Read, 0)[3])
					for range 3 {
						require.True(t, cache.CheckReadPeerAsync(accepted, accepted.GetStorePeer(4), loads, utils.StoreHeartBeatReportInterval))
					}
					after := cache.GetHotPeerStats(utils.Read, 0)
					require.Len(t, after[4], 1, "rejected metadata must not suppress samples from the accepted peer")
					require.True(t, after[2][0].IsLeader())
				})
			})
		}
	}
}

func TestUnknownTermPublicationRestoresReadLeader(t *testing.T) {
	for _, tc := range []struct{ publishBeforeReplay, intermediateRoot bool }{{}, {publishBeforeReplay: true}, {intermediateRoot: true}} {
		t.Run(fmt.Sprintf("publish-before-replay=%t/intermediate-root=%t", tc.publishBeforeReplay, tc.intermediateRoot), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, cache, initial, loads := newHotReadCache(t, []uint64{1, 2, 3})
				cache.CheckRegionFlowAsync(initial)
				_ = cache.GetHotPeerStats(utils.Read, 0)
				release := blockHotReadWorker(t, cache)
				newLeader := initial.Clone(core.WithLeader(initial.GetStorePeer(2)))
				for _, region := range []*core.RegionInfo{newLeader, initial} {
					_, _, err := cluster.PreCheckPutRegion(region)
					require.NoError(t, err)
					cache.CheckRegionFlowAsync(region)
				}
				_, saveCache, _, _ := core.GenerateRegionGuideFunc(false)(core.ContextTODO(), initial, cluster.GetRegion(initial.GetID()))
				require.False(t, saveCache, "the unchanged heartbeat returns without publishing")
				if !tc.publishBeforeReplay {
					release()
					_ = cache.GetHotPeerStats(utils.Read, 0)
				}
				if tc.intermediateRoot {
					progress := initial.Clone(core.SetWrittenBytes(2 << 20))
					cache.CheckRegionFlowAsync(progress)
					_, err := cluster.CheckAndPutRootTree(core.ContextTODO(), progress)
					require.NoError(t, err)
					_ = cache.GetHotPeerStats(utils.Read, 0)
				}
				_, err := cluster.CheckAndPutRootTree(core.ContextTODO(), newLeader)
				require.NoError(t, err)
				if tc.publishBeforeReplay {
					release()
				}
				after := cache.GetHotPeerStats(utils.Read, 0)
				require.False(t, after[1][0].IsLeader(), "queries must follow publication without waiting for read samples")
				require.True(t, after[2][0].IsLeader())
				for range 3 {
					for _, peer := range newLeader.GetPeers() {
						require.True(t, cache.CheckReadPeerAsync(newLeader, peer, loads, utils.StoreHeartBeatReportInterval))
					}
				}
				after = cache.GetHotPeerStats(utils.Read, 0)
				require.False(t, after[1][0].IsLeader())
				require.True(t, after[2][0].IsLeader(), "fresh samples must retain the published leader")
			})
		})
	}
}

func TestInFlightReadMetadataRejectsStaleHeartbeat(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		epoch                 string
		originalTerm, newTerm uint64
		staleTerm             uint64
	}{
		{name: "term", originalTerm: 10, newTerm: 11, staleTerm: 10},
		{name: "confver", epoch: "confver", originalTerm: 10, newTerm: 10, staleTerm: 10},
		{name: "confver-newer-term", epoch: "confver", originalTerm: 10, newTerm: 10, staleTerm: 11},
		{name: "confver-unknown-term", epoch: "confver"},
		{name: "version", epoch: "version", originalTerm: 10, newTerm: 10, staleTerm: 10},
		{name: "version-newer-term", epoch: "version", originalTerm: 10, newTerm: 10, staleTerm: 11},
		{name: "version-unknown-term", epoch: "version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cluster, cache, original, loads := newHotReadCache(t, nil)
				original = withHeartbeatTerm(original, tc.originalTerm)
				cluster.PutRegion(original)
				latest := withHeartbeatTerm(original.Clone(core.WithLeader(original.GetStorePeer(2))), tc.newTerm)
				storeID := uint64(1)
				if tc.epoch != "" {
					cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 4}))
					increment := core.WithIncConfVer()
					if tc.epoch == "version" {
						increment = core.WithIncVersion()
					}
					latest = latest.Clone(core.WithRemoveStorePeer(3), core.WithAddPeer(&metapb.Peer{Id: 4, StoreId: 4}), increment)
					storeID = 4
				}
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
				cache.CheckRegionFlowAsync(latest)
				<-started
				stale := withHeartbeatTerm(original, tc.staleTerm)
				_, _, err := cluster.PreCheckPutRegion(stale)
				require.NoError(t, err, "the older heartbeat must pass admission against the old root")
				cache.CheckRegionFlowAsync(stale)
				_, err = cluster.CheckAndPutRootTree(core.ContextTODO(), latest)
				require.NoError(t, err)
				unblock()
				_ = cache.GetHotPeerStats(utils.Read, 0)
				_, err = cluster.CheckAndPutRootTree(core.ContextTODO(), stale)
				require.Error(t, err, "final root validation must reject the delayed heartbeat")
				for range 3 {
					require.True(t, cache.CheckReadPeerAsync(latest, latest.GetStorePeer(storeID), loads, utils.StoreHeartBeatReportInterval))
				}
				after := cache.GetHotPeerStats(utils.Read, 0)
				require.Len(t, after[storeID], 1, "current peer samples must survive an older detached heartbeat")
				require.False(t, after[storeID][0].IsLeader(), "older metadata must not roll back leadership")
			})
		})
	}
}

func TestQueuedMetadataRetainsLeaderKnowledgeDuringGC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cluster, cache, region, loads := newHotReadCache(t, nil)
		region = withHeartbeatTerm(region, 10)
		cluster.PutRegion(region)
		latest := withHeartbeatTerm(region.Clone(core.WithLeader(region.GetStorePeer(2))), 11)
		cache.CheckRegionFlowAsync(latest)
		_ = cache.GetHotPeerStats(utils.Read, 0)
		time.Sleep(31 * time.Second)
		require.True(t, cache.CheckReadAsync(func(readCache *HotPeerCache) {
			// The older heartbeat passes admission before publication, while a
			// running read task reaches GC only after publication catches up.
			cache.CheckRegionFlowAsync(region)
			cluster.PutRegion(latest)
			readCache.gc()
		}))
		_ = cache.GetHotPeerStats(utils.Read, 0)
		require.True(t, cache.CheckReadPeerAsync(latest, latest.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
		after := cache.GetHotPeerStats(utils.Read, 0)
		require.Len(t, after[1], 1)
		require.False(t, after[1][0].IsLeader(), "GC must not discard the term needed to reject queued older metadata")
		time.Sleep(31 * time.Second)
		cache.CollectMetrics()
		_ = cache.GetHotPeerStats(utils.Read, 0)
		require.Empty(t, cache.readCache.readMetadata, "drained metadata must allow published leader knowledge to be reclaimed")
	})
}

func TestBootstrapRegionRejectsStaleReadMetadata(t *testing.T) {
	cluster, cache, region, _ := newHotReadCache(t, []uint64{1, 2, 3},
		core.WithStartKey(nil), core.WithEndKey(nil), core.SetRegionVersion(1), core.SetRegionConfVer(1))
	region = withHeartbeatTerm(region, 100)
	cluster.PutRegion(region)
	cache.CheckRegionFlowAsync(region)
	before := cache.GetHotPeerStats(utils.Read, 0)
	stale := withHeartbeatTerm(region.Clone(core.WithLeader(region.GetStorePeer(2))), 2)
	_, _, err := cluster.PreCheckPutRegion(stale)
	require.Error(t, err, "the whole-key-range bootstrap region is not an unsafe-recovery recreation")
	cache.CheckRegionFlowAsync(stale)
	require.Equal(t, before, cache.GetHotPeerStats(utils.Read, 0))
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
				require.Empty(t, cache.readCache.readMetadata, "published leadership must not leave pending metadata behind")
				require.True(t, cache.CheckReadPeerAsync(storeSnapshot, storeSnapshot.GetStorePeer(1), loads, utils.StoreHeartBeatReportInterval))
				after = cache.GetHotPeerStats(utils.Read, 0)
				require.Len(t, after[1], 1)
				require.False(t, after[1][0].IsLeader(), "an old snapshot must also be safe after pending metadata is reclaimed")
			})
		})
	}
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
		require.Empty(t, cache.readCache.readMetadata)
	})
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

func TestStaleRegionEpochPreservesReadPeers(t *testing.T) {
	for _, epoch := range []string{"confver", "version"} {
		for _, terms := range [][2]uint64{{0, 0}, {10, 10}, {10, 11}} {
			for _, tc := range []struct {
				name      string
				published bool
				coalesced bool
				expired   bool
			}{
				{name: "root", published: true},
				{name: "processed"},
				{name: "coalesced", coalesced: true},
				{name: "published-coalesced", published: true, coalesced: true},
				{name: "expired-root", published: true, expired: true},
				{name: "expired-processed", expired: true},
			} {
				t.Run(fmt.Sprintf("%s/terms=%v/%s", epoch, terms, tc.name), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						cluster, cache, original, loads := newHotReadCache(t, []uint64{1, 2, 3})
						original = withHeartbeatTerm(original, terms[0])
						cluster.PutRegion(original)
						cache.CheckRegionFlowAsync(original)
						_ = cache.GetHotPeerStats(utils.Read, 0)
						stale := withHeartbeatTerm(original, terms[1])
						// The old heartbeat passes precheck before the peer replacement.
						_, _, err := cluster.PreCheckPutRegion(stale)
						require.NoError(t, err)
						cluster.PutStore(core.NewStoreInfo(&metapb.Store{Id: 4}))
						increment := core.WithIncConfVer()
						if epoch == "version" {
							increment = core.WithIncVersion()
						}
						newer := original.Clone(core.WithRemoveStorePeer(3), core.WithAddPeer(&metapb.Peer{Id: 4, StoreId: 4}), increment)
						publish := func() {
							_, err := cluster.CheckAndPutRootTree(&core.MetaProcessContext{Tracer: core.NewNoopHeartbeatProcessTracer()}, newer)
							require.NoError(t, err)
						}
						var release func()
						if tc.coalesced {
							release = blockHotReadWorker(t, cache)
						}
						cache.CheckRegionFlowAsync(newer)
						if tc.published {
							publish()
						}
						if !tc.coalesced {
							_ = cache.GetHotPeerStats(utils.Read, 0)
						}
						if tc.expired {
							time.Sleep(31 * time.Second)
							cache.CollectMetrics()
							for _, stats := range cache.GetHotPeerStats(utils.Read, 0) {
								require.Empty(t, stats)
							}
						}
						cache.CheckRegionFlowAsync(stale)
						if tc.coalesced {
							release()
							synctest.Wait()
						}
						_ = cache.GetHotPeerStats(utils.Read, 0)
						if !tc.published {
							publish()
						}
						_, err = cluster.CheckAndPutRootTree(&core.MetaProcessContext{Tracer: core.NewNoopHeartbeatProcessTracer()}, stale)
						require.Error(t, err, "core must reject the obsolete epoch")
						for range 3 {
							for _, peer := range newer.GetPeers() {
								require.True(t, cache.CheckReadPeerAsync(newer, peer, loads, utils.StoreHeartBeatReportInterval))
							}
						}
						// An old store snapshot must not restore the removed peer either.
						require.True(t, cache.CheckReadPeerAsync(original, original.GetStorePeer(3), loads, utils.StoreHeartBeatReportInterval))
						after := cache.GetHotPeerStats(utils.Read, 0)
						require.Len(t, after[4], 1, "rejected metadata must not suppress current peer samples")
						require.Empty(t, after[3], "rejected metadata must not restore a removed peer")
					})
				})
			}
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
		require.Empty(t, cache.readCache.readMetadata, "published terms must release pending leader metadata")
	})
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
