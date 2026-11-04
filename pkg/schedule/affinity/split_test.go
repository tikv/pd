// Copyright 2025 TiKV Project Authors.
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

package affinity

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keyutil"
	serverconfig "github.com/tikv/pd/server/config"
)

func TestAllowSplit(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := uint64(1); i <= 3; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: i, Address: fmt.Sprintf("test%d", i)})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	// Enable affinity scheduling
	conf := mockconfig.NewTestOptions()
	cfg := conf.GetScheduleConfig().Clone()
	cfg.AffinityScheduleLimit = 4 // Enable affinity scheduling
	conf.SetScheduleConfig(cfg)

	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	groupID := "g1"
	keyRange := keyutil.KeyRange{StartKey: []byte("a"), EndKey: []byte("z")}
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: groupID, KeyRanges: []keyutil.KeyRange{keyRange}}})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 3})
	re.NoError(err)
	defer func() {
		err := manager.DeleteAffinityGroups([]string{groupID}, true)
		re.NoError(err)
	}()

	conf.SetMaxAffinityMergeRegionSize(10)

	// Case 1: small affinity region will not be split but admin split will always be allowed
	smallRegion := generateRegionForTest(1, []uint64{1, 2, 3}, keyutil.KeyRange{StartKey: []byte("b"), EndKey: []byte("y")})
	smallRegion = smallRegion.Clone(
		core.SetApproximateSize(5),
		core.SetApproximateKeys(int64(5*config.RegionSizeToKeysRatio)),
	)
	re.False(manager.AllowSplit(smallRegion, pdpb.SplitReason_SIZE))
	re.False(manager.AllowSplit(smallRegion, pdpb.SplitReason_LOAD))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_ADMIN))

	// Case 2: large affinity region will be split
	maxSize := (int64(conf.GetMaxAffinityMergeRegionSize()) + affinityRegionSizeBufferMB) * affinityRegionSizeMultiplier
	largeRegion := smallRegion.Clone(
		core.SetApproximateSize(maxSize+1),
		core.SetApproximateKeys((maxSize+1)*int64(config.RegionSizeToKeysRatio)),
	)
	re.True(manager.AllowSplit(largeRegion, pdpb.SplitReason_SIZE))
	re.True(manager.AllowSplit(largeRegion, pdpb.SplitReason_LOAD))
	re.True(manager.AllowSplit(largeRegion, pdpb.SplitReason_ADMIN))

	// Case 3: small affinity region will be split when affinity is disabled
	oldLimit := conf.GetAffinityScheduleLimit()
	conf.SetAffinityScheduleLimit(0)
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_SIZE))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_LOAD))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_ADMIN))
	conf.SetAffinityScheduleLimit(8)
	re.False(manager.AllowSplit(smallRegion, pdpb.SplitReason_SIZE))
	re.False(manager.AllowSplit(smallRegion, pdpb.SplitReason_LOAD))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_ADMIN))
	conf.SetAffinityScheduleLimit(oldLimit)

	// Case 4: small affinity region will be split when affinity group expired
	manager.ExpireAffinityGroup(groupID)
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_SIZE))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_LOAD))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_ADMIN))
}

func newSplitRecoveryTestManager(t *testing.T) (*Manager, *serverconfig.PersistOptions, *core.RegionInfo, *hbstream.HeartbeatStreams) {
	t.Helper()
	re := require.New(t)
	conf := mockconfig.NewTestOptions()
	conf.SetAffinityScheduleLimit(4)
	conf.SetMaxAffinityMergeRegionSize(10)
	store := storage.NewStorageWithMemoryBackend()
	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 3; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id}, core.SetLastHeartbeatTS(time.Now())))
	}
	regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(t.Context(), store, stores, conf, regionLabeler)
	re.NoError(err)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID: "g1", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("z")}},
	}}))
	_, err = manager.UpdateAffinityGroupPeers("g1", 1, []uint64{1, 2, 3})
	re.NoError(err)
	region := generateRegionForTest(1, []uint64{1, 2, 3}, keyutil.KeyRange{StartKey: []byte("b"), EndKey: []byte("y")}).Clone(
		core.SetApproximateSize(60), core.SetApproximateKeys(1000),
	)
	streams := hbstream.NewTestHeartbeatStreams(t.Context(), stores, false)
	t.Cleanup(streams.Close)
	return manager, conf, region, streams
}

func TestEnableSplitAfterAffinityChanges(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *Manager, *serverconfig.PersistOptions, *core.RegionInfo) *core.RegionInfo
	}{
		{"group deleted", func(t *testing.T, m *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			require.NoError(t, m.DeleteAffinityGroups([]string{"g1"}, true))
			return r
		}},
		{"group range shrinks", func(t *testing.T, m *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			require.NoError(t, m.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{
				GroupID: "g1", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("z")}},
			}}))
			require.NoError(t, m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{
				GroupID: "g1", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("m"), EndKey: []byte("z")}},
			}}, nil))
			return r
		}},
		{"group ranges cleared", func(t *testing.T, m *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			require.NoError(t, m.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{
				GroupID: "g1", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("z")}},
			}}))
			return r
		}},
		{"region crosses group boundary", func(_ *testing.T, _ *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			return r.Clone(core.WithEndKey([]byte("zz")), core.WithIncVersion())
		}},
		{"group expired", func(_ *testing.T, m *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			m.ExpireAffinityGroup("g1")
			return r
		}},
		{"degraded group expiration elapsed", func(_ *testing.T, m *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			m.Lock()
			m.groups["g1"].SetAvailability(groupDegraded)
			m.groups["g1"].degradedExpiredAt = uint64(time.Now().Unix() - 1)
			m.Unlock()
			return r
		}},
		{"target cleared by watcher", func(_ *testing.T, m *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			m.SyncGroupFromEtcd(&Group{ID: "g1"})
			return r
		}},
		{"affinity scheduling disabled", func(_ *testing.T, _ *Manager, c *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			c.SetAffinityScheduleLimit(0)
			return r
		}},
		{"affinity merging disabled", func(_ *testing.T, _ *Manager, c *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			c.SetMaxAffinityMergeRegionSize(0)
			return r
		}},
		{"size limit reduced", func(_ *testing.T, _ *Manager, c *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			c.SetMaxAffinityMergeRegionSize(1)
			return r
		}},
		{"size reaches limit", func(_ *testing.T, _ *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			return r.Clone(core.SetApproximateSize(80))
		}},
		{"size exceeds limit", func(_ *testing.T, _ *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			return r.Clone(core.SetApproximateSize(81))
		}},
		{"keys reach limit", func(_ *testing.T, _ *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			return r.Clone(core.SetApproximateKeys(80 * config.RegionSizeToKeysRatio))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			m, conf, region, streams := newSplitRecoveryTestManager(t)
			re.False(m.CheckSplit(region, pdpb.SplitReason_LOAD, streams))
			re.Equal(1, streams.MsgLength())
			re.NoError(streams.Drain(1))
			// Prime the affinity cache to also exercise invalidation after changes.
			m.GetAndCacheRegionAffinityGroupState(region)
			m.MaybeEnableSplit(region, streams)
			re.Zero(streams.MsgLength(), "still denied")

			region = tc.change(t, m, conf, region)
			re.True(m.AllowSplit(region, pdpb.SplitReason_SIZE))
			m.MaybeEnableSplit(region, streams)
			re.Equal(1, streams.MsgLength(), "one recovery notification")
			re.NoError(streams.Drain(1))
			for range 3 {
				m.MaybeEnableSplit(region, streams)
			}
			re.Zero(streams.MsgLength(), "no duplicate recovery")
		})
	}
}

func TestSplitRecoveryOnlyForDeniedRegions(t *testing.T) {
	re := require.New(t)
	m, conf, region, streams := newSplitRecoveryTestManager(t)
	// An allowed administrative split does not enable automatic splitting.
	re.True(m.CheckSplit(region, pdpb.SplitReason_ADMIN, streams))
	m.MaybeEnableSplit(region, streams)
	re.Zero(streams.MsgLength())
	re.False(m.CheckSplit(region, pdpb.SplitReason_SIZE, streams))
	re.NoError(streams.Drain(1))
	re.True(m.CheckSplit(region, pdpb.SplitReason_ADMIN, streams))
	m.MaybeEnableSplit(region, streams)
	re.Zero(streams.MsgLength())

	conf.SetAffinityScheduleLimit(0)
	// No leader cannot consume the recovery; the next leader heartbeat must send it.
	m.MaybeEnableSplit(region.Clone(core.WithLeader(nil)), streams)
	re.Zero(streams.MsgLength())
	m.MaybeEnableSplit(region, streams)
	re.Equal(1, streams.MsgLength())
	re.NoError(streams.Drain(1))
	m.MaybeEnableSplit(region.Clone(core.WithNewRegionID(2)), streams)
	m.MaybeEnableSplit(nil, streams)
	re.True(m.CheckSplit(nil, pdpb.SplitReason_SIZE, streams))
	re.Zero(streams.MsgLength(), "never denied")

	// A new denial after recovery can produce one more recovery notification.
	conf.SetAffinityScheduleLimit(4)
	re.False(m.CheckSplit(region, pdpb.SplitReason_SIZE, streams))
	re.NoError(streams.Drain(1))
	conf.SetAffinityScheduleLimit(0)
	m.MaybeEnableSplit(region, streams)
	re.Equal(1, streams.MsgLength())
}

func TestSplitRecoveryTrackingBounds(t *testing.T) {
	re := require.New(t)
	m, conf, region, streams := newSplitRecoveryTestManager(t)
	re.False(m.CheckSplit(region, pdpb.SplitReason_SIZE, streams))
	re.NoError(streams.Drain(1))
	deadline, ok := m.splitDeniedRegions.Peek(region.GetID())
	re.True(ok)
	m.MaybeEnableSplit(region, streams)
	unchanged, ok := m.splitDeniedRegions.Peek(region.GetID())
	re.True(ok)
	re.Equal(deadline, unchanged, "heartbeats must not extend the TTL")

	m.splitDeniedRegions.Put(region.GetID(), time.Now().Add(-time.Second))
	conf.SetAffinityScheduleLimit(0)
	m.MaybeEnableSplit(region, streams)
	re.Zero(streams.MsgLength(), "expired tracking falls back to storage TTL")
	re.Zero(m.splitDeniedRegions.Len())

	for id := uint64(1); id <= splitDeniedRegionLimit+1; id++ {
		m.splitDeniedRegions.Put(id, time.Now().Add(splitDeniedRegionTTL))
	}
	re.Equal(splitDeniedRegionLimit, m.splitDeniedRegions.Len())
	m.MaybeEnableSplit(region, streams)
	re.Zero(streams.MsgLength(), "oldest denial was evicted")
	m.MaybeEnableSplit(region.Clone(core.WithNewRegionID(splitDeniedRegionLimit+1)), streams)
	re.Equal(1, streams.MsgLength(), "recent denial is retained")
}

func TestSplitRecoveryConcurrentHeartbeats(t *testing.T) {
	re := require.New(t)
	m, conf, region, streams := newSplitRecoveryTestManager(t)
	re.False(m.CheckSplit(region, pdpb.SplitReason_SIZE, streams))
	re.NoError(streams.Drain(1))
	conf.SetAffinityScheduleLimit(0)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { m.MaybeEnableSplit(region, streams) })
	}
	wg.Wait()
	re.Equal(1, streams.MsgLength())
}

func BenchmarkSplitRecoveryUntrackedHeartbeat(b *testing.B) {
	m := &Manager{splitDeniedRegions: cache.NewCache(splitDeniedRegionLimit, cache.LRUCache)}
	region := generateRegionForTest(1, []uint64{1, 2, 3}, keyutil.KeyRange{})
	b.ReportAllocs()
	for b.Loop() {
		// A cache miss must not consult affinity metadata or send a message.
		m.MaybeEnableSplit(region, nil)
	}
}
