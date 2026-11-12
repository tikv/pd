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

// newSplitRecoveryTestManager keeps target stores healthy so a group expires
// only when a test changes its policy, rather than because of stale heartbeats.
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

// TestEnableSplitAfterAffinityChanges requires one recovery notification across
// concurrent heartbeats, including when the previous policy was already cached.
func TestEnableSplitAfterAffinityChanges(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *Manager, *serverconfig.PersistOptions, *core.RegionInfo) *core.RegionInfo
	}{
		{"group deleted", func(t *testing.T, m *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			require.NoError(t, m.DeleteAffinityGroups([]string{"g1"}, true))
			return r
		}},
		{"region crosses group boundary", func(_ *testing.T, _ *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			return r.Clone(core.WithEndKey([]byte("zz")), core.WithIncVersion())
		}},
		{"affinity scheduling disabled", func(_ *testing.T, _ *Manager, c *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			c.SetAffinityScheduleLimit(0)
			return r
		}},
		{"size reaches limit", func(_ *testing.T, _ *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			return r.Clone(core.SetApproximateSize(80))
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
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() { m.MaybeEnableSplit(region, streams) })
			}
			wg.Wait()
			re.Equal(1, streams.MsgLength(), "concurrent heartbeats send one recovery notification")
			msg, ok := streams.PopMsg().(*pdpb.RegionHeartbeatResponse)
			re.True(ok)
			re.NotNil(msg.GetChangeSplit())
			re.True(msg.GetChangeSplit().GetAutoSplitEnabled())
		})
	}
}

// TestSplitTracking covers renewal, expiry and recovery using the same denial.
func TestSplitTracking(t *testing.T) {
	re := require.New(t)
	m, conf, region, streams := newSplitRecoveryTestManager(t)
	admin, expired := region.Clone(core.WithNewRegionID(2)), region.Clone(core.WithNewRegionID(3))
	// Allowed requests, including administrative splits of affinity Regions, are not tracked.
	re.True(m.CheckSplit(nil, pdpb.SplitReason_SIZE, streams))
	re.True(m.CheckSplit(admin, pdpb.SplitReason_ADMIN, streams))
	re.False(m.CheckSplit(region, pdpb.SplitReason_SIZE, streams))
	re.False(m.CheckSplit(expired, pdpb.SplitReason_SIZE, streams))
	re.Equal(2, streams.MsgLength())
	re.NoError(streams.Drain(2))
	deadline, ok := m.splitDeniedRegions.Peek(region.GetID())
	re.True(ok)
	m.MaybeEnableSplit(region, streams)
	unchanged, _ := m.splitDeniedRegions.Peek(region.GetID())
	re.Equal(deadline, unchanged, "heartbeats far from the deadline do not extend the TTL")
	m.splitDeniedRegions.Put(region.GetID(), time.Now().Add(splitDeniedRenewMargin/2))
	m.MaybeEnableSplit(region.Clone(core.WithLeader(nil)), streams)
	re.Zero(streams.MsgLength())
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { m.MaybeEnableSplit(region, streams) })
	}
	wg.Wait()
	re.Equal(1, streams.MsgLength(), "concurrent heartbeats renew once")
	re.NoError(streams.Drain(1))
	deadline, ok = m.splitDeniedRegions.Peek(region.GetID())
	re.True(ok)
	re.Greater(time.Until(deadline.(time.Time)), splitDeniedRegionTTL-time.Minute)
	m.MaybeEnableSplit(region, streams)
	re.Zero(streams.MsgLength(), "renewal restarts the deadline")
	m.splitDeniedRegions.Put(region.GetID(), time.Now().Add(splitDeniedRenewMargin/2))
	m.splitDeniedRegions.Put(expired.GetID(), time.Now().Add(-time.Second))

	conf.SetAffinityScheduleLimit(0)
	// Expired tracking falls back to the storage-side TTL. A heartbeat without
	// a leader keeps the entry for the next leader heartbeat.
	for _, r := range []*core.RegionInfo{nil, admin, expired, region.Clone(core.WithLeader(nil))} {
		m.MaybeEnableSplit(r, streams)
	}
	re.Zero(streams.MsgLength())
	m.MaybeEnableSplit(region, streams)
	re.Equal(1, streams.MsgLength())
	re.Zero(m.splitDeniedRegions.Len())
}

// TestSplitQueueFull leaves dropped notifications to the storage TTL fallback.
func TestSplitQueueFull(t *testing.T) {
	for _, path := range []string{"denial", "renewal", "recovery"} {
		t.Run(path, func(t *testing.T) {
			re := require.New(t)
			m, _, region, streams := newSplitRecoveryTestManager(t)
			if path != "denial" {
				re.False(m.CheckSplit(region, pdpb.SplitReason_SIZE, streams))
				re.NoError(streams.Drain(1))
				m.splitDeniedRegions.Put(region.GetID(), time.Now().Add(splitDeniedRenewMargin/2))
			}
			if path == "recovery" {
				region = region.Clone(core.SetApproximateSize(80))
			}
			for range 1024 {
				streams.SendMsg(region, &hbstream.Operation{})
			}
			send := func() {
				if path == "denial" {
					re.False(m.CheckSplit(region, pdpb.SplitReason_SIZE, streams))
				} else {
					m.MaybeEnableSplit(region, streams)
				}
			}
			// A blocking send fails the test timeout instead of returning here.
			send()
			re.Equal(1024, streams.MsgLength())
			re.True(m.CheckSplit(region.Clone(core.WithNewRegionID(2)), pdpb.SplitReason_ADMIN, streams))
			re.NoError(streams.Drain(1024))
			if path == "recovery" {
				re.Zero(m.splitDeniedRegions.Len())
			} else {
				renewed, ok := m.splitDeniedRegions.Peek(region.GetID())
				re.True(ok)
				re.Greater(time.Until(renewed.(time.Time)), splitDeniedRegionTTL-time.Minute)
			}
			m.MaybeEnableSplit(region, streams)
			re.Zero(streams.MsgLength(), "delivery loss relies on the storage TTL")
		})
	}
}
