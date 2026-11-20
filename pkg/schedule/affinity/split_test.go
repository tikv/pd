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
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
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

	// Case 4: small affinity region will not be split after its group expires, elapsed or explicitly,
	// because the group keeps its target
	manager.DegradeAffinityGroup(groupID)
	manager.Lock()
	manager.groups[groupID].degradedExpiredAt = uint64(time.Now().Unix() - 1)
	manager.Unlock()
	re.False(manager.AllowSplit(smallRegion, pdpb.SplitReason_SIZE))
	manager.ExpireAffinityGroup(groupID)
	re.False(manager.AllowSplit(smallRegion, pdpb.SplitReason_SIZE))
	re.False(manager.AllowSplit(smallRegion, pdpb.SplitReason_LOAD))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_ADMIN))

	// Case 5: small affinity region will be split when the group target is cleared
	manager.SyncGroupFromEtcd(&Group{ID: groupID})
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_SIZE))
	re.True(manager.AllowSplit(smallRegion, pdpb.SplitReason_LOAD))
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

func TestCheckSplitWhenAffinityDisabled(t *testing.T) {
	for _, disabled := range []string{"scheduling", "merging"} {
		t.Run(disabled, func(t *testing.T) {
			re := require.New(t)
			m, conf, region, streams := newSplitRecoveryTestManager(t)
			if disabled == "scheduling" {
				conf.SetAffinityScheduleLimit(0)
			} else {
				conf.SetMaxAffinityMergeRegionSize(0)
			}
			var wg sync.WaitGroup
			defer wg.Wait()
			m.splitMutex.Lock()
			defer m.splitMutex.Unlock()
			done := make(chan bool, 1)
			wg.Go(func() { done <- m.CheckSplit(region, pdpb.SplitReason_SIZE, streams) })
			select {
			case allowed := <-done:
				re.True(allowed)
			case <-time.After(time.Second):
				t.Fatal("disabled affinity split waited for splitMutex")
			}
			re.Zero(streams.MsgLength())
			re.Zero(m.splitDeniedRegions.Len())
		})
	}
}

func TestSplitPolicyLookupDoesNotHoldSplitMutex(t *testing.T) {
	const lookupHook = "github.com/tikv/pd/pkg/schedule/affinity/afterRegionLabelLookup"
	for _, path := range []string{"split request", "heartbeat"} {
		t.Run(path, func(t *testing.T) {
			re := require.New(t)
			m, _, region, streams := newSplitRecoveryTestManager(t)
			re.False(m.CheckSplit(region, pdpb.SplitReason_SIZE, streams))
			re.NotNil(streams.PopMsg())
			m.GetAndCacheRegionAffinityGroupState(region)

			lookedUp := false
			re.NoError(failpoint.EnableCall(lookupHook, func() {
				lookedUp = true
				re.True(m.splitMutex.TryLock(), "a slow label lookup must not hold splitMutex")
				m.splitMutex.Unlock()
				// Record a new denial using the cached policy before the lookup returns.
				re.False(m.CheckSplit(region, pdpb.SplitReason_SIZE, streams))
			}))
			defer func() { re.NoError(failpoint.Disable(lookupHook)) }()

			// This version spans the group boundary, so its policy requires a fresh lookup.
			changed := region.Clone(core.WithEndKey([]byte("zz")), core.WithIncVersion())
			if path == "split request" {
				re.True(m.CheckSplit(changed, pdpb.SplitReason_SIZE, streams))
			} else {
				m.MaybeEnableSplit(changed, streams)
			}
			re.True(lookedUp)
			re.Equal(1, streams.MsgLength(), "the old lookup must not send recovery for the new denial")
			re.Equal(1, m.splitDeniedRegions.Len())
		})
	}
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
		{"group target cleared", func(_ *testing.T, m *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			m.SyncGroupFromEtcd(&Group{ID: "g1"})
			return r
		}},
		{"region crosses group boundary", func(_ *testing.T, _ *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			return r.Clone(core.WithEndKey([]byte("zz")), core.WithIncVersion())
		}},
		{"affinity scheduling disabled", func(_ *testing.T, _ *Manager, c *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			c.SetAffinityScheduleLimit(0)
			return r
		}},
		{"affinity merging disabled", func(_ *testing.T, _ *Manager, c *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			c.SetMaxAffinityMergeRegionSize(0)
			return r
		}},
		{"size reaches limit", func(_ *testing.T, _ *Manager, _ *serverconfig.PersistOptions, r *core.RegionInfo) *core.RegionInfo {
			return r.Clone(core.SetApproximateSize(80))
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
			m.MaybeEnableSplit(region.Clone(core.WithLeader(nil)), streams)
			re.Zero(streams.MsgLength(), "wait for a leader before sending recovery")
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

func TestSplitDenialExpiresWithUnchangedHeartbeats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _, region, streams := newSplitRecoveryTestManager(t)
		re.True(m.CheckSplit(nil, pdpb.SplitReason_SIZE, streams))
		re.True(m.CheckSplit(region, pdpb.SplitReason_ADMIN, streams))
		re.Zero(m.splitDeniedRegions.Len())
		re.False(m.CheckSplit(region, pdpb.SplitReason_SIZE, streams))
		msg, ok := streams.PopMsg().(*pdpb.RegionHeartbeatResponse)
		re.True(ok)
		re.NotNil(msg.GetChangeSplit())
		re.False(msg.GetChangeSplit().GetAutoSplitEnabled())

		// The expired group keeps its target. Disabled split checks continue
		// to report a low estimate during the entire validator TTL.
		m.ExpireAffinityGroup("g1")
		re.False(m.AllowSplit(region, pdpb.SplitReason_SIZE))
		for range splitDeniedRegionTTL / time.Minute {
			time.Sleep(time.Minute)
			m.MaybeEnableSplit(region.Clone(), streams)
			re.Zero(streams.MsgLength(), "a heartbeat must not reset TiKV's validator TTL")
		}
		re.Zero(m.splitDeniedRegions.Len())

		// A normal split check can now report the oversized Region.
		probed := region.Clone(core.SetApproximateSize(80))
		m.MaybeEnableSplit(probed, streams)
		re.True(m.CheckSplit(probed, pdpb.SplitReason_SIZE, streams))
		re.Zero(streams.MsgLength(), "the validator has already expired")
	})
}

// TestSplitQueueFull leaves dropped notifications to the storage TTL fallback.
func TestSplitQueueFull(t *testing.T) {
	for _, path := range []string{"denial", "recovery"} {
		t.Run(path, func(t *testing.T) {
			re := require.New(t)
			m, _, region, streams := newSplitRecoveryTestManager(t)
			if path != "denial" {
				re.False(m.CheckSplit(region, pdpb.SplitReason_SIZE, streams))
				re.NoError(streams.Drain(1))
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
			re.NoError(streams.Drain(1024))
			if path == "recovery" {
				re.Zero(m.splitDeniedRegions.Len())
			} else {
				re.Equal(1, m.splitDeniedRegions.Len())
			}
			m.MaybeEnableSplit(region, streams)
			re.Zero(streams.MsgLength(), "delivery loss relies on the storage TTL")
		})
	}
}
