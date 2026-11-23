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

package affinity

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func newTargetTestManager(t *testing.T) (*Manager, storage.Storage) {
	store := storage.NewStorageWithMemoryBackend()
	return newTargetTestManagerWithStorage(t, t.Context(), store), store
}

func newTargetTestManagerWithStorage(t *testing.T, ctx context.Context, store storage.Storage) *Manager {
	re := require.New(t)
	stores := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, Address: fmt.Sprintf("s%d", id), NodeState: metapb.NodeState_Serving},
			core.SetLastHeartbeatTS(time.Now())))
	}
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	re.NoError(err)
	m, err := NewManager(ctx, store, stores, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	return m
}

func loadStoredGroup(re *require.Assertions, store storage.Storage, id string) *Group {
	value, err := store.LoadAffinityGroup(id)
	re.NoError(err)
	group := &Group{}
	re.NoError(json.Unmarshal([]byte(value), group))
	return group
}

func targetFor(m *Manager, id string, leader uint64, voters ...uint64) GroupTarget {
	return GroupTarget{Expected: m.GetAffinityGroupState(id).Group, LeaderStoreID: leader, VoterStoreIDs: voters}
}

func TestUpdateAffinityGroupTargetsRejectsInvalidInput(t *testing.T) {
	re := require.New(t)
	m, store := newTargetTestManager(t)
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
	for _, targets := range [][]GroupTarget{
		nil,
		{targetFor(m, "g", 1, 1, 2), targetFor(m, "g", 2, 1, 2)},
		{targetFor(m, "g", 3, 1, 2)},
		{targetFor(m, "g", 1, 1, 1, 2)},
		{targetFor(m, "g", 0, 1, 2)},
		{targetFor(m, "g", 9, 9)},
		{targetFor(m, "h", 1, 1, 2), {Expected: Group{ID: "bad id"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}},
	} {
		_, err := m.UpdateAffinityGroupTargets(targets)
		re.Error(err)
	}
	for _, id := range []string{"g", "h"} {
		re.Zero(loadStoredGroup(re, store, id).LeaderStoreID, "validation must precede all writes")
		re.Zero(m.GetAffinityGroupState(id).LeaderStoreID)
	}
	m.applyStoreConditions(map[uint64]storeCondition{4: storeDown})
	stale := targetFor(m, "g", 1, 1, 2)
	stale.Expected.CreateTimestamp++
	for _, skipped := range []GroupTarget{
		{Expected: Group{ID: "missing"}, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}},
		stale, targetFor(m, "g", 1, 1, 4),
	} {
		written, err := m.UpdateAffinityGroupTargets([]GroupTarget{skipped, targetFor(m, "h", 2, 3, 2, 1)})
		re.NoError(err)
		re.Equal([]string{"h"}, written)
	}
	re.Zero(loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Equal([]uint64{1, 2, 3}, m.GetAffinityGroupState("h").VoterStoreIDs)
}

func TestSyncRecreatedAffinityGroup(t *testing.T) {
	for _, path := range []string{"watcher", "conflict", "written"} {
		t.Run(path, func(t *testing.T) {
			re := require.New(t)
			m, store := newTargetTestManager(t)
			old := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
			m.SyncGroupFromEtcdWithRevision(old, 1)
			re.NoError(m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: old.ID, KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}}, nil))
			m.RestoreAffinityGroup(old.ID)
			_, cached := m.GetAndCacheRegionAffinityGroupState(generateRegionForTest(10, old.VoterStoreIDs, nonOverlappingRange))
			re.True(cached)
			recreated := *old
			recreated.CreateTimestamp++
			re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, old.ID, &recreated) }))
			if path == "watcher" {
				m.SyncGroupFromEtcdWithRevision(&recreated, 2)
			} else {
				target := targetFor(m, old.ID, 2, 1, 2, 3)
				if path == "written" {
					target.Expected = recreated
					recreated.LeaderStoreID = 2
				}
				written, err := m.UpdateAffinityGroupTargets([]GroupTarget{target})
				re.NoError(err)
				re.Equal(path == "written", len(written) == 1)
			}
			state := m.GetAffinityGroupState(old.ID)
			re.Equal(recreated, state.Group)
			re.Zero(state.RegionCount)
			re.Equal(1, state.RangeCount)
		})
	}
}

func TestTargetAvailability(t *testing.T) {
	re := require.New(t)
	store := storage.NewStorageWithMemoryBackend()
	group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2, 3}}
	re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, group.ID, group) }))
	m := newTargetTestManagerWithStorage(t, t.Context(), store)
	re.False(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	m.checkGroupsAvailability()
	re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	for _, voters := range [][]uint64{{1, 2, 4}, {1, 2, 3}} {
		m.ExpireAffinityGroup("g")
		m.applyStoreConditions(map[uint64]storeCondition{3: storeRemoving})
		group.VoterStoreIDs = voters
		m.SyncGroupFromEtcd(group)
		state := m.GetAffinityGroupState("g")
		re.Equal(voters[2] != 3, state.AffinitySchedulingAllowed)
		re.Equal(voters[2] == 3, state.RegularSchedulingAllowed)
	}
	group.CreateTimestamp++
	group.VoterStoreIDs = []uint64{1, 2, 4}
	m.SyncGroupFromEtcd(group)
	re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
}

func TestFirstWinWaitsForStoreConditions(t *testing.T) {
	for _, name := range []string{"loaded", "empty"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				m, store := newTargetTestManager(t)
				loaded := name == "loaded"
				if loaded {
					group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
					re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, group.ID, group) }))
					m.SyncGroupFromEtcd(group)
				} else {
					re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
				}
				re.False(m.storeConditionsKnown)
				calls := 0
				m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
					calls++
					return m.UpdateAffinityGroupTargets(targets)
				}))
				m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{2, 1}, nonOverlappingRange), m.GetAffinityGroupState("g"))
				synctest.Wait()
				if loaded {
					re.Zero(calls, "loaded targets wait for the first store health check")
					re.Equal(uint64(1), loadStoredGroup(re, store, "g").LeaderStoreID)
				} else {
					re.Equal(1, calls, "groups without targets can initialize before the first health check")
					re.Equal(uint64(2), loadStoredGroup(re, store, "g").LeaderStoreID)
				}
				m.checkGroupsAvailability()
				re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
			})
		})
	}
}

func TestChangedTargetStartsDegradedWindow(t *testing.T) {
	for _, source := range []string{"watcher", "conflict"} {
		t.Run(source, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				m, store := newTargetTestManager(t)
				group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
				m.SyncGroupFromEtcdWithRevision(group, 1)
				m.applyStoreConditions(map[uint64]storeCondition{2: storeBusy})
				m.ExpireAffinityGroup("g")
				stale := targetFor(m, "g", 1, 1, 3)
				group.LeaderStoreID = 2
				re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, group.ID, group) }))
				if source == "watcher" {
					m.SyncGroupFromEtcdWithRevision(group, 2)
				} else {
					written, err := m.UpdateAffinityGroupTargets([]GroupTarget{stale})
					re.NoError(err)
					re.Empty(written)
				}
				state := m.GetAffinityGroupState("g")
				re.False(state.RegularSchedulingAllowed, "a new target protects its voters during a fresh degraded window")
				re.False(state.AffinitySchedulingAllowed)
				expiresAt := m.groups["g"].degradedExpiredAt
				re.Equal(newDegradedExpiredAtFromNow(), expiresAt)
				calls := 0
				m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
					calls++
					return m.UpdateAffinityGroupTargets(targets)
				}))
				region := generateRegionForTest(10, []uint64{1, 3}, nonOverlappingRange)
				m.ObserveAvailableRegion(region, state)
				synctest.Wait()
				re.Zero(calls)
				re.Equal(*group, *loadStoredGroup(re, store, "g"))
				time.Sleep(time.Second)
				m.SyncGroupFromEtcdWithRevision(group, 3)
				re.Equal(expiresAt, m.groups["g"].degradedExpiredAt, "duplicate targets do not extend the window")
				re.Equal(state, m.GetAffinityGroupState("g"))
				written, err := m.updateGroupTarget(targetFor(m, "g", 2, 1, 2))
				re.NoError(err)
				re.True(written)
				re.Equal(expiresAt, m.groups["g"].degradedExpiredAt, "confirming the stored target does not extend the window")
			})
		})
	}
}

func TestSyncDuplicateTargetPreservesAvailableGroup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
		m.SyncGroupFromEtcdWithRevision(group, 1)
		re.NoError(m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: group.ID, KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}}, nil))
		m.RestoreAffinityGroup(group.ID)
		_, isAffinity := m.GetAndCacheRegionAffinityGroupState(generateRegionForTest(10, group.VoterStoreIDs, nonOverlappingRange))
		re.True(isAffinity)
		before := m.GetAffinityGroupState(group.ID)
		re.False(before.HasUnpersistedTarget())
		re.Equal(PhaseStable, before.Phase)
		expiresAt := m.groups[group.ID].degradedExpiredAt

		time.Sleep(time.Second)
		m.SyncGroupFromEtcdWithRevision(group, 3)
		re.True(m.groups[group.ID].IsAvailable())
		re.Equal(expiresAt, m.groups[group.ID].degradedExpiredAt)
		re.Equal(before, m.GetAffinityGroupState(group.ID))

		stale := *group
		stale.LeaderStoreID = 2
		m.SyncGroupFromEtcdWithRevision(&stale, 2)
		re.Equal(before, m.GetAffinityGroupState(group.ID), "the duplicate PUT must advance the last seen revision")
	})
}

func TestSyncMatchingUnavailableLocalTarget(t *testing.T) {
	for _, availability := range []groupAvailability{groupExpired, groupDegraded} {
		t.Run(availability.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				pdManager, store := newTargetTestManager(t)
				m, _ := newTargetTestManager(t)
				re.NoError(pdManager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
				m.SyncGroupFromEtcd(loadStoredGroup(re, store, "g"))
				m.checkGroupsAvailability()

				calls := 0
				m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
					calls++
					if calls == 1 {
						return nil, ErrTargetWriterUnsupported
					}
					return pdManager.UpdateAffinityGroupTargets(targets)
				}))
				region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
				m.ObserveAvailableRegion(region, m.GetAffinityGroupState("g"))
				synctest.Wait()
				local := m.GetAffinityGroupState("g")
				re.True(local.HasUnpersistedTarget())
				re.True(local.AffinitySchedulingAllowed)

				switch availability {
				case groupExpired:
					m.applyStoreConditions(map[uint64]storeCondition{2: storeDown})
					m.applyStoreConditions(nil)
				case groupDegraded:
					// The scheduling service can still see the store as disconnected when PD writes the target.
					m.applyStoreConditions(map[uint64]storeCondition{2: storeDisconnected})
				}
				re.Equal(availability, m.groups["g"].GetAvailability())
				time.Sleep(time.Second) // Clear the 404 cooldown and distinguish the new deadline.

				_, err := pdManager.UpdateAffinityGroupPeers("g", local.LeaderStoreID, local.VoterStoreIDs)
				re.NoError(err)
				admin := loadStoredGroup(re, store, "g")
				m.SyncGroupFromEtcd(admin)
				synced := m.GetAffinityGroupState("g")
				re.Equal(*admin, synced.Group)
				re.False(synced.HasUnpersistedTarget())

				replacement := generateRegionForTest(20, []uint64{1, 3}, nonOverlappingRange)
				m.ObserveAvailableRegion(replacement, synced)
				synctest.Wait()
				re.Equal(*admin, *loadStoredGroup(re, store, "g"), "patrol must preserve the admin target")
				re.Equal(1, calls)
				re.False(synced.RegularSchedulingAllowed)
				re.Equal(availability == groupExpired, synced.AffinitySchedulingAllowed)
				expiresAt := m.groups["g"].degradedExpiredAt
				re.Equal(newDegradedExpiredAtFromNow(), expiresAt, "a newly persisted target gets a fresh window")

				time.Sleep(time.Second)
				m.SyncGroupFromEtcd(admin)
				re.Equal(expiresAt, m.groups["g"].degradedExpiredAt, "duplicate syncs do not extend the window")
			})
		})
	}
}

func TestTargetWriteResetsVersionOnce(t *testing.T) {
	re := require.New(t)
	m, _ := newTargetTestManager(t)
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	m.applyStoreConditions(map[uint64]storeCondition{})
	target := targetFor(m, "g", 1, 1, 2)
	before := m.GetAffinityGroupState("g").affinityVer
	written, err := m.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.NoError(err)
	re.Equal([]string{"g"}, written)
	re.Equal(before+1, m.GetAffinityGroupState("g").affinityVer)
	m.DegradeAffinityGroup("g")
	before = m.GetAffinityGroupState("g").affinityVer
	written, err = m.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.NoError(err)
	re.Equal([]string{"g"}, written)
	re.Equal(before+1, m.GetAffinityGroupState("g").affinityVer, "an already stored result still confirms the target")
	re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
}

func TestTargetRetryWaitsForStoreConditions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		re.False(m.storeConditionsKnown)
		target := targetFor(m, "g", 1, 1, 2)

		written, err := m.UpdateAffinityGroupTargets([]GroupTarget{target})
		re.NoError(err)
		re.Equal([]string{"g"}, written)
		re.Equal(groupDegraded, m.groups["g"].GetAvailability())
		state := m.GetAffinityGroupState("g")
		re.Equal(*target.result(), state.Group)

		written, err = m.UpdateAffinityGroupTargets([]GroupTarget{target})
		re.NoError(err)
		re.Equal([]string{"g"}, written)
		re.Equal(groupDegraded, m.groups["g"].GetAvailability())
		re.Equal(state.affinityVer, m.GetAffinityGroupState("g").affinityVer)

		m.checkGroupsAvailability()
		re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	})
}

func TestStoredGroupFields(t *testing.T) {
	base := Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
	reordered := base
	reordered.VoterStoreIDs = []uint64{2, 1}
	require.True(t, sameStoredGroup(&base, &reordered))
	for name, change := range map[string]func(*Group){
		"ID":              func(g *Group) { g.ID = "h" },
		"CreateTimestamp": func(g *Group) { g.CreateTimestamp++ },
		"LeaderStoreID":   func(g *Group) { g.LeaderStoreID = 2 },
		"VoterStoreIDs":   func(g *Group) { g.VoterStoreIDs = []uint64{1, 3} },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			change(&changed)
			re := require.New(t)
			re.False(sameStoredGroup(&base, &changed))
		})
	}
}

type targetWriterFunc func(context.Context, []GroupTarget) ([]string, error)

func (f targetWriterFunc) WriteGroupTargets(ctx context.Context, targets []GroupTarget) ([]string, error) {
	return f(ctx, targets)
}

func TestFirstWinSingleFlightAndGlobalBackoff(t *testing.T) {
	for _, outcome := range []string{"failed", "not written", "not ready"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				m, store := newTargetTestManager(t)
				re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
				calls, failures := 0, testutil.ToFloat64(targetWriteFailureCounter)
				release := make(chan struct{})
				defer close(release)
				var sent GroupTarget
				m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
					calls++
					sent = targets[0]
					if calls == 1 {
						<-release
						if outcome == "not written" {
							return []string{"another-group"}, nil
						}
						if outcome == "not ready" {
							return nil, errors.Annotate(ErrTargetWriterNotReady, "PD leader is not found")
						}
						return nil, errors.New("PD unavailable")
					}
					return []string{sent.Expected.ID}, nil
				}))
				region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
				observe := func(id string) {
					m.ObserveAvailableRegion(region, m.GetAffinityGroupState(id))
					synctest.Wait()
				}
				observe("g")
				for range 10 {
					observe("g")
					observe("h")
				}
				re.Equal(1, calls, "busy observations are discarded")
				release <- struct{}{}
				synctest.Wait()
				observe("h")
				re.Equal(1, calls, "failure delays all groups")
				if outcome == "not ready" {
					re.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))
				} else {
					re.Equal(failures+1, testutil.ToFloat64(targetWriteFailureCounter))
				}
				time.Sleep(time.Second)
				observe("h")
				re.Equal(2, calls)
				re.Equal(GroupTarget{Expected: m.GetAffinityGroupState("h").Group, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}, sent)
				re.Zero(m.GetAffinityGroupState("h").LeaderStoreID, "HTTP success waits for the watcher")
				re.Zero(loadStoredGroup(re, store, "h").LeaderStoreID)
				// A store can become unavailable before the watcher publishes the target.
				m.applyStoreConditions(map[uint64]storeCondition{2: storeRemoving})
				m.SyncGroupFromEtcd(sent.result())
				re.False(m.GetAffinityGroupState("h").AffinitySchedulingAllowed)
				re.True(m.GetAffinityGroupState("h").RegularSchedulingAllowed)
				m.applyStoreConditions(map[uint64]storeCondition{})
				re.Zero(m.GetAffinityGroupState("g").LeaderStoreID, "discarded observations are never replayed")
				observe("g")
				re.Equal(3, calls, "success has no cooldown")
				m.SyncGroupFromEtcd(sent.result())
				m.DegradeAffinityGroup("g")
				observe("g")
				re.Equal(3, calls, "a matching Region confirms the target without writing")
				re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
			})
		})
	}
}

func TestFirstWinCancelsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		m := newTargetTestManagerWithStorage(t, ctx, storage.NewStorageWithMemoryBackend())
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		calls := 0
		stopped := false
		m.SetTargetWriter(targetWriterFunc(func(ctx context.Context, _ []GroupTarget) ([]string, error) {
			calls++
			<-ctx.Done()
			stopped = true
			return nil, ctx.Err()
		}))
		region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
		m.ObserveAvailableRegion(region, m.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(1, calls)
		cancel()
		synctest.Wait()
		re.True(stopped)
		m.ObserveAvailableRegion(region, m.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(1, calls)
	})
}

func TestFirstWinReplyKeepsNewerWatcherTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		newer := m.GetAffinityGroupState("g").Group
		newer.LeaderStoreID, newer.VoterStoreIDs = 2, []uint64{1, 2}
		m.SetTargetWriter(targetWriterFunc(func(_ context.Context, _ []GroupTarget) ([]string, error) {
			m.SyncGroupFromEtcd(&newer)
			return []string{"g"}, nil
		}))
		m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange), m.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(newer, m.GetAffinityGroupState("g").Group)
	})
}

func TestFirstWinLateUnsupportedReplyKeepsGroupExpired(t *testing.T) {
	for _, attempt := range []string{"initialization", "persistence retry"} {
		t.Run(attempt, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				m, _ := newTargetTestManager(t)
				re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
				release := make(chan struct{})
				defer close(release)
				blockOn := 1
				if attempt == "persistence retry" {
					blockOn = 2
				}
				calls := 0
				m.SetTargetWriter(targetWriterFunc(func(_ context.Context, _ []GroupTarget) ([]string, error) {
					calls++
					if calls == blockOn {
						<-release
					}
					return nil, ErrTargetWriterUnsupported
				}))
				oldRegion := generateRegionForTest(10, []uint64{1, 2, 3}, nonOverlappingRange)
				if blockOn == 2 {
					m.ObserveAvailableRegion(oldRegion, m.GetAffinityGroupState("g"))
					synctest.Wait()
					state := m.GetAffinityGroupState("g")
					re.True(state.AffinitySchedulingAllowed)
					re.True(state.HasUnpersistedTarget())
					time.Sleep(time.Second)
				}

				m.ObserveAvailableRegion(oldRegion, m.GetAffinityGroupState("g"))
				synctest.Wait()
				re.Equal(blockOn, calls)
				m.ExpireAffinityGroup("g")
				expired := m.GetAffinityGroupState("g")
				release <- struct{}{}
				synctest.Wait()
				state := m.GetAffinityGroupState("g")
				re.True(state.RegularSchedulingAllowed, "a late 404 must leave the group expired")
				re.False(state.AffinitySchedulingAllowed)
				re.Equal(expired.Group, state.Group)
				re.Equal(expired.affinityVer, state.affinityVer)

				replacement := generateRegionForTest(20, []uint64{2, 1}, nonOverlappingRange)
				m.ObserveAvailableRegion(replacement, state)
				synctest.Wait()
				state = m.GetAffinityGroupState("g")
				re.True(state.AffinitySchedulingAllowed)
				re.True(state.HasUnpersistedTarget())
				re.Equal([]uint64{1, 2}, state.VoterStoreIDs)
				re.Equal(blockOn, calls, "the fresh observation replaces the target during the 404 cooldown")
			})
		})
	}
}

func TestFirstWinLateUnsupportedReplyRejectsUnavailableStore(t *testing.T) {
	for _, condition := range []storeCondition{storeDisconnected, storeLowSpace, storeLeaderEvicted, storeDown} {
		t.Run(condition.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				m, _ := newTargetTestManager(t)
				re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
				m.checkGroupsAvailability()
				observed := m.GetAffinityGroupState("g")

				calls := 0
				release := make(chan struct{})
				defer close(release)
				m.SetTargetWriter(targetWriterFunc(func(_ context.Context, _ []GroupTarget) ([]string, error) {
					calls++
					<-release
					return nil, ErrTargetWriterUnsupported
				}))
				region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
				m.ObserveAvailableRegion(region, observed)
				synctest.Wait()
				re.Equal(1, calls)

				m.applyStoreConditions(map[uint64]storeCondition{1: condition})
				re.Equal(observed.affinityVer, m.GetAffinityGroupState("g").affinityVer, "unset groups have no voters")
				release <- struct{}{}
				synctest.Wait()

				state := m.GetAffinityGroupState("g")
				re.Equal(observed.Group, state.Group, "the late 404 must leave the target unset")
				re.Equal(observed.affinityVer, state.affinityVer)
				re.False(state.HasUnpersistedTarget())
				re.True(state.RegularSchedulingAllowed)
			})
		})
	}
}

func TestFirstWinLateUnsupportedReplyAfterRecreation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		created := Group{ID: "g", CreateTimestamp: uint64(time.Now().Unix())}
		m.SyncGroupFromEtcd(&created)
		observed := m.GetAffinityGroupState(created.ID)

		started := make(chan struct{}, 1)
		release := make(chan struct{}, 1)
		defer close(release)
		m.SetTargetWriter(targetWriterFunc(func(context.Context, []GroupTarget) ([]string, error) {
			started <- struct{}{}
			<-release
			return nil, ErrTargetWriterUnsupported
		}))
		region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
		m.ObserveAvailableRegion(region, observed)
		<-started

		m.SyncGroupDeleteFromEtcd(created.ID)
		m.SyncGroupFromEtcd(&created)
		recreated := m.GetAffinityGroupState(created.ID)
		re.NotSame(observed.groupInfoPtr, recreated.groupInfoPtr)
		re.Equal(observed.CreateTimestamp, recreated.CreateTimestamp)
		re.Equal(observed.affinityVer, recreated.affinityVer)

		release <- struct{}{}
		synctest.Wait()
		after := m.GetAffinityGroupState(created.ID)
		re.Equal(recreated.Group, after.Group, "the late 404 must leave the recreated group unset")
		re.False(after.HasUnpersistedTarget())

		m.ObserveAvailableRegion(region, observed)
		re.Equal(recreated.Group, m.GetAffinityGroupState(created.ID).Group)
		m.ObserveAvailableRegion(generateRegionForTest(20, []uint64{2, 3}, nonOverlappingRange), recreated)
		after = m.GetAffinityGroupState(created.ID)
		re.Equal(uint64(2), after.LeaderStoreID)
		re.True(after.HasUnpersistedTarget())
	})
}

func TestFirstWinUnsupportedTargetWriter(t *testing.T) {
	t.Run("self heal after pd upgrade", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			re := require.New(t)
			pdManager, store := newTargetTestManager(t)
			m, _ := newTargetTestManager(t)
			re.NoError(pdManager.CreateAffinityGroups([]GroupKeyRanges{
				{GroupID: "g", KeyRanges: []keyutil.KeyRange{nonOverlappingRange}},
				{GroupID: "h"},
			}))
			createdG := loadStoredGroup(re, store, "g")
			createdH := loadStoredGroup(re, store, "h")
			m.SyncGroupFromEtcd(createdG)
			m.SyncGroupFromEtcd(createdH)
			re.NoError(m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}}, nil))

			calls, failures := 0, testutil.ToFloat64(targetWriteFailureCounter)
			supported := false
			var lastSent GroupTarget
			m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
				calls++
				lastSent = targets[0]
				if !supported {
					return nil, errors.Annotate(ErrTargetWriterUnsupported, "404 Not Found")
				}
				return pdManager.UpdateAffinityGroupTargets(targets)
			}))

			regionG := generateRegionForTest(10, []uint64{2, 1}, nonOverlappingRange)
			regionH := generateRegionForTest(20, []uint64{1, 2}, nonOverlappingRange)
			regionOther := generateRegionForTest(30, []uint64{3, 1}, nonOverlappingRange)
			observe := func(id string, region *core.RegionInfo) {
				m.ObserveAvailableRegion(region, m.GetAffinityGroupState(id))
				synctest.Wait()
			}

			// Old PD returns 404: group initializes locally and activates immediately.
			observe("g", regionG)
			re.Equal(1, calls)
			stateG := m.GetAffinityGroupState("g")
			re.Equal(uint64(2), stateG.LeaderStoreID)
			re.Equal([]uint64{1, 2}, stateG.VoterStoreIDs)
			re.True(stateG.AffinitySchedulingAllowed)
			re.False(stateG.RegularSchedulingAllowed)
			re.True(stateG.HasUnpersistedTarget())
			re.Zero(loadStoredGroup(re, store, "g").LeaderStoreID)
			re.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))

			// The HTTP cooldown still lets a second group initialize locally.
			observe("h", regionH)
			re.Equal(1, calls)
			stateH := m.GetAffinityGroupState("h")
			re.Equal(uint64(1), stateH.LeaderStoreID)
			re.True(stateH.AffinitySchedulingAllowed)
			re.True(stateH.HasUnpersistedTarget())
			re.Zero(loadStoredGroup(re, store, "h").LeaderStoreID)

			// Cache regionG so g reaches PhaseStable; active local target ignores conflicting observations.
			_, isAffinity := m.GetAndCacheRegionAffinityGroupState(regionG)
			re.True(isAffinity)
			stateG = m.GetAffinityGroupState("g")
			re.Equal(PhaseStable, stateG.Phase)
			verBefore := stateG.affinityVer
			observe("g", regionOther)
			re.Equal(1, calls)
			re.Equal(uint64(2), m.GetAffinityGroupState("g").LeaderStoreID)

			// Retrying while PD is still old keeps AffinityVer and PhaseStable intact.
			time.Sleep(time.Second)
			observe("g", regionG)
			re.Equal(2, calls)
			re.Equal(GroupTarget{Expected: *createdG, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}}, lastSent)
			re.Equal(verBefore, m.GetAffinityGroupState("g").affinityVer)
			re.Equal(PhaseStable, m.GetAffinityGroupState("g").Phase)
			// A watcher reload of the still-empty stored group does not drop the local target.
			m.SyncGroupFromEtcd(createdG)
			re.Equal(verBefore, m.GetAffinityGroupState("g").affinityVer)
			re.True(m.GetAffinityGroupState("g").HasUnpersistedTarget())

			// A leader-only condition permits local failover with the same voters.
			m.applyStoreConditions(map[uint64]storeCondition{1: storeLeaderEvicted})
			observe("h", regionOther)
			re.Equal(2, calls, "degraded local target preserves voter stability")
			re.Equal([]uint64{1, 2}, m.GetAffinityGroupState("h").VoterStoreIDs)
			observe("h", regionG)
			stateH = m.GetAffinityGroupState("h")
			re.Equal(uint64(2), stateH.LeaderStoreID)
			re.True(stateH.AffinitySchedulingAllowed)
			re.True(stateH.HasUnpersistedTarget())
			re.Equal(2, calls, "leader failover uses the local path during the cooldown")

			// After expiration, a healthy observation can also replace the voters locally.
			m.ExpireAffinityGroup("h")
			observe("h", regionOther)
			re.Equal(2, calls)
			re.Equal(uint64(3), m.GetAffinityGroupState("h").LeaderStoreID)
			re.Equal([]uint64{1, 3}, m.GetAffinityGroupState("h").VoterStoreIDs)
			re.True(m.GetAffinityGroupState("h").AffinitySchedulingAllowed)
			re.True(m.GetAffinityGroupState("h").HasUnpersistedTarget())

			// PD upgrades and accepts /targets; next matching observation persists without resetting AffinityVer.
			supported = true
			time.Sleep(time.Second)
			observe("g", regionG)
			re.Equal(3, calls)
			re.Equal(GroupTarget{Expected: *createdG, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}}, lastSent)
			storedG := loadStoredGroup(re, store, "g")
			re.Equal(uint64(2), storedG.LeaderStoreID)
			re.Equal([]uint64{1, 2}, storedG.VoterStoreIDs)
			m.SyncGroupFromEtcd(storedG)
			stateG = m.GetAffinityGroupState("g")
			re.False(stateG.HasUnpersistedTarget())
			re.Equal(verBefore, stateG.affinityVer)
			re.Equal(PhaseStable, stateG.Phase)

			observe("h", regionOther)
			re.Equal(4, calls, "the successful probe lets another group persist without waiting")
			re.Equal(GroupTarget{Expected: *createdH, LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 3}}, lastSent)
			storedH := loadStoredGroup(re, store, "h")
			re.Equal(uint64(3), storedH.LeaderStoreID)
			m.SyncGroupFromEtcd(storedH)
			re.False(m.GetAffinityGroupState("h").HasUnpersistedTarget())

			// Once synced, matching observations no longer send conditional target writes.
			observe("g", regionG)
			observe("h", regionOther)
			re.Equal(4, calls)
			re.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))
		})
	})

	t.Run("admin target wins", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			re := require.New(t)
			pdManager, store := newTargetTestManager(t)
			m, _ := newTargetTestManager(t)
			m.checkGroupsAvailability()
			re.NoError(pdManager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}, {GroupID: "k"}}))
			for _, id := range []string{"g", "h", "k"} {
				m.SyncGroupFromEtcd(loadStoredGroup(re, store, id))
			}

			supported := false
			syncInFlightAdmin := false
			m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
				if syncInFlightAdmin {
					_, err := pdManager.UpdateAffinityGroupPeers("k", 4, []uint64{1, 4})
					re.NoError(err)
					m.SyncGroupFromEtcd(loadStoredGroup(re, store, "k"))
				}
				if !supported {
					return nil, errors.Annotate(ErrTargetWriterUnsupported, "404 Not Found")
				}
				return pdManager.UpdateAffinityGroupTargets(targets)
			}))

			region := generateRegionForTest(10, []uint64{2, 1}, nonOverlappingRange)
			observe := func(id string) {
				m.ObserveAvailableRegion(region, m.GetAffinityGroupState(id))
				synctest.Wait()
			}

			observe("g")
			time.Sleep(time.Second)
			observe("h")
			re.True(m.GetAffinityGroupState("g").HasUnpersistedTarget())
			re.True(m.GetAffinityGroupState("h").HasUnpersistedTarget())

			// Case 1: Admin sets target on PD and watcher delivers it while targetUnpersisted is true.
			_, err := pdManager.UpdateAffinityGroupPeers("g", 3, []uint64{1, 3})
			re.NoError(err)
			adminG := loadStoredGroup(re, store, "g")
			m.SyncGroupFromEtcd(adminG)
			stateG := m.GetAffinityGroupState("g")
			re.Equal(*adminG, stateG.Group)
			re.False(stateG.HasUnpersistedTarget())
			re.True(stateG.AffinitySchedulingAllowed)

			// Case 2: Admin target arrives while a 404 request is still in flight.
			time.Sleep(time.Second)
			syncInFlightAdmin = true
			observe("k")
			syncInFlightAdmin = false
			stateK := m.GetAffinityGroupState("k")
			re.Equal(uint64(4), stateK.LeaderStoreID)
			re.Equal([]uint64{1, 4}, stateK.VoterStoreIDs)
			re.False(stateK.HasUnpersistedTarget())

			// Case 3: Admin writes on upgraded PD before watcher delivers to scheduling; PD CAS rejects Expected={0, nil}.
			supported = true
			_, err = pdManager.UpdateAffinityGroupPeers("h", 4, []uint64{2, 4})
			re.NoError(err)
			time.Sleep(time.Second)
			observe("h")
			adminH := loadStoredGroup(re, store, "h")
			re.Equal(uint64(4), adminH.LeaderStoreID)
			re.Equal([]uint64{2, 4}, adminH.VoterStoreIDs)
			m.SyncGroupFromEtcd(adminH)
			stateH := m.GetAffinityGroupState("h")
			re.Equal(*adminH, stateH.Group)
			re.False(stateH.HasUnpersistedTarget())
		})
	})
}

func TestFirstWinUnsupportedTargetWriterReplacesStoredTarget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		condition storeCondition
		voters    []uint64
	}{
		{name: "evicted leader", condition: storeLeaderEvicted, voters: []uint64{1, 2}},
		{name: "expired voters", condition: storeDown, voters: []uint64{1, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				pdManager, store := newTargetTestManager(t)
				m, _ := newTargetTestManager(t)
				re.NoError(pdManager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
				_, err := pdManager.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2})
				re.NoError(err)
				stored := loadStoredGroup(re, store, "g")
				m.SyncGroupFromEtcd(stored)
				m.applyStoreConditions(map[uint64]storeCondition{2: tc.condition})

				var sent []GroupTarget
				supported := false
				m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
					sent = append(sent, targets...)
					if !supported {
						return nil, ErrTargetWriterUnsupported
					}
					return pdManager.UpdateAffinityGroupTargets(targets)
				}))

				region := generateRegionForTest(10, tc.voters, nonOverlappingRange)
				before := m.GetAffinityGroupState("g")
				re.False(before.AffinitySchedulingAllowed)
				re.Equal(tc.condition == storeDown, before.RegularSchedulingAllowed)
				m.ObserveAvailableRegion(region, before)
				synctest.Wait()

				target := GroupTarget{Expected: *stored, LeaderStoreID: 1, VoterStoreIDs: tc.voters}
				re.Equal([]GroupTarget{target}, sent)
				local := m.GetAffinityGroupState("g")
				re.True(local.AffinitySchedulingAllowed)
				re.True(local.HasUnpersistedTarget())
				re.Equal(*target.result(), local.Group)
				re.Equal(stored, loadStoredGroup(re, store, "g"))

				m.SyncGroupFromEtcd(stored)
				re.Equal(local, m.GetAffinityGroupState("g"), "an unchanged etcd target must preserve the local replacement")

				supported = true
				time.Sleep(time.Second)
				m.ObserveAvailableRegion(region, m.GetAffinityGroupState("g"))
				synctest.Wait()
				re.Equal([]GroupTarget{target, target}, sent, "the retry must compare against the old persisted target")
				persisted := loadStoredGroup(re, store, "g")
				re.Equal(local.Group, *persisted)
				m.SyncGroupFromEtcd(persisted)
				confirmed := m.GetAffinityGroupState("g")
				re.False(confirmed.HasUnpersistedTarget())
				re.True(confirmed.AffinitySchedulingAllowed)
				re.Equal(local.affinityVer, confirmed.affinityVer, "confirming the active target preserves its version")
			})
		})
	}
}

func TestFirstWinStoredTargetAfterLocalReplacement(t *testing.T) {
	for _, source := range []string{"admin update", "admin reassertion", "recreation", "matching observation"} {
		t.Run(source, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				m, _ := newTargetTestManager(t)
				stored := Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}}
				m.SyncGroupFromEtcdWithRevision(&stored, 1)
				re.NoError(m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: stored.ID, KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}}, nil))
				m.applyStoreConditions(map[uint64]storeCondition{2: storeLeaderEvicted})
				calls := 0
				m.SetTargetWriter(targetWriterFunc(func(context.Context, []GroupTarget) ([]string, error) {
					calls++
					return nil, ErrTargetWriterUnsupported
				}))

				region := generateRegionForTest(10, []uint64{1, 2}, nonOverlappingRange)
				m.ObserveAvailableRegion(region, m.GetAffinityGroupState("g"))
				synctest.Wait()
				local := m.GetAffinityGroupState("g")
				re.True(local.AffinitySchedulingAllowed)
				re.True(local.HasUnpersistedTarget())
				re.Equal(1, calls)

				want := stored
				switch source {
				case "admin update":
					want.LeaderStoreID = 3
					want.VoterStoreIDs = []uint64{1, 3}
					m.SyncGroupFromEtcdWithRevision(&want, 2)
				case "admin reassertion":
					_, isAffinity := m.GetAndCacheRegionAffinityGroupState(region)
					re.True(isAffinity)
					cached := m.GetAffinityGroupState(stored.ID)
					re.Equal(1, cached.AffinityRegionCount)
					m.SyncGroupFromEtcdWithRevision(&stored, 1)
					re.Equal(cached, m.GetAffinityGroupState(stored.ID), "reloading the same revision must keep the local replacement")
					m.applyStoreConditions(nil)
					m.SyncGroupFromEtcdWithRevision(&want, 2)
					_, isAffinity = m.GetRegionAffinityGroupState(region)
					re.False(isAffinity, "the cache must use the reasserted peers")
					admin := m.GetAffinityGroupState(stored.ID)
					m.SyncGroupFromEtcdWithRevision(&stored, 1)
					re.Equal(admin, m.GetAffinityGroupState(stored.ID), "an older revision cannot change the state")
				case "recreation":
					m.applyStoreConditions(nil)
					want.CreateTimestamp++
					m.SyncGroupFromEtcdWithRevision(&want, 2)
				case "matching observation":
					m.applyStoreConditions(map[uint64]storeCondition{1: storeLeaderEvicted})
					m.ObserveAvailableRegion(generateRegionForTest(20, []uint64{2, 1}, nonOverlappingRange), m.GetAffinityGroupState("g"))
					synctest.Wait()
					re.Equal(1, calls, "the persisted target can be restored without a write")
				}
				state := m.GetAffinityGroupState("g")
				re.Equal(want, state.Group)
				re.False(state.HasUnpersistedTarget())
				re.True(state.AffinitySchedulingAllowed)
				if source == "recreation" {
					re.NotSame(local.groupInfoPtr, state.groupInfoPtr)
				}
				if source == "admin reassertion" {
					re.Greater(state.affinityVer, local.affinityVer)
					re.Zero(state.AffinityRegionCount)
					time.Sleep(time.Second)
					m.ObserveAvailableRegion(region, local)
					m.ObserveAvailableRegion(region, state)
					synctest.Wait()
					re.Equal(1, calls, "stale and fresh patrol observations must not retry the replaced target")
				}
			})
		})
	}
}

func TestFirstWinLateUnsupportedReplyAfterSamePeersWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		stored := Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}}
		m.SyncGroupFromEtcdWithRevision(&stored, 1)
		m.applyStoreConditions(map[uint64]storeCondition{2: storeDown})
		release := make(chan struct{})
		defer close(release)
		m.SetTargetWriter(targetWriterFunc(func(context.Context, []GroupTarget) ([]string, error) {
			<-release
			return nil, ErrTargetWriterUnsupported
		}))
		before := m.GetAffinityGroupState(stored.ID)
		m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{1, 3}, nonOverlappingRange), before)
		synctest.Wait()
		re.True(m.targetWriteInFlight)

		m.applyStoreConditions(nil)
		m.SyncGroupFromEtcdWithRevision(&stored, 2)
		release <- struct{}{}
		synctest.Wait()
		after := m.GetAffinityGroupState(stored.ID)
		re.Equal(stored, after.Group)
		re.True(after.AffinitySchedulingAllowed)
		re.False(after.HasUnpersistedTarget())
		re.Greater(after.affinityVer, before.affinityVer)
	})
}

func TestFirstWinLateUnsupportedReplyAfterSamePeersWriteWithStoreDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		stored := Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}}
		m.SyncGroupFromEtcdWithRevision(&stored, 1)
		m.applyStoreConditions(map[uint64]storeCondition{2: storeDown})
		m.SetTargetWriter(targetWriterFunc(func(context.Context, []GroupTarget) ([]string, error) {
			// The scheduling service can still see a store as down after PD accepts the PUT.
			m.SyncGroupFromEtcdWithRevision(&stored, 2)
			return nil, ErrTargetWriterUnsupported
		}))

		before := m.GetAffinityGroupState(stored.ID)
		m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{1, 3}, nonOverlappingRange), before)
		synctest.Wait()
		re.Equal(int64(2), m.groupModRevisions[stored.ID])
		re.Equal(before, m.GetAffinityGroupState(stored.ID))
	})
}

type targetReplyStorage struct {
	endpoint.AffinityStorage
	beforeCommit func()
	loseReply    bool
}

func (s *targetReplyStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	err := s.AffinityStorage.RunInTxn(ctx, func(txn kv.Txn) error {
		if err := f(txn); err != nil {
			return err
		}
		if s.beforeCommit != nil {
			s.beforeCommit()
		}
		return nil
	})
	if err == nil && s.loseReply {
		return errors.New("committed reply lost")
	}
	return err
}

func TestTargetCASAndLostReplyWithEtcd(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	store := storage.NewStorageWithEtcdBackend(client)
	m := newTargetTestManagerWithStorage(t, t.Context(), store)
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
	other := newTargetTestManagerWithStorage(t, t.Context(), store)
	stale := targetFor(m, "g", 1, 1, 2, 3)
	commits := 0
	wrapped := &targetReplyStorage{AffinityStorage: store, beforeCommit: func() {
		commits++
		if commits == 2 {
			_, err := other.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2, 3})
			re.NoError(err)
		}
	}}
	m.storage = wrapped
	written, err := m.UpdateAffinityGroupTargets([]GroupTarget{targetFor(m, "h", 1, 1, 2, 3), stale})
	re.Error(err, "another Manager wrote after the read, before commit")
	re.Equal([]string{"h"}, written)
	re.Equal(uint64(1), loadStoredGroup(re, store, "h").LeaderStoreID, "earlier commits survive a later error")
	re.Equal(uint64(2), loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Zero(m.GetAffinityGroupState("g").LeaderStoreID)
	wrapped.beforeCommit = nil
	m.applyStoreConditions(map[uint64]storeCondition{2: storeLeaderEvicted})
	written, err = m.UpdateAffinityGroupTargets([]GroupTarget{stale})
	re.NoError(err)
	re.Empty(written)
	re.Equal(uint64(2), m.GetAffinityGroupState("g").LeaderStoreID)
	re.False(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	m.applyStoreConditions(map[uint64]storeCondition{})

	target := targetFor(m, "g", 3, 1, 2, 3)
	wrapped.loseReply = true
	_, err = m.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.ErrorContains(err, "committed reply lost")
	re.Equal(uint64(2), m.GetAffinityGroupState("g").LeaderStoreID)
	re.Equal(uint64(3), loadStoredGroup(re, store, "g").LeaderStoreID)
	wrapped.loseReply = false
	before, err := client.Get(t.Context(), keypath.AffinityGroupPath("g"))
	re.NoError(err)
	target.Expected.VoterStoreIDs = []uint64{3, 2, 1}
	written, err = m.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.NoError(err)
	re.Equal([]string{"g"}, written)
	re.Equal(*loadStoredGroup(re, store, "g"), m.GetAffinityGroupState("g").Group)
	after, err := client.Get(t.Context(), keypath.AffinityGroupPath("g"))
	re.NoError(err)
	re.Equal(before.Kvs[0].ModRevision, after.Kvs[0].ModRevision, "idempotent retries do not put again")
}
