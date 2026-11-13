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
			m.SyncGroupFromEtcd(old)
			re.NoError(m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: old.ID, KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}}, nil))
			m.RestoreAffinityGroup(old.ID)
			_, cached := m.GetAndCacheRegionAffinityGroupState(generateRegionForTest(10, old.VoterStoreIDs, nonOverlappingRange))
			re.True(cached)
			recreated := *old
			recreated.CreateTimestamp++
			re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, old.ID, &recreated) }))
			if path == "watcher" {
				m.SyncGroupFromEtcd(&recreated)
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
		re.Equal(failures+1, testutil.ToFloat64(targetWriteFailureCounter))
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
