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
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
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
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	re.NoError(err)
	m, err := NewManager(ctx, store, stores, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	// MCS may load groups before its store informer has received any stores.
	for id := uint64(1); id <= 4; id++ {
		stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: id, Address: fmt.Sprintf("s%d", id), NodeState: metapb.NodeState_Serving},
			core.SetLastHeartbeatTS(time.Now())))
	}
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
		{targetFor(m, "g", 1, 0, 1)},
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
		stale, targetFor(m, "g", 1, 1, 4), targetFor(m, "g", 9, 9),
	} {
		written, err := m.UpdateAffinityGroupTargets([]GroupTarget{skipped, targetFor(m, "h", 2, 3, 2, 1)})
		re.NoError(err)
		re.Equal([]string{"h"}, written)
	}
	re.Zero(loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Equal([]uint64{1, 2, 3}, m.GetAffinityGroupState("h").VoterStoreIDs)
}

func TestSyncRecreatedAffinityGroup(t *testing.T) {
	for _, path := range []string{"watcher", "conflict", "written", "policy", "peers"} {
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
			switch path {
			case "watcher":
				m.SyncGroupFromEtcd(&recreated)
			case "conflict", "written":
				target := targetFor(m, old.ID, 2, 1, 2, 3)
				if path == "written" {
					target.Expected = recreated
					recreated.LeaderStoreID = 2
				}
				written, err := m.UpdateAffinityGroupTargets([]GroupTarget{target})
				re.NoError(err)
				re.Equal(path == "written", len(written) == 1)
			case "policy":
				_, err := m.UpdateAffinityGroupPolicy(old.ID, BalancePolicyFixed)
				re.NoError(err)
				recreated.BalancePolicy = BalancePolicyFixed
			case "peers":
				_, err := m.UpdateAffinityGroupPeers(old.ID, 1, []uint64{1, 2, 3})
				re.NoError(err)
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
	m.CheckGroupsAvailability()
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

func TestInitialTargetAvailabilityAndRemovedStores(t *testing.T) {
	re := require.New(t)
	store := storage.NewStorageWithMemoryBackend()
	group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
	re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, group.ID, group) }))
	seed := newTargetTestManagerWithStorage(t, t.Context(), store)
	m, err := NewManager(t.Context(), store, seed.storeSetInformer, seed.conf, seed.regionLabeler)
	re.NoError(err)
	re.True(m.storeConditionsKnown)
	re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
	stores := seed.storeSetInformer.(*core.StoresInfo)
	stores.PutStore(stores.GetStore(2).Clone(core.SetStoreState(metapb.StoreState_Tombstone)))
	reloaded, err := NewManager(t.Context(), store, stores, seed.conf, seed.regionLabeler)
	re.NoError(err)
	re.True(reloaded.GetAffinityGroupState("g").RegularSchedulingAllowed)
	re.ErrorIs(m.AdjustGroup(group), errs.ErrAffinityGroupContent)
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "healthy"}}))
	written, err := m.UpdateAffinityGroupTargets([]GroupTarget{targetFor(m, "g", 2, 1, 2), targetFor(m, "healthy", 3, 1, 3)})
	re.NoError(err)
	re.Equal([]string{"healthy"}, written)
	re.Equal(uint64(1), loadStoredGroup(re, store, "g").LeaderStoreID)
	_, err = m.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2})
	re.ErrorIs(err, errs.ErrAffinityGroupContent)
	_, err = seed.UpdateAffinityGroupPeers("g", 1, []uint64{1, 3})
	re.NoError(err)
	re.False(seed.GetAffinityGroupState("g").AffinitySchedulingAllowed, "admin peers wait for the first health snapshot too")
	m, err = NewManager(t.Context(), store, stores, seed.conf, seed.regionLabeler)
	re.NoError(err)
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
				time.Sleep(50 * time.Millisecond)
				synctest.Wait()
				if loaded {
					re.Zero(calls, "loaded targets wait for the first store health check")
					re.Equal(uint64(1), loadStoredGroup(re, store, "g").LeaderStoreID)
					m.ObserveAvailableRegion(generateRegionForTest(11, []uint64{1, 2}, nonOverlappingRange), m.GetAffinityGroupState("g"))
					re.Zero(calls, "same-leader confirmation needs no persistence")
					re.False(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
				} else {
					re.Equal(1, calls, "groups without targets can initialize before the first health check")
					re.Equal(uint64(2), loadStoredGroup(re, store, "g").LeaderStoreID)
				}
				m.CheckGroupsAvailability()
				re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)
			})
		})
	}
}

func TestChangedTargetStartsDegradedWindow(t *testing.T) {
	for _, source := range []string{"watcher", "conflict", "admin", "policy"} {
		t.Run(source, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				m, store := newTargetTestManager(t)
				group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
				m.SyncGroupFromEtcd(group)
				m.applyStoreConditions(map[uint64]storeCondition{2: storeBusy})
				m.ExpireAffinityGroup("g")
				stale := targetFor(m, "g", 1, 1, 3)
				group.LeaderStoreID = 2
				re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, group.ID, group) }))
				switch source {
				case "watcher":
					m.SyncGroupFromEtcd(group)
				case "admin":
					m.applyStoreConditions(map[uint64]storeCondition{})
					m.storage = &replyTestStorage{AffinityStorage: store, onSave: func() {
						m.applyStoreConditions(map[uint64]storeCondition{2: storeBusy})
					}}
					_, err := m.UpdateAffinityGroupPeers("g", 2, group.VoterStoreIDs)
					re.NoError(err)
				case "policy":
					_, err := m.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
					re.NoError(err)
					group.BalancePolicy = BalancePolicyFixed
				default:
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
				m.SyncGroupFromEtcd(group)
				written, err := m.updateGroupTarget(targetFor(m, "g", 2, 1, 2))
				re.NoError(err)
				re.True(written)
				re.Equal(expiresAt, m.groups["g"].degradedExpiredAt, "duplicate targets do not extend the window")
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
	before = m.GetAffinityGroupState("g").affinityVer
	_, err = m.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2})
	re.NoError(err)
	re.Equal(before+1, m.GetAffinityGroupState("g").affinityVer)
}

func TestClassicFirstWinWaitsForStoreConditions(t *testing.T) {
	re := require.New(t)
	m, store := newTargetTestManager(t)
	group := &Group{ID: "g", CreateTimestamp: 1, LeaderStoreID: 1, VoterStoreIDs: []uint64{1, 2}}
	re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, group.ID, group) }))
	m.SyncGroupFromEtcd(group)
	before := m.GetAffinityGroupState("g")
	state, err := m.updateAffinityGroupPeersWithState("g", before, 2, group.VoterStoreIDs)
	re.NoError(err)
	re.Equal(before, state)
	state, err = m.updateAffinityGroupPeersWithState("g", before, 1, group.VoterStoreIDs)
	re.NoError(err)
	re.False(state.AffinitySchedulingAllowed)
	re.False(state.RegularSchedulingAllowed)
	m.applyStoreConditions(map[uint64]storeCondition{})
	state, err = m.updateAffinityGroupPeersWithState("g", m.GetAffinityGroupState("g"), 2, group.VoterStoreIDs)
	re.NoError(err)
	re.Equal(uint64(2), state.LeaderStoreID)
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

func TestFirstWinBatchesAndBackoff(t *testing.T) {
	for _, outcome := range []string{"failed", "not written", "written", "not ready"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				re := require.New(t)
				m, _ := newTargetTestManager(t)
				re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}}))
				failures := testutil.ToFloat64(targetWriteFailureCounter)
				release := make(chan struct{})
				var batches [][]GroupTarget
				var mutex sync.Mutex
				snapshot := func() [][]GroupTarget {
					mutex.Lock()
					defer mutex.Unlock()
					return slices.Clone(batches)
				}
				m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
					mutex.Lock()
					batches = append(batches, slices.Clone(targets))
					first := len(batches) == 1
					mutex.Unlock()
					if first {
						<-release
						switch outcome {
						case "not ready":
							return nil, errors.Annotate(ErrTargetWriterNotReady, "PD leader is not found")
						case "failed":
							return nil, errors.New("PD unavailable")
						case "written":
							return []string{"g"}, nil
						}
					}
					return nil, nil
				}))
				m.sendObservedTarget(observedTarget{target: targetFor(m, "g", 1, 1, 2)})
				time.Sleep(targetBatchInterval)
				synctest.Wait()
				re.Len(snapshot(), 1)
				for range 10 {
					m.sendObservedTarget(observedTarget{target: targetFor(m, "g", 1, 1, 2)})
				}
				latest := targetFor(m, "g", 2, 1, 2)
				m.sendObservedTarget(observedTarget{target: latest})
				m.sendObservedTarget(observedTarget{target: targetFor(m, "h", 1, 1, 2)})
				time.Sleep(2 * time.Second)
				synctest.Wait()
				re.Len(snapshot(), 1, "observations during a request cannot send concurrently")
				close(release)
				synctest.Wait()
				time.Sleep(targetBatchInterval)
				synctest.Wait()
				if outcome == "failed" || outcome == "not ready" {
					re.Len(snapshot(), 1, "failed requests back off")
				} else {
					re.Len(snapshot(), 2)
					re.Equal([]GroupTarget{targetFor(m, "h", 1, 1, 2)}, snapshot()[1], "other groups proceed while g cools down")
				}
				time.Sleep(targetCooldown - 2*targetBatchInterval)
				synctest.Wait()
				for _, batch := range snapshot()[1:] {
					for _, target := range batch {
						re.NotEqual("g", target.Expected.ID)
					}
				}
				time.Sleep(targetBatchInterval)
				synctest.Wait()
				all := snapshot()
				last := all[len(all)-1]
				re.Equal(latest, last[0], "the latest in-flight observation is sent after the window")
				if outcome == "failed" {
					re.Equal(failures+1, testutil.ToFloat64(targetWriteFailureCounter))
				} else {
					re.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))
				}
				re.Zero(m.GetAffinityGroupState("g").LeaderStoreID, "HTTP success waits for the watcher")
			})
		})
	}
}

func TestFirstWinBoundedFIFO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		release := make(chan struct{})
		var batches [][]GroupTarget
		m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
			batches = append(batches, slices.Clone(targets))
			if len(batches) == 1 {
				<-release
			}
			return nil, nil
		}))
		m.sendObservedTarget(observedTarget{target: GroupTarget{Expected: Group{ID: "hot"}}})
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		for i := range maxPendingTargets + 1 {
			m.sendObservedTarget(observedTarget{target: GroupTarget{Expected: Group{ID: fmt.Sprintf("g%04d", i)}}})
		}
		m.sendObservedTarget(observedTarget{target: GroupTarget{Expected: Group{ID: "g0000"}, LeaderStoreID: 2}})
		m.targetWriteMutex.Lock()
		re.Len(m.pendingTargets, maxPendingTargets)
		m.targetWriteMutex.Unlock()
		close(release)
		synctest.Wait()
		for range 9 {
			time.Sleep(50 * time.Millisecond)
			synctest.Wait()
			// Hot IDs go to the tail, behind every previously accepted group.
			m.sendObservedTarget(observedTarget{target: GroupTarget{Expected: Group{ID: "hot"}}})
		}
		var ids []string
		for _, batch := range batches[1:] {
			re.LessOrEqual(len(batch), etcdutil.MaxEtcdTxnOps)
			for _, target := range batch {
				ids = append(ids, target.Expected.ID)
			}
		}
		re.Equal(uint64(2), batches[1][0].LeaderStoreID)
		re.Len(ids, maxPendingTargets-1)
		for i, id := range ids[:maxPendingTargets-1] {
			re.Equal(fmt.Sprintf("g%04d", i), id)
		}
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
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		re.Equal(1, calls)
		cancel()
		synctest.Wait()
		re.True(stopped)
		m.ObserveAvailableRegion(region, m.GetAffinityGroupState("g"))
		synctest.Wait()
		re.Equal(1, calls)
		m.targetWriteMutex.Lock()
		re.Empty(m.pendingTargets)
		re.Empty(m.pendingTargetIDs)
		re.False(m.targetSenderRunning)
		m.targetWriteMutex.Unlock()
	})
}

func TestFirstWinCooldownCapacityAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		m := newTargetTestManagerWithStorage(t, ctx, storage.NewStorageWithMemoryBackend())
		var sent []GroupTarget
		m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
			sent = append(sent, targets...)
			return nil, nil
		}))
		for i := range maxPendingTargets {
			m.sendObservedTarget(observedTarget{target: GroupTarget{Expected: Group{ID: strconv.Itoa(i)}}})
		}
		for range 9 {
			time.Sleep(targetBatchInterval)
			synctest.Wait()
		}
		re.Len(sent, maxPendingTargets)
		m.sendObservedTarget(observedTarget{target: GroupTarget{Expected: Group{ID: "overflow"}}})
		m.targetWriteMutex.Lock()
		re.Len(m.pendingTargets, maxPendingTargets, "cooling groups still occupy capacity")
		_, exists := m.pendingTargets["overflow"]
		re.False(exists)
		m.targetWriteMutex.Unlock()
		latest := GroupTarget{Expected: Group{ID: "0"}, LeaderStoreID: 2}
		m.sendObservedTarget(observedTarget{target: latest})
		time.Sleep(time.Second)
		synctest.Wait()
		re.Equal(latest, sent[len(sent)-1])
		m.sendObservedTarget(observedTarget{target: GroupTarget{Expected: Group{ID: "new"}}})
		time.Sleep(targetBatchInterval)
		synctest.Wait()
		re.Equal("new", sent[len(sent)-1].Expected.ID, "expired cooldowns release capacity")
		m.sendObservedTarget(observedTarget{target: latest})
		cancel()
		synctest.Wait()
		m.targetWriteMutex.Lock()
		re.Empty(m.pendingTargets)
		re.Empty(m.pendingTargetIDs)
		re.False(m.targetSenderRunning)
		m.targetWriteMutex.Unlock()
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
		time.Sleep(50 * time.Millisecond)
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
				re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}, {GroupID: "h"}, {GroupID: "queued"}}))
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
					time.Sleep(targetBatchInterval)
					synctest.Wait()
					state := m.GetAffinityGroupState("g")
					re.True(state.AffinitySchedulingAllowed)
					re.True(state.HasUnpersistedTarget())
					time.Sleep(time.Second)
				}

				m.ObserveAvailableRegion(oldRegion, m.GetAffinityGroupState("g"))
				m.ObserveAvailableRegion(oldRegion, m.GetAffinityGroupState("h"))
				time.Sleep(targetBatchInterval)
				synctest.Wait()
				re.Equal(blockOn, calls)
				// This observation arrives after the batch was sent but before its 404.
				m.ObserveAvailableRegion(oldRegion, m.GetAffinityGroupState("queued"))
				m.ExpireAffinityGroup("g")
				expired := m.GetAffinityGroupState("g")
				release <- struct{}{}
				synctest.Wait()
				state := m.GetAffinityGroupState("g")
				re.True(state.RegularSchedulingAllowed, "a late 404 must leave the group expired")
				re.False(state.AffinitySchedulingAllowed)
				re.Equal(expired.Group, state.Group)
				re.Equal(expired.affinityVer, state.affinityVer)
				for _, id := range []string{"h", "queued"} {
					re.True(m.GetAffinityGroupState(id).HasUnpersistedTarget(), id)
				}

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
				{GroupID: "h"}, {GroupID: "p"},
			}))
			createdG := loadStoredGroup(re, store, "g")
			createdH := loadStoredGroup(re, store, "h")
			m.SyncGroupFromEtcd(createdG)
			m.SyncGroupFromEtcd(createdH)
			m.SyncGroupFromEtcd(loadStoredGroup(re, store, "p"))
			re.NoError(m.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}}, nil))

			calls, failures := 0, testutil.ToFloat64(targetWriteFailureCounter)
			supported := false
			var lastSent []GroupTarget
			m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
				calls++
				lastSent = slices.Clone(targets)
				if !supported {
					return nil, errors.Annotate(ErrTargetWriterUnsupported, "404 Not Found")
				}
				return pdManager.UpdateAffinityGroupTargets(targets)
			}))

			regionG := generateRegionForTest(10, []uint64{2, 1}, nonOverlappingRange)
			regionH := generateRegionForTest(20, []uint64{1, 2}, nonOverlappingRange)
			regionOther := generateRegionForTest(30, []uint64{3, 1}, nonOverlappingRange)
			flush := func() {
				time.Sleep(targetBatchInterval)
				synctest.Wait()
			}
			observe := func(id string, region *core.RegionInfo) {
				m.ObserveAvailableRegion(region, m.GetAffinityGroupState(id))
				flush()
			}

			// One 404 initializes both groups in the coalesced request.
			m.ObserveAvailableRegion(regionG, m.GetAffinityGroupState("g"))
			m.ObserveAvailableRegion(regionG, m.GetAffinityGroupState("p"))
			flush()
			re.Len(lastSent, 2)
			re.True(m.GetAffinityGroupState("p").HasUnpersistedTarget())
			re.Equal(1, calls)
			stateG := m.GetAffinityGroupState("g")
			re.Equal(uint64(2), stateG.LeaderStoreID)
			re.Equal([]uint64{1, 2}, stateG.VoterStoreIDs)
			re.True(stateG.AffinitySchedulingAllowed)
			re.False(stateG.RegularSchedulingAllowed)
			re.True(stateG.HasUnpersistedTarget())
			re.Zero(loadStoredGroup(re, store, "g").LeaderStoreID)
			re.Equal(failures, testutil.ToFloat64(targetWriteFailureCounter))

			// A policy-only watcher update keeps the local target. Its eventual
			// persistence compares against the new policy and the empty stored target.
			_, err := pdManager.UpdateAffinityGroupPolicy("p", BalancePolicyFixed)
			re.NoError(err)
			storedP := loadStoredGroup(re, store, "p")
			m.SyncGroupFromEtcd(storedP)
			stateP := m.GetAffinityGroupState("p")
			re.True(stateP.HasUnpersistedTarget())
			re.Equal(BalancePolicyFixed, stateP.BalancePolicy)
			re.Equal(uint64(2), stateP.LeaderStoreID)
			re.Zero(storedP.LeaderStoreID)
			re.Nil(storedP.VoterStoreIDs)

			// An observation during the cooldown initializes another group locally.
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
			re.Equal([]GroupTarget{{Expected: *createdG, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}}}, lastSent)
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
			m.ObserveAvailableRegion(regionG, m.GetAffinityGroupState("g"))
			m.ObserveAvailableRegion(regionG, m.GetAffinityGroupState("p"))
			flush()
			re.Equal(3, calls)
			re.Equal([]GroupTarget{
				{Expected: *createdG, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}},
				{Expected: *storedP, LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2}},
			}, lastSent)
			persistedP := loadStoredGroup(re, store, "p")
			re.Equal(uint64(2), persistedP.LeaderStoreID)
			re.Equal(BalancePolicyFixed, persistedP.BalancePolicy)
			m.SyncGroupFromEtcd(persistedP)
			re.False(m.GetAffinityGroupState("p").HasUnpersistedTarget())
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
			re.Equal([]GroupTarget{{Expected: *createdH, LeaderStoreID: 3, VoterStoreIDs: []uint64{1, 3}}}, lastSent)
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
			m.CheckGroupsAvailability()
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
				time.Sleep(targetBatchInterval)
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
		if commits == 1 {
			_, err := other.UpdateAffinityGroupPeers("g", 2, []uint64{1, 2, 3})
			re.NoError(err)
		}
	}}
	m.storage = wrapped
	targets := []GroupTarget{targetFor(m, "h", 1, 1, 2, 3), stale}
	written, err := m.UpdateAffinityGroupTargets(targets)
	re.ErrorIs(err, errs.ErrEtcdTxnConflict)
	re.Empty(written, "an out-of-band conflict aborts the whole chunk")
	re.Equal(1, commits, "conflicting chunks must not retry individual targets")
	re.Zero(loadStoredGroup(re, store, "h").LeaderStoreID)
	re.Zero(m.GetAffinityGroupState("g").LeaderStoreID)
	wrapped.beforeCommit = nil
	written, err = m.UpdateAffinityGroupTargets(targets)
	re.NoError(err, "the next patrol skips the stale group during prefetch")
	re.Equal([]string{"h"}, written)
	re.Equal(uint64(1), loadStoredGroup(re, store, "h").LeaderStoreID)
	re.Equal(uint64(2), loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Equal(uint64(2), m.GetAffinityGroupState("g").LeaderStoreID)
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

func TestTargetBatchesWithEtcd(t *testing.T) {
	re := require.New(t)
	var rpcCount atomic.Int64
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, &etcdutil.TestEtcdClusterOptions{
		ClientCfgModifier: func(config *clientv3.Config) {
			config.DialOptions = append(config.DialOptions, grpc.WithChainUnaryInterceptor(
				func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
					invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
					if method == "/etcdserverpb.KV/Txn" || method == "/etcdserverpb.KV/Range" {
						rpcCount.Add(1)
					}
					return invoker(ctx, method, req, reply, cc, opts...)
				}))
		},
	})
	defer clean()
	store := storage.NewStorageWithEtcdBackend(client)
	m := newTargetTestManagerWithStorage(t, t.Context(), store)
	changes := make([]GroupKeyRanges, 2*etcdutil.MaxEtcdTxnOps+1)
	for i := range changes {
		changes[i].GroupID = fmt.Sprintf("g%03d", i)
	}
	re.NoError(m.CreateAffinityGroups(changes))
	targets := make([]GroupTarget, len(changes))
	for i, change := range changes {
		targets[i] = targetFor(m, change.GroupID, 1, 1, 2)
	}
	rpcCount.Store(0)
	written, err := m.UpdateAffinityGroupTargets(targets)
	re.NoError(err)
	re.Len(written, len(changes))
	re.Equal(int64(6), rpcCount.Load(), "each <=120-group chunk needs exactly one read and one commit")
	for _, change := range changes {
		re.Equal(uint64(1), m.GetAffinityGroupState(change.GroupID).LeaderStoreID)
	}
	for i := range targets {
		targets[i] = targetFor(m, changes[i].GroupID, 2, 1, 2)
	}
	for _, conflictChunk := range []int{0, 1} {
		commits := 0
		conflictIndex := conflictChunk*etcdutil.MaxEtcdTxnOps + 60
		m.storage = &targetReplyStorage{AffinityStorage: store, beforeCommit: func() {
			commits++
			if commits == conflictChunk+1 {
				concurrent := *targets[conflictIndex].result()
				concurrent.LeaderStoreID = 3
				concurrent.VoterStoreIDs = []uint64{1, 2, 3}
				value, marshalErr := json.Marshal(concurrent)
				re.NoError(marshalErr)
				// This out-of-band Put is not a Txn/Range RPC from the target writer.
				_, putErr := client.Put(t.Context(), keypath.AffinityGroupPath(concurrent.ID), string(value))
				re.NoError(putErr)
			}
		}}
		rpcCount.Store(0)
		written, err = m.UpdateAffinityGroupTargets(targets)
		re.ErrorIs(err, errs.ErrEtcdTxnConflict)
		re.Equal(conflictChunk+1, commits, "no per-group fallback after an out-of-band conflict")
		re.Equal(int64(2*(conflictChunk+1)), rpcCount.Load(), "even a conflicting 120-group chunk uses at most two RPCs")
		if conflictChunk == 0 {
			re.Empty(written)
		} else {
			re.Len(written, etcdutil.MaxEtcdTxnOps-1, "earlier chunks survive; prefetch skips the previous conflicting key")
			re.Equal(uint64(2), loadStoredGroup(re, store, changes[0].GroupID).LeaderStoreID)
		}
		re.Equal(uint64(1), loadStoredGroup(re, store, changes[conflictIndex-1].GroupID).LeaderStoreID)
		re.Equal(uint64(1), m.GetAffinityGroupState(changes[conflictIndex].GroupID).LeaderStoreID)
		re.Equal(uint64(3), loadStoredGroup(re, store, changes[conflictIndex].GroupID).LeaderStoreID)
	}
	m.storage = store
	rpcCount.Store(0)
	written, err = m.UpdateAffinityGroupTargets(targets)
	re.NoError(err)
	re.Len(written, len(changes)-2, "the next retry skips both stale targets and commits the others")
	re.Equal(int64(6), rpcCount.Load(), "prefetch skips stale keys without extra RPCs")
	for _, index := range []int{60, etcdutil.MaxEtcdTxnOps + 60} {
		re.Equal(uint64(3), m.GetAffinityGroupState(changes[index].GroupID).LeaderStoreID)
	}
}

// TestUpdateAffinityGroupTargetsChecksPolicy checks that a target decided before
// a policy change is not written.
func TestUpdateAffinityGroupTargetsChecksPolicy(t *testing.T) {
	re := require.New(t)
	manager, store := newTargetTestManager(t)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
	target := targetFor(manager, "g", 1, 1, 2, 3)
	_, err := manager.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
	re.NoError(err)

	written, err := manager.UpdateAffinityGroupTargets([]GroupTarget{target})
	re.NoError(err)
	re.Empty(written)
	re.Zero(loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Equal(BalancePolicyFixed, manager.GetAffinityGroupState("g").BalancePolicy)
}

// replyTestStorage counts group saves and can lose the reply of one committed
// transaction.
type replyTestStorage struct {
	endpoint.AffinityStorage
	saves     int
	loseReply bool
	onSave    func()
}

func (s *replyTestStorage) SaveAffinityGroup(txn kv.Txn, groupID string, group any) error {
	s.saves++
	if s.onSave != nil {
		s.onSave()
	}
	return s.AffinityStorage.SaveAffinityGroup(txn, groupID, group)
}

func (s *replyTestStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	if err := s.AffinityStorage.RunInTxn(ctx, f); err != nil || !s.loseReply {
		return err
	}
	s.loseReply = false
	return errors.New("the reply of a committed transaction is lost")
}

// TestConfirmationChecksStoredTarget checks that without a target writer a
// Region with the layout of the target in memory confirms it only if storage
// still holds it. A first-win write whose reply was lost may have stored another
// target, and confirming the old one would keep memory and storage apart.
func TestConfirmationChecksStoredTarget(t *testing.T) {
	re := require.New(t)
	m, store := newTargetTestManager(t)
	re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{nonOverlappingRange}}}))
	_, err := m.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)
	m.CheckGroupsAvailability()
	wrapped := &replyTestStorage{AffinityStorage: store}
	m.storage = wrapped

	// A Region with the current target confirms it without a write.
	m.ExpireAffinityGroup("g")
	m.ObserveAvailableRegion(generateRegionForTest(1, []uint64{1, 2, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
	re.Zero(wrapped.saves)
	re.True(m.GetAffinityGroupState("g").AffinitySchedulingAllowed)

	// First-win stores leader 2, but the reply is lost, so memory keeps leader 1.
	m.ExpireAffinityGroup("g")
	wrapped.loseReply = true
	m.ObserveAvailableRegion(generateRegionForTest(2, []uint64{2, 1, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
	re.Equal(uint64(2), loadStoredGroup(re, store, "g").LeaderStoreID)
	re.Equal(uint64(1), m.GetAffinityGroupState("g").LeaderStoreID)

	// A Region with the old layout does not confirm it: memory takes the stored target.
	m.ObserveAvailableRegion(generateRegionForTest(3, []uint64{1, 2, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
	state := m.GetAffinityGroupState("g")
	re.Equal(*loadStoredGroup(re, store, "g"), state.Group)
	re.Equal(uint64(2), state.LeaderStoreID)
}

func TestClassicFirstWinKeepsCommittedPolicy(t *testing.T) {
	for _, hasTarget := range []bool{false, true} {
		t.Run(fmt.Sprintf("has-target-%t", hasTarget), func(t *testing.T) {
			re := require.New(t)
			m, store := newTargetTestManager(t)
			m.CheckGroupsAvailability()
			re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
			if hasTarget {
				_, err := m.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
				re.NoError(err)
			}
			wrapped := &replyTestStorage{AffinityStorage: store, loseReply: true}
			m.storage = wrapped
			_, err := m.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
			re.Error(err)
			re.Empty(m.GetAffinityGroupState("g").BalancePolicy)
			committed := *loadStoredGroup(re, store, "g")
			re.Equal(BalancePolicyFixed, committed.BalancePolicy)
			saves := wrapped.saves
			m.ExpireAffinityGroup("g")
			m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{2, 1, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
			re.Equal(saves, wrapped.saves, "stale first-win must not overwrite the committed fixed policy")
			re.Equal(committed, *loadStoredGroup(re, store, "g"))
			re.Equal(committed, m.GetAffinityGroupState("g").Group)
			m.ObserveAvailableRegion(generateRegionForTest(11, []uint64{2, 1, 3}, nonOverlappingRange), m.GetAffinityGroupState("g"))
			state := m.GetAffinityGroupState("g")
			re.Equal(BalancePolicyFixed, state.BalancePolicy)
			re.Equal(uint64(2), state.LeaderStoreID)
			re.Equal(state.Group, *loadStoredGroup(re, store, "g"))
		})
	}
}

func TestMCSFirstWinKeepsFixedTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		m, _ := newTargetTestManager(t)
		re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g"}}))
		_, err := m.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2})
		re.NoError(err)
		m.DegradeAffinityGroup("g")
		observed := m.GetAffinityGroupState("g")
		_, err = m.UpdateAffinityGroupPolicy("g", BalancePolicyFixed)
		re.NoError(err)
		calls := 0
		m.SetTargetWriter(targetWriterFunc(func(_ context.Context, targets []GroupTarget) ([]string, error) {
			calls++
			return []string{targets[0].Expected.ID}, nil
		}))
		m.ObserveAvailableRegion(generateRegionForTest(10, []uint64{2, 1}, nonOverlappingRange), observed)
		synctest.Wait()
		re.Zero(calls)
		re.Equal(uint64(1), m.GetAffinityGroupState("g").LeaderStoreID)
	})
}
