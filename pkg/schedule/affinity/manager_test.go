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
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
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

// TestGetRegionAffinityGroupState tests the GetRegionAffinityGroupState method of Manager.
func TestGetRegionAffinityGroupState(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := uint64(1); i <= 4; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: i, Address: "test"})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create affinity group with 6 key ranges for testing
	ranges := createGroupForTest(re, manager, "test_group", 6)

	// Setup: voterStoreIDs will be sorted.
	_, err = manager.UpdateAffinityGroupPeers("test_group", 1, []uint64{3, 2, 1})
	re.NoError(err)
	groupInfo := getGroupForTest(re, manager, "test_group")
	re.True(slices.Equal([]uint64{1, 2, 3}, groupInfo.VoterStoreIDs))

	// Test the positive case first
	t.Run("region conforming to affinity", func(t *testing.T) {
		re := require.New(t)
		region := generateRegionForTest(1, []uint64{1, 2, 3}, ranges[0])
		_, isAffinity := manager.GetRegionAffinityGroupState(region)
		re.True(isAffinity)
	})

	// Test negative cases: all should return false
	testCases := []struct {
		name          string
		regionID      uint64
		peers         []uint64
		keyRange      keyutil.KeyRange
		withoutLeader bool
		setup         func()
	}{
		{
			name:     "region not in any affinity group",
			regionID: 1,
			peers:    []uint64{1, 2, 3},
			keyRange: nonOverlappingRange,
		},
		{
			name:     "region with wrong leader",
			regionID: 1,
			peers:    []uint64{2, 1, 3},
			keyRange: ranges[1],
		},
		{
			name:     "region with wrong voter stores",
			regionID: 3,
			peers:    []uint64{1, 2, 4},
			keyRange: ranges[2],
		},
		{
			name:     "region with different number of voters",
			regionID: 4,
			peers:    []uint64{1, 2},
			keyRange: ranges[3],
		},
		{
			name:          "region without leader",
			regionID:      5,
			peers:         []uint64{1, 2, 3},
			keyRange:      ranges[4],
			withoutLeader: true,
		},
		{
			name:     "group not in effect",
			regionID: 6,
			peers:    []uint64{1, 2, 3},
			keyRange: ranges[5],
			setup: func() {
				manager.ExpireAffinityGroup("test_group")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			if tc.setup != nil {
				tc.setup()
			}

			region := generateRegionForTest(tc.regionID, tc.peers, tc.keyRange)
			if tc.withoutLeader {
				region = region.Clone(core.WithLeader(nil))
			}

			_, isAffinity := manager.GetRegionAffinityGroupState(region)
			re.False(isAffinity)
		})
	}
}

// TestBasicGroupOperations tests basic group CRUD operations
func TestBasicGroupOperations(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	// Create region labeler
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a group
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "group1"}})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers("group1", 1, []uint64{1})
	re.NoError(err)
	re.True(manager.IsGroupExist("group1"))

	// Delete the group (no key ranges, so force=false should work)
	err = manager.DeleteAffinityGroups([]string{"group1"}, false)
	re.NoError(err)
	re.False(manager.IsGroupExist("group1"))
}

func TestCollectGroupStatesByQuery(t *testing.T) {
	re := require.New(t)
	manager := newTxnTestManager(t)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "p-1"}, {GroupID: "p-10"}, {GroupID: "q-1"}}))
	manager.Lock()
	manager.groups["p-1"].VoterStoreIDs = []uint64{1, 2, 3}
	manager.Unlock()

	for _, tc := range []struct {
		ids    []string
		prefix string
		want   []string
	}{
		{nil, "", []string{"p-1", "p-10", "q-1"}},
		{nil, "p-", []string{"p-1", "p-10"}},
		{nil, "p-1", []string{"p-1", "p-10"}},
		{nil, "r", []string{}},
		{[]string{"p-10", "q-1", "missing"}, "p-", []string{"p-10"}},
		{[]string{"q-1"}, "", []string{"q-1"}},
		{[]string{" p-1 ", "p-1", "", "q-1"}, "p-", []string{"p-1"}},
		{[]string{""}, "", []string{}},
	} {
		states, err := CollectGroupStatesByQuery(manager, tc.ids, tc.prefix)
		re.NoError(err)
		re.ElementsMatch(tc.want, slices.Collect(maps.Keys(states)), "%v %q", tc.ids, tc.prefix)
	}
	states, err := CollectGroupStatesByQuery(manager, nil, "p-")
	re.NoError(err)
	states["p-1"].VoterStoreIDs[0] = 99
	re.Equal([]uint64{1, 2, 3}, manager.GetAffinityGroupState("p-1").VoterStoreIDs)
	for _, prefix := range []string{"bad prefix", strings.Repeat("a", 65)} {
		_, err := CollectGroupStatesByQuery(manager, nil, prefix)
		re.ErrorIs(err, errs.ErrAffinityGroupContent)
	}
}

// TestSyncLabelRuleBeforeGroup ensures label rules arriving before group sync are applied after group creation.
func TestSyncLabelRuleBeforeGroup(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	gkr := GroupKeyRanges{
		GroupID: "group-label-first",
		KeyRanges: []keyutil.KeyRange{
			{StartKey: []byte{0x01}, EndKey: []byte{0x02}},
		},
	}
	labelRule := MakeLabelRule(gkr)
	// Simulate watcher path (JSON roundtrip) to ensure Data types match real events.
	b, err := json.Marshal(labelRule)
	re.NoError(err)
	parsedRule, err := labeler.NewLabelRuleFromJSON(b)
	re.NoError(err)
	re.NoError(manager.SyncKeyRangesFromEtcd(parsedRule))

	// Group arrives after label rule.
	manager.SyncGroupFromEtcd(&Group{ID: gkr.GroupID})

	state := manager.GetAffinityGroupState(gkr.GroupID)
	re.NotNil(state)
	re.Equal(1, state.RangeCount)
}

func TestSyncGroupFromEtcdUpdatesBalancePolicy(t *testing.T) {
	re := require.New(t)
	m := newTargetTest(t).Manager
	ranges := createGroupForTest(re, m, "policy", 1)
	m.checkGroupsAvailability()
	re.True(m.storeConditionsKnown)
	_, err := m.UpdateAffinityGroupPeers("policy", 1, []uint64{1, 2, 3})
	re.NoError(err)
	_, isAffinity := m.GetAndCacheRegionAffinityGroupState(generateRegionForTest(1, []uint64{1, 2, 3}, ranges[0]))
	re.True(isAffinity)
	before := m.GetAffinityGroupState("policy")
	re.Equal(PhaseStable, before.Phase)

	// A policy update must neither mutate watcher input nor recompute availability from newly known conditions.
	m.Lock()
	m.unavailableStores[1] = storeDown
	m.Unlock()
	input := &Group{ID: "policy", CreateTimestamp: before.CreateTimestamp, BalancePolicy: BalancePolicyFixed, LeaderStoreID: 1, VoterStoreIDs: []uint64{3, 1, 2}}
	m.SyncGroupFromEtcd(input)
	re.Equal([]uint64{1, 2, 3}, m.GetAffinityGroupState("policy").VoterStoreIDs)
	re.Equal([]uint64{3, 1, 2}, input.VoterStoreIDs, "watcher input must not be mutated")
	for _, policy := range []BalancePolicy{BalancePolicyFixed, BalancePolicyAuto} {
		updated := *input
		updated.BalancePolicy = policy
		m.SyncGroupFromEtcd(&updated)
		after := m.GetAffinityGroupState("policy")
		re.Equal(policy, after.BalancePolicy)
		re.Equal(PhaseStable, after.Phase)
		re.Equal(before.affinityVer, after.affinityVer)
		re.Equal(before.AffinityRegionCount, after.AffinityRegionCount)
		re.Equal(before.RegionCount, after.RegionCount)
		re.Equal(before.AffinitySchedulingAllowed, after.AffinitySchedulingAllowed)
		m.RLock()
		re.Equal(1, m.affinityRegionCount)
		m.RUnlock()
	}

	m.applyStoreConditions(map[uint64]storeCondition{})
	m.DegradeAffinityGroup("policy")
	observed := m.GetAffinityGroupState("policy")
	m.SyncGroupFromEtcd(input)
	auto := *input
	auto.BalancePolicy = BalancePolicyAuto
	m.SyncGroupFromEtcd(&auto)
	updatedPeers, err := m.updateAffinityGroupPeersWithState("policy", observed, 2, observed.VoterStoreIDs)
	re.NoError(err)
	re.Equal(uint64(2), updatedPeers.LeaderStoreID, "policy-only updates must not discard an in-flight peer update")

	// Changing the target still resets Region affinity and applies the conditions.
	m.Lock()
	m.unavailableStores[3] = storeDown
	m.Unlock()
	changed := updatedPeers.Group
	changed.LeaderStoreID = 3
	m.SyncGroupFromEtcd(&changed)
	after := m.GetAffinityGroupState("policy")
	re.Greater(after.affinityVer, updatedPeers.affinityVer)
	re.Zero(after.AffinityRegionCount)
	re.False(after.AffinitySchedulingAllowed)
}

func TestBalancePolicyValidationAtMetadataEntrypoints(t *testing.T) {
	re := require.New(t)
	store := storage.NewStorageWithMemoryBackend()
	for _, group := range []*Group{
		{ID: "empty"},
		{ID: "auto", BalancePolicy: BalancePolicyAuto},
		{ID: "fixed", BalancePolicy: BalancePolicyFixed},
		{ID: "invalid", BalancePolicy: BalancePolicy("manual")},
	} {
		re.NoError(store.RunInTxn(t.Context(), func(txn kv.Txn) error {
			return store.SaveAffinityGroup(txn, group.ID, group)
		}))
	}
	manager := newTargetTestWithStorage(t, t.Context(), store).Manager
	// A group with an unknown policy written by a newer version is still loaded and keeps its target.
	re.Equal(4, manager.GetAffinityGroupCount())
	for id, fixed := range map[string]bool{"empty": false, "auto": false, "fixed": true, "invalid": true} {
		re.Equal(fixed, manager.GetAffinityGroupState(id).BalancePolicy.IsFixed(), id)
	}
	for _, policy := range []BalancePolicy{"", BalancePolicyAuto, BalancePolicyFixed, BalancePolicy("manual")} {
		group := &Group{ID: "watch", BalancePolicy: policy}
		manager.SyncGroupFromEtcd(group)
		re.Equal(policy, manager.GetAffinityGroupState("watch").BalancePolicy)
		if policy == "manual" {
			re.ErrorIs(manager.AdjustGroup(group), errs.ErrAffinityGroupContent)
		} else {
			re.NoError(manager.AdjustGroup(group))
		}
	}
}

func TestUpdateAffinityGroupPolicy(t *testing.T) {
	re := require.New(t)
	test := newTargetTest(t)
	manager := test.Manager
	ranges := createGroupForTest(re, manager, "policy", 1)
	manager.checkGroupsAvailability()
	re.True(manager.storeConditionsKnown)
	_, err := manager.UpdateAffinityGroupPeers("policy", 1, []uint64{1, 2, 3})
	re.NoError(err)
	_, isAffinity := manager.GetAndCacheRegionAffinityGroupState(generateRegionForTest(1, []uint64{1, 2, 3}, ranges[0]))
	re.True(isAffinity)
	before := manager.GetAffinityGroupState("policy")
	re.Equal(PhaseStable, before.Phase)

	for _, policy := range []BalancePolicy{BalancePolicyFixed, BalancePolicyFixed, BalancePolicyAuto} {
		state, err := manager.UpdateAffinityGroupPolicy("policy", policy)
		re.NoError(err)
		re.Equal(policy, state.BalancePolicy)
		re.Equal(before.LeaderStoreID, state.LeaderStoreID)
		re.Equal(before.VoterStoreIDs, state.VoterStoreIDs)
		re.Equal(PhaseStable, state.Phase)
		re.Equal(before.affinityVer, state.affinityVer)
		re.Equal(before.AffinityRegionCount, state.AffinityRegionCount)
		persisted := test.stored("policy")
		re.Equal(policy, persisted.BalancePolicy)
		re.Equal(before.LeaderStoreID, persisted.LeaderStoreID)
		re.Equal(before.VoterStoreIDs, persisted.VoterStoreIDs)
	}

	for _, policy := range []BalancePolicy{"", BalancePolicy("manual")} {
		_, err = manager.UpdateAffinityGroupPolicy("policy", policy)
		re.ErrorIs(err, errs.ErrAffinityGroupContent)
		re.Equal(BalancePolicyAuto, manager.GetAffinityGroupState("policy").BalancePolicy)
	}
	_, err = manager.UpdateAffinityGroupPolicy("missing", BalancePolicyFixed)
	re.ErrorIs(err, errs.ErrAffinityGroupNotFound)
	_, err = manager.UpdateAffinityGroupPolicy("invalid/id", BalancePolicyFixed)
	re.ErrorIs(err, errs.ErrInvalidGroupID)

	// A commit that returned an error leaves memory behind storage; subsequent updates preserve stored fields.
	lost := &lostReplyStorage{AffinityStorage: test.store, lose: true}
	manager.storage = lost
	_, err = manager.UpdateAffinityGroupPeers("policy", 2, []uint64{1, 2, 3})
	re.Error(err)
	manager.Lock()
	manager.unavailableStores = map[uint64]storeCondition{2: storeLeaderEvicted}
	manager.Unlock()
	_, err = manager.UpdateAffinityGroupPolicy("policy", BalancePolicyFixed)
	re.NoError(err)
	re.Equal(uint64(2), test.stored("policy").LeaderStoreID)
	re.Equal(uint64(2), manager.GetAffinityGroupState("policy").LeaderStoreID)
	re.False(manager.GetAffinityGroupState("policy").AffinitySchedulingAllowed)
	manager.Lock()
	manager.unavailableStores = map[uint64]storeCondition{}
	manager.Unlock()

	lost.lose = true
	_, err = manager.UpdateAffinityGroupPolicy("policy", BalancePolicyAuto)
	re.Error(err)
	_, err = manager.UpdateAffinityGroupPeers("policy", 3, []uint64{1, 2, 3})
	re.NoError(err)
	re.Equal(BalancePolicyAuto, test.stored("policy").BalancePolicy)
	re.Equal(BalancePolicyAuto, manager.GetAffinityGroupState("policy").BalancePolicy)

	lost.lose = true
	_, err = manager.UpdateAffinityGroupPolicy("policy", BalancePolicyFixed)
	re.Error(err)
	_, err = manager.UpdateAffinityGroupPolicy("policy", BalancePolicyAuto)
	re.NoError(err)
	re.Equal(BalancePolicyAuto, test.stored("policy").BalancePolicy)
}

// lostReplyStorage returns an error after a transaction commits, like a reply
// lost on its way back.
type lostReplyStorage struct {
	endpoint.AffinityStorage
	lose bool
}

func (s *lostReplyStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	if err := s.AffinityStorage.RunInTxn(ctx, f); err != nil || !s.lose {
		return err
	}
	s.lose = false
	return errors.New("reply lost")
}

// TestRegionCountStaleCache documents that RegionCount counts stale cache entries when group changes.
func TestRegionCountStaleCache(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i < 7; i++ {
		storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i)}, core.SetLastHeartbeatTS(time.Now())))
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	ranges := createGroupForTest(re, manager, "g", 2)
	_, err = manager.UpdateAffinityGroupPeers("g", 1, []uint64{1, 2, 3})
	re.NoError(err)
	region := generateRegionForTest(100, []uint64{1, 2, 3}, ranges[0])

	// test GetRegionAffinityGroupState (read-only, no cache)
	_, isAffinity := manager.GetRegionAffinityGroupState(region)
	re.True(isAffinity)
	groupInfo := getGroupForTest(re, manager, "g")
	re.Zero(groupInfo.AffinityRegionCount)
	re.Empty(groupInfo.Regions)

	// test GetAndCacheRegionAffinityGroupState (saves cache)
	_, isAffinity = manager.GetAndCacheRegionAffinityGroupState(region)
	re.True(isAffinity)
	groupInfo = getGroupForTest(re, manager, "g")
	re.Equal(1, groupInfo.AffinityRegionCount)
	re.Len(groupInfo.Regions, 1)

	// Change peers, which bumps AffinityVer and invalidates affinity for the cached region.
	_, err = manager.UpdateAffinityGroupPeers("g", 4, []uint64{4, 5, 6})
	re.NoError(err)
	group2 := manager.GetAffinityGroupState("g")
	re.NotNil(group2)
	re.Zero(group2.AffinityRegionCount)
	testCacheStale(re, manager, region)

	// Remove key ranges, which bumps AffinityVer and invalidates affinity for the cached region.
	region = generateRegionForTest(200, []uint64{4, 5, 6}, ranges[0])
	_, isAffinity = manager.GetAndCacheRegionAffinityGroupState(region)
	re.True(isAffinity)
	groupInfo = getGroupForTest(re, manager, "g")
	re.Equal(1, groupInfo.AffinityRegionCount)
	re.Len(groupInfo.Regions, 2)
	re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{GroupID: "g", KeyRanges: ranges[1:]}}))
	groupInfo = getGroupForTest(re, manager, "g")
	re.Equal(0, groupInfo.AffinityRegionCount)
	re.Empty(groupInfo.Regions)

	// Add key ranges, which bumps AffinityVer and invalidates affinity for the cached region.
	_, isAffinity = manager.GetAndCacheRegionAffinityGroupState(region)
	re.True(isAffinity)
	groupInfo = getGroupForTest(re, manager, "g")
	re.Equal(1, groupInfo.AffinityRegionCount)
	re.Len(groupInfo.Regions, 1)
	re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: "g", KeyRanges: ranges[1:]}}, nil))
	groupInfo = getGroupForTest(re, manager, "g")
	re.Equal(0, groupInfo.AffinityRegionCount)
	re.Len(groupInfo.Regions, 1)
}

func TestSyncOverlappingAffinityRanges(t *testing.T) {
	a := GroupKeyRanges{GroupID: "a", KeyRanges: testKeyRanges("a", "d")}
	b := GroupKeyRanges{GroupID: "b", KeyRanges: testKeyRanges("b", "c")}
	for _, labelFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("label-first=%t", labelFirst), func(t *testing.T) {
			re := require.New(t)
			store := storage.NewStorageWithMemoryBackend()
			manager := newTestAffinityManager(t, store, core.NewStoresInfo())
			region := generateRegionForTest(1, []uint64{1}, b.KeyRanges[0])
			syncRule := func(change GroupKeyRanges) {
				data, err := json.Marshal(MakeLabelRule(change))
				re.NoError(err)
				rule, err := labeler.NewLabelRuleFromJSON(data)
				re.NoError(err)
				re.NoError(manager.SyncKeyRangesFromEtcd(rule))
			}
			syncGroup := func(change GroupKeyRanges) {
				if labelFirst {
					syncRule(change)
				}
				manager.SyncGroupFromEtcd(&Group{ID: change.GroupID, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}})
				if !labelFirst {
					syncRule(change)
				}
				re.Equal(change, manager.keyRanges[change.GroupID])
				re.NoError(manager.regionLabeler.SetLabelRule(MakeLabelRule(change)))
				manager.RestoreAffinityGroup(change.GroupID)
				re.Equal(1, manager.GetAffinityGroupState(change.GroupID).RangeCount)
				re.Empty(manager.labelRuleBuffer)
			}
			// The label watcher may still have a's old rule when the group arrives.
			old := GroupKeyRanges{GroupID: a.GroupID, KeyRanges: testKeyRanges("a", "d", "x", "y")}
			re.NoError(manager.regionLabeler.SetLabelRule(MakeLabelRule(old)))
			syncGroup(a)
			_, matched := manager.GetAndCacheRegionAffinityGroupState(region)
			re.True(matched)

			manager.SetLabelRuleBatchInProgress(true)
			syncGroup(b)
			reconcile := func(allowed bool) {
				manager.ReconcileRangeConflicts()
				for _, id := range []string{a.GroupID, b.GroupID} {
					state := manager.GetAffinityGroupState(id)
					re.Equal(allowed, state.AffinitySchedulingAllowed, id)
					if !allowed {
						re.True(state.RegularSchedulingAllowed)
						re.Zero(state.RegionCount)
						re.Zero(state.AffinityRegionCount)
					}
				}
			}
			reconcile(true) // A group watcher can finish during a label batch.
			group, _ := manager.GetAndCacheRegionAffinityGroupState(region)
			re.Equal(a.GroupID, group.ID)

			manager.SetLabelRuleBatchInProgress(false)
			reconcile(false)
			group, _ = manager.GetAndCacheRegionAffinityGroupState(region)
			re.Nil(group)

			for i, bounds := range [][]keyutil.KeyRange{testKeyRanges("x", "y"), b.KeyRanges} {
				change := GroupKeyRanges{GroupID: b.GroupID, KeyRanges: bounds}
				syncRule(change)
				re.NoError(manager.regionLabeler.SetLabelRule(MakeLabelRule(change)))
				reconcile(i == 0)
			}

			re.NoError(manager.regionLabeler.DeleteLabelRule(GetLabelRuleID(b.GroupID)))
			deleteFirst := func() { manager.SyncGroupDeleteFromEtcd(b.GroupID) }
			deleteSecond := func() { manager.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID(b.GroupID)) }
			if labelFirst {
				deleteFirst, deleteSecond = deleteSecond, deleteFirst
			}
			deleteFirst()
			re.False(manager.GetAffinityGroupState(a.GroupID).AffinitySchedulingAllowed)
			manager.ReconcileRangeConflicts()
			re.True(manager.GetAffinityGroupState(a.GroupID).AffinitySchedulingAllowed)
			deleteSecond()
			re.False(manager.IsGroupExist(b.GroupID))
			re.NotContains(manager.keyRanges, b.GroupID)
			re.Empty(manager.labelRuleBuffer)
			manager.ReconcileRangeConflicts()
			group, matched = manager.GetAndCacheRegionAffinityGroupState(region)
			re.True(matched)
			re.Equal(a.GroupID, group.ID)
		})
	}
}

func seedOverlappingGroupsForTest(t *testing.T) *Manager {
	t.Helper()
	store, stores := storage.NewStorageWithMemoryBackend(), core.NewStoresInfo()
	stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}, core.SetLastHeartbeatTS(time.Now())))
	saveTestAffinityGroups(t, store, []GroupKeyRanges{
		{GroupID: "conflict-a", KeyRanges: testKeyRanges("a", "d")},
		{GroupID: "conflict-b", KeyRanges: testKeyRanges("b", "c")},
	})
	manager := newTestAffinityManager(t, store, stores)
	manager.RestoreAffinityGroup("conflict-a")
	manager.RestoreAffinityGroup("conflict-b")
	return manager
}

func TestReconcileRangeConflictsAllowsLookups(t *testing.T) {
	const hook = "github.com/tikv/pd/pkg/schedule/affinity/beforeRangeConflictScan"
	re := require.New(t)
	manager := seedOverlappingGroupsForTest(t)
	region := generateRegionForTest(1, []uint64{1}, createGroupForTest(re, manager, "unrelated", 1)[0])
	scans := 0
	re.NoError(failpoint.EnableCall(hook, func() {
		scans++
		re.True(manager.TryRLock(), "unrelated Region lookups must not block during a conflict scan")
		manager.RUnlock()
		group, _ := manager.GetRegionAffinityGroupState(region)
		re.Equal("unrelated", group.ID)
	}))
	t.Cleanup(func() { re.NoError(failpoint.Disable(hook)) })

	createGroupForTest(re, manager, "unrelated-create", 1)
	re.Zero(scans, "a validated creation cannot change existing conflicts")
	manager.SyncGroupFromEtcd(&Group{ID: "changed"})
	manager.ReconcileRangeConflicts()
	re.Equal(1, scans)
	re.False(manager.GetAffinityGroupState("conflict-a").AffinitySchedulingAllowed)

	overlap := GroupKeyRanges{GroupID: "conflict-b", KeyRanges: testKeyRanges("b", "c")}
	re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{overlap}))
	re.Equal(2, scans)
	re.True(manager.GetAffinityGroupState("conflict-a").AffinitySchedulingAllowed)

	rule := MakeLabelRule(overlap)
	re.NoError(manager.regionLabeler.SetLabelRule(rule))
	re.NoError(manager.SyncKeyRangesFromEtcd(rule))
	manager.ReconcileRangeConflicts()
	re.NoError(manager.DeleteAffinityGroups([]string{"conflict-b"}, true))
	re.Equal(4, scans)
	re.True(manager.GetAffinityGroupState("conflict-a").AffinitySchedulingAllowed)
}

func TestRangeConflictOperationsConcurrentWithLookups(t *testing.T) {
	re := require.New(t)
	manager := seedOverlappingGroupsForTest(t)
	ranges := createGroupForTest(re, manager, "unrelated", 1)
	_, err := manager.UpdateAffinityGroupPeers("unrelated", 1, []uint64{1})
	re.NoError(err)

	unrelatedRegion := generateRegionForTest(1, []uint64{1}, ranges[0])
	conflictRegion := generateRegionForTest(2, []uint64{1}, testKeyRanges("b", "c")[0])

	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, lookup := range []func(*core.RegionInfo) (*GroupState, bool){
		manager.GetRegionAffinityGroupState, manager.GetAndCacheRegionAffinityGroupState,
	} {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				group, isAffinity := lookup(unrelatedRegion)
				if group == nil || group.ID != "unrelated" || !isAffinity {
					t.Errorf("unexpected unrelated lookup result: group=%v isAffinity=%t", group, isAffinity)
					return
				}
				_, _ = lookup(conflictRegion)
			}
		})
	}
	var stopOnce sync.Once
	stopWorkers := func() {
		stopOnce.Do(func() {
			close(done)
			wg.Wait()
		})
	}
	defer stopWorkers()

	for range 20 {
		const id = "temporary"
		first := createGroupForTest(re, manager, id, 1)
		second := testKeyRanges("x", "y")
		re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: id, KeyRanges: second}}, nil))
		re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{GroupID: id, KeyRanges: first}}))
		re.NoError(manager.DeleteAffinityGroups([]string{id}, true))
	}
	re.NoError(manager.DeleteAffinityGroups([]string{"conflict-b"}, true))

	stopWorkers()
	group, isAffinity := manager.GetRegionAffinityGroupState(conflictRegion)
	re.True(isAffinity)
	re.Equal("conflict-a", group.ID)
}

// TestDeleteGroupClearsCache verifies that deleting a group clears all related region caches.
func TestDeleteGroupClearsCache(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i <= 3; i++ {
		storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i)}, core.SetLastHeartbeatTS(time.Now())))
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a group and add regions
	ranges := createGroupForTest(re, manager, "test-group", 1)
	_, err = manager.UpdateAffinityGroupPeers("test-group", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Create and associate multiple regions
	region1 := generateRegionForTest(100, []uint64{1, 2, 3}, ranges[0])
	region2 := generateRegionForTest(200, []uint64{1, 2, 3}, ranges[0])

	// Trigger cache population
	_, isAffinity1 := manager.GetAndCacheRegionAffinityGroupState(region1)
	re.True(isAffinity1)
	_, isAffinity2 := manager.GetAndCacheRegionAffinityGroupState(region2)
	re.True(isAffinity2)

	// Verify regions are in cache
	groupInfo := getGroupForTest(re, manager, "test-group")
	re.Len(groupInfo.Regions, 2)
	re.Equal(2, groupInfo.AffinityRegionCount)

	// Verify regions in global cache
	manager.RLock()
	_, exists1 := manager.regions[100]
	_, exists2 := manager.regions[200]
	manager.RUnlock()
	re.True(exists1)
	re.True(exists2)

	// Delete the group
	re.Error(manager.DeleteAffinityGroups([]string{"test-group"}, false))
	re.True(manager.IsGroupExist("test-group"))
	re.NoError(manager.DeleteAffinityGroups([]string{"test-group"}, true))
	re.False(manager.IsGroupExist("test-group"))

	// Verify all regions are cleared from global cache
	manager.RLock()
	_, exists1After := manager.regions[100]
	_, exists2After := manager.regions[200]
	globalAffinityCount := manager.affinityRegionCount
	manager.RUnlock()
	re.False(exists1After, "region 100 should be removed from global cache")
	re.False(exists2After, "region 200 should be removed from global cache")
	re.Zero(globalAffinityCount, "global affinity region count should be 0")
}

// TestLookupRacingWithLabelRuleChange verifies that a lookup which reads the region labeler
// before a label rule change does not cache its outdated result.
func TestLookupRacingWithLabelRuleChange(t *testing.T) {
	const lookupHook = "github.com/tikv/pd/pkg/schedule/affinity/afterRegionLabelLookup"
	const groupID = "g"
	oldRange := keyutil.KeyRange{StartKey: keyForTest([]byte("a")), EndKey: keyForTest([]byte("b"))}
	otherRange := keyutil.KeyRange{StartKey: keyForTest([]byte("c")), EndKey: keyForTest([]byte("d"))}
	testCases := []struct {
		name   string
		ranges []keyutil.KeyRange
		change func(re *require.Assertions, m *Manager)
	}{
		{
			name:   "remove key range",
			ranges: []keyutil.KeyRange{oldRange, otherRange},
			change: func(re *require.Assertions, m *Manager) {
				re.NoError(m.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{
					{GroupID: groupID, KeyRanges: []keyutil.KeyRange{oldRange}},
				}))
			},
		},
		{
			name:   "recreate group with another key range",
			ranges: []keyutil.KeyRange{oldRange},
			change: func(re *require.Assertions, m *Manager) {
				re.NoError(m.DeleteAffinityGroups([]string{groupID}, true))
				re.NoError(m.CreateAffinityGroups([]GroupKeyRanges{
					{GroupID: groupID, KeyRanges: []keyutil.KeyRange{otherRange}},
				}))
			},
		},
		{
			// In the scheduling service, the region label watcher updates the labeler and then clears the cache.
			name:   "update region labeler in scheduling service",
			ranges: []keyutil.KeyRange{oldRange, otherRange},
			change: func(re *require.Assertions, m *Manager) {
				re.NoError(m.regionLabeler.SetLabelRule(MakeLabelRule(GroupKeyRanges{
					GroupID: groupID, KeyRanges: []keyutil.KeyRange{otherRange},
				})))
				m.InvalidCacheByLabelRules([]string{GetLabelRuleID(groupID)})
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			ctx := t.Context()
			store := storage.NewStorageWithMemoryBackend()
			storeInfos := core.NewStoresInfo()
			storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1, Address: "s1"}, core.SetLastHeartbeatTS(time.Now())))
			regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
			re.NoError(err)
			manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
			re.NoError(err)
			re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: groupID, KeyRanges: tc.ranges}}))
			_, err = manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1})
			re.NoError(err)
			region := generateRegionForTest(100, []uint64{1}, oldRange)

			changed := false
			// Change the group after the lookup reads the labeler but before it reads the group.
			re.NoError(failpoint.EnableCall(lookupHook, func() {
				if !changed {
					changed = true
					tc.change(re, manager)
				}
			}))
			defer func() { re.NoError(failpoint.Disable(lookupHook)) }()
			manager.GetAndCacheRegionAffinityGroupState(region)
			re.True(changed)

			manager.RLock()
			_, cached := manager.regions[region.GetID()]
			manager.RUnlock()
			re.False(cached, "the outdated lookup result must not be cached")
			group := manager.GetAffinityGroupState(groupID)
			re.Zero(group.RegionCount)
			re.Zero(group.AffinityRegionCount)
			state, _ := manager.GetAndCacheRegionAffinityGroupState(region)
			re.Nil(state, "the region is no longer covered by the group")
		})
	}
}

// TestInvalidCacheByLabelRules verifies that the Region cache and counts are correct after the scheduling
// service applies a label rule change to the Manager and the region labeler, in either order.
func TestInvalidCacheByLabelRules(t *testing.T) {
	const groupID = "g"
	oldRange := keyutil.KeyRange{StartKey: keyForTest([]byte("a")), EndKey: keyForTest([]byte("b"))}
	otherRange := keyutil.KeyRange{StartKey: keyForTest([]byte("c")), EndKey: keyForTest([]byte("d"))}
	testCases := []struct {
		name         string
		managerFirst bool
	}{
		{name: "manager first", managerFirst: true},
		{name: "region labeler first", managerFirst: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			ctx := t.Context()
			store := storage.NewStorageWithMemoryBackend()
			storeInfos := core.NewStoresInfo()
			storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1, Address: "s1"}, core.SetLastHeartbeatTS(time.Now())))
			regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
			re.NoError(err)
			manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
			re.NoError(err)

			// syncManager applies a label rule to the Manager as the affinity label watcher does.
			syncManager := func(ranges ...keyutil.KeyRange) {
				b, err := json.Marshal(MakeLabelRule(GroupKeyRanges{GroupID: groupID, KeyRanges: ranges}))
				re.NoError(err)
				labelRule, err := labeler.NewLabelRuleFromJSON(b)
				re.NoError(err)
				re.NoError(manager.SyncKeyRangesFromEtcd(labelRule))
			}
			// syncRegionLabeler applies a label rule to the region labeler as the region label watcher does.
			syncRegionLabeler := func(ranges ...keyutil.KeyRange) {
				re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(GroupKeyRanges{GroupID: groupID, KeyRanges: ranges})))
				manager.InvalidCacheByLabelRules([]string{GetLabelRuleID(groupID)})
			}
			manager.SyncGroupFromEtcd(&Group{ID: groupID, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}})
			manager.RestoreAffinityGroup(groupID)
			syncRegionLabeler(oldRange, otherRange)
			syncManager(oldRange, otherRange)
			region := generateRegionForTest(100, []uint64{1}, oldRange)
			state, isAffinity := manager.GetAndCacheRegionAffinityGroupState(region)
			re.NotNil(state)
			re.True(isAffinity)
			group := manager.GetAffinityGroupState(groupID)
			re.Equal(1, group.RegionCount)
			re.Equal(1, group.AffinityRegionCount)

			// Remove oldRange.
			if tc.managerFirst {
				syncManager(otherRange)
				state, _ = manager.GetAndCacheRegionAffinityGroupState(region)
				re.NotNil(state, "the region labeler still has the old label rule")
				syncRegionLabeler(otherRange)
			} else {
				syncRegionLabeler(otherRange)
				state, _ = manager.GetAndCacheRegionAffinityGroupState(region)
				re.Nil(state)
				syncManager(otherRange)
			}
			manager.RLock()
			_, cached := manager.regions[region.GetID()]
			manager.RUnlock()
			re.False(cached)
			group = manager.GetAffinityGroupState(groupID)
			re.Zero(group.RegionCount)
			re.Zero(group.AffinityRegionCount)
			state, _ = manager.GetAndCacheRegionAffinityGroupState(region)
			re.Nil(state)
		})
	}
}

// TestAvailabilityChangeRegionCount verifies that changing group availability clears region cache.
func TestAvailabilityChangeRegionCount(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i <= 3; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i), NodeState: metapb.NodeState_Serving})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a group
	ranges := createGroupForTest(re, manager, "availability-test", 1)
	_, err = manager.UpdateAffinityGroupPeers("availability-test", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Add regions to cache
	region := generateRegionForTest(100, []uint64{1, 2, 3}, ranges[0])
	_, isAffinity := manager.GetAndCacheRegionAffinityGroupState(region)
	re.True(isAffinity)

	// Verify region is cached
	groupState1 := manager.GetAffinityGroupState("availability-test")
	re.NotNil(groupState1)
	re.Equal(1, groupState1.RegionCount)
	re.Equal(1, groupState1.AffinityRegionCount)

	// Make store 2 unhealthy to trigger availability change to degraded
	store2 := storeInfos.GetStore(2)
	store2Down := store2.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Hour)))
	storeInfos.PutStore(store2Down)

	// Trigger availability check
	manager.checkGroupsAvailability()

	// Verify group state changed
	groupInfo := getGroupForTest(re, manager, "availability-test")
	re.False(groupInfo.IsAffinitySchedulingAllowed())

	// Verify cache is cleared
	groupState2 := manager.GetAffinityGroupState("availability-test")
	re.NotNil(groupState2)
	testCacheStale(re, manager, region)
	re.Zero(groupState2.AffinityRegionCount, "AffinityRegionCount should be 0 after availability change")

	// Verify global cache is also cleared
	manager.RLock()
	globalAffinityCount := manager.affinityRegionCount
	manager.RUnlock()
	testCacheStale(re, manager, region)
	re.Zero(globalAffinityCount, "global affinity count should be 0")
}

// TestInvalidCacheMultipleTimes verifies that InvalidCache can be called multiple times safely.
func TestInvalidCacheMultipleTimes(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i <= 3; i++ {
		storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i)}, core.SetLastHeartbeatTS(time.Now())))
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a group
	ranges := createGroupForTest(re, manager, "invalid-test", 1)
	_, err = manager.UpdateAffinityGroupPeers("invalid-test", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Add region
	region := generateRegionForTest(100, []uint64{1, 2, 3}, ranges[0])
	_, isAffinity := manager.GetAndCacheRegionAffinityGroupState(region)
	re.True(isAffinity)

	// Verify region is in cache
	groupState := manager.GetAffinityGroupState("invalid-test")
	re.NotNil(groupState)
	re.Equal(1, groupState.RegionCount)
	re.Equal(1, groupState.AffinityRegionCount)

	// Invalidate cache first time
	manager.InvalidCache(100)

	// Verify cache is cleared
	groupState2 := manager.GetAffinityGroupState("invalid-test")
	re.NotNil(groupState2)
	re.Zero(groupState2.RegionCount)
	re.Zero(groupState2.AffinityRegionCount)

	manager.RLock()
	_, exists := manager.regions[100]
	manager.RUnlock()
	re.False(exists)

	// Invalidate cache second time - should not panic or error
	manager.InvalidCache(100)

	// Verify still cleared
	groupState3 := manager.GetAffinityGroupState("invalid-test")
	re.NotNil(groupState3)
	re.Zero(groupState3.RegionCount)
	re.Zero(groupState3.AffinityRegionCount)

	// Invalidate non-existent region - should not panic
	manager.InvalidCache(999)
}

// TestConcurrentOperations verifies concurrent operations don't cause race conditions.
// Run with: go test -race
func TestConcurrentOperations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		re := require.New(t)
		ctx := t.Context()

		store := storage.NewStorageWithMemoryBackend()
		storeInfos := core.NewStoresInfo()
		for i := 1; i <= 5; i++ {
			storeInfo := core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i), NodeState: metapb.NodeState_Serving})
			storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
			storeInfos.PutStore(storeInfo)
		}

		conf := mockconfig.NewTestOptions()
		regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
		re.NoError(err)
		manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
		re.NoError(err)

		// Create initial groups
		groups := make([][]keyutil.KeyRange, 3)
		for i := range 3 {
			groupID := fmt.Sprintf("concurrent-group-%d", i)
			groups[i] = createGroupForTest(re, manager, groupID, 10)
			_, err = manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 3})
			re.NoError(err)
		}

		// Create test regions
		regions := make([]*core.RegionInfo, 10)
		for i := range uint64(10) {
			regions[i] = generateRegionForTest(100+i, []uint64{1, 2, 3}, groups[i%3][i])
		}

		// Run concurrent operations
		var wg sync.WaitGroup
		errChan := make(chan error, 100)

		// Goroutine 1: Read operations
		for i := range 10 {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				for range 50 {
					region := regions[idx%len(regions)]
					_, _ = manager.GetRegionAffinityGroupState(region)
					groupID := fmt.Sprintf("concurrent-group-%d", idx%3)
					_ = manager.GetAffinityGroupState(groupID)
					_ = manager.IsGroupExist(groupID)
				}
			}(i)
		}

		// Goroutine 2: Update peers
		for i := range 3 {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				groupID := fmt.Sprintf("concurrent-group-%d", idx)
				for j := range 10 {
					_, err := manager.UpdateAffinityGroupPeers(groupID, uint64((j%3)+1), []uint64{1, 2, 3})
					if err != nil {
						errChan <- err
						return
					}
					time.Sleep(time.Millisecond)
				}
			}(i)
		}

		// Goroutine 3: InvalidCache operations
		for i := range 5 {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				for j := range 30 {
					regionID := uint64(100 + (idx*2+j)%10)
					manager.InvalidCache(regionID)
					time.Sleep(time.Millisecond)
				}
			}(i)
		}

		// Goroutine 4: Check availability
		wg.Go(func() {
			for range 20 {
				manager.checkGroupsAvailability()
				time.Sleep(2 * time.Millisecond)
			}
		})

		// Wait for all goroutines
		wg.Wait()
		close(errChan)

		// Check for errors
		for err := range errChan {
			re.NoError(err, "concurrent operation failed")
		}

		// Verify final state is consistent
		for i := range 3 {
			groupID := fmt.Sprintf("concurrent-group-%d", i)
			re.True(manager.IsGroupExist(groupID))
			state := manager.GetAffinityGroupState(groupID)
			re.NotNil(state)
		}
	})
}

// TestDegradedExpiration verifies that a degraded group automatically expires after the configured duration.
func TestDegradedExpiration(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := 1; i <= 3; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: uint64(i), Address: fmt.Sprintf("s%d", i), NodeState: metapb.NodeState_Serving})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	// Create a healthy group
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "expiration-test"}}))
	_, err = manager.UpdateAffinityGroupPeers("expiration-test", 1, []uint64{1, 2, 3})
	re.NoError(err)

	// Verify group is available
	groupState := manager.GetAffinityGroupState("expiration-test")
	re.NotNil(groupState)
	re.True(groupState.AffinitySchedulingAllowed)

	// Make store 2 unhealthy to trigger degraded status
	store2 := storeInfos.GetStore(2)
	store2Down := store2.Clone(core.SetLastHeartbeatTS(time.Now().Add(-2 * time.Minute)))
	storeInfos.PutStore(store2Down)
	manager.checkGroupsAvailability()

	// Verify group became degraded
	groupInfo := getGroupForTest(re, manager, "expiration-test")
	re.Equal(groupDegraded, groupInfo.GetAvailability())
	re.False(groupInfo.IsAffinitySchedulingAllowed())

	// Record the expiration time
	manager.RLock()
	expirationTime := groupInfo.degradedExpiredAt
	manager.RUnlock()

	// Verify the expiration time is approximately 10 minutes (600 seconds) from now
	expectedExpiration := uint64(time.Now().Unix()) + defaultDegradedExpirationSeconds
	// Allow 5 seconds tolerance for test execution time
	re.InDelta(expectedExpiration, expirationTime, 5)

	// Simulate time passing beyond expiration
	manager.Lock()
	groupInfo.degradedExpiredAt = uint64(time.Now().Add(-time.Hour).Unix())
	manager.Unlock()

	// Run availability check again
	manager.checkGroupsAvailability()

	// Verify group is now expired
	re.True(groupInfo.IsExpired())
	re.Equal(groupExpired, groupInfo.GetAvailability())

	// Verify scheduling is still disallowed
	groupState2 := manager.GetAffinityGroupState("expiration-test")
	re.NotNil(groupState2)
	re.False(groupState2.AffinitySchedulingAllowed)
}

// TestSyncGroupAndLabelRuleInAnyOrder applies the events of the affinity group
// watcher and the affinity label watcher in every order the two watchers can
// deliver them. The region labeler, which another watcher syncs, still holds
// the old rule. The group must end up with the ranges of the latest rule.
func TestSyncGroupAndLabelRuleInAnyOrder(t *testing.T) {
	const groupID = "g"
	oldRanges := GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}}
	newRanges := GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte("c"), EndKey: []byte("d")}}}
	type event struct {
		name  string
		apply func(re *require.Assertions, m *Manager)
	}
	putGroup := event{"put group", func(_ *require.Assertions, m *Manager) {
		m.SyncGroupFromEtcd(&Group{ID: groupID, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}})
	}}
	deleteGroup := event{"delete group", func(_ *require.Assertions, m *Manager) {
		m.SyncGroupDeleteFromEtcd(groupID)
	}}
	putRule := func(name string, ranges GroupKeyRanges) event {
		return event{"put " + name, func(re *require.Assertions, m *Manager) {
			// The affinity label watcher decodes its own copy of the rule.
			data, err := json.Marshal(MakeLabelRule(ranges))
			re.NoError(err)
			rule, err := labeler.NewLabelRuleFromJSON(data)
			re.NoError(err)
			re.NoError(m.SyncKeyRangesFromEtcd(rule))
		}}
	}
	deleteRule := event{"delete rule", func(_ *require.Assertions, m *Manager) {
		m.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID(groupID))
	}}
	testCases := []struct {
		name string
		// synced is applied by both watchers before the events.
		synced      []event
		groupEvents []event
		labelEvents []event
		// expected is nil if the group must not exist.
		expected *GroupKeyRanges
	}{
		{
			// Removing all ranges of a group deletes its label rule.
			name:        "create group and remove all ranges",
			groupEvents: []event{putGroup},
			labelEvents: []event{putRule("old rule", oldRanges), deleteRule},
			expected:    &GroupKeyRanges{GroupID: groupID},
		},
		{
			// PD deletes the group and its rule in one transaction, then creates
			// both again in another.
			name:        "recreate group with other ranges",
			synced:      []event{putGroup, putRule("old rule", oldRanges)},
			groupEvents: []event{deleteGroup, putGroup},
			labelEvents: []event{deleteRule, putRule("new rule", newRanges)},
			expected:    &newRanges,
		},
		{
			name:        "delete group",
			synced:      []event{putGroup, putRule("old rule", oldRanges)},
			groupEvents: []event{deleteGroup},
			labelEvents: []event{deleteRule},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, events := range interleave(tc.groupEvents, tc.labelEvents) {
				names := make([]string, 0, len(events))
				for _, e := range events {
					names = append(names, e.name)
				}
				t.Run(strings.Join(names, ", "), func(t *testing.T) {
					re := require.New(t)
					store := storage.NewStorageWithMemoryBackend()
					stores := core.NewStoresInfo()
					stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
					regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, time.Second*5)
					re.NoError(err)
					manager, err := NewManager(t.Context(), store, stores, mockconfig.NewTestOptions(), regionLabeler)
					re.NoError(err)
					re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(oldRanges)))
					for _, e := range slices.Concat(tc.synced, events) {
						e.apply(re, manager)
					}

					re.NotContains(manager.labelRuleBuffer, groupID)
					state := manager.GetAffinityGroupState(groupID)
					if tc.expected == nil {
						re.Nil(state)
						re.NotContains(manager.keyRanges, groupID)
						return
					}
					re.NotNil(state)
					re.Equal(tc.expected.KeyRanges, manager.keyRanges[groupID].KeyRanges)
					re.Equal(len(tc.expected.KeyRanges), state.RangeCount)
				})
			}
		})
	}
}

// interleave returns every merge of a and b that keeps the order within each.
func interleave[T any](a, b []T) [][]T {
	if len(a) == 0 || len(b) == 0 {
		return [][]T{slices.Concat(a, b)}
	}
	var result [][]T
	for _, rest := range interleave(a[1:], b) {
		result = append(result, slices.Concat([]T{a[0]}, rest))
	}
	for _, rest := range interleave(a, b[1:]) {
		result = append(result, slices.Concat([]T{b[0]}, rest))
	}
	return result
}

func TestExpireAffinityGroupIf(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for id := uint64(1); id <= 4; id++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: id, Address: fmt.Sprintf("s%d", id), NodeState: metapb.NodeState_Serving})
		storeInfos.PutStore(storeInfo.Clone(core.SetLastHeartbeatTS(time.Now())))
	}
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, 5*time.Second)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)

	const groupID = "conditional-expire"
	createGroupForTest(re, manager, groupID, 1)
	snapshot, err := manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 3})
	re.NoError(err)
	re.True(snapshot.AffinitySchedulingAllowed)

	expired, ok := manager.ExpireAffinityGroupIf(nil)
	re.False(ok)
	re.Nil(expired)

	// A concurrent peer update invalidates the stale snapshot.
	_, err = manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 4})
	re.NoError(err)
	expired, ok = manager.ExpireAffinityGroupIf(snapshot)
	re.False(ok)
	re.Nil(expired)
	current := manager.GetAffinityGroupState(groupID)
	re.True(current.AffinitySchedulingAllowed)
	re.Equal([]uint64{1, 2, 4}, current.VoterStoreIDs)

	// A same-peers write also invalidates a snapshot used to expire and relearn a target.
	manager.SyncGroupFromEtcdWithRevision(&current.Group, 1)
	watched := manager.GetAffinityGroupState(groupID)
	re.Equal(current.affinityVer, watched.affinityVer)
	expired, ok = manager.ExpireAffinityGroupIf(current)
	re.False(ok)
	re.Nil(expired)
	re.Equal(watched, manager.GetAffinityGroupState(groupID))

	// Matching snapshot expires atomically and yields the new version for relearning.
	expired, ok = manager.ExpireAffinityGroupIf(watched)
	re.True(ok)
	re.NotNil(expired)
	re.False(expired.AffinitySchedulingAllowed)
	re.True(expired.RegularSchedulingAllowed)
	peers := []*metapb.Peer{
		{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
		{Id: 12, StoreId: 2, Role: metapb.PeerRole_Voter},
		{Id: 13, StoreId: 3, Role: metapb.PeerRole_Voter},
	}
	region := core.NewRegionInfo(&metapb.Region{Id: 100, Peers: peers}, peers[0])
	manager.checkGroupsAvailability()
	manager.ObserveAvailableRegion(region, expired)
	relearned := manager.GetAffinityGroupState(groupID)
	re.True(relearned.AffinitySchedulingAllowed)
	re.Equal([]uint64{1, 2, 3}, relearned.VoterStoreIDs)

	// Deleting and recreating a group with the same ID must also reject an old snapshot.
	re.NoError(manager.DeleteAffinityGroups([]string{groupID}, true))
	createGroupForTest(re, manager, groupID, 1)
	_, err = manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1, 2, 3})
	re.NoError(err)
	expired, ok = manager.ExpireAffinityGroupIf(relearned)
	re.False(ok)
	re.Nil(expired)
	re.True(manager.GetAffinityGroupState(groupID).AffinitySchedulingAllowed)
}

func TestUpdateAffinityGroupPolicies(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	store := storage.NewStorageWithEtcdBackend(client)
	test := newTargetTestWithStorage(t, t.Context(), store)
	manager := test.Manager

	ranges := createGroupForTest(re, manager, "a", 1)
	createGroupForTest(re, manager, "b", 1)
	manager.checkGroupsAvailability()
	_, err := manager.UpdateAffinityGroupPeers("a", 1, []uint64{1, 2, 3})
	re.NoError(err)
	_, isAffinity := manager.GetAndCacheRegionAffinityGroupState(generateRegionForTest(1, []uint64{1, 2, 3}, ranges[0]))
	re.True(isAffinity)
	before := manager.GetAffinityGroupState("a")

	// Duplicates are written once, and writing the same policy again keeps the stored groups.
	revisions := make(map[string]int64, 2)
	for attempt := range 2 {
		states, err := manager.UpdateAffinityGroupPolicies([]string{"a", "b", "a"}, BalancePolicyFixed)
		re.NoError(err)
		re.Len(states, 2)
		for _, state := range states {
			re.Equal(BalancePolicyFixed, state.BalancePolicy)
			re.Equal(BalancePolicyFixed, test.stored(state.ID).BalancePolicy)
			resp, err := client.Get(t.Context(), keypath.AffinityGroupPath(state.ID))
			re.NoError(err)
			re.Len(resp.Kvs, 1)
			if attempt == 0 {
				revisions[state.ID] = resp.Kvs[0].ModRevision
			} else {
				re.Equal(revisions[state.ID], resp.Kvs[0].ModRevision)
			}
		}
	}
	// The target and the runtime state are kept.
	after := manager.GetAffinityGroupState("a")
	re.Equal(before.LeaderStoreID, after.LeaderStoreID)
	re.Equal(before.VoterStoreIDs, after.VoterStoreIDs)
	re.Equal(before.CreateTimestamp, after.CreateTimestamp)
	re.Equal(PhaseStable, after.Phase)
	re.Equal(before.affinityVer, after.affinityVer)
	re.Equal(before.AffinityRegionCount, after.AffinityRegionCount)
	re.Equal(before.LeaderStoreID, test.stored("a").LeaderStoreID)

	// Invalid or missing requests fail before anything is written.
	_, err = manager.UpdateAffinityGroupPolicies([]string{"a", "missing"}, BalancePolicyAuto)
	re.ErrorIs(err, errs.ErrAffinityGroupNotFound)
	for _, policy := range []BalancePolicy{"", BalancePolicy("manual")} {
		_, err = manager.UpdateAffinityGroupPolicies([]string{"a"}, policy)
		re.ErrorIs(err, errs.ErrAffinityGroupContent)
	}
	_, err = manager.UpdateAffinityGroupPolicies(nil, BalancePolicyAuto)
	re.ErrorIs(err, errs.ErrEmptyRequest)
	_, err = manager.UpdateAffinityGroupPolicies([]string{"bad id"}, BalancePolicyAuto)
	re.ErrorIs(err, errs.ErrInvalidGroupID)
	for _, id := range []string{"a", "b"} {
		re.Equal(BalancePolicyFixed, test.stored(id).BalancePolicy)
		re.Equal(BalancePolicyFixed, manager.GetAffinityGroupState(id).BalancePolicy)
	}
	re.NoError(manager.DeleteAffinityGroups([]string{"a", "b"}, true))

	// More groups than one etcd transaction can hold.
	ids, changes := makeTestGroupBatch(130, "", false)
	re.NoError(manager.CreateAffinityGroups(changes))
	states, err := manager.UpdateAffinityGroupPolicies(ids, BalancePolicyFixed)
	re.NoError(err)
	re.Len(states, len(ids))
	for _, id := range ids {
		re.Equal(BalancePolicyFixed, test.stored(id).BalancePolicy)
	}

	// A failed chunk keeps the chunks written before it.
	remaining := 2
	test.interceptTxns(func(ctx context.Context, f func(kv.Txn) error) error {
		remaining--
		if remaining == 0 {
			return errAffinityTxn
		}
		return store.RunInTxn(ctx, f)
	})
	_, err = manager.UpdateAffinityGroupPolicies(ids, BalancePolicyAuto)
	re.ErrorContains(err, fmt.Sprintf("updated the balance policy of %d of %d affinity groups before the failure", maxAffinityTxnOps, len(ids)))
	for i, id := range ids {
		want := BalancePolicyAuto
		if i >= maxAffinityTxnOps {
			want = BalancePolicyFixed
		}
		re.Equal(want, test.stored(id).BalancePolicy, id)
		re.Equal(want, manager.GetAffinityGroupState(id).BalancePolicy, id)
	}
	// Repeating the request finishes it.
	_, err = manager.UpdateAffinityGroupPolicies(ids, BalancePolicyAuto)
	re.NoError(err)
	for _, id := range ids {
		re.Equal(BalancePolicyAuto, test.stored(id).BalancePolicy, id)
	}
}

func TestCreateAffinityGroupsInheritPolicy(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()
	store := storage.NewStorageWithEtcdBackend(client)
	test := newTargetTestWithStorage(t, ctx, store, "_tidb_ks1_t_1", "_tidb_ks1_t_9")
	manager := test.Manager

	_, err := manager.UpdateAffinityGroupPolicy("_tidb_ks1_t_1", BalancePolicyFixed)
	re.NoError(err)
	// A policy that this version does not know is copied as stored.
	future := &Group{ID: "_tidb_ks1_t_2", CreateTimestamp: 1, BalancePolicy: BalancePolicy("future")}
	re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error { return store.SaveAffinityGroup(txn, future.ID, future) }))
	manager.SyncGroupFromEtcd(future)
	re.Equal(BalancePolicy("future"), manager.GetAffinityGroupState("_tidb_ks1_t_2").BalancePolicy)

	// The batch spans multiple transactions.
	_, batch := makeTestGroupBatch(130, "_tidb_ks1_t_2", true)
	changes := append([]GroupKeyRanges{
		{GroupID: "_tidb_ks1_t_4", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 1004)}, InheritPolicyFrom: "_tidb_ks1_t_1"},
		{GroupID: "_tidb_ks1_t_5", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 1005)}, InheritPolicyFrom: "_tidb_ks1_t_2"},
		{GroupID: "_tidb_ks1_t_6", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 1006)}, InheritPolicyFrom: "_tidb_ks1_t_3"},
		{GroupID: "_tidb_ks1_t_7", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 1007)}},
	}, batch...)
	re.NoError(manager.CreateAffinityGroupsIfNotExists(changes))
	for _, change := range batch {
		re.Equal(BalancePolicy("future"), manager.GetAffinityGroupState(change.GroupID).BalancePolicy)
		re.Equal(BalancePolicy("future"), test.stored(change.GroupID).BalancePolicy)
	}
	for id, want := range map[string]BalancePolicy{
		"_tidb_ks1_t_4": BalancePolicyFixed,
		"_tidb_ks1_t_5": BalancePolicy("future"),
		"_tidb_ks1_t_6": "",
		"_tidb_ks1_t_7": "",
	} {
		re.Equal(want, manager.GetAffinityGroupState(id).BalancePolicy, id)
		re.Equal(want, test.stored(id).BalancePolicy, id)
	}

	// A group that already exists keeps its policy, as a retried request finds it.
	re.NoError(manager.CreateAffinityGroupsIfNotExists([]GroupKeyRanges{{
		GroupID: "_tidb_ks1_t_7", KeyRanges: changes[3].KeyRanges, InheritPolicyFrom: "_tidb_ks1_t_1",
	}}))
	re.Empty(manager.GetAffinityGroupState("_tidb_ks1_t_7").BalancePolicy)
	re.Empty(test.stored("_tidb_ks1_t_7").BalancePolicy)

	// The source must be a valid group ID.
	re.ErrorIs(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "_tidb_ks1_t_8", InheritPolicyFrom: "bad id"}}), errs.ErrInvalidGroupID)
	re.False(manager.IsGroupExist("_tidb_ks1_t_8"))

	// Inheritance reads the committed source policy from storage even after a lost policy reply.
	test.loseNextTxnReply()
	_, err = manager.UpdateAffinityGroupPolicy("_tidb_ks1_t_9", BalancePolicyFixed)
	re.ErrorIs(err, errAffinityTxn)
	re.Equal(BalancePolicyFixed, test.stored("_tidb_ks1_t_9").BalancePolicy)
	re.Empty(manager.GetAffinityGroupState("_tidb_ks1_t_9").BalancePolicy)
	re.NoError(manager.CreateAffinityGroupsIfNotExists([]GroupKeyRanges{{GroupID: "_tidb_ks1_t_10", InheritPolicyFrom: "_tidb_ks1_t_9"}}))
	re.Equal(BalancePolicyFixed, test.stored("_tidb_ks1_t_10").BalancePolicy)
	re.Equal(BalancePolicyFixed, manager.GetAffinityGroupState("_tidb_ks1_t_10").BalancePolicy)
}
