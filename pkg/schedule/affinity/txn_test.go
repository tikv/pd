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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func newTxnTestManager(t *testing.T) *Manager {
	t.Helper()
	store := storage.NewStorageWithMemoryBackend()
	regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, 5*time.Second)
	require.NoError(t, err)
	manager, err := NewManager(t.Context(), store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
	require.NoError(t, err)
	return manager
}

func transactionRangeForTest(keyspaceID uint32, tableID int64) keyutil.KeyRange {
	key := func(id int64) []byte {
		return codec.EncodeBytes(append(codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, keyspaceID), codec.GenerateTableKey(id)...))
	}
	return keyutil.KeyRange{StartKey: key(tableID), EndKey: key(tableID + 1)}
}

func TestNextGenTiDBGroupOperations(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(false)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	// Seed legacy groups to exercise validation before skip-existing shortcuts.
	legacy := []string{"_tidb_t_1001", "_tidb_pt_1001_p3", "_tidb_tg_100"}
	classicKey := func(id int64) []byte {
		return codec.EncodeBytes(codec.GenerateTableKey(id))
	}
	for i, id := range legacy {
		first := int64(i * 4)
		ranges := []keyutil.KeyRange{
			{StartKey: classicKey(first + 1), EndKey: classicKey(first + 2)},
			{StartKey: classicKey(first + 3), EndKey: classicKey(first + 4)},
		}
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id, KeyRanges: ranges}}))
		_, err := manager.UpdateAffinityGroupPeers(id, 1, []uint64{1})
		re.NoError(err)
	}
	orphan := GroupKeyRanges{GroupID: "_tidb_t_9999", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 9999)}}
	re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(orphan)))
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "custom_group", KeyRanges: []keyutil.KeyRange{
		{StartKey: classicKey(13), EndKey: classicKey(14)},
		{StartKey: classicKey(15), EndKey: classicKey(16)},
	}}}))
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	manager, err = NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	snapshot := func() string {
		persisted := make(map[string]string)
		re.NoError(store.LoadAllAffinityGroups(func(k, v string) { persisted[k] = v }))
		data, err := json.Marshal([]any{persisted, manager.keyRanges})
		re.NoError(err)
		return string(data)
	}
	before := snapshot()
	for _, id := range legacy {
		for _, create := range []func([]GroupKeyRanges) error{manager.CreateAffinityGroups, manager.CreateAffinityGroupsIfNotExists} {
			re.ErrorContains(create([]GroupKeyRanges{{GroupID: "new_group"}, {GroupID: id}}), "pd-ctl --keyspace-id")
			re.False(manager.IsGroupExist("new_group"))
		}
		_, err := manager.UpdateAffinityGroupPeers(id, 1, []uint64{1})
		re.ErrorContains(err, "pd-ctl --keyspace-id")
		_, err = manager.CheckAndGetAffinityGroupState(id)
		re.NoError(err)
		states, err := CollectGroupStates(manager, []string{"custom_group", id})
		re.NoError(err)
		re.Contains(states, id)
		for _, ranges := range [][]keyutil.KeyRange{nil, {{StartKey: []byte("a"), EndKey: []byte("b")}}} {
			op := []GroupKeyRanges{{GroupID: id, KeyRanges: ranges}}
			re.ErrorContains(manager.UpdateAffinityGroupKeyRanges(op, nil), "pd-ctl --keyspace-id")
			if len(ranges) == 0 {
				re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, op))
			} else {
				re.Error(manager.UpdateAffinityGroupKeyRanges(nil, op))
			}
		}
		re.Equal(before, snapshot(), "rejected requests must not change persistent groups or ranges")
	}
	for _, id := range append(legacy, "custom_group") {
		group := manager.keyRanges[id]
		re.Len(group.KeyRanges, 2)
		re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{GroupID: id, KeyRanges: group.KeyRanges[:1]}}))
		group.KeyRanges = group.KeyRanges[1:]
		re.Equal(group, manager.keyRanges[id])
		re.Equal(1, manager.GetAffinityGroupState(id).RangeCount)
		ranges, err := extractKeyRangesFromLabelRule(regionLabeler.GetLabelRule(GetLabelRuleID(id)))
		re.NoError(err)
		re.Equal(group, ranges)
	}
	_, err = manager.CheckAndGetAffinityGroupState(orphan.GroupID)
	re.ErrorIs(err, errs.ErrAffinityGroupNotFound)
	re.NoError(manager.DeleteAffinityGroups(append(legacy, orphan.GroupID), true))
	for _, id := range append(legacy, orphan.GroupID) {
		re.Nil(regionLabeler.GetLabelRule(GetLabelRuleID(id)))
	}
	// Identical table IDs remain independent across keyspaces, including both bounds.
	for _, keyspaceID := range []uint32{0, 42, 43, 16777215} {
		id := fmt.Sprintf("_tidb_ks%d_t_1001", keyspaceID)
		group := GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{transactionRangeForTest(keyspaceID, 1001)}}
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{group}))
		_, err := manager.UpdateAffinityGroupPeers(id, 1, []uint64{1})
		re.NoError(err)
		state, err := manager.CheckAndGetAffinityGroupState(id)
		re.NoError(err)
		re.Equal(uint64(1), state.LeaderStoreID)
		op := []GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{transactionRangeForTest(keyspaceID, 1002)}}}
		re.NoError(manager.UpdateAffinityGroupKeyRanges(op, nil))
		re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, op))
	}
	wrongRange := GroupKeyRanges{GroupID: "_tidb_ks42_t_1001", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}}
	before = snapshot()
	re.Error(manager.CreateAffinityGroupsIfNotExists([]GroupKeyRanges{wrongRange}))
	re.Error(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{wrongRange}, nil))
	re.Error(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{wrongRange}))
	re.Equal(before, snapshot())
	re.NoError(manager.DeleteAffinityGroups([]string{"_tidb_ks42_t_1001"}, true))
	for _, id := range []string{"_tidb_ks0_t_1001", "_tidb_ks43_t_1001", "_tidb_ks16777215_t_1001"} {
		state, err := manager.CheckAndGetAffinityGroupState(id)
		re.NoError(err)
		re.Equal(uint64(1), state.LeaderStoreID)
		re.Len(manager.keyRanges[id].KeyRanges, 1)
	}
}

func TestKeyRangeOverlapValidation(t *testing.T) {
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

	// Create manager with region labeler
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	validate := func(ranges []GroupKeyRanges) error {
		manager.Lock()
		defer manager.Unlock()
		return manager.validateNoOverlapLocked(ranges)
	}

	// Test 1: Non-overlapping ranges should succeed
	keyRanges1 := []GroupKeyRanges{
		{GroupID: "group1", KeyRanges: []keyutil.KeyRange{
			{StartKey: keyForTest([]byte("a")), EndKey: keyForTest([]byte("b"))},
			{StartKey: keyForTest([]byte("c")), EndKey: keyForTest([]byte("d"))},
		}},
	}
	err = validate(keyRanges1)
	re.NoError(err, "Non-overlapping ranges should pass validation")

	// Test 2: Overlapping ranges within same request should fail
	keyRanges2 := []GroupKeyRanges{
		{GroupID: "group1", KeyRanges: []keyutil.KeyRange{
			{StartKey: keyForTest([]byte("a")), EndKey: keyForTest([]byte("c"))},
			{StartKey: keyForTest([]byte("b")), EndKey: keyForTest([]byte("d"))},
		}},
	}
	err = validate(keyRanges2)
	re.Error(err, "Overlapping ranges should fail validation")
	re.Contains(err.Error(), "overlap")

	// Test 3: Adjacent ranges (not overlapping) should succeed
	keyRanges3 := []GroupKeyRanges{
		{GroupID: "group1", KeyRanges: []keyutil.KeyRange{
			{StartKey: keyForTest([]byte("a")), EndKey: keyForTest([]byte("b"))},
			{StartKey: keyForTest([]byte("b")), EndKey: keyForTest([]byte("c"))},
		}},
	}
	err = validate(keyRanges3)
	re.NoError(err, "Adjacent ranges should pass validation")

	// Test 5: Duplicate range should fail validation
	keyRangesDup := []GroupKeyRanges{
		{GroupID: "group1", KeyRanges: []keyutil.KeyRange{
			{StartKey: keyForTest([]byte("a")), EndKey: keyForTest([]byte("c"))},
			{StartKey: keyForTest([]byte("a")), EndKey: keyForTest([]byte("c"))},
		}},
	}
	err = validate(keyRangesDup)
	re.Error(err, "Duplicate ranges should fail validation")
	re.Contains(err.Error(), "overlap")

	// Test 4: Verify checkKeyRangesOverlap function directly
	overlaps := checkKeyRangesOverlap(keyForTest([]byte("a")), keyForTest([]byte("c")), keyForTest([]byte("b")), keyForTest([]byte("d")))
	re.True(overlaps, "Ranges [a,c) and [b,d) should overlap")

	overlaps = checkKeyRangesOverlap(keyForTest([]byte("a")), keyForTest([]byte("b")), keyForTest([]byte("c")), keyForTest([]byte("d")))
	re.False(overlaps, "Ranges [a,b) and [c,d) should not overlap")
}

// TestKeyRangeOverlapRebuild tests rebuild after restart
// Note: Full labeler integration test requires proper JSON serialization handling
func TestKeyRangeOverlapRebuild(t *testing.T) {
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

	// Create two groups without key ranges for basic testing
	err = manager.CreateAffinityGroups([]GroupKeyRanges{
		{GroupID: "group1"},
		{GroupID: "group2"},
	})
	re.NoError(err)

	// Set peers for the groups
	_, err = manager.UpdateAffinityGroupPeers("group1", 1, []uint64{1})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers("group2", 1, []uint64{1})
	re.NoError(err)

	// Verify groups were created
	re.True(manager.IsGroupExist("group1"))
	re.True(manager.IsGroupExist("group2"))

	// Create a new manager to simulate restart
	regionLabeler2, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager2, err := NewManager(ctx, store, storeInfos, conf, regionLabeler2)
	re.NoError(err)

	// Verify groups were loaded from storage
	re.True(manager2.IsGroupExist("group1"))
	re.True(manager2.IsGroupExist("group2"))
}

func testKeyRanges(bounds ...string) []keyutil.KeyRange {
	ranges := make([]keyutil.KeyRange, 0, len(bounds)/2)
	for pair := range slices.Chunk(bounds, 2) {
		ranges = append(ranges, keyutil.KeyRange{StartKey: keyForTest([]byte(pair[0])), EndKey: keyForTest([]byte(pair[1]))})
	}
	return ranges
}

func newTestAffinityManager(t *testing.T, store storage.Storage, stores *core.StoresInfo) *Manager {
	t.Helper()
	regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, 5*time.Second)
	require.NoError(t, err)
	manager, err := NewManager(t.Context(), store, stores, mockconfig.NewTestOptions(), regionLabeler)
	require.NoError(t, err)
	return manager
}

// Persist pairs directly to model legacy data that would fail write validation.
func saveTestAffinityGroups(t *testing.T, store storage.Storage, groups []GroupKeyRanges) {
	t.Helper()
	require.NoError(t, store.RunInTxn(t.Context(), func(txn kv.Txn) error {
		for _, group := range groups {
			id := group.GroupID
			require.NoError(t, store.SaveAffinityGroup(txn, id, &Group{ID: id, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}))
			require.NoError(t, store.SaveRegionRule(txn, GetLabelRuleID(id), MakeLabelRule(group)))
		}
		return nil
	}))
}

func TestLoadOverlappingAffinityRanges(t *testing.T) {
	group := func(id string, bounds ...string) GroupKeyRanges {
		return GroupKeyRanges{GroupID: id, KeyRanges: testKeyRanges(bounds...)}
	}
	for _, tc := range []struct {
		name   string
		groups []GroupKeyRanges
	}{
		{"within group", []GroupKeyRanges{group("self", "a", "d", "b", "c")}},
		{"same start", []GroupKeyRanges{group("self", "a", "z", "a", "b")}},
		{"nested ranges", []GroupKeyRanges{group("outer", "a", "g", "m", "n"), group("inner", "b", "c"), group("later", "d", "e")}},
		{"unbounded ranges", []GroupKeyRanges{group("whole", "", ""), group("inner", "b", "c")}},
		{"later unbounded range", []GroupKeyRanges{group("unbounded", "b", ""), group("earlier", "a", "c"), group("later", "d", "e")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			store := storage.NewStorageWithMemoryBackend()
			saveTestAffinityGroups(t, store, tc.groups)
			keys, values, err := store.LoadRange("", "\xff", 0)
			re.NoError(err)
			logFile := testutil.InitTempFileLogger(t, "warn")
			manager := newTestAffinityManager(t, store, core.NewStoresInfo())
			afterKeys, afterValues, err := store.LoadRange("", "\xff", 0)
			re.NoError(err)
			re.Equal(keys, afterKeys)
			re.Equal(values, afterValues, "loading must not repair persisted data")
			logs, err := os.ReadFile(logFile)
			re.NoError(err)
			const warning = "overlapping affinity key ranges"
			re.Equal(1, strings.Count(string(logs), warning))
			re.Regexp(`\[WARN\][^\n]*`+warning, string(logs))
			for _, change := range tc.groups {
				re.Contains(string(logs), change.GroupID)
				manager.RestoreAffinityGroup(change.GroupID)
				state := manager.GetAffinityGroupState(change.GroupID)
				re.Equal(PhasePending, state.Phase)
				re.False(state.AffinitySchedulingAllowed)
				re.True(state.RegularSchedulingAllowed)
				re.Equal(len(change.KeyRanges), state.RangeCount)
				for _, kr := range change.KeyRanges {
					group, _ := manager.GetAndCacheRegionAffinityGroupState(generateRegionForTest(1, []uint64{1}, kr))
					re.Nil(group)
				}
			}
			probe := testKeyRanges("b", "c")
			re.ErrorContains(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "new-overlap", KeyRanges: probe}}), "overlap")
			first := tc.groups[0]
			if len(tc.groups) == 1 {
				re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{GroupID: first.GroupID, KeyRanges: first.KeyRanges[1:]}}))
				re.Equal(first.KeyRanges[:1], manager.keyRanges[first.GroupID].KeyRanges)
				rule := manager.regionLabeler.GetLabelRule(GetLabelRuleID(first.GroupID))
				stored, err := extractKeyRangesFromLabelRule(rule)
				re.NoError(err)
				re.Equal(first.KeyRanges[:1], stored.KeyRanges)
			} else {
				re.NoError(manager.DeleteAffinityGroups([]string{first.GroupID}, true))
			}
			_, isAffinity := manager.GetRegionAffinityGroupState(generateRegionForTest(1, []uint64{1}, probe[0]))
			re.True(isAffinity)
		})
	}
}

func TestParseKeyRangesFromDataInvalidHex(t *testing.T) {
	re := require.New(t)
	_, err := parseKeyRangesFromData([]*labeler.KeyRangeRule{
		{StartKeyHex: "zz", EndKeyHex: "10"},
	}, "g1")
	re.Error(err)
	re.ErrorContains(err, "invalid hex start key")
}

func TestAffinityPersistenceWithLabeler(t *testing.T) {
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

	keyRanges := createGroupForTest(re, manager, "persist", 10)
	_, err = manager.UpdateAffinityGroupPeers("persist", 1, []uint64{1})
	re.NoError(err)

	// RangeCount should be recorded and label rule created.
	state := manager.GetAffinityGroupState("persist")
	re.NotNil(state)
	re.Equal(10, state.RangeCount)
	re.NotNil(regionLabeler.GetLabelRule(GetLabelRuleID("persist")))

	// Reload manager to verify persistence and loadRegionLabel integration.
	manager2, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)
	state2 := manager2.GetAffinityGroupState("persist")
	re.NotNil(state2)
	re.Equal(10, state2.RangeCount)

	// Remove ranges and ensure cache/label are cleared.
	re.NoError(manager2.UpdateAffinityGroupKeyRanges(
		nil,
		[]GroupKeyRanges{{GroupID: "persist", KeyRanges: keyRanges[:3]}},
	))
	state3 := manager2.GetAffinityGroupState("persist")
	re.NotNil(state3)
	re.Equal(7, state3.RangeCount)
	re.NotNil(regionLabeler.GetLabelRule(GetLabelRuleID("persist")))

	re.NoError(manager2.UpdateAffinityGroupKeyRanges(
		nil,
		[]GroupKeyRanges{{GroupID: "persist", KeyRanges: keyRanges[3:]}},
	))
	state3 = manager2.GetAffinityGroupState("persist")
	re.NotNil(state3)
	re.Equal(0, state3.RangeCount)
	re.Nil(regionLabeler.GetLabelRule(GetLabelRuleID("persist")))
}

func TestLoadRegionLabelIgnoreUnknownGroup(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	// Inject leftover label rules for non-existent groups.
	plan := regionLabeler.NewPlan()
	for i, id := range []string{"ghost", "ghost-2"} {
		rule := &labeler.LabelRule{
			ID:       GetLabelRuleID(id),
			Labels:   []labeler.RegionLabel{{Key: AffinityLabelKey, Value: id}},
			RuleType: labeler.KeyRange,
			Data: []any{
				map[string]any{"start_key": hex.EncodeToString(keyForTest([]byte{byte(2*i + 1)})), "end_key": hex.EncodeToString(keyForTest([]byte{byte(2*i + 2)}))},
			},
		}
		re.NoError(plan.SetLabelRule(rule))
	}
	re.NoError(endpoint.RunBatchOpInTxn(ctx, store, plan.CommitOps()))
	plan.Apply()

	// Manager initialization should ignore the unknown label rules, and report
	// them once at the warning level.
	logCore, logs := observer.New(zapcore.WarnLevel)
	restore := log.ReplaceGlobals(zap.New(logCore), nil)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	restore()
	re.NoError(err)
	re.False(manager.IsGroupExist("ghost"))
	warnings := logs.FilterMessageSnippet("found affinity label rules without affinity groups").All()
	re.Len(warnings, 1)
	fields := warnings[0].ContextMap()
	re.Equal(int64(2), fields["count"])
	re.Equal([]any{"ghost", "ghost-2"}, fields["group-ids"])

	rule := regionLabeler.GetLabelRule(GetLabelRuleID("ghost"))
	orphan, err := extractKeyRangesFromLabelRule(rule)
	re.NoError(err)
	for _, id := range []string{"missing", orphan.GroupID} {
		op := []GroupKeyRanges{{GroupID: id, KeyRanges: orphan.KeyRanges}}
		re.ErrorIs(manager.UpdateAffinityGroupKeyRanges(op, nil), errs.ErrAffinityGroupNotFound)
		re.ErrorIs(manager.UpdateAffinityGroupKeyRanges(nil, op), errs.ErrAffinityGroupNotFound)
	}
	re.Empty(manager.keyRanges)
	re.Equal(rule, regionLabeler.GetLabelRule(rule.ID))
	manager.keyRanges[orphan.GroupID] = orphan
	re.ErrorIs(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{orphan}, nil), errs.ErrAffinityGroupNotFound)
	delete(manager.keyRanges, orphan.GroupID)

	// Creating a new group with overlapping range should succeed (ghost is ignored).
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID: "real",
		KeyRanges: []keyutil.KeyRange{{
			StartKey: keyForTest([]byte{0x01}),
			EndKey:   keyForTest([]byte{0x02}),
		}},
	}})
	re.NoError(err)
}

// TestLabelRuleIntegration tests basic label rule creation and deletion
// Note: Full integration with JSON serialization requires additional handling
func TestLabelRuleIntegration(t *testing.T) {
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

	// Test: Group with no key ranges should not create label rule
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "group_no_label"}})
	re.NoError(err)
	_, err = manager.UpdateAffinityGroupPeers("group_no_label", 1, []uint64{1})
	re.NoError(err)

	labelRuleID := GetLabelRuleID("group_no_label")
	labelRule := regionLabeler.GetLabelRule(labelRuleID)
	re.Nil(labelRule, "No label rule should be created when keyRanges is nil")

	// Verify group was created
	re.True(manager.IsGroupExist("group_no_label"))

	// Delete group (no key ranges, so force=false should work)
	err = manager.DeleteAffinityGroups([]string{"group_no_label"}, false)
	re.NoError(err)
	re.False(manager.IsGroupExist("group_no_label"))
}

type failingAffinityStorage struct {
	storage.Storage
	remaining int
}

var errAffinityTxn = errors.New("injected affinity transaction failure")

func (s *failingAffinityStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	s.remaining--
	if s.remaining == 0 {
		return errAffinityTxn
	}
	return s.Storage.RunInTxn(ctx, f)
}

func TestAffinityGroupsBatchFailure(t *testing.T) {
	const limit = etcdutil.MaxEtcdTxnOps
	for _, tc := range []struct {
		name string
		adds int
	}{{"create and delete", 0}, {"update adds", limit + 1}, {"update removes", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			store, stores := storage.NewStorageWithMemoryBackend(), core.NewStoresInfo()
			stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}, core.SetLastHeartbeatTS(time.Now())))
			failing := &failingAffinityStorage{Storage: store}
			manager := newTestAffinityManager(t, failing, stores)
			changes := make([]GroupKeyRanges, limit+3)
			ids := make([]string, len(changes))
			for i := range changes {
				ids[i] = fmt.Sprintf("batch-%04d", i)
				changes[i] = GroupKeyRanges{GroupID: ids[i], KeyRanges: testKeyRanges(ids[i]+"a", ids[i]+"b")}
			}
			verify := func(want []GroupKeyRanges) {
				t.Helper()
				reloaded := newTestAffinityManager(t, store, stores)
				re.Equal(reloaded.keyRanges, manager.keyRanges)
				re.ElementsMatch(reloaded.regionLabeler.GetAllLabelRules(), manager.regionLabeler.GetAllLabelRules())
				for i, id := range ids {
					for _, m := range []*Manager{reloaded, manager} {
						state := m.GetAffinityGroupState(id)
						if want[i].GroupID == "" {
							re.Nil(state, id)
						} else {
							re.Equal(len(want[i].KeyRanges), state.RangeCount, id)
						}
					}
					re.Equal(len(want[i].KeyRanges) > 0, reloaded.regionLabeler.GetLabelRule(GetLabelRuleID(id)) != nil, id)
				}
			}
			if tc.adds == 0 {
				// The first group has no rule, so cutting exactly at the limit
				// would separate the last group in the transaction from its rule.
				changes[0].KeyRanges = nil
				want := slices.Clone(changes)
				probe := []GroupKeyRanges{{GroupID: "overlap", KeyRanges: changes[1].KeyRanges}}
				failing.remaining = 2
				re.ErrorIs(manager.CreateAffinityGroups(changes), errAffinityTxn)
				clear(want[limit/2:])
				verify(want)
				re.ErrorContains(manager.CreateAffinityGroups(probe), "overlap")
				re.ErrorContains(manager.CreateAffinityGroups(changes), "already exists")
				re.NoError(manager.CreateAffinityGroupsIfNotExists(changes))
				failing.remaining = 2
				re.ErrorIs(manager.DeleteAffinityGroups(ids, true), errAffinityTxn)
				clear(want[:limit/2])
				copy(want[limit/2:], changes[limit/2:])
				verify(want)
				re.NoError(manager.CreateAffinityGroups(probe))
				return
			}
			adds := tc.adds
			initial := slices.Clone(changes)
			for i := range initial {
				if i < adds {
					initial[i].KeyRanges = nil
				} else if i%2 == 0 {
					initial[i].KeyRanges = testKeyRanges(ids[i]+"a", ids[i]+"b", ids[i]+"b", ids[i]+"c")
				}
			}
			re.NoError(manager.CreateAffinityGroups(initial))
			last := len(changes) - 1
			for _, i := range []int{adds, last} {
				_, err := manager.UpdateAffinityGroupPeers(ids[i], 1, []uint64{1})
				re.NoError(err)
				_, matched := manager.GetAndCacheRegionAffinityGroupState(generateRegionForTest(uint64(i+1), []uint64{1}, changes[i].KeyRanges[0]))
				re.True(matched)
			}
			failing.remaining = 1
			re.NoError(manager.UpdateAffinityGroupKeyRanges(initial[:1], []GroupKeyRanges{{GroupID: ids[last]}}))
			re.ErrorIs(manager.UpdateAffinityGroupKeyRanges(changes[:adds], changes[adds:]), errAffinityTxn)
			verify(initial)
			failing.remaining = 2
			re.ErrorIs(manager.UpdateAffinityGroupKeyRanges(changes[:adds], changes[adds:]), errAffinityTxn)
			copy(initial[:min(limit, adds)], changes)
			for i := min(limit, adds); i < limit; i++ {
				initial[i].KeyRanges = initial[i].KeyRanges[1:]
			}
			verify(initial)
			if adds < limit {
				re.Zero(manager.GetAffinityGroupState(ids[adds]).RegionCount)
			}
			re.Equal(1, manager.GetAffinityGroupState(ids[last]).RegionCount)
			re.ErrorContains(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "overlap", KeyRanges: changes[0].KeyRanges}}), "overlap")
		})
	}
}

func TestCreateAffinityGroupsDuplicateIDs(t *testing.T) {
	re := require.New(t)
	manager := newTxnTestManager(t)
	group := GroupKeyRanges{GroupID: "duplicate", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 1)}}
	// Put the duplicate beyond the first transaction to catch partial creation.
	batch := []GroupKeyRanges{group}
	for i := range etcdutil.MaxEtcdTxnOps {
		batch = append(batch, GroupKeyRanges{GroupID: fmt.Sprintf("padding_%d", i)})
	}
	batch = append(batch, GroupKeyRanges{GroupID: group.GroupID})
	assertRejected := func(want int) {
		for _, create := range []func([]GroupKeyRanges) error{manager.CreateAffinityGroups, manager.CreateAffinityGroupsIfNotExists} {
			re.ErrorIs(create(batch), errs.ErrAffinityGroupContent)
			var ids []string
			re.NoError(manager.storage.LoadAllAffinityGroups(func(id, _ string) { ids = append(ids, id) }))
			re.Len(ids, want)
			re.Equal(want, manager.GetAffinityGroupCount())
		}
	}
	assertRejected(0)
	re.Empty(manager.keyRanges)
	re.Nil(manager.regionLabeler.GetLabelRule(GetLabelRuleID(group.GroupID)))
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{group}))
	assertRejected(1)
	re.Equal(group, manager.keyRanges[group.GroupID])
}

func TestUpdateAffinityGroupKeyRangesEmptyOperations(t *testing.T) {
	re := require.New(t)
	manager := newTxnTestManager(t)
	id := "group"
	empty := GroupKeyRanges{GroupID: id}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{empty}))
	op := GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 1)}}
	for _, ops := range [][]GroupKeyRanges{{empty, op}, {op, empty}} {
		re.NoError(manager.UpdateAffinityGroupKeyRanges(ops, []GroupKeyRanges{{GroupID: "missing"}}))
		re.Equal(op, manager.keyRanges[id])
		other := GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 2)}}
		for _, duplicate := range []struct{ add, remove []GroupKeyRanges }{
			{add: []GroupKeyRanges{other, other}},
			{remove: []GroupKeyRanges{op, op}},
			{add: []GroupKeyRanges{other}, remove: []GroupKeyRanges{op}},
		} {
			re.ErrorIs(manager.UpdateAffinityGroupKeyRanges(duplicate.add, duplicate.remove), errs.ErrAffinityGroupExist)
			re.Equal(op, manager.keyRanges[id])
		}
		re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{empty}, []GroupKeyRanges{empty, op}))
		re.Zero(manager.GetAffinityGroupState(id).RangeCount)
		re.Nil(manager.regionLabeler.GetLabelRule(GetLabelRuleID(id)))
	}
	re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{empty, empty, {GroupID: "missing"}}, nil))
}

func TestNextGenCustomGroupMergedRanges(t *testing.T) {
	re := require.New(t)
	manager := newTxnTestManager(t)
	first, other := transactionRangeForTest(1, 1), transactionRangeForTest(2, 1)
	group := GroupKeyRanges{GroupID: "custom", KeyRanges: []keyutil.KeyRange{first}}
	legacy := GroupKeyRanges{GroupID: "legacy", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(3, 1), other}}
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(false)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{group, legacy}))
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	add := GroupKeyRanges{GroupID: group.GroupID, KeyRanges: []keyutil.KeyRange{transactionRangeForTest(4, 1)}}
	re.NoError(validateGroupKeyRanges(add))
	re.ErrorIs(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{add}, nil), errs.ErrAffinityGroupContent)
	re.Equal(group, manager.keyRanges[group.GroupID])
	legacyAdd := GroupKeyRanges{GroupID: legacy.GroupID, KeyRanges: []keyutil.KeyRange{transactionRangeForTest(3, 2)}}
	re.ErrorIs(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{legacyAdd}, nil), errs.ErrAffinityGroupContent)
	re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{GroupID: legacy.GroupID, KeyRanges: legacy.KeyRanges[1:]}}))
	re.Equal(legacy.KeyRanges[:1], manager.keyRanges[legacy.GroupID].KeyRanges)
}

type rangeOrderStorage struct {
	storage.Storage
	operations []string
	failSaveID string
}

func (s *rangeOrderStorage) SaveRegionRule(txn kv.Txn, id string, rule any) error {
	s.operations = append(s.operations, id)
	if id == s.failSaveID {
		return fmt.Errorf("injected range storage failure")
	}
	return s.Storage.SaveRegionRule(txn, id, rule)
}

func (s *rangeOrderStorage) DeleteRegionRule(txn kv.Txn, id string) error {
	s.operations = append(s.operations, id)
	return s.Storage.DeleteRegionRule(txn, id)
}

func TestUpdateAffinityGroupKeyRangesPartialCommit(t *testing.T) {
	re := require.New(t)
	store := &rangeOrderStorage{Storage: storage.NewStorageWithMemoryBackend()}
	regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, 5*time.Second)
	re.NoError(err)
	manager, err := NewManager(t.Context(), store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)

	// Empty operations consume no transaction slots. The first transaction
	// commits every nonempty add and the first two nonempty removes.
	const addCount = etcdutil.MaxEtcdTxnOps - 2
	add := make([]GroupKeyRanges, 1, addCount+1)
	add[0].GroupID = "empty-add"
	initial := make([]GroupKeyRanges, 0, addCount+4)
	expected := make(map[string]GroupKeyRanges, addCount+3)
	for i := range addCount {
		id := fmt.Sprintf("add-%03d", addCount-i)
		op := GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, int64(i+1))}}
		initial = append(initial, GroupKeyRanges{GroupID: id})
		add = append(add, op)
		expected[id] = op
	}
	remove := []GroupKeyRanges{{GroupID: "empty-remove"}}
	for i, tc := range []struct {
		id        string
		removeAll bool
		committed bool
	}{
		{id: "z-remove", committed: true},
		{id: "a-remove", removeAll: true, committed: true},
		{id: "m-remove", removeAll: true},
		{id: "b-remove"},
	} {
		tableID := int64(1000 + 2*i)
		removed := transactionRangeForTest(1, tableID)
		ranges := []keyutil.KeyRange{removed}
		if !tc.removeAll {
			ranges = append(ranges, transactionRangeForTest(1, tableID+1))
		}
		initial = append(initial, GroupKeyRanges{GroupID: tc.id, KeyRanges: ranges})
		remove = append(remove, GroupKeyRanges{GroupID: tc.id, KeyRanges: []keyutil.KeyRange{removed}})
		if tc.committed {
			ranges = ranges[1:]
		}
		if len(ranges) > 0 {
			expected[tc.id] = GroupKeyRanges{GroupID: tc.id, KeyRanges: ranges}
		}
	}
	re.NoError(manager.CreateAffinityGroups(initial))
	store.operations = nil
	// Fail the second transaction after it has staged its first removal.
	store.failSaveID = GetLabelRuleID("b-remove")
	re.ErrorContains(manager.UpdateAffinityGroupKeyRanges(add, remove), "injected range storage failure")

	var order []string
	for _, ops := range [][]GroupKeyRanges{add[1:], remove[1:]} {
		for _, op := range ops {
			order = append(order, GetLabelRuleID(op.GroupID))
		}
	}
	re.Equal(order, store.operations, "storage must follow request order, with all adds before removes")

	reloadedLabeler, err := labeler.NewRegionLabeler(t.Context(), store, 5*time.Second)
	re.NoError(err)
	reloaded, err := NewManager(t.Context(), store, core.NewStoresInfo(), mockconfig.NewTestOptions(), reloadedLabeler)
	re.NoError(err)
	re.Equal(expected, reloaded.keyRanges, "storage should contain only the committed updates")
	re.Equal(expected, manager.keyRanges, "cached key ranges should match storage")
	for _, group := range initial {
		id := group.GroupID
		re.Equal(len(expected[id].KeyRanges), manager.GetAffinityGroupState(id).RangeCount, id)
		ruleID := GetLabelRuleID(id)
		re.Equal(reloadedLabeler.GetLabelRule(ruleID), regionLabeler.GetLabelRule(ruleID), id)
	}
}

func TestDuplicateRangeAdd(t *testing.T) {
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

	r := keyutil.KeyRange{StartKey: keyForTest([]byte{0x00}), EndKey: keyForTest([]byte{0x10})}

	// First add.
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "dup",
		KeyRanges: []keyutil.KeyRange{r}}}))
	// Second add with same range should be rejected due to duplicate/overlap.
	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{GroupID: "dup", KeyRanges: []keyutil.KeyRange{r}}},
		nil,
	)
	re.Error(err)
}

// TestDeleteAffinityGroupsForceMissing verifies force deletion tolerates missing IDs.
func TestDeleteAffinityGroupsForceMissing(t *testing.T) {
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

	// Create a group with a key range.
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID: "with-range",
		KeyRanges: []keyutil.KeyRange{{
			StartKey: keyForTest([]byte{0x00}),
			EndKey:   keyForTest([]byte{0x10}),
		}},
	}}))

	// Force delete should tolerate missing IDs and remove existing groups with ranges.
	err = manager.DeleteAffinityGroups([]string{"missing-group", "with-range"}, true)
	re.NoError(err)
	re.False(manager.IsGroupExist("with-range"))
}

// TestSameGroupNonOverlappingAdd ensures adding disjoint ranges to the same group is allowed.
func TestSameGroupNonOverlappingAdd(t *testing.T) {
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

	r1 := keyutil.KeyRange{StartKey: keyForTest([]byte{0x00}), EndKey: keyForTest([]byte{0x10})}
	r2 := keyutil.KeyRange{StartKey: keyForTest([]byte{0x20}), EndKey: keyForTest([]byte{0x30})}

	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{r1}}}))

	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{r2}}},
		nil,
	)
	re.NoError(err)

	state := manager.GetAffinityGroupState("g")
	re.NotNil(state)
	re.Equal(2, state.RangeCount)
}

// TestOverlapDuringMigration documents that add+remove with overlapping ranges is rejected.
// Scenario: group1 has [0x00,0x10], we try to add group2 [0x00,0x10] while removing group1 [0x00,0x10].
// Current logic reports overlap (expected current design).
func TestOverlapDuringMigration(t *testing.T) {
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

	r := keyutil.KeyRange{StartKey: keyForTest([]byte{0x00}), EndKey: keyForTest([]byte{0x10})}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g1", KeyRanges: []keyutil.KeyRange{r}}}))

	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{GroupID: "g2", KeyRanges: []keyutil.KeyRange{r}}},
		[]GroupKeyRanges{{GroupID: "g1", KeyRanges: []keyutil.KeyRange{r}}},
	)
	re.Error(err)
}

// TestPartialOverlapSameGroup documents that add+remove in the same request for same group is rejected.
// Scenario: existing [0,10), request removes [0,10) and adds [0,5) for the same group. Current design forbids mixed add/remove same group.
func TestPartialOverlapSameGroup(t *testing.T) {
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

	rOld := keyutil.KeyRange{StartKey: keyForTest([]byte{0x00}), EndKey: keyForTest([]byte{0x10})}
	rNew := keyutil.KeyRange{StartKey: keyForTest([]byte{0x00}), EndKey: keyForTest([]byte{0x05})}

	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{rOld}}}))

	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{rNew}}},
		[]GroupKeyRanges{{GroupID: "g", KeyRanges: []keyutil.KeyRange{rOld}}},
	)
	re.Error(err)
}

// TestNewGroupOverlapWithExistingGroup verifies that adding a new group's ranges
// that overlap with an existing group's ranges is correctly rejected.
// This test validates the fix for the regression where group A's new ranges
// vs group B's existing ranges were not being checked.
func TestNewGroupOverlapWithExistingGroup(t *testing.T) {
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

	// Create group A with range [0x00, 0x20]
	rangeA := keyutil.KeyRange{StartKey: keyForTest([]byte{0x00}), EndKey: keyForTest([]byte{0x20})}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID:   "groupA",
		KeyRanges: []keyutil.KeyRange{rangeA},
	}}))

	// Try to create group B with overlapping range [0x10, 0x30]
	// This should be rejected because it overlaps with group A's existing range
	rangeB := keyutil.KeyRange{StartKey: keyForTest([]byte{0x10}), EndKey: keyForTest([]byte{0x30})}
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID:   "groupB",
		KeyRanges: []keyutil.KeyRange{rangeB},
	}})
	re.Error(err, "New group B's range should be rejected due to overlap with existing group A")
	re.Contains(err.Error(), "overlap")

	// Verify group B was not created
	re.False(manager.IsGroupExist("groupB"))

	// Create group C without ranges first
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "groupC"}}))

	// Try to add range to group C that overlaps with group A
	// This should also be rejected
	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{
			GroupID:   "groupC",
			KeyRanges: []keyutil.KeyRange{rangeB},
		}},
		nil,
	)
	re.Error(err, "Adding overlapping range to group C should be rejected")
	re.Contains(err.Error(), "overlap")

	// Now test the critical case: batch update with multiple groups
	// Create group D with range [0x40, 0x50]
	rangeD := keyutil.KeyRange{StartKey: keyForTest([]byte{0x40}), EndKey: keyForTest([]byte{0x50})}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID:   "groupD",
		KeyRanges: []keyutil.KeyRange{rangeD},
	}}))

	// Create group E without ranges
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "groupE"}}))

	// Try to add ranges to both group C and group E in a single batch,
	// where group C's new range [0x45, 0x55] overlaps with group D's existing range [0x40, 0x50]
	rangeC := keyutil.KeyRange{StartKey: keyForTest([]byte{0x45}), EndKey: keyForTest([]byte{0x55})} // overlaps with D
	rangeE := keyutil.KeyRange{StartKey: keyForTest([]byte{0x60}), EndKey: keyForTest([]byte{0x70})} // no overlap
	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{
			{GroupID: "groupC", KeyRanges: []keyutil.KeyRange{rangeC}},
			{GroupID: "groupE", KeyRanges: []keyutil.KeyRange{rangeE}},
		},
		nil,
	)
	// This is the key test: C's new range vs D's existing range should be detected
	// This validates that when updating multiple groups in a batch,
	// the validation correctly checks new ranges against ALL existing ranges,
	// not just the groups being updated
	re.Error(err, "Batch update should detect overlap between C's new range and D's existing range")
	re.Contains(err.Error(), "overlap")
}
