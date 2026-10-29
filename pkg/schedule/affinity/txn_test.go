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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

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
	legacy := []string{"_tidb_t_1001", "_tidb_pt_1001_p3"}
	for _, id := range legacy {
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id}}))
		_, err := manager.UpdateAffinityGroupPeers(id, 1, []uint64{1})
		re.NoError(err)
		_, err = manager.CheckAndGetAffinityGroupState(id)
		re.NoError(err)
		op := []GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}}}
		re.NoError(manager.UpdateAffinityGroupKeyRanges(op, nil))
		re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, op))
		re.NoError(manager.DeleteAffinityGroups([]string{id}, true))
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id}}))
	}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "custom_group"}}))
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
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
		for _, force := range []bool{false, true} {
			re.ErrorContains(manager.DeleteAffinityGroups([]string{"custom_group", id}, force), "pd-ctl --keyspace-id")
			re.ErrorContains(manager.DeleteAffinityGroups([]string{id + "_missing"}, force), "pd-ctl --keyspace-id")
		}
		_, err := manager.UpdateAffinityGroupPeers(id, 1, []uint64{1})
		re.ErrorContains(err, "pd-ctl --keyspace-id")
		_, err = manager.CheckAndGetAffinityGroupState(id)
		re.ErrorContains(err, "pd-ctl --keyspace-id")
		_, err = CollectGroupStates(manager, []string{"custom_group", id})
		re.ErrorContains(err, "pd-ctl --keyspace-id")
		for _, ranges := range [][]keyutil.KeyRange{nil, {{StartKey: []byte("a"), EndKey: []byte("b")}}} {
			op := []GroupKeyRanges{{GroupID: id, KeyRanges: ranges}}
			re.ErrorContains(manager.UpdateAffinityGroupKeyRanges(op, nil), "pd-ctl --keyspace-id")
			re.ErrorContains(manager.UpdateAffinityGroupKeyRanges(nil, op), "pd-ctl --keyspace-id")
		}
		re.Equal(before, snapshot(), "rejected requests must not change persistent groups or ranges")
	}
	// Identical table IDs remain independent across keyspaces, including both bounds.
	for _, keyspaceID := range []uint32{0, 42, 43, 16777215} {
		id := fmt.Sprintf("_tidb_ks%d_t_1001", keyspaceID)
		rangeForKeyspace := func(tableID int64) keyutil.KeyRange {
			prefix := codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, keyspaceID)
			return keyutil.KeyRange{
				StartKey: codec.EncodeBytes(append(prefix, codec.GenerateTableKey(tableID)...)),
				EndKey:   codec.EncodeBytes(append(prefix, codec.GenerateTableKey(tableID+1)...)),
			}
		}
		group := GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{rangeForKeyspace(1001)}}
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{group}))
		_, err := manager.UpdateAffinityGroupPeers(id, 1, []uint64{1})
		re.NoError(err)
		state, err := manager.CheckAndGetAffinityGroupState(id)
		re.NoError(err)
		re.Equal(uint64(1), state.LeaderStoreID)
		op := []GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{rangeForKeyspace(1002)}}}
		re.NoError(manager.UpdateAffinityGroupKeyRanges(op, nil))
		re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, op))
		wrongRange := GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}}
		before := snapshot()
		re.Error(manager.CreateAffinityGroupsIfNotExists([]GroupKeyRanges{wrongRange}))
		re.Error(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{wrongRange}, nil))
		re.Error(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{wrongRange}))
		re.Equal(before, snapshot())
	}
	re.NoError(manager.DeleteAffinityGroups([]string{"_tidb_ks42_t_1001"}, true))
	for _, id := range []string{"_tidb_ks0_t_1001", "_tidb_ks43_t_1001", "_tidb_ks16777215_t_1001"} {
		state, err := manager.CheckAndGetAffinityGroupState(id)
		re.NoError(err)
		re.Equal(uint64(1), state.LeaderStoreID)
		re.Len(manager.keyRanges[id].KeyRanges, 1)
	}
}

func TestNextGenSkipsLegacyGroupsDuringLoadAndSync(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)

	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	re.NoError(failpoint.Enable(modeFlag, "return(false)"))
	classicManager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	re.NoError(classicManager.CreateAffinityGroups([]GroupKeyRanges{
		{GroupID: "_tidb_t_1001", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}},
		{GroupID: "_tidb_pt_1001_p3", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("c"), EndKey: []byte("d")}}},
		{GroupID: "_tidb_ks42_t_1001"},
	}))
	re.Len(classicManager.GetAllAffinityGroupStates(), 3)
	before := make(map[string]string)
	re.NoError(store.LoadAllAffinityGroups(func(k, v string) { before[k] = v }))

	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	nextGenManager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	re.False(nextGenManager.IsGroupExist("_tidb_t_1001"))
	re.False(nextGenManager.IsGroupExist("_tidb_pt_1001_p3"))
	re.True(nextGenManager.IsGroupExist("_tidb_ks42_t_1001"))
	re.Len(nextGenManager.GetAllAffinityGroupStates(), 1)
	re.Empty(nextGenManager.keyRanges, "legacy groups' ranges must not be loaded")

	for _, id := range []string{"_tidb_t_1001", "_tidb_pt_1001_p3", "_tidb_ks042_t_1001"} {
		nextGenManager.SyncGroupFromEtcd(&Group{ID: id})
		re.False(nextGenManager.IsGroupExist(id))
		re.NoError(nextGenManager.SyncKeyRangesFromEtcd(MakeLabelRule(GroupKeyRanges{
			GroupID:   id,
			KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}},
		})))
		re.NotContains(nextGenManager.labelRuleBuffer, id)
	}

	// Valid keyspace-aware labels may arrive before their groups and must still attach.
	lower := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 44)
	upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 45)
	validRanges := GroupKeyRanges{GroupID: "_tidb_ks44_t_1001", KeyRanges: []keyutil.KeyRange{{StartKey: lower[:], EndKey: upper[:]}}}
	data, err := json.Marshal(MakeLabelRule(validRanges))
	re.NoError(err)
	rule, err := labeler.NewLabelRuleFromJSON(data)
	re.NoError(err)
	re.NoError(nextGenManager.SyncKeyRangesFromEtcd(rule))
	re.Contains(nextGenManager.labelRuleBuffer, validRanges.GroupID)
	nextGenManager.SyncGroupFromEtcd(&Group{ID: validRanges.GroupID})
	re.Equal(validRanges, nextGenManager.keyRanges[validRanges.GroupID])
	re.Equal(1, nextGenManager.GetAffinityGroupState(validRanges.GroupID).RangeCount)

	after := make(map[string]string)
	re.NoError(store.LoadAllAffinityGroups(func(k, v string) { after[k] = v }))
	re.Equal(before, after, "skipping invalid groups must not delete persistent metadata")
}

func testKeyRangeForKeyspace(keyspaceID uint32) keyutil.KeyRange {
	prefix := codec.MakeKeyspacePrefix(codec.TxnKeyspaceModePrefix, keyspaceID)
	return keyutil.KeyRange{
		StartKey: codec.EncodeBytes(append(prefix, codec.GenerateTableKey(1001)...)),
		EndKey:   codec.EncodeBytes(append(prefix, codec.GenerateTableKey(1002)...)),
	}
}

func TestNextGenRejectsMismatchedAffinityRangesOnLoadAndSync(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	const groupID = "_tidb_ks42_t_1001"
	wrongRanges := GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{testKeyRangeForKeyspace(42), testKeyRangeForKeyspace(43)}}

	t.Run("load", func(t *testing.T) {
		re := require.New(t)
		store := storage.NewStorageWithMemoryBackend()
		storeInfos := core.NewStoresInfo()
		storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
		regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
		re.NoError(err)
		re.NoError(failpoint.Enable(modeFlag, "return(false)"))
		classicManager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
		re.NoError(err)
		re.NoError(classicManager.CreateAffinityGroups([]GroupKeyRanges{wrongRanges}))
		invalidRule := MakeLabelRule(wrongRanges)
		invalidRule.Index = 5
		re.NoError(regionLabeler.SetLabelRule(invalidRule))
		_, err = classicManager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1})
		re.NoError(err)
		region := generateRegionForTest(21, []uint64{1}, wrongRanges.KeyRanges[0])
		state, isAffinity := classicManager.GetRegionAffinityGroupState(region)
		re.NotNil(state)
		re.Equal(groupID, state.ID)
		re.True(isAffinity, "Classic mode keeps its existing unscoped behavior")

		re.NoError(failpoint.Enable(modeFlag, "return(true)"))
		manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
		re.NoError(err)
		re.True(manager.IsGroupExist(groupID))
		re.Empty(manager.keyRanges)
		state, isAffinity = manager.GetRegionAffinityGroupState(region)
		re.Nil(state, "a rule with any invalid range must be inactive in every keyspace")
		re.False(isAffinity)
		re.NoError(manager.SyncKeyRangesFromEtcd(regionLabeler.GetLabelRule(GetLabelRuleID(groupID))))
		re.Empty(manager.keyRanges)

		// A valid ks42 group can reuse the valid portion of the skipped rule.
		otherRanges := GroupKeyRanges{GroupID: "_tidb_ks42_t_1002", KeyRanges: wrongRanges.KeyRanges[:1]}
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{otherRanges}))
		_, err = manager.UpdateAffinityGroupPeers(otherRanges.GroupID, 1, []uint64{1})
		re.NoError(err)
		state, isAffinity = manager.GetRegionAffinityGroupState(region)
		re.NotNil(state)
		re.Equal(otherRanges.GroupID, state.ID, "the invalid higher-index rule must not shadow a valid group")
		re.True(isAffinity)
		otherState := manager.GetAffinityGroupState(otherRanges.GroupID)
		persistedRules := make(map[string]string)
		re.NoError(store.LoadRegionRules(func(k, v string) { persistedRules[k] = v }))
		recoveredRule := regionLabeler.GetLabelRule(GetLabelRuleID(groupID))
		validOp := []GroupKeyRanges{{GroupID: groupID, KeyRanges: []keyutil.KeyRange{testKeyRangeForKeyspace(42)}}}
		for _, tc := range []struct {
			name        string
			add, remove []GroupKeyRanges
		}{
			{name: "add", add: validOp},
			{name: "remove", remove: validOp},
		} {
			t.Run(tc.name, func(t *testing.T) {
				re := require.New(t)
				re.ErrorContains(manager.UpdateAffinityGroupKeyRanges(tc.add, tc.remove), "must belong to transaction keyspace 42")
				re.NotContains(manager.keyRanges, groupID, "updates must not restore skipped ranges")
				re.Equal(0, manager.GetAffinityGroupState(groupID).RangeCount)
				re.Equal(recoveredRule, regionLabeler.GetLabelRule(GetLabelRuleID(groupID)))
				re.Equal(otherRanges, manager.keyRanges[otherRanges.GroupID])
				re.Equal(otherState, manager.GetAffinityGroupState(otherRanges.GroupID))
				afterRules := make(map[string]string)
				re.NoError(store.LoadRegionRules(func(k, v string) { afterRules[k] = v }))
				re.Equal(persistedRules, afterRules, "rejected updates must not change persisted label rules")
			})
		}
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{
			{GroupID: "_tidb_ks44_t_1001", KeyRanges: []keyutil.KeyRange{testKeyRangeForKeyspace(44)}},
		}), "rejected updates must not block unrelated groups")
	})

	t.Run("watcher label first", func(t *testing.T) {
		re := require.New(t)
		store := storage.NewStorageWithMemoryBackend()
		storeInfos := core.NewStoresInfo()
		regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
		re.NoError(err)
		re.NoError(failpoint.Enable(modeFlag, "return(true)"))
		manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
		re.NoError(err)
		re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(wrongRanges)))
		rule := regionLabeler.GetLabelRule(GetLabelRuleID(groupID))
		re.NoError(manager.SyncKeyRangesFromEtcd(rule))
		re.NotContains(manager.labelRuleBuffer, groupID)
		manager.SyncGroupFromEtcd(&Group{ID: groupID})
		re.True(manager.IsGroupExist(groupID))
		re.NotContains(manager.keyRanges, groupID)
		re.Equal(0, manager.GetAffinityGroupState(groupID).RangeCount)
		region := generateRegionForTest(22, []uint64{1}, wrongRanges.KeyRanges[0])
		state, isAffinity := manager.GetRegionAffinityGroupState(region)
		re.Nil(state)
		re.False(isAffinity)
	})

	t.Run("watcher group first", func(t *testing.T) {
		re := require.New(t)
		store := storage.NewStorageWithMemoryBackend()
		regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
		re.NoError(err)
		re.NoError(failpoint.Enable(modeFlag, "return(true)"))
		manager, err := NewManager(ctx, store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
		re.NoError(err)
		manager.SyncGroupFromEtcd(&Group{ID: groupID})
		re.True(manager.IsGroupExist(groupID))
		re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(wrongRanges)))
		re.NoError(manager.SyncKeyRangesFromEtcd(regionLabeler.GetLabelRule(GetLabelRuleID(groupID))))
		re.NotContains(manager.keyRanges, groupID)
		re.Equal(0, manager.GetAffinityGroupState(groupID).RangeCount)
	})

	t.Run("group attach with existing invalid label", func(t *testing.T) {
		re := require.New(t)
		store := storage.NewStorageWithMemoryBackend()
		regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
		re.NoError(err)
		re.NoError(failpoint.Enable(modeFlag, "return(true)"))
		manager, err := NewManager(ctx, store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
		re.NoError(err)
		re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(wrongRanges)))
		manager.SyncGroupFromEtcd(&Group{ID: groupID})
		re.True(manager.IsGroupExist(groupID))
		re.NotContains(manager.keyRanges, groupID)
		re.Equal(0, manager.GetAffinityGroupState(groupID).RangeCount)
	})
}

func TestNextGenRejectsMismatchedRuntimeAffinityLabel(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	const groupID = "_tidb_ks42_t_1001"
	correctRanges := GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{testKeyRangeForKeyspace(42)}}
	wrongRanges := GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{testKeyRangeForKeyspace(42), testKeyRangeForKeyspace(43)}}
	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{correctRanges}))
	_, err = manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1})
	re.NoError(err)
	correctRegion := generateRegionForTest(23, []uint64{1}, correctRanges.KeyRanges[0])
	state, isAffinity := manager.GetRegionAffinityGroupState(correctRegion)
	re.NotNil(state)
	re.Equal(groupID, state.ID)
	re.True(isAffinity)

	wrongRegion := generateRegionForTest(24, []uint64{1}, wrongRanges.KeyRanges[1])
	re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(wrongRanges)))
	re.Equal(groupID, regionLabeler.GetRegionLabel(wrongRegion, labelKey), "the generic labeler API accepts this write")
	state, isAffinity = manager.GetRegionAffinityGroupState(wrongRegion)
	re.Nil(state)
	re.False(isAffinity)
	state, isAffinity = manager.GetAndCacheRegionAffinityGroupState(wrongRegion)
	re.Nil(state)
	re.False(isAffinity)
	cache, _ := manager.getCache(wrongRegion)
	re.Nil(cache)
	state, isAffinity = manager.GetRegionAffinityGroupState(correctRegion)
	re.Nil(state, "the valid portion of a mixed-keyspace rule must also be inactive")
	re.False(isAffinity)
	re.Equal(correctRanges, manager.keyRanges[groupID])
	re.NoError(manager.SyncKeyRangesFromEtcd(regionLabeler.GetLabelRule(GetLabelRuleID(groupID))))
	re.NotContains(manager.keyRanges, groupID, "invalid rules must not reserve ranges in the overlap cache")
	state, isAffinity = manager.GetRegionAffinityGroupState(wrongRegion)
	re.Nil(state)
	re.False(isAffinity)

	customRanges := GroupKeyRanges{GroupID: "custom_group", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("b")}}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{customRanges}))
	_, err = manager.UpdateAffinityGroupPeers(customRanges.GroupID, 1, []uint64{1})
	re.NoError(err)
	customRegion := generateRegionForTest(25, []uint64{1}, customRanges.KeyRanges[0])
	state, isAffinity = manager.GetRegionAffinityGroupState(customRegion)
	re.NotNil(state)
	re.Equal(customRanges.GroupID, state.ID)
	re.True(isAffinity)
}

func TestNextGenRuntimeAffinityLabelRangeEdges(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	const groupID = "_tidb_ks42_t_1001"
	keyRange := testKeyRangeForKeyspace(42)
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: groupID, KeyRanges: []keyutil.KeyRange{keyRange}}}))
	_, err = manager.UpdateAffinityGroupPeers(groupID, 1, []uint64{1})
	re.NoError(err)
	region := generateRegionForTest(31, []uint64{1}, keyRange)
	lower := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 42)
	upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 43)
	// The generic labeler API accepts these rules; the runtime lookup decides validity.
	for _, tc := range []struct {
		name   string
		ranges []keyutil.KeyRange
		valid  bool
	}{
		{"unordered ranges", []keyutil.KeyRange{{StartKey: keyRange.EndKey, EndKey: upper[:]}, {StartKey: lower[:], EndKey: keyRange.EndKey}}, true},
		{"unbounded end", []keyutil.KeyRange{keyRange, {StartKey: keyRange.EndKey}}, false},
		{"unbounded end first", []keyutil.KeyRange{{StartKey: keyRange.EndKey}, keyRange}, false},
		{"later range before keyspace", []keyutil.KeyRange{keyRange, testKeyRangeForKeyspace(41)}, false},
		{"first range after keyspace", []keyutil.KeyRange{testKeyRangeForKeyspace(43), keyRange}, false},
		{"whole keyspace", []keyutil.KeyRange{{StartKey: lower[:], EndKey: upper[:]}}, true},
		{"single range", []keyutil.KeyRange{keyRange}, true},
	} {
		re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(GroupKeyRanges{GroupID: groupID, KeyRanges: tc.ranges})), tc.name)
		state, isAffinity := manager.GetRegionAffinityGroupState(region)
		re.Equal(tc.valid, isAffinity, tc.name)
		if tc.valid {
			re.NotNil(state, tc.name)
			re.Equal(groupID, state.ID, tc.name)
		} else {
			re.Nil(state, tc.name)
		}
	}
}

func TestNextGenInvalidAffinityRuleClearsCache(t *testing.T) {
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	require.NoError(t, failpoint.Enable(modeFlag, "return(true)"))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(modeFlag)) })
	for _, labelerFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("labeler first=%t", labelerFirst), func(t *testing.T) {
			re := require.New(t)
			store := storage.NewStorageWithMemoryBackend()
			stores := core.NewStoresInfo()
			stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
			regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, time.Second*5)
			re.NoError(err)
			manager, err := NewManager(t.Context(), store, stores, mockconfig.NewTestOptions(), regionLabeler)
			re.NoError(err)
			valid := GroupKeyRanges{GroupID: "_tidb_ks42_t_1001", KeyRanges: []keyutil.KeyRange{testKeyRangeForKeyspace(42)}}
			re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{valid}))
			_, err = manager.UpdateAffinityGroupPeers(valid.GroupID, 1, []uint64{1})
			re.NoError(err)
			region := generateRegionForTest(26, []uint64{1}, valid.KeyRanges[0])
			state, isAffinity := manager.GetAndCacheRegionAffinityGroupState(region)
			re.NotNil(state)
			re.True(isAffinity)
			cache, _ := manager.getCache(region)
			re.NotNil(cache)

			mixed := GroupKeyRanges{GroupID: valid.GroupID, KeyRanges: []keyutil.KeyRange{testKeyRangeForKeyspace(42), testKeyRangeForKeyspace(43)}}
			// Watchers receive independently decoded, adjusted rules.
			data, err := json.Marshal(MakeLabelRule(mixed))
			re.NoError(err)
			invalidRule, err := labeler.NewLabelRuleFromJSON(data)
			re.NoError(err)
			if labelerFirst {
				re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(mixed)))
			}
			re.NoError(manager.SyncKeyRangesFromEtcd(invalidRule))
			re.NotContains(manager.keyRanges, valid.GroupID)
			re.Zero(manager.GetAffinityGroupState(valid.GroupID).RangeCount)
			cache, _ = manager.getCache(region)
			re.Nil(cache)
			state, isAffinity = manager.GetAndCacheRegionAffinityGroupState(region)
			re.Nil(state, "the labeler may still contain the old valid rule")
			re.False(isAffinity)
			if !labelerFirst {
				re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(mixed)))
			}
			state, isAffinity = manager.GetAndCacheRegionAffinityGroupState(region)
			re.Nil(state)
			re.False(isAffinity)

			re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(valid)))
			re.NoError(manager.SyncKeyRangesFromEtcd(regionLabeler.GetLabelRule(GetLabelRuleID(valid.GroupID))))
			state, isAffinity = manager.GetAndCacheRegionAffinityGroupState(region)
			re.NotNil(state)
			re.Equal(valid.GroupID, state.ID)
			re.True(isAffinity, "a fully valid replacement must restore affinity")
		})
	}
}

func TestNextGenSyncGroupPrefersBufferedLabelRule(t *testing.T) {
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	require.NoError(t, failpoint.Enable(modeFlag, "return(true)"))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(modeFlag)) })
	const groupID = "_tidb_ks42_t_1001"
	keyRange := testKeyRangeForKeyspace(42)
	upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 43)
	valid := GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{keyRange}}
	mixed := GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{keyRange, testKeyRangeForKeyspace(43)}}
	// Watchers receive independently decoded, adjusted rules.
	watcherRule := func(re *require.Assertions, gkr GroupKeyRanges) *labeler.LabelRule {
		data, err := json.Marshal(MakeLabelRule(gkr))
		re.NoError(err)
		rule, err := labeler.NewLabelRuleFromJSON(data)
		re.NoError(err)
		return rule
	}
	// The labeler is synced by another watcher and may still hold an older rule
	// when the group arrives after its buffered rule.
	for _, tc := range []struct {
		name  string
		stale GroupKeyRanges
	}{
		{"stale invalid rule", mixed},
		{"stale valid rule", GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{{StartKey: keyRange.EndKey, EndKey: upper[:]}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re := require.New(t)
			store := storage.NewStorageWithMemoryBackend()
			stores := core.NewStoresInfo()
			stores.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
			regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, time.Second*5)
			re.NoError(err)
			manager, err := NewManager(t.Context(), store, stores, mockconfig.NewTestOptions(), regionLabeler)
			re.NoError(err)
			re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(tc.stale)))
			re.NoError(manager.SyncKeyRangesFromEtcd(watcherRule(re, valid)))
			re.Contains(manager.labelRuleBuffer, groupID)
			manager.SyncGroupFromEtcd(&Group{ID: groupID, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}})
			re.NotContains(manager.labelRuleBuffer, groupID)
			re.Equal(valid, manager.keyRanges[groupID])
			re.Equal(1, manager.GetAffinityGroupState(groupID).RangeCount)
			re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(valid)))
			region := generateRegionForTest(28, []uint64{1}, keyRange)
			state, _ := manager.GetAndCacheRegionAffinityGroupState(region)
			re.NotNil(state)
			re.Equal(groupID, state.ID)

			// Later invalid and delete events still deactivate the group.
			re.NoError(manager.SyncKeyRangesFromEtcd(watcherRule(re, mixed)))
			re.NotContains(manager.keyRanges, groupID)
			re.Zero(manager.GetAffinityGroupState(groupID).RangeCount)
			state, _ = manager.GetAndCacheRegionAffinityGroupState(region)
			re.Nil(state, "the labeler may still contain the valid rule")
			re.NoError(manager.SyncKeyRangesFromEtcd(watcherRule(re, valid)))
			re.Equal(valid, manager.keyRanges[groupID])
			manager.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID(groupID))
			re.NotContains(manager.keyRanges, groupID)
			re.Zero(manager.GetAffinityGroupState(groupID).RangeCount)
			state, _ = manager.GetAndCacheRegionAffinityGroupState(region)
			re.Nil(state)
		})
	}
}

func TestNextGenAffinityLookupDoesNotHoldManagerLock(t *testing.T) {
	re := require.New(t)
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	store := storage.NewStorageWithMemoryBackend()
	regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, time.Second*5)
	re.NoError(err)
	manager := &Manager{regionLabeler: regionLabeler, groups: make(map[string]*runtimeGroupInfo)}
	const lookupHook = "github.com/tikv/pd/pkg/schedule/affinity/beforeRegionLabelLookup"
	called, unlocked := false, false
	re.NoError(failpoint.EnableCall(lookupHook, func() {
		called = true
		unlocked = manager.TryLock()
		if unlocked {
			manager.Unlock()
		}
	}))
	t.Cleanup(func() { re.NoError(failpoint.Disable(lookupHook)) })
	manager.findRegionAffinityGroupState(generateRegionForTest(27, []uint64{1}, testKeyRangeForKeyspace(42)))
	re.True(called)
	re.True(unlocked, "waiting for the labeler must not block manager writers")
}

func TestNextGenLegacyLabelDoesNotShadowCanonicalGroup(t *testing.T) {
	re := require.New(t)
	ctx := t.Context()
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	keyRange := testKeyRangeForKeyspace(43)
	legacyID := "_tidb_t_1001"
	canonicalID := "_tidb_ks43_t_1001"
	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	storeInfos.PutStore(core.NewStoreInfo(&metapb.Store{Id: 1}))
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	re.NoError(failpoint.Enable(modeFlag, "return(false)"))
	classicManager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	re.NoError(classicManager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: legacyID, KeyRanges: []keyutil.KeyRange{keyRange}}}))
	legacyRule := MakeLabelRule(GroupKeyRanges{GroupID: legacyID, KeyRanges: []keyutil.KeyRange{keyRange}})
	legacyRule.Index = 5
	re.NoError(regionLabeler.SetLabelRule(legacyRule))

	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	manager, err := NewManager(ctx, store, storeInfos, mockconfig.NewTestOptions(), regionLabeler)
	re.NoError(err)
	re.False(manager.IsGroupExist(legacyID))
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: canonicalID, KeyRanges: []keyutil.KeyRange{keyRange}}}))
	_, err = manager.UpdateAffinityGroupPeers(canonicalID, 1, []uint64{1})
	re.NoError(err)
	region := generateRegionForTest(30, []uint64{1}, keyRange)
	re.Equal(legacyID, regionLabeler.GetRegionLabel(region, labelKey), "higher-index legacy label is the raw labeler winner")
	state, isAffinity := manager.GetRegionAffinityGroupState(region)
	re.NotNil(state)
	re.Equal(canonicalID, state.ID)
	re.True(isAffinity)

	legacyRule = MakeLabelRule(GroupKeyRanges{GroupID: legacyID, KeyRanges: []keyutil.KeyRange{keyRange}})
	re.NoError(regionLabeler.SetLabelRule(legacyRule))
	state, isAffinity = manager.GetRegionAffinityGroupState(region)
	re.NotNil(state)
	re.Equal(canonicalID, state.ID, "same-index rules must still resolve to the valid group")
	re.True(isAffinity)
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
			{StartKey: []byte("a"), EndKey: []byte("b")},
			{StartKey: []byte("c"), EndKey: []byte("d")},
		}},
	}
	err = validate(keyRanges1)
	re.NoError(err, "Non-overlapping ranges should pass validation")

	// Test 2: Overlapping ranges within same request should fail
	keyRanges2 := []GroupKeyRanges{
		{GroupID: "group1", KeyRanges: []keyutil.KeyRange{
			{StartKey: []byte("a"), EndKey: []byte("c")},
			{StartKey: []byte("b"), EndKey: []byte("d")},
		}},
	}
	err = validate(keyRanges2)
	re.Error(err, "Overlapping ranges should fail validation")
	re.Contains(err.Error(), "overlap")

	// Test 3: Adjacent ranges (not overlapping) should succeed
	keyRanges3 := []GroupKeyRanges{
		{GroupID: "group1", KeyRanges: []keyutil.KeyRange{
			{StartKey: []byte("a"), EndKey: []byte("b")},
			{StartKey: []byte("b"), EndKey: []byte("c")},
		}},
	}
	err = validate(keyRanges3)
	re.NoError(err, "Adjacent ranges should pass validation")

	// Test 5: Duplicate range should fail validation
	keyRangesDup := []GroupKeyRanges{
		{GroupID: "group1", KeyRanges: []keyutil.KeyRange{
			{StartKey: []byte("a"), EndKey: []byte("c")},
			{StartKey: []byte("a"), EndKey: []byte("c")},
		}},
	}
	err = validate(keyRangesDup)
	re.Error(err, "Duplicate ranges should fail validation")
	re.Contains(err.Error(), "overlap")

	// Test 4: Verify checkKeyRangesOverlap function directly
	overlaps := checkKeyRangesOverlap([]byte("a"), []byte("c"), []byte("b"), []byte("d"))
	re.True(overlaps, "Ranges [a,c) and [b,d) should overlap")

	overlaps = checkKeyRangesOverlap([]byte("a"), []byte("b"), []byte("c"), []byte("d"))
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

	// Inject a leftover label rule for a non-existent group "ghost".
	plan := regionLabeler.NewPlan()
	rule := &labeler.LabelRule{
		ID:       GetLabelRuleID("ghost"),
		Labels:   []labeler.RegionLabel{{Key: labelKey, Value: "ghost"}},
		RuleType: labeler.KeyRange,
		Data: []any{
			map[string]any{"start_key": hex.EncodeToString([]byte{0x01}), "end_key": hex.EncodeToString([]byte{0x02})},
		},
	}
	re.NoError(plan.SetLabelRule(rule))
	re.NoError(endpoint.RunBatchOpInTxn(ctx, store, plan.CommitOps()))
	plan.Apply()

	// Manager initialization should ignore the unknown label rule.
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)
	re.False(manager.IsGroupExist("ghost"))

	// Creating a new group with overlapping range should succeed (ghost is ignored).
	err = manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID: "real",
		KeyRanges: []keyutil.KeyRange{{
			StartKey: []byte{0x01},
			EndKey:   []byte{0x02},
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

// TestUpdateAffinityGroupKeyRangesAddToEmptyGroup documents the current failure
// when adding key ranges to a group created without initial ranges.
func TestUpdateAffinityGroupKeyRangesAddToEmptyGroup(t *testing.T) {
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

	// Create a group without key ranges or peers.
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "empty-group"}}))

	// Adding ranges should succeed; currently it returns "label rule not found".
	err = manager.UpdateAffinityGroupKeyRanges(
		[]GroupKeyRanges{{
			GroupID: "empty-group",
			KeyRanges: []keyutil.KeyRange{{
				StartKey: []byte{0x00},
				EndKey:   []byte{0x01},
			}},
		}},
		nil,
	)
	re.NoError(err)
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

	r := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x10}}

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
			StartKey: []byte{0x00},
			EndKey:   []byte{0x10},
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

	r1 := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x10}}
	r2 := keyutil.KeyRange{StartKey: []byte{0x20}, EndKey: []byte{0x30}}

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

	r := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x10}}
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

	rOld := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x10}}
	rNew := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x05}}

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
	rangeA := keyutil.KeyRange{StartKey: []byte{0x00}, EndKey: []byte{0x20}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID:   "groupA",
		KeyRanges: []keyutil.KeyRange{rangeA},
	}}))

	// Try to create group B with overlapping range [0x10, 0x30]
	// This should be rejected because it overlaps with group A's existing range
	rangeB := keyutil.KeyRange{StartKey: []byte{0x10}, EndKey: []byte{0x30}}
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
	rangeD := keyutil.KeyRange{StartKey: []byte{0x40}, EndKey: []byte{0x50}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID:   "groupD",
		KeyRanges: []keyutil.KeyRange{rangeD},
	}}))

	// Create group E without ranges
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "groupE"}}))

	// Try to add ranges to both group C and group E in a single batch,
	// where group C's new range [0x45, 0x55] overlaps with group D's existing range [0x40, 0x50]
	rangeC := keyutil.KeyRange{StartKey: []byte{0x45}, EndKey: []byte{0x55}} // overlaps with D
	rangeE := keyutil.KeyRange{StartKey: []byte{0x60}, EndKey: []byte{0x70}} // no overlap
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
