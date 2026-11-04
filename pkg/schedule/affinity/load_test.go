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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestLoadOverlappingAffinityRanges(t *testing.T) {
	keyRange := func(start, end string) keyutil.KeyRange {
		return keyutil.KeyRange{StartKey: []byte(start), EndKey: []byte(end)}
	}
	for _, testCase := range []struct {
		name   string
		groups []GroupKeyRanges
	}{
		{
			name: "between groups",
			groups: []GroupKeyRanges{
				{GroupID: "a", KeyRanges: []keyutil.KeyRange{keyRange("a", "d")}},
				{GroupID: "b", KeyRanges: []keyutil.KeyRange{keyRange("b", "c")}},
			},
		},
		{
			name: "within group",
			groups: []GroupKeyRanges{
				{GroupID: "a", KeyRanges: []keyutil.KeyRange{keyRange("a", "d"), keyRange("b", "c")}},
			},
		},
		{
			name: "nested ranges",
			groups: []GroupKeyRanges{
				{GroupID: "a", KeyRanges: []keyutil.KeyRange{keyRange("a", "g")}},
				{GroupID: "b", KeyRanges: []keyutil.KeyRange{keyRange("b", "c")}},
				{GroupID: "c", KeyRanges: []keyutil.KeyRange{keyRange("d", "e")}},
			},
		},
		{
			name: "unbounded ranges",
			groups: []GroupKeyRanges{
				{GroupID: "a", KeyRanges: []keyutil.KeyRange{keyRange("", "")}},
				{GroupID: "b", KeyRanges: []keyutil.KeyRange{keyRange("b", "c")}},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			ctx := t.Context()
			store := storage.NewStorageWithMemoryBackend()
			for _, change := range testCase.groups {
				re.NoError(store.RunInTxn(ctx, func(txn kv.Txn) error {
					if err := store.SaveAffinityGroup(txn, change.GroupID, &Group{
						ID: change.GroupID, LeaderStoreID: 1, VoterStoreIDs: []uint64{1},
					}); err != nil {
						return err
					}
					return store.SaveRegionRule(txn, GetLabelRuleID(change.GroupID), MakeLabelRule(change))
				}))
			}
			snapshot := func() map[string]string {
				data := make(map[string]string)
				re.NoError(store.LoadAllAffinityGroups(func(k, v string) { data["group/"+k] = v }))
				re.NoError(store.LoadRegionRules(func(k, v string) { data["rule/"+k] = v }))
				return data
			}
			before := snapshot()
			logFile := testutil.InitTempFileLogger(t, "error")
			regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Hour)
			re.NoError(err)
			manager, err := NewManager(ctx, store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
			re.NoError(err, "persisted overlaps must not prevent startup")
			re.Equal(before, snapshot(), "loading must not repair persistent data")
			region := generateRegionForTest(1, []uint64{1}, keyRange("b", "c"))
			for _, change := range testCase.groups {
				manager.RestoreAffinityGroup(change.GroupID)
				state := manager.GetAffinityGroupState(change.GroupID)
				re.NotNil(state)
				re.Equal(PhasePending, state.Phase)
				re.False(state.AffinitySchedulingAllowed)
				re.True(state.RegularSchedulingAllowed)
				re.Equal(len(change.KeyRanges), state.RangeCount)
			}
			group, isAffinity := manager.GetAndCacheRegionAffinityGroupState(region)
			re.Nil(group, "conflicting groups must not acquire Regions by label iteration order")
			re.False(isAffinity)
			logs, err := os.ReadFile(logFile)
			re.NoError(err)
			re.Equal(1, strings.Count(string(logs), "overlapping affinity key ranges"))
			for _, change := range testCase.groups {
				re.Contains(string(logs), change.GroupID)
			}
			re.Contains(string(logs), "start-key")
			re.Contains(string(logs), "end-key")

			if testCase.name != "unbounded ranges" {
				unrelated := GroupKeyRanges{GroupID: "unrelated", KeyRanges: []keyutil.KeyRange{keyRange("x", "y")}}
				re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{unrelated}))
				extra := []GroupKeyRanges{{GroupID: unrelated.GroupID, KeyRanges: []keyutil.KeyRange{keyRange("y", "z")}}}
				re.NoError(manager.UpdateAffinityGroupKeyRanges(extra, nil))
				re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, extra))
				re.NoError(manager.DeleteAffinityGroups([]string{unrelated.GroupID}, true))
			}
			re.Error(manager.CreateAffinityGroups([]GroupKeyRanges{{
				GroupID: "new-overlap", KeyRanges: []keyutil.KeyRange{keyRange("b", "c")},
			}}), "new overlaps must still be rejected")

			// Removing one of the overlapping ranges/groups restores ordinary use.
			if len(testCase.groups) == 1 {
				re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{
					GroupID: "a", KeyRanges: testCase.groups[0].KeyRanges[1:],
				}}))
			} else {
				re.NoError(manager.DeleteAffinityGroups([]string{"a"}, true))
			}
			group, isAffinity = manager.GetRegionAffinityGroupState(region)
			re.NotNil(group)
			re.True(isAffinity)
			re.True(group.AffinitySchedulingAllowed)
			_, err = NewManager(ctx, store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
			re.NoError(err, "reloading repaired metadata must also succeed")
		})
	}
}

func TestSyncOverlappingAffinityRanges(t *testing.T) {
	for _, labelFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("label-first=%t", labelFirst), func(t *testing.T) {
			re := require.New(t)
			ctx := t.Context()
			store := storage.NewStorageWithMemoryBackend()
			regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Hour)
			re.NoError(err)
			manager, err := NewManager(ctx, store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
			re.NoError(err)
			ranges := []GroupKeyRanges{
				{GroupID: "a", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("a"), EndKey: []byte("d")}}},
				{GroupID: "b", KeyRanges: []keyutil.KeyRange{{StartKey: []byte("b"), EndKey: []byte("c")}}},
			}
			region := generateRegionForTest(1, []uint64{1}, ranges[1].KeyRanges[0])
			for i, groupRanges := range ranges {
				group := &Group{ID: groupRanges.GroupID, LeaderStoreID: 1, VoterStoreIDs: []uint64{1}}
				data, err := json.Marshal(MakeLabelRule(groupRanges))
				re.NoError(err)
				rule, err := labeler.NewLabelRuleFromJSON(data)
				re.NoError(err)
				if labelFirst {
					re.NoError(manager.SyncKeyRangesFromEtcd(rule))
					manager.SyncGroupFromEtcd(group)
				} else {
					manager.SyncGroupFromEtcd(group)
					re.NoError(manager.SyncKeyRangesFromEtcd(rule))
				}
				re.NoError(regionLabeler.SetLabelRule(MakeLabelRule(groupRanges)))
				manager.RestoreAffinityGroup(group.ID)
				if i == 0 {
					_, isAffinity := manager.GetAndCacheRegionAffinityGroupState(region)
					re.True(isAffinity)
				}
			}
			for _, groupRanges := range ranges {
				state := manager.GetAffinityGroupState(groupRanges.GroupID)
				re.False(state.AffinitySchedulingAllowed)
				re.True(state.RegularSchedulingAllowed)
				re.Zero(state.RegionCount)
				re.Zero(state.AffinityRegionCount)
			}
			group, isAffinity := manager.GetAndCacheRegionAffinityGroupState(region)
			re.Nil(group)
			re.False(isAffinity)

			re.NoError(regionLabeler.DeleteLabelRule(GetLabelRuleID("b")))
			if labelFirst {
				manager.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID("b"))
				manager.SyncGroupDeleteFromEtcd("b")
			} else {
				manager.SyncGroupDeleteFromEtcd("b")
				manager.SyncKeyRangesDeleteFromEtcd(GetLabelRuleID("b"))
			}
			group, isAffinity = manager.GetAndCacheRegionAffinityGroupState(region)
			re.NotNil(group)
			re.Equal("a", group.ID)
			re.True(isAffinity)
		})
	}
}
