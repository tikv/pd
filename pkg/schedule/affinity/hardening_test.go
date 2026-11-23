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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func newRangeTestManager(t *testing.T) *Manager {
	t.Helper()
	store := storage.NewStorageWithMemoryBackend()
	regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, time.Second*5)
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

func TestCreateAffinityGroupsDuplicateIDs(t *testing.T) {
	for _, size := range []int{2, etcdutil.MaxEtcdTxnOps + 2} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			re := require.New(t)
			manager := newRangeTestManager(t)
			first := GroupKeyRanges{GroupID: "duplicate", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 1)}}
			changes := make([]GroupKeyRanges, 0, size)
			changes = append(changes, first)
			for i := 1; i < size-1; i++ {
				changes = append(changes, GroupKeyRanges{GroupID: fmt.Sprintf("padding_%d", i)})
			}
			changes = append(changes, GroupKeyRanges{GroupID: first.GroupID})
			storedGroups := func() map[string]string {
				groups := make(map[string]string)
				re.NoError(manager.storage.LoadAllAffinityGroups(func(key, value string) { groups[key] = value }))
				return groups
			}
			assertRejected := func() {
				before := storedGroups()
				for _, create := range []func([]GroupKeyRanges) error{
					manager.CreateAffinityGroups, manager.CreateAffinityGroupsIfNotExists,
				} {
					re.ErrorIs(create(changes), errs.ErrAffinityGroupContent)
					re.Equal(before, storedGroups())
					re.Equal(len(before), manager.GetAffinityGroupCount())
				}
			}
			re.Empty(storedGroups())
			assertRejected()
			re.Empty(manager.keyRanges)
			re.Nil(manager.regionLabeler.GetLabelRule(GetLabelRuleID(first.GroupID)))
			re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{first}))
			assertRejected()
			re.Equal(first, manager.keyRanges[first.GroupID])
			re.ErrorIs(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "overlap", KeyRanges: first.KeyRanges}}), errs.ErrAffinityGroupContent)
		})
	}
}

func TestUpdateAffinityGroupKeyRangesEmptyOperations(t *testing.T) {
	re := require.New(t)
	manager := newRangeTestManager(t)
	initial := GroupKeyRanges{GroupID: "ranges", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 1)}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{initial, {GroupID: "empty"}}))
	for _, id := range []string{"ranges", "empty", "missing"} {
		op := []GroupKeyRanges{{GroupID: id}}
		re.NoError(manager.UpdateAffinityGroupKeyRanges(op, nil))
		re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, op))
	}
	nonempty := GroupKeyRanges{GroupID: "ranges", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 2)}}
	empty := GroupKeyRanges{GroupID: "ranges"}
	for _, ops := range []struct{ add, remove []GroupKeyRanges }{
		{add: []GroupKeyRanges{nonempty, empty}},
		{add: []GroupKeyRanges{empty, nonempty}},
		{add: []GroupKeyRanges{nonempty}, remove: []GroupKeyRanges{empty}},
	} {
		re.NoError(manager.UpdateAffinityGroupKeyRanges(ops.add, ops.remove))
		re.Equal(2, manager.GetAffinityGroupState("ranges").RangeCount)
		re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{empty}, []GroupKeyRanges{nonempty, empty}))
		re.Equal(initial, manager.keyRanges[initial.GroupID])
	}
	re.NoError(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{empty, empty}, nil))
	re.ErrorIs(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{nonempty, nonempty}, nil), errs.ErrAffinityGroupExist)
	re.ErrorIs(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{initial, initial}), errs.ErrAffinityGroupExist)
	re.ErrorIs(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{nonempty}, []GroupKeyRanges{initial}), errs.ErrAffinityGroupExist)
	re.Equal(initial, manager.keyRanges[initial.GroupID])
	re.Equal(1, manager.GetAffinityGroupState("ranges").RangeCount)
	re.Nil(manager.regionLabeler.GetLabelRule(GetLabelRuleID("empty")))
}

func TestUpdateAffinityGroupKeyRangesMissingGroup(t *testing.T) {
	re := require.New(t)
	manager := newRangeTestManager(t)
	orphan := GroupKeyRanges{GroupID: "orphan", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 1)}}
	rule := MakeLabelRule(orphan)
	re.NoError(manager.regionLabeler.SetLabelRule(rule))
	for _, id := range []string{"missing", "orphan"} {
		op := []GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 2)}}}
		re.ErrorIs(manager.UpdateAffinityGroupKeyRanges(op, nil), errs.ErrAffinityGroupNotFound)
		op[0].KeyRanges = orphan.KeyRanges
		re.ErrorIs(manager.UpdateAffinityGroupKeyRanges(nil, op), errs.ErrAffinityGroupNotFound)
	}
	re.Empty(manager.keyRanges)
	re.Equal(rule, manager.regionLabeler.GetLabelRule(rule.ID))
	manager.keyRanges[orphan.GroupID] = orphan
	re.ErrorIs(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{orphan}, nil), errs.ErrAffinityGroupNotFound)
}

func TestNextGenCustomGroupMergedRanges(t *testing.T) {
	re := require.New(t)
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	manager := newRangeTestManager(t)
	initial := GroupKeyRanges{GroupID: "custom", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 1)}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{initial}))
	other := GroupKeyRanges{GroupID: "custom", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(2, 1)}}
	// The incoming range is valid by itself; only the merged ranges cross keyspaces.
	re.NoError(validateGroupKeyRanges(other))
	re.ErrorIs(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{other}, nil), errs.ErrAffinityGroupContent)
	re.Equal(initial, manager.keyRanges[initial.GroupID])
	for _, ranges := range [][]keyutil.KeyRange{
		{{}},
		{{StartKey: initial.KeyRanges[0].StartKey}},
		{{EndKey: initial.KeyRanges[0].EndKey}},
		{{StartKey: initial.KeyRanges[0].StartKey, EndKey: other.KeyRanges[0].EndKey}},
		{initial.KeyRanges[0], other.KeyRanges[0]},
	} {
		re.ErrorIs(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: "invalid", KeyRanges: ranges}}), errs.ErrAffinityGroupContent)
		re.False(manager.IsGroupExist("invalid"))
	}
	// Seed an old cross-keyspace rule, then remove it without weakening the
	// validation of the ranges that remain.
	re.NoError(failpoint.Enable(modeFlag, "return(false)"))
	legacy := GroupKeyRanges{GroupID: "legacy", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(3, 1), transactionRangeForTest(4, 1)}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{legacy}))
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	re.ErrorIs(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: "legacy", KeyRanges: []keyutil.KeyRange{transactionRangeForTest(3, 2)}}}, nil), errs.ErrAffinityGroupContent)
	re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{GroupID: "legacy", KeyRanges: legacy.KeyRanges[1:]}}))
	re.Equal(1, manager.GetAffinityGroupState("legacy").RangeCount)
}
