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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type rangeOrderStorage struct {
	storage.Storage
	operations []string
	failSaveID string
}

func (s *rangeOrderStorage) SaveRegionRule(txn kv.Txn, id string, rule any) error {
	s.operations = append(s.operations, id)
	if id == s.failSaveID {
		return errors.New("injected range storage failure")
	}
	return s.Storage.SaveRegionRule(txn, id, rule)
}

func (s *rangeOrderStorage) DeleteRegionRule(txn kv.Txn, id string) error {
	s.operations = append(s.operations, id)
	return s.Storage.DeleteRegionRule(txn, id)
}

func TestUpdateAffinityGroupKeyRangesRequestOrder(t *testing.T) {
	re := require.New(t)
	for range 32 {
		store := &rangeOrderStorage{Storage: storage.NewStorageWithMemoryBackend()}
		regionLabeler, err := labeler.NewRegionLabeler(t.Context(), store, 5*time.Second)
		re.NoError(err)
		manager, err := NewManager(t.Context(), store, core.NewStoresInfo(), mockconfig.NewTestOptions(), regionLabeler)
		re.NoError(err)
		add, remove := []GroupKeyRanges{{GroupID: "empty-add"}}, []GroupKeyRanges{{GroupID: "empty-remove"}}
		var expected []string
		for i, id := range []string{"z-add", "a-add", "m-add", "z-remove", "a-remove", "m-remove"} {
			op := GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, int64(10+i*2))}}
			if i < 3 {
				re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id}}))
				add = append(add, op)
			} else {
				re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{op}))
				remove = append(remove, op)
			}
			expected = append(expected, GetLabelRuleID(id))
		}
		store.operations = nil
		re.NoError(manager.UpdateAffinityGroupKeyRanges(add, remove))
		re.Equal(expected, store.operations, "storage must follow request order, with all adds before removes")
		for _, op := range add[1:] {
			re.Equal(op, manager.keyRanges[op.GroupID])
			re.Equal(1, manager.GetAffinityGroupState(op.GroupID).RangeCount)
		}
		for _, op := range remove[1:] {
			re.NotContains(manager.keyRanges, op.GroupID)
			re.Zero(manager.GetAffinityGroupState(op.GroupID).RangeCount)
		}
		// A failed transaction must leave both caches and the label index intact.
		op := GroupKeyRanges{GroupID: add[1].GroupID, KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 100)}}
		store.failSaveID = GetLabelRuleID(op.GroupID)
		re.ErrorContains(manager.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{op}, nil), "injected range storage failure")
		re.Equal(add[1], manager.keyRanges[op.GroupID])
		re.Equal(1, manager.GetAffinityGroupState(op.GroupID).RangeCount)
		ranges, err := extractKeyRangesFromLabelRule(regionLabeler.GetLabelRule(GetLabelRuleID(op.GroupID)))
		re.NoError(err)
		re.Equal(add[1], ranges)
	}
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
		ranges := []keyutil.KeyRange{transactionRangeForTest(1, tableID)}
		if !tc.removeAll {
			ranges = append(ranges, transactionRangeForTest(1, tableID+1))
		}
		initial = append(initial, GroupKeyRanges{GroupID: tc.id, KeyRanges: ranges})
		remove = append(remove, GroupKeyRanges{GroupID: tc.id, KeyRanges: ranges[:1]})
		if tc.committed {
			ranges = ranges[1:]
		}
		if len(ranges) > 0 {
			expected[tc.id] = GroupKeyRanges{GroupID: tc.id, KeyRanges: ranges}
		}
	}
	re.NoError(manager.CreateAffinityGroups(initial))
	// Fail the second transaction after it has staged its first removal.
	store.failSaveID = GetLabelRuleID("b-remove")
	re.ErrorContains(manager.UpdateAffinityGroupKeyRanges(add, remove), "injected range storage failure")

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

func TestNextGenRemoveOneLegacyRange(t *testing.T) {
	re := require.New(t)
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(false)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	manager := newRangeTestManager(t)
	classicRange := func(tableID int64) keyutil.KeyRange {
		return keyutil.KeyRange{
			StartKey: codec.EncodeBytes(codec.GenerateTableKey(tableID)),
			EndKey:   codec.EncodeBytes(codec.GenerateTableKey(tableID + 1)),
		}
	}
	legacy := GroupKeyRanges{GroupID: "legacy", KeyRanges: []keyutil.KeyRange{classicRange(1), classicRange(3)}}
	re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{legacy}))

	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	remaining := GroupKeyRanges{GroupID: legacy.GroupID, KeyRanges: legacy.KeyRanges[1:]}
	re.ErrorIs(validateGroupKeyRanges(remaining), errs.ErrAffinityGroupContent)
	re.NoError(manager.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{GroupID: legacy.GroupID, KeyRanges: legacy.KeyRanges[:1]}}))
	re.Equal(remaining, manager.keyRanges[legacy.GroupID])
	re.Equal(1, manager.GetAffinityGroupState(legacy.GroupID).RangeCount)
	ranges, err := extractKeyRangesFromLabelRule(manager.regionLabeler.GetLabelRule(GetLabelRuleID(legacy.GroupID)))
	re.NoError(err)
	re.Equal(remaining, ranges)
}

func TestNextGenLegacyGroupCleanup(t *testing.T) {
	re := require.New(t)
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(false)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	manager := newRangeTestManager(t)
	ids := []string{"_tidb_t_1001", "_tidb_pt_1001_p3", "_tidb_tg_100"}
	for i, id := range ids {
		re.NoError(manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: id, KeyRanges: []keyutil.KeyRange{
			transactionRangeForTest(1, int64(i*10+1)), transactionRangeForTest(1, int64(i*10+2)),
		}}}))
	}
	orphan := "_tidb_t_9999"
	re.NoError(manager.regionLabeler.SetLabelRule(MakeLabelRule(GroupKeyRanges{GroupID: orphan, KeyRanges: []keyutil.KeyRange{transactionRangeForTest(1, 9999)}})))
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	// Exercise the startup path that deliberately preserves old groups and orphan rules.
	loaded, err := NewManager(t.Context(), manager.storage, core.NewStoresInfo(), mockconfig.NewTestOptions(), manager.regionLabeler)
	re.NoError(err)
	for _, id := range ids {
		state, err := loaded.CheckAndGetAffinityGroupState(id)
		re.NoError(err)
		re.Equal(2, state.RangeCount)
		ranges := loaded.keyRanges[id]
		re.ErrorContains(loaded.UpdateAffinityGroupKeyRanges([]GroupKeyRanges{{GroupID: id}}, nil), "pd-ctl --keyspace-id")
		re.NoError(loaded.UpdateAffinityGroupKeyRanges(nil, []GroupKeyRanges{{GroupID: id, KeyRanges: ranges.KeyRanges[:1]}}))
		re.Equal(1, loaded.GetAffinityGroupState(id).RangeCount)
		re.NoError(loaded.DeleteAffinityGroups([]string{id}, true))
		re.Nil(loaded.regionLabeler.GetLabelRule(GetLabelRuleID(id)))
	}
	_, err = loaded.CheckAndGetAffinityGroupState(orphan)
	re.ErrorIs(err, errs.ErrAffinityGroupNotFound)
	re.NoError(loaded.DeleteAffinityGroups([]string{orphan}, true))
	re.Nil(loaded.regionLabeler.GetLabelRule(GetLabelRuleID(orphan)))
}

func TestNextGenRejectNonTxnPrefixes(t *testing.T) {
	re := require.New(t)
	const modeFlag = "github.com/tikv/pd/pkg/versioninfo/kerneltype/mockNextGenBuildFlag"
	re.NoError(failpoint.Enable(modeFlag, "return(true)"))
	t.Cleanup(func() { re.NoError(failpoint.Disable(modeFlag)) })
	upper := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 1<<24)
	for _, id := range []string{"custom", "_tidb_ks16777215_t_1"} {
		t.Run(id, func(t *testing.T) {
			for _, start := range [][]byte{codec.EncodeBytes([]byte("y")), codec.EncodeBytes([]byte("y\x00")), {'x', 0xff, 0xff, 0xff, 't'}} {
				require.ErrorIs(t, validateGroupKeyRanges(GroupKeyRanges{GroupID: id, KeyRanges: []keyutil.KeyRange{{StartKey: start, EndKey: upper[:]}}}), errs.ErrAffinityGroupContent)
			}
		})
	}
	// The upper fencepost remains valid as an end key for the last keyspace.
	lower := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, (1<<24)-1)
	re.NoError(validateGroupKeyRanges(GroupKeyRanges{GroupID: "custom", KeyRanges: []keyutil.KeyRange{{StartKey: lower[:], EndKey: upper[:]}}}))
	// A short encoded x-prefix below boundary(2) still belongs to keyspace 1.
	end := codec.EncodeKeyspaceBoundary(codec.TxnKeyspaceModePrefix, 2)
	beforeTwo := append([]byte(nil), end[:]...)
	beforeTwo[len(beforeTwo)-1]--
	re.NoError(validateGroupKeyRanges(GroupKeyRanges{GroupID: "custom", KeyRanges: []keyutil.KeyRange{{StartKey: beforeTwo, EndKey: end[:]}}}))
}
