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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

func prepareBalanceCommit(t *testing.T, manager *Manager, groupID string) *BalanceSnapshot {
	t.Helper()
	snapshot := manager.GetBalanceSnapshot(groupID)
	require.NotNil(t, snapshot)
	require.NoError(t, manager.PrepareBalanceCommit(t.Context(), snapshot))
	return snapshot
}

func balanceCommitTarget(snapshot *BalanceSnapshot, groupID string, leaderStoreID uint64) *Group {
	for _, group := range snapshot.Groups {
		if group.ID == groupID {
			target := group.Group
			target.LeaderStoreID = leaderStoreID
			return &target
		}
	}
	return nil
}

func loadBalanceGroup(t *testing.T, manager *Manager, groupID string) Group {
	t.Helper()
	value, err := manager.storage.LoadAffinityGroup(groupID)
	require.NoError(t, err)
	var group Group
	require.NoError(t, json.Unmarshal([]byte(value), &group))
	return group
}

type balanceConditionalStorage struct {
	storage.Storage
	expected clientv3.Cmp
	received [][]clientv3.Cmp
}

func (s *balanceConditionalStorage) RunInTxnWithConditions(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	s.received = append(s.received, slices.Clone(conditions))
	if len(conditions) != 1 ||
		string(conditions[0].KeyBytes()) != string(s.expected.KeyBytes()) ||
		conditions[0].Target != s.expected.Target || conditions[0].Result != s.expected.Result ||
		string(conditions[0].ValueBytes()) != string(s.expected.ValueBytes()) {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	value, err := s.Load(string(s.expected.KeyBytes()))
	if err != nil {
		return err
	}
	if value != string(s.expected.ValueBytes()) {
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return s.RunInTxn(ctx, f)
}

func TestBalanceCommitRejectsChangedTableTarget(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	snapshot := prepareBalanceCommit(t, manager, ids[0])

	remote := manager.GetAffinityGroupState(ids[1]).Group
	remote.LeaderStoreID = 2
	require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return manager.storage.SaveAffinityGroup(txn, remote.ID, &remote)
	}))

	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, balanceCommitTarget(snapshot, ids[0], 3))
	require.NoError(t, err)
	require.False(t, committed)
	require.Equal(t, uint64(1), manager.GetAffinityGroupState(ids[0]).LeaderStoreID)
}

func TestBalanceCommitRejectsChangedTableMembership(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	snapshot := prepareBalanceCommit(t, manager, ids[0])
	remote := Group{ID: "_tidb_pt_10_p99", LeaderStoreID: 2, VoterStoreIDs: []uint64{1, 2, 3}}
	require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return manager.storage.SaveAffinityGroup(txn, remote.ID, &remote)
	}))

	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, balanceCommitTarget(snapshot, ids[0], 3))
	require.NoError(t, err)
	require.False(t, committed)
}

func TestBalanceCommitRejectsPlacementTokenChange(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	snapshot := prepareBalanceCommit(t, manager, ids[0])
	placementKey := endpoint.AffinityPlacementRevisionKey()
	require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return txn.Save(placementKey, "external-placement")
	}))

	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, balanceCommitTarget(snapshot, ids[0], 2))
	require.NoError(t, err)
	require.False(t, committed)
	require.Equal(t, uint64(1), manager.GetAffinityGroupState(ids[0]).LeaderStoreID)
}

func TestPrepareBalanceCommitRejectsMissingPrimaryTerm(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	manager.SetBalanceCommitGuard(func() ([]clientv3.Cmp, bool) {
		return nil, false
	})
	snapshot := manager.GetBalanceSnapshot(ids[0])
	require.NotNil(t, snapshot)
	require.True(t, errs.ErrEtcdTxnConflict.Equal(manager.PrepareBalanceCommit(t.Context(), snapshot)))
}

func TestBalanceCommitRejectsRevokedPrimaryTerm(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	base := manager.storage.(storage.Storage)
	termKey, termValue := "affinity-test-primary", "primary-term-1"
	require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return txn.Save(termKey, termValue)
	}))
	expected := clientv3.Compare(clientv3.Value(termKey), "=", termValue)
	conditional := &balanceConditionalStorage{Storage: base, expected: expected}
	manager.storage = conditional
	manager.SetBalanceCommitGuard(func() ([]clientv3.Cmp, bool) {
		return []clientv3.Cmp{expected}, true
	})

	snapshot := manager.GetBalanceSnapshot(ids[0])
	require.NotNil(t, snapshot)
	require.NoError(t, manager.PrepareBalanceCommit(t.Context(), snapshot))
	require.Len(t, conditional.received, 1)
	require.Equal(t, []clientv3.Cmp{expected}, conditional.received[0])

	require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return txn.Save(termKey, "primary-term-2")
	}))
	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, balanceCommitTarget(snapshot, ids[0], 2))
	require.NoError(t, err)
	require.False(t, committed)
	require.Len(t, conditional.received, 2)
	require.Equal(t, []clientv3.Cmp{expected}, conditional.received[1])
	require.Equal(t, uint64(1), manager.GetAffinityGroupState(ids[0]).LeaderStoreID)
	require.Equal(t, uint64(1), loadBalanceGroup(t, manager, ids[0]).LeaderStoreID)
}

func TestPrepareBalanceCommitRejectsIndependentRangeWatcherUpdates(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*testing.T, *Manager, string)
	}{
		{
			name: "labeler-ahead-of-range-cache",
			apply: func(t *testing.T, manager *Manager, groupID string) {
				rule := MakeLabelRule(GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{9}, EndKey: []byte{10}}}})
				require.NoError(t, manager.regionLabeler.SetLabelRule(rule))
			},
		},
		{
			name: "range-cache-ahead-of-labeler",
			apply: func(t *testing.T, manager *Manager, groupID string) {
				rule := MakeLabelRule(GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{9}, EndKey: []byte{10}}}})
				data, err := json.Marshal(rule)
				require.NoError(t, err)
				rule, err = labeler.NewLabelRuleFromJSON(data)
				require.NoError(t, err)
				require.NoError(t, manager.SyncKeyRangesFromEtcd(rule))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newBalanceRuntimeManager(t)
			groupID := seedBalanceGroups(t, manager, []uint64{1})[0]
			tc.apply(t, manager, groupID)
			snapshot := manager.GetBalanceSnapshot(groupID)
			require.NotNil(t, snapshot)
			require.True(t, manager.BalanceSnapshotCurrent(snapshot))

			err := manager.PrepareBalanceCommit(t.Context(), snapshot)
			require.True(t, errs.ErrEtcdTxnConflict.Equal(err))
		})
	}
}

func TestPrepareBalanceCommitIgnoresRangeOrder(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ranges := GroupKeyRanges{GroupID: "_tidb_pt_10_p1", KeyRanges: []keyutil.KeyRange{
		{StartKey: []byte{0}, EndKey: []byte{1}}, {StartKey: []byte{2}, EndKey: []byte{3}},
	}}
	require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{ranges}))
	reordered := cloneBalanceRanges(ranges)
	slices.Reverse(reordered.KeyRanges)
	require.NoError(t, manager.regionLabeler.SetLabelRule(MakeLabelRule(reordered)))

	snapshot := manager.GetBalanceSnapshot(ranges.GroupID)
	require.NotNil(t, snapshot)
	require.NoError(t, manager.PrepareBalanceCommit(t.Context(), snapshot))
	require.Equal(t, ranges, snapshot.Ranges[ranges.GroupID])
}

func TestPrepareBalanceCommitIgnoresSimilarTableKeys(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	otherID := "_tidb_pt_10_pother_p1"
	require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{{
		GroupID: otherID,
		KeyRanges: []keyutil.KeyRange{{
			StartKey: []byte{10},
			EndKey:   []byte{11},
		}},
	}}))

	snapshot := manager.GetBalanceSnapshot(ids[0])
	require.NotNil(t, snapshot)
	require.NoError(t, manager.PrepareBalanceCommit(t.Context(), snapshot))
}

func TestBalanceCommitDoesNotOverwriteLaterWatcherTarget(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	snapshot := prepareBalanceCommit(t, manager, ids[0])
	base := manager.storage.(storage.Storage)
	manager.storage = &balanceSamplingStorage{Storage: base, after: func() {
		later := manager.GetAffinityGroupState(ids[0]).Group
		later.LeaderStoreID = 3
		require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
			return base.SaveAffinityGroup(txn, later.ID, &later)
		}))
		manager.SyncGroupFromEtcd(&later)
	}}

	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, balanceCommitTarget(snapshot, ids[0], 2))
	require.NoError(t, err)
	require.True(t, committed)
	require.Equal(t, uint64(3), manager.GetAffinityGroupState(ids[0]).LeaderStoreID)
	require.Equal(t, uint64(3), loadBalanceGroup(t, manager, ids[0]).LeaderStoreID)
	key := endpoint.AffinityTableRevisionKey(snapshot.TableID)
	stored, err := base.Load(key)
	require.NoError(t, err)
	require.NotEqual(t, stored, snapshot.dependencies[key])
}

func TestBalanceCommitDoesNotPublishToRebuiltGroupWithSameID(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1})
	snapshot := prepareBalanceCommit(t, manager, ids[0])
	base := manager.storage.(storage.Storage)
	rebuilt := manager.GetAffinityGroupState(ids[0]).Group
	rebuilt.CreateTimestamp++
	manager.storage = &balanceSamplingStorage{Storage: base, after: func() {
		require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
			return base.SaveAffinityGroup(txn, rebuilt.ID, &rebuilt)
		}))
		manager.SyncGroupFromEtcd(&rebuilt)
	}}

	committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, balanceCommitTarget(snapshot, ids[0], 2))
	require.NoError(t, err)
	require.True(t, committed)
	current := manager.GetAffinityGroupState(ids[0])
	require.Equal(t, rebuilt.CreateTimestamp, current.CreateTimestamp)
	require.Equal(t, uint64(1), current.LeaderStoreID)
	persisted := loadBalanceGroup(t, manager, ids[0])
	require.Equal(t, rebuilt.CreateTimestamp, persisted.CreateTimestamp)
	require.Equal(t, uint64(1), persisted.LeaderStoreID)
}

func TestDeleteAffinityGroupsCleansOnlyEmptyTableRevisions(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	base := manager.storage.(storage.Storage)
	otherID := "_tidb_pt_10_pother_p1"
	require.NoError(t, manager.CreateAffinityGroups([]GroupKeyRanges{{GroupID: otherID}}))
	otherToken, err := base.Load(endpoint.AffinityTableRevisionKey(otherID))
	require.NoError(t, err)
	tokens := []string{endpoint.AffinityTableRevisionKey(ids[0]), endpoint.AffinityTableMetadataRevisionKey(ids[0])}
	require.NoError(t, manager.DeleteAffinityGroups(ids[:1], true))
	for _, key := range tokens {
		value, err := base.Load(key)
		require.NoError(t, err)
		require.NotEmpty(t, value, "remaining partitions retain their shared token")
	}
	require.NoError(t, manager.DeleteAffinityGroups(ids[1:], true))
	require.NoError(t, manager.DeleteAffinityGroups([]string{ids[0], "custom-missing"}, true))
	for _, id := range []string{ids[0], "custom-missing"} {
		for _, key := range []string{endpoint.AffinityTableRevisionKey(id), endpoint.AffinityTableMetadataRevisionKey(id)} {
			value, err := base.Load(key)
			require.NoError(t, err)
			require.Empty(t, value, "deleting absent groups must leave no version keys")
		}
	}
	currentOtherToken, err := base.Load(endpoint.AffinityTableRevisionKey(otherID))
	require.NoError(t, err)
	require.Equal(t, otherToken, currentOtherToken)
}

func TestBalanceCommitRejectsDeletedOrRecreatedTable(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, recreate := range []bool{false, true} {
			name := "versioned"
			if legacy {
				name = "missing tokens"
			}
			if recreate {
				name += "/recreated"
			} else {
				name += "/deleted"
			}
			t.Run(name, func(t *testing.T) {
				manager := newBalanceRuntimeManager(t)
				id := seedBalanceGroups(t, manager, []uint64{1})[0]
				base := manager.storage.(storage.Storage)
				if legacy {
					require.NoError(t, base.Remove(endpoint.AffinityTableRevisionKey(id)))
					require.NoError(t, base.Remove(endpoint.AffinityTableMetadataRevisionKey(id)))
				}
				snapshot := prepareBalanceCommit(t, manager, id)
				original := snapshot.Groups[0].Group
				labelKey := keypath.RegionLabelKeyPath(GetLabelRuleID(id))
				label, err := base.Load(labelKey)
				require.NoError(t, err)
				// Keep Manager stale, as it can be while MCS watchers catch up.
				require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
					if err := base.DeleteAffinityGroup(txn, id); err != nil {
						return err
					}
					return txn.Remove(labelKey)
				}))
				require.NoError(t, manager.cleanupBalanceRevisions(base, snapshot.TableID))
				if recreate {
					require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
						if err := base.SaveAffinityGroup(txn, id, &original); err != nil {
							return err
						}
						return txn.Save(labelKey, label)
					}))
				}
				committed, err := manager.CommitBalanceTarget(t.Context(), snapshot, balanceCommitTarget(snapshot, id, 2))
				require.NoError(t, err)
				require.False(t, committed)
				if recreate {
					require.Equal(t, uint64(1), loadBalanceGroup(t, manager, id).LeaderStoreID)
				} else {
					value, err := base.LoadAffinityGroup(id)
					require.NoError(t, err)
					require.Empty(t, value)
				}
			})
		}
	}
}

type balanceCleanupSamplingStorage struct {
	storage.Storage
	afterEmptyRange func()
}

func (s *balanceCleanupSamplingStorage) LoadRange(start, end string, limit int) (keys, values []string, err error) {
	keys, values, err = s.Storage.LoadRange(start, end, limit)
	if err == nil && len(keys) == 0 && s.afterEmptyRange != nil && start == keypath.RegionLabelKeyPath(GetLabelRuleID("_tidb_pt_10_p")) {
		s.afterEmptyRange()
	}
	return keys, values, err
}

func TestBalanceCleanupDoesNotDeleteRecreatedTable(t *testing.T) {
	_, client, cleanup := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer cleanup()
	manager := newBalanceRuntimeManager(t)
	base := storage.NewStorageWithEtcdBackend(client)
	manager.storage = base
	id := seedBalanceGroups(t, manager, []uint64{1})[0]
	recreated := manager.GetAffinityGroupState(id).Group
	require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
		if err := base.DeleteAffinityGroup(txn, id); err != nil {
			return err
		}
		return base.DeleteRegionRule(txn, GetLabelRuleID(id))
	}))
	manager.storage = &balanceCleanupSamplingStorage{Storage: base, afterEmptyRange: func() {
		require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
			return base.SaveAffinityGroup(txn, id, &recreated)
		}))
	}}

	err := manager.cleanupBalanceRevisions(base, TableGroupPrefix(id))

	require.True(t, errs.ErrEtcdTxnConflict.Equal(err))
	require.Equal(t, recreated, loadBalanceGroup(t, manager, id))
	for _, key := range []string{endpoint.AffinityTableRevisionKey(id), endpoint.AffinityTableMetadataRevisionKey(id)} {
		value, err := base.Load(key)
		require.NoError(t, err)
		require.NotEmpty(t, value)
	}
}
