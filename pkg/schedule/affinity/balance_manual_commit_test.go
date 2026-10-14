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
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keyutil"
)

type balanceTxnCountingStorage struct {
	storage.Storage
	transactions int
}

func (s *balanceTxnCountingStorage) RunInTxn(ctx context.Context, f func(kv.Txn) error) error {
	s.transactions++
	return s.Storage.RunInTxn(ctx, f)
}

func TestCommitBalanceTargetsUsesOneTransactionAndRetainsPolicy(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	snapshot := prepareBalanceCommit(t, manager, ids[0])
	base := manager.storage.(storage.Storage)
	counting := &balanceTxnCountingStorage{Storage: base}
	manager.storage = counting

	committed, err := manager.CommitBalanceTargets(t.Context(), snapshot, []*Group{
		balanceCommitTarget(snapshot, ids[0], 2),
		balanceCommitTarget(snapshot, ids[1], 3),
	})

	require.NoError(t, err)
	require.True(t, committed)
	require.Equal(t, 1, counting.transactions)
	for i, expectedLeader := range []uint64{2, 3} {
		persisted := loadBalanceGroup(t, manager, ids[i])
		require.Equal(t, expectedLeader, persisted.LeaderStoreID)
		require.Equal(t, BalancePolicyAuto, persisted.BalancePolicy)
		require.Equal(t, BalancePolicyAuto, manager.GetAffinityGroupState(ids[i]).BalancePolicy)
	}
}

func TestCommitBalanceTargetsDoesNotOverwriteWatcherUpdate(t *testing.T) {
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

	committed, err := manager.CommitBalanceTargets(t.Context(), snapshot, []*Group{
		balanceCommitTarget(snapshot, ids[0], 2),
		balanceCommitTarget(snapshot, ids[1], 3),
	})

	require.NoError(t, err)
	require.True(t, committed)
	require.Equal(t, uint64(3), manager.GetAffinityGroupState(ids[0]).LeaderStoreID)
	require.Equal(t, uint64(3), loadBalanceGroup(t, manager, ids[0]).LeaderStoreID)
	require.Equal(t, uint64(3), manager.GetAffinityGroupState(ids[1]).LeaderStoreID)
	require.Equal(t, uint64(3), loadBalanceGroup(t, manager, ids[1]).LeaderStoreID)
}

func TestCommitBalanceTargetsKeepsFirstBatchAfterLaterTargetConflict(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	ids := seedBalanceGroups(t, manager, []uint64{1, 1})
	firstSnapshot := prepareBalanceCommit(t, manager, ids[0])
	committed, err := manager.CommitBalanceTargets(t.Context(), firstSnapshot, []*Group{
		balanceCommitTarget(firstSnapshot, ids[0], 2),
	})
	require.NoError(t, err)
	require.True(t, committed)

	secondSnapshot := prepareBalanceCommit(t, manager, ids[0])
	_, err = manager.UpdateAffinityGroupPeers(ids[1], 2, []uint64{1, 2, 3})
	require.NoError(t, err)
	committed, err = manager.CommitBalanceTargets(t.Context(), secondSnapshot, []*Group{
		balanceCommitTarget(secondSnapshot, ids[1], 3),
	})

	require.True(t, errs.ErrEtcdTxnConflict.Equal(err))
	require.False(t, committed)
	require.Equal(t, uint64(2), loadBalanceGroup(t, manager, ids[0]).LeaderStoreID)
	require.Equal(t, uint64(2), loadBalanceGroup(t, manager, ids[1]).LeaderStoreID)
}

func TestRefreshBalanceSnapshotAcceptsRegionGenerationChange(t *testing.T) {
	for _, duringRead := range []bool{false, true} {
		name := "before refresh"
		if duringRead {
			name = "during dependency read"
		}
		t.Run(name, func(t *testing.T) {
			manager := newBalanceRuntimeManager(t)
			groupID := seedBalanceGroups(t, manager, []uint64{1})[0]
			expected := prepareBalanceCommit(t, manager, groupID)
			region := manager.regions[1].balanceInput()
			heartbeat := func() {
				manager.ObserveBalanceRegion(region.Clone(core.WithLeader(&metapb.Peer{Id: 2, StoreId: 2})))
			}
			if duringRead {
				manager.storage = &balanceSamplingStorage{Storage: manager.storage.(storage.Storage), after: heartbeat}
			} else {
				heartbeat()
			}

			refreshed, err := manager.RefreshBalanceSnapshot(t.Context(), expected)

			require.NoError(t, err)
			version, exists := manager.GetBalanceVersion(groupID)
			require.True(t, exists)
			require.Greater(t, version.Table, expected.Version.Table)
			require.Equal(t, expected.Groups[0].Group, refreshed.Groups[0].Group)
			require.Equal(t, expected.dependencies, refreshed.dependencies)
			require.NoError(t, manager.CheckBalanceDependencies(t.Context(), refreshed))
		})
	}
}

func TestRefreshBalanceSnapshotRejectsRangeOrGroupMetadataChange(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *Manager, string)
	}{
		{
			name: "range",
			change: func(t *testing.T, manager *Manager, groupID string) {
				rule := MakeLabelRule(GroupKeyRanges{GroupID: groupID, KeyRanges: []keyutil.KeyRange{{StartKey: []byte{8}, EndKey: []byte{9}}}})
				data, err := json.Marshal(rule)
				require.NoError(t, err)
				rule, err = labeler.NewLabelRuleFromJSON(data)
				require.NoError(t, err)
				require.NoError(t, manager.SyncKeyRangesFromEtcd(rule))
			},
		},
		{
			name: "group-policy",
			change: func(t *testing.T, manager *Manager, groupID string) {
				_, err := manager.UpdateAffinityGroupPolicy(groupID, BalancePolicyFixed)
				require.NoError(t, err)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newBalanceRuntimeManager(t)
			groupID := seedBalanceGroups(t, manager, []uint64{1})[0]
			expected := prepareBalanceCommit(t, manager, groupID)
			tc.change(t, manager, groupID)

			refreshed, err := manager.RefreshBalanceSnapshot(t.Context(), expected)

			require.Nil(t, refreshed)
			require.True(t, errs.ErrEtcdTxnConflict.Equal(err))
		})
	}
}

func TestRefreshBalanceSnapshotDoesNotCaptureLaterPrimaryTerm(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	groupID := seedBalanceGroups(t, manager, []uint64{1})[0]
	base := manager.storage.(storage.Storage)
	termKey, termValue := "affinity-test-primary", "primary-term-1"
	require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return txn.Save(termKey, termValue)
	}))
	expectedTerm := clientv3.Compare(clientv3.Value(termKey), "=", termValue)
	conditional := &balanceConditionalStorage{Storage: base, expected: expectedTerm}
	manager.storage = conditional
	manager.SetBalanceCommitGuard(func() ([]clientv3.Cmp, bool) {
		return []clientv3.Cmp{expectedTerm}, true
	}, nil)
	snapshot := manager.GetBalanceSnapshot(groupID)
	require.NotNil(t, snapshot)
	require.NoError(t, manager.PrepareBalanceCommit(t.Context(), snapshot))
	require.Len(t, conditional.received, 1)

	require.NoError(t, base.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return txn.Save(termKey, "primary-term-2")
	}))
	refreshed, err := manager.RefreshBalanceSnapshot(t.Context(), snapshot)

	require.Nil(t, refreshed)
	require.True(t, errs.ErrEtcdTxnConflict.Equal(err))
	require.Len(t, conditional.received, 2)
	require.Equal(t, []clientv3.Cmp{expectedTerm}, conditional.received[1])
	require.Equal(t, uint64(1), manager.GetAffinityGroupState(groupID).LeaderStoreID)
	require.Equal(t, uint64(1), loadBalanceGroup(t, manager, groupID).LeaderStoreID)
}
