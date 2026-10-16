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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
)

func TestBalancePolicyValidationBoundaries(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	for _, tc := range []struct {
		id     string
		policy BalancePolicy
	}{
		{id: "valid-empty", policy: ""},
		{id: "valid-auto", policy: BalancePolicyAuto},
		{id: "valid-fixed", policy: BalancePolicyFixed},
	} {
		group := &Group{ID: tc.id, BalancePolicy: tc.policy}
		require.NoError(t, manager.AdjustGroup(group))
		manager.SyncGroupFromEtcd(group)
		require.NotNil(t, manager.GetAffinityGroupState(group.ID))
	}

	invalid := &Group{ID: "invalid-policy", BalancePolicy: BalancePolicy("manual")}
	require.ErrorIs(t, manager.AdjustGroup(invalid), errs.ErrAffinityGroupContent)
	manager.SyncGroupFromEtcd(invalid)
	require.Nil(t, manager.GetAffinityGroupState(invalid.ID))
}

func TestUpdateAffinityGroupPolicyPersistsAndInvalidates(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	groupID := seedBalanceGroups(t, manager, []uint64{1})[0]
	group := manager.GetAffinityGroupState(groupID)
	require.NotNil(t, group)
	affinityVersion := group.affinityVer
	balanceVersion := manager.balanceTables[TableGroupPrefix(groupID)].version

	updated, err := manager.UpdateAffinityGroupPolicy(groupID, BalancePolicyFixed)
	require.NoError(t, err)
	require.Equal(t, BalancePolicyFixed, updated.BalancePolicy)
	require.Empty(t, manager.GetAffinityGroupState(groupID).AffinityRegionCount)
	current := manager.GetAffinityGroupState(groupID)
	require.Greater(t, current.affinityVer, affinityVersion)
	require.Greater(t, manager.balanceTables[TableGroupPrefix(groupID)].version, balanceVersion)

	value, err := manager.storage.LoadAffinityGroup(groupID)
	require.NoError(t, err)
	var persisted Group
	require.NoError(t, json.Unmarshal([]byte(value), &persisted))
	require.Equal(t, BalancePolicyFixed, persisted.BalancePolicy)

	affinityVersion = current.affinityVer
	balanceVersion = manager.balanceTables[TableGroupPrefix(groupID)].version
	updated, err = manager.UpdateAffinityGroupPolicy(groupID, BalancePolicyFixed)
	require.NoError(t, err)
	require.Equal(t, affinityVersion, updated.affinityVer)
	require.Equal(t, balanceVersion, manager.balanceTables[TableGroupPrefix(groupID)].version)

	updated, err = manager.UpdateAffinityGroupPolicy(groupID, "")
	require.NoError(t, err)
	require.Equal(t, BalancePolicyAuto, updated.BalancePolicy)

	_, err = manager.UpdateAffinityGroupPolicy(groupID, BalancePolicy("manual"))
	require.ErrorIs(t, err, errs.ErrAffinityGroupContent)
	_, err = manager.UpdateAffinityGroupPolicy("missing", BalancePolicyAuto)
	require.ErrorIs(t, err, errs.ErrAffinityGroupNotFound)
}

func TestUpdateAffinityGroupPolicyRejectsStaleStorage(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	groupID := seedBalanceGroups(t, manager, []uint64{1})[0]
	remote := manager.GetAffinityGroupState(groupID).Group
	remote.LeaderStoreID = 2
	require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return manager.storage.SaveAffinityGroup(txn, groupID, &remote)
	}))

	_, err := manager.UpdateAffinityGroupPolicy(groupID, BalancePolicyFixed)
	require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
	require.Equal(t, BalancePolicyAuto, manager.GetAffinityGroupState(groupID).BalancePolicy)
	value, err := manager.storage.LoadAffinityGroup(groupID)
	require.NoError(t, err)
	var persisted Group
	require.NoError(t, json.Unmarshal([]byte(value), &persisted))
	require.Equal(t, BalancePolicyAuto, persisted.BalancePolicy)
	require.Equal(t, uint64(2), persisted.LeaderStoreID)
}

func TestUpdateAffinityGroupPolicyRejectsStaleNoop(t *testing.T) {
	for _, policy := range []BalancePolicy{BalancePolicyAuto, BalancePolicyFixed} {
		t.Run(string(policy), func(t *testing.T) {
			manager := newBalanceRuntimeManager(t)
			groupID := seedBalanceGroups(t, manager, []uint64{1})[0]
			_, err := manager.UpdateAffinityGroupPolicy(groupID, policy)
			require.NoError(t, err)

			remote := manager.GetAffinityGroupState(groupID).Group
			remote.BalancePolicy = BalancePolicyFixed
			if policy == BalancePolicyFixed {
				remote.BalancePolicy = BalancePolicyAuto
			}
			require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
				return manager.storage.SaveAffinityGroup(txn, groupID, &remote)
			}))

			// The watcher has not delivered the other instance's policy change.
			require.Equal(t, policy, manager.GetAffinityGroupState(groupID).BalancePolicy)
			_, err = manager.UpdateAffinityGroupPolicy(groupID, policy)
			require.ErrorIs(t, err, errs.ErrEtcdTxnConflict)
			require.Equal(t, remote, loadBalanceGroup(t, manager, groupID))
			require.Equal(t, policy, manager.GetAffinityGroupState(groupID).BalancePolicy)

			manager.SyncGroupFromEtcd(&remote)
			updated, err := manager.UpdateAffinityGroupPolicy(groupID, policy)
			require.NoError(t, err)
			require.Equal(t, policy, updated.BalancePolicy)
			remote.BalancePolicy = policy
			require.Equal(t, remote, loadBalanceGroup(t, manager, groupID))
		})
	}
}

func TestAdminPeersUpdatePreservesRemotePolicy(t *testing.T) {
	manager := newBalanceRuntimeManager(t)
	id := seedBalanceGroups(t, manager, []uint64{1})[0]
	remote := manager.GetAffinityGroupState(id).Group
	remote.BalancePolicy = BalancePolicyFixed
	require.NoError(t, manager.storage.RunInTxn(t.Context(), func(txn kv.Txn) error {
		return manager.storage.SaveAffinityGroup(txn, id, &remote)
	}))
	// Scheduling's policy write has not reached PD's local Manager.
	require.Equal(t, BalancePolicyAuto, manager.GetAffinityGroupState(id).BalancePolicy)
	updated, err := manager.UpdateAffinityGroupPeers(id, 2, []uint64{3, 1, 2})
	require.NoError(t, err)
	require.Equal(t, BalancePolicyFixed, updated.BalancePolicy)
	require.Equal(t, BalancePolicyFixed, manager.GetAffinityGroupState(id).BalancePolicy)
	value, err := manager.storage.LoadAffinityGroup(id)
	require.NoError(t, err)
	var persisted Group
	require.NoError(t, json.Unmarshal([]byte(value), &persisted))
	require.Equal(t, BalancePolicyFixed, persisted.BalancePolicy)
	require.Equal(t, uint64(2), persisted.LeaderStoreID)
	require.Equal(t, []uint64{1, 2, 3}, persisted.VoterStoreIDs)
}
