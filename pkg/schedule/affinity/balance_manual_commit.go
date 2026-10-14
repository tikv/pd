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
	"maps"
	"slices"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

// Revision reports the durable target, range and placement versions used by
// a preview. Apply captures its own dependencies instead of replaying a preview.
func (snapshot *BalanceSnapshot) Revision() string {
	encoded, _ := json.Marshal(struct {
		Table     string `json:"table"`
		Ranges    string `json:"ranges"`
		Placement string `json:"placement"`
	}{snapshot.dependencies[endpoint.AffinityTableRevisionKey(snapshot.TableID)],
		snapshot.dependencies[endpoint.AffinityTableMetadataRevisionKey(snapshot.TableID)],
		snapshot.dependencies[endpoint.AffinityPlacementRevisionKey()]})
	return string(encoded)
}

// RefreshBalanceSnapshot keeps a manual operation's expected targets, ranges
// and authority while refreshing Region and shared-metadata generations.
// The checker must also compare the constraints affecting this table and
// revalidate targets before every write.
func (m *Manager) RefreshBalanceSnapshot(ctx context.Context, expected *BalanceSnapshot) (*BalanceSnapshot, error) {
	current := m.GetBalanceSnapshot(expected.TableID)
	if current == nil || len(current.Groups) != len(expected.Groups) ||
		current.Version.Ranges != expected.Version.Ranges || current.Version.Stores != expected.Version.Stores {
		return nil, balanceConflict()
	}
	for i, group := range current.Groups {
		old := expected.Groups[i]
		if group.ID != old.ID || group.CreateTimestamp != old.CreateTimestamp || group.BalancePolicy != old.BalancePolicy ||
			group.LeaderStoreID != old.LeaderStoreID || !slices.Equal(group.VoterStoreIDs, old.VoterStoreIDs) {
			return nil, balanceConflict()
		}
	}
	// Reconciliation may capture new durable placement versions after an
	// unrelated change, but can never borrow a later primary's authority.
	current.conditions, current.termCaptured = slices.Clone(expected.conditions), expected.termCaptured
	if current.Version.Rules == expected.Version.Rules && current.Version.Labels == expected.Version.Labels {
		// Normal migration heartbeats may advance the Region generation during
		// this read. The captured target, range and term conditions still apply.
		current.dependencies = maps.Clone(expected.dependencies)
		if current.dependencies == nil {
			return nil, balanceConflict()
		}
		if err := m.CheckBalanceDependencies(ctx, current); err != nil {
			return nil, err
		}
	} else {
		if err := m.PrepareBalanceCommit(ctx, current); err != nil {
			return nil, err
		}
	}
	for _, key := range []string{endpoint.AffinityTableRevisionKey(expected.TableID), endpoint.AffinityTableMetadataRevisionKey(expected.TableID)} {
		if current.dependencies[key] != expected.dependencies[key] {
			return nil, balanceConflict()
		}
	}
	return current, nil
}

// CheckBalanceDependencies verifies the captured durable versions and term.
// It does not compare Region generations, which change during convergence.
func (m *Manager) CheckBalanceDependencies(ctx context.Context, snapshot *BalanceSnapshot) error {
	return m.runBalanceTxn(ctx, snapshot.conditions, func(txn kv.Txn) error {
		return checkBalanceDependencies(txn, snapshot)
	})
}

// CommitBalanceTargets atomically submits one capacity-bounded manual batch.
// The caller validates every target and submits all batches before waiting.
func (m *Manager) CommitBalanceTargets(ctx context.Context, snapshot *BalanceSnapshot, targets []*Group) (bool, error) {
	if len(targets) == 0 || len(targets) > endpoint.MaxAffinityTxnBatchOps || !m.BalanceSnapshotCurrent(snapshot) {
		return false, balanceConflict()
	}
	groups := make([]Group, len(targets))
	originals := make([]*GroupState, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for i, target := range targets {
		index := slices.IndexFunc(snapshot.Groups, func(group *GroupState) bool { return group.ID == target.ID })
		if index < 0 || snapshot.Groups[index].BalancePolicy == BalancePolicyFixed {
			return false, balanceConflict()
		}
		if _, duplicate := seen[target.ID]; duplicate {
			return false, balanceConflict()
		}
		seen[target.ID] = struct{}{}
		originals[i] = snapshot.Groups[index]
		groups[i] = originals[i].Group
		groups[i].LeaderStoreID, groups[i].VoterStoreIDs = target.LeaderStoreID, normalizeVoters(target.VoterStoreIDs)
		if err := m.AdjustGroup(&groups[i]); err != nil {
			return false, err
		}
		if err := m.checkBalanceTargetStores(groups[i].LeaderStoreID, groups[i].VoterStoreIDs); err != nil {
			return false, nil
		}
	}
	var token string
	err := m.runBalanceTxn(ctx, snapshot.conditions, func(txn kv.Txn) error {
		if err := checkBalanceDependencies(txn, snapshot); err != nil {
			return err
		}
		if !m.BalanceSnapshotCurrent(snapshot) {
			return balanceConflict()
		}
		for i := range groups {
			if err := m.storage.SaveAffinityGroup(txn, groups[i].ID, &groups[i]); err != nil {
				return err
			}
		}
		var err error
		token, err = endpoint.SaveAffinityTableRevision(txn, snapshot.TableID)
		return err
	})
	if errs.ErrEtcdTxnConflict.Equal(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	snapshot.dependencies[endpoint.AffinityTableRevisionKey(snapshot.TableID)] = token
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	for i, group := range groups {
		original := originals[i]
		current := m.GetAffinityGroupState(group.ID)
		if current != nil && current.CreateTimestamp == original.CreateTimestamp && current.BalancePolicy == original.BalancePolicy &&
			current.LeaderStoreID == original.LeaderStoreID && slices.Equal(current.VoterStoreIDs, original.VoterStoreIDs) {
			if _, err := m.updateGroupTarget(group); err != nil {
				return true, err
			}
		}
		original.Group = group
	}
	return true, nil
}
