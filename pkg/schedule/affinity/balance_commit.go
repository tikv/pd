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

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// SetBalanceCommitGuard installs the current authority and placement metadata
// providers. A term is captured before planning, never renewed at submission.
func (m *Manager) SetBalanceCommitGuard(term func() ([]clientv3.Cmp, bool), metadata func() map[string]string) {
	m.Lock()
	defer m.Unlock()
	if term != nil {
		m.balanceTerm = term
	}
	if metadata != nil {
		m.balanceMetadata = metadata
	}
}

// SetBalanceAdmission supplies the affinity checker's current execution gate.
func (m *Manager) SetBalanceAdmission(check func() bool) {
	m.Lock()
	defer m.Unlock()
	m.balanceAdmission = check
}

func balanceConflict() error { return errs.ErrEtcdTxnConflict.FastGenByArgs() }

func balanceBatchError(err error, committed int) error {
	if committed == 0 {
		return err
	}
	return errors.Annotatef(err, "affinity rebalance already committed %d groups", committed)
}

func canonicalJSON(value string) string {
	var decoded any
	if json.Unmarshal([]byte(value), &decoded) != nil {
		return value
	}
	data, _ := json.Marshal(decoded)
	return string(data)
}

func (m *Manager) captureBalanceDependencies(ctx context.Context, snapshot *balanceSnapshot) error {
	if snapshot.dependencies != nil {
		return nil
	}
	m.RLock()
	term, metadata := m.balanceTerm, m.balanceMetadata
	m.RUnlock()
	if term != nil {
		var ok bool
		snapshot.conditions, ok = term()
		if !ok {
			return balanceConflict()
		}
	}
	reader, ok := m.storage.(kv.BaseReadWrite)
	if !ok {
		return balanceConflict()
	}
	expected := make(map[string]string)
	for _, group := range snapshot.groups {
		data, _ := json.Marshal(group.Group)
		expected[keypath.AffinityGroupPath(group.ID)] = canonicalJSON(string(data))
	}
	snapshot.metadata = make(map[string]string)
	if metadata != nil {
		for key, value := range metadata() {
			snapshot.metadata[key] = canonicalJSON(value)
		}
	}
	m.regionLabeler.IterateLabelRules(func(rule *labeler.LabelRule) bool {
		data, _ := json.Marshal(rule)
		snapshot.metadata[keypath.RegionLabelKeyPath(rule.ID)] = canonicalJSON(string(data))
		return true
	})
	snapshot.dependencies = make(map[string]string, 2)
	if err := m.runBalanceTxn(ctx, snapshot.conditions, func(txn kv.Txn) error {
		for _, key := range []string{endpoint.AffinityTableRevisionKey(snapshot.tableID), endpoint.AffinityMetadataRevisionKey()} {
			value, err := txn.Load(key)
			if err != nil {
				return err
			}
			snapshot.dependencies[key] = value
		}
		// Compare the authoritative inputs with the in-memory watcher snapshot.
		// The generation reads fence additions and deletions as well as updates;
		// data reads deliberately add no per-key transaction comparisons.
		prefix := keypath.AffinityGroupPath(snapshot.tableID)
		end := prefix + "\x00"
		if slices.ContainsFunc(snapshot.groups, func(g *GroupState) bool { return g.ID != snapshot.tableID }) {
			prefix += "_p"
			end = clientv3.GetPrefixRangeEnd(prefix)
		}
		keys, values, err := reader.LoadRange(prefix, end, 0)
		if err != nil {
			return err
		}
		actual := make(map[string]string, len(keys))
		for i, key := range keys {
			var group Group
			if err := json.Unmarshal([]byte(values[i]), &group); err != nil {
				return err
			}
			group.VoterStoreIDs = normalizeVoters(group.VoterStoreIDs)
			data, _ := json.Marshal(group)
			actual[key] = canonicalJSON(string(data))
		}
		if !maps.Equal(expected, actual) {
			return balanceConflict()
		}
		actual = make(map[string]string)
		prefixes := []string{keypath.RegionLabelPathPrefix()}
		if metadata != nil {
			prefixes = append(prefixes, keypath.RulesPathPrefix(), keypath.RuleGroupPathPrefix())
		}
		for _, prefix := range prefixes {
			keys, values, err = reader.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), 0)
			if err != nil {
				return err
			}
			for i, key := range keys {
				actual[key] = canonicalJSON(values[i])
			}
		}
		if !maps.Equal(snapshot.metadata, actual) {
			return balanceConflict()
		}
		return nil
	}); err != nil {
		snapshot.dependencies = nil
		return err
	}
	m.Lock()
	if state := m.balanceTables[snapshot.tableID]; state != nil && m.balanceFingerprintLocked(state) == snapshot.fingerprint {
		state.dependencies = maps.Clone(snapshot.dependencies)
		state.dependencyConditions = slices.Clone(snapshot.conditions)
		state.dependencyFingerprint = snapshot.fingerprint
	}
	m.Unlock()
	return nil
}

func (m *Manager) runBalanceTxn(ctx context.Context, conditions []clientv3.Cmp, f func(kv.Txn) error) error {
	if len(conditions) == 0 {
		return m.storage.RunInTxn(ctx, f)
	}
	runner, ok := m.storage.(kv.ConditionalTxnRunner)
	if !ok {
		return balanceConflict()
	}
	return runner.RunInTxnWithConditions(ctx, conditions, f)
}

func (m *Manager) balanceSnapshotCurrent(snapshot *balanceSnapshot) bool {
	m.RLock()
	defer m.RUnlock()
	table := m.balanceTables[snapshot.tableID]
	return table != nil && m.balanceFingerprintLocked(table) == snapshot.fingerprint
}

// commitBalancePlans protects table scoring and placement dependencies, not only
// the selected group. No Manager or RuleManager lock is held during network IO.
func (m *Manager) commitBalancePlans(ctx context.Context, snapshot *balanceSnapshot, plans []BalancePlan) (bool, error) {
	defer m.releaseBalanceDependencies(snapshot)
	if snapshot.admission != nil && !snapshot.admission() {
		return false, nil
	}
	changed := slices.DeleteFunc(slices.Clone(plans), func(plan BalancePlan) bool { return !plan.Changed })
	const batchSize = etcdutil.MaxEtcdTxnOps - 8
	if len(changed) > batchSize {
		// A manual table plan may exceed etcd's transaction limit. Keep every
		// batch fenced to the original authority, and recapture table inputs
		// after our own writes. A concurrent edit aborts the remaining batches.
		conditions := slices.Clone(snapshot.conditions)
		expectedGroups := make(map[string]Group, len(snapshot.groups))
		for _, group := range snapshot.groups {
			expectedGroups[group.ID] = group.Group
		}
		current := snapshot
		for start := 0; start < len(changed); start += batchSize {
			end := min(start+batchSize, len(changed))
			applied, err := m.commitBalancePlans(ctx, current, changed[start:end])
			if err != nil {
				return false, balanceBatchError(err, start)
			}
			if !applied {
				return false, balanceBatchError(balanceConflict(), start)
			}
			for _, plan := range changed[start:end] {
				group := expectedGroups[plan.GroupID]
				group.LeaderStoreID, group.VoterStoreIDs = plan.LeaderStoreID, normalizeVoters(plan.VoterStoreIDs)
				expectedGroups[plan.GroupID] = group
			}
			if end < len(changed) {
				current = m.balanceSnapshot(snapshot.groups[0].ID)
				if current == nil {
					return false, balanceBatchError(balanceConflict(), end)
				}
				if err := m.captureBalanceDependencies(ctx, current); err != nil {
					return false, balanceBatchError(err, end)
				}
				if len(current.groups) != len(expectedGroups) || current.fingerprint.stores != snapshot.fingerprint.stores ||
					current.fingerprint.rules != snapshot.fingerprint.rules ||
					current.dependencies[endpoint.AffinityMetadataRevisionKey()] != snapshot.dependencies[endpoint.AffinityMetadataRevisionKey()] {
					return false, balanceBatchError(balanceConflict(), end)
				}
				for _, group := range current.groups {
					expected := expectedGroups[group.ID]
					if group.String() != expected.String() {
						return false, balanceBatchError(balanceConflict(), end)
					}
				}
				current.conditions = conditions
			}
		}
		snapshot.applied = true
		return true, nil
	}
	if !m.balanceSnapshotCurrent(snapshot) {
		return false, nil
	}
	originals := make(map[string]*GroupState, len(snapshot.groups))
	for _, group := range snapshot.groups {
		originals[group.ID] = group
	}
	updates := make([]Group, 0)
	for _, plan := range plans {
		if !plan.Changed {
			continue
		}
		old := originals[plan.GroupID]
		if old == nil || old.BalancePolicy == BalancePolicyFixed {
			return false, nil
		}
		group := old.Group
		group.LeaderStoreID, group.VoterStoreIDs = plan.LeaderStoreID, normalizeVoters(plan.VoterStoreIDs)
		if err := m.AdjustGroup(&group); err != nil {
			return false, err
		}
		if err := m.checkBalanceTargetStores(group.LeaderStoreID, group.VoterStoreIDs); err != nil {
			return false, nil
		}
		updates = append(updates, group)
	}
	if len(updates) == 0 {
		return false, nil
	}
	// Serialize local metadata writers, but keep the checker-facing mutex free.
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	err := m.runBalanceTxn(ctx, snapshot.conditions, func(txn kv.Txn) error {
		for key, expected := range snapshot.dependencies {
			value, err := txn.Load(key)
			if err != nil {
				return err
			}
			if value != expected {
				return balanceConflict()
			}
		}
		if !m.balanceSnapshotCurrent(snapshot) || (snapshot.admission != nil && !snapshot.admission()) {
			return balanceConflict()
		}
		for i := range updates {
			if err := m.storage.SaveAffinityGroup(txn, updates[i].ID, &updates[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if errs.ErrEtcdTxnConflict.Equal(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, group := range updates {
		if _, err := m.updateGroupPeers(group.ID, group.LeaderStoreID, group.VoterStoreIDs); err != nil {
			return false, err
		}
	}
	m.Lock()
	if table := m.balanceTables[snapshot.tableID]; table != nil {
		table.phase = balanceConverging
	}
	m.Unlock()
	snapshot.applied = true
	return true, nil
}
