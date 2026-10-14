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
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"slices"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/keyutil"
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
		m.balanceMetadataSource++
		m.balanceMetadataVerified = nil
	}
}

// SetBalanceAdmission supplies the affinity checker's current execution gate.
func (m *Manager) SetBalanceAdmission(check func() bool) {
	m.Lock()
	defer m.Unlock()
	m.balanceAdmission = check
}

func balanceConflict() error { return errs.ErrEtcdTxnConflict.FastGenByArgs() }

func canonicalJSON(value string) string {
	var decoded any
	if json.Unmarshal([]byte(value), &decoded) != nil {
		return value
	}
	data, _ := json.Marshal(decoded)
	return string(data)
}

// balanceMetadataVersion records a successful comparison with authoritative
// metadata. Local generations prevent reusing it while a watcher is behind.
type balanceMetadataVersion struct {
	token  string
	rules  uint64
	labels uint64
	source uint64
}

func (m *Manager) balanceMetadataCurrent(version balanceMetadataVersion, ruleRevision func() uint64) bool {
	return m.regionLabeler.GetRevision() == version.labels &&
		(ruleRevision == nil || ruleRevision() == version.rules)
}

// balanceSnapshotRangesCurrent fences the independent affinity and labeler
// watchers. A globally verified label revision does not prove that this table's
// cached ranges have caught up with the label rules at that revision.
func (m *Manager) balanceSnapshotRangesCurrent(snapshot *balanceSnapshot) bool {
	matched := true
	m.regionLabeler.RLock()
	for _, group := range snapshot.groups {
		actual, err := extractKeyRangesFromLabelRule(m.regionLabeler.GetLabelRuleLocked(GetLabelRuleID(group.ID)))
		expected := snapshot.ranges[group.ID]
		if err != nil || len(expected.KeyRanges) != len(actual.KeyRanges) {
			matched = false
			break
		}
		if len(expected.KeyRanges) == 0 {
			continue // Missing rules and empty rules both describe no ranges.
		}
		// Range order has no scheduling meaning. Sort private slices to avoid
		// mutating Manager or RegionLabeler data while comparing coverage.
		expected.KeyRanges = slices.Clone(expected.KeyRanges)
		compare := func(a, b keyutil.KeyRange) int {
			if cmp := bytes.Compare(a.StartKey, b.StartKey); cmp != 0 {
				return cmp
			}
			return bytes.Compare(a.EndKey, b.EndKey)
		}
		slices.SortFunc(expected.KeyRanges, compare)
		slices.SortFunc(actual.KeyRanges, compare)
		if !balanceRangesEqual(expected, actual) {
			matched = false
			break
		}
	}
	m.regionLabeler.RUnlock()
	return matched
}

func (m *Manager) captureBalanceDependencies(ctx context.Context, snapshot *balanceSnapshot) error {
	if snapshot.dependencies != nil {
		// Recheck only this table's ranges. The labeler revision is global, so
		// comparing it here would make an unrelated table's label update abort
		// this table's in-flight plan.
		if !m.balanceSnapshotRangesCurrent(snapshot) {
			return balanceConflict()
		}
		return nil
	}
	// Every table must verify its own ranges before consulting the shared
	// global metadata fast path.
	if !m.balanceSnapshotRangesCurrent(snapshot) {
		return balanceConflict()
	}
	m.RLock()
	term, metadata, ruleRevision := m.balanceTerm, m.balanceMetadata, m.balanceRuleRevision
	version := balanceMetadataVersion{source: m.balanceMetadataSource}
	verified := m.balanceMetadataVerified
	m.RUnlock()
	// Label revisions are global and must not be part of a table fingerprint.
	// Capture the current revision only for the short authoritative metadata
	// comparison below; table range equality fences the independent watchers.
	version.labels = m.regionLabeler.GetRevision()
	if ruleRevision != nil {
		version.rules = ruleRevision()
	}
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
	metadataKey := endpoint.AffinityTableMetadataRevisionKey(snapshot.tableID)
	snapshot.dependencies = make(map[string]string, 2)
	var globalMetadataToken string
	if err := m.runBalanceTxn(ctx, snapshot.conditions, func(txn kv.Txn) error {
		for _, key := range []string{endpoint.AffinityTableRevisionKey(snapshot.tableID), metadataKey} {
			value, err := txn.Load(key)
			if err != nil {
				return err
			}
			snapshot.dependencies[key] = value
		}
		// Keep the cluster-wide token only for the shared metadata verification
		// cache. It is deliberately not a submission dependency: unrelated
		// affinity tables must not abort this table's plan.
		var err error
		globalMetadataToken, err = txn.Load(endpoint.AffinityMetadataRevisionKey())
		if err != nil {
			return err
		}
		version.token = globalMetadataToken
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
		// A version already verified by another table needs only the token
		// comparisons above, not another cluster-wide read and serialization.
		if verified != nil && *verified == version && (metadata == nil || ruleRevision != nil) {
			if !m.balanceMetadataCurrent(version, ruleRevision) {
				return balanceConflict()
			}
			return nil
		}
		expectedMetadata := make(map[string]string)
		if metadata != nil {
			for key, value := range metadata() {
				expectedMetadata[key] = canonicalJSON(value)
			}
		}
		m.regionLabeler.IterateLabelRules(func(rule *labeler.LabelRule) bool {
			data, _ := json.Marshal(rule)
			expectedMetadata[keypath.RegionLabelKeyPath(rule.ID)] = canonicalJSON(string(data))
			return true
		})
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
		if !maps.Equal(expectedMetadata, actual) || !m.balanceMetadataCurrent(version, ruleRevision) {
			return balanceConflict()
		}
		return nil
	}); err != nil {
		snapshot.dependencies = nil
		return err
	}
	if !m.balanceMetadataCurrent(version, ruleRevision) {
		snapshot.dependencies = nil
		return balanceConflict()
	}
	m.Lock()
	defer m.Unlock()
	if m.balanceMetadataSource != version.source {
		snapshot.dependencies = nil
		return balanceConflict()
	}
	m.balanceMetadataVerified = &version
	if state := m.balanceTables[snapshot.tableID]; state != nil && m.balanceFingerprintLocked(state) == snapshot.fingerprint {
		state.dependencies = maps.Clone(snapshot.dependencies)
		state.dependencyConditions = slices.Clone(snapshot.conditions)
		state.dependencyFingerprint = snapshot.fingerprint
	}
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
	// Both automatic and manual callers submit one group and wait before the
	// next target. Reject batches rather than introducing a second apply path.
	if len(changed) != 1 {
		return false, balanceConflict()
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
		if _, err := m.updateGroupTarget(group); err != nil {
			return false, err
		}
	}
	m.Lock()
	if table := m.balanceTables[snapshot.tableID]; table != nil {
		table.phase = balanceConverging
		table.converging = &balanceConvergence{target: updates[0], ranges: cloneBalanceRanges(snapshot.ranges[updates[0].ID])}
	}
	m.Unlock()
	snapshot.applied = true
	return true, nil
}
