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
	"strings"

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

// balanceMetadataVersion caches one authoritative comparison of shared
// constraints. Affinity ranges are reconciled separately for every table.
type balanceMetadataVersion struct {
	token  string
	rules  uint64
	source uint64
}

func canonicalLabelJSON(rule *labeler.LabelRule) string {
	copy := *rule
	ranges := slices.Clone(rule.GetKeyRanges())
	slices.SortFunc(ranges, func(a, b *labeler.KeyRangeRule) int {
		if cmp := bytes.Compare(a.StartKey, b.StartKey); cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.EndKey, b.EndKey)
	})
	copy.Data = ranges
	data, _ := json.Marshal(&copy)
	return canonicalJSON(string(data))
}

func (m *Manager) balanceSnapshotLabels(snapshot *balanceSnapshot) (map[string]string, bool) {
	expected := make(map[string]string, len(snapshot.groups))
	m.regionLabeler.RLock()
	defer m.regionLabeler.RUnlock()
	for _, group := range snapshot.groups {
		rule := m.regionLabeler.GetLabelRuleLocked(GetLabelRuleID(group.ID))
		actual, err := extractKeyRangesFromLabelRule(rule)
		ranges := cloneBalanceRanges(snapshot.ranges[group.ID])
		sortRanges := func(ranges GroupKeyRanges) {
			slices.SortFunc(ranges.KeyRanges, func(a, b keyutil.KeyRange) int {
				if cmp := bytes.Compare(a.StartKey, b.StartKey); cmp != 0 {
					return cmp
				}
				return bytes.Compare(a.EndKey, b.EndKey)
			})
		}
		sortRanges(ranges)
		sortRanges(actual)
		if err != nil || !balanceRangesEqual(ranges, actual) {
			return nil, false
		}
		if rule != nil {
			expected[keypath.RegionLabelKeyPath(rule.ID)] = canonicalLabelJSON(rule)
		}
	}
	return expected, true
}

func balanceTableKeyRange(snapshot *balanceSnapshot, prefix string) (string, string) {
	for _, group := range snapshot.groups {
		if group.ID != snapshot.tableID {
			prefix += "_p"
			return prefix, clientv3.GetPrefixRangeEnd(prefix)
		}
	}
	return prefix, prefix + "\x00"
}

func (m *Manager) captureBalanceDependencies(ctx context.Context, snapshot *balanceSnapshot) error {
	if !m.balanceSnapshotCurrent(snapshot) {
		return balanceConflict()
	}
	// Sample before range reconciliation and verify after the authoritative
	// reads. Independent watchers cannot pair old ranges with a newer token.
	labels := m.regionLabeler.GetRevision()
	expectedLabels, matched := m.balanceSnapshotLabels(snapshot)
	if !matched {
		return balanceConflict()
	}
	m.RLock()
	term, metadata := m.balanceTerm, m.balanceMetadata
	version := balanceMetadataVersion{rules: snapshot.ruleRevision, source: m.balanceMetadataSource}
	verified := m.balanceMetadataVerified
	rulesCurrent := m.currentBalanceRuleRevisionLocked() == snapshot.ruleRevision
	m.RUnlock()
	if !rulesCurrent {
		return balanceConflict()
	}
	if snapshot.dependencies != nil {
		if labels != m.regionLabeler.GetRevision() {
			return balanceConflict()
		}
		return nil
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
	dependencies := make(map[string]string, 3)
	expectedGroups := make(map[string]string, len(snapshot.groups))
	for _, group := range snapshot.groups {
		data, _ := json.Marshal(group.Group)
		expectedGroups[keypath.AffinityGroupPath(group.ID)] = canonicalJSON(string(data))
	}
	loadMap := func(prefix, end string, normalize func(string) (string, error)) (map[string]string, error) {
		keys, values, err := reader.LoadRange(prefix, end, 0)
		if err != nil {
			return nil, err
		}
		actual := make(map[string]string, len(keys))
		for i, key := range keys {
			value, err := normalize(values[i])
			if err != nil {
				return nil, err
			}
			actual[key] = value
		}
		return actual, nil
	}
	err := m.runBalanceTxn(ctx, snapshot.conditions, func(txn kv.Txn) error {
		for _, key := range []string{
			endpoint.AffinityTableRevisionKey(snapshot.tableID),
			endpoint.AffinityTableMetadataRevisionKey(snapshot.tableID),
			endpoint.AffinityPlacementRevisionKey(),
		} {
			value, err := txn.Load(key)
			if err != nil {
				return err
			}
			dependencies[key] = value
		}
		version.token = dependencies[endpoint.AffinityPlacementRevisionKey()]
		prefix, end := balanceTableKeyRange(snapshot, keypath.AffinityGroupPath(snapshot.tableID))
		actual, err := loadMap(prefix, end, func(value string) (string, error) {
			var group Group
			if err := json.Unmarshal([]byte(value), &group); err != nil {
				return "", err
			}
			group.VoterStoreIDs = normalizeVoters(group.VoterStoreIDs)
			data, _ := json.Marshal(group)
			return canonicalJSON(string(data)), nil
		})
		if err != nil {
			return err
		}
		if !maps.Equal(expectedGroups, actual) {
			return balanceConflict()
		}
		prefix, end = balanceTableKeyRange(snapshot, keypath.RegionLabelKeyPath(GetLabelRuleID(snapshot.tableID)))
		actual, err = loadMap(prefix, end, func(value string) (string, error) {
			rule, err := labeler.NewLabelRuleFromJSON([]byte(value))
			if err != nil {
				return "", err
			}
			return canonicalLabelJSON(rule), nil
		})
		if err != nil {
			return err
		}
		if !maps.Equal(expectedLabels, actual) {
			return balanceConflict()
		}
		if verified != nil && *verified == version {
			return nil
		}
		expected := make(map[string]string)
		if metadata != nil {
			for key, value := range metadata() {
				expected[key] = canonicalJSON(value)
			}
		}
		m.regionLabeler.IterateLabelRules(func(rule *labeler.LabelRule) bool {
			if !strings.HasPrefix(rule.ID, LabelRuleIDPrefix) {
				data, _ := json.Marshal(rule)
				expected[keypath.RegionLabelKeyPath(rule.ID)] = canonicalJSON(string(data))
			}
			return true
		})
		actual = make(map[string]string)
		prefixes := []string{keypath.RegionLabelPathPrefix()}
		if metadata != nil {
			prefixes = append(prefixes, keypath.RulesPathPrefix(), keypath.RuleGroupPathPrefix())
		}
		for _, prefix := range prefixes {
			keys, values, err := reader.LoadRange(prefix, clientv3.GetPrefixRangeEnd(prefix), 0)
			if err != nil {
				return err
			}
			for i, key := range keys {
				if strings.HasPrefix(key, keypath.RegionLabelKeyPath(LabelRuleIDPrefix)) {
					continue
				}
				actual[key] = canonicalJSON(values[i])
			}
		}
		if !maps.Equal(expected, actual) {
			return balanceConflict()
		}
		return nil
	})
	if err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()
	state := m.balanceTables[snapshot.tableID]
	if state == nil || state.rangeVersion != snapshot.rangeVersion ||
		labels != m.regionLabeler.GetRevision() || m.balanceMetadataSource != version.source ||
		m.currentBalanceRuleRevisionLocked() != snapshot.ruleRevision {
		return balanceConflict()
	}
	m.balanceMetadataVerified = &version
	snapshot.dependencies = dependencies
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
	return table != nil && m.balanceFingerprintLocked(table) == snapshot.fingerprint && table.rangeVersion == snapshot.rangeVersion &&
		m.currentBalanceRuleRevisionLocked() == snapshot.ruleRevision
}

func checkBalanceDependencies(txn kv.Txn, snapshot *balanceSnapshot) error {
	if len(snapshot.dependencies) != 3 {
		return balanceConflict()
	}
	for key, expected := range snapshot.dependencies {
		value, err := txn.Load(key)
		if err != nil {
			return err
		}
		if value != expected {
			return balanceConflict()
		}
	}
	return nil
}

// commitBalancePlans shares the same bounded target transaction for automatic
// and manual callers. Durable generations fence all table targets, ranges,
// shared constraints and the original authority term, without holding Manager
// or RuleManager locks during network IO.
func (m *Manager) commitBalancePlans(ctx context.Context, snapshot *balanceSnapshot, plans []BalancePlan) (bool, error) {
	changed := slices.DeleteFunc(slices.Clone(plans), func(plan BalancePlan) bool { return !plan.Changed })
	if len(changed) == 0 || len(changed) > endpoint.MaxAffinityTxnBatchOps {
		return false, balanceConflict()
	}
	updates := make([]Group, 0, len(changed))
	for _, plan := range changed {
		index := slices.IndexFunc(snapshot.groups, func(group *GroupState) bool { return group.ID == plan.GroupID })
		if index < 0 || snapshot.groups[index].BalancePolicy == BalancePolicyFixed {
			return false, balanceConflict()
		}
		group := snapshot.groups[index].Group
		group.LeaderStoreID, group.VoterStoreIDs = plan.LeaderStoreID, normalizeVoters(plan.VoterStoreIDs)
		if err := m.AdjustGroup(&group); err != nil {
			return false, err
		}
		if err := m.checkBalanceTargetStores(group.LeaderStoreID, group.VoterStoreIDs); err != nil {
			return false, nil
		}
		updates = append(updates, group)
	}
	var targetToken string
	err := m.runBalanceTxn(ctx, snapshot.conditions, func(txn kv.Txn) error {
		if err := checkBalanceDependencies(txn, snapshot); err != nil {
			return err
		}
		m.RLock()
		current := m.currentBalanceRuleRevisionLocked() == snapshot.ruleRevision
		m.RUnlock()
		if !current || snapshot.admission != nil && !snapshot.admission() {
			return balanceConflict()
		}
		for i := range updates {
			if err := m.storage.SaveAffinityGroup(txn, updates[i].ID, &updates[i]); err != nil {
				return err
			}
		}
		var err error
		targetToken, err = endpoint.SaveAffinityTableRevision(txn, snapshot.tableID)
		return err
	})
	if errs.ErrEtcdTxnConflict.Equal(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Remember exactly our own token. A later read could silently adopt an
	// intervening management change and let a subsequent manual batch proceed.
	snapshot.dependencies[endpoint.AffinityTableRevisionKey(snapshot.tableID)] = targetToken
	for _, group := range updates {
		state, err := m.updateGroupTarget(group)
		if err != nil {
			return false, err
		}
		index := slices.IndexFunc(snapshot.groups, func(g *GroupState) bool { return g.ID == group.ID })
		snapshot.groups[index] = state
	}
	m.Lock()
	if table := m.balanceTables[snapshot.tableID]; table != nil {
		table.phase = balanceConverging
	}
	m.Unlock()
	snapshot.applied = true
	return true, nil
}
