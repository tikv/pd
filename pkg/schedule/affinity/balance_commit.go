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
// providers. A term is captured before the final validation and is not renewed
// by the target write.
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

func balanceConflict() error { return errs.ErrEtcdTxnConflict.FastGenByArgs() }

func canonicalJSON(value string) string {
	var decoded any
	if json.Unmarshal([]byte(value), &decoded) != nil {
		return value
	}
	data, _ := json.Marshal(decoded)
	return string(data)
}

func canonicalBalanceMetadata(key, value string) string {
	// RuleManager assigns these runtime fields after persisting the rule.
	// Compare its placement configuration, fenced by the durable token.
	if strings.HasPrefix(key, keypath.RulesPathPrefix()) {
		var rule map[string]any
		if json.Unmarshal([]byte(value), &rule) == nil {
			delete(rule, "version")
			delete(rule, "create_timestamp")
			data, _ := json.Marshal(rule)
			return string(data)
		}
	}
	return canonicalJSON(value)
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

func (m *Manager) balanceSnapshotLabels(snapshot *BalanceSnapshot) (map[string]string, bool) {
	expected := make(map[string]string, len(snapshot.Groups))
	m.regionLabeler.RLock()
	defer m.regionLabeler.RUnlock()
	for _, group := range snapshot.Groups {
		rule := m.regionLabeler.GetLabelRuleLocked(GetLabelRuleID(group.ID))
		actual, err := extractKeyRangesFromLabelRule(rule)
		ranges := cloneBalanceRanges(snapshot.Ranges[group.ID])
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

func balanceTableKeyRange(snapshot *BalanceSnapshot, prefix string) (start, end string) {
	for _, group := range snapshot.Groups {
		if group.ID != snapshot.TableID {
			prefix += "_p"
			return prefix, clientv3.GetPrefixRangeEnd(prefix)
		}
	}
	return prefix, prefix + "\x00"
}

// PrepareBalanceCommit reconciles local metadata with storage and captures
// the table, range, placement and authority conditions for a subsequent write.
func (m *Manager) PrepareBalanceCommit(ctx context.Context, snapshot *BalanceSnapshot) error {
	if !m.BalanceSnapshotCurrent(snapshot) {
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
	version := balanceMetadataVersion{rules: snapshot.Version.Rules, source: m.balanceMetadataSource}
	verified := m.balanceMetadataVerified
	rulesCurrent := m.currentBalanceRuleRevisionLocked() == snapshot.Version.Rules
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
	if term != nil && !snapshot.termCaptured {
		var ok bool
		snapshot.conditions, ok = term()
		if !ok {
			return balanceConflict()
		}
	}
	snapshot.termCaptured = true
	reader, ok := m.storage.(kv.BaseReadWrite)
	if !ok {
		return balanceConflict()
	}
	dependencies := make(map[string]string, 3)
	expectedGroups := make(map[string]string, len(snapshot.Groups))
	for _, group := range snapshot.Groups {
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
			endpoint.AffinityTableRevisionKey(snapshot.TableID),
			endpoint.AffinityTableMetadataRevisionKey(snapshot.TableID),
			endpoint.AffinityPlacementRevisionKey(),
		} {
			value, err := txn.Load(key)
			if err != nil {
				return err
			}
			dependencies[key] = value
		}
		version.token = dependencies[endpoint.AffinityPlacementRevisionKey()]
		prefix, end := balanceTableKeyRange(snapshot, keypath.AffinityGroupPath(snapshot.TableID))
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
		prefix, end = balanceTableKeyRange(snapshot, keypath.RegionLabelKeyPath(GetLabelRuleID(snapshot.TableID)))
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
				expected[key] = canonicalBalanceMetadata(key, value)
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
				actual[key] = canonicalBalanceMetadata(key, values[i])
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
	state := m.balanceTables[snapshot.TableID]
	if state == nil || state.rangeVersion != snapshot.Version.Ranges ||
		labels != m.regionLabeler.GetRevision() || m.balanceMetadataSource != version.source ||
		m.currentBalanceRuleRevisionLocked() != snapshot.Version.Rules {
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

// BalanceSnapshotCurrent reports whether local search inputs are unchanged.
func (m *Manager) BalanceSnapshotCurrent(snapshot *BalanceSnapshot) bool {
	version, exists := m.GetBalanceVersion(snapshot.TableID)
	return exists && version == snapshot.Version
}

func checkBalanceDependencies(txn kv.Txn, snapshot *BalanceSnapshot) error {
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

// CommitBalanceTarget updates one target using the captured durable versions
// and primary term. Network IO runs without Manager or RuleManager locks.
func (m *Manager) CommitBalanceTarget(ctx context.Context, snapshot *BalanceSnapshot, target *Group) (bool, error) {
	index := slices.IndexFunc(snapshot.Groups, func(group *GroupState) bool { return group.ID == target.ID })
	if index < 0 || snapshot.Groups[index].BalancePolicy == BalancePolicyFixed || !m.BalanceSnapshotCurrent(snapshot) ||
		m.IsManualBalanceRunning(snapshot.TableID) {
		return false, balanceConflict()
	}
	original := snapshot.Groups[index]
	group := original.Group
	group.LeaderStoreID, group.VoterStoreIDs = target.LeaderStoreID, normalizeVoters(target.VoterStoreIDs)
	if err := m.AdjustGroup(&group); err != nil {
		return false, err
	}
	if err := m.checkBalanceTargetStores(group.LeaderStoreID, group.VoterStoreIDs); err != nil {
		return false, nil
	}
	var targetToken string
	err := m.runBalanceTxn(ctx, snapshot.conditions, func(txn kv.Txn) error {
		if err := checkBalanceDependencies(txn, snapshot); err != nil {
			return err
		}
		if !m.BalanceSnapshotCurrent(snapshot) {
			return balanceConflict()
		}
		if m.IsManualBalanceRunning(snapshot.TableID) {
			return balanceConflict()
		}
		if err := m.storage.SaveAffinityGroup(txn, group.ID, &group); err != nil {
			return err
		}
		var err error
		targetToken, err = endpoint.SaveAffinityTableRevision(txn, snapshot.TableID)
		return err
	})
	if errs.ErrEtcdTxnConflict.Equal(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	snapshot.dependencies[endpoint.AffinityTableRevisionKey(snapshot.TableID)] = targetToken
	// Publish only if a watcher has not already observed a later management
	// update. The network transaction never holds this metadata mutex.
	m.metaMutex.Lock()
	defer m.metaMutex.Unlock()
	current := m.GetAffinityGroupState(group.ID)
	if current != nil && current.CreateTimestamp == original.CreateTimestamp && current.BalancePolicy == original.BalancePolicy &&
		current.LeaderStoreID == original.LeaderStoreID && slices.Equal(current.VoterStoreIDs, original.VoterStoreIDs) {
		if _, err := m.updateGroupTarget(group); err != nil {
			return true, err
		}
	}
	return true, nil
}
