// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// RuleManager is responsible for the lifecycle of all placement Rules.
// It is threadsafe.
type RuleManager struct {
	store *core.Storage
	sync.RWMutex
	initialized bool
	rules       map[[2]string]*Rule
	ruleList    ruleList
}

// NewRuleManager creates a RuleManager instance.
func NewRuleManager(store *core.Storage) *RuleManager {
	return &RuleManager{
		store: store,
		rules: make(map[[2]string]*Rule),
	}
}

// Initialize loads rules from storage. If Placement Rules feature is never enabled, it creates default rule that is
// compatible with previous configuration.
func (m *RuleManager) Initialize(maxReplica int, locationLabels []string) error {
	m.Lock()
	defer m.Unlock()
	if m.initialized {
		return nil
	}

	if err := m.loadRules(); err != nil {
		return err
	}
	if len(m.rules) == 0 {
		// migrate from old config.
		defaultRule := &Rule{
			GroupID:        "pd",
			ID:             "default",
			Role:           Voter,
			Count:          maxReplica,
			LocationLabels: locationLabels,
		}
		if err := m.store.SaveRule(defaultRule.StoreKey(), defaultRule); err != nil {
			return err
		}
		m.rules[defaultRule.Key()] = defaultRule
	}
	m.ruleList = buildRuleList(m.rules)
	m.initialized = true
	return nil
}

func (m *RuleManager) loadRules() error {
	var toSave []*Rule
	var toDelete []string
	_, err := m.store.LoadRules(func(k, v string) {
		var r Rule
		if err := json.Unmarshal([]byte(v), &r); err != nil {
			log.Error("failed to unmarshal rule value", zap.String("rule-key", k), zap.String("rule-value", v))
			toDelete = append(toDelete, k)
			return
		}
		if err := m.adjustRule(&r); err != nil {
			log.Error("rule is in bad format", zap.Error(err), zap.String("rule-key", k), zap.String("rule-value", v))
			toDelete = append(toDelete, k)
			return
		}
		if _, ok := m.rules[r.Key()]; ok {
			log.Error("duplicated rule key", zap.String("rule-key", k), zap.String("rule-value", v))
			toDelete = append(toDelete, k)
			return
		}
		if k != r.StoreKey() {
			log.Error("mismatch data key, need to restore", zap.String("rule-key", k), zap.String("rule-value", v))
			toDelete = append(toDelete, k)
			toSave = append(toSave, &r)
		}
		m.rules[r.Key()] = &r
	})
	if err != nil {
		return err
	}
	for _, s := range toSave {
		if err = m.store.SaveRule(s.StoreKey(), s); err != nil {
			return err
		}
	}
	for _, d := range toDelete {
		if err = m.store.DeleteRule(d); err != nil {
			return err
		}
	}
	return nil
}

// check and adjust rule from client or storage.
func (m *RuleManager) adjustRule(r *Rule) error {
	var err error
	r.StartKey, err = hex.DecodeString(r.StartKeyHex)
	if err != nil {
		return errors.Wrap(err, "start key is not hex format")
	}
	r.EndKey, err = hex.DecodeString(r.EndKeyHex)
	if err != nil {
		return errors.Wrap(err, "end key is not hex format")
	}
	if len(r.EndKey) > 0 && bytes.Compare(r.EndKey, r.StartKey) <= 0 {
		return errors.New("endKey should be greater than startKey")
	}
	if r.GroupID == "" {
		return errors.New("group ID should not be empty")
	}
	if r.ID == "" {
		return errors.New("ID should not be empty")
	}
	if !validateRole(r.Role) {
		return errors.Errorf("invalid role %s", r.Role)
	}
	if r.Count <= 0 {
		return errors.Errorf("invalid count %v", r.Count)
	}
	for _, c := range r.LabelConstraints {
		if !validateOp(c.Op) {
			return errors.Errorf("invalid op %s", c.Op)
		}
	}
	return nil
}

// GetRule returns the Rule with the same (group, id).
func (m *RuleManager) GetRule(group, id string) *Rule {
	m.RLock()
	defer m.RUnlock()
	return m.rules[[2]string{group, id}]
}

// SetRule inserts or updates a Rule.
func (m *RuleManager) SetRule(rule *Rule) error {
	err := m.adjustRule(rule)
	if err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()
	old := m.rules[rule.Key()]
	m.rules[rule.Key()] = rule

	if err = m.store.SaveRule(rule.StoreKey(), rule); err != nil {
		// restore
		if old == nil {
			delete(m.rules, rule.Key())
		} else {
			m.rules[old.Key()] = old
		}
		return err
	}

	log.Info("placement rule updated", zap.Stringer("rule", rule))
	m.ruleList = buildRuleList(m.rules)
	return nil
}

// DeleteRule removes a Rule.
func (m *RuleManager) DeleteRule(group, id string) error {
	m.Lock()
	defer m.Unlock()
	key := [2]string{group, id}
	old, ok := m.rules[key]
	if !ok {
		return nil
	}
	delete(m.rules, [2]string{group, id})
	if err := m.store.DeleteRule(old.StoreKey()); err != nil {
		// restore
		m.rules[key] = old
		return err
	}
	log.Info("placement rule removed", zap.Stringer("rule", old))
	m.ruleList = buildRuleList(m.rules)
	return nil
}

// GetSplitKeys returns all split keys in the range (start, end).
func (m *RuleManager) GetSplitKeys(start, end []byte) [][]byte {
	m.RLock()
	defer m.RUnlock()
	return m.ruleList.getSplitKeys(start, end)
}

// GetAllRules returns sorted all rules.
func (m *RuleManager) GetAllRules() []*Rule {
	m.RLock()
	defer m.RUnlock()
	rules := make([]*Rule, 0, len(m.rules))
	for _, r := range m.rules {
		rules = append(rules, r)
	}
	sortRules(rules)
	return rules
}

// GetRulesByGroup returns sorted rules of a group.
func (m *RuleManager) GetRulesByGroup(group string) []*Rule {
	m.RLock()
	defer m.RUnlock()
	var rules []*Rule
	for _, r := range m.rules {
		if r.GroupID == group {
			rules = append(rules, r)
		}
	}
	sortRules(rules)
	return rules
}

// GetRulesByKey returns sorted rules that affects a key.
func (m *RuleManager) GetRulesByKey(key []byte) []*Rule {
	m.RLock()
	defer m.RUnlock()
	return m.ruleList.getRulesByKey(key)
}

// GetRulesForApplyRegion returns the rules list that should be applied to a region.
func (m *RuleManager) GetRulesForApplyRegion(region *core.RegionInfo) []*Rule {
	m.RLock()
	defer m.RUnlock()
	return m.ruleList.getRulesForApplyRegion(region.GetStartKey(), region.GetEndKey())
}

// FitRegion fits a region to the rules it matches.
func (m *RuleManager) FitRegion(stores core.StoreSetInformer, region *core.RegionInfo) *RegionFit {
	rules := m.GetRulesForApplyRegion(region)
	return FitRegion(stores, region, rules)
}
