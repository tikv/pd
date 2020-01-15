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
	"sort"
	"sync"

	"github.com/pingcap/pd/server/core"
	"github.com/pkg/errors"
)

// RuleManager is responsible for the lifecycle of all placement Rules.
// It is threadsafe.
type RuleManager struct {
	store *core.Storage
	sync.RWMutex
	initialized bool
	rules       map[[2]string]*Rule
	splitKeys   [][]byte
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

	var rules []*Rule
	ok, err := m.store.LoadRules(func(v string) error {
		var r Rule
		if err := json.Unmarshal([]byte(v), &r); err != nil {
			return err
		}
		rules = append(rules, &r)
		return nil
	})
	if err != nil {
		return err
	}
	if !ok {
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
		rules = append(rules, defaultRule)
	}
	for _, r := range rules {
		if err = m.adjustRule(r); err != nil {
			return err
		}
		m.rules[r.Key()] = r
	}
	m.updateSplitKeys()
	m.initialized = true
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
	m.updateSplitKeys()
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
	m.updateSplitKeys()
	return nil
}

func (m *RuleManager) updateSplitKeys() {
	keys := m.splitKeys[:0]
	m.splitKeys = m.splitKeys[:0]
	for _, r := range m.rules {
		if len(r.StartKey) > 0 {
			keys = append(keys, r.StartKey)
		}
		if len(r.EndKey) > 0 {
			keys = append(keys, r.EndKey)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	for _, k := range keys {
		if len(m.splitKeys) == 0 || !bytes.Equal(m.splitKeys[len(m.splitKeys)-1], k) {
			m.splitKeys = append(m.splitKeys, k)
		}
	}
}

// GetSplitKeys returns all split keys in the range (start, end).
func (m *RuleManager) GetSplitKeys(start, end []byte) [][]byte {
	m.RLock()
	defer m.RUnlock()
	var keys [][]byte
	i := sort.Search(len(m.splitKeys), func(i int) bool {
		return bytes.Compare(m.splitKeys[i], start) > 0
	})
	for ; i < len(m.splitKeys) && (len(end) == 0 || bytes.Compare(m.splitKeys[i], end) < 0); i++ {
		keys = append(keys, m.splitKeys[i])
	}
	return keys
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
	var rules []*Rule
	for _, r := range m.rules {
		// rule.start <= key < rule.end
		if bytes.Compare(r.StartKey, key) <= 0 &&
			(len(r.EndKey) == 0 || bytes.Compare(key, r.EndKey) < 0) {
			rules = append(rules, r)
		}
	}
	sortRules(rules)
	return rules
}

// GetRulesForApplyRegion returns the rules list that should be applied to a region.
func (m *RuleManager) GetRulesForApplyRegion(region *core.RegionInfo) []*Rule {
	m.RLock()
	defer m.RUnlock()
	var rules []*Rule
	for _, r := range m.rules {
		// no intersection
		//                  |<-- rule -->|
		// |<-- region -->|
		// or
		// |<-- rule -->|
		//                 |<-- region -->|
		if (len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), r.StartKey) <= 0) ||
			len(r.EndKey) > 0 && bytes.Compare(region.GetStartKey(), r.EndKey) >= 0 {
			continue
		}
		// in range
		//   |<----- rule ----->|
		//     |<-- region -->|
		if bytes.Compare(region.GetStartKey(), r.StartKey) >= 0 && (len(r.EndKey) == 0 || (len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), r.EndKey) <= 0)) {
			rules = append(rules, r)
			continue
		}
		// region spans multiple rule ranges.
		// |<-- rule -->|
		//       |<-- region -->|
		// or
		//         |<-- rule -->|
		// |<-- region -->|
		// or
		//   |<-- rule -->|
		// |<--- region --->|
		return nil // It will considered abnormal when a region is not covered by any rule. Then Rule checker will try to split the region.
	}
	return prepareRulesForApply(rules)
}

// FitRegion fits a region to the rules it matches.
func (m *RuleManager) FitRegion(stores core.StoreSetInformer, region *core.RegionInfo) *RegionFit {
	rules := m.GetRulesForApplyRegion(region)
	return FitRegion(stores, region, rules)
}
