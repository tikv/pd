// Copyright 2019 TiKV Project Authors.
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
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

// RuleManager is responsible for the lifecycle of all placement Rules.
// It is threadsafe.
type RuleManager struct {
	store *core.Storage
	sync.RWMutex
	initialized bool
	// Key(RuleGroupID,RuleID) => Rule
	rules map[[2]string]*Rule
	// GroupID => RuleGroup
	groups map[string]*RuleGroup
	// constructed by rebuildRuleList in runtime, store groups with default configuration
	defGroups map[string]struct{}
	ruleList  ruleList
}

// NewRuleManager creates a RuleManager instance.
func NewRuleManager(store *core.Storage) *RuleManager {
	return &RuleManager{
		store:  store,
		rules:  make(map[[2]string]*Rule),
		groups: make(map[string]*RuleGroup),
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
	if err := m.loadGroups(); err != nil {
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
	ruleList, err := m.rebuildRuleList()
	if err != nil {
		return err
	}
	m.ruleList = ruleList
	m.initialized = true
	return nil
}

func (m *RuleManager) loadRules() error {
	var toSave []*Rule
	var toDelete []string
	err := m.store.LoadRules(func(k, v string) {
		var r Rule
		if err := json.Unmarshal([]byte(v), &r); err != nil {
			log.Error("failed to unmarshal rule value", zap.String("rule-key", k), zap.String("rule-value", v), zap.Error(errs.ErrLoadRule.FastGenByArgs()))
			toDelete = append(toDelete, k)
			return
		}
		if err := m.adjustRule(&r); err != nil {
			log.Error("rule is in bad format", zap.String("rule-key", k), zap.String("rule-value", v), zap.Error(errs.ErrLoadRule.FastGenByArgs()), zap.NamedError("cause", err))
			toDelete = append(toDelete, k)
			return
		}
		if _, ok := m.rules[r.Key()]; ok {
			log.Error("duplicated rule key", zap.String("rule-key", k), zap.String("rule-value", v), zap.Error(errs.ErrLoadRule.FastGenByArgs()))
			toDelete = append(toDelete, k)
			return
		}
		if k != r.StoreKey() {
			log.Error("mismatch data key, need to restore", zap.String("rule-key", k), zap.String("rule-value", v), zap.Error(errs.ErrLoadRule.FastGenByArgs()))
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

func (m *RuleManager) loadGroups() error {
	return m.store.LoadRuleGroups(func(k, v string) {
		var g RuleGroup
		if err := json.Unmarshal([]byte(v), &g); err != nil {
			log.Error("failed to unmarshal rule group", zap.String("group-id", k), zap.Error(errs.ErrLoadRuleGroup.FastGenByArgs()), zap.NamedError("cause", err))
			return
		}
		m.groups[g.ID] = &g
	})
}

// check and adjust rule from client or storage.
func (m *RuleManager) adjustRule(r *Rule) error {
	var err error
	r.StartKey, err = hex.DecodeString(r.StartKeyHex)
	if err != nil {
		return errs.ErrRuleContent.FastGenByArgs("start key is not hex format")
	}
	r.EndKey, err = hex.DecodeString(r.EndKeyHex)
	if err != nil {
		return errs.ErrRuleContent.FastGenByArgs("end key is not hex format")
	}
	if len(r.EndKey) > 0 && bytes.Compare(r.EndKey, r.StartKey) <= 0 {
		return errs.ErrRuleContent.FastGenByArgs("endKey should be greater than startKey")
	}
	if r.GroupID == "" {
		return errs.ErrRuleContent.FastGenByArgs("group ID should not be empty")
	}
	if r.ID == "" {
		return errs.ErrRuleContent.FastGenByArgs("ID should not be empty")
	}
	if !validateRole(r.Role) {
		return errs.ErrRuleContent.FastGenByArgs(fmt.Sprintf("invalid role %s", r.Role))
	}
	if r.Count <= 0 {
		return errs.ErrRuleContent.FastGenByArgs(fmt.Sprintf("invalid count %d", r.Count))
	}
	for _, c := range r.LabelConstraints {
		if !validateOp(c.Op) {
			return errs.ErrRuleContent.FastGenByArgs(fmt.Sprintf("invalid op %s", c.Op))
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
	return m.Batch([]RuleOp{{
		Rule:   rule,
		Action: RuleOpAdd,
	}})
}

// DeleteRule removes a Rule.
func (m *RuleManager) DeleteRule(group, id string) error {
	return m.Batch([]RuleOp{{
		Rule:   &Rule{GroupID: group, ID: id},
		Action: RuleOpDel,
	}})
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
func (m *RuleManager) FitRegion(stores StoreSet, region *core.RegionInfo) *RegionFit {
	rules := m.GetRulesForApplyRegion(region)
	return FitRegion(stores, region, rules)
}

func (m *RuleManager) tryBuildSave(oldRules map[[2]string]*Rule) error {
	ruleList, err := m.rebuildRuleList()
	if err == nil {
		for key := range oldRules {
			rule := m.rules[key]
			if rule != nil {
				err = m.store.SaveRule(rule.StoreKey(), rule)
			} else {
				r := Rule{
					GroupID: key[0],
					ID:      key[1],
				}
				err = m.store.DeleteRule(r.StoreKey())
			}
			if err != nil {
				// TODO: it is not completely safe
				// 1. in case that half of rules applied, error.. we have to cancel persisted rules
				// but that may fail too, causing memory/disk inconsistency
				// either rely a transaction API, or clients to request again until success
				// 2. in case that PD is suddenly down in the loop, inconsistency again
				// now we can only rely clients to request again
				break
			}
		}
	}

	if err != nil {
		for key, rule := range oldRules {
			if rule == nil {
				delete(m.rules, key)
			} else {
				m.rules[key] = rule
			}
		}
		return err
	}

	m.ruleList = ruleList
	return nil
}

func (m *RuleManager) addRule(rule *Rule, oldRules map[[2]string]*Rule) {
	old := m.rules[rule.Key()]
	m.rules[rule.Key()] = rule
	oldRules[rule.Key()] = old
}

// SetRules inserts or updates lots of Rules at once.
func (m *RuleManager) SetRules(rules []*Rule) error {
	ruleOps := make([]RuleOp, len(rules))
	for i, rule := range rules {
		err := m.adjustRule(rule)
		if err != nil {
			return err
		}
		ruleOps[i] = RuleOp{Rule: rule, Action: RuleOpAdd}
	}
	return m.Batch(ruleOps)
}

func (m *RuleManager) delRuleByID(group, id string, oldRules map[[2]string]*Rule) {
	key := [2]string{group, id}
	old, ok := m.rules[key]
	if ok {
		delete(m.rules, key)
	}
	oldRules[key] = old
}

func (m *RuleManager) delRule(t *RuleOp, oldRules map[[2]string]*Rule) {
	if !t.DeleteByIDPrefix {
		m.delRuleByID(t.GroupID, t.ID, oldRules)
	} else {
		for key := range m.rules {
			if key[0] == t.GroupID && strings.HasPrefix(key[1], t.ID) {
				m.delRuleByID(key[0], key[1], oldRules)
			}
		}
	}
}

// RuleOpType indicates the operation type
type RuleOpType string

const (
	// RuleOpAdd a placement rule, only need to specify the field *Rule
	RuleOpAdd RuleOpType = "add"
	// RuleOpDel a placement rule, only need to specify the field `GroupID`, `ID`, `MatchID`
	RuleOpDel RuleOpType = "del"
)

// RuleOp is for batching placement rule actions. The action type is
// distinguished by the field `Action`.
type RuleOp struct {
	*Rule                       // information of the placement rule to add/delete
	Action           RuleOpType `json:"action"`              // the operation type
	DeleteByIDPrefix bool       `json:"delete_by_id_prefix"` // if action == delete, delete by the prefix of id
}

func (r RuleOp) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// Batch executes a series of actions at once.
func (m *RuleManager) Batch(todo []RuleOp) error {
	for _, t := range todo {
		switch t.Action {
		case RuleOpAdd:
			err := m.adjustRule(t.Rule)
			if err != nil {
				return err
			}
		}
	}

	m.Lock()
	defer m.Unlock()

	oldRules := make(map[[2]string]*Rule)

	for _, t := range todo {
		switch t.Action {
		case RuleOpAdd:
			m.addRule(t.Rule, oldRules)
		case RuleOpDel:
			m.delRule(&t, oldRules)
		}
	}

	if err := m.tryBuildSave(oldRules); err != nil {
		return err
	}

	log.Info("placement rules updated", zap.String("batch", fmt.Sprint(todo)))
	return nil
}

func (m *RuleManager) rebuildRuleList() (ruleList, error) {
	m.defGroups = make(map[string]struct{})
	for _, r := range m.rules {
		r.group = m.groups[r.GroupID]
		if r.group == nil {
			m.defGroups[r.GroupID] = struct{}{}
		}
	}
	rl, err := buildRuleList(m.rules)
	if err != nil {
		return ruleList{}, err
	}
	return rl, nil
}

// GetRuleGroup returns a RuleGroup configuration.
func (m *RuleManager) GetRuleGroup(id string) *RuleGroup {
	m.RLock()
	defer m.RUnlock()
	if _, ok := m.defGroups[id]; ok {
		return &RuleGroup{ID: id}
	}
	return m.groups[id]
}

// GetRuleGroups returns all RuleGroup configuration.
func (m *RuleManager) GetRuleGroups() []*RuleGroup {
	m.RLock()
	defer m.RUnlock()
	var groups []*RuleGroup
	for _, g := range m.groups {
		groups = append(groups, g)
	}
	for id := range m.defGroups {
		groups = append(groups, &RuleGroup{ID: id})
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Index < groups[j].Index ||
			(groups[i].Index == groups[j].Index && groups[i].ID < groups[j].ID)
	})
	return groups
}

// SetRuleGroup updates a RuleGroup.
func (m *RuleManager) SetRuleGroup(group *RuleGroup) error {
	m.Lock()
	defer m.Unlock()
	return m.tryUpdateGroup(group.ID, group)
}

// DeleteRuleGroup removes a RuleGroup.
func (m *RuleManager) DeleteRuleGroup(id string) error {
	m.Lock()
	defer m.Unlock()
	return m.tryUpdateGroup(id, nil)
}

func (m *RuleManager) tryUpdateGroup(id string, newGroup *RuleGroup) error {
	oldGroup := m.groups[id]

	if newGroup != nil {
		m.groups[id] = newGroup
	} else {
		delete(m.groups, id)
	}

	rl, err := m.rebuildRuleList()
	if err == nil {
		if newGroup != nil {
			err = m.store.SaveRuleGroup(id, newGroup)
		} else {
			err = m.store.DeleteRuleGroup(id)
		}
	}
	if err != nil {
		// recover old:
		if oldGroup != nil {
			m.groups[id] = oldGroup
		} else {
			delete(m.groups, id)
		}
		return err
	}
	m.ruleList = rl
	return nil
}
