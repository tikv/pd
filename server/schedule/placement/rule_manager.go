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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

// RuleManager is responsible for the lifecycle of all placement Rules.
// It is thread safe.
type RuleManager struct {
	storage *core.Storage
	sync.RWMutex
	initialized bool
	ruleConfig  *ruleConfig
	ruleList    ruleList

	// used for rule validation
	keyType          string
	storeSetInformer core.StoreSetInformer
	cache            *RegionRuleFitCacheManager
	opt              *config.PersistOptions
}

// NewRuleManager creates a RuleManager instance.
func NewRuleManager(storage *core.Storage, storeSetInformer core.StoreSetInformer, opt *config.PersistOptions) *RuleManager {
	return &RuleManager{
		storage:          storage,
		storeSetInformer: storeSetInformer,
		opt:              opt,
		ruleConfig:       newRuleConfig(),
		cache:            NewRegionRuleFitCacheManager(),
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
	if len(m.ruleConfig.rules) == 0 {
		// migrate from old config.
		defaultRule := &Rule{
			GroupID:        "pd",
			ID:             "default",
			Role:           Voter,
			Count:          maxReplica,
			LocationLabels: locationLabels,
		}
		if err := m.storage.SaveRule(defaultRule.StoreKey(), defaultRule); err != nil {
			return err
		}
		m.ruleConfig.setRule(defaultRule)
	}
	m.ruleConfig.adjust()
	ruleList, err := buildRuleList(m.ruleConfig)
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
	err := m.storage.LoadRules(func(k, v string) {
		var r Rule
		if err := json.Unmarshal([]byte(v), &r); err != nil {
			log.Error("failed to unmarshal rule value", zap.String("rule-key", k), zap.String("rule-value", v), errs.ZapError(errs.ErrLoadRule))
			toDelete = append(toDelete, k)
			return
		}
		if err := m.adjustRule(&r, ""); err != nil {
			log.Error("rule is in bad format", zap.String("rule-key", k), zap.String("rule-value", v), errs.ZapError(errs.ErrLoadRule, err))
			toDelete = append(toDelete, k)
			return
		}
		if _, ok := m.ruleConfig.rules[r.Key()]; ok {
			log.Error("duplicated rule key", zap.String("rule-key", k), zap.String("rule-value", v), errs.ZapError(errs.ErrLoadRule))
			toDelete = append(toDelete, k)
			return
		}
		if k != r.StoreKey() {
			log.Error("mismatch data key, need to restore", zap.String("rule-key", k), zap.String("rule-value", v), errs.ZapError(errs.ErrLoadRule))
			toDelete = append(toDelete, k)
			toSave = append(toSave, &r)
		}
		m.ruleConfig.rules[r.Key()] = &r
	})
	if err != nil {
		return err
	}
	for _, s := range toSave {
		if err = m.storage.SaveRule(s.StoreKey(), s); err != nil {
			return err
		}
	}
	for _, d := range toDelete {
		if err = m.storage.DeleteRule(d); err != nil {
			return err
		}
	}
	return nil
}

func (m *RuleManager) loadGroups() error {
	return m.storage.LoadRuleGroups(func(k, v string) {
		var g RuleGroup
		if err := json.Unmarshal([]byte(v), &g); err != nil {
			log.Error("failed to unmarshal rule group", zap.String("group-id", k), errs.ZapError(errs.ErrLoadRuleGroup, err))
			return
		}
		m.ruleConfig.groups[g.ID] = &g
	})
}

// check and adjust rule from client or storage.
func (m *RuleManager) adjustRule(r *Rule, groupID string) (err error) {
	r.StartKey, err = hex.DecodeString(r.StartKeyHex)
	if err != nil {
		return errs.ErrHexDecodingString.FastGenByArgs(r.StartKeyHex)
	}
	r.EndKey, err = hex.DecodeString(r.EndKeyHex)
	if err != nil {
		return errs.ErrHexDecodingString.FastGenByArgs(r.EndKeyHex)
	}
	if len(r.EndKey) > 0 && bytes.Compare(r.EndKey, r.StartKey) <= 0 {
		return errs.ErrRuleContent.FastGenByArgs("endKey should be greater than startKey")
	}

	if m.keyType == core.Table.String() || m.keyType == core.Txn.String() {
		if len(r.StartKey) > 0 {
			if _, _, err = codec.DecodeBytes(r.StartKey); err != nil {
				return errs.ErrRuleContent.FastGenByArgs(errors.Wrapf(err, "start key should be encoded in %s mode", m.keyType).Error())
			}
		}
		if len(r.EndKey) > 0 {
			if _, _, err = codec.DecodeBytes(r.EndKey); err != nil {
				return errs.ErrRuleContent.FastGenByArgs(errors.Wrapf(err, "end key should be encoded in %s mode", m.keyType).Error())
			}
		}
	}

	if groupID != "" {
		if r.GroupID == "" {
			r.GroupID = groupID
		} else if groupID != r.GroupID {
			return errs.ErrRuleContent.FastGenByArgs(fmt.Sprintf("rule group %s does not match group ID %s", r.GroupID, groupID))
		}
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
	if r.Role == Leader && r.Count > 1 {
		return errs.ErrRuleContent.FastGenByArgs(fmt.Sprintf("define multiple leaders by count %d", r.Count))
	}
	for _, c := range r.LabelConstraints {
		if !validateOp(c.Op) {
			return errs.ErrRuleContent.FastGenByArgs(fmt.Sprintf("invalid op %s", c.Op))
		}
	}

	if m.storeSetInformer != nil {
		stores := m.storeSetInformer.GetStores()
		if len(stores) > 0 && !checkRule(r, stores) {
			return errs.ErrRuleContent.FastGenByArgs(fmt.Sprintf("rule '%s' from rule group '%s' can not match any store", r.ID, r.GroupID))
		}
	}

	return nil
}

// GetRule returns the Rule with the same (group, id).
func (m *RuleManager) GetRule(group, id string) *Rule {
	m.RLock()
	defer m.RUnlock()
	if r := m.ruleConfig.getRule([2]string{group, id}); r != nil {
		return r.Clone()
	}
	return nil
}

// SetRule inserts or updates a Rule.
func (m *RuleManager) SetRule(rule *Rule) error {
	if err := m.adjustRule(rule, ""); err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	p.setRule(rule)
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("placement rule updated", zap.String("rule", fmt.Sprint(rule)))
	return nil
}

// DeleteRule removes a Rule.
func (m *RuleManager) DeleteRule(group, id string) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	p.deleteRule(group, id)
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("placement rule is removed", zap.String("group", group), zap.String("id", id))
	return nil
}

// GetSplitKeys returns all split keys in the range (start, end).
func (m *RuleManager) GetSplitKeys(start, end []byte) [][]byte {
	m.RLock()
	defer m.RUnlock()
	return m.ruleList.rangeList.GetSplitKeys(start, end)
}

// GetAllRules returns sorted all rules.
func (m *RuleManager) GetAllRules() []*Rule {
	m.RLock()
	defer m.RUnlock()
	rules := make([]*Rule, 0, len(m.ruleConfig.rules))
	for _, r := range m.ruleConfig.rules {
		rules = append(rules, r.Clone())
	}
	sortRules(rules)
	return rules
}

// GetRulesByGroup returns sorted rules of a group.
func (m *RuleManager) GetRulesByGroup(group string) []*Rule {
	m.RLock()
	defer m.RUnlock()
	var rules []*Rule
	for _, r := range m.ruleConfig.rules {
		if r.GroupID == group {
			rules = append(rules, r.Clone())
		}
	}
	sortRules(rules)
	return rules
}

// GetRulesByKey returns sorted rules that affects a key.
func (m *RuleManager) GetRulesByKey(key []byte) []*Rule {
	m.RLock()
	defer m.RUnlock()
	rules := m.ruleList.getRulesByKey(key)
	ret := make([]*Rule, 0, len(rules))
	for _, r := range rules {
		ret = append(ret, r.Clone())
	}
	return ret
}

// GetRulesForApplyRegion returns the rules list that should be applied to a region.
func (m *RuleManager) GetRulesForApplyRegion(region *core.RegionInfo) []*Rule {
	m.RLock()
	defer m.RUnlock()
	return m.ruleList.getRulesForApplyRegion(region.GetStartKey(), region.GetEndKey())
}

// FitRegion fits a region to the rules it matches.
func (m *RuleManager) FitRegion(storeSet StoreSet, region *core.RegionInfo) *RegionFit {
	regionStores := getStoresByRegion(storeSet, region)
	rules := m.GetRulesForApplyRegion(region)
	if m.opt.IsPlacementRulesCacheEnabled() {
		if ok, fit := m.cache.CheckAndGetCache(region, rules, regionStores); fit != nil && ok {
			return fit
		}
	}
	fit := fitRegion(regionStores, region, rules)
	fit.regionStores = regionStores
	fit.rules = rules
	return fit
}

// SetRegionFitCache sets RegionFitCache
func (m *RuleManager) SetRegionFitCache(region *core.RegionInfo, fit *RegionFit) {
	m.cache.SetCache(region, fit)
}

// InvalidCache invalids the cache.
func (m *RuleManager) InvalidCache(regionID uint64) {
	m.cache.Invalid(regionID)
}

func (m *RuleManager) beginPatch() *ruleConfigPatch {
	return m.ruleConfig.beginPatch()
}

func (m *RuleManager) tryCommitPatch(patch *ruleConfigPatch) error {
	patch.adjust()

	ruleList, err := buildRuleList(patch)
	if err != nil {
		return err
	}

	patch.trim()

	// save updates
	err = m.savePatch(patch.mut)
	if err != nil {
		return err
	}

	// update in-memory state
	patch.commit()
	m.ruleList = ruleList
	return nil
}

func (m *RuleManager) savePatch(p *ruleConfig) error {
	// TODO: it is not completely safe
	// 1. in case that half of rules applied, error.. we have to cancel persisted rules
	// but that may fail too, causing memory/disk inconsistency
	// either rely a transaction API, or clients to request again until success
	// 2. in case that PD is suddenly down in the loop, inconsistency again
	// now we can only rely clients to request again
	var err error
	for key, r := range p.rules {
		if r == nil {
			r = &Rule{GroupID: key[0], ID: key[1]}
			err = m.storage.DeleteRule(r.StoreKey())
		} else {
			err = m.storage.SaveRule(r.StoreKey(), r)
		}
		if err != nil {
			return err
		}
	}
	for id, g := range p.groups {
		if g.isDefault() {
			err = m.storage.DeleteRuleGroup(id)
		} else {
			err = m.storage.SaveRuleGroup(id, g)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// SetRules inserts or updates lots of Rules at once.
func (m *RuleManager) SetRules(rules []*Rule) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	for _, r := range rules {
		if err := m.adjustRule(r, ""); err != nil {
			return err
		}
		p.setRule(r)
	}
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}

	log.Info("placement rules updated", zap.String("rules", fmt.Sprint(rules)))
	return nil
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
	*Rule                       // information of the placement rule to add/delete the operation type
	Action           RuleOpType `json:"action"`
	DeleteByIDPrefix bool       `json:"delete_by_id_prefix"` // if action == delete, delete by the prefix of id
}

func (r RuleOp) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// Batch executes a series of actions at once.
func (m *RuleManager) Batch(todo []RuleOp) error {
	for _, t := range todo {
		if t.Action == RuleOpAdd {
			err := m.adjustRule(t.Rule, "")
			if err != nil {
				return err
			}
		}
	}

	m.Lock()
	defer m.Unlock()

	patch := m.beginPatch()
	for _, t := range todo {
		switch t.Action {
		case RuleOpAdd:
			patch.setRule(t.Rule)
		case RuleOpDel:
			if !t.DeleteByIDPrefix {
				patch.deleteRule(t.GroupID, t.ID)
			} else {
				m.ruleConfig.iterateRules(func(r *Rule) {
					if r.GroupID == t.GroupID && strings.HasPrefix(r.ID, t.ID) {
						patch.deleteRule(r.GroupID, r.ID)
					}
				})
			}
		}
	}

	if err := m.tryCommitPatch(patch); err != nil {
		return err
	}

	log.Info("placement rules updated", zap.String("batch", fmt.Sprint(todo)))
	return nil
}

// GetRuleGroup returns a RuleGroup configuration.
func (m *RuleManager) GetRuleGroup(id string) *RuleGroup {
	m.RLock()
	defer m.RUnlock()
	return m.ruleConfig.groups[id]
}

// GetRuleGroups returns all RuleGroup configuration.
func (m *RuleManager) GetRuleGroups() []*RuleGroup {
	m.RLock()
	defer m.RUnlock()
	groups := make([]*RuleGroup, 0, len(m.ruleConfig.groups))
	for _, g := range m.ruleConfig.groups {
		groups = append(groups, g)
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
	p := m.beginPatch()
	p.setGroup(group)
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("group config updated", zap.String("group", fmt.Sprint(group)))
	return nil
}

// DeleteRuleGroup removes a RuleGroup.
func (m *RuleManager) DeleteRuleGroup(id string) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	p.deleteGroup(id)
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("group config reset", zap.String("group", id))
	return nil
}

// GetAllGroupBundles returns all rules and groups configuration. Rules are
// grouped by groups.
func (m *RuleManager) GetAllGroupBundles() []GroupBundle {
	m.RLock()
	defer m.RUnlock()
	bundles := make([]GroupBundle, 0, len(m.ruleConfig.groups))
	for _, g := range m.ruleConfig.groups {
		bundles = append(bundles, GroupBundle{
			ID:       g.ID,
			Index:    g.Index,
			Override: g.Override,
		})
	}
	for _, r := range m.ruleConfig.rules {
		for i := range bundles {
			if bundles[i].ID == r.GroupID {
				bundles[i].Rules = append(bundles[i].Rules, r)
			}
		}
	}
	sort.Slice(bundles, func(i, j int) bool {
		return bundles[i].Index < bundles[j].Index ||
			(bundles[i].Index == bundles[j].Index && bundles[i].ID < bundles[j].ID)
	})
	for _, b := range bundles {
		sortRules(b.Rules)
	}
	return bundles
}

// GetGroupBundle returns a group and all rules belong to it.
func (m *RuleManager) GetGroupBundle(id string) (b GroupBundle) {
	m.RLock()
	defer m.RUnlock()
	b.ID = id
	if g := m.ruleConfig.groups[id]; g != nil {
		b.Index, b.Override = g.Index, g.Override
		for _, r := range m.ruleConfig.rules {
			if r.GroupID == id {
				b.Rules = append(b.Rules, r)
			}
		}
		sortRules(b.Rules)
	}
	return
}

// SetAllGroupBundles resets configuration. If override is true, all old configurations are dropped.
func (m *RuleManager) SetAllGroupBundles(groups []GroupBundle, override bool) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	matchID := func(a string) bool {
		for _, g := range groups {
			if g.ID == a {
				return true
			}
		}
		return false
	}
	for k := range m.ruleConfig.rules {
		if override || matchID(k[0]) {
			p.deleteRule(k[0], k[1])
		}
	}
	for id := range m.ruleConfig.groups {
		if override || matchID(id) {
			p.deleteGroup(id)
		}
	}
	for _, g := range groups {
		p.setGroup(&RuleGroup{
			ID:       g.ID,
			Index:    g.Index,
			Override: g.Override,
		})
		for _, r := range g.Rules {
			if err := m.adjustRule(r, g.ID); err != nil {
				return err
			}
			p.setRule(r)
		}
	}
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("full config reset", zap.String("config", fmt.Sprint(groups)))
	return nil
}

// SetGroupBundle resets a Group and all rules belong to it. All old rules
// belong to the Group are dropped.
func (m *RuleManager) SetGroupBundle(group GroupBundle) error {
	m.Lock()
	defer m.Unlock()
	p := m.beginPatch()
	if _, ok := m.ruleConfig.groups[group.ID]; ok {
		for k := range m.ruleConfig.rules {
			if k[0] == group.ID {
				p.deleteRule(k[0], k[1])
			}
		}
	}
	p.setGroup(&RuleGroup{
		ID:       group.ID,
		Index:    group.Index,
		Override: group.Override,
	})
	for _, r := range group.Rules {
		if err := m.adjustRule(r, group.ID); err != nil {
			return err
		}
		p.setRule(r)
	}
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("group is reset", zap.String("group", fmt.Sprint(group)))
	return nil
}

// DeleteGroupBundle removes a Group and all rules belong to it. If `regex` is
// true, `id` is a regexp expression.
func (m *RuleManager) DeleteGroupBundle(id string, regex bool) error {
	m.Lock()
	defer m.Unlock()
	matchID := func(a string) bool { return a == id }
	if regex {
		r, err := regexp.Compile(id)
		if err != nil {
			return err
		}
		matchID = r.MatchString
	}

	p := m.beginPatch()
	for k := range m.ruleConfig.rules {
		if matchID(k[0]) {
			p.deleteRule(k[0], k[1])
		}
	}
	for _, g := range m.ruleConfig.groups {
		if matchID(g.ID) {
			p.deleteGroup(g.ID)
		}
	}
	if err := m.tryCommitPatch(p); err != nil {
		return err
	}
	log.Info("groups are removed", zap.String("id", id), zap.Bool("regexp", regex))
	return nil
}

// IsInitialized returns whether the rule manager is initialized.
func (m *RuleManager) IsInitialized() bool {
	m.RLock()
	defer m.RUnlock()
	return m.initialized
}

// checkRule check the rule whether will have RuleFit after FitRegion
// in order to reduce the calculation.
func checkRule(rule *Rule, stores []*core.StoreInfo) bool {
	for _, store := range stores {
		if MatchLabelConstraints(store, rule.LabelConstraints) {
			return true
		}
	}
	return false
}

// SetKeyType will update keyType for adjustRule()
func (m *RuleManager) SetKeyType(h string) *RuleManager {
	m.Lock()
	defer m.Unlock()
	m.keyType = h
	return m
}

func getStoresByRegion(storeSet StoreSet, region *core.RegionInfo) []*core.StoreInfo {
	r := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, peer := range region.GetPeers() {
		store := storeSet.GetStore(peer.GetStoreId())
		if store != nil {
			r = append(r, store)
		}
	}
	return r
}

func getStoreByID(stores []*core.StoreInfo, id uint64) *core.StoreInfo {
	for _, store := range stores {
		if store != nil && store.GetID() == id {
			return store
		}
	}
	return nil
}
