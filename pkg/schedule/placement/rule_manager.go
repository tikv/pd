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
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	// DefaultGroupID is the default rule group ID.
	DefaultGroupID = "pd"
	// DefaultRuleID is the default rule ID.
	DefaultRuleID = "default"
	// defaultWitnessRuleID is the default witness rule ID.
	defaultWitnessRuleID = "witness"
)

// RuleManager is responsible for the lifecycle of all placement Rules.
// It is thread safe.
type RuleManager struct {
	ctx     context.Context
	storage endpoint.RuleStorage
	syncutil.RWMutex
	initialized bool
	ruleConfig  *ruleConfig
	ruleList    ruleList

	// used for rule validation
	keyType          string
	storeSetInformer core.StoreSetInformer
	cache            *RegionRuleFitCacheManager
	conf             config.SharedConfigProvider
}

// NewRuleManager creates a RuleManager instance.
func NewRuleManager(ctx context.Context, storage endpoint.RuleStorage, storeSetInformer core.StoreSetInformer, conf config.SharedConfigProvider) *RuleManager {
	return &RuleManager{
		ctx:              ctx,
		storage:          storage,
		storeSetInformer: storeSetInformer,
		conf:             conf,
		ruleConfig:       newRuleConfig(),
		cache:            NewRegionRuleFitCacheManager(),
	}
}

// Initialize loads rules from storage. If Placement Rules feature is never enabled, it creates default rule that is
// compatible with previous configuration.
func (m *RuleManager) Initialize(maxReplica int, locationLabels []string, isolationLevel string) error {
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
		var defaultRules []*Rule
		if m.conf != nil && m.conf.IsWitnessAllowed() && maxReplica >= 3 {
			// Because maxReplica is actually always an odd number, so directly divided by 2
			witnessCount := maxReplica / 2
			defaultRules = append(defaultRules,
				[]*Rule{
					{
						GroupID:        DefaultGroupID,
						ID:             DefaultRuleID,
						Role:           Voter,
						Count:          maxReplica - witnessCount,
						LocationLabels: locationLabels,
						IsolationLevel: isolationLevel,
					},
					{
						GroupID:        DefaultGroupID,
						ID:             defaultWitnessRuleID,
						Role:           Voter,
						Count:          witnessCount,
						IsWitness:      true,
						LocationLabels: locationLabels,
						IsolationLevel: isolationLevel,
					},
				}...,
			)
		} else {
			defaultRules = append(defaultRules, &Rule{
				GroupID:        DefaultGroupID,
				ID:             DefaultRuleID,
				Role:           Voter,
				Count:          maxReplica,
				LocationLabels: locationLabels,
				IsolationLevel: isolationLevel,
			})
		}
		if err := m.storage.RunInTxn(m.ctx, func(txn kv.Txn) (err error) {
			for _, defaultRule := range defaultRules {
				if err := m.storage.SaveRule(txn, defaultRule.StoreKey(), defaultRule); err != nil {
					// TODO: Need to delete the previously successfully saved Rules?
					return err
				}
				m.ruleConfig.setRule(defaultRule)
			}
			return nil
		}); err != nil {
			return err
		}
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
	var (
		toSave   []*Rule
		toDelete []string
	)
	// load rules from storage
	err := m.storage.LoadRules(func(k, v string) {
		r, err := NewRuleFromJSON([]byte(v))
		if err != nil {
			log.Error("failed to unmarshal rule value", zap.String("rule-key", k), zap.String("rule-value", v), errs.ZapError(errs.ErrLoadRule))
			toDelete = append(toDelete, k)
			return
		}
		err = m.AdjustRule(r, "")
		if err != nil {
			log.Error("rule is in bad format", zap.String("rule-key", k), zap.String("rule-value", v), errs.ZapError(errs.ErrLoadRule, err))
			toDelete = append(toDelete, k)
			return
		}
		_, ok := m.ruleConfig.rules[r.Key()]
		if ok {
			log.Error("duplicated rule key", zap.String("rule-key", k), zap.String("rule-value", v), errs.ZapError(errs.ErrLoadRule))
			toDelete = append(toDelete, k)
			return
		}
		if k != r.StoreKey() {
			log.Error("mismatch data key, need to restore", zap.String("rule-key", k), zap.String("rule-value", v), errs.ZapError(errs.ErrLoadRule))
			toDelete = append(toDelete, k)
			toSave = append(toSave, r)
		}
		m.ruleConfig.rules[r.Key()] = r
	})
	if err != nil {
		return err
	}
	// save the rules with mismatch data key or bad format
	var batch []func(kv.Txn) error
	for _, s := range toSave {
		localRule := s
		batch = append(batch, func(txn kv.Txn) error {
			return m.storage.SaveRule(txn, localRule.StoreKey(), localRule)
		})
	}
	for _, d := range toDelete {
		localKey := d
		batch = append(batch, func(txn kv.Txn) error {
			return m.storage.DeleteRule(txn, localKey)
		})
	}
	return endpoint.RunBatchOpInTxn(m.ctx, m.storage, batch)
}

func (m *RuleManager) loadGroups() error {
	return m.storage.LoadRuleGroups(func(k, v string) {
		g, err := NewRuleGroupFromJSON([]byte(v))
		if err != nil {
			log.Error("failed to unmarshal rule group", zap.String("group-id", k), errs.ZapError(errs.ErrLoadRuleGroup, err))
			return
		}
		m.ruleConfig.groups[g.ID] = g
	})
}

// AdjustRule check and adjust rule from client or storage.
func (m *RuleManager) AdjustRule(r *Rule, groupID string) (err error) {
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

	if m.keyType == constant.Table.String() || m.keyType == constant.Txn.String() {
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
	if r.IsWitness && r.Count > m.conf.GetMaxReplicas()/2 {
		return errs.ErrRuleContent.FastGenByArgs(fmt.Sprintf("define too many witness by count %d", r.Count))
	}
	for _, c := range r.LabelConstraints {
		if !validateOp(c.Op) {
			return errs.ErrRuleContent.FastGenByArgs(fmt.Sprintf("invalid op %s", c.Op))
		}
		if r.IsWitness && c.Key == core.EngineKey && slices.Contains(c.Values, core.EngineTiFlash) {
			return errs.ErrRuleContent.FastGenByArgs("witness can't combine with tiflash")
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
	return m.GetRuleLocked(group, id)
}

// GetRuleLocked returns the Rule with the same (group, id).
func (m *RuleManager) GetRuleLocked(group, id string) *Rule {
	if r := m.ruleConfig.getRule([2]string{group, id}); r != nil {
		return r.Clone()
	}
	return nil
}

// SetRule inserts or updates a Rule.
func (m *RuleManager) SetRule(rule *Rule) error {
	if err := m.AdjustRule(rule, ""); err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()
	p := m.BeginPatch()
	p.SetRule(rule)
	if err := m.TryCommitPatchLocked(p); err != nil {
		return err
	}
	log.Info("placement rule updated", zap.String("rule", fmt.Sprint(rule)))
	return nil
}

// DeleteRule removes a Rule.
func (m *RuleManager) DeleteRule(group, id string) error {
	m.Lock()
	defer m.Unlock()
	p := m.BeginPatch()
	p.DeleteRule(group, id)
	if err := m.TryCommitPatchLocked(p); err != nil {
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

// GetRulesCount returns the number of rules.
func (m *RuleManager) GetRulesCount() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.ruleConfig.rules)
}

// GetGroupsCount returns the number of rule groups.
func (m *RuleManager) GetGroupsCount() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.ruleConfig.groups)
}

// GetRulesByGroup returns sorted rules of a group.
func (m *RuleManager) GetRulesByGroup(group string) []*Rule {
	m.RLock()
	defer m.RUnlock()
	return m.GetRulesByGroupLocked(group)
}

// GetRulesByGroupLocked returns sorted rules of a group.
func (m *RuleManager) GetRulesByGroupLocked(group string) []*Rule {
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
	return m.ruleList.getRulesForApplyRange(region.GetStartKey(), region.GetEndKey())
}

// GetRulesForApplyRange returns the rules list that should be applied to a range.
func (m *RuleManager) GetRulesForApplyRange(start, end []byte) []*Rule {
	m.RLock()
	defer m.RUnlock()
	return m.ruleList.getRulesForApplyRange(start, end)
}

// IsRegionFitCached returns whether the RegionFit can be cached.
func (m *RuleManager) IsRegionFitCached(storeSet StoreSet, region *core.RegionInfo) bool {
	regionStores := getStoresByRegion(storeSet, region)
	rules := m.GetRulesForApplyRegion(region)
	isCached, _ := m.cache.CheckAndGetCache(region, rules, regionStores)
	return isCached
}

// FitRegion fits a region to the rules it matches.
func (m *RuleManager) FitRegion(storeSet StoreSet, region *core.RegionInfo) (fit *RegionFit) {
	regionStores := getStoresByRegion(storeSet, region)
	rules := m.GetRulesForApplyRegion(region)
	var isCached bool
	if m.conf.IsPlacementRulesCacheEnabled() {
		if isCached, fit = m.cache.CheckAndGetCache(region, rules, regionStores); isCached && fit != nil {
			return fit
		}
	}
	fit = fitRegion(regionStores, region, rules, m.conf.IsWitnessAllowed())
	fit.regionStores = regionStores
	fit.rules = rules
	if isCached {
		m.SetRegionFitCache(region, fit)
	}
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

// SetPlaceholderRegionFitCache sets a placeholder region fit cache information
// Only used for testing
func (m *RuleManager) SetPlaceholderRegionFitCache(region *core.RegionInfo) {
	placeholderCache := &regionRuleFitCache{region: toRegionCache(region)}
	m.cache.mu.Lock()
	defer m.cache.mu.Unlock()
	m.cache.regionCaches[region.GetID()] = placeholderCache
}

// CheckIsCachedDirectly returns whether the region's fit is cached
// Only used for testing
func (m *RuleManager) CheckIsCachedDirectly(regionID uint64) bool {
	m.cache.mu.RLock()
	defer m.cache.mu.RUnlock()
	_, ok := m.cache.regionCaches[regionID]
	return ok
}

// BeginPatch returns a patch for multiple changes.
func (m *RuleManager) BeginPatch() *RuleConfigPatch {
	return m.ruleConfig.beginPatch()
}

// TryCommitPatchLocked tries to commit a patch.
func (m *RuleManager) TryCommitPatchLocked(patch *RuleConfigPatch) error {
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
	var batch []func(kv.Txn) error
	// add rules to batch
	for key, r := range p.rules {
		localKey, localRule := key, r
		if r == nil {
			rule := &Rule{GroupID: localKey[0], ID: localKey[1]}
			batch = append(batch, func(txn kv.Txn) error {
				return m.storage.DeleteRule(txn, rule.StoreKey())
			})
		} else {
			batch = append(batch, func(txn kv.Txn) error {
				return m.storage.SaveRule(txn, localRule.StoreKey(), localRule)
			})
		}
	}
	// add groups to batch
	for id, g := range p.groups {
		localID, localGroup := id, g
		if g.isDefault() {
			batch = append(batch, func(txn kv.Txn) error {
				return m.storage.DeleteRuleGroup(txn, localID)
			})
		} else {
			batch = append(batch, func(txn kv.Txn) error {
				return m.storage.SaveRuleGroup(txn, localID, localGroup)
			})
		}
	}
	return endpoint.RunBatchOpInTxn(m.ctx, m.storage, batch)
}

// SetRules inserts or updates lots of Rules at once.
func (m *RuleManager) SetRules(rules []*Rule) error {
	m.Lock()
	defer m.Unlock()
	p := m.BeginPatch()
	for _, r := range rules {
		if err := m.AdjustRule(r, ""); err != nil {
			return err
		}
		p.SetRule(r)
	}
	if err := m.TryCommitPatchLocked(p); err != nil {
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
			err := m.AdjustRule(t.Rule, "")
			if err != nil {
				return err
			}
		}
	}

	m.Lock()
	defer m.Unlock()

	patch := m.BeginPatch()
	for _, t := range todo {
		switch t.Action {
		case RuleOpAdd:
			patch.SetRule(t.Rule)
		case RuleOpDel:
			if !t.DeleteByIDPrefix {
				patch.DeleteRule(t.GroupID, t.ID)
			} else {
				m.ruleConfig.iterateRules(func(r *Rule) {
					if r.GroupID == t.GroupID && strings.HasPrefix(r.ID, t.ID) {
						patch.DeleteRule(r.GroupID, r.ID)
					}
				})
			}
		}
	}

	if err := m.TryCommitPatchLocked(patch); err != nil {
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
	p := m.BeginPatch()
	p.SetGroup(group)
	if err := m.TryCommitPatchLocked(p); err != nil {
		return err
	}
	log.Info("group config updated", zap.String("group", fmt.Sprint(group)))
	return nil
}

// DeleteRuleGroup removes a RuleGroup.
func (m *RuleManager) DeleteRuleGroup(id string) error {
	m.Lock()
	defer m.Unlock()
	p := m.BeginPatch()
	p.DeleteGroup(id)
	if err := m.TryCommitPatchLocked(p); err != nil {
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
	p := m.BeginPatch()
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
			p.DeleteRule(k[0], k[1])
		}
	}
	for id := range m.ruleConfig.groups {
		if override || matchID(id) {
			p.DeleteGroup(id)
		}
	}
	for _, g := range groups {
		p.SetGroup(&RuleGroup{
			ID:       g.ID,
			Index:    g.Index,
			Override: g.Override,
		})
		for _, r := range g.Rules {
			if err := m.AdjustRule(r, g.ID); err != nil {
				return err
			}
			p.SetRule(r)
		}
	}
	if err := m.TryCommitPatchLocked(p); err != nil {
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
	p := m.BeginPatch()
	if _, ok := m.ruleConfig.groups[group.ID]; ok {
		for k := range m.ruleConfig.rules {
			if k[0] == group.ID {
				p.DeleteRule(k[0], k[1])
			}
		}
	}
	p.SetGroup(&RuleGroup{
		ID:       group.ID,
		Index:    group.Index,
		Override: group.Override,
	})
	for _, r := range group.Rules {
		if err := m.AdjustRule(r, group.ID); err != nil {
			return err
		}
		p.SetRule(r)
	}
	if err := m.TryCommitPatchLocked(p); err != nil {
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

	p := m.BeginPatch()
	for k := range m.ruleConfig.rules {
		if matchID(k[0]) {
			p.DeleteRule(k[0], k[1])
		}
	}
	for _, g := range m.ruleConfig.groups {
		if matchID(g.ID) {
			p.DeleteGroup(g.ID)
		}
	}
	if err := m.TryCommitPatchLocked(p); err != nil {
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
	return slice.AnyOf(stores, func(idx int) bool {
		return MatchLabelConstraints(stores[idx], rule.LabelConstraints)
	})
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
