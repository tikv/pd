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

package labeler

import (
	"time"

	"github.com/tikv/pd/pkg/schedule/rangelist"
)

// labelRuleIndex owns all in-memory views of label rules. rules is the
// authoritative map, generic and keyspaces partition it, and ranges is derived
// from generic. Its caller must hold RegionLabeler's lock while mutating it.
type labelRuleIndex struct {
	rules       map[string]*LabelRule
	generic     map[string]*LabelRule
	keyspaces   keyspaceRuleIndex
	ranges      rangelist.List
	rangesDirty bool
	minExpire   *time.Time
}

func newLabelRuleIndex(rules map[string]*LabelRule) labelRuleIndex {
	if rules == nil {
		rules = make(map[string]*LabelRule)
	}
	index := labelRuleIndex{
		rules:       rules,
		generic:     make(map[string]*LabelRule),
		rangesDirty: true,
	}
	for _, rule := range rules {
		if !index.keyspaces.Add(rule) {
			index.generic[rule.ID] = rule
		}
	}
	index.buildRanges()
	return index
}

func (i *labelRuleIndex) set(rule *LabelRule) {
	if old, ok := i.rules[rule.ID]; ok {
		if i.keyspaces.Replace(old, rule) {
			i.rules[rule.ID] = rule
			return
		}
		if !i.keyspaces.Remove(rule.ID, old) {
			delete(i.generic, old.ID)
			i.rangesDirty = true
		}
	}
	i.rules[rule.ID] = rule
	if !i.keyspaces.Add(rule) {
		i.generic[rule.ID] = rule
		i.rangesDirty = true
	}
}

func (i *labelRuleIndex) delete(id string) {
	rule, ok := i.rules[id]
	if !ok {
		return
	}
	if !i.keyspaces.Remove(id, rule) {
		delete(i.generic, id)
		i.rangesDirty = true
	}
	delete(i.rules, id)
}

func (i *labelRuleIndex) removeExpiredLabels(rule *LabelRule, now time.Time) bool {
	if !rule.checkAndRemoveExpireLabels(now) {
		return false
	}
	// Rebuild on publish to refresh minExpire together with the range view.
	i.rangesDirty = true
	return true
}

func (i *labelRuleIndex) buildRanges() {
	if !i.rangesDirty {
		return
	}

	builder := rangelist.NewBuilder()
	i.minExpire = nil
	for _, rule := range i.generic {
		if i.minExpire == nil || rule.expireBefore(*i.minExpire) {
			i.minExpire = rule.minExpire
		}
		if rule.RuleType == KeyRange {
			for _, r := range rule.Data.([]*KeyRangeRule) {
				builder.AddItem(r.StartKey, r.EndKey, rule)
			}
		}
	}
	i.ranges = builder.Build()
	i.rangesDirty = false
}

func (i *labelRuleIndex) getSplitKeys(start, end []byte) [][]byte {
	return mergeSplitKeys(
		i.ranges.GetSplitKeys(start, end),
		i.keyspaces.GetSplitKeys(start, end),
	)
}

func (i *labelRuleIndex) getRangeRules(start, end []byte) ([]any, *LabelRule, bool) {
	rules, ok := i.ranges.GetDataByRange(start, end)
	if !ok {
		return nil, nil, false
	}
	keyspaceRule := i.keyspaces.GetRule(start, end)
	if keyspaceRule == nil && i.keyspaces.HasSplitKey(start, end) {
		return nil, nil, false
	}
	return rules, keyspaceRule, true
}
