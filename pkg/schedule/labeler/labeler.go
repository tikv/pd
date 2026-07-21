// Copyright 2021 TiKV Project Authors.
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
	"context"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/rangelist"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

// RegionLabeler is utility to label regions.
type RegionLabeler struct {
	storage endpoint.RuleStorage
	syncutil.RWMutex
	labelRules      map[string]*LabelRule
	genericRules    map[string]*LabelRule
	keyspaceRules   keyspaceRuleIndex
	rangeList       rangelist.List // sorted generic LabelRules of the type `KeyRange`
	rangeListDirty  bool
	rangeIndexReady bool
	ctx             context.Context
	minExpire       *time.Time
}

// NewRegionLabeler creates a Labeler instance.
func NewRegionLabeler(ctx context.Context, storage endpoint.RuleStorage, gcInterval time.Duration) (*RegionLabeler, error) {
	start := time.Now()
	defer func() {
		newRegionLabelerDuration.Observe(time.Since(start).Seconds())
	}()

	l := &RegionLabeler{
		storage:        storage,
		labelRules:     make(map[string]*LabelRule),
		genericRules:   make(map[string]*LabelRule),
		rangeListDirty: true,
		ctx:            ctx,
		minExpire:      nil,
	}

	if err := l.loadRules(); err != nil {
		return nil, err
	}
	log.Info("new region labeler created", zap.Int("label-rules-count", len(l.labelRules)))
	go l.doGC(gcInterval)
	return l, nil
}

func (l *RegionLabeler) doGC(gcInterval time.Duration) {
	defer logutil.LogPanic()

	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.checkAndClearExpiredLabels()
			log.Debug("region labeler GC")
		case <-l.ctx.Done():
			log.Info("region labeler GC stopped")
			return
		}
	}
}

func (l *RegionLabeler) checkAndClearExpiredLabels() {
	now := time.Now()
	l.Lock()
	defer l.Unlock()

	if l.minExpire == nil || l.minExpire.After(now) {
		return
	}
	var err error
	deleted := false

	for key, rule := range l.labelRules {
		if !rule.checkAndRemoveExpireLabels(now) {
			continue
		}
		if len(rule.Labels) == 0 {
			err = l.DeleteLabelRuleLocked(key)
			if err == nil {
				deleted = true
			}
		} else {
			err = l.storage.RunInTxn(l.ctx, func(txn kv.Txn) error {
				return l.storage.SaveRegionRule(txn, rule.ID, rule)
			})
		}
		if err != nil {
			log.Error("failed to save rule expired label rule", zap.String("rule-key", key), zap.Error(err))
		}
	}
	if deleted {
		l.BuildRangeListLocked()
	}
}

func (l *RegionLabeler) loadRules() error {
	var toDelete []string
	err := l.storage.LoadRegionRules(func(k, v string) {
		r, err := NewLabelRuleFromJSON([]byte(v))
		if err != nil {
			if errs.ErrRegionRuleContent.Equal(err) {
				log.Warn("failed to adjust label rule", zap.String("rule-key", k), zap.String("rule-value", v), zap.Error(err))
			} else {
				log.Warn("failed to unmarshal label rule value", zap.String("rule-key", k), zap.String("rule-value", v), errs.ZapError(errs.ErrLoadRule))
			}
			toDelete = append(toDelete, k)
			return
		}
		l.labelRules[r.ID] = r
	})
	if err != nil {
		return err
	}
	for _, id := range toDelete {
		if err := l.storage.RunInTxn(l.ctx, func(txn kv.Txn) error {
			return l.storage.DeleteRegionRule(txn, id)
		}); err != nil {
			return err
		}
	}
	l.BuildRangeListLocked()
	return nil
}

// BuildRangeListLocked builds the generic range list when necessary. Canonical
// keyspace rules are indexed separately and updated incrementally.
func (l *RegionLabeler) BuildRangeListLocked() {
	if !l.rangeIndexReady {
		for _, rule := range l.labelRules {
			if !l.keyspaceRules.Add(rule) {
				l.genericRules[rule.ID] = rule
			}
		}
		l.rangeIndexReady = true
		l.rangeListDirty = true
	}
	if !l.rangeListDirty {
		return
	}

	builder := rangelist.NewBuilder()
	l.minExpire = nil
	for _, rule := range l.genericRules {
		if l.minExpire == nil || rule.expireBefore(*l.minExpire) {
			l.minExpire = rule.minExpire
		}
		if rule.RuleType == KeyRange {
			rs := rule.Data.([]*KeyRangeRule)
			for _, r := range rs {
				builder.AddItem(r.StartKey, r.EndKey, rule)
			}
		}
	}
	l.rangeList = builder.Build()
	l.rangeListDirty = false
}

// GetSplitKeys returns all split keys in the range (start, end).
func (l *RegionLabeler) GetSplitKeys(start, end []byte) [][]byte {
	l.RLock()
	defer l.RUnlock()
	return mergeSplitKeys(
		l.rangeList.GetSplitKeys(start, end),
		l.keyspaceRules.GetSplitKeys(start, end),
	)
}

func filterExpiredLabels(rule *LabelRule, now time.Time) *LabelRule {
	if rule == nil {
		return nil
	}

	hasExpired := false
	for i := range rule.Labels {
		if rule.Labels[i].expireBefore(now) {
			hasExpired = true
			break
		}
	}
	if !hasExpired {
		return rule
	}

	labels := make([]RegionLabel, 0, len(rule.Labels))
	for i := range rule.Labels {
		if !rule.Labels[i].expireBefore(now) {
			labels = append(labels, rule.Labels[i])
		}
	}
	if len(labels) == 0 {
		return nil
	}
	return &LabelRule{
		ID:       rule.ID,
		Index:    rule.Index,
		RuleType: rule.RuleType,
		Data:     rule.Data,
		Labels:   labels,
	}
}

// GetAllLabelRules returns all the rules.
func (l *RegionLabeler) GetAllLabelRules() []*LabelRule {
	l.RLock()
	defer l.RUnlock()

	now := time.Now()
	rules := make([]*LabelRule, 0, len(l.labelRules))
	for _, rule := range l.labelRules {
		if filteredRule := filterExpiredLabels(rule, now); filteredRule != nil {
			rules = append(rules, filteredRule)
		}
	}
	return rules
}

// GetRuleAndKeyRangeCounts returns the number of effective label rules and key ranges.
func (l *RegionLabeler) GetRuleAndKeyRangeCounts() (ruleCount, keyRangeCount int) {
	l.RLock()
	defer l.RUnlock()

	now := time.Now()
	for _, rule := range l.labelRules {
		filteredRule := filterExpiredLabels(rule, now)
		if filteredRule == nil {
			continue
		}
		ruleCount++
		keyRangeCount += len(filteredRule.GetKeyRanges())
	}
	return ruleCount, keyRangeCount
}

// GetLabelRules returns the rules that match the given ids.
func (l *RegionLabeler) GetLabelRules(ids []string) ([]*LabelRule, error) {
	l.RLock()
	defer l.RUnlock()

	now := time.Now()
	rules := make([]*LabelRule, 0, len(ids))
	for _, id := range ids {
		if rule, ok := l.labelRules[id]; ok {
			if filteredRule := filterExpiredLabels(rule, now); filteredRule != nil {
				rules = append(rules, filteredRule)
			}
		}
	}
	return rules, nil
}

// GetLabelRule returns the Rule with the same ID.
func (l *RegionLabeler) GetLabelRule(id string) *LabelRule {
	l.RLock()
	defer l.RUnlock()
	return l.GetLabelRuleLocked(id)
}

// GetLabelRuleLocked returns the Rule with the same ID.
func (l *RegionLabeler) GetLabelRuleLocked(id string) *LabelRule {
	rule, ok := l.labelRules[id]
	if !ok {
		return nil
	}
	return filterExpiredLabels(rule, time.Now())
}

// SetLabelRule inserts or updates a LabelRule.
func (l *RegionLabeler) SetLabelRule(rule *LabelRule) error {
	if err := rule.checkAndAdjust(); err != nil {
		return err
	}
	if err := l.storage.RunInTxn(l.ctx, func(txn kv.Txn) error {
		return l.storage.SaveRegionRule(txn, rule.ID, rule)
	}); err != nil {
		return err
	}

	// only Lock for in-memory update
	l.Lock()
	defer l.Unlock()
	l.setLabelRuleInMemoryLocked(rule)
	l.BuildRangeListLocked()
	return nil
}

// SetLabelRuleLocked inserts or updates a LabelRule but not buildRangeList.
// It updates the in-memory states and storage at the same time.
// Callers must have already validated/adjusted the rule (checkAndAdjust or
// NewLabelRuleFromJSON), because this method does not re-validate.
// It should be used in watcher.
func (l *RegionLabeler) SetLabelRuleLocked(rule *LabelRule) error {
	if err := l.storage.RunInTxn(l.ctx, func(txn kv.Txn) error {
		return l.storage.SaveRegionRule(txn, rule.ID, rule)
	}); err != nil {
		return err
	}
	l.setLabelRuleInMemoryLocked(rule)
	return nil
}

// DeleteLabelRule removes a LabelRule.
func (l *RegionLabeler) DeleteLabelRule(id string) error {
	if err := l.storage.RunInTxn(l.ctx, func(txn kv.Txn) error {
		return l.storage.DeleteRegionRule(txn, id)
	}); err != nil {
		return err
	}

	// only Lock for in-memory update
	l.Lock()
	defer l.Unlock()
	if _, ok := l.labelRules[id]; !ok {
		return errs.ErrRegionRuleNotFound.FastGenByArgs(id)
	}
	l.deleteLabelRuleInMemoryLocked(id)
	l.BuildRangeListLocked()
	return nil
}

// DeleteLabelRuleLocked removes a LabelRule but not buildRangeList.
// It updates the in-memory states and storage at the same time.
// It should be used in watcher.
func (l *RegionLabeler) DeleteLabelRuleLocked(id string) error {
	if err := l.storage.RunInTxn(l.ctx, func(txn kv.Txn) error {
		return l.storage.DeleteRegionRule(txn, id)
	}); err != nil {
		return err
	}
	l.deleteLabelRuleInMemoryLocked(id)
	return nil
}

func (l *RegionLabeler) setLabelRuleInMemoryLocked(rule *LabelRule) {
	if old, ok := l.labelRules[rule.ID]; ok {
		if !l.keyspaceRules.Remove(rule.ID, old) {
			delete(l.genericRules, old.ID)
			l.rangeListDirty = true
		}
	}
	l.labelRules[rule.ID] = rule
	if !l.keyspaceRules.Add(rule) {
		l.genericRules[rule.ID] = rule
		l.rangeListDirty = true
	}
}

func (l *RegionLabeler) deleteLabelRuleInMemoryLocked(id string) {
	rule, ok := l.labelRules[id]
	if !ok {
		return
	}
	if !l.keyspaceRules.Remove(id, rule) {
		delete(l.genericRules, id)
		l.rangeListDirty = true
	}
	delete(l.labelRules, id)
}

// Patch updates multiple region rules in a batch.
func (l *RegionLabeler) Patch(patch LabelRulePatch) error {
	// setRulesMap is used to solve duplicate entries in DeleteRules and SetRules.
	// Note: We maintain compatibility with the previous behavior, which is to process DeleteRules before SetRules
	// If there are duplicate rules, we will prioritize SetRules and select the last one from SetRules.
	setRulesMap := make(map[string]*LabelRule)

	for _, rule := range patch.SetRules {
		if err := rule.checkAndAdjust(); err != nil {
			return err
		}
		setRulesMap[rule.ID] = rule
	}

	// save to storage
	var batch []func(kv.Txn) error
	for _, key := range patch.DeleteRules {
		if _, ok := setRulesMap[key]; ok {
			continue
		}
		localKey := key
		batch = append(batch, func(txn kv.Txn) error {
			return l.storage.DeleteRegionRule(txn, localKey)
		})
	}
	for _, rule := range setRulesMap {
		localID, localRule := rule.ID, rule
		batch = append(batch, func(txn kv.Txn) error {
			return l.storage.SaveRegionRule(txn, localID, localRule)
		})
	}
	if err := endpoint.RunBatchOpInTxn(l.ctx, l.storage, batch); err != nil {
		return err
	}

	// update in-memory states.
	l.Lock()
	defer l.Unlock()

	for _, key := range patch.DeleteRules {
		l.deleteLabelRuleInMemoryLocked(key)
	}
	for _, rule := range setRulesMap {
		l.setLabelRuleInMemoryLocked(rule)
	}
	l.BuildRangeListLocked()
	return nil
}

// GetRegionLabel returns the label of the region for a key.
// If there are multiple rules that match the key, the one with max rule index will be returned.
func (l *RegionLabeler) GetRegionLabel(region *core.RegionInfo, key string) string {
	l.RLock()
	defer l.RUnlock()
	now := time.Now()
	value, index := "", -1
	// search ranges
	rules, keyspaceRule, ok := l.getRangeRulesLocked(region.GetStartKey(), region.GetEndKey())
	if !ok {
		return ""
	}
	applyRule := func(r *LabelRule) {
		if r.Index <= index && value != "" {
			return
		}
		for _, label := range r.Labels {
			if label.expireBefore(now) {
				continue
			}
			if label.Key == key {
				value, index = label.Value, r.Index
			}
		}
	}
	for _, rule := range rules {
		applyRule(rule.(*LabelRule))
	}
	if keyspaceRule != nil {
		applyRule(keyspaceRule)
	}
	return value
}

// ScheduleDisabled returns true if the region is labeled with schedule-disabled.
func (l *RegionLabeler) ScheduleDisabled(region *core.RegionInfo) bool {
	v := l.GetRegionLabel(region, scheduleOptionLabel)
	return strings.EqualFold(v, scheduleOptionValueDeny)
}

// GetRegionLabels returns the labels of the region.
// For each key, the label with max rule index will be returned.
func (l *RegionLabeler) GetRegionLabels(region *core.RegionInfo) []*RegionLabel {
	l.RLock()
	defer l.RUnlock()
	type valueIndex struct {
		value string
		index int
	}
	labels := make(map[string]valueIndex)
	now := time.Now()
	// search ranges
	rules, keyspaceRule, ok := l.getRangeRulesLocked(region.GetStartKey(), region.GetEndKey())
	if ok {
		applyRule := func(r *LabelRule) {
			for _, label := range r.Labels {
				if label.expireBefore(now) {
					continue
				}
				if old, ok := labels[label.Key]; !ok || old.index < r.Index {
					labels[label.Key] = valueIndex{label.Value, r.Index}
				}
			}
		}
		for _, rule := range rules {
			applyRule(rule.(*LabelRule))
		}
		if keyspaceRule != nil {
			applyRule(keyspaceRule)
		}
	}
	result := make([]*RegionLabel, 0, len(labels))
	for k, l := range labels {
		result = append(result, &RegionLabel{
			Key:   k,
			Value: l.value,
		})
	}
	return result
}

func (l *RegionLabeler) getRangeRulesLocked(start, end []byte) ([]any, *LabelRule, bool) {
	rules, ok := l.rangeList.GetDataByRange(start, end)
	if !ok {
		return nil, nil, false
	}
	keyspaceRule := l.keyspaceRules.GetRule(start, end)
	if keyspaceRule == nil && l.keyspaceRules.HasSplitKey(start, end) {
		return nil, nil, false
	}
	return rules, keyspaceRule, true
}

// MakeKeyRanges is a helper function to make key ranges.
func MakeKeyRanges(keys ...string) []any {
	var res []any
	for i := 0; i < len(keys); i += 2 {
		res = append(res, map[string]any{"start_key": keys[i], "end_key": keys[i+1]})
	}
	return res
}

// IterateLabelRules iterates the label rules. It will return once the iterator returns false.
func (l *RegionLabeler) IterateLabelRules(iterator func(rule *LabelRule) bool) {
	l.RLock()
	defer l.RUnlock()
	for _, rule := range l.labelRules {
		if !iterator(rule) {
			return
		}
	}
}
