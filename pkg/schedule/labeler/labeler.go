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

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/rangelist"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.uber.org/zap"
)

// RegionLabeler is utility to label regions.
type RegionLabeler struct {
	storage endpoint.RuleStorage
	syncutil.RWMutex
	labelRules map[string]*LabelRule
	rangeList  rangelist.List // sorted LabelRules of the type `KeyRange`
	ctx        context.Context
	minExpire  *time.Time
}

// NewRegionLabeler creates a Labeler instance.
func NewRegionLabeler(ctx context.Context, storage endpoint.RuleStorage, gcInterval time.Duration) (*RegionLabeler, error) {
	l := &RegionLabeler{
		storage:    storage,
		labelRules: make(map[string]*LabelRule),
		ctx:        ctx,
		minExpire:  nil,
	}

	if err := l.loadRules(); err != nil {
		return nil, err
	}
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
			err = l.SaveLabelRuleLocked(rule)
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
			log.Error("failed to unmarshal label rule value", zap.String("rule-key", k), zap.String("rule-value", v), errs.ZapError(errs.ErrLoadRule))
			toDelete = append(toDelete, k)
			return
		}
		err = r.checkAndAdjust()
		if err != nil {
			log.Error("failed to adjust label rule", zap.String("rule-key", k), zap.String("rule-value", v), zap.Error(err))
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

// BuildRangeListLocked builds the range list.
func (l *RegionLabeler) BuildRangeListLocked() {
	builder := rangelist.NewBuilder()
	l.minExpire = nil
	for _, rule := range l.labelRules {
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
}

// GetSplitKeys returns all split keys in the range (start, end).
func (l *RegionLabeler) GetSplitKeys(start, end []byte) [][]byte {
	l.RLock()
	defer l.RUnlock()
	return l.rangeList.GetSplitKeys(start, end)
}

// GetAllLabelRules returns all the rules.
func (l *RegionLabeler) GetAllLabelRules() []*LabelRule {
	l.checkAndClearExpiredLabels()
	l.RLock()
	defer l.RUnlock()
	rules := make([]*LabelRule, 0, len(l.labelRules))
	for _, rule := range l.labelRules {
		rules = append(rules, rule)
	}
	return rules
}

// GetLabelRules returns the rules that match the given ids.
func (l *RegionLabeler) GetLabelRules(ids []string) ([]*LabelRule, error) {
	now := time.Now()
	rules := make([]*LabelRule, 0, len(ids))
	for _, id := range ids {
		if rule := l.getAndCheckRule(id, now); rule != nil {
			rules = append(rules, rule)
		}
	}
	return rules, nil
}

// GetLabelRule returns the Rule with the same ID.
func (l *RegionLabeler) GetLabelRule(id string) *LabelRule {
	return l.getAndCheckRule(id, time.Now())
}

func (l *RegionLabeler) getAndCheckRule(id string, now time.Time) *LabelRule {
	l.Lock()
	defer l.Unlock()
	rule, ok := l.labelRules[id]
	if !ok {
		return nil
	}
	if !rule.checkAndRemoveExpireLabels(now) {
		return rule
	}
	if len(rule.Labels) == 0 {
		if err := l.DeleteLabelRuleLocked(id); err != nil {
			log.Error("failed to delete label rule", zap.String("rule-key", id), zap.Error(err))
		}
		return nil
	}
	_ = l.SaveLabelRuleLocked(rule)
	return rule
}

// SetLabelRule inserts or updates a LabelRule.
func (l *RegionLabeler) SetLabelRule(rule *LabelRule) error {
	l.Lock()
	defer l.Unlock()
	if err := l.SetLabelRuleLocked(rule); err != nil {
		return err
	}
	l.BuildRangeListLocked()
	return nil
}

// SetLabelRuleLocked inserts or updates a LabelRule but not buildRangeList.
func (l *RegionLabeler) SetLabelRuleLocked(rule *LabelRule) error {
	if err := rule.checkAndAdjust(); err != nil {
		return err
	}
	if err := l.SaveLabelRuleLocked(rule); err != nil {
		return err
	}
	l.labelRules[rule.ID] = rule
	return nil
}

// SaveLabelRuleLocked inserts or updates a LabelRule but not buildRangeList.
// It only saves the rule to storage, and does not update the in-memory states.
func (l *RegionLabeler) SaveLabelRuleLocked(rule *LabelRule) error {
	return l.storage.RunInTxn(l.ctx, func(txn kv.Txn) error {
		return l.storage.SaveRegionRule(txn, rule.ID, rule)
	})
}

// DeleteLabelRule removes a LabelRule.
func (l *RegionLabeler) DeleteLabelRule(id string) error {
	l.Lock()
	defer l.Unlock()
	if _, ok := l.labelRules[id]; !ok {
		return errs.ErrRegionRuleNotFound.FastGenByArgs(id)
	}
	if err := l.DeleteLabelRuleLocked(id); err != nil {
		return err
	}
	l.BuildRangeListLocked()
	return nil
}

// DeleteLabelRuleLocked removes a LabelRule but not buildRangeList.
func (l *RegionLabeler) DeleteLabelRuleLocked(id string) error {
	if err := l.storage.RunInTxn(l.ctx, func(txn kv.Txn) error {
		return l.storage.DeleteRegionRule(txn, id)
	}); err != nil {
		return err
	}
	delete(l.labelRules, id)
	return nil
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
		delete(l.labelRules, key)
	}
	for _, rule := range setRulesMap {
		l.labelRules[rule.ID] = rule
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
	if i, data := l.rangeList.GetData(region.GetStartKey(), region.GetEndKey()); i != -1 {
		for _, rule := range data {
			r := rule.(*LabelRule)
			if r.Index <= index && value != "" {
				continue
			}
			for _, l := range r.Labels {
				if l.expireBefore(now) {
					continue
				}
				if l.Key == key {
					value, index = l.Value, r.Index
				}
			}
		}
	}
	return value
}

// ScheduleDisabled returns true if the region is lablelld with schedule-disabled.
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
	if i, data := l.rangeList.GetData(region.GetStartKey(), region.GetEndKey()); i != -1 {
		for _, rule := range data {
			r := rule.(*LabelRule)
			for _, l := range r.Labels {
				if l.expireBefore(now) {
					continue
				}
				if old, ok := labels[l.Key]; !ok || old.index < r.Index {
					labels[l.Key] = valueIndex{l.Value, r.Index}
				}
			}
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

// MakeKeyRanges is a helper function to make key ranges.
func MakeKeyRanges(keys ...string) []any {
	var res []any
	for i := 0; i < len(keys); i += 2 {
		res = append(res, map[string]any{"start_key": keys[i], "end_key": keys[i+1]})
	}
	return res
}
