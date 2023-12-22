// Copyright 2022 TiKV Project Authors.
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

package endpoint

import (
	"context"

	"github.com/tikv/pd/pkg/storage/kv"
)

// RuleStorage defines the storage operations on the rule.
type RuleStorage interface {
	LoadRules(txn kv.Txn, f func(k, v string)) error
	SaveRule(txn kv.Txn, ruleKey string, rule interface{}) error
	DeleteRule(txn kv.Txn, ruleKey string) error
	LoadRuleGroups(txn kv.Txn, f func(k, v string)) error
	SaveRuleGroup(txn kv.Txn, groupID string, group interface{}) error
	DeleteRuleGroup(txn kv.Txn, groupID string) error
	// LoadRule is used only in rule watcher.
	LoadRule(ruleKey string) (string, error)

	LoadRegionRules(f func(k, v string)) error
	SaveRegionRule(ruleKey string, rule interface{}) error
	DeleteRegionRule(ruleKey string) error
	RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error
}

var _ RuleStorage = (*StorageEndpoint)(nil)

// SaveRule stores a rule cfg to the rulesPath.
func (se *StorageEndpoint) SaveRule(txn kv.Txn, ruleKey string, rule interface{}) error {
	return saveJSONInTxn(txn, ruleKeyPath(ruleKey), rule)
}

// DeleteRule removes a rule from storage.
func (se *StorageEndpoint) DeleteRule(txn kv.Txn, ruleKey string) error {
	return txn.Remove(ruleKeyPath(ruleKey))
}

// LoadRuleGroups loads all rule groups from storage.
func (se *StorageEndpoint) LoadRuleGroups(txn kv.Txn, f func(k, v string)) error {
	return loadRangeByPrefixInTxn(txn, ruleGroupPath+"/", f)
}

// SaveRuleGroup stores a rule group config to storage.
func (se *StorageEndpoint) SaveRuleGroup(txn kv.Txn, groupID string, group interface{}) error {
	return saveJSONInTxn(txn, ruleGroupIDPath(groupID), group)
}

// DeleteRuleGroup removes a rule group from storage.
func (se *StorageEndpoint) DeleteRuleGroup(txn kv.Txn, groupID string) error {
	return txn.Remove(ruleGroupIDPath(groupID))
}

// LoadRegionRules loads region rules from storage.
func (se *StorageEndpoint) LoadRegionRules(f func(k, v string)) error {
	return se.loadRangeByPrefix(regionLabelPath+"/", f)
}

// SaveRegionRule saves a region rule to the storage.
func (se *StorageEndpoint) SaveRegionRule(ruleKey string, rule interface{}) error {
	return se.saveJSON(regionLabelKeyPath(ruleKey), rule)
}

// DeleteRegionRule removes a region rule from storage.
func (se *StorageEndpoint) DeleteRegionRule(ruleKey string) error {
	return se.Remove(regionLabelKeyPath(ruleKey))
}

// LoadRule load a placement rule from storage.
func (se *StorageEndpoint) LoadRule(ruleKey string) (string, error) {
	return se.Load(ruleKeyPath(ruleKey))
}

// LoadRules loads placement rules from storage.
func (se *StorageEndpoint) LoadRules(txn kv.Txn, f func(k, v string)) error {
	return loadRangeByPrefixInTxn(txn, rulesPath+"/", f)
}
