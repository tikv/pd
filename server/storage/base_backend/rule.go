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

package backend

import (
	"strings"

	storage "github.com/tikv/pd/server/storage/base_storage"
	"go.etcd.io/etcd/clientv3"
)

var _ storage.RuleStorage = (*BaseBackend)(nil)

// SaveRule stores a rule cfg to the rulesPath.
func (bb *BaseBackend) SaveRule(ruleKey string, rule interface{}) error {
	return bb.saveJSON(rulesPath, ruleKey, rule)
}

// DeleteRule removes a rule from storage.
func (bb *BaseBackend) DeleteRule(ruleKey string) error {
	return bb.Remove(ruleKeyPath(ruleKey))
}

// LoadRuleGroups loads all rule groups from storage.
func (bb *BaseBackend) LoadRuleGroups(f func(k, v string)) error {
	return bb.loadRangeByPrefix(ruleGroupPath+"/", f)
}

// SaveRuleGroup stores a rule group config to storage.
func (bb *BaseBackend) SaveRuleGroup(groupID string, group interface{}) error {
	return bb.saveJSON(ruleGroupPath, groupID, group)
}

// DeleteRuleGroup removes a rule group from storage.
func (bb *BaseBackend) DeleteRuleGroup(groupID string) error {
	return bb.Remove(ruleGroupIDPath(groupID))
}

// LoadRegionRules loads region rules from storage.
func (bb *BaseBackend) LoadRegionRules(f func(k, v string)) error {
	return bb.loadRangeByPrefix(regionLabelPath+"/", f)
}

// SaveRegionRule saves a region rule to the storage.
func (bb *BaseBackend) SaveRegionRule(ruleKey string, rule interface{}) error {
	return bb.saveJSON(regionLabelPath, ruleKey, rule)
}

// DeleteRegionRule removes a region rule from storage.
func (bb *BaseBackend) DeleteRegionRule(ruleKey string) error {
	return bb.Remove(regionLabelKeyPath(ruleKey))
}

// LoadRules loads placement rules from storage.
func (bb *BaseBackend) LoadRules(f func(k, v string)) error {
	return bb.loadRangeByPrefix(rulesPath+"/", f)
}

// loadRangeByPrefix iterates all key-value pairs in the storage that has the prefix.
func (bb *BaseBackend) loadRangeByPrefix(prefix string, f func(k, v string)) error {
	nextKey := prefix
	endKey := clientv3.GetPrefixRangeEnd(prefix)
	for {
		keys, values, err := bb.LoadRange(nextKey, endKey, minKVRangeLimit)
		if err != nil {
			return err
		}
		for i := range keys {
			f(strings.TrimPrefix(keys[i], prefix), values[i])
		}
		if len(keys) < minKVRangeLimit {
			return nil
		}
		nextKey = keys[len(keys)-1] + "\x00"
	}
}
