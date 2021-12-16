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

package base

import (
	"encoding/json"
	"path"
	"strings"

	"github.com/tikv/pd/pkg/errs"
	"go.etcd.io/etcd/clientv3"
)

const (
	rulesPath       = "rules"
	ruleGroupPath   = "rule_group"
	regionLabelPath = "region_label"
)

// RuleStorage defines the storage operations on the rule.
type RuleStorage interface {
	LoadRules(f func(k, v string)) error
	SaveRule(ruleKey string, rule interface{}) error
	DeleteRule(ruleKey string) error
	LoadRuleGroups(f func(k, v string)) error
	SaveRuleGroup(groupID string, group interface{}) error
	DeleteRuleGroup(groupID string) error
	LoadRegionRules(f func(k, v string)) error
	SaveRegionRule(ruleKey string, rule interface{}) error
	DeleteRegionRule(ruleKey string) error
}

// SaveRule stores a rule cfg to the rulesPath.
func (s *Storage) SaveRule(ruleKey string, rule interface{}) error {
	return s.saveJSON(rulesPath, ruleKey, rule)
}

// DeleteRule removes a rule from storage.
func (s *Storage) DeleteRule(ruleKey string) error {
	return s.Remove(path.Join(rulesPath, ruleKey))
}

func (s *Storage) saveJSON(prefix, key string, data interface{}) error {
	value, err := json.Marshal(data)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return s.Save(path.Join(prefix, key), string(value))
}

// LoadRuleGroups loads all rule groups from storage.
func (s *Storage) LoadRuleGroups(f func(k, v string)) error {
	return s.loadRangeByPrefix(ruleGroupPath+"/", f)
}

// SaveRuleGroup stores a rule group config to storage.
func (s *Storage) SaveRuleGroup(groupID string, group interface{}) error {
	return s.saveJSON(ruleGroupPath, groupID, group)
}

// DeleteRuleGroup removes a rule group from storage.
func (s *Storage) DeleteRuleGroup(groupID string) error {
	return s.Remove(path.Join(ruleGroupPath, groupID))
}

// LoadRegionRules loads region rules from storage.
func (s *Storage) LoadRegionRules(f func(k, v string)) error {
	return s.loadRangeByPrefix(regionLabelPath+"/", f)
}

// SaveRegionRule saves a region rule to the storage.
func (s *Storage) SaveRegionRule(ruleKey string, rule interface{}) error {
	return s.saveJSON(regionLabelPath, ruleKey, rule)
}

// DeleteRegionRule removes a region rule from storage.
func (s *Storage) DeleteRegionRule(ruleKey string) error {
	return s.Remove(path.Join(regionLabelPath, ruleKey))
}

// LoadRules loads placement rules from storage.
func (s *Storage) LoadRules(f func(k, v string)) error {
	return s.loadRangeByPrefix(rulesPath+"/", f)
}

// loadRangeByPrefix iterates all key-value pairs in the storage that has the prefix.
func (s *Storage) loadRangeByPrefix(prefix string, f func(k, v string)) error {
	nextKey := prefix
	endKey := clientv3.GetPrefixRangeEnd(prefix)
	for {
		keys, values, err := s.LoadRange(nextKey, endKey, minKVRangeLimit)
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
