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

package storage

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
