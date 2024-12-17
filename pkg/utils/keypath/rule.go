// Copyright 2024 TiKV Project Authors.
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

package keypath

import (
	"fmt"
)

// RulesPathPrefix returns the path prefix to save the placement rules.
func RulesPathPrefix() string {
	return Prefix(RuleKeyPath(""))
}

// RuleKeyPath returns the path to save the placement rule with the given rule key.
func RuleKeyPath(ruleKey string) string {
	return fmt.Sprintf(rulePathFormat, ClusterID(), ruleKey)
}

// RuleCommonPathPrefix returns the path prefix to save the placement rule common config.
func RuleCommonPathPrefix() string {
	return fmt.Sprintf(ruleConfigPrefixFormat, ClusterID())
}

// RuleGroupIDPath returns the path to save the placement rule group with the given group ID.
func RuleGroupIDPath(groupID string) string {
	return fmt.Sprintf(ruleGroupPathFormat, ClusterID(), groupID)
}

// RegionLabelKeyPath returns the path to save the region label with the given rule key.
func RegionLabelKeyPath(ruleKey string) string {
	return fmt.Sprintf(regionLablePathFormat, ClusterID(), ruleKey)
}

// RuleGroupPathPrefix returns the path prefix to save the placement rule groups.
func RuleGroupPathPrefix() string {
	return Prefix(RuleGroupIDPath(""))
}

// RegionLabelPathPrefix returns the path prefix to save the region label.
func RegionLabelPathPrefix() string {
	return Prefix(RegionLabelKeyPath(""))
}
