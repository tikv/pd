// Copyright 2025 TiKV Project Authors.
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
	"github.com/tikv/pd/pkg/utils/keypath"
)

// RegionLabelRuleStorage defines the storage operations on the rule.
type RegionLabelRuleStorage interface {
	// Load in txn is unnecessary and may cause txn too large.
	// because scheduling server will load rules from etcd rather than watching.
	LoadRegionRules(f func(k, v string)) error

	// We need to use txn to avoid concurrent modification.
	// And it is helpful for the scheduling server to watch the rule.
	SaveRegionRule(txn kv.Txn, ruleKey string, rule any) error
	DeleteRegionRule(txn kv.Txn, ruleKey string) error

	RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error
}

var _ RegionLabelRuleStorage = (*StorageEndpoint)(nil)

// LoadRegionRules loads region rules from storage.
func (se *StorageEndpoint) LoadRegionRules(f func(k, v string)) error {
	return se.loadRangeByPrefix(keypath.RegionLabelPathPrefix(), f)
}

// SaveRegionRule saves a region rule to the storage.
func (*StorageEndpoint) SaveRegionRule(txn kv.Txn, ruleKey string, rule any) error {
	return saveJSONInTxn(txn, keypath.RegionLabelKeyPath(ruleKey), rule)
}

// DeleteRegionRule removes a region rule from storage.
func (*StorageEndpoint) DeleteRegionRule(txn kv.Txn, ruleKey string) error {
	return txn.Remove(keypath.RegionLabelKeyPath(ruleKey))
}
