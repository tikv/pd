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

// AffinityStorage defines the storage operations on the affinity group.
type AffinityStorage interface {
	// LoadAffinityGroup loads a single affinity group as a raw JSON string.
	LoadAffinityGroup(groupID string) (string, error)
	// LoadAllAffinityGroups loads all affinity groups, passing raw key/value strings to the callback.
	LoadAllAffinityGroups(f func(k, v string)) error

	// SaveAffinityGroup saves an affinity group within a transaction.
	SaveAffinityGroup(txn kv.Txn, groupID string, group any) error
	// DeleteAffinityGroup deletes an affinity group within a transaction.
	DeleteAffinityGroup(txn kv.Txn, groupID string) error

	// RunInTxn provides a way to run operations in a transaction.
	RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error
}

var _ AffinityStorage = (*StorageEndpoint)(nil)

// SaveAffinityGroup stores an affinity group configuration to storage.
func (*StorageEndpoint) SaveAffinityGroup(txn kv.Txn, groupID string, group any) error {
	return saveJSONInTxn(txn, keypath.AffinityGroupPath(groupID), group)
}

// DeleteAffinityGroup removes an affinity group configuration from storage.
func (*StorageEndpoint) DeleteAffinityGroup(txn kv.Txn, groupID string) error {
	return txn.Remove(keypath.AffinityGroupPath(groupID))
}

// LoadAffinityGroup loads a single affinity group configuration as a raw string.
// The caller is responsible for JSON unmarshaling.
func (se *StorageEndpoint) LoadAffinityGroup(groupID string) (string, error) {
	return se.Load(keypath.AffinityGroupPath(groupID))
}

// LoadAllAffinityGroups loads all affinity group configurations from storage.
// It uses loadRangeByPrefix and passes raw key/value strings to the callback function 'f'.
func (se *StorageEndpoint) LoadAllAffinityGroups(f func(k, v string)) error {
	return se.loadRangeByPrefix(keypath.AffinityGroupsPrefix(), f)
}
