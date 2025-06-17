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
	"strconv"

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// MetaServiceGroupStorage defines storage operations on meta-service group related data.
type MetaServiceGroupStorage interface {
	IncrementAssignmentCount(txn kv.Txn, id string, delta int) error
	GetAssignmentCount(txn kv.Txn, ids map[string]string) (map[string]int, error)
	RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error
}

// IncrementAssignmentCount increments the assignment count of the designated meta-service group by delta
func (*StorageEndpoint) IncrementAssignmentCount(txn kv.Txn, id string, delta int) error {
	count, err := loadAssignmentCount(txn, id)
	if err != nil {
		return err
	}
	count += delta
	return saveAssignmentCount(txn, id, count)
}

// GetAssignmentCount returns the assignment count of the designated meta-service group.
func (*StorageEndpoint) GetAssignmentCount(txn kv.Txn, ids map[string]string) (map[string]int, error) {
	counts := make(map[string]int)
	for id := range ids {
		count, err := loadAssignmentCount(txn, id)
		if err != nil {
			return nil, err
		}
		counts[id] = count
	}
	return counts, nil
}

func loadAssignmentCount(txn kv.Txn, id string) (int, error) {
	countPath := keypath.MetaServiceGroupAssignmentCountPath(id)
	countVal, err := txn.Load(countPath)
	if err != nil {
		return 0, err
	}
	if countVal == "" {
		return 0, nil
	}
	return strconv.Atoi(countVal)
}

func saveAssignmentCount(txn kv.Txn, id string, count int) error {
	countPath := keypath.MetaServiceGroupAssignmentCountPath(id)
	countVal := strconv.Itoa(count)
	return txn.Save(countPath, countVal)
}
