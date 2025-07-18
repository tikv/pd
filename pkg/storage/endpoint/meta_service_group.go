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
	"encoding/json"

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// MetaServiceGroupStorage defines storage operations on meta-service group related data.
type MetaServiceGroupStorage interface {
	SaveMetaServiceGroupStatus(txn kv.Txn, id string, status *MetaServiceGroupStatus) error
	LoadMetaServiceGroupStatus(txn kv.Txn, ids map[string]string) (map[string]*MetaServiceGroupStatus, error)
	RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error
}

// MetaServiceGroupStatus represents the status of a meta-service group.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MetaServiceGroupStatus struct {
	AssignmentCount int  `json:"assignment_count"`
	Enabled         bool `json:"enabled"`
}

// SaveMetaServiceGroupStatus saves the meta service group status to the storage.
func (*StorageEndpoint) SaveMetaServiceGroupStatus(txn kv.Txn, id string, status *MetaServiceGroupStatus) error {
	statusPath := keypath.MetaServiceGroupStatusPath(id)
	statusVal, err := json.Marshal(status)
	if err != nil {
		return err
	}
	return txn.Save(statusPath, string(statusVal))
}

// LoadMetaServiceGroupStatus returns the status of the designated meta-service group.
func (*StorageEndpoint) LoadMetaServiceGroupStatus(txn kv.Txn, ids map[string]string) (map[string]*MetaServiceGroupStatus, error) {
	statusMap := make(map[string]*MetaServiceGroupStatus)
	for id := range ids {
		status, err := loadMetaServiceGroupStatus(txn, id)
		if err != nil {
			return nil, err
		}
		statusMap[id] = status
	}
	return statusMap, nil
}

func loadMetaServiceGroupStatus(txn kv.Txn, id string) (*MetaServiceGroupStatus, error) {
	statusPath := keypath.MetaServiceGroupStatusPath(id)
	statusVal, err := txn.Load(statusPath)
	if err != nil {
		return nil, err
	}
	status := &MetaServiceGroupStatus{}
	if statusVal == "" {
		return status, nil
	}
	if err = json.Unmarshal([]byte(statusVal), status); err != nil {
		return nil, err
	}
	return status, nil
}
