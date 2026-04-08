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

package endpoint

import (
	"encoding/json"

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// KeyspaceRotationMeta stores the rotation state for a keyspace.
type KeyspaceRotationMeta struct {
	KeyspaceID    uint32 `json:"keyspace_id"`
	CurrentFileID uint64 `json:"current_file_id"`
	NextFileID    uint64 `json:"next_file_id"`
	StartedAt     int64  `json:"started_at"`
}

// KeyspaceRotationStorage defines storage operations on keyspace rotation metadata.
type KeyspaceRotationStorage interface {
	SaveKeyspaceRotation(txn kv.Txn, meta *KeyspaceRotationMeta) error
	LoadKeyspaceRotation(txn kv.Txn, keyspaceID uint32) (*KeyspaceRotationMeta, error)
}

var _ KeyspaceRotationStorage = (*StorageEndpoint)(nil)

// SaveKeyspaceRotation saves the keyspace rotation metadata.
func (*StorageEndpoint) SaveKeyspaceRotation(txn kv.Txn, meta *KeyspaceRotationMeta) error {
	rotationPath := keypath.KeyspaceRotationPath(meta.KeyspaceID)
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return txn.Save(rotationPath, string(data))
}

// LoadKeyspaceRotation loads the keyspace rotation metadata.
// Returns nil if the rotation metadata does not exist.
func (*StorageEndpoint) LoadKeyspaceRotation(txn kv.Txn, keyspaceID uint32) (*KeyspaceRotationMeta, error) {
	rotationPath := keypath.KeyspaceRotationPath(keyspaceID)
	data, err := txn.Load(rotationPath)
	if err != nil || data == "" {
		return nil, err
	}
	meta := &KeyspaceRotationMeta{}
	if err := json.Unmarshal([]byte(data), meta); err != nil {
		return nil, err
	}
	return meta, nil
}
