// Copyright 2023 TiKV Project Authors.
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
	"go.etcd.io/etcd/clientv3"
)

// UserKind represents the user kind.
type UserKind int

// Different user kinds.
const (
	Basic UserKind = iota
	Standard
	Enterprise

	UserKindCount
)

// StringUserKind creates a UserKind with string.
func StringUserKind(input string) UserKind {
	switch input {
	case Basic.String():
		return Basic
	case Standard.String():
		return Standard
	case Enterprise.String():
		return Enterprise
	default:
		return Basic
	}
}

func (k UserKind) String() string {
	switch k {
	case Basic:
		return "basic"
	case Standard:
		return "standard"
	case Enterprise:
		return "enterprise"
	}
	return "unknown UserKind"
}

// IsUserKindValid checks if the user kind is valid.
func IsUserKindValid(kind string) bool {
	switch kind {
	case Basic.String(), Standard.String(), Enterprise.String():
		return true
	default:
		return false
	}
}

// KeyspaceGroupMember defines an election member which campaigns for the primary of the keyspace group.
type KeyspaceGroupMember struct {
	Address string `json:"address"`
}

// SplitState defines the split state of a keyspace group.
type SplitState struct {
	// SplitSource is the current keyspace group ID from which the keyspace group is split.
	// When the keyspace group is being split to another keyspace group, the split-source will
	// be set to its own ID.
	SplitSource uint32 `json:"split-source"`
}

// KeyspaceGroup is the keyspace group.
type KeyspaceGroup struct {
	ID       uint32 `json:"id"`
	UserKind string `json:"user-kind"`
	// SplitState is the current split state of the keyspace group.
	SplitState *SplitState `json:"split-state,omitempty"`
	// Members are the election members which campaign for the primary of the keyspace group.
	Members []KeyspaceGroupMember `json:"members"`
	// Keyspaces are the keyspace IDs which belong to the keyspace group.
	Keyspaces []uint32 `json:"keyspaces"`
	// KeyspaceLookupTable is for fast lookup if a given keyspace belongs to this keyspace group.
	// It's not persisted and will be built when loading from storage.
	KeyspaceLookupTable map[uint32]struct{} `json:"-"`
}

// IsSplitting checks if the keyspace group is in split state.
func (kg *KeyspaceGroup) IsSplitting() bool {
	return kg != nil && kg.SplitState != nil
}

// IsSplitTarget checks if the keyspace group is in split state and is the split target.
func (kg *KeyspaceGroup) IsSplitTarget() bool {
	return kg.IsSplitting() && kg.SplitState.SplitSource != kg.ID
}

// IsSplitSource checks if the keyspace group is in split state and is the split source.
func (kg *KeyspaceGroup) IsSplitSource() bool {
	return kg.IsSplitting() && kg.SplitState.SplitSource == kg.ID
}

// SplitSource returns the keyspace group split source ID. When the keyspace group is the split source
// itself, it will return its own ID.
func (kg *KeyspaceGroup) SplitSource() uint32 {
	if kg.IsSplitting() {
		return kg.SplitState.SplitSource
	}
	return 0
}

// KeyspaceGroupStorage is the interface for keyspace group storage.
type KeyspaceGroupStorage interface {
	LoadKeyspaceGroups(startID uint32, limit int) ([]*KeyspaceGroup, error)
	LoadKeyspaceGroup(txn kv.Txn, id uint32) (*KeyspaceGroup, error)
	SaveKeyspaceGroup(txn kv.Txn, kg *KeyspaceGroup) error
	DeleteKeyspaceGroup(txn kv.Txn, id uint32) error
	// TODO: add more interfaces.
	RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error
}

var _ KeyspaceGroupStorage = (*StorageEndpoint)(nil)

// LoadKeyspaceGroup loads the keyspace group by ID.
func (se *StorageEndpoint) LoadKeyspaceGroup(txn kv.Txn, id uint32) (*KeyspaceGroup, error) {
	value, err := txn.Load(KeyspaceGroupIDPath(id))
	if err != nil || value == "" {
		return nil, err
	}
	kg := &KeyspaceGroup{}
	if err := json.Unmarshal([]byte(value), kg); err != nil {
		return nil, err
	}
	return kg, nil
}

// SaveKeyspaceGroup saves the keyspace group.
func (se *StorageEndpoint) SaveKeyspaceGroup(txn kv.Txn, kg *KeyspaceGroup) error {
	key := KeyspaceGroupIDPath(kg.ID)
	value, err := json.Marshal(kg)
	if err != nil {
		return err
	}
	return txn.Save(key, string(value))
}

// DeleteKeyspaceGroup deletes the keyspace group.
func (se *StorageEndpoint) DeleteKeyspaceGroup(txn kv.Txn, id uint32) error {
	return txn.Remove(KeyspaceGroupIDPath(id))
}

// LoadKeyspaceGroups loads keyspace groups from the start ID with limit.
// If limit is 0, it will load all keyspace groups from the start ID.
func (se *StorageEndpoint) LoadKeyspaceGroups(startID uint32, limit int) ([]*KeyspaceGroup, error) {
	prefix := KeyspaceGroupIDPath(startID)
	prefixEnd := clientv3.GetPrefixRangeEnd(KeyspaceGroupIDPrefix())
	keys, values, err := se.LoadRange(prefix, prefixEnd, limit)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*KeyspaceGroup{}, nil
	}
	kgs := make([]*KeyspaceGroup, 0, len(keys))
	for _, value := range values {
		kg := &KeyspaceGroup{}
		if err = json.Unmarshal([]byte(value), kg); err != nil {
			return nil, err
		}
		kgs = append(kgs, kg)
	}
	return kgs, nil
}
