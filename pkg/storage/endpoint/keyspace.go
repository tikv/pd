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

package endpoint

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"go.etcd.io/etcd/clientv3"
)

const (
	// SpaceIDBase is base used to encode/decode spaceID.
	// It's set to 10 for better readability.
	SpaceIDBase = 10
	// spaceIDBitSizeMax is the max bitSize of spaceID.
	// It's currently set to 24 (3bytes).
	spaceIDBitSizeMax = 24
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

// KeyspaceGroupMember defines an election member which campaigns for the primary of the keyspace group.
type KeyspaceGroupMember struct {
	Address string `json:"address"`
}

// KeyspaceGroup is the keyspace group.
type KeyspaceGroup struct {
	ID       uint32 `json:"id"`
	UserKind string `json:"user-kind"`
	// Members are the election members which campaign for the primary of the keyspace group.
	Members []KeyspaceGroupMember `json:"members"`
	// Keyspaces are the keyspace IDs which belong to the keyspace group.
	Keyspaces []uint32 `json:"keyspaces"`
}

// KeyspaceStorage defines storage operations on keyspace related data.
type KeyspaceStorage interface {
	SaveKeyspaceMeta(txn kv.Txn, meta *keyspacepb.KeyspaceMeta) error
	LoadKeyspaceMeta(txn kv.Txn, id uint32) (*keyspacepb.KeyspaceMeta, error)
	SaveKeyspaceID(txn kv.Txn, id uint32, name string) error
	LoadKeyspaceID(txn kv.Txn, name string) (bool, uint32, error)
	// LoadRangeKeyspace loads no more than limit keyspaces starting at startID.
	LoadRangeKeyspace(startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error)
	LoadKeyspaceGroups(startID uint32, limit int) ([]*KeyspaceGroup, error)
	LoadKeyspaceGroup(txn kv.Txn, id uint32) (*KeyspaceGroup, error)
	SaveKeyspaceGroup(txn kv.Txn, kg *KeyspaceGroup) error
	DeleteKeyspaceGroup(txn kv.Txn, id uint32) error
	RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error
}

var _ KeyspaceStorage = (*StorageEndpoint)(nil)

// SaveKeyspaceMeta adds a save keyspace meta operation to target transaction.
func (se *StorageEndpoint) SaveKeyspaceMeta(txn kv.Txn, meta *keyspacepb.KeyspaceMeta) error {
	metaPath := KeyspaceMetaPath(meta.GetId())
	metaVal, err := proto.Marshal(meta)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
	}
	return txn.Save(metaPath, string(metaVal))
}

// LoadKeyspaceMeta load and return keyspace meta specified by id.
// If keyspace does not exist or error occurs, returned meta will be nil.
func (se *StorageEndpoint) LoadKeyspaceMeta(txn kv.Txn, id uint32) (*keyspacepb.KeyspaceMeta, error) {
	metaPath := KeyspaceMetaPath(id)
	metaVal, err := txn.Load(metaPath)
	if err != nil || metaVal == "" {
		return nil, err
	}
	meta := &keyspacepb.KeyspaceMeta{}
	err = proto.Unmarshal([]byte(metaVal), meta)
	if err != nil {
		return nil, errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return meta, nil
}

// SaveKeyspaceID saves keyspace ID to the path specified by keyspace name.
func (se *StorageEndpoint) SaveKeyspaceID(txn kv.Txn, id uint32, name string) error {
	idPath := KeyspaceIDPath(name)
	idVal := strconv.FormatUint(uint64(id), SpaceIDBase)
	return txn.Save(idPath, idVal)
}

// LoadKeyspaceID loads keyspace ID from the path specified by keyspace name.
// An additional boolean is returned to indicate whether target id exists,
// it returns false if target id not found, or if error occurred.
func (se *StorageEndpoint) LoadKeyspaceID(txn kv.Txn, name string) (bool, uint32, error) {
	idPath := KeyspaceIDPath(name)
	idVal, err := txn.Load(idPath)
	// Failed to load the keyspaceID if loading operation errored, or if keyspace does not exist.
	if err != nil || idVal == "" {
		return false, 0, err
	}
	id64, err := strconv.ParseUint(idVal, SpaceIDBase, spaceIDBitSizeMax)
	if err != nil {
		return false, 0, err
	}
	return true, uint32(id64), nil
}

// RunInTxn runs the given function in a transaction.
func (se *StorageEndpoint) RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error {
	return se.Base.RunInTxn(ctx, f)
}

// LoadRangeKeyspace loads keyspaces starting at startID.
// limit specifies the limit of loaded keyspaces.
func (se *StorageEndpoint) LoadRangeKeyspace(startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error) {
	startKey := KeyspaceMetaPath(startID)
	endKey := clientv3.GetPrefixRangeEnd(KeyspaceMetaPrefix())
	keys, values, err := se.LoadRange(startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*keyspacepb.KeyspaceMeta{}, nil
	}
	keyspaces := make([]*keyspacepb.KeyspaceMeta, 0, len(keys))
	for _, value := range values {
		keyspace := &keyspacepb.KeyspaceMeta{}
		if err = proto.Unmarshal([]byte(value), keyspace); err != nil {
			return nil, err
		}
		keyspaces = append(keyspaces, keyspace)
	}
	return keyspaces, nil
}

// LoadKeyspaceGroup loads the keyspace group by id.
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
