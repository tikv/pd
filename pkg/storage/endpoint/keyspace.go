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
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// SpaceIDBase is base used to encode/decode spaceID.
	// It's set to 10 for better readability.
	SpaceIDBase = 10
	// spaceIDBitSizeMax is the max bitSize of spaceID.
	// It's currently set to 24 (3bytes).
	spaceIDBitSizeMax = 24
)

// KeyspaceStorage defines storage operations on keyspace related data.
type KeyspaceStorage interface {
	SaveKeyspaceMeta(txn kv.Txn, meta *keyspacepb.KeyspaceMeta) error
	LoadKeyspaceMeta(txn kv.Txn, id uint32) (*keyspacepb.KeyspaceMeta, error)
	SaveKeyspaceID(txn kv.Txn, id uint32, name string) error
	LoadKeyspaceID(txn kv.Txn, name string) (bool, uint32, error)
	// LoadRangeKeyspace loads no more than limit keyspaces starting at startID.
	LoadRangeKeyspace(txn kv.Txn, startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error)
	RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error
}

var _ KeyspaceStorage = (*StorageEndpoint)(nil)

// SaveKeyspaceMeta adds a save keyspace meta operation to target transaction.
func (*StorageEndpoint) SaveKeyspaceMeta(txn kv.Txn, meta *keyspacepb.KeyspaceMeta) error {
	metaPath := KeyspaceMetaPath(meta.GetId())
	metaVal, err := proto.Marshal(meta)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
	}
	return txn.Save(metaPath, string(metaVal))
}

// LoadKeyspaceMeta load and return keyspace meta specified by id.
// If keyspace does not exist or error occurs, returned meta will be nil.
func (*StorageEndpoint) LoadKeyspaceMeta(txn kv.Txn, id uint32) (*keyspacepb.KeyspaceMeta, error) {
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
func (*StorageEndpoint) SaveKeyspaceID(txn kv.Txn, id uint32, name string) error {
	idPath := KeyspaceIDPath(name)
	idVal := strconv.FormatUint(uint64(id), SpaceIDBase)
	return txn.Save(idPath, idVal)
}

// LoadKeyspaceID loads keyspace ID from the path specified by keyspace name.
// An additional boolean is returned to indicate whether target id exists,
// it returns false if target id not found, or if error occurred.
func (*StorageEndpoint) LoadKeyspaceID(txn kv.Txn, name string) (bool, uint32, error) {
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

// LoadRangeKeyspace loads keyspaces starting at startID.
// limit specifies the limit of loaded keyspaces.
func (*StorageEndpoint) LoadRangeKeyspace(txn kv.Txn, startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error) {
	startKey := KeyspaceMetaPath(startID)
	endKey := clientv3.GetPrefixRangeEnd(KeyspaceMetaPrefix())
	keys, values, err := txn.LoadRange(startKey, endKey, limit)
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
