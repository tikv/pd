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
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"go.etcd.io/etcd/clientv3"
)

type KeyspaceStorage interface {
	// SaveKeyspace saves the given keyspace to the storage.
	SaveKeyspace(*keyspacepb.KeyspaceMeta) error
	// LoadKeyspace loads keyspace specified by spaceID.
	LoadKeyspace(spaceID uint32, keyspace *keyspacepb.KeyspaceMeta) (bool, error)
	// LoadRangeKeyspace loads no more than limit keyspaces starting at startID.
	LoadRangeKeyspace(startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error)
}

var _ KeyspaceStorage = (*StorageEndpoint)(nil)

// SaveKeyspace saves the given keyspace to the storage.
func (se *StorageEndpoint) SaveKeyspace(keyspace *keyspacepb.KeyspaceMeta) error {
	key := KeyspaceMetaPath(keyspace.GetId())
	return se.saveProto(key, keyspace)
}

// LoadKeyspace loads keyspace specified by spaceID.
func (se *StorageEndpoint) LoadKeyspace(spaceID uint32, keyspace *keyspacepb.KeyspaceMeta) (bool, error) {
	key := KeyspaceMetaPath(spaceID)
	return se.loadProto(key, keyspace)
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
