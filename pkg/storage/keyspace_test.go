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

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

func TestSaveLoadKeyspace(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	// Store test keyspace id and meta.
	keyspaces := makeTestKeyspaces()
	err := storage.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		for _, keyspace := range keyspaces {
			re.NoError(storage.SaveKeyspaceID(txn, keyspace.Id, keyspace.Name))
			re.NoError(storage.SaveKeyspaceMeta(txn, keyspace))
		}
		return nil
	})
	re.NoError(err)
	// Test load keyspace id and meta
	err = storage.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		for _, expectedMeta := range keyspaces {
			loadSuccess, id, err := storage.LoadKeyspaceID(txn, expectedMeta.Name)
			re.NoError(err)
			re.True(loadSuccess)
			re.Equal(expectedMeta.Id, id)
			// Test load keyspace.
			loadedMeta, err := storage.LoadKeyspaceMeta(txn, expectedMeta.Id)
			re.NoError(err)
			re.Equal(expectedMeta, loadedMeta)
		}
		return nil
	})
	re.NoError(err)

	err = storage.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		// Loading a non-existing keyspace id should be unsuccessful but no error.
		loadSuccess, id, err := storage.LoadKeyspaceID(txn, "non-existing keyspace")
		re.NoError(err)
		re.False(loadSuccess)
		re.Zero(id)
		// Loading a non-existing keyspace meta should be unsuccessful but no error.
		meta, err := storage.LoadKeyspaceMeta(txn, 999)
		re.NoError(err)
		re.Nil(meta)
		return nil
	})
	re.NoError(err)
}

func TestLoadRangeKeyspaces(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()
	keyspaces := makeTestKeyspaces()
	// Store test keyspace meta.
	err := storage.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		for _, keyspace := range keyspaces {
			re.NoError(storage.SaveKeyspaceMeta(txn, keyspace))
		}
		return nil
	})
	re.NoError(err)
	// Test load range keyspaces.
	err = storage.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		// Load all keyspaces.
		loadedKeyspaces, err := storage.LoadRangeKeyspace(txn, keyspaces[0].GetId(), 0)
		re.NoError(err)
		re.ElementsMatch(keyspaces, loadedKeyspaces)

		// Load keyspaces with id >= second test keyspace's id.
		loadedKeyspaces2, err := storage.LoadRangeKeyspace(txn, keyspaces[1].GetId(), 0)
		re.NoError(err)
		re.ElementsMatch(keyspaces[1:], loadedKeyspaces2)

		// Load keyspace with the smallest id.
		loadedKeyspace3, err := storage.LoadRangeKeyspace(txn, 1, 1)
		re.NoError(err)
		re.ElementsMatch(keyspaces[:1], loadedKeyspace3)

		return nil
	})
	re.NoError(err)
}

func makeTestKeyspaces() []*keyspacepb.KeyspaceMeta {
	now := time.Now().Unix()
	return []*keyspacepb.KeyspaceMeta{
		{
			Id:             10,
			Name:           "keyspace1",
			State:          keyspacepb.KeyspaceState_ENABLED,
			CreatedAt:      now,
			StateChangedAt: now,
			Config: map[string]string{
				"gc_life_time": "6000",
				"gc_interval":  "3000",
			},
		},
		{
			Id:             11,
			Name:           "keyspace2",
			State:          keyspacepb.KeyspaceState_ARCHIVED,
			CreatedAt:      now + 300,
			StateChangedAt: now + 300,
			Config: map[string]string{
				"gc_life_time": "1000",
				"gc_interval":  "5000",
			},
		},
		{
			Id:             100,
			Name:           "keyspace3",
			State:          keyspacepb.KeyspaceState_DISABLED,
			CreatedAt:      now + 500,
			StateChangedAt: now + 500,
			Config: map[string]string{
				"gc_life_time": "4000",
				"gc_interval":  "2000",
			},
		},
	}
}

// TestEncodeSpaceID test spaceID encoding.
func TestEncodeSpaceID(t *testing.T) {
	re := require.New(t)
	re.Equal("keyspaces/meta/00000000", keypath.KeyspaceMetaPath(0))
	re.Equal("keyspaces/meta/16777215", keypath.KeyspaceMetaPath(1<<24-1))
	re.Equal("keyspaces/meta/00000100", keypath.KeyspaceMetaPath(100))
	re.Equal("keyspaces/meta/00000011", keypath.KeyspaceMetaPath(11))
	re.Equal("keyspaces/meta/00000010", keypath.KeyspaceMetaPath(10))
}
