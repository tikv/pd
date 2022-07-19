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
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server/storage/endpoint"
)

func TestSaveLoadKeyspace(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()

	keyspaces := makeTestKeyspaces()
	for _, keyspace := range keyspaces {
		re.NoError(storage.SaveKeyspace(keyspace))
	}

	for _, keyspace := range keyspaces {
		spaceID := keyspace.GetId()
		loadedKeyspace := &keyspacepb.KeyspaceMeta{}
		// Test load keyspace.
		success, err := storage.LoadKeyspace(spaceID, loadedKeyspace)
		re.True(success)
		re.NoError(err)
		re.Equal(keyspace, loadedKeyspace)
		// Test remove keyspace.
		re.NoError(storage.RemoveKeyspace(spaceID))
		success, err = storage.LoadKeyspace(spaceID, loadedKeyspace)
		// Loading a non-existing keyspace should be unsuccessful.
		re.False(success)
		// Loading a non-existing keyspace should not return error.
		re.NoError(err)
	}
}

func TestLoadRangeKeyspaces(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()

	keyspaces := makeTestKeyspaces()
	for _, keyspace := range keyspaces {
		re.NoError(storage.SaveKeyspace(keyspace))
	}

	// Load all keyspaces.
	loadedKeyspaces, err := storage.LoadRangeKeyspace(keyspaces[0].GetId(), 0)
	re.NoError(err)
	re.ElementsMatch(keyspaces, loadedKeyspaces)

	// Load keyspaces with id >= second test keyspace's id.
	loadedKeyspaces2, err := storage.LoadRangeKeyspace(keyspaces[1].GetId(), 0)
	re.NoError(err)
	re.ElementsMatch(keyspaces[1:], loadedKeyspaces2)

	// Load keyspace with the smallest id.
	loadedKeyspace3, err := storage.LoadRangeKeyspace(1, 1)
	re.NoError(err)
	re.ElementsMatch(keyspaces[:1], loadedKeyspace3)
}

func TestSaveLoadKeyspaceID(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()

	ids := []uint32{100, 200, 300}
	names := []string{"keyspace1", "keyspace2", "keyspace3"}
	for i := range ids {
		re.NoError(storage.SaveKeyspaceIDByName(ids[i], names[i]))
	}

	for i := range names {
		success, id, err := storage.LoadKeyspaceIDByName(names[i])
		re.NoError(err)
		re.True(success)
		re.Equal(ids[i], id)
	}
	// Loading non-existing id should return false, 0, nil.
	success, id, err := storage.LoadKeyspaceIDByName("non-existing")
	re.NoError(err)
	re.False(success)
	re.Equal(uint32(0), id)
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
	re.Equal("keyspaces/meta/00000000", endpoint.KeyspaceMetaPath(0))
	re.Equal("keyspaces/meta/16777215", endpoint.KeyspaceMetaPath(1<<24-1))
	re.Equal("keyspaces/meta/00000100", endpoint.KeyspaceMetaPath(100))
	re.Equal("keyspaces/meta/00000011", endpoint.KeyspaceMetaPath(11))
	re.Equal("keyspaces/meta/00000010", endpoint.KeyspaceMetaPath(10))
}
