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
)

func TestSaveLoadKeyspace(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()

	keyspaces := testKeyspaces()
	for _, keyspace := range keyspaces {
		re.NoError(storage.SaveKeyspace(keyspace))
	}

	for _, keyspace := range keyspaces {
		spaceID := keyspace.GetId()
		loadedKeyspace := &keyspacepb.KeyspaceMeta{}
		success, err := storage.LoadKeyspace(spaceID, loadedKeyspace)
		re.True(success)
		re.NoError(err)
		re.Equal(keyspace, loadedKeyspace)
	}
}

func TestLoadRangeKeyspaces(t *testing.T) {
	re := require.New(t)
	storage := NewStorageWithMemoryBackend()

	keyspaces := testKeyspaces()
	for _, keyspace := range keyspaces {
		re.NoError(storage.SaveKeyspace(keyspace))
	}

	// load all keyspaces.
	loadedKeyspaces, err := storage.LoadRangeKeyspace(keyspaces[0].GetId(), 0)
	re.NoError(err)
	re.ElementsMatch(keyspaces, loadedKeyspaces)

	// load keyspaces that with id no less than second test keyspace.
	loadedKeyspaces2, err := storage.LoadRangeKeyspace(keyspaces[1].GetId(), 0)
	re.NoError(err)
	re.ElementsMatch(keyspaces[1:], loadedKeyspaces2)

	// load keyspace with the smallest id.
	loadedKeyspace3, err := storage.LoadRangeKeyspace(1, 1)
	re.NoError(err)
	re.ElementsMatch(keyspaces[:1], loadedKeyspace3)
}

func testKeyspaces() []*keyspacepb.KeyspaceMeta {
	now := time.Now().Unix()
	return []*keyspacepb.KeyspaceMeta{
		{
			Id:             500,
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
			Id:             700,
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
			Id:             800,
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
