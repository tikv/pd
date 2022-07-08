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

package keyspace

import (
	"fmt"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/storage/kv"
)

func newKeyspaceManager() *Manager {
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	allocator := mockid.NewIDAllocator()
	return NewKeyspaceManager(store, allocator)
}

func createKeyspaceRequests(count int) []CreateKeyspaceRequest {
	now := time.Now()
	requests := make([]CreateKeyspaceRequest, count)
	for i := 0; i < count; i++ {
		requests[i] = CreateKeyspaceRequest{
			Name: fmt.Sprintf("test_keyspace%d", i),
			InitialConfig: map[string]string{
				"config_entry_1": "100",
				"config_entry_2": "200",
			},
			Now: now,
		}
	}
	return requests
}
func TestCreateKeyspace(t *testing.T) {
	re := require.New(t)
	manager := newKeyspaceManager()
	requests := createKeyspaceRequests(10)

	for i, request := range requests {
		expected := &keyspacepb.KeyspaceMeta{
			Id:             uint32(i + 1),
			Name:           request.Name,
			State:          keyspacepb.KeyspaceState_ENABLED,
			CreatedAt:      request.Now.Unix(),
			StateChangedAt: request.Now.Unix(),
			Config:         request.InitialConfig,
		}

		created, err := manager.CreateKeyspace(request)
		re.NoError(err)
		re.Equal(expected, created)

		loaded, err := manager.LoadKeyspace(request.Name)
		re.NoError(err)
		re.Equal(expected, loaded)
	}

	// create a keyspace with existing name must return error
	_, err := manager.CreateKeyspace(requests[0])
	re.Error(err)

	// create a keyspace with empty name must return error
	_, err = manager.CreateKeyspace(CreateKeyspaceRequest{Name: ""})
	re.Error(err)
}

func TestUpdateKeyspace(t *testing.T) {
	re := require.New(t)
	manager := newKeyspaceManager()
	requests := createKeyspaceRequests(10)
	for i, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
		updateRequest := UpdateKeyspaceRequest{
			Name:        createRequest.Name,
			UpdateState: true,
			NewState:    keyspacepb.KeyspaceState(rand.Int31n(3)),
			Now:         time.Now(),
			ToPut: map[string]string{
				"config_entry_1":    strconv.Itoa(i),
				"config_entry_2":    strconv.Itoa(i),
				"additional_config": strconv.Itoa(i),
			},
			ToDelete: []string{"config_entry_2"},
		}

		updated, err := manager.UpdateKeyspace(updateRequest)
		re.NoError(err)

		re.Equal(updateRequest.NewState, updated.State)
		if updateRequest.NewState != keyspacepb.KeyspaceState_ENABLED {
			// if state changed, then new state change time must be recorded
			re.Equal(updateRequest.Now.Unix(), updated.StateChangedAt)
		} else {
			// otherwise state change time should not change
			re.Equal(createRequest.Now.Unix(), updated.StateChangedAt)
		}
		// check for puts
		re.Equal(strconv.Itoa(i), updated.Config["config_entry_1"])
		re.Equal(strconv.Itoa(i), updated.Config["additional_config"])
		// deleted key must be deleted after put
		_, ok := updated.Config["config_entry_2"]
		re.False(ok)
	}
}

func TestLoadRangeKeyspace(t *testing.T) {
	re := require.New(t)
	manager := newKeyspaceManager()
	requests := createKeyspaceRequests(10)

	// force keyspace id start with 100
	for i := 0; i < 100; i++ {
		_, _ = manager.idAllocator.Alloc()
	}
	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
	}

	// load all 10 of them
	keyspaces, err := manager.LoadRangeKeyspace(100, 10)
	re.NoError(err)
	re.Equal(10, len(keyspaces))
}
