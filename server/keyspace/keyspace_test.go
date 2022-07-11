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
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/storage/kv"
)

const testConfig = "test config"

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
		created, err := manager.CreateKeyspace(request)
		re.NoError(err)
		re.Equal(uint32(i+1), created.Id)
		matchCreateRequest(re, request, created)

		loaded, err := manager.LoadKeyspace(request.Name)
		re.NoError(err)
		re.Equal(uint32(i+1), loaded.Id)
		matchCreateRequest(re, request, loaded)
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
	requests := createKeyspaceRequests(5)
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
		matchUpdateRequest(re, updateRequest, updated)
	}
}

func TestLoadRangeKeyspace(t *testing.T) {
	re := require.New(t)
	manager := newKeyspaceManager()
	requests := createKeyspaceRequests(10)

	startID := 100
	// force keyspace id start with startID
	for i := 0; i < startID-1; i++ {
		_, _ = manager.idAllocator.Alloc()
	}
	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
	}

	// load all of them
	keyspaces, err := manager.LoadRangeKeyspace(0, 0)
	re.NoError(err)
	re.Equal(10, len(keyspaces))
	for i := range keyspaces {
		re.Equal(uint32(startID+i), keyspaces[i].Id)
		matchCreateRequest(re, requests[i], keyspaces[i])
	}

	// load first 3
	keyspaces, err = manager.LoadRangeKeyspace(0, 3)
	re.NoError(err)
	re.Equal(3, len(keyspaces))
	for i := range keyspaces {
		re.Equal(uint32(startID+i), keyspaces[i].Id)
		matchCreateRequest(re, requests[i], keyspaces[i])
	}

	// load 3 starting with startID + 5
	loadStart := startID + 5
	keyspaces, err = manager.LoadRangeKeyspace(uint32(loadStart), 3)
	re.NoError(err)
	re.Equal(3, len(keyspaces))
	for i := range keyspaces {
		re.Equal(uint32(loadStart+i), keyspaces[i].Id)
		matchCreateRequest(re, requests[i+5], keyspaces[i])
	}
}

// TestUpdateMultipleKeyspace checks that updating multiple keyspace's config simultaneously
// will be successful.
func TestUpdateMultipleKeyspace(t *testing.T) {
	re := require.New(t)
	manager := newKeyspaceManager()
	requests := createKeyspaceRequests(5)
	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
	}

	// concurrently update all keyspaces' testConfig sequentially.
	end := 100
	wg := sync.WaitGroup{}
	for _, request := range requests {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			updateKeyspaceConfig(re, manager, name, end)
		}(request.Name)
	}
	wg.Wait()

	// check that eventually all test keyspaces' test config reaches end
	for _, request := range requests {
		keyspace, err := manager.LoadKeyspace(request.Name)
		re.NoError(err)
		re.Equal(keyspace.Config[testConfig], strconv.Itoa(end))
	}
}

// matchCreateRequest verifies a keyspace meta matches a create request.
func matchCreateRequest(re *require.Assertions, request CreateKeyspaceRequest, meta *keyspacepb.KeyspaceMeta) {
	re.Equal(request.Name, meta.Name)
	re.Equal(request.Now.Unix(), meta.CreatedAt)
	re.Equal(request.Now.Unix(), meta.StateChangedAt)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, meta.State)
	re.Equal(request.InitialConfig, meta.Config)
}

// matchUpdateRequest verifies a keyspace meta could be the immediate result of an update request.
func matchUpdateRequest(re *require.Assertions, request UpdateKeyspaceRequest, meta *keyspacepb.KeyspaceMeta) {
	re.Equal(request.Name, meta.Name)
	if request.UpdateState {
		re.Equal(request.NewState, meta.State)
		// keyspace's state change time at must be less (state changed) or equal (no change) than request's.
		re.GreaterOrEqual(request.Now.Unix(), meta.StateChangedAt)
	}
	// checkMap represent kvs to check in the meta.
	checkMap := make(map[string]string)
	for put, putVal := range request.ToPut {
		checkMap[put] = putVal
	}
	for _, toDelete := range request.ToDelete {
		delete(checkMap, toDelete)
		// must delete key from config
		_, ok := meta.Config[toDelete]
		re.False(ok)
	}
	// check that meta contains target kvs.
	for checkKey, checkValue := range checkMap {
		v, ok := meta.Config[checkKey]
		re.True(ok)
		re.Equal(checkValue, v)
	}
}

// updateKeyspaceConfig sequentially updates given keyspace's entry.
func updateKeyspaceConfig(re *require.Assertions, manager *Manager, name string, end int) {
	for i := 0; i <= end; i++ {
		request := UpdateKeyspaceRequest{
			Name:  name,
			ToPut: map[string]string{testConfig: strconv.Itoa(i)},
		}
		updated, err := manager.UpdateKeyspace(request)
		re.NoError(err)
		matchUpdateRequest(re, request, updated)
	}
}
