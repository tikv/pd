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
	"math"
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

const (
	testConfig  = "test config"
	testConfig1 = "config_entry_1"
	testConfig2 = "config_entry_2"
)

func newKeyspaceManager() *Manager {
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	allocator := mockid.NewIDAllocator()
	return NewKeyspaceManager(store, allocator)
}

func makeCreateKeyspaceRequests(count int) []*CreateKeyspaceRequest {
	now := time.Now()
	requests := make([]*CreateKeyspaceRequest, count)
	for i := 0; i < count; i++ {
		requests[i] = &CreateKeyspaceRequest{
			Name: fmt.Sprintf("test_keyspace%d", i),
			InitialConfig: map[string]string{
				testConfig1: "100",
				testConfig2: "200",
			},
			Now: now,
		}
	}
	return requests
}

func TestCreateKeyspace(t *testing.T) {
	re := require.New(t)
	manager := newKeyspaceManager()
	requests := makeCreateKeyspaceRequests(10)

	for i, request := range requests {
		created, err := manager.CreateKeyspace(request)
		re.NoError(err)
		re.Equal(uint32(i+1), created.Id)
		checkCreateRequest(re, request, created)

		loaded, err := manager.LoadKeyspace(request.Name)
		re.NoError(err)
		re.Equal(uint32(i+1), loaded.Id)
		checkCreateRequest(re, request, loaded)
	}

	// Create a keyspace with existing name must return error.
	_, err := manager.CreateKeyspace(requests[0])
	re.Error(err)

	// Create a keyspace with empty name must return error.
	_, err = manager.CreateKeyspace(&CreateKeyspaceRequest{Name: ""})
	re.Error(err)
}

func makeMutations() []*keyspacepb.Mutation {
	return []*keyspacepb.Mutation{
		{
			Op:    keyspacepb.Op_PUT,
			Key:   []byte(testConfig1),
			Value: []byte("new val"),
		},
		{
			Op:    keyspacepb.Op_PUT,
			Key:   []byte("new config"),
			Value: []byte("new val"),
		},
		{
			Op:  keyspacepb.Op_DEL,
			Key: []byte(testConfig2),
		},
	}
}

func TestUpdateKeyspaceConfig(t *testing.T) {
	re := require.New(t)
	manager := newKeyspaceManager()
	requests := makeCreateKeyspaceRequests(5)
	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
		mutations := makeMutations()
		updated, err := manager.UpdateKeyspaceConfig(createRequest.Name, mutations)
		re.NoError(err)
		checkMutations(re, createRequest.InitialConfig, updated.Config, mutations)
		// Changing config of a ARCHIVED keyspace is not allowed.
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_DISABLED, time.Now())
		re.NoError(err)
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ARCHIVED, time.Now())
		re.NoError(err)
		_, err = manager.UpdateKeyspaceConfig(createRequest.Name, mutations)
		re.Error(err)
	}
}

func TestUpdateKeyspaceState(t *testing.T) {
	re := require.New(t)
	manager := newKeyspaceManager()
	requests := makeCreateKeyspaceRequests(5)
	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
		oldTime := time.Now()
		// Archiving an ENABLED keyspace is not allowed.
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ARCHIVED, oldTime)
		re.Error(err)
		// Disabling an ENABLED keyspace is allowed. Should update StateChangedAt.
		updated, err := manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_DISABLED, oldTime)
		re.NoError(err)
		re.Equal(updated.State, keyspacepb.KeyspaceState_DISABLED)
		re.Equal(updated.StateChangedAt, oldTime.Unix())

		newTime := time.Now()
		// Disabling an DISABLED keyspace is allowed. Should NOT update StateChangedAt.
		updated, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_DISABLED, newTime)
		re.NoError(err)
		re.Equal(updated.State, keyspacepb.KeyspaceState_DISABLED)
		re.Equal(updated.StateChangedAt, oldTime.Unix())
		// Archiving a DISABLED keyspace is allowed. Should update StateChangeAt.
		updated, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ARCHIVED, newTime)
		re.NoError(err)
		re.Equal(updated.State, keyspacepb.KeyspaceState_ARCHIVED)
		re.Equal(updated.StateChangedAt, newTime.Unix())
		// Changing state of an ARCHIVED keyspace is not allowed.
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ENABLED, newTime)
		re.Error(err)
	}
}

func TestLoadRangeKeyspace(t *testing.T) {
	re := require.New(t)
	manager := newKeyspaceManager()
	// Test with 100 keyspaces.
	// Keyspace ids are 1 - 101.
	total := 100
	requests := makeCreateKeyspaceRequests(total)

	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
	}

	// Load all keyspaces.
	keyspaces, err := manager.LoadRangeKeyspace(0, 0)
	re.NoError(err)
	re.Equal(total, len(keyspaces))
	for i := range keyspaces {
		re.Equal(uint32(i+1), keyspaces[i].Id)
		checkCreateRequest(re, requests[i], keyspaces[i])
	}

	// Load first 50 keyspaces.
	keyspaces, err = manager.LoadRangeKeyspace(0, 50)
	re.NoError(err)
	re.Equal(50, len(keyspaces))
	for i := range keyspaces {
		re.Equal(uint32(i+1), keyspaces[i].Id)
		checkCreateRequest(re, requests[i], keyspaces[i])
	}

	// Load 20 keyspaces starting from keyspace with id 33.
	loadStart := 33
	keyspaces, err = manager.LoadRangeKeyspace(uint32(loadStart), 20)
	re.NoError(err)
	re.Equal(20, len(keyspaces))
	for i := range keyspaces {
		re.Equal(uint32(loadStart+i), keyspaces[i].Id)
		checkCreateRequest(re, requests[i+loadStart-1], keyspaces[i])
	}

	// Attempts to load 30 keyspaces starting from keyspace with id 90.
	// Scan result should be keyspaces with id 90-101.
	loadStart = 90
	keyspaces, err = manager.LoadRangeKeyspace(uint32(loadStart), 30)
	re.NoError(err)
	re.Equal(11, len(keyspaces))
	for i := range keyspaces {
		re.Equal(uint32(loadStart+i), keyspaces[i].Id)
		checkCreateRequest(re, requests[i+loadStart-1], keyspaces[i])
	}

	// Loading starting from non-existing keyspace ID should result in empty result.
	loadStart = 900
	keyspaces, err = manager.LoadRangeKeyspace(uint32(loadStart), 0)
	re.NoError(err)
	re.Empty(keyspaces)

	// Scanning starting from a non-zero illegal index should result in error.
	loadStart = math.MaxUint32
	_, err = manager.LoadRangeKeyspace(uint32(loadStart), 0)
	re.Error(err)
}

// TestUpdateMultipleKeyspace checks that updating multiple keyspace's config simultaneously
// will be successful.
func TestUpdateMultipleKeyspace(t *testing.T) {
	re := require.New(t)
	manager := newKeyspaceManager()
	requests := makeCreateKeyspaceRequests(50)
	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
	}

	// Concurrently update all keyspaces' testConfig sequentially.
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

	// Check that eventually all test keyspaces' test config reaches end
	for _, request := range requests {
		keyspace, err := manager.LoadKeyspace(request.Name)
		re.NoError(err)
		re.Equal(keyspace.Config[testConfig], strconv.Itoa(end))
	}
}

// checkCreateRequest verifies a keyspace meta matches a create request.
func checkCreateRequest(re *require.Assertions, request *CreateKeyspaceRequest, meta *keyspacepb.KeyspaceMeta) {
	re.Equal(request.Name, meta.Name)
	re.Equal(request.Now.Unix(), meta.CreatedAt)
	re.Equal(request.Now.Unix(), meta.StateChangedAt)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, meta.State)
	re.Equal(request.InitialConfig, meta.Config)
}

// checkMutations verifies that performing mutations on old config would result in new config.
func checkMutations(re *require.Assertions, oldConfig, newConfig map[string]string, mutations []*keyspacepb.Mutation) {
	// Copy oldConfig to expected to avoid modifying its content.
	expected := map[string]string{}
	for k, v := range oldConfig {
		expected[k] = v
	}
	for _, mutation := range mutations {
		switch mutation.Op {
		case keyspacepb.Op_PUT:
			expected[string(mutation.Key)] = string(mutation.Value)
		case keyspacepb.Op_DEL:
			delete(expected, string(mutation.Key))
		}
	}
	re.Equal(expected, newConfig)
}

// updateKeyspaceConfig sequentially updates given keyspace's entry.
func updateKeyspaceConfig(re *require.Assertions, manager *Manager, name string, end int) {
	oldMeta, err := manager.LoadKeyspace(name)
	re.NoError(err)
	for i := 0; i <= end; i++ {
		mutations := []*keyspacepb.Mutation{
			{
				Op:    keyspacepb.Op_PUT,
				Key:   []byte(testConfig),
				Value: []byte(strconv.Itoa(i)),
			},
		}
		updatedMeta, err := manager.UpdateKeyspaceConfig(name, mutations)
		re.NoError(err)
		checkMutations(re, oldMeta.Config, updatedMeta.Config, mutations)
		oldMeta = updatedMeta
	}
}
