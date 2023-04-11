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
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

const (
	testConfig  = "test config"
	testConfig1 = "config_entry_1"
	testConfig2 = "config_entry_2"
)

type keyspaceTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	manager *Manager
}

func TestKeyspaceTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceTestSuite))
}

type mockConfig struct {
	PreAlloc []string
}

func (m *mockConfig) GetPreAlloc() []string { return m.PreAlloc }

func (suite *keyspaceTestSuite) SetupTest() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	allocator := mockid.NewIDAllocator()
	kgm := NewKeyspaceGroupManager(suite.ctx, store, nil, 0)
	suite.manager = NewKeyspaceManager(store, nil, allocator, &mockConfig{}, kgm)
	suite.NoError(kgm.Bootstrap())
	suite.NoError(suite.manager.Bootstrap())
}

func (suite *keyspaceTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *keyspaceTestSuite) SetupSuite() {
	suite.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
}

func (suite *keyspaceTestSuite) TearDownSuite() {
	suite.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
}

func makeCreateKeyspaceRequests(count int) []*CreateKeyspaceRequest {
	now := time.Now().Unix()
	requests := make([]*CreateKeyspaceRequest, count)
	for i := 0; i < count; i++ {
		requests[i] = &CreateKeyspaceRequest{
			Name: fmt.Sprintf("test_keyspace_%d", i),
			Config: map[string]string{
				testConfig1: "100",
				testConfig2: "200",
			},
			CreateTime: now,
		}
	}
	return requests
}

func (suite *keyspaceTestSuite) TestCreateKeyspace() {
	re := suite.Require()
	manager := suite.manager
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

		loaded, err = manager.LoadKeyspaceByID(created.Id)
		re.NoError(err)
		re.Equal(loaded.Name, request.Name)
		checkCreateRequest(re, request, loaded)
	}

	// Create a keyspace with existing name must return error.
	_, err := manager.CreateKeyspace(requests[0])
	re.Error(err)

	// Create a keyspace with empty name must return error.
	_, err = manager.CreateKeyspace(&CreateKeyspaceRequest{Name: ""})
	re.Error(err)
}

func makeMutations() []*Mutation {
	return []*Mutation{
		{
			Op:    OpPut,
			Key:   testConfig1,
			Value: "new val",
		},
		{
			Op:    OpPut,
			Key:   "new config",
			Value: "new val",
		},
		{
			Op:  OpDel,
			Key: testConfig2,
		},
	}
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceConfig() {
	re := suite.Require()
	manager := suite.manager
	requests := makeCreateKeyspaceRequests(5)
	mutations := makeMutations()
	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
		updated, err := manager.UpdateKeyspaceConfig(createRequest.Name, mutations)
		re.NoError(err)
		checkMutations(re, createRequest.Config, updated.Config, mutations)
		// Changing config of a ARCHIVED keyspace is not allowed.
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_DISABLED, time.Now().Unix())
		re.NoError(err)
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ARCHIVED, time.Now().Unix())
		re.NoError(err)
		_, err = manager.UpdateKeyspaceConfig(createRequest.Name, mutations)
		re.Error(err)
	}
	// Changing config of DEFAULT keyspace is allowed.
	updated, err := manager.UpdateKeyspaceConfig(DefaultKeyspaceName, mutations)
	re.NoError(err)
	// remove auto filled fields
	delete(updated.Config, TSOKeyspaceGroupIDKey)
	delete(updated.Config, UserKindKey)
	checkMutations(re, nil, updated.Config, mutations)
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceState() {
	re := suite.Require()
	manager := suite.manager
	requests := makeCreateKeyspaceRequests(5)
	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
		oldTime := time.Now().Unix()
		// Archiving an ENABLED keyspace is not allowed.
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ARCHIVED, oldTime)
		re.Error(err)
		// Disabling an ENABLED keyspace is allowed. Should update StateChangedAt.
		updated, err := manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_DISABLED, oldTime)
		re.NoError(err)
		re.Equal(updated.State, keyspacepb.KeyspaceState_DISABLED)
		re.Equal(updated.StateChangedAt, oldTime)

		newTime := time.Now().Unix()
		// Disabling an DISABLED keyspace is allowed. Should NOT update StateChangedAt.
		updated, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_DISABLED, newTime)
		re.NoError(err)
		re.Equal(updated.State, keyspacepb.KeyspaceState_DISABLED)
		re.Equal(updated.StateChangedAt, oldTime)
		// Archiving a DISABLED keyspace is allowed. Should update StateChangeAt.
		updated, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ARCHIVED, newTime)
		re.NoError(err)
		re.Equal(updated.State, keyspacepb.KeyspaceState_ARCHIVED)
		re.Equal(updated.StateChangedAt, newTime)
		// Changing state of an ARCHIVED keyspace is not allowed.
		_, err = manager.UpdateKeyspaceState(createRequest.Name, keyspacepb.KeyspaceState_ENABLED, newTime)
		re.Error(err)
		// Changing state of DEFAULT keyspace is not allowed.
		_, err = manager.UpdateKeyspaceState(DefaultKeyspaceName, keyspacepb.KeyspaceState_DISABLED, newTime)
		re.Error(err)
	}
}

func (suite *keyspaceTestSuite) TestLoadRangeKeyspace() {
	re := suite.Require()
	manager := suite.manager
	// Test with 100 keyspaces.
	// Created keyspace ids are 1 - 100.
	total := 100
	requests := makeCreateKeyspaceRequests(total)

	for _, createRequest := range requests {
		_, err := manager.CreateKeyspace(createRequest)
		re.NoError(err)
	}

	// Load all keyspaces including the default keyspace.
	keyspaces, err := manager.LoadRangeKeyspace(0, 0)
	re.NoError(err)
	re.Equal(total+1, len(keyspaces))
	for i := range keyspaces {
		re.Equal(uint32(i), keyspaces[i].Id)
		if i != 0 {
			checkCreateRequest(re, requests[i-1], keyspaces[i])
		}
	}

	// Load first 50 keyspaces.
	// Result should be keyspaces with id 0 - 49.
	keyspaces, err = manager.LoadRangeKeyspace(0, 50)
	re.NoError(err)
	re.Equal(50, len(keyspaces))
	for i := range keyspaces {
		re.Equal(uint32(i), keyspaces[i].Id)
		if i != 0 {
			checkCreateRequest(re, requests[i-1], keyspaces[i])
		}
	}

	// Load 20 keyspaces starting from keyspace with id 33.
	// Result should be keyspaces with id 33 - 52.
	loadStart := 33
	keyspaces, err = manager.LoadRangeKeyspace(uint32(loadStart), 20)
	re.NoError(err)
	re.Equal(20, len(keyspaces))
	for i := range keyspaces {
		re.Equal(uint32(loadStart+i), keyspaces[i].Id)
		checkCreateRequest(re, requests[i+loadStart-1], keyspaces[i])
	}

	// Attempts to load 30 keyspaces starting from keyspace with id 90.
	// Scan result should be keyspaces with id 90-100.
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
func (suite *keyspaceTestSuite) TestUpdateMultipleKeyspace() {
	re := suite.Require()
	manager := suite.manager
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
	re.Equal(request.Name, meta.GetName())
	re.Equal(request.CreateTime, meta.GetCreatedAt())
	re.Equal(request.CreateTime, meta.GetStateChangedAt())
	re.Equal(keyspacepb.KeyspaceState_ENABLED, meta.GetState())
	re.Equal(request.Config, meta.GetConfig())
}

// checkMutations verifies that performing mutations on old config would result in new config.
func checkMutations(re *require.Assertions, oldConfig, newConfig map[string]string, mutations []*Mutation) {
	// Copy oldConfig to expected to avoid modifying its content.
	expected := map[string]string{}
	for k, v := range oldConfig {
		expected[k] = v
	}
	for _, mutation := range mutations {
		switch mutation.Op {
		case OpPut:
			expected[mutation.Key] = mutation.Value
		case OpDel:
			delete(expected, mutation.Key)
		}
	}
	re.Equal(expected, newConfig)
}

// updateKeyspaceConfig sequentially updates given keyspace's entry.
func updateKeyspaceConfig(re *require.Assertions, manager *Manager, name string, end int) {
	oldMeta, err := manager.LoadKeyspace(name)
	re.NoError(err)
	for i := 0; i <= end; i++ {
		mutations := []*Mutation{
			{
				Op:    OpPut,
				Key:   testConfig,
				Value: strconv.Itoa(i),
			},
		}
		updatedMeta, err := manager.UpdateKeyspaceConfig(name, mutations)
		re.NoError(err)
		checkMutations(re, oldMeta.GetConfig(), updatedMeta.GetConfig(), mutations)
		oldMeta = updatedMeta
	}
}
