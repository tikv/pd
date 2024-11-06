// Copyright 2021 TiKV Project Authors.
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

package client_test

import (
	"fmt"
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server"
)

const (
	testConfig1       = "config_entry_1"
	testConfig2       = "config_entry_2"
	testKeyspaceCount = 10
)

func mustMakeTestKeyspaces(re *require.Assertions, server *server.Server, start int) []*keyspacepb.KeyspaceMeta {
	now := time.Now().Unix()
	var err error
	keyspaces := make([]*keyspacepb.KeyspaceMeta, testKeyspaceCount)
	manager := server.GetKeyspaceManager()
	for i := range testKeyspaceCount {
		keyspaces[i], err = manager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
			Name: fmt.Sprintf("test_keyspace_%d", start+i),
			Config: map[string]string{
				testConfig1: "100",
				testConfig2: "200",
			},
			CreateTime: now,
			IsPreAlloc: true, // skip wait region split
		})
		re.NoError(err)
	}
	return keyspaces
}

func (suite *clientTestSuite) TestLoadKeyspace() {
	re := suite.Require()
	metas := mustMakeTestKeyspaces(re, suite.srv, 0)
	for _, expected := range metas {
		loaded, err := suite.client.LoadKeyspace(suite.ctx, expected.GetName())
		re.NoError(err)
		re.Equal(expected, loaded)
	}
	// Loading non-existing keyspace should result in error.
	_, err := suite.client.LoadKeyspace(suite.ctx, "non-existing keyspace")
	re.Error(err)
	// Loading default keyspace should be successful.
	keyspaceDefault, err := suite.client.LoadKeyspace(suite.ctx, constant.DefaultKeyspaceName)
	re.NoError(err)
	re.Equal(constant.DefaultKeyspaceID, keyspaceDefault.GetId())
	re.Equal(constant.DefaultKeyspaceName, keyspaceDefault.GetName())
}

func (suite *clientTestSuite) TestGetAllKeyspaces() {
	re := suite.Require()
	metas := mustMakeTestKeyspaces(re, suite.srv, 20)
	for _, expected := range metas {
		loaded, err := suite.client.LoadKeyspace(suite.ctx, expected.GetName())
		re.NoError(err)
		re.Equal(expected, loaded)
	}
	// Get all keyspaces.
	resKeyspaces, err := suite.client.GetAllKeyspaces(suite.ctx, 1, math.MaxUint32)
	re.NoError(err)
	re.Equal(len(metas), len(resKeyspaces))
	// Check expected keyspaces all in resKeyspaces.
	for _, expected := range metas {
		var isExists bool
		for _, resKeyspace := range resKeyspaces {
			if expected.GetName() == resKeyspace.GetName() {
				isExists = true
				continue
			}
		}
		if !isExists {
			re.Fail("not exists keyspace")
		}
	}
}

func mustCreateKeyspaceAtState(re *require.Assertions, server *server.Server, index int, state keyspacepb.KeyspaceState) *keyspacepb.KeyspaceMeta {
	manager := server.GetKeyspaceManager()
	meta, err := manager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
		Name:       fmt.Sprintf("test_keyspace_%d", index),
		Config:     nil,
		CreateTime: 0,    // Use 0 to indicate unchanged keyspace.
		IsPreAlloc: true, // skip wait region split
	})
	re.NoError(err)
	switch state {
	case keyspacepb.KeyspaceState_ENABLED:
	case keyspacepb.KeyspaceState_DISABLED:
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_DISABLED, 0)
		re.NoError(err)
	case keyspacepb.KeyspaceState_ARCHIVED:
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_DISABLED, 0)
		re.NoError(err)
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_ARCHIVED, 0)
		re.NoError(err)
	case keyspacepb.KeyspaceState_TOMBSTONE:
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_DISABLED, 0)
		re.NoError(err)
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_ARCHIVED, 0)
		re.NoError(err)
		meta, err = manager.UpdateKeyspaceStateByID(meta.GetId(), keyspacepb.KeyspaceState_TOMBSTONE, 0)
		re.NoError(err)
	default:
		re.Fail("unknown keyspace state")
	}
	return meta
}

func (suite *clientTestSuite) TestUpdateKeyspaceState() {
	re := suite.Require()
	allStates := []keyspacepb.KeyspaceState{
		keyspacepb.KeyspaceState_ENABLED,
		keyspacepb.KeyspaceState_DISABLED,
		keyspacepb.KeyspaceState_ARCHIVED,
		keyspacepb.KeyspaceState_TOMBSTONE,
	}
	allowedTransitions := map[keyspacepb.KeyspaceState][]keyspacepb.KeyspaceState{
		keyspacepb.KeyspaceState_ENABLED:   {keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_DISABLED},
		keyspacepb.KeyspaceState_DISABLED:  {keyspacepb.KeyspaceState_DISABLED, keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_ARCHIVED},
		keyspacepb.KeyspaceState_ARCHIVED:  {keyspacepb.KeyspaceState_ARCHIVED, keyspacepb.KeyspaceState_TOMBSTONE},
		keyspacepb.KeyspaceState_TOMBSTONE: {keyspacepb.KeyspaceState_TOMBSTONE},
	}
	// Use index to avoid collision with other tests.
	index := 1000
	for _, originState := range allStates {
		for _, targetState := range allStates {
			meta := mustCreateKeyspaceAtState(re, suite.srv, index, originState)
			updated, err := suite.client.UpdateKeyspaceState(suite.ctx, meta.GetId(), targetState)
			if slice.Contains(allowedTransitions[originState], targetState) {
				// If transition is allowed, then update must be successful.
				re.NoError(err)
				if originState != targetState {
					// If changing state, then must record stateChangedAt.
					re.NotEqual(updated.GetStateChangedAt(), meta.GetStateChangedAt())
				} else {
					// Otherwise the request should be idempotent.
					re.Equal(updated, meta)
				}
			} else {
				// If operation is not allowed, then update must fail, returned meta must be nil.
				re.Error(err)
				re.Nil(updated)
			}
			index++
		}
	}
}
