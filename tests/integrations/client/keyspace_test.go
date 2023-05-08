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
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server"
)

const (
	testConfig1 = "config_entry_1"
	testConfig2 = "config_entry_2"
)

func mustMakeTestKeyspaces(re *require.Assertions, server *server.Server, start, count int) []*keyspacepb.KeyspaceMeta {
	now := time.Now().Unix()
	var err error
	keyspaces := make([]*keyspacepb.KeyspaceMeta, count)
	manager := server.GetKeyspaceManager()
	for i := 0; i < count; i++ {
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
	metas := mustMakeTestKeyspaces(re, suite.srv, 0, 10)
	for _, expected := range metas {
		loaded, err := suite.client.LoadKeyspace(suite.ctx, expected.GetName())
		re.NoError(err)
		re.Equal(expected, loaded)
	}
	// Loading non-existing keyspace should result in error.
	_, err := suite.client.LoadKeyspace(suite.ctx, "non-existing keyspace")
	re.Error(err)
	// Loading default keyspace should be successful.
	keyspaceDefault, err := suite.client.LoadKeyspace(suite.ctx, utils.DefaultKeyspaceName)
	re.NoError(err)
	re.Equal(utils.DefaultKeyspaceID, keyspaceDefault.GetId())
	re.Equal(utils.DefaultKeyspaceName, keyspaceDefault.GetName())
}

func (suite *clientTestSuite) TestWatchKeyspaces() {
	re := suite.Require()
	initialKeyspaces := mustMakeTestKeyspaces(re, suite.srv, 10, 10)
	watchChan, err := suite.client.WatchKeyspaces(suite.ctx)
	re.NoError(err)
	// First batch of watchChan message should contain all existing keyspaces.
	initialLoaded := <-watchChan
	for i := range initialKeyspaces {
		re.Contains(initialLoaded, initialKeyspaces[i])
	}
	// Each additional message contains extra put events.
	additionalKeyspaces := mustMakeTestKeyspaces(re, suite.srv, 30, 10)
	re.NoError(err)
	// Checks that all additional keyspaces are captured by watch channel.
	for i := 0; i < 10; {
		loadedKeyspaces := <-watchChan
		re.NotEmpty(loadedKeyspaces)
		for j := range loadedKeyspaces {
			re.Equal(additionalKeyspaces[i+j], loadedKeyspaces[j])
		}
		i += len(loadedKeyspaces)
	}
	// Updates to state should also be captured.
	expected, err := suite.srv.GetKeyspaceManager().UpdateKeyspaceState(initialKeyspaces[0].Name, keyspacepb.KeyspaceState_DISABLED, time.Now().Unix())
	re.NoError(err)
	loaded := <-watchChan
	re.Equal([]*keyspacepb.KeyspaceMeta{expected}, loaded)
	// Updates to config should also be captured.
	expected, err = suite.srv.GetKeyspaceManager().UpdateKeyspaceConfig(initialKeyspaces[0].Name, []*keyspace.Mutation{
		{
			Op:  keyspace.OpDel,
			Key: testConfig1,
		},
	})
	re.NoError(err)
	loaded = <-watchChan
	re.Equal([]*keyspacepb.KeyspaceMeta{expected}, loaded)
	// Updates to default keyspace's config should also be captured.
	expected, err = suite.srv.GetKeyspaceManager().UpdateKeyspaceConfig(utils.DefaultKeyspaceName, []*keyspace.Mutation{
		{
			Op:    keyspace.OpPut,
			Key:   "config",
			Value: "value",
		},
	})
	re.NoError(err)
	loaded = <-watchChan
	re.Equal([]*keyspacepb.KeyspaceMeta{expected}, loaded)
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
