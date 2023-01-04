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
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/keyspace"
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
			Name: fmt.Sprintf("test_keyspace%d", start+i),
			Config: map[string]string{
				testConfig1: "100",
				testConfig2: "200",
			},
			Now: now,
		})
		re.NoError(err)
	}
	return keyspaces
}

func (suite *clientTestSuite) TestLoadKeyspace() {
	re := suite.Require()
	metas := mustMakeTestKeyspaces(re, suite.srv, 0, 10)
	for _, expected := range metas {
		loaded, err := suite.client.LoadKeyspace(suite.ctx, expected.Name)
		re.NoError(err)
		re.Equal(expected, loaded)
	}
	// Loading non-existing keyspace should result in error.
	_, err := suite.client.LoadKeyspace(suite.ctx, "non-existing keyspace")
	re.Error(err)
	// Loading default keyspace should be successful.
	keyspaceDefault, err := suite.client.LoadKeyspace(suite.ctx, keyspace.DefaultKeyspaceName)
	re.NoError(err)
	re.Equal(keyspace.DefaultKeyspaceID, keyspaceDefault.Id)
	re.Equal(keyspace.DefaultKeyspaceName, keyspaceDefault.Name)
}

func (suite *clientTestSuite) TestWatchKeyspace() {
	re := suite.Require()
	initialKeyspaces := mustMakeTestKeyspaces(re, suite.srv, 10, 10)
	watchChan, err := suite.client.WatchKeyspaces(suite.ctx)
	re.NoError(err)
	// First batch of watchChan message should contain all existing keyspaces, including the default.
	initialLoaded := <-watchChan
	for i := range initialKeyspaces {
		re.Contains(initialLoaded, initialKeyspaces[i])
	}
	keyspaceDefault, err := suite.client.LoadKeyspace(suite.ctx, keyspace.DefaultKeyspaceName)
	re.Contains(initialLoaded, keyspaceDefault)
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
	expected, err = suite.srv.GetKeyspaceManager().UpdateKeyspaceConfig(keyspace.DefaultKeyspaceName, []*keyspace.Mutation{
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
