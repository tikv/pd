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
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/keyspace"
	"time"
)

const (
	testConfig1 = "config_entry_1"
	testConfig2 = "config_entry_2"
)

func mustMakeTestKeyspaces(re *require.Assertions, server *server.Server, start, count int) []*keyspacepb.KeyspaceMeta {
	now := time.Now()
	var err error
	keyspaces := make([]*keyspacepb.KeyspaceMeta, count)
	manager := server.GetKeyspaceManager()
	for i := 0; i < count; i++ {
		keyspaces[i], err = manager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
			Name: fmt.Sprintf("test_keyspace%d", start+i),
			InitialConfig: map[string]string{
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
}

func (suite *clientTestSuite) TestUpdateKeyspaceConfig() {
	re := suite.Require()
	metas := mustMakeTestKeyspaces(re, suite.srv, 0, 10)
	// Update keyspace configs.
	for _, meta := range metas {
		_, err := suite.client.UpdateKeyspaceConfig(suite.ctx, meta.Name, []*keyspacepb.Mutation{
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
		})
		re.NoError(err)
	}
	// Verify updated keyspaces' configs matches the expectation.
	expectedConfig := map[string]string{
		testConfig1:  "new val",
		"new config": "new val",
	}
	for _, meta := range metas {
		loaded, err := suite.client.LoadKeyspace(suite.ctx, meta.Name)
		re.NoError(err)
		re.Equal(expectedConfig, loaded.Config)
	}
	// Updating a non-existing keyspace should result in error.
	_, err := suite.client.UpdateKeyspaceConfig(suite.ctx, "non-existing keyspace", nil)
	re.Error(err)
}

func (suite *clientTestSuite) TestWatchKeyspace() {
	re := suite.Require()
	initialKeyspaces := mustMakeTestKeyspaces(re, suite.srv, 0, 10)
	watchChan, err := suite.client.WatchKeyspaces(suite.ctx)
	re.NoError(err)
	// First batch of watchChan message should contain all existing keyspaces.
	initialLoaded := <-watchChan
	re.Equal(initialKeyspaces, initialLoaded)
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
	expected, err := suite.srv.GetKeyspaceManager().UpdateKeyspaceState(initialKeyspaces[0].Name, keyspacepb.KeyspaceState_DISABLED, time.Now())
	re.NoError(err)
	loaded := <-watchChan
	re.Equal([]*keyspacepb.KeyspaceMeta{expected}, loaded)
	// Updates to config should also be captured.
	expected, err = suite.srv.GetKeyspaceManager().UpdateKeyspaceConfig(initialKeyspaces[0].Name, []*keyspacepb.Mutation{
		{
			Op:  keyspacepb.Op_DEL,
			Key: []byte(testConfig1),
		},
	})
	re.NoError(err)
	loaded = <-watchChan
	re.Equal([]*keyspacepb.KeyspaceMeta{expected}, loaded)
}
