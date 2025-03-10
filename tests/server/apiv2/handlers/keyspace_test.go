// Copyright 2023 TiKV Project Authors.
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

package handlers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type keyspaceTestSuite struct {
	suite.Suite
	cleanup func()
	cluster *tests.TestCluster
	server  *tests.TestServer
}

func TestKeyspaceTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceTestSuite))
}

func (suite *keyspaceTestSuite) SetupTest() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 1)
	suite.cluster = cluster
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	re.NoError(suite.server.BootstrapCluster())
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
}

func (suite *keyspaceTestSuite) TearDownTest() {
	re := suite.Require()
	suite.cleanup()
	suite.cluster.Destroy()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
}

func (suite *keyspaceTestSuite) TestCreateLoadKeyspace() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 10)
	for _, created := range keyspaces {
		loaded := mustLoadKeyspaces(re, suite.server, created.Name)
		re.Equal(created, loaded)
	}
	defaultKeyspace := mustLoadKeyspaces(re, suite.server, constant.DefaultKeyspaceName)
	re.Equal(constant.DefaultKeyspaceName, defaultKeyspace.Name)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, defaultKeyspace.State)
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceConfig() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 10)
	for _, created := range keyspaces {
		config1val := "300"
		updateRequest := &handlers.UpdateConfigParams{
			Config: map[string]*string{
				"config1": &config1val,
				"config2": nil,
			},
		}
		updated := mustUpdateKeyspaceConfig(re, suite.server, created.Name, updateRequest)
		checkUpdateRequest(re, updateRequest, created.Config, updated.Config)
	}
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceState() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 10)
	for _, created := range keyspaces {
		// Should NOT allow archiving ENABLED keyspace.
		success, _ := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "archived"})
		re.False(success)
		// Disabling an ENABLED keyspace is allowed.
		success, disabled := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "disabled"})
		re.True(success)
		re.Equal(keyspacepb.KeyspaceState_DISABLED, disabled.State)
		// Disabling an already DISABLED keyspace should not result in any change.
		success, disabledAgain := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "disabled"})
		re.True(success)
		re.Equal(disabled, disabledAgain)
		// Tombstone a DISABLED keyspace should not be allowed.
		success, _ = sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "tombstone"})
		re.False(success)
		// Archiving a DISABLED keyspace should be allowed.
		success, archived := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "archived"})
		re.True(success)
		re.Equal(keyspacepb.KeyspaceState_ARCHIVED, archived.State)
		// Enabling an ARCHIVED keyspace is not allowed.
		success, _ = sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "enabled"})
		re.False(success)
		// Tombstone an ARCHIVED keyspace is allowed.
		success, tombstone := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "tombstone"})
		re.True(success)
		re.Equal(keyspacepb.KeyspaceState_TOMBSTONE, tombstone.State)
	}
	// Changing default keyspace's state is NOT allowed.
	success, _ := sendUpdateStateRequest(re, suite.server, constant.DefaultKeyspaceName, &handlers.UpdateStateParam{State: "disabled"})
	re.False(success)
}

func (suite *keyspaceTestSuite) TestLoadRangeKeyspace() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 50)
	loadResponse := sendLoadRangeRequest(re, suite.server, "", "")
	re.Empty(loadResponse.NextPageToken) // Load response should contain no more pages.
	// Load response should contain all created keyspace and a default.
	re.Len(loadResponse.Keyspaces, len(keyspaces)+1)
	for i, created := range keyspaces {
		re.Equal(created, loadResponse.Keyspaces[i+1].KeyspaceMeta)
	}
	re.Equal(constant.DefaultKeyspaceName, loadResponse.Keyspaces[0].Name)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, loadResponse.Keyspaces[0].State)
}

func mustMakeTestKeyspaces(re *require.Assertions, server *tests.TestServer, count int) []*keyspacepb.KeyspaceMeta {
	testConfig := map[string]string{
		"config1": "100",
		"config2": "200",
	}
	resultMeta := make([]*keyspacepb.KeyspaceMeta, count)
	for i := range count {
		createRequest := &handlers.CreateKeyspaceParams{
			Name:   fmt.Sprintf("test_keyspace_%d", i),
			Config: testConfig,
		}
		resultMeta[i] = MustCreateKeyspace(re, server, createRequest)
	}
	return resultMeta
}

// checkUpdateRequest verifies a keyspace meta matches a update request.
func checkUpdateRequest(re *require.Assertions, request *handlers.UpdateConfigParams, oldConfig, newConfig map[string]string) {
	expected := map[string]string{}
	for k, v := range oldConfig {
		expected[k] = v
	}
	for k, v := range request.Config {
		if v == nil {
			delete(expected, k)
		} else {
			expected[k] = *v
		}
	}
	re.Equal(expected, newConfig)
}
