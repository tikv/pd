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

package keyspace_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/testutil"
	api "github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

func TestKeyspace(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	keyspaces := make([]string, 0)
	for i := 1; i < 10; i++ {
		keyspaces = append(keyspaces, fmt.Sprintf("keyspace_%d", i))
	}
	tc, err := pdTests.NewTestAPICluster(ctx, 3, func(conf *config.Config, _ string) {
		conf.Keyspace.PreAlloc = keyspaces
	})
	re.NoError(err)
	defer tc.Destroy()
	err = tc.RunInitialServers()
	re.NoError(err)
	pdAddr := tc.GetConfig().GetClientURL()

	ttc, err := pdTests.NewTestTSOCluster(ctx, 2, pdAddr)
	re.NoError(err)
	defer ttc.Destroy()
	cmd := ctl.GetRootCmd()

	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	defaultKeyspaceGroupID := fmt.Sprintf("%d", constant.DefaultKeyspaceGroupID)

	var k api.KeyspaceMeta
	keyspaceName := "keyspace_1"
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace", "show", "name", keyspaceName}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &k))
		return k.GetName() == keyspaceName
	})
	re.Equal(uint32(1), k.GetId())
	re.Equal(defaultKeyspaceGroupID, k.Config[keyspace.TSOKeyspaceGroupIDKey])

	// split keyspace group.
	newGroupID := "2"
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace-group", "split", "0", newGroupID, "1"}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		return strings.Contains(string(output), "Success")
	})

	// check keyspace group in keyspace whether changed.
	testutil.Eventually(re, func() bool {
		args := []string{"-u", pdAddr, "keyspace", "show", "name", keyspaceName}
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		re.NoError(json.Unmarshal(output, &k))
		return newGroupID == k.Config[keyspace.TSOKeyspaceGroupIDKey]
	})

	// test error name
	args := []string{"-u", pdAddr, "keyspace", "show", "name", "error_name"}
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Fail")
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastGroupSplitPatroller"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
}

// Show command should auto retry without refresh_group_id if keyspace group manager not initialized.
// See issue: #7441
func TestKeyspaceGroupUninitialized(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	tc, err := pdTests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer tc.Destroy()
	re.NoError(tc.RunInitialServers())
	tc.WaitLeader()
	re.NoError(tc.GetLeaderServer().BootstrapCluster())
	pdAddr := tc.GetConfig().GetClientURL()

	keyspaceName := "DEFAULT"
	keyspaceID := uint32(0)
	args := []string{"-u", pdAddr, "keyspace", "show", "name", keyspaceName}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	var meta api.KeyspaceMeta
	re.NoError(json.Unmarshal(output, &meta))
	re.Equal(keyspaceName, meta.GetName())
	re.Equal(keyspaceID, meta.GetId())

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
}

type keyspaceTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *pdTests.TestCluster
	pdAddr  string
}

func TestKeyspaceTestsuite(t *testing.T) {
	suite.Run(t, new(keyspaceTestSuite))
}

func (suite *keyspaceTestSuite) SetupTest() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	tc, err := pdTests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)
	re.NoError(tc.RunInitialServers())
	tc.WaitLeader()
	leaderServer := tc.GetLeaderServer()
	re.NoError(leaderServer.BootstrapCluster())
	suite.cluster = tc
	suite.pdAddr = tc.GetConfig().GetClientURL()
}

func (suite *keyspaceTestSuite) TearDownTest() {
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
}

func (suite *keyspaceTestSuite) TearDownSuite() {
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *keyspaceTestSuite) TestShowKeyspace() {
	re := suite.Require()
	keyspaceName := "DEFAULT"
	keyspaceID := uint32(0)
	var k1, k2 api.KeyspaceMeta
	// Show by name.
	args := []string{"-u", suite.pdAddr, "keyspace", "show", "name", keyspaceName}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &k1))
	re.Equal(keyspaceName, k1.GetName())
	re.Equal(keyspaceID, k1.GetId())
	// Show by ID.
	args = []string{"-u", suite.pdAddr, "keyspace", "show", "id", strconv.Itoa(int(keyspaceID))}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &k2))
	re.Equal(k1, k2)
}

func mustCreateKeyspace(suite *keyspaceTestSuite, param api.CreateKeyspaceParams) api.KeyspaceMeta {
	re := suite.Require()
	var meta api.KeyspaceMeta
	args := []string{"-u", suite.pdAddr, "keyspace", "create", param.Name}
	for k, v := range param.Config {
		args = append(args, "--config", fmt.Sprintf("%s=%s", k, v))
	}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &meta))
	return meta
}

func (suite *keyspaceTestSuite) TestCreateKeyspace() {
	re := suite.Require()
	param := api.CreateKeyspaceParams{
		Name: "test_keyspace",
		Config: map[string]string{
			"foo":  "bar",
			"foo2": "bar2",
		},
	}
	meta := mustCreateKeyspace(suite, param)
	re.Equal(param.Name, meta.GetName())
	for k, v := range param.Config {
		re.Equal(v, meta.Config[k])
	}
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceConfig() {
	re := suite.Require()
	param := api.CreateKeyspaceParams{
		Name:   "test_keyspace",
		Config: map[string]string{"foo": "1"},
	}
	meta := mustCreateKeyspace(suite, param)
	re.Equal("1", meta.Config["foo"])

	// Update one existing config and add a new config, resulting in config: {foo: 2, foo2: 1}.
	args := []string{"-u", suite.pdAddr, "keyspace", "update-config", param.Name, "--update", "foo=2,foo2=1"}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &meta))
	re.Equal("test_keyspace", meta.GetName())
	re.Equal("2", meta.Config["foo"])
	re.Equal("1", meta.Config["foo2"])
	// Update one existing config and remove a config, resulting in config: {foo: 3}.
	args = []string{"-u", suite.pdAddr, "keyspace", "update-config", param.Name, "--update", "foo=3", "--remove", "foo2"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &meta))
	re.Equal("test_keyspace", meta.GetName())
	re.Equal("3", meta.Config["foo"])
	re.NotContains(meta.GetConfig(), "foo2")
	// Error if a key is specified in both --update and --remove list.
	args = []string{"-u", suite.pdAddr, "keyspace", "update-config", param.Name, "--update", "foo=4", "--remove", "foo"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), "Fail")
	// Error if a key is specified multiple times.
	args = []string{"-u", suite.pdAddr, "keyspace", "update-config", param.Name, "--update", "foo=4,foo=5"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), "Fail")
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceState() {
	re := suite.Require()
	param := api.CreateKeyspaceParams{
		Name: "test_keyspace",
	}
	meta := mustCreateKeyspace(suite, param)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, meta.State)
	// Disable the keyspace, capitalization shouldn't matter.
	args := []string{"-u", suite.pdAddr, "keyspace", "update-state", param.Name, "DiSAbleD"}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &meta))
	re.Equal(keyspacepb.KeyspaceState_DISABLED, meta.State)
	// Tombstone the keyspace without archiving should fail.
	args = []string{"-u", suite.pdAddr, "keyspace", "update-state", param.Name, "TOMBSTONE"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.Contains(string(output), "Fail")
}

func (suite *keyspaceTestSuite) TestListKeyspace() {
	re := suite.Require()
	var param api.CreateKeyspaceParams
	for i := range 10 {
		param = api.CreateKeyspaceParams{
			Name: fmt.Sprintf("test_keyspace_%d", i),
			Config: map[string]string{
				"foo": fmt.Sprintf("bar_%d", i),
			},
		}
		mustCreateKeyspace(suite, param)
	}
	// List all keyspaces, there should be 11 of them (default + 10 created above).
	args := []string{"-u", suite.pdAddr, "keyspace", "list"}
	output, err := tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	var resp api.LoadAllKeyspacesResponse
	re.NoError(json.Unmarshal(output, &resp))
	re.Len(resp.Keyspaces, 11)
	re.Equal("", resp.NextPageToken) // No next page token since we load them all.
	re.Equal("DEFAULT", resp.Keyspaces[0].GetName())
	for i, meta := range resp.Keyspaces[1:] {
		re.Equal(fmt.Sprintf("test_keyspace_%d", i), meta.GetName())
		re.Equal(fmt.Sprintf("bar_%d", i), meta.Config["foo"])
	}
	// List 3 keyspaces staring with keyspace id 3, should results in keyspace id 3, 4, 5 and next page token 6.
	args = []string{"-u", suite.pdAddr, "keyspace", "list", "--limit", "3", "--page_token", "3"}
	output, err = tests.ExecuteCommand(ctl.GetRootCmd(), args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &resp))
	re.Len(resp.Keyspaces, 3)
	for i, meta := range resp.Keyspaces {
		re.Equal(uint32(i+3), meta.GetId())
		re.Equal(fmt.Sprintf("test_keyspace_%d", i+2), meta.GetName())
		re.Equal(fmt.Sprintf("bar_%d", i+2), meta.Config["foo"])
	}
	re.Equal("6", resp.NextPageToken)
}
