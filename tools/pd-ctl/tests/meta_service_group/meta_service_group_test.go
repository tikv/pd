// Copyright 2026 TiKV Project Authors.
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

package meta_service_group_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"

	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

type metaServiceGroupCLITestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *pdTests.TestCluster
	pdAddr  string
}

func TestMetaServiceGroupCLITestSuite(t *testing.T) {
	suite.Run(t, new(metaServiceGroupCLITestSuite))
}

func (suite *metaServiceGroupCLITestSuite) SetupTest() {
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/delayStartServerLoop", `return(true)`))
	tc, err := pdTests.NewTestCluster(suite.ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.MetaServiceGroups = map[string]string{
			"group-0": "addr0.example.com",
			"group-1": "addr1.example.com",
		}
		conf.Keyspace.WaitRegionSplit = false
	})
	re.NoError(err)
	re.NoError(tc.RunInitialServers())
	re.NotEmpty(tc.WaitLeader())
	re.NoError(tc.GetLeaderServer().BootstrapCluster())
	suite.cluster = tc
	suite.pdAddr = tc.GetConfig().GetClientURL()
}

func (suite *metaServiceGroupCLITestSuite) TearDownTest() {
	re := suite.Require()
	suite.cancel()
	suite.cluster.Destroy()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/delayStartServerLoop"))
}

func (suite *metaServiceGroupCLITestSuite) mustListGroups() []*handlers.MetaServiceGroupStatus {
	re := suite.Require()
	cmd := ctl.GetRootCmd()
	output, err := tests.ExecuteCommand(cmd, "-u", suite.pdAddr, "meta-service-group", "list")
	re.NoError(err)
	var groups []*handlers.MetaServiceGroupStatus
	re.NoError(json.Unmarshal(output, &groups))
	return groups
}

func (suite *metaServiceGroupCLITestSuite) TestListMetaServiceGroups() {
	re := suite.Require()
	groups := suite.mustListGroups()
	re.Len(groups, 2)
	groupMap := make(map[string]string)
	for _, g := range groups {
		groupMap[g.ID] = g.Addresses
	}
	re.Equal("addr0.example.com", groupMap["group-0"])
	re.Equal("addr1.example.com", groupMap["group-1"])
}

func (suite *metaServiceGroupCLITestSuite) TestUpsetMetaServiceGroup() {
	re := suite.Require()
	cmd := ctl.GetRootCmd()

	// Add a new group.
	output, err := tests.ExecuteCommand(cmd, "-u", suite.pdAddr, "meta-service-group", "upset",
		"--group", "group-2=addr2.example.com")
	re.NoError(err)
	var groups []*handlers.MetaServiceGroupStatus
	re.NoError(json.Unmarshal(output, &groups))
	re.Len(groups, 3)
	found := false
	for _, g := range groups {
		if g.ID == "group-2" {
			found = true
			re.Equal("addr2.example.com", g.Addresses)
		}
	}
	re.True(found)

	// Update an existing group address.
	output, err = tests.ExecuteCommand(cmd, "-u", suite.pdAddr, "meta-service-group", "upset",
		"--group", "group-0=new-addr0.example.com")
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &groups))
	for _, g := range groups {
		if g.ID == "group-0" {
			re.Equal("new-addr0.example.com", g.Addresses)
		}
	}

	// Upset multiple groups at once.
	output, err = tests.ExecuteCommand(cmd, "-u", suite.pdAddr, "meta-service-group", "upset",
		"--group", "group-3=addr3.example.com",
		"--group", "group-4=addr4.example.com")
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &groups))
	re.Len(groups, 5)
}

func (suite *metaServiceGroupCLITestSuite) TestDeleteMetaServiceGroup() {
	re := suite.Require()
	cmd := ctl.GetRootCmd()

	output, err := tests.ExecuteCommand(cmd, "-u", suite.pdAddr, "meta-service-group", "delete", "group-1")
	re.NoError(err)
	var groups []*handlers.MetaServiceGroupStatus
	re.NoError(json.Unmarshal(output, &groups))
	re.Len(groups, 1)
	re.Equal("group-0", groups[0].ID)
}

func (suite *metaServiceGroupCLITestSuite) TestUpsetInvalidInput() {
	re := suite.Require()
	cmd := ctl.GetRootCmd()

	// Missing "=" separator.
	output, err := tests.ExecuteCommand(cmd, "-u", suite.pdAddr, "meta-service-group", "upset",
		"--group", "group-noeq")
	re.NoError(err)
	re.Contains(string(output), "Invalid --group format")

	// Empty ID.
	output, err = tests.ExecuteCommand(cmd, "-u", suite.pdAddr, "meta-service-group", "upset",
		"--group", "=addr.example.com")
	re.NoError(err)
	re.Contains(string(output), "ID cannot be empty")

	// Empty address.
	output, err = tests.ExecuteCommand(cmd, "-u", suite.pdAddr, "meta-service-group", "upset",
		"--group", "group-x=")
	re.NoError(err)
	re.Contains(string(output), "address cannot be empty")
}
