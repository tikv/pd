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

package members_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"

	pdClient "github.com/tikv/pd/client/http"
	bs "github.com/tikv/pd/pkg/basicserver"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/tso/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type memberTestSuite struct {
	suite.Suite
	ctx              context.Context
	cleanupFunc      []testutil.CleanupFunc
	cluster          *tests.TestCluster
	server           *tests.TestServer
	backendEndpoints string
	pdClient         pdClient.Client

	// We only test `DefaultKeyspaceGroupID` here.
	// tsoAvailMembers is used to check the tso members which in the DefaultKeyspaceGroupID.
	tsoAvailMembers map[string]bool
	tsoNodes        map[string]bs.Server
	schedulingNodes map[string]bs.Server
}

func TestMemberTestSuite(t *testing.T) {
	suite.Run(t, new(memberTestSuite))
}

func (suite *memberTestSuite) SetupTest() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	ctx, cancel := context.WithCancel(context.Background())
	suite.ctx = ctx
	cluster, err := tests.NewTestPDServiceCluster(suite.ctx, 1)
	suite.cluster = cluster
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	re.NoError(suite.server.BootstrapCluster())
	suite.backendEndpoints = suite.server.GetAddr()
	suite.pdClient = pdClient.NewClient("mcs-member-test", []string{suite.server.GetAddr()})

	// TSO
	nodes := make(map[string]bs.Server)
	// mock 3 tso nodes, which is more than the default replica count(DefaultKeyspaceGroupReplicaCount).
	for range 3 {
		s, cleanup := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		nodes[s.GetAddr()] = s
		suite.cleanupFunc = append(suite.cleanupFunc, func() {
			cleanup()
		})
	}
	primary := tests.WaitForPrimaryServing(re, nodes)
	members := mustGetKeyspaceGroupMembers(re, nodes[primary].(*tso.Server))
	// Get the tso nodes
	suite.tsoNodes = nodes
	// We only test `DefaultKeyspaceGroupID` here.
	// tsoAvailMembers is used to check the tso members which in the DefaultKeyspaceGroupID.
	suite.tsoAvailMembers = make(map[string]bool)
	for _, member := range members[constant.DefaultKeyspaceGroupID].Group.Members {
		suite.tsoAvailMembers[member.Address] = true
	}

	// Scheduling
	nodes = make(map[string]bs.Server)
	for range 3 {
		s, cleanup := tests.StartSingleSchedulingTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		nodes[s.GetAddr()] = s
		suite.cleanupFunc = append(suite.cleanupFunc, func() {
			cleanup()
		})
	}
	tests.WaitForPrimaryServing(re, nodes)
	suite.schedulingNodes = nodes

	suite.cleanupFunc = append(suite.cleanupFunc, func() {
		cancel()
	})
}

func (suite *memberTestSuite) TearDownTest() {
	for _, cleanup := range suite.cleanupFunc {
		cleanup()
	}
	if suite.pdClient != nil {
		suite.pdClient.Close()
	}
	suite.cluster.Destroy()
	re := suite.Require()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes"))
}

func (suite *memberTestSuite) TestMembers() {
	re := suite.Require()
	members, err := suite.pdClient.GetMicroserviceMembers(suite.ctx, "tso")
	re.NoError(err)
	re.Len(members, 3)

	members, err = suite.pdClient.GetMicroserviceMembers(suite.ctx, "scheduling")
	re.NoError(err)
	re.Len(members, 3)
}

func (suite *memberTestSuite) TestPrimary() {
	re := suite.Require()
	primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, "tso")
	re.NoError(err)
	re.NotEmpty(primary)

	primary, err = suite.pdClient.GetMicroservicePrimary(suite.ctx, "scheduling")
	re.NoError(err)
	re.NotEmpty(primary)
}

func (suite *memberTestSuite) TestPrimaryWorkWhileOtherServerClose() {
	re := suite.Require()
	primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, "tso")
	re.NoError(err)
	re.NotEmpty(primary)

	supportedServices := []string{"tso", "scheduling"}
	for _, service := range supportedServices {
		var nodes map[string]bs.Server
		switch service {
		case "tso":
			nodes = suite.tsoNodes
		case "scheduling":
			nodes = suite.schedulingNodes
		}

		primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)

		// Close non-primary node.
		for _, member := range nodes {
			if member.GetAddr() != primary {
				nodes[member.Name()].Close()
			}
		}
		tests.WaitForPrimaryServing(re, nodes)

		// primary should be same with before.
		curPrimary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)
		re.Equal(primary, curPrimary)
	}
}

func (suite *memberTestSuite) TestTransferPrimary() {
	re := suite.Require()
	supportedServices := []string{"tso", "scheduling"}
	for _, service := range supportedServices {
		var nodes map[string]bs.Server
		switch service {
		case "tso":
			nodes = suite.tsoNodes
		case "scheduling":
			nodes = suite.schedulingNodes
		}

		// Test resign primary by random
		primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)

		newPrimaryData := make(map[string]any)
		newPrimaryData["new_primary"] = ""
		data, _ := json.Marshal(newPrimaryData)
		resp, err := tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, service),
			"application/json", bytes.NewBuffer(data))
		re.NoError(err)
		re.Equal(http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		testutil.Eventually(re, func() bool {
			for _, member := range nodes {
				if member.GetAddr() != primary && member.IsServing() {
					return true
				}
			}
			return false
		}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

		primary, err = suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)

		// Test transfer primary to a specific node
		var newPrimary string
		for _, member := range nodes {
			if service == "tso" && !suite.tsoAvailMembers[member.GetAddr()] {
				continue
			}
			if member.GetAddr() != primary {
				newPrimary = member.Name()
				break
			}
		}
		newPrimaryData["new_primary"] = newPrimary
		data, _ = json.Marshal(newPrimaryData)
		resp, err = tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, service),
			"application/json", bytes.NewBuffer(data))
		re.NoError(err)
		re.Equal(http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		testutil.Eventually(re, func() bool {
			return nodes[newPrimary].IsServing()
		}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

		primary, err = suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)
		re.Equal(primary, newPrimary)

		// Test transfer primary to a non-exist node
		newPrimary = "http://"
		newPrimaryData["new_primary"] = newPrimary
		data, _ = json.Marshal(newPrimaryData)
		resp, err = tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, service),
			"application/json", bytes.NewBuffer(data))
		re.NoError(err)
		re.Equal(http.StatusInternalServerError, resp.StatusCode)
		resp.Body.Close()
	}
}

func (suite *memberTestSuite) TestCampaignPrimaryAfterTransfer() {
	re := suite.Require()
	supportedServices := []string{"tso", "scheduling"}
	for _, service := range supportedServices {
		var nodes map[string]bs.Server
		switch service {
		case "tso":
			nodes = suite.tsoNodes
		case "scheduling":
			nodes = suite.schedulingNodes
		}

		primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)

		// Test transfer primary to a specific node
		var newPrimary string
		for _, member := range nodes {
			if service == "tso" && !suite.tsoAvailMembers[member.GetAddr()] {
				continue
			}
			if member.GetAddr() != primary {
				newPrimary = member.Name()
				break
			}
		}
		newPrimaryData := make(map[string]any)
		newPrimaryData["new_primary"] = newPrimary
		data, _ := json.Marshal(newPrimaryData)
		resp, err := tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, service),
			"application/json", bytes.NewBuffer(data))
		re.NoError(err)
		re.Equal(http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		tests.WaitForPrimaryServing(re, nodes)
		newPrimary, err = suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)
		re.NotEqual(primary, newPrimary)

		// Close primary to push other nodes campaign primary
		nodes[newPrimary].Close()
		tests.WaitForPrimaryServing(re, nodes)
		// Primary should be different with before
		anotherPrimary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)
		re.NotEqual(newPrimary, anotherPrimary)
	}
}

func (suite *memberTestSuite) TestTransferPrimaryWhileLeaseExpired() {
	re := suite.Require()
	primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, "tso")
	re.NoError(err)
	re.NotEmpty(primary)

	supportedServices := []string{"tso", "scheduling"}
	for _, service := range supportedServices {
		var nodes map[string]bs.Server
		switch service {
		case "tso":
			nodes = suite.tsoNodes
		case "scheduling":
			nodes = suite.schedulingNodes
		}

		primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)

		// Test transfer primary to a specific node
		var newPrimary string
		for _, member := range nodes {
			if service == "tso" && !suite.tsoAvailMembers[member.GetAddr()] {
				continue
			}
			if member.GetAddr() != primary {
				newPrimary = member.Name()
				break
			}
		}
		// Mock the new primary can not grant leader which means the lease will expire
		re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/election/skipGrantLeader", `return()`))
		newPrimaryData := make(map[string]any)
		newPrimaryData["new_primary"] = newPrimary
		data, _ := json.Marshal(newPrimaryData)
		resp, err := tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, service),
			"application/json", bytes.NewBuffer(data))
		re.NoError(err)
		re.Equal(http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		// Wait for the old primary exit and new primary campaign
		// cannot check newPrimary isServing when skipGrantLeader is enabled
		testutil.Eventually(re, func() bool {
			return !nodes[primary].IsServing()
		}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

		// TODO: Add campaign times check in mcs to avoid frequent campaign
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/election/skipGrantLeader"))
		// Can still work after lease expired
		tests.WaitForPrimaryServing(re, nodes)
	}
}

// TestTransferPrimaryWhileLeaseExpiredAndServerDown tests transfer primary while lease expired and server down
func (suite *memberTestSuite) TestTransferPrimaryWhileLeaseExpiredAndServerDown() {
	re := suite.Require()
	primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, "tso")
	re.NoError(err)
	re.NotEmpty(primary)

	supportedServices := []string{"tso", "scheduling"}
	for _, service := range supportedServices {
		var nodes map[string]bs.Server
		switch service {
		case "tso":
			nodes = suite.tsoNodes
		case "scheduling":
			nodes = suite.schedulingNodes
		}

		primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)

		// Test transfer primary to a specific node
		var newPrimary string
		for _, member := range nodes {
			if service == "tso" && !suite.tsoAvailMembers[member.GetAddr()] {
				continue
			}
			if member.GetAddr() != primary {
				newPrimary = member.Name()
				break
			}
		}
		// Mock the new primary can not grant leader which means the lease will expire
		re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/election/skipGrantLeader", `return()`))
		newPrimaryData := make(map[string]any)
		newPrimaryData["new_primary"] = ""
		data, _ := json.Marshal(newPrimaryData)
		resp, err := tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, service),
			"application/json", bytes.NewBuffer(data))
		re.NoError(err)
		re.Equal(http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		// Wait for the old primary exit and new primary campaign
		// cannot check newPrimary isServing when skipGrantLeader is enabled
		testutil.Eventually(re, func() bool {
			return !nodes[primary].IsServing()
		}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

		// TODO: Add campaign times check in mcs to avoid frequent campaign
		// for now, close the current primary to mock the server down
		nodes[newPrimary].Close()
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/election/skipGrantLeader"))

		tests.WaitForPrimaryServing(re, nodes)
		// Primary should be different with before
		onlyPrimary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)
		re.NotEqual(newPrimary, onlyPrimary)
	}
}

func mustGetKeyspaceGroupMembers(re *require.Assertions, server *tso.Server) map[uint32]*apis.KeyspaceGroupMember {
	httpReq, err := http.NewRequest(http.MethodGet, server.GetAddr()+"/tso/api/v1/keyspace-groups/members", nil)
	re.NoError(err)
	httpResp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer httpResp.Body.Close()
	data, err := io.ReadAll(httpResp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, httpResp.StatusCode, string(data))
	var resp map[uint32]*apis.KeyspaceGroupMember
	re.NoError(json.Unmarshal(data, &resp))
	return resp
}
