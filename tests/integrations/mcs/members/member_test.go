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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"

	pdClient "github.com/tikv/pd/client/http"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/keyspace/constant"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	"github.com/tikv/pd/pkg/mcs/tso/server/apis/v1"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
	handlersutil "github.com/tikv/pd/tests/server/apiv2/handlers"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

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
	tsoAvailMembers      map[string]bool
	tsoNodes             map[string]bs.Server
	schedulingNodes      map[string]bs.Server
	resourceManagerNodes map[string]bs.Server
}

func TestMemberTestSuite(t *testing.T) {
	suite.Run(t, new(memberTestSuite))
}

func (suite *memberTestSuite) SetupTest() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/acceleratedAllocNodes", `return(true)`))
	ctx, cancel := context.WithCancel(context.Background())
	suite.ctx = ctx
	cluster, err := tests.NewTestClusterWithKeyspaceGroup(suite.ctx, 1)
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

	// resource manager
	nodes = make(map[string]bs.Server)
	for range 3 {
		s, cleanup := tests.StartSingleResourceManagerTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
		nodes[s.GetAddr()] = s
		suite.cleanupFunc = append(suite.cleanupFunc, func() {
			cleanup()
		})
	}
	tests.WaitForPrimaryServing(re, nodes)
	suite.resourceManagerNodes = nodes

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

	members, err = suite.pdClient.GetMicroserviceMembers(suite.ctx, "resource_manager")
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

	primary, err = suite.pdClient.GetMicroservicePrimary(suite.ctx, "resource_manager")
	re.NoError(err)
	re.NotEmpty(primary)
}

func (suite *memberTestSuite) TestPrimaryWorkWhileOtherServerClose() {
	re := suite.Require()
	primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, "tso")
	re.NoError(err)
	re.NotEmpty(primary)

	supportedServices := []string{"tso", "scheduling", "resource_manager"}
	for _, service := range supportedServices {
		var nodes map[string]bs.Server
		switch service {
		case "tso":
			nodes = suite.tsoNodes
		case "scheduling":
			nodes = suite.schedulingNodes
		case "resource_manager":
			nodes = suite.resourceManagerNodes
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
	supportedServices := []string{"tso", "scheduling", "resource_manager"}
	for _, service := range supportedServices {
		var nodes map[string]bs.Server
		switch service {
		case "tso":
			nodes = suite.tsoNodes
		case "scheduling":
			nodes = suite.schedulingNodes
		case "resource_manager":
			nodes = suite.resourceManagerNodes
		}

		// Test resign primary by random
		primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, service)
		re.NoError(err)

		newPrimaryData := make(map[string]any)
		newPrimaryData["new_primary"] = ""
		data, err := json.Marshal(newPrimaryData)
		re.NoError(err)
		resp, err := tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, strings.ReplaceAll(service, "_", "-")),
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
		data, err = json.Marshal(newPrimaryData)
		re.NoError(err)
		resp, err = tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, strings.ReplaceAll(service, "_", "-")),
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
		data, err = json.Marshal(newPrimaryData)
		re.NoError(err)
		resp, err = tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, strings.ReplaceAll(service, "_", "-")),
			"application/json", bytes.NewBuffer(data))
		re.NoError(err)
		re.Equal(http.StatusInternalServerError, resp.StatusCode)
		resp.Body.Close()
	}
}

func (suite *memberTestSuite) TestCampaignPrimaryAfterTransfer() {
	re := suite.Require()
	supportedServices := []string{"tso", "scheduling", "resource_manager"}
	for _, service := range supportedServices {
		var nodes map[string]bs.Server
		switch service {
		case "tso":
			nodes = suite.tsoNodes
		case "scheduling":
			nodes = suite.schedulingNodes
		case "resource_manager":
			nodes = suite.resourceManagerNodes
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
		data, err := json.Marshal(newPrimaryData)
		re.NoError(err)
		resp, err := tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, strings.ReplaceAll(service, "_", "-")),
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

	supportedServices := []string{"tso", "scheduling", "resource_manager"}
	for _, service := range supportedServices {
		var nodes map[string]bs.Server
		switch service {
		case "tso":
			nodes = suite.tsoNodes
		case "scheduling":
			nodes = suite.schedulingNodes
		case "resource_manager":
			nodes = suite.resourceManagerNodes
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
		data, err := json.Marshal(newPrimaryData)
		re.NoError(err)
		resp, err := tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, strings.ReplaceAll(service, "_", "-")),
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

	supportedServices := []string{"tso", "scheduling", "resource_manager"}
	for _, service := range supportedServices {
		var nodes map[string]bs.Server
		switch service {
		case "tso":
			nodes = suite.tsoNodes
		case "scheduling":
			nodes = suite.schedulingNodes
		case "resource_manager":
			nodes = suite.resourceManagerNodes
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
		// Transfer to `newPrimary` explicitly (not a random target), so the expected
		// primary flag deterministically points at the node we close below. This is
		// what makes this test actually exercise the marker-TTL-expiry recovery path.
		newPrimaryData := make(map[string]any)
		newPrimaryData["new_primary"] = newPrimary
		data, err := json.Marshal(newPrimaryData)
		re.NoError(err)
		resp, err := tests.TestDialClient.Post(fmt.Sprintf("%s/%s/api/v1/primary/transfer", primary, strings.ReplaceAll(service, "_", "-")),
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
		// for now, close the transfer target to mock the server down
		nodes[newPrimary].Close()
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/election/skipGrantLeader"))

		// The transfer target (which we just closed) is the one named in the expected
		// primary flag, so the other replicas back off until the flag's TTL (a few
		// leader leases) expires, after which a free re-election elects a live primary.
		// Wait long enough to cover that worst case.
		recoverWait := time.Duration(mcs.TransferPrimaryLeaseMultiplier*mcs.DefaultLease)*time.Second + 10*time.Second
		serving := make([]string, 0, len(nodes))
		testutil.Eventually(re, func() bool {
			serving = serving[:0]
			for name, s := range nodes {
				if s.IsServing() {
					serving = append(serving, name)
				}
			}
			return len(serving) == 1 && serving[0] != newPrimary
		}, testutil.WithWaitFor(recoverWait), testutil.WithTickInterval(50*time.Millisecond))
		// Exactly one node serves and it is not the one we closed.
		re.Len(serving, 1)
		re.NotEqual(newPrimary, serving[0])
	}
}

// mustSetupKeyspaceGroupWithoutKeyspaces creates a keyspace group that has no
// keyspaces assigned, allocates TSO nodes for it, and waits until one of them
// becomes the primary. It returns the primary's address.
func (suite *memberTestSuite) mustSetupKeyspaceGroupWithoutKeyspaces(re *require.Assertions, groupID uint32) string {
	// Create the keyspace group without members first, then use AllocNodes so that
	// TSO nodes discover the group via etcd watch and elect a primary.
	handlersutil.MustCreateKeyspaceGroup(re, suite.server, &handlers.CreateKeyspaceGroupParams{
		KeyspaceGroups: []*endpoint.KeyspaceGroup{
			{
				ID:       groupID,
				UserKind: endpoint.Standard.String(),
			},
		},
	})

	// Allocate nodes for the group via PD's AllocNodes API so TSO nodes can
	// discover their membership through etcd and start campaigning for primary.
	allocBody, err := json.Marshal(&handlers.AllocNodesForKeyspaceGroupParams{
		Replica: mcs.DefaultKeyspaceGroupReplicaCount,
	})
	re.NoError(err)
	allocReq, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/pd/api/v2/tso/keyspace-groups/%d/alloc", suite.backendEndpoints, groupID),
		bytes.NewBuffer(allocBody))
	re.NoError(err)
	allocResp, err := tests.TestDialClient.Do(allocReq)
	re.NoError(err)
	re.Equal(http.StatusOK, allocResp.StatusCode)
	allocResp.Body.Close()

	// Wait until the group is loaded on a TSO node and has elected a primary.
	var groupPrimary string
	testutil.Eventually(re, func() bool {
		for _, s := range suite.tsoNodes {
			tsoSvr := s.(*tso.Server)
			members := mustGetKeyspaceGroupMembers(re, tsoSvr)
			if m, ok := members[groupID]; ok && m.IsPrimary {
				groupPrimary = tsoSvr.GetAddr()
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(30*time.Second), testutil.WithTickInterval(200*time.Millisecond))
	re.NotEmpty(groupPrimary)
	return groupPrimary
}

// TestKeyspaceGroupMembersWithoutKeyspaces verifies that the keyspace-groups
// members API returns a keyspace group that is served by the node but has no
// keyspaces assigned. This guards against regressing back to GetKeyspaceGroups(),
// which is built from the keyspace lookup table and would miss such a group, so
// its primary would never be observed through this API.
func (suite *memberTestSuite) TestKeyspaceGroupMembersWithoutKeyspaces() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	}()

	const testGroupID = uint32(1)
	groupPrimary := suite.mustSetupKeyspaceGroupWithoutKeyspaces(re, testGroupID)

	// The group with no keyspaces must be returned by the members API, and its
	// member list must not carry any keyspace.
	members := mustGetKeyspaceGroupMembers(re, suite.tsoNodes[groupPrimary].(*tso.Server))
	m, ok := members[testGroupID]
	re.True(ok)
	re.True(m.IsPrimary)
	re.Empty(m.Group.Keyspaces)
}

// TestTransferPrimaryWithKeyspaceGroup tests that the transfer primary API accepts
// a keyspace_group_id field and routes the transfer to the correct keyspace group.
func (suite *memberTestSuite) TestTransferPrimaryWithKeyspaceGroup() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	}()

	const testGroupID = uint32(1)
	groupPrimary := suite.mustSetupKeyspaceGroupWithoutKeyspaces(re, testGroupID)

	// Transfer primary within keyspace group 1 (random resign — new_primary left empty).
	body := map[string]any{
		"new_primary":       "",
		"keyspace_group_id": testGroupID,
	}
	data, err := json.Marshal(body)
	re.NoError(err)
	resp, err := tests.TestDialClient.Post(
		fmt.Sprintf("%s/tso/api/v1/primary/transfer", groupPrimary),
		"application/json", bytes.NewBuffer(data))
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Wait for the primary of group 1 to move to a different node.
	testutil.Eventually(re, func() bool {
		for _, s := range suite.tsoNodes {
			tsoSvr := s.(*tso.Server)
			members := mustGetKeyspaceGroupMembers(re, tsoSvr)
			if m, ok := members[testGroupID]; ok && m.IsPrimary {
				return tsoSvr.GetAddr() != groupPrimary
			}
		}
		return false
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(100*time.Millisecond))

	// Transfer primary to a specific node within keyspace group 1.
	var newPrimary string
	for _, s := range suite.tsoNodes {
		tsoSvr := s.(*tso.Server)
		members := mustGetKeyspaceGroupMembers(re, tsoSvr)
		if m, ok := members[testGroupID]; ok && m.IsPrimary {
			groupPrimary = tsoSvr.GetAddr()
		} else if _, ok := members[testGroupID]; ok {
			newPrimary = tsoSvr.Name()
		}
	}
	re.NotEmpty(newPrimary)

	body["new_primary"] = newPrimary
	body["keyspace_group_id"] = testGroupID
	data, err = json.Marshal(body)
	re.NoError(err)
	resp, err = tests.TestDialClient.Post(
		fmt.Sprintf("%s/tso/api/v1/primary/transfer", groupPrimary),
		"application/json", bytes.NewBuffer(data))
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Verify the specified node becomes primary.
	testutil.Eventually(re, func() bool {
		for _, s := range suite.tsoNodes {
			tsoSvr := s.(*tso.Server)
			if tsoSvr.Name() != newPrimary {
				continue
			}
			members := mustGetKeyspaceGroupMembers(re, tsoSvr)
			if m, ok := members[testGroupID]; ok && m.IsPrimary {
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(100*time.Millisecond))

	// Sending the transfer request to a non-primary member of the group should
	// be forwarded to the group's primary and still succeed.
	var nonPrimaryAddr string
	for _, s := range suite.tsoNodes {
		tsoSvr := s.(*tso.Server)
		members := mustGetKeyspaceGroupMembers(re, tsoSvr)
		if m, ok := members[testGroupID]; ok && !m.IsPrimary {
			nonPrimaryAddr = tsoSvr.GetAddr()
			break
		}
	}
	re.NotEmpty(nonPrimaryAddr)
	body["new_primary"] = ""
	body["keyspace_group_id"] = testGroupID
	data, err = json.Marshal(body)
	re.NoError(err)
	// The follower forwards the request to the group's primary. While a primary
	// election is in flight the primary address can be briefly unavailable, so
	// retry until the forwarded request succeeds.
	testutil.Eventually(re, func() bool {
		resp, err := tests.TestDialClient.Post(
			fmt.Sprintf("%s/tso/api/v1/primary/transfer", nonPrimaryAddr),
			"application/json", bytes.NewBuffer(data))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(200*time.Millisecond))

	// Requesting transfer for a non-existent keyspace group should return 400.
	body["keyspace_group_id"] = uint32(9999)
	data, err = json.Marshal(body)
	re.NoError(err)
	resp, err = tests.TestDialClient.Post(
		fmt.Sprintf("%s/tso/api/v1/primary/transfer", groupPrimary),
		"application/json", bytes.NewBuffer(data))
	re.NoError(err)
	re.Equal(http.StatusBadRequest, resp.StatusCode)
	resp.Body.Close()

	// An empty request body must be rejected instead of silently falling back to
	// a random transfer on the default keyspace group.
	resp, err = tests.TestDialClient.Post(
		fmt.Sprintf("%s/tso/api/v1/primary/transfer", groupPrimary),
		"application/json", http.NoBody)
	re.NoError(err)
	re.Equal(http.StatusBadRequest, resp.StatusCode)
	resp.Body.Close()
}

// TestTransferPrimaryWhileLeaseExpiredAndServerDownWithKeyspaceGroup covers the
// marker-TTL-expiry recovery path for a NON-default keyspace group (the default
// group is covered by TestTransferPrimaryWhileLeaseExpiredAndServerDown): transfer
// the group's primary to a specific member, take that member down before it can
// win, and verify the group recovers a live primary once the flag TTL expires.
func (suite *memberTestSuite) TestTransferPrimaryWhileLeaseExpiredAndServerDownWithKeyspaceGroup() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
	}()

	const testGroupID = uint32(1)
	groupPrimary := suite.mustSetupKeyspaceGroupWithoutKeyspaces(re, testGroupID)

	// Pick a non-primary member of the group as the explicit transfer target.
	var target, targetAddr string
	for _, s := range suite.tsoNodes {
		tsoSvr := s.(*tso.Server)
		members := mustGetKeyspaceGroupMembers(re, tsoSvr)
		if m, ok := members[testGroupID]; ok && !m.IsPrimary {
			target, targetAddr = tsoSvr.Name(), tsoSvr.GetAddr()
			break
		}
	}
	re.NotEmpty(target)

	// Prevent anyone from granting leadership so the transfer target cannot win.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/election/skipGrantLeader", `return()`))
	data, err := json.Marshal(map[string]any{"new_primary": target, "keyspace_group_id": testGroupID})
	re.NoError(err)
	resp, err := tests.TestDialClient.Post(
		fmt.Sprintf("%s/tso/api/v1/primary/transfer", groupPrimary),
		"application/json", bytes.NewBuffer(data))
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Wait until the old group primary steps down (group temporarily has no primary).
	testutil.Eventually(re, func() bool {
		for _, s := range suite.tsoNodes {
			tsoSvr := s.(*tso.Server)
			members := mustGetKeyspaceGroupMembers(re, tsoSvr)
			if m, ok := members[testGroupID]; ok && m.IsPrimary {
				return false
			}
		}
		return true
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	// Take the transfer target (the node named in the expected primary flag) down,
	// then allow campaigns again. The remaining group member backs off until the
	// flag TTL expires, after which a free re-election elects a live primary.
	suite.tsoNodes[targetAddr].Close()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/election/skipGrantLeader"))

	recoverWait := time.Duration(mcs.TransferPrimaryLeaseMultiplier*mcs.DefaultLease)*time.Second + 10*time.Second
	var recovered string
	testutil.Eventually(re, func() bool {
		recovered = ""
		for addr, s := range suite.tsoNodes {
			if addr == targetAddr {
				continue
			}
			tsoSvr := s.(*tso.Server)
			members := mustGetKeyspaceGroupMembers(re, tsoSvr)
			if m, ok := members[testGroupID]; ok && m.IsPrimary {
				recovered = tsoSvr.Name()
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(recoverWait), testutil.WithTickInterval(100*time.Millisecond))
	re.NotEmpty(recovered)
	re.NotEqual(target, recovered)
}

// TestTransferPrimaryToDefaultGroupFollower verifies that the transfer primary
// request for the default keyspace group is still forwarded from a follower to
// the primary and succeeds, preserving backward compatibility with the previous
// ServiceRedirector behavior.
func (suite *memberTestSuite) TestTransferPrimaryToDefaultGroupFollower() {
	re := suite.Require()

	primary, err := suite.pdClient.GetMicroservicePrimary(suite.ctx, "tso")
	re.NoError(err)
	re.NotEmpty(primary)

	// Pick a follower of the default keyspace group.
	var followerAddr string
	for addr := range suite.tsoAvailMembers {
		if addr != primary {
			followerAddr = addr
			break
		}
	}
	re.NotEmpty(followerAddr)

	// Omit keyspace_group_id so it defaults to the default keyspace group (0).
	// The follower should forward the request to the primary and succeed. Retry
	// to ride over any in-flight primary election.
	data, err := json.Marshal(map[string]any{"new_primary": ""})
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		resp, err := tests.TestDialClient.Post(
			fmt.Sprintf("%s/tso/api/v1/primary/transfer", followerAddr),
			"application/json", bytes.NewBuffer(data))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, testutil.WithWaitFor(10*time.Second), testutil.WithTickInterval(200*time.Millisecond))
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
