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

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

type metaServiceGroupTestSuite struct {
	suite.Suite
	cleanup func()
	cluster *tests.TestCluster
	server  *tests.TestServer
}

func TestMetaServiceGroupTestSuite(t *testing.T) {
	suite.Run(t, new(metaServiceGroupTestSuite))
}

func mockMetaServiceGroups() map[string]string {
	return map[string]string{
		"etcd-group-0": "etcd-group-0.tidb-serverless.cluster.svc.local",
		"etcd-group-1": "etcd-group-1.tidb-serverless.cluster.svc.local",
		"etcd-group-2": "etcd-group-2.tidb-serverless.cluster.svc.local",
	}
}

func (suite *metaServiceGroupTestSuite) SetupTest() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Keyspace.MetaServiceGroups = mockMetaServiceGroups()
		conf.Keyspace.WaitRegionSplit = false
	})
	suite.cluster = cluster
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	re.NoError(suite.server.BootstrapCluster())
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
}

func (suite *metaServiceGroupTestSuite) TearDownTest() {
	re := suite.Require()
	suite.cleanup()
	suite.cluster.Destroy()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
}

func collectStatus(re *require.Assertions, keyspaces []*keyspacepb.KeyspaceMeta) map[string]*handlers.MetaServiceGroupStatus {
	collectedStatuses := make(map[string]*handlers.MetaServiceGroupStatus)
	for _, meta := range keyspaces {
		id := meta.GetConfig()[keyspace.MetaServiceGroupIDKey]
		addresses := meta.GetConfig()[keyspace.MetaServiceGroupAddressesKey]
		re.NotEmpty(id)
		if collectedStatuses[id] == nil {
			collectedStatuses[id] = &handlers.MetaServiceGroupStatus{
				ID:                id,
				Addresses:         addresses,
				AssignedKeyspaces: 1,
			}
		} else {
			re.Equal(id, collectedStatuses[id].ID)
			re.Equal(addresses, collectedStatuses[id].Addresses)
			collectedStatuses[id].AssignedKeyspaces++
		}
	}
	return collectedStatuses
}

// TestUpdateMetaServiceGroupsViaConfigAPI verifies that changing
// meta-service-groups through the generic /config API is routed through the
// MetaServiceGroupManager (UpdateGroupsSafely) rather than directly mutating the
// keyspace manager. The /config payload is merged into the existing config, so
// groups can be added or updated, and the change is reflected by the v2 API.
func (suite *metaServiceGroupTestSuite) TestUpdateMetaServiceGroupsViaConfigAPI() {
	re := suite.Require()
	// Adding a new group through /config should succeed and be visible via v2 API.
	added := mockMetaServiceGroups()
	added["etcd-group-x"] = "etcd-group-x.example.local"
	code, body := suite.setMetaServiceGroupsViaConfig(re, added)
	re.Equal(http.StatusOK, code, body)
	groups := mustLoadMetaServiceGroups(re, suite.server)
	re.Len(groups, len(added))
	var x *handlers.MetaServiceGroupStatus
	for _, group := range groups {
		if group.ID == "etcd-group-x" {
			x = group
		}
	}
	re.NotNil(x, "etcd-group-x should be added via /config")
	re.Equal("etcd-group-x.example.local", x.Addresses)

	// Updating an existing group's address through /config should also work.
	added["etcd-group-x"] = "etcd-group-x-modified.example.local"
	code, body = suite.setMetaServiceGroupsViaConfig(re, added)
	re.Equal(http.StatusOK, code, body)
	groups = mustLoadMetaServiceGroups(re, suite.server)
	for _, group := range groups {
		if group.ID == "etcd-group-x" {
			re.Equal("etcd-group-x-modified.example.local", group.Addresses)
		}
	}
}

func (suite *metaServiceGroupTestSuite) setMetaServiceGroupsViaConfig(re *require.Assertions, groups map[string]string) (int, string) {
	payload, err := json.Marshal(map[string]any{"keyspace.meta-service-groups": groups})
	re.NoError(err)
	httpReq, err := http.NewRequest(http.MethodPost, suite.server.GetAddr()+"/pd/api/v1/config", bytes.NewBuffer(payload))
	re.NoError(err)
	resp, err := tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	re.NoError(err)
	return resp.StatusCode, string(data)
}

func (suite *metaServiceGroupTestSuite) TestMetaServiceGroupOperations() {
	re := suite.Require()
	// Default keyspace must not contain any meta-service group config.
	defaultKeyspace := mustLoadKeyspaces(re, suite.server, keyspace.GetBootstrapKeyspaceName())
	re.NotContains(defaultKeyspace.GetConfig(), keyspace.MetaServiceGroupIDKey)
	re.NotContains(defaultKeyspace.GetConfig(), keyspace.MetaServiceGroupAddressesKey)
	// Create keyspaces and collect their meta-service group configs.
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 20)
	collectedGroups := collectStatus(re, keyspaces)
	// Make sure result collected from keyspace config and load meta-service group api matches.
	groups := mustLoadMetaServiceGroups(re, suite.server)
	re.Len(groups, len(collectedGroups))
	for _, group := range groups {
		collectedStatus := collectedGroups[group.ID]
		re.Equal(collectedStatus.ID, group.ID)
		re.Equal(collectedStatus.Addresses, group.Addresses)
		re.Equal(collectedStatus.AssignedKeyspaces, group.AssignedKeyspaces)
		// Make sure keyspaces are relatively evenly distributed among meta-service groups.
		re.InDelta(collectedStatus.AssignedKeyspaces, len(keyspaces)/len(groups), 1)
	}
	// Add two more meta-service groups.
	addr4 := "etcd-group-4.tidb-serverless.cluster.svc.local"
	addr5 := "etcd-group-5.tidb-serverless.cluster.svc.local"
	patch := map[string]*string{
		"etcd-group-4": &addr4,
		"etcd-group-5": &addr5,
	}

	groups = mustPatchMetaServiceGroups(re, suite.server, patch)
	re.Equal(len(groups), len(mockMetaServiceGroups())+len(patch))
	// Newly assigned meta-service group should have no assigned keyspace.
	for _, group := range groups {
		if collectedGroups[group.ID] == nil {
			re.Zero(group.AssignedKeyspaces)
		}
	}
	// Create more keyspaces and check that newly added meta-service groups are used.
	keyspaces = append(keyspaces, mustMakeTestKeyspaces(re, suite.server, 40)...)
	collectedGroups = collectStatus(re, keyspaces)
	groups = mustLoadMetaServiceGroups(re, suite.server)
	for _, group := range groups {
		collectedStatus := collectedGroups[group.ID]
		re.Equal(collectedStatus.ID, group.ID)
		re.Equal(collectedStatus.Addresses, group.Addresses)
		re.Equal(collectedStatus.AssignedKeyspaces, group.AssignedKeyspaces)
		// Make sure keyspaces are relatively evenly distributed among meta-service groups.
		re.InDelta(collectedStatus.AssignedKeyspaces, len(keyspaces)/len(groups), 1)
	}
	// Modify address of etcd-group-1
	newAddr := "etcd-group-1-modified.tidb-serverless.cluster.svc.local"
	modifyPatch := map[string]*string{
		"etcd-group-1": &newAddr,
	}
	groups = mustPatchMetaServiceGroups(re, suite.server, modifyPatch)
	found := false
	for _, group := range groups {
		if group.ID == "etcd-group-1" {
			found = true
			re.Equal(newAddr, group.Addresses)
		}
	}
	re.True(found, "etcd-group-1 should exist after modify")

	// Deleting a group with assigned keyspaces should be rejected.
	rejectPatch := map[string]*string{
		"etcd-group-2": nil,
	}
	mustPatchMetaServiceGroupsFail(re, suite.server, rejectPatch)

	// Deleting etcd-group-4 should also be rejected once it has assigned keyspaces.
	deletePatch := map[string]*string{
		"etcd-group-4": nil,
	}
	mustPatchMetaServiceGroupsFail(re, suite.server, deletePatch)

	// A top-level JSON null body should be rejected rather than treated as a no-op.
	mustPatchMetaServiceGroupsRawFail(re, suite.server, []byte("null"))

	// Duplicated group IDs after normalization should be rejected.
	normalizedDuplicateAddr := "etcd-group-6.tidb-serverless.cluster.svc.local"
	normalizedDuplicatePatch := map[string]*string{
		"etcd-group-6":   &normalizedDuplicateAddr,
		" etcd-group-6 ": nil, //nolint:gocritic // The whitespace is intentional to verify ID normalization.
	}
	mustPatchMetaServiceGroupsFail(re, suite.server, normalizedDuplicatePatch)

	// Delete a newly-added group with no assigned keyspaces.
	unusedAddr := "etcd-group-unused.tidb-serverless.cluster.svc.local"
	groups = mustPatchMetaServiceGroups(re, suite.server, map[string]*string{
		"etcd-group-unused": &unusedAddr,
	})
	re.Len(groups, len(collectedGroups)+1)
	groups = mustPatchMetaServiceGroups(re, suite.server, map[string]*string{
		"etcd-group-unused": nil,
	})
	for _, group := range groups {
		re.NotEqual("etcd-group-unused", group.ID)
	}
}
