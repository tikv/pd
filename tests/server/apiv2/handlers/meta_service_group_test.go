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
	"context"
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
	newGroups := []*handlers.AddMetaServiceGroupRequest{
		{
			ID:        "etcd-group-4",
			Addresses: "etcd-group-4.tidb-serverless.cluster.svc.local",
		},
		{
			ID:        "etcd-group-5",
			Addresses: "etcd-group-5.tidb-serverless.cluster.svc.local",
		},
	}
	groups = mustAddMetaServiceGroups(re, suite.server, newGroups)
	re.Equal(len(groups), len(mockMetaServiceGroups())+len(newGroups))
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
	// Add the same keyspace group should result in error.
	code, _ := tryAddMetaServiceGroups(re, suite.server, newGroups)
	re.Equal(http.StatusBadRequest, code)

	// Empty fields should be rejected.
	code, _ = tryAddMetaServiceGroups(re, suite.server, []*handlers.AddMetaServiceGroupRequest{{
		ID:        "",
		Addresses: "etcd-group-6.tidb-serverless.cluster.svc.local",
	}})
	re.Equal(http.StatusBadRequest, code)

	// Duplicates within the same request should be rejected.
	code, _ = tryAddMetaServiceGroups(re, suite.server, []*handlers.AddMetaServiceGroupRequest{
		{
			ID:        "etcd-group-6",
			Addresses: "etcd-group-6.tidb-serverless.cluster.svc.local",
		},
		{
			ID:        "etcd-group-6",
			Addresses: "etcd-group-6-dup.tidb-serverless.cluster.svc.local",
		},
	})
	re.Equal(http.StatusBadRequest, code)
}
