// Copyright 2025 TiKV Project Authors.
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
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
)

type MetaServiceGroupIntegrationTestSuite struct {
	suite.Suite
	ctx      context.Context
	cancel   context.CancelFunc
	cluster  *tests.TestCluster
	pdLeader *tests.TestServer
	manager  *keyspace.MetaServiceGroupManager
}

func TestMetaServiceGroupIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(MetaServiceGroupIntegrationTestSuite))
}

func (suite *MetaServiceGroupIntegrationTestSuite) SetupSuite() {
	re := suite.Require()
	var err error

	// NewTestAPICluster creates a real PD cluster with embedded etcd.
	// The storage backend uses etcd (via NewStorageWithEtcdBackend in server.startServer),
	// so meta-service group data will be stored in etcd.
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	re.NotEmpty(leaderName)
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	re.NoError(suite.pdLeader.BootstrapCluster())

	// Verify that the server uses etcd storage
	// GetServer().GetClient() returns the etcd client, which confirms etcd is being used
	srv := suite.pdLeader.GetServer()
	etcdClient := srv.GetClient()
	re.NotNil(etcdClient, "server should have etcd client")
	re.NotEmpty(etcdClient.Endpoints(), "etcd client should have endpoints")

	// Configure meta-service groups
	keyspaceCfg := srv.GetConfig().Keyspace
	keyspaceCfg.MetaServiceGroups = mockMetaServiceGroups()
	keyspaceCfg.AutoAssignMetaServiceGroups = true
	keyspaceCfg.MetaServiceGroupsFallbackRatio = 0
	err = srv.SetKeyspaceConfig(keyspaceCfg)
	re.NoError(err)

	suite.manager = srv.GetMetaServiceGroupManager()
	re.NotNil(suite.manager)
}

func (suite *MetaServiceGroupIntegrationTestSuite) TearDownSuite() {
	if suite.cluster != nil {
		suite.cluster.Destroy()
	}
	if suite.cancel != nil {
		suite.cancel()
	}
}

// TearDownTest cleans up resources after each test to prevent test interference.
func (suite *MetaServiceGroupIntegrationTestSuite) TearDownTest() {
	// Reset assignment count for all meta-service groups to 0
	// This ensures tests don't interfere with each other
	if suite.manager == nil {
		return
	}
	re := suite.Require()
	for groupID := range mockMetaServiceGroups() {
		zero := 0
		err := suite.manager.PatchStatus(groupID, &keyspace.MetaServiceGroupStatusPatch{
			AssignedCount: &zero,
		})
		// Log error but don't fail the test if cleanup fails
		// This can happen if the test setup failed
		if err != nil {
			re.NoError(err, "failed to reset assignment count for group %s", groupID)
		}
	}
}

func mockMetaServiceGroups() map[string]string {
	return map[string]string{
		"etcd-group-0": "etcd-group-0.tidb-serverless.cluster.svc.local",
		"etcd-group-1": "etcd-group-1.tidb-serverless.cluster.svc.local",
		"etcd-group-2": "etcd-group-2.tidb-serverless.cluster.svc.local",
	}
}

// setupConcurrentAssignTest prepares the test environment for concurrent assignment tests.
func (suite *MetaServiceGroupIntegrationTestSuite) setupConcurrentAssignTest() {
	re := suite.Require()
	// Enable all meta-service groups
	for groupID := range mockMetaServiceGroups() {
		enable := true
		re.NoError(suite.manager.PatchStatus(groupID, &keyspace.MetaServiceGroupStatusPatch{
			Enabled: &enable,
		}))
	}
}

// runConcurrentAssignToGroup concurrently calls AssignToGroup and returns results.
func (suite *MetaServiceGroupIntegrationTestSuite) runConcurrentAssignToGroup(concurrentCount, assignCount int) []string {
	var wg sync.WaitGroup
	results := make([]string, concurrentCount)
	for i := range concurrentCount {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index] = suite.manager.AssignToGroup(assignCount)
		}(i)
	}
	wg.Wait()
	return results
}

// getTotalAssignedCount returns the total assignment count from all meta-service groups.
func (suite *MetaServiceGroupIntegrationTestSuite) getTotalAssignedCount() (int, error) {
	statusMap, err := suite.manager.GetStatus()
	if err != nil {
		return 0, err
	}
	totalAssigned := 0
	for _, status := range statusMap {
		totalAssigned += status.AssignmentCount
	}
	return totalAssigned, nil
}

// TestAssignToGroupConcurrent tests that concurrent calls to AssignToGroup
// keep in-memory assignment counts consistent.
func (suite *MetaServiceGroupIntegrationTestSuite) TestAssignToGroupConcurrent() {
	re := suite.Require()
	suite.setupConcurrentAssignTest()

	// Concurrently call AssignToGroup multiple times
	concurrentCount := 50
	assignCount := 1
	results := suite.runConcurrentAssignToGroup(concurrentCount, assignCount)
	// All result should be from mockMetaServiceGroups
	for _, result := range results {
		_, ok := mockMetaServiceGroups()[result]
		re.True(ok, "result should be from mockMetaServiceGroups")
	}

	// Verify the total assignment count is correct
	totalAssigned, err := suite.getTotalAssignedCount()
	re.NoError(err)
	re.Equal(concurrentCount*assignCount, totalAssigned,
		"total assignment count should equal the sum of all concurrent assignments")
}

// TestRefreshCacheReloadsFromStorage verifies cache reload uses storage data.
func (suite *MetaServiceGroupIntegrationTestSuite) TestRefreshCacheReloadsFromStorage() {
	re := suite.Require()
	// Enable groups and set initial in-memory status.
	suite.setupConcurrentAssignTest()

	assignedCount := 1
	disabled := false
	// Set initial status for group-0 in cache.
	re.NoError(suite.manager.PatchStatus("etcd-group-0", &keyspace.MetaServiceGroupStatusPatch{
		AssignedCount: &assignedCount,
		Enabled:       &disabled,
	}))

	// Overwrite status in storage.
	storage := suite.pdLeader.GetServer().GetStorage()
	expected := &endpoint.MetaServiceGroupStatus{
		AssignmentCount: 7,
		Enabled:         true,
	}
	err := storage.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return storage.SaveMetaServiceGroupStatus(txn, "etcd-group-0", expected)
	})
	re.NoError(err)

	// Refresh cache and verify it reflects storage.
	re.NoError(suite.manager.RefreshCache())
	statusMap, err := suite.manager.GetStatus()
	re.NoError(err)
	re.Equal(expected.AssignmentCount, statusMap["etcd-group-0"].AssignmentCount)
	re.Equal(expected.Enabled, statusMap["etcd-group-0"].Enabled)
}

// TestRefreshCacheAfterLeaderTransfer verifies cache loads after leadership change.
func TestRefreshCacheAfterLeaderTransfer(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start a two-node cluster and bootstrap leader.
	cluster, err := tests.NewTestAPICluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())

	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	leaderServer := cluster.GetServer(leaderName)
	re.NoError(leaderServer.BootstrapCluster())

	// Configure meta-service groups on the leader.
	srv := leaderServer.GetServer()
	keyspaceCfg := srv.GetConfig().Keyspace
	keyspaceCfg.MetaServiceGroups = mockMetaServiceGroups()
	keyspaceCfg.AutoAssignMetaServiceGroups = true
	keyspaceCfg.MetaServiceGroupsFallbackRatio = 0
	re.NoError(srv.SetKeyspaceConfig(keyspaceCfg))

	// Enable all groups for assignment updates.
	for groupID := range mockMetaServiceGroups() {
		enable := true
		re.NoError(srv.GetMetaServiceGroupManager().PatchStatus(groupID, &keyspace.MetaServiceGroupStatusPatch{
			Enabled: &enable,
		}))
	}

	// Seed storage with expected status on leader.
	storage := srv.GetStorage()
	expected := &endpoint.MetaServiceGroupStatus{
		AssignmentCount: 7,
		Enabled:         true,
	}
	re.NoError(storage.RunInTxn(ctx, func(txn kv.Txn) error {
		return storage.SaveMetaServiceGroupStatus(txn, "etcd-group-0", expected)
	}))

	// Pick a follower and verify it does not yet see expected status.
	followerName := ""
	for name := range cluster.GetServers() {
		if name != leaderName {
			followerName = name
			break
		}
	}
	re.NotEmpty(followerName)
	followerServer := cluster.GetServer(followerName).GetServer()
	followerStatus, err := followerServer.GetMetaServiceGroupManager().GetStatus()
	re.NoError(err)
	if status, ok := followerStatus["etcd-group-0"]; ok && status != nil {
		re.NotEqual(expected.AssignmentCount, status.AssignmentCount)
	}

	// Transfer leadership and wait for cache refresh on new leader.
	re.NoError(leaderServer.ResignLeader())
	newLeaderName := cluster.WaitLeader()
	re.NotEmpty(newLeaderName)
	re.NotEqual(leaderName, newLeaderName)
	newLeaderServer := cluster.GetServer(newLeaderName).GetServer()

	// Eventually the new leader should load expected status.
	testutil.Eventually(re, func() bool {
		statusMap, err := newLeaderServer.GetMetaServiceGroupManager().GetStatus()
		if err != nil {
			return false
		}
		status := statusMap["etcd-group-0"]
		if status == nil {
			return false
		}
		return status.AssignmentCount == expected.AssignmentCount && status.Enabled == expected.Enabled
	})
}
