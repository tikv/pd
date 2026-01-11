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

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/keyspace"
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

// runConcurrentAssignToGroup concurrently calls AssignToGroup and returns results and errors.
func (suite *MetaServiceGroupIntegrationTestSuite) runConcurrentAssignToGroup(concurrentCount, assignCount int) ([]string, []error) {
	var wg sync.WaitGroup
	results := make([]string, concurrentCount)
	errors := make([]error, concurrentCount)

	for i := range concurrentCount {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			assigned, err := suite.manager.AssignToGroup(assignCount)
			results[index] = assigned
			errors[index] = err
		}(i)
	}
	wg.Wait()
	return results, errors
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
// are serialized to avoid etcd txn conflicts.
func (suite *MetaServiceGroupIntegrationTestSuite) TestAssignToGroupConcurrent() {
	re := suite.Require()
	suite.setupConcurrentAssignTest()

	// Concurrently call AssignToGroup multiple times
	concurrentCount := 50
	assignCount := 1
	results, errors := suite.runConcurrentAssignToGroup(concurrentCount, assignCount)

	// Verify all calls succeeded
	for i, err := range errors {
		re.NoError(err, "AssignToGroup should succeed for concurrent call %d", i)
		re.NotEmpty(results[i], "AssignToGroup should return a group for concurrent call %d", i)
	}

	// Verify the total assignment count is correct
	totalAssigned, err := suite.getTotalAssignedCount()
	re.NoError(err)
	re.Equal(concurrentCount*assignCount, totalAssigned,
		"total assignment count should equal the sum of all concurrent assignments")
}

// TestAssignToGroupConcurrentWithRLock tests that concurrent calls to AssignToGroup
// with RLock (via failpoint) will cause etcd txn conflicts and some calls will fail.
func (suite *MetaServiceGroupIntegrationTestSuite) TestAssignToGroupConcurrentWithRLock() {
	re := suite.Require()
	suite.setupConcurrentAssignTest()

	// Enable failpoint to use RLock instead of Lock
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/useRLockInAssignToGroup", "return()"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/useRLockInAssignToGroup"))
	}()

	// Concurrently call AssignToGroup multiple times
	concurrentCount := 50
	assignCount := 1
	_, errors := suite.runConcurrentAssignToGroup(concurrentCount, assignCount)

	// Verify that some calls failed due to etcd txn conflicts
	failedCount := 0
	for _, err := range errors {
		if err != nil {
			failedCount++
		}
	}

	// With RLock, concurrent etcd txn operations should cause conflicts
	// We expect at least some failures (etcd txn conflicts)
	re.Positive(failedCount, "with RLock, some AssignToGroup calls should fail due to etcd txn conflicts")

	// Verify the total assignment count is less than expected due to conflicts
	totalAssigned, err := suite.getTotalAssignedCount()
	re.NoError(err)
	re.Less(totalAssigned, concurrentCount*assignCount,
		"total assignment count should be less than expected due to etcd txn conflicts when using RLock")
}
