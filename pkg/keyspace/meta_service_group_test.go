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

package keyspace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/testutil"
)

type metaServiceGroupTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	manager *MetaServiceGroupManager
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
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	cfg := mockConfig{
		AutoAssignMetaServiceGroups: true,
		MetaServiceGroups:           mockMetaServiceGroups(),
	}
	var err error
	suite.manager, err = NewMetaServiceGroupManager(suite.ctx, store, &cfg)
	suite.Require().NoError(err)
}

func (suite *metaServiceGroupTestSuite) TearDownTest() {
	suite.cancel()
}

// TestInitialState verifies initial status map and fallback behavior.
func (suite *metaServiceGroupTestSuite) TestInitialState() {
	re := suite.Require()
	// Load status and verify defaults for all groups.
	statusMap, err := suite.manager.GetStatus()
	re.NoError(err)
	for id := range mockMetaServiceGroups() {
		status, exists := statusMap[id]
		re.True(exists)
		re.Zero(status.AssignmentCount)
		re.False(status.Enabled)
	}
	// Assignment should fallback to PD
	// Verify assignment returns empty when groups are disabled.
	group := suite.manager.AssignToGroup(1)
	re.Empty(group)
}

// TestAssignToGroup verifies enabled group selection and counts.
func (suite *metaServiceGroupTestSuite) TestAssignToGroup() {
	re := suite.Require()
	// Enable all groups.
	for groupID := range mockMetaServiceGroups() {
		enable := true
		re.NoError(suite.manager.PatchStatus(groupID, &MetaServiceGroupStatusPatch{
			Enabled: &enable,
		}))
	}
	// Assign a request and capture selection.
	request := 5
	assigned := suite.manager.AssignToGroup(request)
	re.NotEmpty(assigned)

	// Verify the returned group is one of the mockMetaServiceGroups keys.
	_, isValid := mockMetaServiceGroups()[assigned]
	re.True(isValid)

	// Verify the chosen group's count increments by 'request'.
	statusMap, err := suite.manager.GetStatus()
	re.NoError(err)
	re.Contains(statusMap, assigned)
	re.Equal(request, statusMap[assigned].AssignmentCount)

	// All other groups must remain at 0.
	for groupID := range mockMetaServiceGroups() {
		if groupID == assigned {
			continue
		}
		re.Contains(statusMap, groupID)
		re.Equal(0, statusMap[groupID].AssignmentCount)
	}
}

// TestUpdateAssignment verifies moving assignments between groups.
func (suite *metaServiceGroupTestSuite) TestUpdateAssignment() {
	re := suite.Require()
	// Enable all groups for assignment updates.
	for groupID := range mockMetaServiceGroups() {
		enable := true
		re.NoError(suite.manager.PatchStatus(groupID, &MetaServiceGroupStatusPatch{
			Enabled: &enable,
		}))
	}
	// Assign from empty to group-0 and validate counts.
	err := suite.manager.UpdateAssignment("", "etcd-group-0")
	re.NoError(err)
	statusMap, err := suite.manager.GetStatus()
	re.NoError(err)
	re.Equal(1, statusMap["etcd-group-0"].AssignmentCount)
	re.Equal(0, statusMap["etcd-group-1"].AssignmentCount)
	re.Equal(0, statusMap["etcd-group-2"].AssignmentCount)

	// Move assignment from group-0 to group-1.
	err = suite.manager.UpdateAssignment("etcd-group-0", "etcd-group-1")
	re.NoError(err)

	statusMap, err = suite.manager.GetStatus()
	re.NoError(err)
	re.Equal(0, statusMap["etcd-group-0"].AssignmentCount)
	re.Equal(1, statusMap["etcd-group-1"].AssignmentCount)
	re.Equal(0, statusMap["etcd-group-2"].AssignmentCount)
}

// TestUpdateAssignmentUnknownNewGroup returns error for unknown target.
func (suite *metaServiceGroupTestSuite) TestUpdateAssignmentUnknownNewGroup() {
	re := suite.Require()
	// Update to unknown group should fail.
	err := suite.manager.UpdateAssignment("", "nonexistent")
	re.Equal(errUnknownMetaServiceGroup, err)
}

// TestRefreshCacheLoadsFromStorage verifies cache reloads from storage.
func (suite *metaServiceGroupTestSuite) TestRefreshCacheLoadsFromStorage() {
	re := suite.Require()
	// Seed storage with status for group-0.
	status := &endpoint.MetaServiceGroupStatus{
		AssignmentCount: 10,
		Enabled:         true,
	}
	err := suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return suite.manager.store.SaveMetaServiceGroupStatus(txn, "etcd-group-0", status)
	})
	re.NoError(err)

	// Refresh cache and verify values.
	re.NoError(suite.manager.RefreshCache())
	statusMap, err := suite.manager.GetStatus()
	re.NoError(err)
	re.Equal(10, statusMap["etcd-group-0"].AssignmentCount)
	re.True(statusMap["etcd-group-0"].Enabled)
}

// TestFlushAfterWriteThreshold verifies flush after threshold writes.
func (suite *metaServiceGroupTestSuite) TestFlushAfterWriteThreshold() {
	re := suite.Require()
	// Enable all groups for assignment.
	for groupID := range mockMetaServiceGroups() {
		enable := true
		re.NoError(suite.manager.PatchStatus(groupID, &MetaServiceGroupStatusPatch{
			Enabled: &enable,
		}))
	}

	// Trigger threshold assignment.
	assigned := suite.manager.AssignToGroup(1000)
	re.NotEmpty(assigned)

	// Eventually verify storage was flushed.
	testutil.Eventually(re, func() bool {
		var (
			statusMap map[string]*endpoint.MetaServiceGroupStatus
			err       error
		)
		err = suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
			statusMap, err = suite.manager.store.LoadMetaServiceGroupStatus(txn, mockMetaServiceGroups())
			return err
		})
		if err != nil {
			return false
		}
		status := statusMap[assigned]
		if status == nil {
			return false
		}
		return status.AssignmentCount == 1000
	})
}

// TestAttachEndpoints verifies endpoints are filled by group ID.
func (suite *metaServiceGroupTestSuite) TestAttachEndpoints() {
	re := suite.Require()
	// Attach endpoints for an existing group.
	keyspaceConfig := map[string]string{
		MetaServiceGroupIDKey: "etcd-group-1",
	}
	suite.manager.AttachEndpoints(keyspaceConfig)

	// Validate endpoint value matches config.
	expected := mockMetaServiceGroups()["etcd-group-1"]
	actual := keyspaceConfig[MetaServiceGroupAddressesKey]
	re.Equal(expected, actual, "AttachEndpoints should set the metaServiceGroups value")
}

// TestAttachEndpointsMissingGroup verifies no endpoints when group missing.
func (suite *metaServiceGroupTestSuite) TestAttachEndpointsMissingGroup() {
	re := suite.Require()
	// Missing group ID should not set endpoint.
	configA := map[string]string{}
	suite.manager.AttachEndpoints(configA)
	_, existsA := configA[MetaServiceGroupAddressesKey]
	re.False(existsA, "should not set metaServiceGroups if MetaServiceGroupIDKey is missing")

	// Empty group ID should not set endpoint.
	configB := map[string]string{MetaServiceGroupIDKey: ""}
	suite.manager.AttachEndpoints(configB)
	valB, existsB := configB[MetaServiceGroupAddressesKey]
	re.False(existsB, "should not set metaServiceGroups if MetaServiceGroupIDKey == \"\"")
	re.Equal("", valB, "value must be empty if metaServiceGroups key somehow exists")
}

// TestUpdateEndpoints verifies endpoints use updated config map.
func (suite *metaServiceGroupTestSuite) TestUpdateEndpoints() {
	re := suite.Require()
	// Update config map and verify attach uses it.
	newMap := map[string]string{
		"foo": "foo.bar.local",
	}
	suite.manager.updateConfig(true, newMap, 0)
	config := map[string]string{MetaServiceGroupIDKey: "foo"}
	suite.manager.AttachEndpoints(config)
	re.Equal("foo.bar.local", config[MetaServiceGroupAddressesKey], "should read from updated metaServiceGroups map")
}

// TestUpdateEndpointsAndUpdateAssignment verifies new group assignment.
func (suite *metaServiceGroupTestSuite) TestUpdateEndpointsAndUpdateAssignment() {
	re := suite.Require()
	// Enable all groups for assignment.
	for groupID := range mockMetaServiceGroups() {
		enable := true
		re.NoError(suite.manager.PatchStatus(groupID, &MetaServiceGroupStatusPatch{
			Enabled: &enable,
		}))
	}
	// Create an initial assignment.
	assigned := suite.manager.AssignToGroup(1)
	re.NotEmpty(assigned, "expected AssignToGroup to return a non-empty group")
	statusMap, err := suite.manager.GetStatus()
	re.NoError(err)
	re.Contains(statusMap, assigned, "assigned group should be in status map")
	re.Equal(1, statusMap[assigned].AssignmentCount, "assigned group should have count 1")

	// Extend config with a new group.
	newMap := mockMetaServiceGroups()
	newMap["etcd-group-3"] = "etcd-group-3.tidb-serverless.cluster.svc.local"
	suite.manager.updateConfig(true, newMap, 0)

	// Move the assignment from the originally assigned group to "etcd-group-3"
	err = suite.manager.UpdateAssignment(assigned, "etcd-group-3")
	re.NoError(err)

	// the original group should have decreased from 1 → 0
	// "etcd-group-3" should have increased from 0 → 1
	statusMap, err = suite.manager.GetStatus()
	re.NoError(err)
	re.Equal(0, statusMap[assigned].AssignmentCount, "original group should have count 0 after moving assignment")
	re.Equal(1, statusMap["etcd-group-3"].AssignmentCount, "new group should have count 1")

	// All other preexisting groups (besides assigned and etcd-group-3) remain at 0
	for groupID := range mockMetaServiceGroups() {
		if groupID == assigned {
			continue
		}
		re.Equal(0, statusMap[groupID].AssignmentCount, "other original groups should remain at 0")
	}
}

// TestFallbackRatio verifies probabilistic fallback behavior.
func (suite *metaServiceGroupTestSuite) TestFallbackRatio() {
	re := suite.Require()
	// Enable all groups for assignment.
	for groupID := range mockMetaServiceGroups() {
		enable := true
		re.NoError(suite.manager.PatchStatus(groupID, &MetaServiceGroupStatusPatch{
			Enabled: &enable,
		}))
	}
	// Fallback ratio defaults to 0, all keyspace should be assigned to a group.
	for range 10 {
		assigned := suite.manager.AssignToGroup(1)
		re.NotEmpty(assigned, "expected AssignToGroup to return a non-empty group")
	}
	// Fallback ratio set to 0.5, some keyspace should be assigned to a group, some should fallback.
	batchSize := 100
	expectedFallbackRatio := 0.5
	suite.manager.fallbackRatio = expectedFallbackRatio
	totalFallback := 0
	for range batchSize {
		assigned := suite.manager.AssignToGroup(1)
		if assigned == "" {
			totalFallback++
		}
	}
	re.InDelta(expectedFallbackRatio, float64(totalFallback)/float64(batchSize), 0.1)

	// Fallback ratio set to 1, all keyspace should fallback to pd.
	suite.manager.fallbackRatio = 1.0
	totalFallback = 0
	for range batchSize {
		assigned := suite.manager.AssignToGroup(1)
		if assigned == "" {
			totalFallback++
		}
	}
	re.Equal(batchSize, totalFallback, "all keyspace should fallback to pd when fallback ratio is 1.0")
}
