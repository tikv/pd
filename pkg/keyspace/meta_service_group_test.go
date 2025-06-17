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
	suite.manager = NewMetaServiceGroupManager(suite.ctx, store, mockMetaServiceGroups())
}

func (suite *metaServiceGroupTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *metaServiceGroupTestSuite) TestGetAssignmentCountsInitialZero() {
	re := suite.Require()
	counts, err := suite.manager.GetAssignmentCounts()
	re.NoError(err)

	for grp := range mockMetaServiceGroups() {
		val, exists := counts[grp]
		re.True(exists, "expected group %q to be present in counts", grp)
		re.Equal(0, val, "expected initial count of %q to be 0", grp)
	}
}

func (suite *metaServiceGroupTestSuite) TestAssignToGroup() {
	re := suite.Require()
	request := 5
	assigned, err := suite.manager.AssignToGroup(request)
	re.NoError(err)
	re.NotEmpty(assigned, "expected some non-empty group name")

	// Verify the returned group is one of the mockMetaServiceGroups keys.
	_, isValid := mockMetaServiceGroups()[assigned]
	re.True(isValid, "assigned group must be from mockMetaServiceGroups")

	// Verify the chosen group's count increments by 'request'.
	counts, err := suite.manager.GetAssignmentCounts()
	re.NoError(err)
	re.Equal(request, counts[assigned], "chosen group's count should equal the requested increment")

	// All other groups must remain at 0.
	for grp := range mockMetaServiceGroups() {
		if grp == assigned {
			continue
		}
		re.Equal(0, counts[grp], "other groups should remain at 0")
	}
}

func (suite *metaServiceGroupTestSuite) TestUpdateAssignment() {
	re := suite.Require()
	err := suite.manager.UpdateAssignment("", "etcd-group-0")
	re.NoError(err)

	counts, err := suite.manager.GetAssignmentCounts()
	re.NoError(err)
	re.Equal(1, counts["etcd-group-0"])
	re.Equal(0, counts["etcd-group-1"])
	re.Equal(0, counts["etcd-group-2"])

	err = suite.manager.UpdateAssignment("etcd-group-0", "etcd-group-1")
	re.NoError(err)

	counts, err = suite.manager.GetAssignmentCounts()
	re.NoError(err)
	re.Equal(0, counts["etcd-group-0"], "expected decremented back to 0")
	re.Equal(1, counts["etcd-group-1"], "expected incremented to 1")
	re.Equal(0, counts["etcd-group-2"], "unchanged")
}

func (suite *metaServiceGroupTestSuite) TestUpdateAssignmentUnknownNewGroup() {
	re := suite.Require()
	err := suite.manager.UpdateAssignment("", "nonexistent")
	re.Equal(errUnknownMetaServiceGroup, err)
}

func (suite *metaServiceGroupTestSuite) TestAttachEndpoints() {
	re := suite.Require()
	keyspaceConfig := map[string]string{
		MetaServiceGroupIDKey: "etcd-group-1",
	}
	suite.manager.AttachEndpoints(keyspaceConfig)

	expected := mockMetaServiceGroups()["etcd-group-1"]
	actual := keyspaceConfig[MetaServiceGroupAddressesKey]
	re.Equal(expected, actual, "AttachEndpoints should set the metaServiceGroups value")
}

func (suite *metaServiceGroupTestSuite) TestAttachEndpointsMissingGroup() {
	re := suite.Require()
	// MetaServiceGroupIDKey missing
	configA := map[string]string{}
	suite.manager.AttachEndpoints(configA)
	_, existsA := configA[MetaServiceGroupAddressesKey]
	re.False(existsA, "should not set metaServiceGroups if MetaServiceGroupIDKey is missing")

	// MetaServiceGroupIDKey empty
	configB := map[string]string{MetaServiceGroupIDKey: ""}
	suite.manager.AttachEndpoints(configB)
	valB, existsB := configB[MetaServiceGroupAddressesKey]
	re.False(existsB, "should not set metaServiceGroups if MetaServiceGroupIDKey == \"\"")
	re.Equal("", valB, "value must be empty if metaServiceGroups key somehow exists")
}

func (suite *metaServiceGroupTestSuite) TestUpdateEndpoints() {
	re := suite.Require()
	newMap := map[string]string{
		"foo": "foo.bar.local",
	}
	suite.manager.updateGroups(newMap)
	config := map[string]string{MetaServiceGroupIDKey: "foo"}
	suite.manager.AttachEndpoints(config)
	re.Equal("foo.bar.local", config[MetaServiceGroupAddressesKey], "should read from updated metaServiceGroups map")
}

func (suite *metaServiceGroupTestSuite) TestUpdateEndpointsAndUpdateAssignment() {
	re := suite.Require()
	// Assign to some existing group
	assigned, err := suite.manager.AssignToGroup(1)
	re.NoError(err)
	re.NotEmpty(assigned, "expected AssignToGroup to return a non-empty group")
	counts, err := suite.manager.GetAssignmentCounts()
	re.NoError(err)
	re.Equal(1, counts[assigned], "assigned group should have count 1")

	// Add a new group "etcd-group-3"
	newMap := mockMetaServiceGroups()
	newMap["etcd-group-3"] = "etcd-group-3.tidb-serverless.cluster.svc.local"
	suite.manager.updateGroups(newMap)

	// Move the assignment from the originally assigned group to "etcd-group-3"
	err = suite.manager.UpdateAssignment(assigned, "etcd-group-3")
	re.NoError(err)

	// the original group should have decreased from 1 → 0
	// "etcd-group-3" should have increased from 0 → 1
	counts, err = suite.manager.GetAssignmentCounts()
	re.NoError(err)
	re.Equal(0, counts[assigned], "original group should have count 0 after moving assignment")
	re.Equal(1, counts["etcd-group-3"], "new group should have count 1")

	// All other preexisting groups (besides assigned and etcd-group-3) remain at 0
	for grp := range mockMetaServiceGroups() {
		if grp == assigned {
			continue
		}
		re.Equal(0, counts[grp], "other original groups should remain at 0")
	}
}
