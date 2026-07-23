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

package keyspace

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
)

var errInjectedStatusCleanup = errors.New("injected status cleanup failure")

type cleanupFailingMetaServiceGroupStorage struct {
	*endpoint.StorageEndpoint
}

func (*cleanupFailingMetaServiceGroupStorage) RemoveMetaServiceGroupStatus(kv.Txn, string) error {
	return errInjectedStatusCleanup
}

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
	var err error
	suite.manager, err = NewMetaServiceGroupManager(suite.ctx, store, mockMetaServiceGroups())
	suite.Require().NoError(err)
}

func (suite *metaServiceGroupTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *metaServiceGroupTestSuite) TestGetAssignmentCountsInitialZero() {
	re := suite.Require()
	statusMap, err := suite.manager.GetStatus(suite.ctx)
	re.NoError(err)

	for grp := range mockMetaServiceGroups() {
		status, exists := statusMap[grp]
		re.True(exists, "expected group %q to be present in counts", grp)
		re.Equal(0, status.AssignmentCount, "expected initial count of %q to be 0", grp)
		re.False(status.Enabled, "expected initial status of %q to be disabled", grp)
	}

	_, err = suite.manager.AssignToGroup(suite.ctx, 1)
	re.Error(err)
}

func (suite *metaServiceGroupTestSuite) TestAssignToGroup() {
	re := suite.Require()
	suite.enableAllGroups()
	request := 5
	assigned, err := suite.manager.AssignToGroup(suite.ctx, request)
	re.NoError(err)
	re.NotEmpty(assigned, "expected some non-empty group name")

	// Verify the returned group is one of the mockMetaServiceGroups keys.
	_, isValid := mockMetaServiceGroups()[assigned]
	re.True(isValid, "assigned group must be from mockMetaServiceGroups")

	// Verify the chosen group's count increments by 'request'.
	counts, err := suite.manager.GetAssignmentCounts(suite.ctx)
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

func (suite *metaServiceGroupTestSuite) TestPickGroup() {
	re := suite.Require()
	suite.enableAllGroups()
	assigned, err := suite.manager.PickGroup(suite.ctx)
	re.NoError(err)
	re.NotEmpty(assigned, "expected PickGroup to return a non-empty group")

	_, isValid := mockMetaServiceGroups()[assigned]
	re.True(isValid, "picked group must be from mockMetaServiceGroups")

	counts, err := suite.manager.GetAssignmentCounts(suite.ctx)
	re.NoError(err)
	re.Equal(1, counts[assigned], "picked group's count should be incremented once")
	for grp := range mockMetaServiceGroups() {
		if grp == assigned {
			continue
		}
		re.Equal(0, counts[grp], "other groups should remain at 0")
	}
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
	re.Empty(valB, "value must be empty if metaServiceGroups key somehow exists")
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

func (suite *metaServiceGroupTestSuite) TestGetGroupsReturnsCopy() {
	re := suite.Require()
	groups := suite.manager.GetGroups()
	groups["etcd-group-0"] = "mutated"
	delete(groups, "etcd-group-1")

	currentGroups := suite.manager.GetGroups()
	re.Equal(mockMetaServiceGroups()["etcd-group-0"], currentGroups["etcd-group-0"])
	re.Equal(mockMetaServiceGroups()["etcd-group-1"], currentGroups["etcd-group-1"])
}

func (suite *metaServiceGroupTestSuite) TestUpdateEndpointsAndUpdateAssignment() {
	re := suite.Require()
	suite.enableAllGroups()
	// Assign to some existing group
	assigned, err := suite.manager.AssignToGroup(suite.ctx, 1)
	re.NoError(err)
	re.NotEmpty(assigned, "expected AssignToGroup to return a non-empty group")
	counts, err := suite.manager.GetAssignmentCounts(suite.ctx)
	re.NoError(err)
	re.Equal(1, counts[assigned], "assigned group should have count 1")

	// Add a new group "etcd-group-3"
	newMap := mockMetaServiceGroups()
	newMap["etcd-group-3"] = "etcd-group-3.tidb-serverless.cluster.svc.local"
	suite.manager.updateGroups(newMap)

	// Move the assignment from the originally assigned group to "etcd-group-3"
	err = suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return suite.manager.updateAssignmentTxn(txn, assigned, "etcd-group-3")
	})
	re.NoError(err)

	// the original group should have decreased from 1 → 0
	// "etcd-group-3" should have increased from 0 → 1
	counts, err = suite.manager.GetAssignmentCounts(suite.ctx)
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

// TestUpdateGroupsSafelyUsesAuthoritativeCount verifies the delete guard relies
// on the actual keyspace assignments rather than the persisted counter, so a
// stale (drifted) counter cannot permanently block removing an empty group, and
// a group with real keyspaces is still protected.
func (suite *metaServiceGroupTestSuite) TestUpdateGroupsSafelyUsesAuthoritativeCount() {
	re := suite.Require()
	// Simulate a stale persisted status: etcd-group-2 reports assigned keyspaces
	// even though none actually reference it.
	err := suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return suite.manager.store.SaveMetaServiceGroupStatus(txn, "etcd-group-2", &endpoint.MetaServiceGroupStatus{AssignmentCount: 3})
	})
	re.NoError(err)

	// Authoritative scanner reports the real assignments.
	actual := map[string]int{}
	suite.manager.SetKeyspaceAssignmentCounter(func(_ context.Context, ids map[string]struct{}) (map[string]int, error) {
		res := make(map[string]int, len(ids))
		for id := range ids {
			res[id] = actual[id]
		}
		return res, nil
	})

	// Deleting etcd-group-2 must succeed despite the stale positive counter.
	groups := mockMetaServiceGroups()
	delete(groups, "etcd-group-2")
	persisted := false
	err = suite.manager.UpdateGroupsSafely(suite.ctx, groups, []string{"etcd-group-2"},
		func() error { persisted = true; return nil }, nil)
	re.NoError(err)
	re.True(persisted, "deletion of an actually-empty group must be persisted")

	// The persisted status for the deleted group must be cleared, so re-adding
	// the same ID later does not inherit the stale count.
	var residual map[string]*endpoint.MetaServiceGroupStatus
	err = suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		var err error
		residual, err = suite.manager.store.LoadMetaServiceGroupStatus(txn, map[string]string{"etcd-group-2": ""})
		return err
	})
	re.NoError(err)
	re.Equal(0, residual["etcd-group-2"].AssignmentCount, "deleted group's persisted count must be cleared")

	// A group with real keyspaces must still be rejected.
	actual["etcd-group-1"] = 1
	groups2 := mockMetaServiceGroups()
	delete(groups2, "etcd-group-1")
	delete(groups2, "etcd-group-2")
	err = suite.manager.UpdateGroupsSafely(suite.ctx, groups2, []string{"etcd-group-1"},
		func() error { return nil }, nil)
	re.ErrorIs(err, ErrGroupHasAssignedKeyspaces)
}

func (suite *metaServiceGroupTestSuite) TestAssignToGroupRejectsNegativeCount() {
	re := suite.Require()
	_, err := suite.manager.AssignToGroup(suite.ctx, -1)
	re.ErrorIs(err, ErrInvalidAssignmentCount)
}

func (suite *metaServiceGroupTestSuite) TestPatchStatusRejectsAssignmentCount() {
	re := suite.Require()
	count := 7
	err := suite.manager.PatchStatus(suite.ctx, "etcd-group-0", &MetaServiceGroupStatusPatch{AssignmentCount: &count})
	re.ErrorIs(err, ErrAssignmentCountPatchUnsupported)
}

// TestUpdateGroupsSafelyResetsStatusForReaddedGroup verifies that re-adding a
// group whose Enabled flag still lingers in storage resets it, so RefreshCache
// starts the group disabled instead of resurrecting the stale flag. The count is
// derived from keyspace metadata, so it starts at zero.
func (suite *metaServiceGroupTestSuite) TestUpdateGroupsSafelyResetsStatusForReaddedGroup() {
	re := suite.Require()
	const groupID = "etcd-group-readded"
	// Simulate a stale persisted status for a group the manager does not know yet.
	re.NoError(suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return suite.manager.store.SaveMetaServiceGroupStatus(txn, groupID,
			&endpoint.MetaServiceGroupStatus{AssignmentCount: 5, Enabled: true})
	}))

	groups := mockMetaServiceGroups()
	groups[groupID] = "etcd-group-readded.tidb-serverless.cluster.svc.local"
	re.NoError(suite.manager.UpdateGroupsSafely(suite.ctx, groups, nil,
		func() error { return nil }, nil))

	// Reloading must see a disabled group with a zero derived count, not the stale one.
	re.NoError(suite.manager.RefreshCache())
	statusMap, err := suite.manager.GetStatus(suite.ctx)
	re.NoError(err)
	re.NotNil(statusMap[groupID])
	re.Equal(0, statusMap[groupID].AssignmentCount)
	re.False(statusMap[groupID].Enabled)
}

func (suite *metaServiceGroupTestSuite) TestUpdateGroupsSafelyDoesNotCommitConfigWhenAddedStatusResetFails() {
	re := suite.Require()
	store := &cleanupFailingMetaServiceGroupStorage{
		StorageEndpoint: endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil),
	}
	manager, err := NewMetaServiceGroupManager(suite.ctx, store, mockMetaServiceGroups())
	re.NoError(err)

	groups := mockMetaServiceGroups()
	groups["etcd-group-readded"] = "etcd-group-readded.tidb-serverless.cluster.svc.local"
	configPersisted := false
	err = manager.UpdateGroupsSafely(suite.ctx, groups, nil, func() error {
		configPersisted = true
		return nil
	}, nil)
	re.ErrorIs(err, errInjectedStatusCleanup)
	re.False(configPersisted)
	re.NotContains(manager.GetGroups(), "etcd-group-readded")
	statusMap, err := manager.GetStatus(suite.ctx)
	re.NoError(err)
	re.NotContains(statusMap, "etcd-group-readded")
}

func (suite *metaServiceGroupTestSuite) TestReassignRejectsDisabledGroup() {
	re := suite.Require()
	// Groups are disabled by default, so reassigning a keyspace into one must be
	// rejected.
	err := suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return suite.manager.reassignKeyspaceLocked(txn, "", "etcd-group-0")
	})
	re.ErrorIs(err, ErrMetaServiceGroupDisabled)
	// An unknown group is still rejected as unknown.
	err = suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return suite.manager.reassignKeyspaceLocked(txn, "", "nonexistent")
	})
	re.ErrorIs(err, ErrUnknownMetaServiceGroup)
	// Once enabled, the reassignment succeeds.
	suite.enableAllGroups()
	err = suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return suite.manager.reassignKeyspaceLocked(txn, "", "etcd-group-0")
	})
	re.NoError(err)
}

// TestRefreshCacheRebuildsFromStorageAndScan verifies that RefreshCache takes the
// Enabled flag from storage (authoritative for that field) but rebuilds
// AssignmentCount from the keyspace scan, ignoring any stale persisted count.
func (suite *metaServiceGroupTestSuite) TestRefreshCacheRebuildsFromStorageAndScan() {
	re := suite.Require()
	// Persist a stale status: the persisted count must be ignored, the flag kept.
	err := suite.manager.store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		return suite.manager.store.SaveMetaServiceGroupStatus(txn, "etcd-group-0",
			&endpoint.MetaServiceGroupStatus{AssignmentCount: 10, Enabled: true})
	})
	re.NoError(err)
	// The authoritative counter reports the real assignment count.
	suite.manager.SetKeyspaceAssignmentCounter(func(_ context.Context, ids map[string]struct{}) (map[string]int, error) {
		res := make(map[string]int, len(ids))
		if _, ok := ids["etcd-group-0"]; ok {
			res["etcd-group-0"] = 3
		}
		return res, nil
	})

	re.NoError(suite.manager.RefreshCache())
	statusMap, err := suite.manager.GetStatus(suite.ctx)
	re.NoError(err)
	re.True(statusMap["etcd-group-0"].Enabled)             // from storage
	re.Equal(3, statusMap["etcd-group-0"].AssignmentCount) // from the scan, not the stale 10
}

func (suite *metaServiceGroupTestSuite) TestRefreshPersistedStatusDoesNotScanAssignmentCounts() {
	re := suite.Require()
	called := false
	suite.manager.SetKeyspaceAssignmentCounter(func(context.Context, map[string]struct{}) (map[string]int, error) {
		called = true
		return map[string]int{"etcd-group-0": 3}, nil
	})

	re.NoError(suite.manager.RefreshPersistedStatus())
	re.False(called)
	re.False(suite.manager.IsAssignmentCountReady())
}

func (suite *metaServiceGroupTestSuite) TestStartAssignmentCountRebuildMarksReadyAfterAsyncScan() {
	re := suite.Require()
	suite.manager.SetKeyspaceAssignmentCounter(func(_ context.Context, ids map[string]struct{}) (map[string]int, error) {
		res := make(map[string]int, len(ids))
		if _, ok := ids["etcd-group-0"]; ok {
			res["etcd-group-0"] = 3
		}
		return res, nil
	})

	suite.manager.StartAssignmentCountRebuild(suite.ctx)
	re.Eventually(suite.manager.IsAssignmentCountReady, time.Second, time.Millisecond)
	statusMap, err := suite.manager.GetStatus(suite.ctx)
	re.NoError(err)
	re.Equal(3, statusMap["etcd-group-0"].AssignmentCount)
}

func (suite *metaServiceGroupTestSuite) TestAssignmentCountRebuildRetriesWhenEpochChanges() {
	re := suite.Require()
	suite.enableAllGroups()
	suite.manager.statusMu.Lock()
	suite.manager.cachedStatus["etcd-group-1"].AssignmentCount = 10
	suite.manager.cachedStatus["etcd-group-2"].AssignmentCount = 10
	suite.manager.statusMu.Unlock()
	scanStarted := make(chan struct{})
	resumeScan := make(chan struct{})
	firstScan := true
	suite.manager.SetKeyspaceAssignmentCounter(func(_ context.Context, ids map[string]struct{}) (map[string]int, error) {
		res := make(map[string]int, len(ids))
		if firstScan {
			firstScan = false
			close(scanStarted)
			<-resumeScan
			res["etcd-group-0"] = 0
			return res, nil
		}
		res["etcd-group-0"] = 1
		return res, nil
	})

	suite.manager.StartAssignmentCountRebuild(suite.ctx)
	<-scanStarted
	_, err := suite.manager.PickGroup(suite.ctx)
	re.NoError(err)
	close(resumeScan)

	re.Eventually(suite.manager.IsAssignmentCountReady, time.Second, time.Millisecond)
	statusMap, err := suite.manager.GetStatus(suite.ctx)
	re.NoError(err)
	re.Equal(1, statusMap["etcd-group-0"].AssignmentCount)
}

func (suite *metaServiceGroupTestSuite) enableAllGroups() {
	re := suite.Require()
	enabled := true
	for groupID := range mockMetaServiceGroups() {
		re.NoError(suite.manager.PatchStatus(suite.ctx, groupID, &MetaServiceGroupStatusPatch{
			Enabled: &enabled,
		}))
	}
}
