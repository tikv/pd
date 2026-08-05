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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
)

var errInjectedStatusCleanup = errors.New("injected status cleanup failure")

type cleanupFailingMetaServiceGroupStorage struct {
	*endpoint.StorageEndpoint
}

func (*cleanupFailingMetaServiceGroupStorage) CASMetaServiceGroupStatus(string, int64, *endpoint.MetaServiceGroupStatus) (bool, error) {
	return false, errInjectedStatusCleanup
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

// TestPatchStatusHonorsCanceledContext guards against PatchStatus silently
// persisting and publishing a patch whose request context was already
// canceled: the CAS helpers it calls don't take a context themselves, so
// without an explicit check a canceled request would still land.
func (suite *metaServiceGroupTestSuite) TestPatchStatusHonorsCanceledContext() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	cancel()

	enabled := true
	err := suite.manager.PatchStatus(ctx, "etcd-group-0", &MetaServiceGroupStatusPatch{Enabled: &enabled})
	re.ErrorIs(err, context.Canceled)

	re.NoError(suite.manager.RefreshCache(suite.ctx))
	status, err := suite.manager.GetStatus(suite.ctx)
	re.NoError(err)
	re.False(status["etcd-group-0"].Enabled, "a canceled patch must not be persisted or published")
}

// postCommitBlockingMetaServiceGroupStorage pauses RunInTxn after the
// underlying transaction has committed, once, the next time blockNextTxn is
// armed. It lets a test hold a PatchStatus call open between "storage write
// committed" and "PatchStatus returns", to probe what a concurrent
// PatchStatus observes during that window.
type postCommitBlockingMetaServiceGroupStorage struct {
	*endpoint.StorageEndpoint
	blockNextTxn atomic.Bool
	txnCommitted chan struct{}
	resumeTxn    chan struct{}
}

func (s *postCommitBlockingMetaServiceGroupStorage) CASMetaServiceGroupStatus(
	id string, expectedModRevision int64, status *endpoint.MetaServiceGroupStatus,
) (bool, error) {
	committed, err := s.StorageEndpoint.CASMetaServiceGroupStatus(id, expectedModRevision, status)
	if err == nil && s.blockNextTxn.CompareAndSwap(true, false) {
		close(s.txnCommitted)
		<-s.resumeTxn
	}
	return committed, err
}

// TestConcurrentPatchStatusKeepsCacheAtLastPersistedValue guards against
// PatchStatus publishing an out-of-order value to cachedStatus: without
// patchMu serializing the persist-then-publish sequence, a PatchStatus call
// paused between its storage commit and its cache update can resume after a
// later, already-published PatchStatus call and overwrite cachedStatus with
// its own, now-stale value, so storage and cache disagree indefinitely.
func (suite *metaServiceGroupTestSuite) TestConcurrentPatchStatusKeepsCacheAtLastPersistedValue() {
	re := suite.Require()
	store := &postCommitBlockingMetaServiceGroupStorage{
		StorageEndpoint: endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil),
		txnCommitted:    make(chan struct{}),
		resumeTxn:       make(chan struct{}),
	}
	manager, err := NewMetaServiceGroupManager(suite.ctx, store, mockMetaServiceGroups())
	re.NoError(err)

	enabled, disabled := true, false
	store.blockNextTxn.Store(true)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- manager.PatchStatus(suite.ctx, "etcd-group-0", &MetaServiceGroupStatusPatch{Enabled: &enabled})
	}()
	<-store.txnCommitted

	// patchMu should keep this call blocked until the first one fully
	// releases it below, so run it in its own goroutine rather than inline.
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- manager.PatchStatus(suite.ctx, "etcd-group-0", &MetaServiceGroupStatusPatch{Enabled: &disabled})
	}()
	close(store.resumeTxn)
	re.NoError(<-firstDone)
	re.NoError(<-secondDone)

	var persisted map[string]*endpoint.MetaServiceGroupStatus
	re.NoError(store.RunInTxn(suite.ctx, func(txn kv.Txn) error {
		var err error
		persisted, err = store.LoadMetaServiceGroupStatus(txn, map[string]string{"etcd-group-0": ""})
		return err
	}))
	cached, err := manager.GetStatus(suite.ctx)
	re.NoError(err)
	re.False(persisted["etcd-group-0"].Enabled)
	re.False(cached["etcd-group-0"].Enabled)
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
	re.NoError(suite.manager.RefreshCache(suite.ctx))
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

// TestUpdateGroupsSafelyCommitsConfigEvenWhenDeletedStatusCleanupFails documents
// a deliberate asymmetry with the added-group case above: cleaning up a deleted
// group's now-orphan persisted status runs after persist() and is best-effort,
// not rolled back on failure. This is safe because the deleted group is removed
// from metaServiceGroups (hence unreachable for new assignment) in the same call
// regardless of cleanup outcome, and a future re-add resets any leftover status
// (see TestUpdateGroupsSafelyResetsStatusForReaddedGroup). Making this atomic
// would instead make an unrelated group deletion hostage to cleaning up a status
// key nothing will ever read again.
func (suite *metaServiceGroupTestSuite) TestUpdateGroupsSafelyCommitsConfigEvenWhenDeletedStatusCleanupFails() {
	re := suite.Require()
	store := &cleanupFailingMetaServiceGroupStorage{
		StorageEndpoint: endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil),
	}
	manager, err := NewMetaServiceGroupManager(suite.ctx, store, mockMetaServiceGroups())
	re.NoError(err)

	groups := mockMetaServiceGroups()
	delete(groups, "etcd-group-0")
	configPersisted := false
	err = manager.UpdateGroupsSafely(suite.ctx, groups, []string{"etcd-group-0"}, func() error {
		configPersisted = true
		return nil
	}, nil)
	re.NoError(err)
	re.True(configPersisted)
	re.NotContains(manager.GetGroups(), "etcd-group-0")
	statusMap, err := manager.GetStatus(suite.ctx)
	re.NoError(err)
	re.NotContains(statusMap, "etcd-group-0")
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
// TestNewManagerCountsNotReadyUntilCounterWired verifies that RefreshCache at
// construction time (before SetKeyspaceAssignmentCounter has been called by the
// keyspace manager) does not mark assignment counts ready: a zero count derived
// from a nil counter is not an authoritative scan result, so callers relying on
// assignment_count_ready must keep waiting for the real rebuild.
func (suite *metaServiceGroupTestSuite) TestNewManagerCountsNotReadyUntilCounterWired() {
	re := suite.Require()
	re.False(suite.manager.IsAssignmentCountReady())
}

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

	re.NoError(suite.manager.RefreshCache(suite.ctx))
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

	re.NoError(suite.manager.RefreshPersistedStatus(suite.ctx))
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

// TestAssignmentCountRebuildMergesInFlightDeltas verifies the termGen + delta
// merge semantics: an assignment applied while a rebuild scan is in flight does
// not discard the scan and is not clobbered by the scan result. The scan
// completes in a single pass, then applies the in-flight delta on top of its
// authoritative result.
func (suite *metaServiceGroupTestSuite) TestAssignmentCountRebuildMergesInFlightDeltas() {
	re := suite.Require()
	suite.enableAllGroups()
	suite.manager.statusMu.Lock()
	suite.manager.cachedStatus["etcd-group-1"].AssignmentCount = 10
	suite.manager.cachedStatus["etcd-group-2"].AssignmentCount = 10
	suite.manager.statusMu.Unlock()
	scanStarted := make(chan struct{})
	resumeScan := make(chan struct{})
	var scanCount int32
	suite.manager.SetKeyspaceAssignmentCounter(func(_ context.Context, ids map[string]struct{}) (map[string]int, error) {
		res := make(map[string]int, len(ids))
		if atomic.AddInt32(&scanCount, 1) == 1 {
			close(scanStarted)
			<-resumeScan
		}
		res["etcd-group-0"] = 5
		return res, nil
	})

	suite.manager.StartAssignmentCountRebuild(suite.ctx)
	<-scanStarted
	// Apply a delta (min group is etcd-group-0 at 0) while the scan is in flight.
	_, err := suite.manager.PickGroup(suite.ctx)
	re.NoError(err)
	close(resumeScan)

	re.Eventually(suite.manager.IsAssignmentCountReady, time.Second, time.Millisecond)
	statusMap, err := suite.manager.GetStatus(suite.ctx)
	re.NoError(err)
	// The scan is merged with the in-flight increment, and only a single scan ran
	// (no discard-and-retry).
	re.Equal(6, statusMap["etcd-group-0"].AssignmentCount)
	re.Equal(int32(1), atomic.LoadInt32(&scanCount))
}

func (suite *metaServiceGroupTestSuite) TestAssignmentCountRebuildMergesInFlightMoveDeltas() {
	re := suite.Require()
	suite.enableAllGroups()
	suite.manager.statusMu.Lock()
	suite.manager.cachedStatus["etcd-group-0"].AssignmentCount = 0
	suite.manager.cachedStatus["etcd-group-1"].AssignmentCount = 0
	suite.manager.cachedStatus["etcd-group-2"].AssignmentCount = 0
	suite.manager.statusMu.Unlock()
	scanStarted := make(chan struct{})
	resumeScan := make(chan struct{})
	var scanCount int32
	suite.manager.SetKeyspaceAssignmentCounter(func(_ context.Context, ids map[string]struct{}) (map[string]int, error) {
		res := make(map[string]int, len(ids))
		if atomic.AddInt32(&scanCount, 1) == 1 {
			close(scanStarted)
			<-resumeScan
		}
		res["etcd-group-0"] = 10
		res["etcd-group-1"] = 10
		return res, nil
	})

	suite.manager.StartAssignmentCountRebuild(suite.ctx)
	<-scanStarted
	suite.manager.Lock()
	err := suite.manager.reassignKeyspaceLocked(nil, "etcd-group-0", "etcd-group-1")
	suite.manager.Unlock()
	re.NoError(err)
	close(resumeScan)

	re.Eventually(suite.manager.IsAssignmentCountReady, time.Second, time.Millisecond)
	statusMap, err := suite.manager.GetStatus(suite.ctx)
	re.NoError(err)
	re.Equal(9, statusMap["etcd-group-0"].AssignmentCount)
	re.Equal(11, statusMap["etcd-group-1"].AssignmentCount)
	re.Equal(int32(1), atomic.LoadInt32(&scanCount))
}

func (suite *metaServiceGroupTestSuite) TestAssignmentCountRebuildMergesInFlightAssignToGroupDelta() {
	re := suite.Require()
	suite.enableAllGroups()
	suite.manager.statusMu.Lock()
	suite.manager.cachedStatus["etcd-group-1"].AssignmentCount = 10
	suite.manager.cachedStatus["etcd-group-2"].AssignmentCount = 10
	suite.manager.statusMu.Unlock()
	scanStarted := make(chan struct{})
	resumeScan := make(chan struct{})
	suite.manager.SetKeyspaceAssignmentCounter(func(_ context.Context, ids map[string]struct{}) (map[string]int, error) {
		res := make(map[string]int, len(ids))
		close(scanStarted)
		<-resumeScan
		res["etcd-group-0"] = 5
		return res, nil
	})

	suite.manager.StartAssignmentCountRebuild(suite.ctx)
	<-scanStarted
	assigned, err := suite.manager.AssignToGroup(suite.ctx, 3)
	re.NoError(err)
	re.Equal("etcd-group-0", assigned)
	close(resumeScan)

	re.Eventually(suite.manager.IsAssignmentCountReady, time.Second, time.Millisecond)
	statusMap, err := suite.manager.GetStatus(suite.ctx)
	re.NoError(err)
	re.Equal(8, statusMap["etcd-group-0"].AssignmentCount)
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

// preCommitBlockingMetaServiceGroupStorage pauses CASMetaServiceGroupStatus,
// once, the next time blockNextCAS is armed, right before it delegates to the
// real implementation. That lets a test hold a PatchStatus call open between
// "read the status key's modification revision" and "commit the CAS against
// it", to simulate a leader whose term ends while a patch request is already
// in flight against it.
type preCommitBlockingMetaServiceGroupStorage struct {
	*endpoint.StorageEndpoint
	blockNextCAS atomic.Bool
	casPrepared  chan struct{}
	resumeCAS    chan struct{}
}

func (s *preCommitBlockingMetaServiceGroupStorage) CASMetaServiceGroupStatus(
	id string, expectedModRevision int64, status *endpoint.MetaServiceGroupStatus,
) (bool, error) {
	if s.blockNextCAS.CompareAndSwap(true, false) {
		close(s.casPrepared)
		<-s.resumeCAS
	}
	return s.StorageEndpoint.CASMetaServiceGroupStatus(id, expectedModRevision, status)
}

// TestPatchStatusModRevisionCASRejectsFormerLeaderAfterABA guards against the
// ABA hole a value-only CAS has: a former leader reads Enabled=false, a
// current leader writes true and then back to false, and the former leader's
// write then sees "the value is still what I read" and would wrongly commit.
// Comparing the modification revision instead of the value closes it, since
// the revision keeps advancing across the intervening writes even though the
// value returns to what the former leader saw.
func TestPatchStatusModRevisionCASRejectsFormerLeaderAfterABA(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	base := kv.NewEtcdKVBase(client)
	currentStore := endpoint.NewStorageEndpoint(base, nil)
	groups := map[string]string{"group": "addr"}
	disabled, enabled := false, true
	re.NoError(currentStore.RunInTxn(context.Background(), func(txn kv.Txn) error {
		return currentStore.SaveMetaServiceGroupStatus(txn, "group", &endpoint.MetaServiceGroupStatus{Enabled: false})
	}))

	formerStore := &preCommitBlockingMetaServiceGroupStorage{
		StorageEndpoint: endpoint.NewStorageEndpoint(base, nil),
		casPrepared:     make(chan struct{}),
		resumeCAS:       make(chan struct{}),
	}
	former, err := NewMetaServiceGroupManager(context.Background(), formerStore, groups)
	re.NoError(err)
	current, err := NewMetaServiceGroupManager(context.Background(), currentStore, groups)
	re.NoError(err)

	formerStore.blockNextCAS.Store(true)
	done := make(chan error, 1)
	go func() {
		done <- former.PatchStatus(context.Background(), "group", &MetaServiceGroupStatusPatch{Enabled: &enabled})
	}()
	<-formerStore.casPrepared

	// The current leader writes true, then back to false: an ABA on the
	// value, but the modification revision has advanced twice.
	re.NoError(current.PatchStatus(context.Background(), "group", &MetaServiceGroupStatusPatch{Enabled: &enabled}))
	re.NoError(current.PatchStatus(context.Background(), "group", &MetaServiceGroupStatusPatch{Enabled: &disabled}))
	close(formerStore.resumeCAS)

	// The former leader's stale-revision CAS must be rejected, not silently
	// committed just because the value happens to match again.
	re.ErrorIs(<-done, ErrMetaServiceGroupStatusConflict)

	re.NoError(current.RefreshCache(context.Background()))
	status, err := current.GetStatus(context.Background())
	re.NoError(err)
	re.False(status["group"].Enabled)
}

// TestPatchStatusModRevisionCASRejectsFormerLeaderAcrossDeleteRecreateCycle
// guards against a second-order ABA the modification-revision CAS alone
// doesn't close: etcd compares a missing key's modification revision as a
// constant 0 no matter how many times it has been created and removed, so a
// former leader that observed a missing key can still commit after the key
// went missing again through an intervening delete-and-recreate cycle.
// persistGroupsLocked closes this by overwriting the status key on both the
// added-group reset and the deleted-group cleanup instead of removing it, so
// the revision keeps climbing across the group's whole lifetime and a
// deleted-then-recreated key never compares as "still absent" again.
func TestPatchStatusModRevisionCASRejectsFormerLeaderAcrossDeleteRecreateCycle(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	base := kv.NewEtcdKVBase(client)
	currentStore := endpoint.NewStorageEndpoint(base, nil)
	groups := map[string]string{"group": "addr"}

	formerStore := &preCommitBlockingMetaServiceGroupStorage{
		StorageEndpoint: endpoint.NewStorageEndpoint(base, nil),
		casPrepared:     make(chan struct{}),
		resumeCAS:       make(chan struct{}),
	}
	former, err := NewMetaServiceGroupManager(context.Background(), formerStore, groups)
	re.NoError(err)
	current, err := NewMetaServiceGroupManager(context.Background(), currentStore, groups)
	re.NoError(err)

	enabled := true
	formerStore.blockNextCAS.Store(true)
	done := make(chan error, 1)
	go func() {
		done <- former.PatchStatus(context.Background(), "group", &MetaServiceGroupStatusPatch{Enabled: &enabled})
	}()
	<-formerStore.casPrepared

	// The former leader observed a missing key (modification revision 0).
	// Leave it missing again after an intervening create, delete, and
	// group re-add.
	re.NoError(current.PatchStatus(context.Background(), "group", &MetaServiceGroupStatusPatch{Enabled: &enabled}))
	re.NoError(current.UpdateGroupsSafely(context.Background(), map[string]string{}, []string{"group"}, func() error { return nil }, nil))
	re.NoError(current.UpdateGroupsSafely(context.Background(), groups, nil, func() error { return nil }, nil))
	close(formerStore.resumeCAS)

	// The former leader's stale CAS must still be rejected, not silently
	// accepted because the key is absent again.
	re.ErrorIs(<-done, ErrMetaServiceGroupStatusConflict)

	re.NoError(current.RefreshCache(context.Background()))
	status, err := current.GetStatus(context.Background())
	re.NoError(err)
	re.False(status["group"].Enabled, "a former leader must not enable a re-added group")
}

// TestFormerLeaderDeletedGroupCleanupDoesNotOverwriteReaddedGroupPatch guards
// against persistGroupsLocked's deleted-group status cleanup itself being an
// unfenced stale-term write: it runs after persist() has already committed
// the deletion, so it can be arbitrarily delayed. Without routing it through
// the same modification-revision CAS PatchStatus uses, a former leader's
// delayed cleanup could silently overwrite a newer leader's legitimate patch
// to the same, since-recreated group with no evidence beyond "this ID used to
// be deleted".
func TestFormerLeaderDeletedGroupCleanupDoesNotOverwriteReaddedGroupPatch(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	base := kv.NewEtcdKVBase(client)
	groups := map[string]string{"group": "addr"}

	formerStore := &preCommitBlockingMetaServiceGroupStorage{
		StorageEndpoint: endpoint.NewStorageEndpoint(base, nil),
		casPrepared:     make(chan struct{}),
		resumeCAS:       make(chan struct{}),
	}
	former, err := NewMetaServiceGroupManager(context.Background(), formerStore, groups)
	re.NoError(err)

	formerStore.blockNextCAS.Store(true)
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- former.UpdateGroupsSafely(context.Background(), map[string]string{},
			[]string{"group"}, func() error { return nil }, nil)
	}()
	<-formerStore.casPrepared // The deletion config is already persisted; only the best-effort cleanup CAS is paused.

	currentStore := endpoint.NewStorageEndpoint(base, nil)
	current, err := NewMetaServiceGroupManager(context.Background(), currentStore, map[string]string{})
	re.NoError(err)
	re.NoError(current.UpdateGroupsSafely(context.Background(), groups, nil, func() error { return nil }, nil))
	enabled := true
	re.NoError(current.PatchStatus(context.Background(), "group", &MetaServiceGroupStatusPatch{Enabled: &enabled}))

	close(formerStore.resumeCAS)
	// The cleanup is best-effort: losing its CAS must not fail the delete
	// that already committed.
	re.NoError(<-deleteDone)

	re.NoError(current.RefreshCache(context.Background()))
	status, err := current.GetStatus(context.Background())
	re.NoError(err)
	re.True(status["group"].Enabled, "a former leader's delayed cleanup must not overwrite the re-added group's patch")
}

// blockingTermContext pauses the first call to get(), once armed, right
// before returning the context it currently holds. It lets a test hold a
// casMetaServiceGroupStatusLocked call open at its term check, i.e. before
// the call has read the status key at all, to simulate a leader delayed
// before its read rather than between its read and its write.
type blockingTermContext struct {
	mu           sync.Mutex
	ctx          context.Context
	blockNext    atomic.Bool
	checkStarted chan struct{}
	resumeCheck  chan struct{}
}

func (b *blockingTermContext) setCtx(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ctx = ctx
}

func (b *blockingTermContext) get() context.Context {
	if b.blockNext.CompareAndSwap(true, false) {
		close(b.checkStarted)
		<-b.resumeCheck
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ctx
}

// TestCASMetaServiceGroupStatusLockedRejectsAlreadyCanceledTerm is a direct,
// non-concurrent check that the term-context gate in
// casMetaServiceGroupStatusLocked actually fires: a canceled term context
// must reject the write before it ever reads storage.
func TestCASMetaServiceGroupStatusLockedRejectsAlreadyCanceledTerm(t *testing.T) {
	re := require.New(t)
	store := endpoint.NewStorageEndpoint(kv.NewMemoryKV(), nil)
	manager, err := NewMetaServiceGroupManager(context.Background(), store, map[string]string{"group": "addr"})
	re.NoError(err)

	termCtx, cancelTerm := context.WithCancel(context.Background())
	cancelTerm()
	manager.SetTermContextFunc(func() context.Context { return termCtx })

	enabled := true
	err = manager.PatchStatus(context.Background(), "group", &MetaServiceGroupStatusPatch{Enabled: &enabled})
	re.ErrorIs(err, context.Canceled)

	status, err := manager.GetStatus(context.Background())
	re.NoError(err)
	re.False(status["group"].Enabled, "a patch rejected by an already-ended term must not be published to the cache")
}

// TestFormerLeaderRejectedByTermContextBeforeReadingDeletedGroupStatus closes
// the gap the modification-revision CAS alone cannot: a former leader paused
// before it ever reads the status key (not just before it commits) would
// otherwise resume, read a revision that already reflects a newer leader's
// legitimate re-add-and-patch, and CAS a stale reset against it successfully,
// because that read-write window is internally consistent even though it's
// anchored on stale premises. Checking the term context before the read
// closes it: a newer leader acting at all requires this leader's term to
// have already ended, so by the time this leader's paused goroutine resumes,
// its own term context is already canceled.
func TestFormerLeaderRejectedByTermContextBeforeReadingDeletedGroupStatus(t *testing.T) {
	re := require.New(t)
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1, nil)
	defer clean()

	base := kv.NewEtcdKVBase(client)
	groups := map[string]string{"group": "addr"}

	formerStore := endpoint.NewStorageEndpoint(base, nil)
	former, err := NewMetaServiceGroupManager(context.Background(), formerStore, groups)
	re.NoError(err)
	formerTermCtx, cancelFormerTerm := context.WithCancel(context.Background())
	blocking := &blockingTermContext{
		checkStarted: make(chan struct{}),
		resumeCheck:  make(chan struct{}),
	}
	blocking.setCtx(formerTermCtx)
	former.SetTermContextFunc(blocking.get)

	blocking.blockNext.Store(true)
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- former.UpdateGroupsSafely(context.Background(), map[string]string{},
			[]string{"group"}, func() error { return nil }, nil)
	}()
	<-blocking.checkStarted // Config deletion is persisted; the cleanup is about to check its term, before reading the status key.

	currentStore := endpoint.NewStorageEndpoint(base, nil)
	current, err := NewMetaServiceGroupManager(context.Background(), currentStore, map[string]string{})
	re.NoError(err)
	re.NoError(current.UpdateGroupsSafely(context.Background(), groups, nil, func() error { return nil }, nil))
	enabled := true
	re.NoError(current.PatchStatus(context.Background(), "group", &MetaServiceGroupStatusPatch{Enabled: &enabled}))

	// Former's term actually ends now (e.g. its lease was lost), which is
	// exactly what must have already happened for current's actions above to
	// be legitimate in the first place.
	cancelFormerTerm()
	close(blocking.resumeCheck)

	// The cleanup is best-effort: being rejected by the term check must not
	// fail the delete that already committed.
	re.NoError(<-deleteDone)

	re.NoError(current.RefreshCache(context.Background()))
	status, err := current.GetStatus(context.Background())
	re.NoError(err)
	re.True(status["group"].Enabled,
		"a former leader rejected by its term context must not overwrite the re-added group's patch")
}
