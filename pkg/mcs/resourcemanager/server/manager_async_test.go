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

package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/testutil"
)

type blockingResourceGroupStorage struct {
	storage.Storage

	once        sync.Once
	releaseOnce sync.Once
	entered     chan struct{}
	release     chan struct{}

	// failNextState, when true, makes the very next LoadResourceGroupState
	// call fail once, then resets itself.
	failNextState atomic.Bool

	// pausePointState, when true, makes the very next LoadResourceGroupState
	// call signal pointReached and then block on pointRelease, so a test can
	// hold a lazy load right after its storage read but before it inserts.
	pausePointState atomic.Bool
	pointReached    chan struct{}
	pointRelease    chan struct{}

	// pauseNextStates, when true, makes the very next bulk
	// LoadResourceGroupStates call signal statesReached and then block on
	// statesRelease, so a test can hold an async loader after it has captured
	// the settings scan but before it merges.
	pauseNextStates   atomic.Bool
	statesReached     chan struct{}
	statesRelease     chan struct{}
	statesReleaseOnce sync.Once
}

func newBlockingResourceGroupStorage() *blockingResourceGroupStorage {
	return &blockingResourceGroupStorage{
		Storage:       storage.NewStorageWithMemoryBackend(),
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
		pointReached:  make(chan struct{}),
		pointRelease:  make(chan struct{}),
		statesReached: make(chan struct{}),
		statesRelease: make(chan struct{}),
	}
}

func (s *blockingResourceGroupStorage) LoadResourceGroupSettings(f func(keyspaceID uint32, name, rawValue string)) error {
	s.once.Do(func() {
		close(s.entered)
		<-s.release
	})
	return s.Storage.LoadResourceGroupSettings(f)
}

func (s *blockingResourceGroupStorage) LoadResourceGroupState(keyspaceID uint32, name string) (string, error) {
	if s.failNextState.CompareAndSwap(true, false) {
		return "", errors.New("injected resource group state load failure")
	}
	if s.pausePointState.CompareAndSwap(true, false) {
		close(s.pointReached)
		<-s.pointRelease
	}
	return s.Storage.LoadResourceGroupState(keyspaceID, name)
}

func (s *blockingResourceGroupStorage) LoadResourceGroupStates(f func(keyspaceID uint32, name, rawValue string)) error {
	if s.pauseNextStates.CompareAndSwap(true, false) {
		close(s.statesReached)
		<-s.statesRelease
	}
	return s.Storage.LoadResourceGroupStates(f)
}

func (s *blockingResourceGroupStorage) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async resource group loading")
	}
}

func (s *blockingResourceGroupStorage) unblock() {
	s.releaseOnce.Do(func() {
		close(s.release)
	})
}

func (s *blockingResourceGroupStorage) unblockStates() {
	s.statesReleaseOnce.Do(func() {
		close(s.statesRelease)
	})
}

// asyncTestGroupFillRate is the fill rate used by all async-loading test
// groups; kept as a named constant so the setup and the assertions stay in
// sync.
const asyncTestGroupFillRate = 100

func newAsyncTestGroup(name string) *resource_manager.ResourceGroup {
	return &resource_manager.ResourceGroup{
		Name:     name,
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: middlePriority,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   asyncTestGroupFillRate,
					BurstLimit: asyncTestGroupFillRate,
				},
			},
		},
	}
}

func stopAsyncTestManager(m *Manager) {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

func TestAsyncLoadResourceGroupsLazyGet(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	re.NoError(store.SaveResourceGroupSetting(1, "lazy-group", newAsyncTestGroup("lazy-group")))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	// Unblock the async loader first (LIFO) so stopAsyncTestManager's wg.Wait()
	// cannot hang if a later assertion aborts the test before the explicit
	// store.unblock() call below is reached.
	defer store.unblock()

	store.waitEntered(t)

	_, err := m.GetResourceGroupList(1, false)
	re.ErrorIs(err, errs.ErrResourceGroupsLoading)

	group, err := m.GetResourceGroup(1, "lazy-group", false)
	re.NoError(err)
	re.NotNil(group)
	re.Equal("lazy-group", group.Name)
	re.Equal(float64(asyncTestGroupFillRate), group.RUSettings.RU.getFillRate())

	store.unblock()
	testutil.Eventually(re, func() bool {
		groups, err := m.GetResourceGroupList(1, false)
		return err == nil && len(groups) == 2
	}, testutil.WithTickInterval(20*time.Millisecond))
}

func TestAsyncLoadResourceGroupsDoesNotRestoreDeletedLazyGroup(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	re.NoError(store.SaveResourceGroupSetting(1, "deleted-group", newAsyncTestGroup("deleted-group")))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	// Unblock the async loader first (LIFO) so stopAsyncTestManager's wg.Wait()
	// cannot hang if a later assertion aborts the test before the explicit
	// store.unblock() call below is reached.
	defer store.unblock()

	store.waitEntered(t)

	group, err := m.GetResourceGroup(1, "deleted-group", false)
	re.NoError(err)
	re.NotNil(group)
	re.NoError(m.DeleteResourceGroup(1, "deleted-group"))

	store.unblock()
	testutil.Eventually(re, func() bool {
		groups, err := m.GetResourceGroupList(1, false)
		if err != nil {
			return false
		}
		for _, group := range groups {
			if group.Name == "deleted-group" {
				return false
			}
		}
		return true
	}, testutil.WithTickInterval(20*time.Millisecond))
}

// TestAsyncLoadResourceGroupsLazyGetLegacyKeyspace guards against the point
// loaders (LoadResourceGroupSetting/LoadResourceGroupState) diverging from
// the bulk loaders on legacy, pre-keyspace resource groups: those are saved
// under constant.NullKeyspaceID, and a lazy Get during async loading must be
// able to find one the same way the bulk scan would once it completes.
func TestAsyncLoadResourceGroupsLazyGetLegacyKeyspace(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	re.NoError(store.SaveResourceGroupSetting(constant.NullKeyspaceID, "legacy-group", newAsyncTestGroup("legacy-group")))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	defer store.unblock()

	store.waitEntered(t)

	group, err := m.GetResourceGroup(constant.NullKeyspaceID, "legacy-group", false)
	re.NoError(err)
	re.NotNil(group)
	re.Equal("legacy-group", group.Name)
	re.Equal(float64(asyncTestGroupFillRate), group.RUSettings.RU.getFillRate())

	store.unblock()
	testutil.Eventually(re, func() bool {
		group, err := m.GetResourceGroup(constant.NullKeyspaceID, "legacy-group", false)
		return err == nil && group != nil
	}, testutil.WithTickInterval(20*time.Millisecond))
}

// TestAsyncLoadResourceGroupsRecoversFromStateLoadFailure guards against a
// group getting stuck marked reserved forever after a transient
// LoadResourceGroupState failure during lazy loading: once the async bulk
// load subsequently installs the fully-loaded (settings and state)
// confirmed data for the same group, the reserved marker must be cleared,
// otherwise loadResourceGroupIfNeeded and the state persist loop would keep
// treating already-recovered, correct data as an unconfirmed placeholder.
func TestAsyncLoadResourceGroupsRecoversFromStateLoadFailure(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	group := newAsyncTestGroup("flaky-group")
	re.NoError(store.SaveResourceGroupSetting(1, "flaky-group", group))
	re.NoError(store.SaveResourceGroupStates(1, "flaky-group", FromProtoResourceGroup(group).GetGroupStates()))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	defer store.unblock()

	store.waitEntered(t)

	// Make the lazy load's own state read fail once, so the group is cached
	// as a metadata-only, still-reserved entry.
	store.failNextState.Store(true)
	fetched, err := m.GetResourceGroup(1, "flaky-group", false)
	re.NoError(err)
	re.NotNil(fetched)

	krgm := m.getKeyspaceResourceGroupManager(1)
	re.NotNil(krgm)
	re.True(krgm.isReserved("flaky-group"), "group should still be reserved after a failed state load")

	// Let the async bulk load proceed; its own state read is unaffected
	// (failNextState was already consumed) and should install confirmed data.
	store.unblock()
	testutil.Eventually(re, func() bool {
		return !krgm.isReserved("flaky-group")
	}, testutil.WithTickInterval(20*time.Millisecond))
}

// TestAsyncLoadResourceGroupsDeleteRaceDoesNotResurrect reproduces the
// lazy-load vs concurrent Delete race deterministically: a lazy load reads a
// group from storage, then a Delete removes it before the lazy load inserts.
// The stale insert must be rejected (via the delete-generation check) so the
// deleted group is not resurrected for the rest of the manager's lifetime.
func TestAsyncLoadResourceGroupsDeleteRaceDoesNotResurrect(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	group := newAsyncTestGroup("race-group")
	re.NoError(store.SaveResourceGroupSetting(1, "race-group", group))
	re.NoError(store.SaveResourceGroupStates(1, "race-group", FromProtoResourceGroup(group).GetGroupStates()))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	defer store.unblock()

	// Async bulk load is blocked, so loadingState stays in progress and lazy
	// loading is active.
	store.waitEntered(t)

	// Start a lazy Get that will pause inside its state read, i.e. after it has
	// read the group from storage but before it inserts into the cache.
	store.pausePointState.Store(true)
	var (
		gotGroup *ResourceGroup
		gotErr   error
	)
	getDone := make(chan struct{})
	go func() {
		defer close(getDone)
		gotGroup, gotErr = m.GetResourceGroup(1, "race-group", false)
	}()

	select {
	case <-store.pointReached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the lazy load to reach its state read")
	}

	// While the lazy load is paused, delete the group. Delete does its own
	// (unpaused) load-then-delete, removing it from storage and cache and
	// bumping the delete generation.
	re.NoError(m.DeleteResourceGroup(1, "race-group"))

	// Release the paused lazy load; its now-stale insert must be rejected.
	close(store.pointRelease)
	<-getDone
	// The generation mismatch makes the lazy load retry its storage read,
	// which now correctly observes the group as deleted.
	re.ErrorContains(gotErr, "does not exist")
	re.Nil(gotGroup, "the racing lazy load must observe the group as deleted")

	krgm := m.getKeyspaceResourceGroupManager(1)
	re.NotNil(krgm)
	re.Nil(krgm.getMutableResourceGroup("race-group"), "deleted group must not be resurrected by the racing lazy load")

	// Finishing async loading must not bring the deleted group back either.
	store.unblock()
	testutil.Eventually(re, func() bool {
		groups, err := m.GetResourceGroupList(1, false)
		if err != nil {
			return false
		}
		for _, g := range groups {
			if g.Name == "race-group" {
				return false
			}
		}
		return true
	}, testutil.WithTickInterval(20*time.Millisecond))
}

// TestAsyncLoadResourceGroupsStaleLoaderDoesNotPolluteNewTerm reproduces the
// stale-loader race: a loader from an old term is blocked in its storage scan
// while the leadership changes and Init runs again for a new term. When the
// old loader finally wakes up, it must not merge its stale scan into the new
// term's maps, clear the new term's syncLoadedGroups, or publish completion.
func TestAsyncLoadResourceGroupsStaleLoaderDoesNotPolluteNewTerm(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	group := newAsyncTestGroup("stale-group")
	re.NoError(store.SaveResourceGroupSetting(1, "stale-group", group))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	// Term 1: the loader blocks at the start of its settings scan.
	re.NoError(m.Init(context.Background()))
	cancelTerm1 := m.cancel
	defer stopAsyncTestManager(m)
	defer store.unblock()
	defer store.unblockStates()

	store.waitEntered(t)

	// Let the term-1 loader run its settings scan (capturing stale-group into
	// its temp result) and then block again in the states scan, i.e. after it
	// has read storage but before it merges.
	store.pauseNextStates.Store(true)
	store.unblock()
	select {
	case <-store.statesReached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the term-1 loader to reach its states scan")
	}

	// Leadership changes: cancel term 1 and reinitialize for term 2. The
	// term-2 loader hits neither block (both were consumed) and completes.
	cancelTerm1()
	re.NoError(m.Init(context.Background()))
	testutil.Eventually(re, func() bool {
		groups, err := m.GetResourceGroupList(1, false)
		return err == nil && len(groups) == 2
	}, testutil.WithTickInterval(20*time.Millisecond))

	// Delete the group in term 2, after loading completed.
	re.NoError(m.DeleteResourceGroup(1, "stale-group"))

	// Release the stale term-1 loader. It must observe its cancelled context /
	// stale epoch and exit without resurrecting the deleted group or touching
	// the new term's loading state.
	store.unblockStates()
	time.Sleep(200 * time.Millisecond)

	krgm := m.getKeyspaceResourceGroupManager(1)
	re.NotNil(krgm)
	re.Nil(krgm.getMutableResourceGroup("stale-group"), "stale loader must not merge into the new term")
	groups, err := m.GetResourceGroupList(1, false)
	re.NoError(err)
	for _, g := range groups {
		re.NotEqual("stale-group", g.Name)
	}
}

// TestAsyncLoadResourceGroupsMergeKeepsModifiedSettings reproduces the
// stale-settings clobber: a group's lazy load reads its settings but fails to
// read its state, then the group is modified (and the modification persisted)
// while the async bulk scan still holds the pre-modification settings. The
// bulk merge must adopt only the scanned state into the cached entry, not
// replace it wholesale, so the modified settings survive in the serving cache.
func TestAsyncLoadResourceGroupsMergeKeepsModifiedSettings(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	group := newAsyncTestGroup("mod-group")
	re.NoError(store.SaveResourceGroupSetting(1, "mod-group", group))
	// Persist a state with a recognizable consumption so the test can tell
	// that the merge really adopted the scanned state.
	states := FromProtoResourceGroup(group).GetGroupStates()
	states.RUConsumption.RRU = 123
	re.NoError(store.SaveResourceGroupStates(1, "mod-group", states))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	defer store.unblock()
	defer store.unblockStates()

	store.waitEntered(t)

	// Let the bulk loader capture the pre-modification settings, then hold it
	// right before its states scan (i.e. before it merges).
	store.pauseNextStates.Store(true)
	store.unblock()
	select {
	case <-store.statesReached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the bulk loader to reach its states scan")
	}

	// Lazily load the group with a failing state read: it is cached with
	// confirmed settings but unconfirmed (fresh) state.
	store.failNextState.Store(true)
	fetched, err := m.GetResourceGroup(1, "mod-group", false)
	re.NoError(err)
	re.NotNil(fetched)
	krgm := m.getKeyspaceResourceGroupManager(1)
	re.NotNil(krgm)
	re.True(krgm.isReserved("mod-group"))

	// Modify the group (fill rate 100 -> 200) and persist it. The state read
	// of Modify's own lazy load fails again, so the entry stays state-only
	// reserved and out of syncLoadedGroups.
	store.failNextState.Store(true)
	modified := newAsyncTestGroup("mod-group")
	modified.RUSettings.RU.Settings.FillRate = 200
	modified.KeyspaceId = &resource_manager.KeyspaceIDValue{Value: 1}
	re.NoError(m.ModifyResourceGroup(modified))

	// Release the bulk loader; its merge must keep the modified settings and
	// only adopt the scanned state.
	store.unblockStates()
	testutil.Eventually(re, func() bool {
		_, err := m.GetResourceGroupList(1, false)
		return err == nil
	}, testutil.WithTickInterval(20*time.Millisecond))

	got, err := m.GetResourceGroup(1, "mod-group", false)
	re.NoError(err)
	re.NotNil(got)
	re.Equal(float64(200), got.RUSettings.RU.getFillRate(), "modified settings must survive the bulk merge")
	re.False(krgm.isReserved("mod-group"), "state adoption must clear the reserved marker")
	re.Equal(float64(123), krgm.getMutableResourceGroup("mod-group").GetGroupStates().RUConsumption.RRU,
		"the scanned state must be adopted into the cached entry")
}

// TestAsyncLoadResourceGroupsFreshStoreDefaultPersisted guards against the
// fresh-store dead end: initReservedInCache pre-inserts a synthetic default
// placeholder, and on a store with nothing persisted, the confirmed-not-found
// fallback used to bail out on its cache-exists check, leaving the default
// group an unconfirmed placeholder forever — settings never persisted and its
// state persistence permanently skipped.
func TestAsyncLoadResourceGroupsFreshStoreDefaultPersisted(t *testing.T) {
	re := require.New(t)
	// A completely fresh store: nothing persisted at all.
	store := newBlockingResourceGroupStorage()

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	defer store.unblock()

	store.waitEntered(t)

	// Fetch the default group while async loading is still in progress: the
	// point load confirms nothing is persisted, so the placeholder must be
	// promoted to a real, persisted default group.
	group, err := m.GetResourceGroup(constant.NullKeyspaceID, DefaultResourceGroupName, false)
	re.NoError(err)
	re.NotNil(group)
	krgm := m.getKeyspaceResourceGroupManager(constant.NullKeyspaceID)
	re.NotNil(krgm)
	re.False(krgm.isReserved(DefaultResourceGroupName), "the default group must be confirmed after synthesis")
	raw, err := store.LoadResourceGroupSetting(constant.NullKeyspaceID, DefaultResourceGroupName)
	re.NoError(err)
	re.NotEmpty(raw, "the synthesized default group settings must be persisted")

	// Loading completion must keep it confirmed.
	store.unblock()
	testutil.Eventually(re, func() bool {
		_, err := m.GetResourceGroupList(constant.NullKeyspaceID, false)
		return err == nil
	}, testutil.WithTickInterval(20*time.Millisecond))
	re.False(krgm.isReserved(DefaultResourceGroupName))
}

// TestAsyncLoadResourceGroupsUnrelatedDeleteDoesNotFailLazyLoad guards against
// the delete-generation check being too coarse: deleting group B while group A
// is being lazily loaded must not make A's request spuriously report the group
// as missing — the lazy load retries its storage read and succeeds.
func TestAsyncLoadResourceGroupsUnrelatedDeleteDoesNotFailLazyLoad(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	re.NoError(store.SaveResourceGroupSetting(1, "group-a", newAsyncTestGroup("group-a")))
	re.NoError(store.SaveResourceGroupSetting(1, "group-b", newAsyncTestGroup("group-b")))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	defer store.unblock()

	store.waitEntered(t)

	// Start a lazy Get of group-a and pause it inside its state read, i.e.
	// after it has read the group from storage but before it inserts.
	store.pausePointState.Store(true)
	var (
		gotGroup *ResourceGroup
		gotErr   error
	)
	getDone := make(chan struct{})
	go func() {
		defer close(getDone)
		gotGroup, gotErr = m.GetResourceGroup(1, "group-a", false)
	}()
	select {
	case <-store.pointReached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the lazy load to reach its state read")
	}

	// Delete the unrelated group-b while group-a's lazy load is paused; this
	// bumps the keyspace's delete generation.
	re.NoError(m.DeleteResourceGroup(1, "group-b"))

	// Release group-a's lazy load: the generation mismatch must make it retry
	// and succeed, not report group-a as missing.
	close(store.pointRelease)
	<-getDone
	re.NoError(gotErr)
	re.NotNil(gotGroup, "an unrelated delete must not fail the lazy load")
	re.Equal("group-a", gotGroup.Name)

	// After loading completes, group-a is present and group-b stays deleted.
	store.unblock()
	testutil.Eventually(re, func() bool {
		groups, err := m.GetResourceGroupList(1, false)
		if err != nil {
			return false
		}
		foundA := false
		for _, g := range groups {
			if g.Name == "group-b" {
				return false
			}
			if g.Name == "group-a" {
				foundA = true
			}
		}
		return foundA
	}, testutil.WithTickInterval(20*time.Millisecond))
}
