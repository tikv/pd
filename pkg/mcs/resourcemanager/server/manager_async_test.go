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

	"github.com/pingcap/failpoint"
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

	// statePause, when armed via armStatePause, makes the next
	// LoadResourceGroupState call for the armed group name signal reached and
	// then block on release, so a test can hold a lazy load right after its
	// storage read but before it inserts. Re-armable, and filtered by name so
	// unrelated groups' loads pass through undisturbed.
	statePause atomic.Pointer[statePause]

	// pauseNextStates, when true, makes the very next bulk
	// LoadResourceGroupStates call signal statesReached and then block on
	// statesRelease, so a test can hold an async loader after it has captured
	// the settings scan but before it merges.
	pauseNextStates   atomic.Bool
	statesReached     chan struct{}
	statesRelease     chan struct{}
	statesReleaseOnce sync.Once
}

type statePause struct {
	name    string
	reached chan struct{}
	release chan struct{}
}

func newBlockingResourceGroupStorage() *blockingResourceGroupStorage {
	return &blockingResourceGroupStorage{
		Storage:       storage.NewStorageWithMemoryBackend(),
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
		statesReached: make(chan struct{}),
		statesRelease: make(chan struct{}),
	}
}

// armStatePause arms a one-shot pause on the next LoadResourceGroupState call
// for the given group name and returns the pause handle. The test must wait
// on reached and eventually close release.
func (s *blockingResourceGroupStorage) armStatePause(name string) *statePause {
	p := &statePause{name: name, reached: make(chan struct{}), release: make(chan struct{})}
	s.statePause.Store(p)
	return p
}

func waitStatePauseReached(t *testing.T, p *statePause) {
	t.Helper()
	select {
	case <-p.reached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the lazy load to reach its state read")
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
	if p := s.statePause.Load(); p != nil && p.name == name && s.statePause.CompareAndSwap(p, nil) {
		close(p.reached)
		<-p.release
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

// TestAsyncLoadResourceGroupsDoesNotServeStateLoadFailure guards against a
// group with confirmed settings but failed state loading being exposed with a
// fresh token bucket before the async bulk loader can recover its persisted
// state.
func TestAsyncLoadResourceGroupsDoesNotServeStateLoadFailure(t *testing.T) {
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

	// Make the lazy load's own state read fail once. The group must remain
	// unavailable rather than being returned with a fresh token bucket state.
	store.failNextState.Store(true)
	fetched, err := m.GetResourceGroup(1, "flaky-group", false)
	re.Error(err)
	re.Nil(fetched)

	krgm := m.getKeyspaceResourceGroupManager(1)
	re.NotNil(krgm)
	re.Nil(krgm.getMutableResourceGroup("flaky-group"), "failed state load must not publish the group")

	// Let the async bulk load proceed; its own state read is unaffected
	// (failNextState was already consumed) and should install confirmed data.
	store.unblock()
	testutil.Eventually(re, func() bool {
		return krgm.getMutableResourceGroup("flaky-group") != nil && !krgm.isReserved("flaky-group")
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
	pause := store.armStatePause("race-group")
	var (
		gotGroup *ResourceGroup
		gotErr   error
	)
	getDone := make(chan struct{})
	go func() {
		defer close(getDone)
		gotGroup, gotErr = m.GetResourceGroup(1, "race-group", false)
	}()

	waitStatePauseReached(t, pause)

	// While the lazy load is paused, delete the group. Delete does its own
	// (unpaused) load-then-delete, removing it from storage and cache and
	// bumping the delete generation.
	re.NoError(m.DeleteResourceGroup(1, "race-group"))

	// Release the paused lazy load; its now-stale insert must be rejected.
	close(pause.release)
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

// TestAsyncLoadResourceGroupsLazyPublishAndMarkAreAtomic reproduces the race
// where a lazy load publishes a cache entry before recording it in
// syncLoadedGroups. A bulk merge entering that gap can overwrite mutable state
// updated by token-bucket handling with its older scan result.
func TestAsyncLoadResourceGroupsLazyPublishAndMarkAreAtomic(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	group := newAsyncTestGroup("atomic-group")
	re.NoError(store.SaveResourceGroupSetting(1, "atomic-group", group))
	re.NoError(store.SaveResourceGroupStates(1, "atomic-group", FromProtoResourceGroup(group).GetGroupStates()))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	defer store.unblock()
	defer store.unblockStates()

	store.waitEntered(t)

	// Let the bulk loader capture fill rate 100, then hold it before merge.
	store.pauseNextStates.Store(true)
	store.unblock()
	select {
	case <-store.statesReached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the bulk loader to reach its states scan")
	}

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/lazyLoadAfterCachePublish", `pause`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/lazyLoadAfterCachePublish"))
	}()

	getDone := make(chan struct{})
	go func() {
		defer close(getDone)
		_, err := m.GetResourceGroup(1, "atomic-group", false)
		re.NoError(err)
	}()

	var krgm *keyspaceResourceGroupManager
	testutil.Eventually(re, func() bool {
		krgm = m.getKeyspaceResourceGroupManager(1)
		if krgm == nil {
			return false
		}
		return krgm.getMutableResourceGroup("atomic-group") != nil
	}, testutil.WithTickInterval(20*time.Millisecond))

	krgm.getMutableResourceGroup("atomic-group").UpdateRUConsumption(&resource_manager.Consumption{RRU: 10})

	// With the lazy load paused at the cache-publish hook, the bulk merge must
	// not be able to overwrite the updated cache entry.
	store.unblockStates()
	testutil.Eventually(re, func() bool {
		_, err := m.GetResourceGroupList(1, false)
		return err == nil
	}, testutil.WithTickInterval(20*time.Millisecond))

	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/lazyLoadAfterCachePublish"))
	<-getDone

	got, err := m.GetResourceGroup(1, "atomic-group", false)
	re.NoError(err)
	re.NotNil(got)
	re.Equal(float64(10), krgm.getMutableResourceGroup("atomic-group").GetGroupStates().RUConsumption.RRU,
		"bulk merge must not overwrite the published lazy-loaded group")
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
	pause := store.armStatePause("group-a")
	var (
		gotGroup *ResourceGroup
		gotErr   error
	)
	getDone := make(chan struct{})
	go func() {
		defer close(getDone)
		gotGroup, gotErr = m.GetResourceGroup(1, "group-a", false)
	}()
	waitStatePauseReached(t, pause)

	// Delete the unrelated group-b while group-a's lazy load is paused; this
	// bumps the keyspace's delete generation.
	re.NoError(m.DeleteResourceGroup(1, "group-b"))

	// Release group-a's lazy load: the generation mismatch must make it retry
	// and succeed, not report group-a as missing.
	close(pause.release)
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

// TestAsyncLoadResourceGroupsStaleLazyLoadRetriesNewTerm reproduces the
// cross-term lazy load: the load captures the keyspace manager, then blocks in
// its storage read while the leadership changes and Init replaces m.krgms and
// syncLoadedGroups. On resume it must not publish into the detached old
// manager while marking the group in the new term's map (which would make the
// new bulk merge skip a group its cache doesn't contain); instead it retries
// against the freshly captured state and publishes into the new term.
func TestAsyncLoadResourceGroupsStaleLazyLoadRetriesNewTerm(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	re.NoError(store.SaveResourceGroupSetting(1, "cross-term", newAsyncTestGroup("cross-term")))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	cancelTerm1 := m.cancel
	defer stopAsyncTestManager(m)
	defer store.unblock()

	store.waitEntered(t)

	// The term-1 lazy load pauses inside its state read, holding the old
	// term's keyspace manager.
	pause := store.armStatePause("cross-term")
	var (
		gotGroup *ResourceGroup
		gotErr   error
	)
	getDone := make(chan struct{})
	go func() {
		defer close(getDone)
		gotGroup, gotErr = m.GetResourceGroup(1, "cross-term", false)
	}()
	waitStatePauseReached(t, pause)

	// Leadership changes: reinitialize the manager for term 2 while the
	// term-1 lazy load is still blocked. Term 2's bulk loader parks on the
	// same settings-scan block until store.unblock().
	cancelTerm1()
	re.NoError(m.Init(context.Background()))

	// Release the stale lazy load: it must detect the term change, retry, and
	// publish into the new term's manager, so the request still succeeds.
	close(pause.release)
	<-getDone
	re.NoError(gotErr)
	re.NotNil(gotGroup, "the cross-term lazy load must retry and succeed against the new term")
	re.Equal("cross-term", gotGroup.Name)

	// Finish loading; the group must remain present after the term-2 bulk
	// merge (it was correctly marked in the same term it was published in).
	store.unblock()
	testutil.Eventually(re, func() bool {
		g, err := m.GetResourceGroup(1, "cross-term", false)
		return err == nil && g != nil
	}, testutil.WithTickInterval(20*time.Millisecond))
}

// TestAsyncLoadResourceGroupsCrossTermDeletePublishesToNewTerm reproduces the
// cross-term delete race: a Delete resolves its keyspace manager, then stalls
// before its storage phase while the leadership changes and the new term's
// bulk loader snapshots storage (still containing the group). When the Delete
// resumes, it removes the group from storage but its cache effect and
// sync-loaded marker must land in the *current* term — otherwise the new
// merge would reinstall its pre-deletion snapshot and the API would report
// success while the group stays in the live cache.
func TestAsyncLoadResourceGroupsCrossTermDeletePublishesToNewTerm(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	re.NoError(store.SaveResourceGroupSetting(1, "ct-del", newAsyncTestGroup("ct-del")))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	cancelTerm1 := m.cancel
	defer stopAsyncTestManager(m)
	defer store.unblock()
	defer store.unblockStates()

	// Let term 1 load fully so the Delete starts against a settled term.
	store.waitEntered(t)
	store.unblock()
	testutil.Eventually(re, func() bool {
		groups, err := m.GetResourceGroupList(1, false)
		return err == nil && len(groups) == 2
	}, testutil.WithTickInterval(20*time.Millisecond))

	// Park the Delete between resolving its keyspace manager and its storage
	// phase.
	reached := make(chan struct{})
	release := make(chan struct{})
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/pkg/mcs/resourcemanager/server/deleteResourceGroupBeforeStorage", func() {
		close(reached)
		<-release
	}))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/deleteResourceGroupBeforeStorage"))
	}()
	var delErr error
	delDone := make(chan struct{})
	go func() {
		defer close(delDone)
		delErr = m.DeleteResourceGroup(1, "ct-del")
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the delete to reach its storage phase")
	}

	// Leadership changes: term 2's loader snapshots storage (the group is
	// still there) and parks before merging.
	cancelTerm1()
	store.pauseNextStates.Store(true)
	re.NoError(m.Init(context.Background()))
	select {
	case <-store.statesReached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the term-2 loader to snapshot storage")
	}

	// Resume the Delete: storage removal proceeds, and the cache effect and
	// marker must be published into term 2, not the detached term-1 manager.
	close(release)
	<-delDone
	re.NoError(delErr)

	// Let term 2 merge its pre-deletion snapshot; the marker must make it
	// skip the deleted group.
	store.unblockStates()
	testutil.Eventually(re, func() bool {
		groups, err := m.GetResourceGroupList(1, false)
		if err != nil {
			return false
		}
		for _, g := range groups {
			if g.Name == "ct-del" {
				return false
			}
		}
		return true
	}, testutil.WithTickInterval(20*time.Millisecond))
	g, err := m.GetResourceGroup(1, "ct-del", false)
	re.NoError(err)
	re.Nil(g, "the deleted group must not be resurrected by the new term's merge")
}

// TestAsyncLoadResourceGroupsExhaustedRetriesReturnLoadingError guards the
// exhausted-retry path of the lazy load: when every attempt loses the
// delete-generation race, the load must fail with a retryable loading error
// instead of reporting success without publishing the group, which callers
// would misread as the group not existing.
func TestAsyncLoadResourceGroupsExhaustedRetriesReturnLoadingError(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	re.NoError(store.SaveResourceGroupSetting(1, "keep-a", newAsyncTestGroup("keep-a")))
	for _, name := range []string{"del-b1", "del-b2", "del-b3"} {
		re.NoError(store.SaveResourceGroupSetting(1, name, newAsyncTestGroup(name)))
	}

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	defer store.unblock()

	// Keep the bulk loader parked so lazy loading stays active.
	store.waitEntered(t)

	// Park keep-a's lazy load inside each of its three read attempts, and
	// delete an unrelated group while it's parked so every attempt observes a
	// delete-generation change.
	pause := store.armStatePause("keep-a")
	var (
		gotGroup *ResourceGroup
		gotErr   error
	)
	getDone := make(chan struct{})
	go func() {
		defer close(getDone)
		gotGroup, gotErr = m.GetResourceGroup(1, "keep-a", false)
	}()
	for _, victim := range []string{"del-b1", "del-b2", "del-b3"} {
		waitStatePauseReached(t, pause)
		re.NoError(m.DeleteResourceGroup(1, victim))
		next := store.armStatePause("keep-a")
		close(pause.release)
		pause = next
	}
	<-getDone
	// The last arm is left unconsumed; drop it so later loads pass through.
	store.statePause.Store(nil)

	re.ErrorIs(gotErr, errs.ErrResourceGroupsLoading,
		"exhausted retries must surface a retryable loading error, not a bogus success")
	re.Nil(gotGroup)

	// The group still exists; once loading completes it must be served again.
	store.unblock()
	testutil.Eventually(re, func() bool {
		g, err := m.GetResourceGroup(1, "keep-a", false)
		return err == nil && g != nil
	}, testutil.WithTickInterval(20*time.Millisecond))
}

// TestAsyncLoadResourceGroupsCrossTermModifyDefaultStaysConfirmed guards the
// Modify-of-default publish path across a leadership change. A Modify patches
// and persists the default group in term 1, then stalls before publishing.
// Term 2 reinitializes the manager, giving it a fresh reserved default
// placeholder. When the Modify resumes, publishing must leave the group
// confirmed (not reserved) in the live term with the modified settings -
// otherwise it stays a reserved placeholder that the bulk merge or
// initReserved can revert to a pre-modification/synthetic default while
// storage keeps the new value.
func TestAsyncLoadResourceGroupsCrossTermModifyDefaultStaysConfirmed(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	// Seed a persisted default with a recognizable running state so the test
	// can tell a confirmed republish (state preserved) from a synthetic
	// placeholder (state reset).
	seed := newAsyncTestGroup(DefaultResourceGroupName)
	re.NoError(store.SaveResourceGroupSetting(constant.NullKeyspaceID, DefaultResourceGroupName, seed))
	seedStates := FromProtoResourceGroup(seed).GetGroupStates()
	seedStates.RUConsumption.RRU = 777
	re.NoError(store.SaveResourceGroupStates(constant.NullKeyspaceID, DefaultResourceGroupName, seedStates))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	cancelTerm1 := m.cancel
	defer stopAsyncTestManager(m)
	defer store.unblock()
	defer store.unblockStates()

	// Let term 1 load fully so the default group is confirmed and persisted.
	store.waitEntered(t)
	store.unblock()
	testutil.Eventually(re, func() bool {
		_, err := m.GetResourceGroupList(constant.NullKeyspaceID, false)
		return err == nil
	}, testutil.WithTickInterval(20*time.Millisecond))

	// Park the Modify after it patched and persisted, before it publishes.
	reached := make(chan struct{})
	release := make(chan struct{})
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/pkg/mcs/resourcemanager/server/modifyResourceGroupBeforePublish", func() {
		close(reached)
		<-release
	}))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/modifyResourceGroupBeforePublish"))
	}()
	modified := newAsyncTestGroup(DefaultResourceGroupName)
	modified.RUSettings.RU.Settings.FillRate = 4242
	var modErr error
	modDone := make(chan struct{})
	go func() {
		defer close(modDone)
		modErr = m.ModifyResourceGroup(modified)
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the modify to reach its publish phase")
	}

	// Leadership changes: term 2 gets a fresh reserved default placeholder,
	// with its loader parked before merging.
	cancelTerm1()
	store.pauseNextStates.Store(true)
	re.NoError(m.Init(context.Background()))
	select {
	case <-store.statesReached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the term-2 loader to snapshot storage")
	}

	// Resume the Modify's publish into term 2.
	close(release)
	<-modDone
	re.NoError(modErr)

	// The default in term 2 must be confirmed (not a reserved placeholder),
	// so neither the merge nor initReserved reverts the modified settings.
	krgm := m.getKeyspaceResourceGroupManager(constant.NullKeyspaceID)
	re.NotNil(krgm)
	re.False(krgm.isReserved(DefaultResourceGroupName),
		"a modified default must be published as confirmed data, not left reserved")

	store.unblockStates()
	testutil.Eventually(re, func() bool {
		_, err := m.GetResourceGroupList(constant.NullKeyspaceID, false)
		return err == nil
	}, testutil.WithTickInterval(20*time.Millisecond))
	g, err := m.GetResourceGroup(constant.NullKeyspaceID, DefaultResourceGroupName, false)
	re.NoError(err)
	re.NotNil(g)
	re.Equal(float64(4242), g.RUSettings.RU.getFillRate(),
		"the modified default settings must survive into the new term")
	// The confirmed running state must survive too, not revert to the fresh
	// synthetic placeholder state.
	re.Equal(float64(777), krgm.getMutableResourceGroup(DefaultResourceGroupName).GetGroupStates().RUConsumption.RRU,
		"the confirmed running state must be preserved, not reset to a synthetic placeholder")
}
