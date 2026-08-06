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
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

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

func (s *blockingResourceGroupStorage) waitEntered(tb testing.TB) {
	tb.Helper()
	select {
	case <-s.entered:
	case <-time.After(5 * time.Second):
		tb.Fatal("timed out waiting for async resource group loading")
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

// TestInitDefaultResourceGroupMarksAtomicallyWithPublish guards against a
// window where a synthesized default group became visible in the cache
// before its sync-loaded marker was set. initDefaultResourceGroup used to
// publish through krgm's own lock directly, with the caller (loadResourceGroupIfNeeded)
// setting the sync-loaded marker afterward in a separate, later m.Lock()
// critical section. A concurrent bulk-merge batch - which only skips an item
// already marked - could run in that window and overwrite the synthesized
// group, including any live consumption/token update applied to it in the
// meantime, with its own possibly-stale scanned copy. initDefaultResourceGroup
// now publishes through publishResourceGroupMutation, which holds m.Lock()
// across both the cache-visibility change and the marker set, so nothing
// that also needs m.Lock() - including a merge batch - can ever observe one
// without the other.
func TestInitDefaultResourceGroupMarksAtomicallyWithPublish(t *testing.T) {
	re := require.New(t)
	m := prepareManager()
	const keyspaceID = 1
	krgm := newKeyspaceResourceGroupManager(keyspaceID, m.storage, m.writeRole)
	m.krgms[keyspaceID] = krgm

	reached := make(chan struct{})
	release := make(chan struct{})
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/pkg/mcs/resourcemanager/server/publishMutationBeforeMark", func() {
		close(reached)
		<-release
	}))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/publishMutationBeforeMark"))
	}()

	var initErr error
	initDone := make(chan struct{})
	go func() {
		defer close(initDone)
		_, initErr = m.initDefaultResourceGroup(keyspaceID, krgm, nil)
	}()

	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initDefaultResourceGroup to reach its publish")
	}

	// While parked between the cache-visibility change and the marker set,
	// nothing that needs m.Lock() (e.g. a concurrent merge batch checking
	// whether to skip this group) should be able to proceed.
	checkDone := make(chan bool)
	go func() {
		m.RLock()
		_, marked := m.syncLoadedGroups[trackerKey{keyspaceID: keyspaceID, groupName: DefaultResourceGroupName}]
		m.RUnlock()
		checkDone <- marked
	}()

	select {
	case <-checkDone:
		t.Fatal("a concurrent m.Lock()-holding operation must not proceed while the publish is parked between visibility and marking")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	<-initDone
	re.NoError(initErr)
	marked := <-checkDone
	re.True(marked, "the marker must already be set by the time any concurrent m.Lock() holder can observe the published group")
}

// TestAsyncLoadResourceGroupsCrossTermSetServiceLimitPublishesToNewTerm
// reproduces a leadership change straddling SetKeyspaceServiceLimit: it
// resolves a keyspace manager, then stalls before its storage phase while the
// leadership change replaces m.krgms and the new term's synchronous
// loadServiceLimits reads the still-unwritten old value. When the call
// resumes, its storage write must still land, and its cache effect must be
// mirrored into the *current* term's keyspace manager, not just the detached
// term-1 one - otherwise the new term keeps serving the stale (default zero)
// limit indefinitely, with nothing to ever re-sync it.
func TestAsyncLoadResourceGroupsCrossTermSetServiceLimitPublishesToNewTerm(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	cancelTerm1 := m.cancel
	defer stopAsyncTestManager(m)
	defer store.unblock()

	// Let term 1 load fully so the call starts against a settled term.
	store.waitEntered(t)
	store.unblock()
	testutil.Eventually(re, func() bool {
		_, err := m.GetResourceGroupList(constant.NullKeyspaceID, false)
		return err == nil
	}, testutil.WithTickInterval(20*time.Millisecond))

	// Park SetKeyspaceServiceLimit between resolving its keyspace manager and
	// its storage phase.
	reached := make(chan struct{})
	release := make(chan struct{})
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/pkg/mcs/resourcemanager/server/setServiceLimitBeforeStorage", func() {
		close(reached)
		<-release
	}))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/setServiceLimitBeforeStorage"))
	}()
	var setErr error
	setDone := make(chan struct{})
	go func() {
		defer close(setDone)
		setErr = m.SetKeyspaceServiceLimit(constant.NullKeyspaceID, 4242)
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SetKeyspaceServiceLimit to reach its storage phase")
	}

	// Leadership changes while the write is parked: term 2's synchronous
	// loadServiceLimits runs and completes here, reading storage before the
	// parked write has persisted anything.
	cancelTerm1()
	re.NoError(m.Init(context.Background()))

	// Resume the write: it persists into storage now, then must publish into
	// whichever keyspace manager is current (term 2), not the detached term-1
	// one.
	close(release)
	<-setDone
	re.NoError(setErr)

	limiter := m.GetKeyspaceServiceLimiter(constant.NullKeyspaceID)
	re.NotNil(limiter)
	re.Equal(float64(4242), limiter.ServiceLimit,
		"the service limit set during the leadership change must be visible in the new term, not stuck on the detached old one")

	raw, err := store.LoadServiceLimit(constant.NullKeyspaceID)
	re.NoError(err)
	re.Equal(float64(4242), raw, "the service limit must be persisted regardless of the leadership change")
}

// TestAsyncLoadResourceGroupsCrossTermSetServiceLimitSerializesAgainstCompetingCall
// guards against the unconditional publish-phase mirror in
// SetKeyspaceServiceLimit clobbering a competing, fully-completed call for
// the same keyspace. Without serviceLimitLocks, an old-term call parked
// mid-persist could resume after a new-term call for the same keyspace
// already persisted and published its own value, and mirror its own older
// value back in - leaving the live cache stale even though storage (and
// every other observer) already moved on. serviceLimitLocks makes the
// new-term call wait for the old one to fully finish, including its own
// mirror step, before it can even start, so this interleaving can no longer
// happen.
func TestAsyncLoadResourceGroupsCrossTermSetServiceLimitSerializesAgainstCompetingCall(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	cancelTerm1 := m.cancel
	defer stopAsyncTestManager(m)
	defer store.unblock()

	store.waitEntered(t)
	store.unblock()
	testutil.Eventually(re, func() bool {
		_, err := m.GetResourceGroupList(constant.NullKeyspaceID, false)
		return err == nil
	}, testutil.WithTickInterval(20*time.Millisecond))

	// Park the old-term call between resolving its keyspace manager and its
	// storage phase - it already holds serviceLimitLocks for this keyspace
	// by this point. The new-term call goes through this same, still-armed
	// failpoint too once serviceLimitLocks lets it proceed; parkOnce keeps
	// only the first (old-term) hit actually parking, so the second one
	// passes straight through instead of trying to close(reached) again.
	reached := make(chan struct{})
	release := make(chan struct{})
	var parkOnce sync.Once
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/pkg/mcs/resourcemanager/server/setServiceLimitBeforeStorage", func() {
		parkOnce.Do(func() {
			close(reached)
			<-release
		})
	}))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/setServiceLimitBeforeStorage"))
	}()
	var oldErr error
	oldDone := make(chan struct{})
	go func() {
		defer close(oldDone)
		oldErr = m.SetKeyspaceServiceLimit(constant.NullKeyspaceID, 100)
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the old-term call to reach its storage phase")
	}

	// Leadership changes while the old-term call is parked.
	cancelTerm1()
	re.NoError(m.Init(context.Background()))

	// A new-term call for the same keyspace must not be able to run while the
	// old-term call still holds serviceLimitLocks for it: it should block
	// before even reaching the (still-armed) failpoint above, since
	// serviceLimitLocks is acquired first.
	var newErr error
	newDone := make(chan struct{})
	go func() {
		defer close(newDone)
		newErr = m.SetKeyspaceServiceLimit(constant.NullKeyspaceID, 200)
	}()
	select {
	case <-newDone:
		t.Fatal("the new-term call must be blocked by serviceLimitLocks while the old-term call is still parked")
	case <-time.After(100 * time.Millisecond):
	}

	// Resume the old-term call: it persists and publishes 100, then releases
	// serviceLimitLocks, letting the new-term call finally run and persist
	// and publish 200.
	close(release)
	<-oldDone
	re.NoError(oldErr)
	<-newDone
	re.NoError(newErr)

	limiter := m.GetKeyspaceServiceLimiter(constant.NullKeyspaceID)
	re.NotNil(limiter)
	re.Equal(float64(200), limiter.ServiceLimit,
		"the new-term call must run - and win - only after the old-term call fully finished, not race ahead of or get clobbered by it")
}

// TestLoadServiceLimitsDoesNotClobberConcurrentSet guards against a race
// between loadServiceLimits' bulk replay (run on every Init/leadership
// change) and a concurrent SetKeyspaceServiceLimit call for the same
// keyspace. loadServiceLimits used to apply the value its bulk storage scan
// had already read before doing any locking; if a concurrent
// SetKeyspaceServiceLimit call persisted and mirrored a newer value while the
// replay's callback for that keyspace was still in flight, the replay would
// silently overwrite the cache with its own now-stale snapshot, leaving the
// cache stuck behind storage until the next full reload. serviceLimitLocks
// alone (the fix for the sibling cross-term race above) does not close this:
// the stale value here was captured before any lock was ever taken, so
// merely serializing the two callers does not stop the replay from applying
// data that was already out of date the moment it read it. The fix instead
// re-reads the keyspace's service limit from storage under the same lock,
// discarding the bulk scan's value entirely.
func TestLoadServiceLimitsDoesNotClobberConcurrentSet(t *testing.T) {
	re := require.New(t)
	m := prepareManager()
	const keyspaceID = 1
	re.NoError(m.storage.SaveServiceLimit(keyspaceID, 100))

	reached := make(chan struct{})
	release := make(chan struct{})
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/pkg/mcs/resourcemanager/server/loadServiceLimitsBeforeApply", func(gotKeyspaceID uint32) {
		if gotKeyspaceID != keyspaceID {
			return
		}
		close(reached)
		<-release
	}))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/loadServiceLimitsBeforeApply"))
	}()

	loadDone := make(chan struct{})
	go func() {
		defer close(loadDone)
		re.NoError(m.loadServiceLimits())
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loadServiceLimits to reach its replay callback")
	}

	// While the replay is parked - holding only the (100) value its scan
	// already captured - a concurrent SetKeyspaceServiceLimit call persists
	// and mirrors a newer value into the same, live keyspace manager.
	re.NoError(m.SetKeyspaceServiceLimit(keyspaceID, 200))

	close(release)
	<-loadDone

	limiter := m.GetKeyspaceServiceLimiter(keyspaceID)
	re.NotNil(limiter)
	re.Equal(float64(200), limiter.ServiceLimit,
		"the replay must not overwrite a concurrently-set newer value with its own stale snapshot")
}

// failingServiceLimitLoadStorage makes LoadServiceLimit (the point re-read
// loadServiceLimits performs under serviceLimitLocks) fail a controlled
// number of times for one keyspace, to exercise its retry-then-fallback
// behavior. LoadServiceLimits (the bulk scan) is left untouched.
type failingServiceLimitLoadStorage struct {
	storage.Storage
	keyspaceID   uint32
	failuresLeft atomic.Int32
	calls        atomic.Int32
}

func (s *failingServiceLimitLoadStorage) LoadServiceLimit(keyspaceID uint32) (float64, error) {
	if keyspaceID == s.keyspaceID {
		s.calls.Add(1)
		if s.failuresLeft.Add(-1) >= 0 {
			return 0, errors.New("injected service limit load failure")
		}
	}
	return s.Storage.LoadServiceLimit(keyspaceID)
}

// TestLoadServiceLimitsRetriesPointReadBeforeFallingBack guards against
// loadServiceLimits treating a single point-read failure as fatal. The point
// re-read added to close the race in TestLoadServiceLimitsDoesNotClobberConcurrentSet
// above can itself fail transiently (e.g. a storage blip), so it must retry a
// few times and apply the value once a retry succeeds, instead of giving up
// and falling back to the (potentially stale) bulk-scanned value on the very
// first failure.
func TestLoadServiceLimitsRetriesPointReadBeforeFallingBack(t *testing.T) {
	re := require.New(t)
	const keyspaceID = 1
	base := storage.NewStorageWithMemoryBackend()
	re.NoError(base.SaveServiceLimit(keyspaceID, 100))
	store := &failingServiceLimitLoadStorage{Storage: base, keyspaceID: keyspaceID}
	// Fail once, then succeed on the second attempt - strictly fewer calls
	// than the retry budget, to prove it stops retrying once a read succeeds
	// instead of always spending the full budget.
	store.failuresLeft.Store(1)

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store

	re.NoError(m.loadServiceLimits())

	limiter := m.GetKeyspaceServiceLimiter(keyspaceID)
	re.NotNil(limiter)
	re.Equal(float64(100), limiter.ServiceLimit,
		"a point read that succeeds within the retry budget must be applied")
	re.EqualValues(2, store.calls.Load(),
		"must retry the point read instead of falling back after only one failure, and stop once it succeeds")
}

// TestLoadServiceLimitsFallsBackToBulkValueOnPersistentFailure guards against
// loadServiceLimits silently dropping a keyspace's service limit entirely
// when its point re-read keeps failing (e.g. a persistent storage issue).
// Without a fallback, the keyspace would run with no service limit cached at
// all - letting burstable groups bypass the configured cap indefinitely,
// since nothing else retries this load until the next Init - which is worse
// than falling back to the bulk-scanned value and re-admitting its narrow,
// already-covered staleness window.
func TestLoadServiceLimitsFallsBackToBulkValueOnPersistentFailure(t *testing.T) {
	re := require.New(t)
	const keyspaceID = 1
	base := storage.NewStorageWithMemoryBackend()
	re.NoError(base.SaveServiceLimit(keyspaceID, 100))
	store := &failingServiceLimitLoadStorage{Storage: base, keyspaceID: keyspaceID}
	// Always fail the point read, however many times it's retried.
	store.failuresLeft.Store(math.MaxInt32)

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store

	re.NoError(m.loadServiceLimits())

	limiter := m.GetKeyspaceServiceLimiter(keyspaceID)
	re.NotNil(limiter)
	re.Equal(float64(100), limiter.ServiceLimit,
		"must fall back to the bulk-scanned value rather than leaving the service limit uncached")
	re.EqualValues(maxServiceLimitReloadAttempts, store.calls.Load(),
		"must exhaust the retry budget before falling back")
}

// TestAsyncLoadResourceGroupsMergeGroupNeverVisibleUnsynced guards against a
// window in the bulk merge where a newly-loaded group became visible in
// krgm.groups (readable by any concurrent GetResourceGroup/token request)
// before its burst limit was synced against the keyspace's active service
// limit. A group with no explicit burst limit reads as unbounded until that
// sync runs, so a request landing in the gap could be granted unlimited
// burst and bypass the service limit until the merge caught up. The fix
// moved the sync inside the same krgm.Lock() critical section as the insert,
// so a concurrent reader - which also needs krgm's lock - can no longer
// observe the group until both have happened.
func TestAsyncLoadResourceGroupsMergeGroupNeverVisibleUnsynced(t *testing.T) {
	re := require.New(t)
	store := storage.NewStorageWithMemoryBackend()
	const keyspaceID = 1
	const groupName = "burstable-rg"
	unbounded := &resource_manager.ResourceGroup{
		Name:     groupName,
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: middlePriority,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   UnlimitedRate,
					BurstLimit: UnlimitedBurstLimit,
				},
			},
		},
	}
	re.NoError(store.SaveResourceGroupSetting(keyspaceID, groupName, unbounded))
	re.NoError(store.SaveServiceLimit(keyspaceID, 50))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store

	reached := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	re.NoError(failpoint.EnableCall("github.com/tikv/pd/pkg/mcs/resourcemanager/server/mergeBeforeBurstSync", func(gotKeyspaceID uint32, gotName string) {
		if gotKeyspaceID != keyspaceID || gotName != groupName {
			return
		}
		close(reached)
		<-release
	}))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/mergeBeforeBurstSync"))
	}()

	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	// Unblock the merge first (LIFO), so stopAsyncTestManager's wg.Wait()
	// cannot hang if a later assertion aborts the test before the explicit
	// unblock() call below is reached.
	defer unblock()

	// loadServiceLimits (run synchronously inside Init, before the async
	// merge starts) already created krgm for keyspaceID via the service
	// limit saved above - capture it now, before parking, so the reader
	// below can call krgm.getResourceGroup directly instead of going through
	// m.getKeyspaceResourceGroupManager. The latter needs m.RLock(), which
	// the merge holds for its *entire* batch regardless of this fix, so
	// routing the read through it would block on the wrong lock and the test
	// would pass even without the fix; reading via the captured krgm
	// isolates the one lock (krgm's own) this test is actually about.
	krgm := m.getKeyspaceResourceGroupManager(keyspaceID)
	re.NotNil(krgm)

	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the merge to reach its burst-sync point")
	}

	readDone := make(chan *ResourceGroup)
	go func() {
		readDone <- krgm.getResourceGroup(groupName, false)
	}()

	select {
	case <-readDone:
		t.Fatal("a concurrent reader must not observe the group while the merge is parked between insert and burst sync - krgm's lock should still be held")
	case <-time.After(100 * time.Millisecond):
	}

	unblock()
	group := <-readDone
	re.NotNil(group)
	re.GreaterOrEqual(group.getOverrideBurstLimit(), int64(0),
		"the group must never become visible without its burst override already synced")
}

// BenchmarkAsyncLoadMergeReaderStall measures the worst-case time a concurrent
// reader is blocked while the async bulk merge installs a large number of
// resource groups. The probe uses GetControllerConfig, whose only cost is the
// manager read lock - the same lock the merge takes - so it isolates how long
// the merge stalls readers. It guards against the merge holding that lock
// across the whole O(total groups) work, which would stall every point and
// token request until loading completes on a cluster with many groups.
//
// Run with:
//
//	go test -run '^$' -bench BenchmarkAsyncLoadMergeReaderStall ./pkg/mcs/resourcemanager/server/
func BenchmarkAsyncLoadMergeReaderStall(b *testing.B) {
	const groupCount = 500000
	store := newBlockingResourceGroupStorage()
	for i := range groupCount {
		name := fmt.Sprintf("bench-group-%06d", i)
		if err := store.SaveResourceGroupSetting(1, name, newAsyncTestGroup(name)); err != nil {
			b.Fatal(err)
		}
	}

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	if err := m.Init(context.Background()); err != nil {
		b.Fatal(err)
	}
	defer stopAsyncTestManager(m)
	defer store.unblock()

	store.waitEntered(b)

	// Concurrent readers take only the manager read lock and record the
	// longest single acquisition seen while the merge runs.
	stop := make(chan struct{})
	var maxStall atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now()
				_ = m.GetControllerConfig()
				if d := time.Since(start).Nanoseconds(); d > maxStall.Load() {
					maxStall.Store(d)
				}
			}
		}()
	}

	b.ResetTimer()
	store.unblock()
	deadline := time.Now().Add(30 * time.Second)
	for !m.isResourceGroupLoadingComplete() {
		if time.Now().After(deadline) {
			b.Fatal("timed out waiting for async loading to complete")
		}
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()

	close(stop)
	wg.Wait()
	b.ReportMetric(float64(maxStall.Load())/1e6, "max-reader-stall-ms")
}

// fakeTokenBucketsStream feeds a fixed set of requests to AcquireTokenBuckets
// and records what it sends back. grpc.ServerStream stays nil since
// AcquireTokenBuckets only ever calls Send/Recv on it.
type fakeTokenBucketsStream struct {
	grpc.ServerStream

	requests []*resource_manager.TokenBucketsRequest
	recvCnt  int
	sent     []*resource_manager.TokenBucketsResponse
}

// Context is needed by the metrics stream wrapper, which resolves the peer IP
// for its labels.
func (*fakeTokenBucketsStream) Context() context.Context { return context.Background() }

func (s *fakeTokenBucketsStream) Recv() (*resource_manager.TokenBucketsRequest, error) {
	if s.recvCnt >= len(s.requests) {
		return nil, io.EOF
	}
	req := s.requests[s.recvCnt]
	s.recvCnt++
	return req, nil
}

func (s *fakeTokenBucketsStream) Send(resp *resource_manager.TokenBucketsResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

func newRUTokenBucketRequest(keyspaceID uint32, name string, ru float64) *resource_manager.TokenBucketRequest {
	return &resource_manager.TokenBucketRequest{
		ResourceGroupName: name,
		KeyspaceId:        &resource_manager.KeyspaceIDValue{Keyspace: &resource_manager.KeyspaceIDValue_Value{Value: keyspaceID}},
		Request: &resource_manager.TokenBucketRequest_RuItems{
			RuItems: &resource_manager.TokenBucketRequest_RequestRU{
				RequestRU: []*resource_manager.RequestUnitItem{
					{Type: resource_manager.RequestUnitType_RU, Value: ru},
				},
			},
		},
		ConsumptionSinceLastRequest: &resource_manager.Consumption{},
	}
}

// TestAcquireTokenBucketsSurvivesLazyLoadFailure guards against a transient
// lazy-load failure tearing down the whole token bucket stream: the error
// belongs to a single resource group, so the other groups multiplexed on the
// same stream must still be served instead of every client being forced to
// reconnect.
func TestAcquireTokenBucketsSurvivesLazyLoadFailure(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	for _, name := range []string{"bad-group", "good-group"} {
		group := newAsyncTestGroup(name)
		re.NoError(store.SaveResourceGroupSetting(1, name, group))
		re.NoError(store.SaveResourceGroupStates(1, name, FromProtoResourceGroup(group).GetGroupStates()))
	}

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	m.srv = &testBasicServer{}
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)
	defer store.unblock()

	// Keep the bulk loader parked so lazy loading stays active.
	store.waitEntered(t)

	// Make bad-group's lazy load fail on its state read. good-group is
	// requested in the same batch and must still get its tokens.
	store.failNextState.Store(true)
	stream := &fakeTokenBucketsStream{
		requests: []*resource_manager.TokenBucketsRequest{{
			TargetRequestPeriodMs: 1000,
			ClientUniqueId:        1,
			Requests: []*resource_manager.TokenBucketRequest{
				newRUTokenBucketRequest(1, "bad-group", 10),
				newRUTokenBucketRequest(1, "good-group", 10),
			},
		}},
	}
	svc := &Service{ctx: context.Background(), manager: m}

	re.NoError(svc.AcquireTokenBuckets(stream),
		"a single group's lazy-load failure must not fail the stream")
	re.Len(stream.sent, 1)
	re.Len(stream.sent[0].Responses, 1, "only the loadable group should be answered")
	re.Equal("good-group", stream.sent[0].Responses[0].ResourceGroupName)
}
