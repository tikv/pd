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
}

func newBlockingResourceGroupStorage() *blockingResourceGroupStorage {
	return &blockingResourceGroupStorage{
		Storage: storage.NewStorageWithMemoryBackend(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
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
	return s.Storage.LoadResourceGroupState(keyspaceID, name)
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
