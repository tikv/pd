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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/testutil"
)

type blockingResourceGroupStorage struct {
	storage.Storage

	once    sync.Once
	entered chan struct{}
	release chan struct{}
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

func (s *blockingResourceGroupStorage) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async resource group loading")
	}
}

func (s *blockingResourceGroupStorage) unblock() {
	close(s.release)
}

func newAsyncTestGroup(name string, fillRate uint64) *resource_manager.ResourceGroup {
	return &resource_manager.ResourceGroup{
		Name:     name,
		Mode:     resource_manager.GroupMode_RUMode,
		Priority: middlePriority,
		RUSettings: &resource_manager.GroupRequestUnitSettings{
			RU: &resource_manager.TokenBucket{
				Settings: &resource_manager.TokenLimitSettings{
					FillRate:   fillRate,
					BurstLimit: int64(fillRate),
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
	re.NoError(store.SaveResourceGroupSetting(1, "lazy-group", newAsyncTestGroup("lazy-group", 100)))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)

	store.waitEntered(t)

	_, err := m.GetResourceGroupList(1, false)
	re.ErrorIs(err, errs.ErrResourceGroupsLoading)

	group, err := m.GetResourceGroup(1, "lazy-group", false)
	re.NoError(err)
	re.NotNil(group)
	re.Equal("lazy-group", group.Name)
	re.Equal(float64(100), group.RUSettings.RU.getFillRate())

	store.unblock()
	testutil.Eventually(re, func() bool {
		groups, err := m.GetResourceGroupList(1, false)
		return err == nil && len(groups) == 2
	}, testutil.WithTickInterval(20*time.Millisecond))
}

func TestAsyncLoadResourceGroupsDoesNotRestoreDeletedLazyGroup(t *testing.T) {
	re := require.New(t)
	store := newBlockingResourceGroupStorage()
	re.NoError(store.SaveResourceGroupSetting(1, "deleted-group", newAsyncTestGroup("deleted-group", 100)))

	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = store
	re.NoError(m.Init(context.Background()))
	defer stopAsyncTestManager(m)

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
