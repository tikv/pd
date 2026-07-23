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
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keypath"
)

type countingServiceLimitLoadStorage struct {
	storage.Storage
	loadServiceLimitsCount int
}

func (s *countingServiceLimitLoadStorage) LoadServiceLimits(f func(keyspaceID uint32, serviceLimit float64)) error {
	s.loadServiceLimitsCount++
	return s.Storage.LoadServiceLimits(f)
}

func newMetadataWatcherTestManager(store storage.Storage) *Manager {
	return &Manager{
		storage:          store,
		krgms:            make(map[uint32]*keyspaceResourceGroupManager),
		controllerConfig: &ControllerConfig{},
	}
}

func newMetadataWatcherResourceGroup(name string, priority uint32, fillRate uint64, burstLimit int64) *rmpb.ResourceGroup {
	return &rmpb.ResourceGroup{
		Name:     name,
		Mode:     rmpb.GroupMode_RUMode,
		Priority: priority,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   fillRate,
					BurstLimit: burstLimit,
				},
			},
		},
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: 10},
	}
}

func TestParseResourceGroupWatchPath(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		path   string
		ok     bool
		target resourceGroupWatchTarget
	}{
		{
			path: "resource_group/controller",
			ok:   true,
			target: resourceGroupWatchTarget{
				entryType: resourceGroupWatchEntryController,
			},
		},
		{
			path: "resource_group/settings/default",
			ok:   true,
			target: resourceGroupWatchTarget{
				entryType:  resourceGroupWatchEntrySettings,
				keyspaceID: constant.NullKeyspaceID,
				groupName:  DefaultResourceGroupName,
			},
		},
		{
			path: "resource_group/states/default",
			ok:   true,
			target: resourceGroupWatchTarget{
				entryType:  resourceGroupWatchEntryStates,
				keyspaceID: constant.NullKeyspaceID,
				groupName:  DefaultResourceGroupName,
			},
		},
		{
			path: "resource_group/keyspace/settings/42/group-a",
			ok:   true,
			target: resourceGroupWatchTarget{
				entryType:  resourceGroupWatchEntrySettings,
				keyspaceID: 42,
				groupName:  "group-a",
			},
		},
		{
			path: "resource_group/keyspace/states/7/group-b",
			ok:   true,
			target: resourceGroupWatchTarget{
				entryType:  resourceGroupWatchEntryStates,
				keyspaceID: 7,
				groupName:  "group-b",
			},
		},
		{
			path: "resource_group/keyspace/service_limits/7",
			ok:   true,
			target: resourceGroupWatchTarget{
				entryType:  resourceGroupWatchEntryServiceLimit,
				keyspaceID: 7,
			},
		},
		{
			path: "settings/default",
			ok:   false,
		},
		{
			path: "resource_group/settings/",
			ok:   false,
		},
		{
			path: "resource_group/keyspace/settings/abc/group",
			ok:   false,
		},
		{
			path: "resource_group/keyspace/states/1/",
			ok:   false,
		},
	}

	for _, tc := range testCases {
		target, ok := parseResourceGroupWatchPath(tc.path)
		re.Equal(tc.ok, ok, tc.path)
		if tc.ok {
			re.Equal(tc.target, target, tc.path)
		}
	}
}

func TestMetadataWatcherHandlePut(t *testing.T) {
	t.Run("loads_settings_states_controller_config_and_service_limit", func(t *testing.T) {
		re := require.New(t)

		m := newMetadataWatcherTestManager(storage.NewStorageWithMemoryBackend())
		group := newMetadataWatcherResourceGroup("test_group", 5, 100, 200)
		re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/settings/10/test_group", mustMarshalResourceGroup(t, group)))

		krgm := m.getKeyspaceResourceGroupManager(10)
		re.NotNil(krgm)
		cachedGroup := krgm.getResourceGroup("test_group", false)
		re.NotNil(cachedGroup)
		re.Equal(uint32(5), cachedGroup.Priority)
		re.Equal(float64(100), cachedGroup.getFillRate())

		now := time.Now()
		rawStates, err := json.Marshal(&GroupStates{
			RU: &GroupTokenBucketState{
				Tokens:     321,
				LastUpdate: &now,
			},
			RUConsumption: &rmpb.Consumption{RRU: 11, WRU: 22},
		})
		re.NoError(err)
		re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/states/10/test_group", string(rawStates)))

		cachedGroup = krgm.getResourceGroup("test_group", true)
		re.NotNil(cachedGroup)
		re.InDelta(321, cachedGroup.RUSettings.RU.Tokens, 0.001)
		re.Equal(float64(11), cachedGroup.RUConsumption.RRU)
		re.Equal(float64(22), cachedGroup.RUConsumption.WRU)

		re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/states/99/ghost", string(rawStates)))

		rawControllerConfig, err := json.Marshal(&ControllerConfig{
			RequestUnit: RequestUnitConfig{
				ReadBaseCost:  0.5,
				WriteBaseCost: 2.0,
			},
		})
		re.NoError(err)
		re.NoError(m.handleMetadataWatchPut("resource_group/controller", string(rawControllerConfig)))
		re.InDelta(0.5, m.GetControllerConfig().RequestUnit.ReadBaseCost, 0.00001)
		re.InDelta(2.0, m.GetControllerConfig().RequestUnit.WriteBaseCost, 0.00001)

		re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/service_limits/10", "123.5"))
		re.InDelta(123.5, m.GetKeyspaceServiceLimiter(10).ServiceLimit, 0.00001)
	})

	t.Run("initializes_default_group_for_watcher_created_keyspace", func(t *testing.T) {
		testCases := []struct {
			name     string
			key      string
			rawValue string
			check    func(*require.Assertions, *Manager)
		}{
			{
				name:     "settings_update",
				key:      "resource_group/keyspace/settings/10/test_group",
				rawValue: mustMarshalResourceGroup(t, newMetadataWatcherResourceGroup("test_group", 5, 100, 200)),
				check: func(re *require.Assertions, m *Manager) {
					krgm := m.getKeyspaceResourceGroupManager(10)
					re.NotNil(krgm)
					re.NotNil(krgm.getResourceGroup(DefaultResourceGroupName, false))
				},
			},
			{
				name:     "service_limit_update",
				key:      "resource_group/keyspace/service_limits/10",
				rawValue: "123.5",
				check: func(re *require.Assertions, m *Manager) {
					krgm := m.getKeyspaceResourceGroupManager(10)
					re.NotNil(krgm)
					re.NotNil(krgm.getResourceGroup(DefaultResourceGroupName, false))
					re.InDelta(123.5, m.GetKeyspaceServiceLimiter(10).ServiceLimit, 0.00001)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				re := require.New(t)
				m := newMetadataWatcherTestManager(storage.NewStorageWithMemoryBackend())

				re.NoError(m.handleMetadataWatchPut(tc.key, tc.rawValue))
				tc.check(re, m)
			})
		}
	})

	t.Run("applies_existing_service_limit_to_new_group", func(t *testing.T) {
		re := require.New(t)

		m := newMetadataWatcherTestManager(storage.NewStorageWithMemoryBackend())
		re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/service_limits/10", "123.5"))

		group := newMetadataWatcherResourceGroup("burstable_group", 5, 100, -1)
		re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/settings/10/burstable_group", mustMarshalResourceGroup(t, group)))

		krgm := m.getKeyspaceResourceGroupManager(10)
		re.NotNil(krgm)
		current := krgm.getMutableResourceGroup(group.Name)
		re.NotNil(current)
		re.Equal(int64(123), current.getOverrideBurstLimit())
		re.Equal(int64(123), current.getBurstLimit())
	})

	t.Run("does_not_overwrite_storage_payload_when_default_group_arrives_first", func(t *testing.T) {
		re := require.New(t)

		memStorage := storage.NewStorageWithMemoryBackend()
		m := newMetadataWatcherTestManager(memStorage)
		defaultGroup := newMetadataWatcherResourceGroup(DefaultResourceGroupName, 1, 1000, -1)
		rawDefaultGroup := mustMarshalResourceGroup(t, defaultGroup)
		re.NoError(memStorage.Save(keypath.KeyspaceResourceGroupSettingPath(10, DefaultResourceGroupName), rawDefaultGroup))

		re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/settings/10/default", rawDefaultGroup))

		stored, err := memStorage.Load(keypath.KeyspaceResourceGroupSettingPath(10, DefaultResourceGroupName))
		re.NoError(err)
		re.NotEmpty(stored)

		persisted := &rmpb.ResourceGroup{}
		re.NoError(proto.Unmarshal([]byte(stored), persisted))
		re.Equal(defaultGroup.Priority, persisted.Priority)
		re.Equal(
			defaultGroup.GetRUSettings().GetRU().GetSettings().GetFillRate(),
			persisted.GetRUSettings().GetRU().GetSettings().GetFillRate(),
		)
	})

	t.Run("service_limit_update_does_not_persist_reserved_default_group", func(t *testing.T) {
		re := require.New(t)

		memStorage := storage.NewStorageWithMemoryBackend()
		m := newMetadataWatcherTestManager(memStorage)

		re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/service_limits/10", "123.5"))

		storedSettings, err := memStorage.Load(keypath.KeyspaceResourceGroupSettingPath(10, DefaultResourceGroupName))
		re.NoError(err)
		re.Empty(storedSettings)

		storedStates, err := memStorage.Load(keypath.KeyspaceResourceGroupStatePath(10, DefaultResourceGroupName))
		re.NoError(err)
		re.Empty(storedStates)
	})
}

func TestMetadataWatcherHandleDelete(t *testing.T) {
	t.Run("removes_group_and_service_limit", func(t *testing.T) {
		re := require.New(t)

		m := newMetadataWatcherTestManager(storage.NewStorageWithMemoryBackend())
		group := newMetadataWatcherResourceGroup("test_group", 5, 100, 200)
		re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/settings/10/test_group", mustMarshalResourceGroup(t, group)))
		re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/service_limits/10", "123.5"))

		krgm := m.getKeyspaceResourceGroupManager(10)
		re.NotNil(krgm)

		re.NoError(m.handleMetadataWatchDelete("resource_group/keyspace/settings/10/test_group"))
		re.Nil(krgm.getResourceGroup("test_group", false))

		re.NoError(m.handleMetadataWatchDelete("resource_group/keyspace/service_limits/10"))
		re.InDelta(0.0, m.GetKeyspaceServiceLimiter(10).ServiceLimit, 0.00001)
	})

	t.Run("restores_default_group_runtime_fields", func(t *testing.T) {
		re := require.New(t)

		m := newMetadataWatcherTestManager(storage.NewStorageWithMemoryBackend())
		defaultGroup := newMetadataWatcherResourceGroup(DefaultResourceGroupName, middlePriority, 1000, -1)
		re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/settings/10/default", mustMarshalResourceGroup(t, defaultGroup)))

		krgm := m.getKeyspaceResourceGroupManager(10)
		re.NotNil(krgm)
		currentDefault := krgm.getResourceGroup(DefaultResourceGroupName, false)
		re.NotNil(currentDefault)
		re.Equal(float64(1000), currentDefault.getFillRate())

		re.NoError(m.handleMetadataWatchDelete("resource_group/keyspace/settings/10/default"))
		currentDefault = krgm.getResourceGroup(DefaultResourceGroupName, false)
		re.NotNil(currentDefault)
		re.Equal(float64(UnlimitedRate), currentDefault.getFillRate())
		re.Equal(int64(UnlimitedBurstLimit), currentDefault.getBurstLimit())
		re.Equal(uint32(middlePriority), currentDefault.Priority)

		mutableDefault := krgm.getMutableResourceGroup(DefaultResourceGroupName)
		re.NotNil(mutableDefault)
		re.NotNil(mutableDefault.RUConsumption)
		re.NotPanics(func() {
			mutableDefault.UpdateRUConsumption(&rmpb.Consumption{RRU: 1, WRU: 2})
		})
		re.Equal(float64(1), mutableDefault.RUConsumption.RRU)
		re.Equal(float64(2), mutableDefault.RUConsumption.WRU)
	})
}

func TestMetadataSnapshotReconciliation(t *testing.T) {
	t.Run("removes_entries_missing_from_successful_snapshot", func(t *testing.T) {
		re := require.New(t)
		m := newMetadataWatcherTestManager(storage.NewStorageWithMemoryBackend())

		firstGeneration := m.beginMetadataSnapshot()
		staleGroup := newMetadataWatcherResourceGroup("stale_group", 5, 100, 200)
		keptGroup := newMetadataWatcherResourceGroup("kept_group", 5, 100, -1)
		customDefault := newMetadataWatcherResourceGroup(DefaultResourceGroupName, 1, 1000, -1)
		re.NoError(m.handleMetadataWatchPutWithGeneration(
			"resource_group/keyspace/settings/10/stale_group",
			mustMarshalResourceGroup(t, staleGroup),
			firstGeneration,
		))
		re.NoError(m.handleMetadataWatchPutWithGeneration(
			"resource_group/keyspace/settings/10/kept_group",
			mustMarshalResourceGroup(t, keptGroup),
			firstGeneration,
		))
		re.NoError(m.handleMetadataWatchPutWithGeneration(
			"resource_group/keyspace/settings/10/default",
			mustMarshalResourceGroup(t, customDefault),
			firstGeneration,
		))
		re.NoError(m.handleMetadataWatchPutWithGeneration(
			"resource_group/keyspace/service_limits/10",
			"123.5",
			firstGeneration,
		))
		re.NoError(m.finishMetadataSnapshot(firstGeneration, nil))

		krgm := m.getKeyspaceResourceGroupManager(10)
		re.NotNil(krgm)
		re.Equal(int64(123), krgm.getMutableResourceGroup("kept_group").getOverrideBurstLimit())

		secondGeneration := m.beginMetadataSnapshot()
		re.NoError(m.handleMetadataWatchPutWithGeneration(
			"resource_group/keyspace/settings/10/kept_group",
			mustMarshalResourceGroup(t, keptGroup),
			secondGeneration,
		))
		re.NoError(m.finishMetadataSnapshot(secondGeneration, nil))

		re.Nil(krgm.getResourceGroup("stale_group", false))
		re.NotNil(krgm.getResourceGroup("kept_group", false))
		re.Zero(m.GetKeyspaceServiceLimiter(10).ServiceLimit)
		re.Equal(int64(-1), krgm.getMutableResourceGroup("kept_group").getOverrideBurstLimit())
		defaultGroup := krgm.getResourceGroup(DefaultResourceGroupName, false)
		re.NotNil(defaultGroup)
		re.Equal(float64(UnlimitedRate), defaultGroup.getFillRate())
		re.Equal(int64(UnlimitedBurstLimit), defaultGroup.getBurstLimit())
		re.Equal(uint32(middlePriority), defaultGroup.Priority)
	})

	t.Run("does_not_remove_entries_after_failed_snapshot", func(t *testing.T) {
		re := require.New(t)
		m := newMetadataWatcherTestManager(storage.NewStorageWithMemoryBackend())
		group := newMetadataWatcherResourceGroup("kept_group", 5, 100, 200)

		firstGeneration := m.beginMetadataSnapshot()
		re.NoError(m.handleMetadataWatchPutWithGeneration(
			"resource_group/keyspace/settings/10/kept_group",
			mustMarshalResourceGroup(t, group),
			firstGeneration,
		))
		re.NoError(m.handleMetadataWatchPutWithGeneration(
			"resource_group/keyspace/service_limits/10",
			"123.5",
			firstGeneration,
		))
		re.NoError(m.finishMetadataSnapshot(firstGeneration, nil))

		failedGeneration := m.beginMetadataSnapshot()
		re.NoError(m.finishMetadataSnapshot(failedGeneration, errors.New("load failed")))

		krgm := m.getKeyspaceResourceGroupManager(10)
		re.NotNil(krgm.getResourceGroup("kept_group", false))
		re.InDelta(123.5, m.GetKeyspaceServiceLimiter(10).ServiceLimit, 0.00001)
		re.Zero(m.getActiveMetadataSnapshotGeneration())
	})

	t.Run("retains_api_writes_overlapping_snapshot", func(t *testing.T) {
		re := require.New(t)
		m := newMetadataWatcherTestManager(storage.NewStorageWithMemoryBackend())
		group := newMetadataWatcherResourceGroup("api_group", 5, 100, 200)
		re.NoError(m.AddResourceGroup(group))
		re.NoError(m.SetKeyspaceServiceLimit(10, 100))

		generation := m.beginMetadataSnapshot()
		group.Priority = 6
		re.NoError(m.ModifyResourceGroup(group))
		re.NoError(m.SetKeyspaceServiceLimit(10, 200))
		addedGroup := newMetadataWatcherResourceGroup("added_during_snapshot", 7, 200, 300)
		re.NoError(m.AddResourceGroup(addedGroup))
		deletedGroup := newMetadataWatcherResourceGroup("deleted_during_snapshot", 8, 300, 400)
		re.NoError(m.AddResourceGroup(deletedGroup))
		re.NoError(m.DeleteResourceGroup(10, deletedGroup.Name))

		// Model stale values read at the snapshot revision before the API writes.
		re.NoError(m.handleMetadataWatchPutWithGeneration(
			"resource_group/keyspace/settings/10/api_group",
			mustMarshalResourceGroup(t, newMetadataWatcherResourceGroup("api_group", 5, 100, 200)),
			generation,
		))
		re.NoError(m.handleMetadataWatchPutWithGeneration(
			"resource_group/keyspace/service_limits/10",
			"100",
			generation,
		))
		re.NoError(m.handleMetadataWatchPutWithGeneration(
			"resource_group/keyspace/settings/10/deleted_during_snapshot",
			mustMarshalResourceGroup(t, deletedGroup),
			generation,
		))
		re.NoError(m.finishMetadataSnapshot(generation, nil))

		krgm := m.getKeyspaceResourceGroupManager(10)
		re.Equal(uint32(6), krgm.getResourceGroup("api_group", false).Priority)
		re.NotNil(krgm.getResourceGroup("added_during_snapshot", false))
		re.Nil(krgm.getResourceGroup("deleted_during_snapshot", false))
		re.InDelta(200, m.GetKeyspaceServiceLimiter(10).ServiceLimit, 0.00001)
	})
}

func BenchmarkMetadataSnapshotReconciliation1MKeyspaces(b *testing.B) {
	const generation = uint64(1)
	manager := newMetadataWatcherTestManager(nil)
	for keyspaceID := range uint32(1_000_000) {
		krgm := newKeyspaceResourceGroupManager(keyspaceID, nil)
		defaultGroup := newDefaultResourceGroup()
		defaultGroup.metadataSnapshotGeneration = generation
		krgm.groups[DefaultResourceGroupName] = defaultGroup
		krgm.serviceLimiter.metadataSnapshotGeneration = generation
		manager.krgms[keyspaceID] = krgm
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		manager.RLock()
		for _, krgm := range manager.krgms {
			krgm.reconcileMetadataSnapshot(generation)
		}
		manager.RUnlock()
	}
}

func TestInitializeMetadataWatcher(t *testing.T) {
	t.Run("does_not_reload_service_limits", func(t *testing.T) {
		re := require.New(t)

		memStorage := &countingServiceLimitLoadStorage{Storage: storage.NewStorageWithMemoryBackend()}
		m := newMetadataWatcherTestManager(memStorage)
		m.srv = &testBasicServer{}

		originalFactory := newMetadataLoopWatcher
		defer func() { newMetadataLoopWatcher = originalFactory }()
		newMetadataLoopWatcher = func(
			_ context.Context,
			_ *sync.WaitGroup,
			_ *clientv3.Client,
			_, _ string,
			_ func([]*clientv3.Event) error,
			putFn, _ func(*mvccpb.KeyValue) error,
			_ func([]*clientv3.Event) error,
			_ bool,
		) metadataLoopWatcher {
			return &fakeMetadataLoopWatcher{
				waitLoadFn: func() error {
					return putFn(&mvccpb.KeyValue{
						Key:   []byte("resource_group/keyspace/service_limits/10"),
						Value: []byte("123.5"),
					})
				},
			}
		}

		re.NoError(m.initializeMetadataWatcher(context.Background()))
		re.Zero(memStorage.loadServiceLimitsCount)
		re.InDelta(123.5, m.GetKeyspaceServiceLimiter(10).ServiceLimit, 0.00001)
	})
}
