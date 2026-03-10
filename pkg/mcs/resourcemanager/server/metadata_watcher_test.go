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
)

type countingServiceLimitLoadStorage struct {
	storage.Storage
	loadServiceLimitsCount int
}

func (s *countingServiceLimitLoadStorage) LoadServiceLimits(f func(keyspaceID uint32, serviceLimit float64)) error {
	s.loadServiceLimitsCount++
	return s.Storage.LoadServiceLimits(f)
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

func TestHandleMetadataWatchPutAndDelete(t *testing.T) {
	re := require.New(t)

	m := &Manager{
		storage:          storage.NewStorageWithMemoryBackend(),
		krgms:            make(map[uint32]*keyspaceResourceGroupManager),
		controllerConfig: &ControllerConfig{},
	}

	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: 10},
	}
	rawGroup, err := proto.Marshal(group)
	re.NoError(err)
	re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/settings/10/test_group", string(rawGroup)))

	krgm := m.getKeyspaceResourceGroupManager(10)
	re.NotNil(krgm)
	cachedGroup := krgm.getResourceGroup("test_group", false)
	re.NotNil(cachedGroup)
	re.Equal(uint32(5), cachedGroup.Priority)
	re.Equal(float64(100), cachedGroup.getFillRate())

	now := time.Now()
	states := &GroupStates{
		RU: &GroupTokenBucketState{
			Tokens:     321,
			LastUpdate: &now,
		},
		RUConsumption: &rmpb.Consumption{RRU: 11, WRU: 22},
	}
	rawStates, err := json.Marshal(states)
	re.NoError(err)
	re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/states/10/test_group", string(rawStates)))

	cachedGroup = krgm.getResourceGroup("test_group", true)
	re.NotNil(cachedGroup)
	re.InDelta(321, cachedGroup.RUSettings.RU.Tokens, 0.001)
	re.Equal(float64(11), cachedGroup.RUConsumption.RRU)
	re.Equal(float64(22), cachedGroup.RUConsumption.WRU)

	re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/states/99/ghost", string(rawStates)))

	controllerConfig := &ControllerConfig{
		RequestUnit: RequestUnitConfig{
			ReadBaseCost:  0.5,
			WriteBaseCost: 2.0,
		},
	}
	rawControllerConfig, err := json.Marshal(controllerConfig)
	re.NoError(err)
	re.NoError(m.handleMetadataWatchPut("resource_group/controller", string(rawControllerConfig)))
	re.InDelta(0.5, m.GetControllerConfig().RequestUnit.ReadBaseCost, 0.00001)
	re.InDelta(2.0, m.GetControllerConfig().RequestUnit.WriteBaseCost, 0.00001)

	re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/service_limits/10", "123.5"))
	re.InDelta(123.5, m.GetKeyspaceServiceLimiter(10).ServiceLimit, 0.00001)

	re.NoError(m.handleMetadataWatchDelete("resource_group/keyspace/settings/10/test_group"))
	re.Nil(krgm.getResourceGroup("test_group", false))

	re.NoError(m.handleMetadataWatchDelete("resource_group/keyspace/service_limits/10"))
	re.InDelta(0.0, m.GetKeyspaceServiceLimiter(10).ServiceLimit, 0.00001)

	defaultGroup := &rmpb.ResourceGroup{
		Name: DefaultResourceGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   1000,
					BurstLimit: -1,
				},
			},
		},
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: 10},
	}
	rawDefaultGroup, err := proto.Marshal(defaultGroup)
	re.NoError(err)
	re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/settings/10/default", string(rawDefaultGroup)))
	currentDefault := krgm.getResourceGroup(DefaultResourceGroupName, false)
	re.NotNil(currentDefault)
	re.Equal(float64(1000), currentDefault.getFillRate())
	re.NoError(m.handleMetadataWatchDelete("resource_group/keyspace/settings/10/default"))
	currentDefault = krgm.getResourceGroup(DefaultResourceGroupName, false)
	re.NotNil(currentDefault)
	re.Equal(float64(UnlimitedRate), currentDefault.getFillRate())
	re.Equal(int64(UnlimitedBurstLimit), currentDefault.getBurstLimit())
	re.Equal(uint32(middlePriority), currentDefault.Priority)
}

func TestHandleMetadataWatchPutAppliesExistingServiceLimitToNewGroup(t *testing.T) {
	re := require.New(t)

	m := &Manager{
		storage:          storage.NewStorageWithMemoryBackend(),
		krgms:            make(map[uint32]*keyspaceResourceGroupManager),
		controllerConfig: &ControllerConfig{},
	}

	re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/service_limits/10", "123.5"))

	group := &rmpb.ResourceGroup{
		Name:     "burstable_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: -1,
				},
			},
		},
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: 10},
	}
	rawGroup, err := proto.Marshal(group)
	re.NoError(err)
	re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/settings/10/burstable_group", string(rawGroup)))

	krgm := m.getKeyspaceResourceGroupManager(10)
	re.NotNil(krgm)
	current := krgm.getMutableResourceGroup(group.Name)
	re.NotNil(current)
	re.Equal(int64(123), current.getOverrideBurstLimit())
	re.Equal(int64(123), current.getBurstLimit())
}

func TestHandleMetadataWatchDeleteRestoresDefaultGroupRuntimeFields(t *testing.T) {
	re := require.New(t)

	m := &Manager{
		storage:          storage.NewStorageWithMemoryBackend(),
		krgms:            make(map[uint32]*keyspaceResourceGroupManager),
		controllerConfig: &ControllerConfig{},
	}

	defaultGroup := &rmpb.ResourceGroup{
		Name: DefaultResourceGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   1000,
					BurstLimit: -1,
				},
			},
		},
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: 10},
	}
	rawDefaultGroup, err := proto.Marshal(defaultGroup)
	re.NoError(err)
	re.NoError(m.handleMetadataWatchPut("resource_group/keyspace/settings/10/default", string(rawDefaultGroup)))
	re.NoError(m.handleMetadataWatchDelete("resource_group/keyspace/settings/10/default"))

	krgm := m.getKeyspaceResourceGroupManager(10)
	re.NotNil(krgm)
	currentDefault := krgm.getMutableResourceGroup(DefaultResourceGroupName)
	re.NotNil(currentDefault)
	re.NotNil(currentDefault.RUConsumption)
	re.NotPanics(func() {
		currentDefault.UpdateRUConsumption(&rmpb.Consumption{RRU: 1, WRU: 2})
	})
	re.Equal(float64(1), currentDefault.RUConsumption.RRU)
	re.Equal(float64(2), currentDefault.RUConsumption.WRU)
}

func TestInitializeMetadataWatcherDoesNotReloadServiceLimits(t *testing.T) {
	re := require.New(t)

	memStorage := &countingServiceLimitLoadStorage{Storage: storage.NewStorageWithMemoryBackend()}
	m := &Manager{
		storage:          memStorage,
		krgms:            make(map[uint32]*keyspaceResourceGroupManager),
		controllerConfig: &ControllerConfig{},
		srv:              &testBasicServer{},
	}

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
}
