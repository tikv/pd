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
	"encoding/json"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage"
)

func TestParseResourceGroupWatchPath(t *testing.T) {
	re := require.New(t)

	testCases := []struct {
		path   string
		ok     bool
		target resourceGroupWatchTarget
	}{
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
			path: "resource_group/controller",
			ok:   false,
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
		storage: storage.NewStorageWithMemoryBackend(),
		krgms:   make(map[uint32]*keyspaceResourceGroupManager),
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
	re.NoError(m.handleMetadataWatchPut("resource_group/controller", "ignored"))

	re.NoError(m.handleMetadataWatchDelete("resource_group/keyspace/settings/10/test_group"))
	re.Nil(krgm.getResourceGroup("test_group", false))

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
	re.NotNil(krgm.getResourceGroup(DefaultResourceGroupName, false))
	re.NoError(m.handleMetadataWatchDelete("resource_group/keyspace/settings/10/default"))
	re.NotNil(krgm.getResourceGroup(DefaultResourceGroupName, false))
}
