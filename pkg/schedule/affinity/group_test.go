// Copyright 2025 TiKV Project Authors.
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

package affinity

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage"
)

func TestStoreCondition(t *testing.T) {
	re := require.New(t)
	re.Equal(groupDegraded, storeDisconnected.groupAvailability())
	re.Equal(groupDegraded, storeEvictLeader.groupAvailability())
	re.Equal(groupDegraded, storeLowSpace.groupAvailability())
	re.Equal(groupExpired, storeDown.groupAvailability())
	re.Equal(groupExpired, storeRemovingOrRemoved.groupAvailability())

	re.True(storeEvictLeader.affectsLeaderOnly())
	re.False(storeDisconnected.affectsLeaderOnly())
	re.False(storeDown.affectsLeaderOnly())
}

func TestGroupState(t *testing.T) {
	re := require.New(t)
	group := &GroupState{
		Group: Group{
			ID:            "group1",
			LeaderStoreID: 1,
			VoterStoreIDs: []uint64{1, 2, 3},
		},
		AffinitySchedulingEnabled: true,
	}
	// keyRange is unused in this test.
	region := generateRegionForTest(100, []uint64{1, 2, 3}, nonOverlappingRange)
	re.True(group.isRegionAffinity(region))
	region = generateRegionForTest(100, []uint64{2, 1, 3}, nonOverlappingRange)
	re.False(group.isRegionAffinity(region))
	region = generateRegionForTest(100, []uint64{1, 2, 4}, nonOverlappingRange)
	re.False(group.isRegionAffinity(region))

	group.AffinitySchedulingEnabled = false
	region = generateRegionForTest(100, []uint64{1, 2, 3}, nonOverlappingRange)
	re.False(group.isRegionAffinity(region))
}

func TestAdjustGroup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := uint64(1); i <= 3; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: i, Address: "test"})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	regionLabeler, err := labeler.NewRegionLabeler(ctx, store, time.Second*5)
	re.NoError(err)
	manager, err := NewManager(ctx, store, storeInfos, conf, regionLabeler)
	re.NoError(err)

	group := &Group{
		ID:            "TEST-group_1",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	re.NoError(manager.AdjustGroup(group))
	// test id error
	group.ID = ""
	re.Error(manager.AdjustGroup(group))
	group.ID = strings.Repeat("a", 65)
	re.Error(manager.AdjustGroup(group))
	group.ID = "TEST-group@2"
	re.Error(manager.AdjustGroup(group))
	// test no leader
	group.ID = "TEST-group_1"
	group.LeaderStoreID = 0
	re.Error(manager.AdjustGroup(group))
	// test no voters
	group.LeaderStoreID = 1
	group.VoterStoreIDs = nil
	re.Error(manager.AdjustGroup(group))
	// test leader not in voters
	group.LeaderStoreID = 1
	group.VoterStoreIDs = []uint64{2, 3}
	re.Error(manager.AdjustGroup(group))
	// test duplicate voter id
	group.LeaderStoreID = 1
	group.VoterStoreIDs = []uint64{1, 1, 3}
	re.Error(manager.AdjustGroup(group))
	// test error store id
	group.LeaderStoreID = 1
	group.VoterStoreIDs = []uint64{1, 4}
	re.Error(manager.AdjustGroup(group))
}
