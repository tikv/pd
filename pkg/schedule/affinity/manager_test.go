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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/storage"
)

// TestIsRegionAffinity tests the IsRegionAffinity method of Manager.
func TestIsRegionAffinity(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	for i := uint64(1); i <= 4; i++ {
		storeInfo := core.NewStoreInfo(&metapb.Store{Id: i, Address: "test"})
		storeInfo = storeInfo.Clone(core.SetLastHeartbeatTS(time.Now()))
		storeInfos.PutStore(storeInfo)
	}

	conf := mockconfig.NewTestOptions()
	manager := NewManager(ctx, store, storeInfos, conf, nil)
	err := manager.Initialize()
	re.NoError(err)

	// Create affinity group
	group := &Group{
		ID:            "test_group",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1, 2, 3},
	}
	err = manager.SaveAffinityGroups([]GroupWithRanges{{Group: group}})
	re.NoError(err)

	// Test 1: Region not belonging to any affinity group should return false
	region1 := core.NewRegionInfo(
		&metapb.Region{Id: 1, Peers: []*metapb.Peer{
			{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 12, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 13, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 11, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	re.False(manager.IsRegionAffinity(region1), "Region not in group should return false")

	// Add region to group
	manager.SetRegionGroup(1, "test_group")

	// Test 2: Region conforming to affinity requirements should return true
	re.True(manager.IsRegionAffinity(region1), "Region conforming to affinity should return true")

	// Test 3: Region with wrong leader should return false
	region2 := core.NewRegionInfo(
		&metapb.Region{Id: 2, Peers: []*metapb.Peer{
			{Id: 21, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 22, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 23, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 22, StoreId: 2, Role: metapb.PeerRole_Voter}, // Leader on store 2, not 1
	)
	manager.SetRegionGroup(2, "test_group")
	re.False(manager.IsRegionAffinity(region2), "Region with wrong leader should return false")

	// Test 4: Region with wrong voter stores should return false
	region3 := core.NewRegionInfo(
		&metapb.Region{Id: 3, Peers: []*metapb.Peer{
			{Id: 31, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 32, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 33, StoreId: 4, Role: metapb.PeerRole_Voter}, // Store 4 instead of 3
		}},
		&metapb.Peer{Id: 31, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	manager.SetRegionGroup(3, "test_group")
	re.False(manager.IsRegionAffinity(region3), "Region with wrong voter stores should return false")

	// Test 5: Region with different number of voters should return false
	region4 := core.NewRegionInfo(
		&metapb.Region{Id: 4, Peers: []*metapb.Peer{
			{Id: 41, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 42, StoreId: 2, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 41, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	manager.SetRegionGroup(4, "test_group")
	re.False(manager.IsRegionAffinity(region4), "Region with wrong number of voters should return false")

	// Test 6: Region without leader should return false
	region5 := core.NewRegionInfo(
		&metapb.Region{Id: 5, Peers: []*metapb.Peer{
			{Id: 51, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 52, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 53, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		nil, // No leader
	)
	manager.SetRegionGroup(5, "test_group")
	re.False(manager.IsRegionAffinity(region5), "Region without leader should return false")

	// Test 7: Group not in effect should return false
	groupInfo := manager.GetGroups()["test_group"]
	groupInfo.Effect = false
	region6 := core.NewRegionInfo(
		&metapb.Region{Id: 6, Peers: []*metapb.Peer{
			{Id: 61, StoreId: 1, Role: metapb.PeerRole_Voter},
			{Id: 62, StoreId: 2, Role: metapb.PeerRole_Voter},
			{Id: 63, StoreId: 3, Role: metapb.PeerRole_Voter},
		}},
		&metapb.Peer{Id: 61, StoreId: 1, Role: metapb.PeerRole_Voter},
	)
	manager.SetRegionGroup(6, "test_group")
	re.False(manager.IsRegionAffinity(region6), "Group not in effect should return false")
}

// TestBasicGroupOperations tests basic group CRUD operations
func TestBasicGroupOperations(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewStorageWithMemoryBackend()
	storeInfos := core.NewStoresInfo()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "test1"})
	store1 = store1.Clone(core.SetLastHeartbeatTS(time.Now()))
	storeInfos.PutStore(store1)

	conf := mockconfig.NewTestOptions()

	manager := NewManager(ctx, store, storeInfos, conf, nil)
	err := manager.Initialize()
	re.NoError(err)

	// Create a group
	group1 := &Group{
		ID:            "group1",
		LeaderStoreID: 1,
		VoterStoreIDs: []uint64{1},
	}
	err = manager.SaveAffinityGroups([]GroupWithRanges{{Group: group1}})
	re.NoError(err)
	re.True(manager.IsGroupExist("group1"))

	// Delete the group (no key ranges, so force=false should work)
	err = manager.DeleteAffinityGroup("group1", false)
	re.NoError(err)
	re.False(manager.IsGroupExist("group1"))
}
