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

package statistics

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
)

func TestGetHotStoreLeaders(t *testing.T) {
	re := require.New(t)

	cluster := core.NewBasicCluster()

	store1 := metapb.Store{Id: 1, Address: "mock://tikv-1:1", State: metapb.StoreState_Up}
	store2 := metapb.Store{Id: 2, Address: "mock://tikv-2:2", State: metapb.StoreState_Up}
	store3 := metapb.Store{Id: 3, Address: "mock://tikv-3:3", State: metapb.StoreState_Up}

	// Add the stores to the cluster
	cluster.PutStore(core.NewStoreInfo(&store1))
	cluster.PutStore(core.NewStoreInfo(&store2))
	cluster.PutStore(core.NewStoreInfo(&store3))

	// Create mock hot peers
	storeHotWritePeers := make(map[uint64][]*HotPeerStat)
	storeHotReadPeers := make(map[uint64][]*HotPeerStat)

	// Create hot write peers for store 1
	storeHotWritePeers[1] = []*HotPeerStat{
		{
			RegionID:  101,
			StoreID:   1,
			isLeader:  true,
			HotDegree: 1,
			AntiCount: 0,
			Loads:     []float64{100, 0, 0},
		},
		{
			RegionID:  102,
			StoreID:   1,
			isLeader:  true,
			HotDegree: 1,
			AntiCount: 0,
			Loads:     []float64{200, 0, 0},
		},
		{
			RegionID:  103,
			StoreID:   1,
			isLeader:  false, // Not leader
			HotDegree: 1,
			AntiCount: 0,
			Loads:     []float64{50, 0, 0},
		},
	}

	// Create hot write peers for store 2
	storeHotWritePeers[2] = []*HotPeerStat{
		{
			RegionID:  201,
			StoreID:   2,
			isLeader:  true,
			HotDegree: 1,
			AntiCount: 0,
			Loads:     []float64{50, 0, 0},
		},
		{
			RegionID:  202,
			StoreID:   2,
			isLeader:  false, // Not leader
			HotDegree: 1,
			AntiCount: 0,
			Loads:     []float64{250, 0, 0},
		},
	}

	// Create hot read peers for store 1
	storeHotReadPeers[1] = []*HotPeerStat{
		{
			RegionID:  104,
			StoreID:   1,
			isLeader:  true,
			HotDegree: 1,
			AntiCount: 0,
			Loads:     []float64{0, 0, 300},
		},
		{
			RegionID:  105,
			StoreID:   1,
			isLeader:  false, // Not leader
			HotDegree: 1,
			AntiCount: 0,
			Loads:     []float64{0, 0, 50},
		},
	}

	// Create hot read peers for store 2
	storeHotReadPeers[2] = []*HotPeerStat{
		{
			RegionID:  203,
			StoreID:   2,
			isLeader:  true,
			HotDegree: 1,
			AntiCount: 0,
			Loads:     []float64{0, 0, 150},
		},
		{
			RegionID:  204,
			StoreID:   2,
			isLeader:  true,
			HotDegree: 1,
			AntiCount: 0,
			Loads:     []float64{0, 0, 350},
		},
	}

	// Add mock regions with leaders to the cluster
	cluster.PutRegion(core.NewRegionInfo(
		&metapb.Region{Id: 101, StartKey: []byte("a"), EndKey: []byte("b")},
		&metapb.Peer{StoreId: 1, Id: 1001, Role: metapb.PeerRole_Voter}))
	cluster.PutRegion(core.NewRegionInfo(
		&metapb.Region{Id: 102, StartKey: []byte("b"), EndKey: []byte("c")},
		&metapb.Peer{StoreId: 1, Id: 1002, Role: metapb.PeerRole_Voter}))
	cluster.PutRegion(core.NewRegionInfo(
		&metapb.Region{Id: 103, StartKey: []byte("c"), EndKey: []byte("d")},
		&metapb.Peer{StoreId: 2, Id: 1003, Role: metapb.PeerRole_Voter}))
	cluster.PutRegion(core.NewRegionInfo(
		&metapb.Region{Id: 104, StartKey: []byte("d"), EndKey: []byte("e")},
		&metapb.Peer{StoreId: 1, Id: 1004, Role: metapb.PeerRole_Voter}))
	cluster.PutRegion(core.NewRegionInfo(
		&metapb.Region{Id: 105, StartKey: []byte("e"), EndKey: []byte("f")},
		&metapb.Peer{StoreId: 2, Id: 1005, Role: metapb.PeerRole_Voter}))
	cluster.PutRegion(core.NewRegionInfo(
		&metapb.Region{Id: 201, StartKey: []byte("f"), EndKey: []byte("g")},
		&metapb.Peer{StoreId: 2, Id: 2001, Role: metapb.PeerRole_Voter}))
	cluster.PutRegion(core.NewRegionInfo(
		&metapb.Region{Id: 202, StartKey: []byte("g"), EndKey: []byte("h")},
		&metapb.Peer{StoreId: 1, Id: 2002, Role: metapb.PeerRole_Voter}))
	cluster.PutRegion(core.NewRegionInfo(
		&metapb.Region{Id: 203, StartKey: []byte("h"), EndKey: []byte("i")},
		&metapb.Peer{StoreId: 2, Id: 2003, Role: metapb.PeerRole_Voter}))
	cluster.PutRegion(core.NewRegionInfo(
		&metapb.Region{Id: 204, StartKey: []byte("i"), EndKey: []byte("j")},
		&metapb.Peer{StoreId: 2, Id: 2004, Role: metapb.PeerRole_Voter}))

	// Test GetHotStoreLeaders for store 1
	hotWriteLeaders := GetHotStoreLeaders(cluster, 1, storeHotWritePeers)
	re.Len(hotWriteLeaders, 2, "Store 1 should have 2 hot write leaders")
	hotReadLeaders := GetHotStoreLeaders(cluster, 1, storeHotReadPeers)
	re.Len(hotReadLeaders, 1, "Store 1 should have 1 hot read leaders")

	// Verify regions 101, 102, 104 (the leaders) are returned
	regionIDs := make(map[uint64]bool)
	for _, region := range hotWriteLeaders {
		regionIDs[region.GetID()] = true
	}
	for _, region := range hotReadLeaders {
		regionIDs[region.GetID()] = true
	}
	re.True(regionIDs[101], "Region 101 should be a hot leader for store 1")
	re.True(regionIDs[102], "Region 102 should be a hot leader for store 1")
	re.True(regionIDs[104], "Region 104 should be a hot leader for store 1")

	// Test GetHotStoreLeaders for store 2
	hotWriteLeaders = GetHotStoreLeaders(cluster, 2, storeHotWritePeers)
	re.Len(hotWriteLeaders, 1, "Store 2 should have 1 hot write leaders")
	hotReadLeaders = GetHotStoreLeaders(cluster, 2, storeHotReadPeers)
	re.Len(hotReadLeaders, 2, "Store 2 should have 2 hot read leaders")

	regionIDs = make(map[uint64]bool)
	for _, region := range hotWriteLeaders {
		regionIDs[region.GetID()] = true
	}
	for _, region := range hotReadLeaders {
		regionIDs[region.GetID()] = true
	}
	re.True(regionIDs[201], "Region 201 should be a hot leader for store 2")
	re.True(regionIDs[203], "Region 203 should be a hot leader for store 2")
	re.True(regionIDs[204], "Region 204 should be a hot leader for store 2")

	// Test GetHotStoreLeaders for store that has no hot peers
	hotWriteLeaders = GetHotStoreLeaders(cluster, 3, storeHotWritePeers)
	re.Empty(hotWriteLeaders, "Store 3 should have no hot write leaders")
	hotReadLeaders = GetHotStoreLeaders(cluster, 3, storeHotReadPeers)
	re.Empty(hotReadLeaders, "Store 3 should have no hot read leaders")

	// Test GetHotStoreLeaders for non-existent store
	hotWriteLeaders = GetHotStoreLeaders(cluster, 100, storeHotWritePeers)
	re.Empty(hotWriteLeaders, "Non-existent store should have no hot write leaders")
	hotReadLeaders = GetHotStoreLeaders(cluster, 100, storeHotReadPeers)
	re.Empty(hotReadLeaders, "Non-existent store should have no hot read leaders")
}
