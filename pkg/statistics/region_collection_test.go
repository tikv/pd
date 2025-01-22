// Copyright 2018 TiKV Project Authors.
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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage"
)

func TestRegionStatistics(t *testing.T) {
	re := require.New(t)
	store := storage.NewStorageWithMemoryBackend()
	manager := placement.NewRuleManager(context.Background(), store, nil, nil)
	err := manager.Initialize(3, []string{"zone", "rack", "host"}, "", false)
	re.NoError(err)
	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	peers := []*metapb.Peer{
		{Id: 5, StoreId: 1},
		{Id: 6, StoreId: 2},
		{Id: 4, StoreId: 3},
		{Id: 8, StoreId: 7, Role: metapb.PeerRole_Learner},
	}

	metaStores := []*metapb.Store{
		{Id: 1, Address: "mock://tikv-1"},
		{Id: 2, Address: "mock://tikv-2"},
		{Id: 3, Address: "mock://tikv-3"},
		{Id: 7, Address: "mock://tikv-7"},
	}

	stores := make([]*core.StoreInfo, 0, len(metaStores))
	for _, m := range metaStores {
		s := core.NewStoreInfo(m)
		stores = append(stores, s)
	}

	downPeers := []*pdpb.PeerStats{
		{Peer: peers[0], DownSeconds: 3608},
		{Peer: peers[1], DownSeconds: 3608},
	}

	store3 := stores[3].Clone(core.SetStoreState(metapb.StoreState_Offline, false))
	stores[3] = store3
	r1 := &metapb.Region{Id: 1, Peers: peers, StartKey: []byte("aa"), EndKey: []byte("bb")}
	r2 := &metapb.Region{Id: 2, Peers: peers[0:2], StartKey: []byte("cc"), EndKey: []byte("dd")}
	region1 := core.NewRegionInfo(r1, peers[0])
	region2 := core.NewRegionInfo(r2, peers[0])
	regionStats := NewRegionStatistics(nil, opt, manager)
	regionStats.Observe(region1, stores)
	re.Len(regionStats.stats[ExtraPeer], 1)
	re.Len(regionStats.stats[LearnerPeer], 1)
	re.Len(regionStats.stats[EmptyRegion], 1)
	re.Len(regionStats.stats[UndersizedRegion], 1)
	re.Len(regionStats.stats[OfflinePeer], 1)

	region1 = region1.Clone(
		core.WithDownPeers(downPeers),
		core.WithPendingPeers(peers[0:1]),
		core.SetApproximateSize(144),
	)
	regionStats.Observe(region1, stores)
	re.Len(regionStats.stats[ExtraPeer], 1)
	re.Empty(regionStats.stats[MissPeer])
	re.Len(regionStats.stats[DownPeer], 1)
	re.Len(regionStats.stats[PendingPeer], 1)
	re.Len(regionStats.stats[LearnerPeer], 1)
	re.Empty(regionStats.stats[EmptyRegion])
	re.Len(regionStats.stats[OversizedRegion], 1)
	re.Empty(regionStats.stats[UndersizedRegion])
	re.Len(regionStats.stats[OfflinePeer], 1)

	region2 = region2.Clone(core.WithDownPeers(downPeers[0:1]))
	regionStats.Observe(region2, stores[0:2])
	re.Len(regionStats.stats[ExtraPeer], 1)
	re.Len(regionStats.stats[MissPeer], 1)
	re.Len(regionStats.stats[DownPeer], 2)
	re.Len(regionStats.stats[PendingPeer], 1)
	re.Len(regionStats.stats[LearnerPeer], 1)
	re.Len(regionStats.stats[OversizedRegion], 1)
	re.Len(regionStats.stats[UndersizedRegion], 1)
	re.Len(regionStats.stats[OfflinePeer], 1)

	region1 = region1.Clone(core.WithRemoveStorePeer(7))
	regionStats.Observe(region1, stores[0:3])
	re.Empty(regionStats.stats[ExtraPeer])
	re.Len(regionStats.stats[MissPeer], 1)
	re.Len(regionStats.stats[DownPeer], 2)
	re.Len(regionStats.stats[PendingPeer], 1)
	re.Empty(regionStats.stats[LearnerPeer])
	re.Empty(regionStats.stats[OfflinePeer])

	store3 = stores[3].Clone(core.SetStoreState(metapb.StoreState_Up))
	stores[3] = store3
	regionStats.Observe(region1, stores)
	re.Empty(regionStats.stats[OfflinePeer])
}

func TestRegionStatisticsWithPlacementRule(t *testing.T) {
	re := require.New(t)
	store := storage.NewStorageWithMemoryBackend()
	manager := placement.NewRuleManager(context.Background(), store, nil, nil)
	err := manager.Initialize(3, []string{"zone", "rack", "host"}, "", false)
	re.NoError(err)
	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(true)
	peers := []*metapb.Peer{
		{Id: 5, StoreId: 1},
		{Id: 6, StoreId: 2},
		{Id: 4, StoreId: 3},
		{Id: 8, StoreId: 7, Role: metapb.PeerRole_Learner},
		{Id: 9, StoreId: 8, IsWitness: true},
	}
	metaStores := []*metapb.Store{
		{Id: 1, Address: "mock://tikv-1"},
		{Id: 2, Address: "mock://tikv-2"},
		{Id: 3, Address: "mock://tikv-3"},
		{Id: 7, Address: "mock://tikv-7"},
	}

	stores := make([]*core.StoreInfo, 0, len(metaStores))
	for _, m := range metaStores {
		s := core.NewStoreInfo(m)
		stores = append(stores, s)
	}
	r2 := &metapb.Region{Id: 0, Peers: peers[0:1], StartKey: []byte("aa"), EndKey: []byte("bb")}
	r3 := &metapb.Region{Id: 1, Peers: peers, StartKey: []byte("ee"), EndKey: []byte("ff")}
	r4 := &metapb.Region{Id: 2, Peers: peers[0:3], StartKey: []byte("gg"), EndKey: []byte("hh")}
	r5 := &metapb.Region{Id: 0, Peers: peers[2:], StartKey: []byte("aa"), EndKey: []byte("bb")}
	region2 := core.NewRegionInfo(r2, peers[0])
	region3 := core.NewRegionInfo(r3, peers[0])
	region4 := core.NewRegionInfo(r4, peers[0])
	region5 := core.NewRegionInfo(r5, peers[4])
	regionStats := NewRegionStatistics(nil, opt, manager)
	// r2 didn't match the rules
	regionStats.Observe(region2, stores)
	re.Len(regionStats.stats[MissPeer], 1)
	regionStats.Observe(region3, stores)
	// r3 didn't match the rules
	re.Len(regionStats.stats[ExtraPeer], 1)
	regionStats.Observe(region4, stores)
	// r4 match the rules
	re.Len(regionStats.stats[MissPeer], 1)
	re.Len(regionStats.stats[ExtraPeer], 1)
	regionStats.Observe(region5, stores)
	// r5 match the rule
	re.Len(regionStats.stats[WitnessLeader], 1)
}

func TestRegionLabelIsolationLevel(t *testing.T) {
	re := require.New(t)
	locationLabels := []string{"zone", "rack", "host"}
	labelLevelStats := NewLabelStatistics()
	labelsSet := [][]map[string]string{
		{
			// isolated by rack
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r1", "host": "h2"},
			{"zone": "z2", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by host when location labels is ["zone", "rack", "host"]
			// cannot be isolated when location labels is ["zone", "rack"]
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r2", "host": "h2"},
			{"zone": "z2", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by zone
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r2", "host": "h2"},
			{"zone": "z3", "rack": "r2", "host": "h3"},
		},
		{
			// isolated by rack
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "rack": "r3", "host": "h3"},
		},
		{
			// cannot be isolated
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
		},
		{
			// isolated by rack
			{"rack": "r1", "host": "h1"},
			{"rack": "r2", "host": "h2"},
			{"rack": "r3", "host": "h3"},
		},
		{
			// isolated by host
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "host": "h3"},
		},
	}
	res := []string{"rack", "host", "zone", "rack", "none", "rack", "host"}
	counter := map[string]int{"none": 1, "host": 2, "rack": 3, "zone": 1}
	regionID := 1
	f := func(labels []map[string]string, res string, locationLabels []string) {
		metaStores := []*metapb.Store{
			{Id: 1, Address: "mock://tikv-1"},
			{Id: 2, Address: "mock://tikv-2"},
			{Id: 3, Address: "mock://tikv-3"},
		}
		stores := make([]*core.StoreInfo, 0, len(labels))
		for i, m := range metaStores {
			var newLabels []*metapb.StoreLabel
			for k, v := range labels[i] {
				newLabels = append(newLabels, &metapb.StoreLabel{Key: k, Value: v})
			}
			s := core.NewStoreInfo(m, core.SetStoreLabels(newLabels))

			stores = append(stores, s)
		}
		region := core.NewRegionInfo(&metapb.Region{Id: uint64(regionID)}, nil)
		label := GetRegionLabelIsolation(stores, locationLabels)
		labelLevelStats.Observe(region, stores, locationLabels)
		re.Equal(res, label)
		regionID++
	}

	for i, labels := range labelsSet {
		f(labels, res[i], locationLabels)
	}
	for i, res := range counter {
		re.Equal(res, labelLevelStats.labelCounter[i])
	}

	label := GetRegionLabelIsolation(nil, locationLabels)
	re.Equal(nonIsolation, label)
	label = GetRegionLabelIsolation(nil, nil)
	re.Equal(nonIsolation, label)
	store := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "mock://tikv-1"}, core.SetStoreLabels([]*metapb.StoreLabel{{Key: "foo", Value: "bar"}}))
	label = GetRegionLabelIsolation([]*core.StoreInfo{store}, locationLabels)
	re.Equal("zone", label)

	regionID = 1
	res = []string{"rack", "none", "zone", "rack", "none", "rack", "none"}
	counter = map[string]int{"none": 3, "host": 0, "rack": 3, "zone": 1}
	locationLabels = []string{"zone", "rack"}

	for i, labels := range labelsSet {
		f(labels, res[i], locationLabels)
	}
	for i, res := range counter {
		re.Equal(res, labelLevelStats.labelCounter[i])
	}
}

func BenchmarkObserve(b *testing.B) {
	// Setup
	store := storage.NewStorageWithMemoryBackend()
	manager := placement.NewRuleManager(context.Background(), store, nil, nil)
	manager.Initialize(3, []string{"zone", "rack", "host"}, "", false)
	opt := mockconfig.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	peers := []*metapb.Peer{
		{Id: 4, StoreId: 1},
		{Id: 5, StoreId: 2},
		{Id: 6, StoreId: 3},
	}

	metaStores := []*metapb.Store{
		{Id: 1, Address: "mock://tikv-1"},
		{Id: 2, Address: "mock://tikv-2"},
		{Id: 3, Address: "mock://tikv-3"},
	}

	stores := make([]*core.StoreInfo, 0, len(metaStores))
	for _, m := range metaStores {
		s := core.NewStoreInfo(m)
		stores = append(stores, s)
	}

	regionNum := uint64(1000000)
	regions := make([]*core.RegionInfo, 0, regionNum)
	for i := uint64(1); i <= regionNum; i++ {
		r := &metapb.Region{Id: i, Peers: peers, StartKey: []byte{byte(i)}, EndKey: []byte{byte(i + 1)}}
		regions = append(regions, core.NewRegionInfo(r, peers[0]))
	}
	regionStats := NewRegionStatistics(nil, opt, manager)

	b.ResetTimer()
	// Run the Observe function b.N times
	for i := 0; i < b.N; i++ {
		regionStats.Observe(regions[i%int(regionNum)], stores)
	}
}
