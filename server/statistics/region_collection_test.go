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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/server/storage/base"
)

func TestStatistics(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRegionStatisticsSuite{})

type testRegionStatisticsSuite struct {
	store   base.RuleStorage
	manager *placement.RuleManager
}

func (t *testRegionStatisticsSuite) SetUpTest(c *C) {
	t.store = storage.NewBuilder().WithMemoryBackend().Build()
	var err error
	t.manager = placement.NewRuleManager(t.store, nil, nil)
	err = t.manager.Initialize(3, []string{"zone", "rack", "host"})
	c.Assert(err, IsNil)
}

func (t *testRegionStatisticsSuite) TestRegionStatistics(c *C) {
	opt := config.NewTestOptions()
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

	store3 := stores[3].Clone(core.OfflineStore(false))
	stores[3] = store3
	r1 := &metapb.Region{Id: 1, Peers: peers, StartKey: []byte("aa"), EndKey: []byte("bb")}
	r2 := &metapb.Region{Id: 2, Peers: peers[0:2], StartKey: []byte("cc"), EndKey: []byte("dd")}
	region1 := core.NewRegionInfo(r1, peers[0])
	region2 := core.NewRegionInfo(r2, peers[0])
	regionStats := NewRegionStatistics(opt, t.manager)
	regionStats.Observe(region1, stores)
	c.Assert(regionStats.stats[ExtraPeer], HasLen, 1)
	c.Assert(regionStats.stats[LearnerPeer], HasLen, 1)
	c.Assert(regionStats.stats[EmptyRegion], HasLen, 1)
	c.Assert(regionStats.offlineStats[ExtraPeer], HasLen, 1)
	c.Assert(regionStats.offlineStats[LearnerPeer], HasLen, 1)
	c.Assert(regionStats.offlineStats[EmptyRegion], HasLen, 1)
	c.Assert(regionStats.offlineStats[OfflinePeer], HasLen, 1)

	region1 = region1.Clone(
		core.WithDownPeers(downPeers),
		core.WithPendingPeers(peers[0:1]),
		core.SetApproximateSize(144),
	)
	regionStats.Observe(region1, stores)
	c.Assert(regionStats.stats[ExtraPeer], HasLen, 1)
	c.Assert(regionStats.stats[MissPeer], HasLen, 0)
	c.Assert(regionStats.stats[DownPeer], HasLen, 1)
	c.Assert(regionStats.stats[PendingPeer], HasLen, 1)
	c.Assert(regionStats.stats[LearnerPeer], HasLen, 1)
	c.Assert(regionStats.stats[EmptyRegion], HasLen, 0)
	c.Assert(regionStats.offlineStats[ExtraPeer], HasLen, 1)
	c.Assert(regionStats.offlineStats[MissPeer], HasLen, 0)
	c.Assert(regionStats.offlineStats[DownPeer], HasLen, 1)
	c.Assert(regionStats.offlineStats[PendingPeer], HasLen, 1)
	c.Assert(regionStats.offlineStats[LearnerPeer], HasLen, 1)
	c.Assert(regionStats.offlineStats[EmptyRegion], HasLen, 0)
	c.Assert(regionStats.offlineStats[OfflinePeer], HasLen, 1)

	region2 = region2.Clone(core.WithDownPeers(downPeers[0:1]))
	regionStats.Observe(region2, stores[0:2])
	c.Assert(regionStats.stats[ExtraPeer], HasLen, 1)
	c.Assert(regionStats.stats[MissPeer], HasLen, 1)
	c.Assert(regionStats.stats[DownPeer], HasLen, 2)
	c.Assert(regionStats.stats[PendingPeer], HasLen, 1)
	c.Assert(regionStats.stats[LearnerPeer], HasLen, 1)
	c.Assert(regionStats.offlineStats[ExtraPeer], HasLen, 1)
	c.Assert(regionStats.offlineStats[MissPeer], HasLen, 0)
	c.Assert(regionStats.offlineStats[DownPeer], HasLen, 1)
	c.Assert(regionStats.offlineStats[PendingPeer], HasLen, 1)
	c.Assert(regionStats.offlineStats[LearnerPeer], HasLen, 1)
	c.Assert(regionStats.offlineStats[OfflinePeer], HasLen, 1)

	region1 = region1.Clone(core.WithRemoveStorePeer(7))
	regionStats.Observe(region1, stores[0:3])
	c.Assert(regionStats.stats[ExtraPeer], HasLen, 0)
	c.Assert(regionStats.stats[MissPeer], HasLen, 1)
	c.Assert(regionStats.stats[DownPeer], HasLen, 2)
	c.Assert(regionStats.stats[PendingPeer], HasLen, 1)
	c.Assert(regionStats.stats[LearnerPeer], HasLen, 0)
	c.Assert(regionStats.offlineStats[ExtraPeer], HasLen, 0)
	c.Assert(regionStats.offlineStats[MissPeer], HasLen, 0)
	c.Assert(regionStats.offlineStats[DownPeer], HasLen, 0)
	c.Assert(regionStats.offlineStats[PendingPeer], HasLen, 0)
	c.Assert(regionStats.offlineStats[LearnerPeer], HasLen, 0)
	c.Assert(regionStats.offlineStats[OfflinePeer], HasLen, 0)

	store3 = stores[3].Clone(core.UpStore())
	stores[3] = store3
	regionStats.Observe(region1, stores)
	c.Assert(regionStats.stats[OfflinePeer], HasLen, 0)
}

func (t *testRegionStatisticsSuite) TestRegionStatisticsWithPlacementRule(c *C) {
	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(true)
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
	r2 := &metapb.Region{Id: 0, Peers: peers[0:1], StartKey: []byte("aa"), EndKey: []byte("bb")}
	r3 := &metapb.Region{Id: 1, Peers: peers, StartKey: []byte("ee"), EndKey: []byte("ff")}
	r4 := &metapb.Region{Id: 2, Peers: peers[0:3], StartKey: []byte("gg"), EndKey: []byte("hh")}
	region2 := core.NewRegionInfo(r2, peers[0])
	region3 := core.NewRegionInfo(r3, peers[0])
	region4 := core.NewRegionInfo(r4, peers[0])
	regionStats := NewRegionStatistics(opt, t.manager)
	// r2 didn't match the rules
	regionStats.Observe(region2, stores)
	c.Assert(regionStats.stats[MissPeer], HasLen, 1)
	regionStats.Observe(region3, stores)
	// r3 didn't match the rules
	c.Assert(regionStats.stats[ExtraPeer], HasLen, 1)
	regionStats.Observe(region4, stores)
	// r4 match the rules
	c.Assert(regionStats.stats[MissPeer], HasLen, 1)
	c.Assert(regionStats.stats[ExtraPeer], HasLen, 1)
}

func (t *testRegionStatisticsSuite) TestRegionLabelIsolationLevel(c *C) {
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
		label := getRegionLabelIsolation(stores, locationLabels)
		labelLevelStats.Observe(region, stores, locationLabels)
		c.Assert(label, Equals, res)
		regionID++
	}

	for i, labels := range labelsSet {
		f(labels, res[i], locationLabels)
	}
	for i, res := range counter {
		c.Assert(labelLevelStats.labelCounter[i], Equals, res)
	}

	label := getRegionLabelIsolation(nil, locationLabels)
	c.Assert(label, Equals, nonIsolation)
	label = getRegionLabelIsolation(nil, nil)
	c.Assert(label, Equals, nonIsolation)
	store := core.NewStoreInfo(&metapb.Store{Id: 1, Address: "mock://tikv-1"}, core.SetStoreLabels([]*metapb.StoreLabel{{Key: "foo", Value: "bar"}}))
	label = getRegionLabelIsolation([]*core.StoreInfo{store}, locationLabels)
	c.Assert(label, Equals, "zone")

	regionID = 1
	res = []string{"rack", "none", "zone", "rack", "none", "rack", "none"}
	counter = map[string]int{"none": 3, "host": 0, "rack": 3, "zone": 1}
	locationLabels = []string{"zone", "rack"}

	for i, labels := range labelsSet {
		f(labels, res[i], locationLabels)
	}
	for i, res := range counter {
		c.Assert(labelLevelStats.labelCounter[i], Equals, res)
	}
}
