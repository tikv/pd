// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
)

type mockClassifier struct{}

func (c mockClassifier) GetAllNamespaces() []string {
	return []string{"global", "unknown"}
}

func (c mockClassifier) GetStoreNamespace(store *core.StoreInfo) string {
	if store.GetId() < 5 {
		return "global"
	}
	return "unknown"
}

func (c mockClassifier) GetRegionNamespace(*core.RegionInfo) string {
	return "global"
}

func (c mockClassifier) IsNamespaceExist(name string) bool {
	return true
}

var _ = Suite(&testRegionStatistcs{})

type testRegionStatistcs struct{}

func (t *testRegionStatistcs) TestRegionStatistics(c *C) {
	_, opt := newTestScheduleConfig()
	peers := []*metapb.Peer{
		{Id: 5, StoreId: 1},
		{Id: 6, StoreId: 2},
		{Id: 4, StoreId: 3},
		{Id: 8, StoreId: 7},
	}

	metaStores := []*metapb.Store{
		{Id: 1, Address: "mock://tikv-1"},
		{Id: 2, Address: "mock://tikv-2"},
		{Id: 3, Address: "mock://tikv-3"},
		{Id: 7, Address: "mock://tikv-7"},
	}
	var stores []*core.StoreInfo
	for _, m := range metaStores {
		s := core.NewStoreInfo(m)
		stores = append(stores, s)
	}

	downPeers := []*pdpb.PeerStats{
		{Peer: peers[0], DownSeconds: 3608},
		{Peer: peers[1], DownSeconds: 3608},
	}
	r1 := &metapb.Region{Id: 1, Peers: peers, StartKey: []byte("aa"), EndKey: []byte("bb")}
	r2 := &metapb.Region{Id: 2, Peers: peers[0:2], StartKey: []byte("cc"), EndKey: []byte("dd")}
	region1 := core.NewRegionInfo(r1, peers[0])
	region2 := core.NewRegionInfo(r2, peers[0])
	regionStats := newRegionStatistics(opt, mockClassifier{})
	regionStats.Observe(region1, stores, nil)
	c.Assert(len(regionStats.stats[extraPeer]), Equals, 1)
	region1.DownPeers = downPeers
	region1.PendingPeers = peers[0:1]
	regionStats.Observe(region1, stores, nil)
	c.Assert(len(regionStats.stats[extraPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[missPeer]), Equals, 0)
	c.Assert(len(regionStats.stats[downPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[pendingPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[incorrectNamespace]), Equals, 1)
	region2.DownPeers = downPeers[0:1]
	regionStats.Observe(region2, stores[0:2], nil)
	c.Assert(len(regionStats.stats[extraPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[missPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[downPeer]), Equals, 2)
	c.Assert(len(regionStats.stats[pendingPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[incorrectNamespace]), Equals, 1)
}

func (t *testRegionStatistcs) TestRegionLabelIsolationLevel(c *C) {
	labelsSet := [][]map[string]string{
		{
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r1", "host": "h2"},
			{"zone": "z2", "rack": "r2", "host": "h3"},
		},
		{
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r2", "host": "h2"},
			{"zone": "z2", "rack": "r2", "host": "h3"},
		},
		{
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z2", "rack": "r2", "host": "h2"},
			{"zone": "z3", "rack": "r2", "host": "h3"},
		},
		{
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "rack": "r3", "host": "h3"},
		},
		{
			{"zone": "z1", "rack": "r1", "host": "h1"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
			{"zone": "z1", "rack": "r2", "host": "h2"},
		},
	}
	res := []int{2, 3, 1, 2, 0}
	f := func(labels []map[string]string, res int) {
		metaStores := []*metapb.Store{
			{Id: 1, Address: "mock://tikv-1"},
			{Id: 2, Address: "mock://tikv-2"},
			{Id: 3, Address: "mock://tikv-3"},
		}
		stores := make([]*core.StoreInfo, 0, len(labels))
		for i, m := range metaStores {
			s := core.NewStoreInfo(m)
			for k, v := range labels[i] {
				s.Labels = append(s.Labels, &metapb.StoreLabel{Key: k, Value: v})
			}
			stores = append(stores, s)
		}
		level := getRegionLabelIsolationLevel(stores, []string{"zone", "rack", "host"})
		c.Assert(level, Equals, res)
	}

	for i, labels := range labelsSet {
		f(labels, res[i])

	}
	level := getRegionLabelIsolationLevel(nil, []string{"zone", "rack", "host"})
	c.Assert(level, Equals, 0)
	level = getRegionLabelIsolationLevel(nil, nil)
	c.Assert(level, Equals, 0)

}
