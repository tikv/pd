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
	"github.com/pingcap/pd/server/namespace"
)

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
	regionStats := newRegionStatistics(opt, namespace.DefaultClassifier)
	regionStats.Observe(region1, stores)
	c.Assert(len(regionStats.stats[morePeer]), Equals, 1)
	region1.DownPeers = downPeers
	region1.PendingPeers = peers[0:1]
	region1.Peers = peers[0:3]
	regionStats.Observe(region1, stores)
	c.Assert(len(regionStats.stats[morePeer]), Equals, 0)
	c.Assert(len(regionStats.stats[missPeer]), Equals, 0)
	c.Assert(len(regionStats.stats[downPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[pendingPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[incorrectNamespace]), Equals, 0)
	region2.DownPeers = downPeers[0:1]
	regionStats.Observe(region2, stores[0:2])
	c.Assert(len(regionStats.stats[morePeer]), Equals, 0)
	c.Assert(len(regionStats.stats[missPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[downPeer]), Equals, 2)
	c.Assert(len(regionStats.stats[pendingPeer]), Equals, 1)
	c.Assert(len(regionStats.stats[incorrectNamespace]), Equals, 0)
}
