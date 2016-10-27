// Copyright 2016 PingCAP, Inc.
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
)

var _ = Suite(&testRegionSuite{})

type testRegionSuite struct{}

func (s *testRegionSuite) TestRegionInfo(c *C) {
	n := uint64(3)

	peers := make([]*metapb.Peer, 0, n)
	for i := uint64(0); i < n; i++ {
		p := &metapb.Peer{
			Id:      i,
			StoreId: i,
		}
		peers = append(peers, p)
	}
	downPeers := []*pdpb.PeerStats{
		{Peer: peers[n-1], DownSeconds: new(uint64)},
	}
	region := &metapb.Region{
		Peers: peers,
	}

	r := newRegionInfo(region, peers[0])
	r.DownPeers = downPeers
	r = r.clone()

	for i := uint64(0); i < n; i++ {
		c.Assert(r.GetPeer(i), Equals, r.Peers[i])
		c.Assert(r.ContainsPeer(i), IsTrue)
	}
	c.Assert(r.GetPeer(n), IsNil)
	c.Assert(r.ContainsPeer(n), IsFalse)

	for i := uint64(0); i < n; i++ {
		c.Assert(r.GetStorePeer(i).GetStoreId(), Equals, i)
	}
	c.Assert(r.GetStorePeer(n), IsNil)

	stores := r.GetStoreIds()
	c.Assert(stores, HasLen, int(n))
	for i := uint64(0); i < n; i++ {
		_, ok := stores[i]
		c.Assert(ok, IsTrue)
	}

	followers := r.GetFollowers()
	c.Assert(followers, HasLen, int(n-1))
	for i := uint64(1); i < n; i++ {
		c.Assert(followers[peers[i].GetStoreId()], DeepEquals, peers[i])
	}
}

func (s *testRegionSuite) TestRegionItem(c *C) {
	item := newRegionItem([]byte("b"), []byte{})

	c.Assert(item.Less(newRegionItem([]byte("a"), []byte{})), IsTrue)
	c.Assert(item.Less(newRegionItem([]byte("b"), []byte{})), IsFalse)
	c.Assert(item.Less(newRegionItem([]byte("c"), []byte{})), IsFalse)

	c.Assert(item.Contains([]byte("a")), IsFalse)
	c.Assert(item.Contains([]byte("b")), IsTrue)
	c.Assert(item.Contains([]byte("c")), IsTrue)

	item = newRegionItem([]byte("b"), []byte("d"))
	c.Assert(item.Contains([]byte("a")), IsFalse)
	c.Assert(item.Contains([]byte("b")), IsTrue)
	c.Assert(item.Contains([]byte("c")), IsTrue)
	c.Assert(item.Contains([]byte("d")), IsFalse)
}

func (s *testRegionSuite) TestRegionTree(c *C) {
	tree := newRegionTree()

	c.Assert(tree.search([]byte("a")), IsNil)

	regionA := newRegion([]byte("a"), []byte("b"))
	regionC := newRegion([]byte("c"), []byte("d"))
	regionD := newRegion([]byte("d"), []byte{})

	tree.insert(regionA)
	tree.insert(regionC)

	c.Assert(tree.search([]byte{}), IsNil)
	c.Assert(tree.search([]byte("a")), Equals, regionA)
	c.Assert(tree.search([]byte("b")), IsNil)
	c.Assert(tree.search([]byte("c")), Equals, regionC)
	c.Assert(tree.search([]byte("d")), IsNil)
	c.Assert(tree.search([]byte("e")), IsNil)

	tree.remove(regionC)
	tree.insert(regionD)

	c.Assert(tree.search([]byte{}), IsNil)
	c.Assert(tree.search([]byte("a")), Equals, regionA)
	c.Assert(tree.search([]byte("b")), IsNil)
	c.Assert(tree.search([]byte("c")), IsNil)
	c.Assert(tree.search([]byte("d")), Equals, regionD)
	c.Assert(tree.search([]byte("e")), Equals, regionD)
}

func newRegion(start, end []byte) *metapb.Region {
	return &metapb.Region{
		StartKey:    start,
		EndKey:      end,
		RegionEpoch: &metapb.RegionEpoch{},
	}
}

func newRegionItem(start, end []byte) *regionItem {
	return &regionItem{region: newRegion(start, end)}
}
