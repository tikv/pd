// Copyright 2019 PingCAP, Inc.
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

package statistics

import (
	"math/rand"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/server/core"
)

var _ = Suite(&testHotPeerCache{})

type testHotPeerCache struct{}

func (t *testHotPeerCache) TestStoreTimeUnsync(c *C) {
	cache := NewHotStoresStats(WriteFlow)
	stats := NewStoresStats()
	peers := newPeers(3,
		func(i int) uint64 { return uint64(10000 + i) },
		func(i int) uint64 { return uint64(i) })
	meta := &metapb.Region{
		Id:          1000,
		Peers:       peers,
		StartKey:    []byte(""),
		EndKey:      []byte(""),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 6, Version: 6},
	}
	intervals := []uint64{120, 60}
	for _, interval := range intervals {
		region := core.NewRegionInfo(meta, peers[0],
			// interval is [0, interval]
			core.SetReportInterval(interval),
			core.SetWrittenBytes(interval*100*1024))

		checkAndUpdate(c, cache, region, stats, 3)
		{
			stats := cache.RegionStats()
			c.Assert(stats, HasLen, 3)
			for _, s := range stats {
				c.Assert(s, HasLen, 1)
			}
		}
	}
}

type operator int

const (
	transferLeader operator = iota
	movePeer
	addReplica
)

type testCacheCase struct {
	kind     FlowKind
	operator operator
	expect   int
}

func (t *testHotPeerCache) TestCache(c *C) {
	tests := []*testCacheCase{
		{ReadFlow, transferLeader, 1},
		{ReadFlow, movePeer, 1},
		{ReadFlow, addReplica, 1},
		{WriteFlow, transferLeader, 3},
		{WriteFlow, movePeer, 4},
		{WriteFlow, addReplica, 4},
	}
	for _, t := range tests {
		testCache(c, t)
	}
}

func testCache(c *C, t *testCacheCase) {
	cache := NewHotStoresStats(t.kind)
	stats := NewStoresStats()
	region := buildRegion(nil, nil, t.kind)
	if t.kind == ReadFlow {
		checkAndUpdate(c, cache, region, stats, 1) // only leader
	} else {
		checkAndUpdate(c, cache, region, stats, 3) // all peer
	}
	checkHit(c, cache, region, t.kind, false) // all peers are new
	schedule(t.operator, region, t.kind)
	checkAndUpdate(c, cache, region, stats, t.expect)
	checkHit(c, cache, region, t.kind, true) // hit cache
}

func checkAndUpdate(c *C, cache *hotPeerCache, region *core.RegionInfo, stats *StoresStats, num int) {
	res := cache.CheckRegionFlow(region, stats)
	c.Assert(res, HasLen, num)
	for _, p := range res {
		cache.Update(p)
	}
}

func checkHit(c *C, cache *hotPeerCache, region *core.RegionInfo, kind FlowKind, isHit bool) {
	var peers []*metapb.Peer
	if kind == ReadFlow {
		peers = []*metapb.Peer{region.GetLeader()}
	} else {
		peers = region.GetPeers()
	}
	for _, peer := range peers {
		item := cache.getOldHotPeerStat(region.GetID(), peer.StoreId)
		c.Assert(item, NotNil)
		c.Assert(item.isNew, Equals, !isHit)
	}
}

const interval = uint64(60)

func schedule(operator operator, region *core.RegionInfo, kind FlowKind) *core.RegionInfo {
	switch operator {
	case transferLeader:
		_, newLeader := pickFollower(region)
		return buildRegion(region.GetMeta(), newLeader, kind)
	case movePeer:
		index, _ := pickFollower(region)
		meta := region.GetMeta()
		meta.Peers[index] = &metapb.Peer{Id: 4, StoreId: 4}
		return buildRegion(meta, region.GetLeader(), kind)
	case addReplica:
		meta := region.GetMeta()
		meta.Peers = append(meta.Peers, &metapb.Peer{Id: 4, StoreId: 4})
		return buildRegion(meta, region.GetLeader(), kind)
	default:
		return nil
	}
}

func pickFollower(region *core.RegionInfo) (index int, peer *metapb.Peer) {
	var dst int
	meta := region.GetMeta()
	srcStoreID := region.GetLeader().StoreId

	for index, peer := range meta.Peers {
		if peer.StoreId == srcStoreID {
			continue
		}
		dst = index
		if rand.Intn(2) == 0 {
			break
		}
	}
	return dst, meta.Peers[dst]
}

func buildRegion(meta *metapb.Region, leader *metapb.Peer, kind FlowKind) *core.RegionInfo {
	if meta == nil {
		peer1 := &metapb.Peer{Id: 1, StoreId: 1}
		peer2 := &metapb.Peer{Id: 2, StoreId: 2}
		peer3 := &metapb.Peer{Id: 3, StoreId: 3}

		meta = &metapb.Region{
			Id:          1000,
			Peers:       []*metapb.Peer{peer1, peer2, peer3},
			StartKey:    []byte(""),
			EndKey:      []byte(""),
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 6, Version: 6},
		}
		leader = meta.Peers[rand.Intn(3)]
	}

	switch kind {
	case ReadFlow:
		return core.NewRegionInfo(meta, leader, core.SetReportInterval(interval),
			core.SetReadBytes(interval*100*1024))
	case WriteFlow:
		return core.NewRegionInfo(meta, leader, core.SetReportInterval(interval),
			core.SetWrittenBytes(interval*100*1024))
	default:
		return nil
	}
}

type genID func(i int) uint64

func newPeers(n int, pid genID, sid genID) []*metapb.Peer {
	peers := make([]*metapb.Peer, 0, n)
	for i := 1; i <= n; i++ {
		peer := &metapb.Peer{
			Id: pid(i),
		}
		peer.StoreId = sid(i)
		peers = append(peers, peer)
	}
	return peers
}
