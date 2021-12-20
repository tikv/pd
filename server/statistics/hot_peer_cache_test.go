// Copyright 2019 TiKV Project Authors.
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
	"sort"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server/core"
)

var _ = Suite(&testHotPeerCache{})

type testHotPeerCache struct{}

func (t *testHotPeerCache) TestStoreTimeUnsync(c *C) {
	cache := NewHotStoresStats(WriteFlow)
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

		checkAndUpdate(c, cache, region, 3)
		{
			stats := cache.RegionStats(0)
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
	removeReplica
)

type testCacheCase struct {
	kind     FlowKind
	operator operator
	expect   int
}

func (t *testHotPeerCache) TestCache(c *C) {
	tests := []*testCacheCase{
		{ReadFlow, transferLeader, 2},
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
	defaultSize := map[FlowKind]int{
		ReadFlow:  1, // only leader
		WriteFlow: 3, // all peers
	}
	cache := NewHotStoresStats(t.kind)
	region := buildRegion(t.kind, 3, 60)
	checkAndUpdate(c, cache, region, defaultSize[t.kind])
	checkHit(c, cache, region, t.kind, false) // all peers are new

	srcStore, region := schedule(c, t.operator, region, 10)
	res := checkAndUpdate(c, cache, region, t.expect)
	checkHit(c, cache, region, t.kind, true) // hit cache
	if t.expect != defaultSize[t.kind] {
		checkNeedDelete(c, res, srcStore)
	}
}

func checkAndUpdate(c *C, cache *hotPeerCache, region *core.RegionInfo, expect ...int) []*HotPeerStat {
	res := cache.CheckRegionFlow(region)
	if len(expect) != 0 {
		c.Assert(res, HasLen, expect[0])
	}
	for _, p := range res {
		cache.Update(p)
	}
	return res
}

func checkAndUpdateSkipOne(c *C, cache *hotPeerCache, region *core.RegionInfo, expect ...int) []*HotPeerStat {
	peers := region.GetPeers()
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})
	res := cache.CheckRegionFlow(region.Clone(core.SetPeers(peers[1:])))
	if len(expect) != 0 {
		c.Assert(res, HasLen, expect[0])
	}
	for _, p := range res[1:] {
		cache.Update(p)
	}
	return res
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

func checkIntervalSum(c *C, cache *hotPeerCache, region *core.RegionInfo, peerCount int) {
	var intervalSums []int
	for _, peer := range region.GetPeers() {
		oldItem := cache.getOldHotPeerStat(region.GetID(), peer.StoreId)
		intervalSums = append(intervalSums, int(oldItem.getIntervalSum()))
	}
	c.Assert(intervalSums, HasLen, peerCount)
	sort.Ints(intervalSums)
	c.Assert(intervalSums[0] != intervalSums[peerCount-1], IsTrue)
}

func checkNeedDelete(c *C, ret []*HotPeerStat, storeID uint64) {
	for _, item := range ret {
		if item.StoreID == storeID {
			c.Assert(item.needDelete, IsTrue)
			return
		}
	}
}

func schedule(c *C, operator operator, region *core.RegionInfo, targets ...uint64) (srcStore uint64, _ *core.RegionInfo) {
	switch operator {
	case transferLeader:
		_, newLeader := pickFollower(region)
		return region.GetLeader().StoreId, region.Clone(core.WithLeader(newLeader))
	case movePeer:
		c.Assert(targets, HasLen, 1)
		index, _ := pickFollower(region)
		srcStore := region.GetPeers()[index].StoreId
		region := region.Clone(core.WithAddPeer(&metapb.Peer{Id: targets[0]*10 + 1, StoreId: targets[0]}))
		region = region.Clone(core.WithRemoveStorePeer(srcStore))
		return srcStore, region
	case addReplica:
		c.Assert(targets, HasLen, 1)
		region := region.Clone(core.WithAddPeer(&metapb.Peer{Id: targets[0]*10 + 1, StoreId: targets[0]}))
		return 0, region
	case removeReplica:
		if len(targets) == 0 {
			index, _ := pickFollower(region)
			srcStore = region.GetPeers()[index].StoreId
		} else {
			srcStore = targets[0]
		}
		region = region.Clone(core.WithRemoveStorePeer(srcStore))
		return srcStore, region

	default:
		return 0, nil
	}
}

func pickFollower(region *core.RegionInfo) (index int, peer *metapb.Peer) {
	var dst int
	meta := region.GetMeta()

	for index, peer := range meta.Peers {
		if peer.StoreId == region.GetLeader().StoreId {
			continue
		}
		dst = index
		if rand.Intn(2) == 0 {
			break
		}
	}
	return dst, meta.Peers[dst]
}

func buildRegion(kind FlowKind, peerCount int, interval uint64) *core.RegionInfo {
	peers := newPeers(peerCount,
		func(i int) uint64 { return uint64(10000 + i) },
		func(i int) uint64 { return uint64(i) })
	meta := &metapb.Region{
		Id:          1000,
		Peers:       peers,
		StartKey:    []byte(""),
		EndKey:      []byte(""),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 6, Version: 6},
	}

	leader := meta.Peers[rand.Intn(3)]

	switch kind {
	case ReadFlow:
		return core.NewRegionInfo(
			meta,
			leader,
			core.SetReportInterval(interval),
			core.SetReadBytes(10*1024*1024*interval),
			core.SetReadKeys(10*1024*1024*interval),
		)
	case WriteFlow:
		return core.NewRegionInfo(
			meta,
			leader,
			core.SetReportInterval(interval),
			core.SetWrittenBytes(10*1024*1024*interval),
			core.SetWrittenKeys(10*1024*1024*interval),
		)
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

func (t *testHotPeerCache) TestUpdateHotPeerStat(c *C) {
	cache := NewHotStoresStats(ReadFlow)

	// skip interval=0
	newItem := &HotPeerStat{needDelete: false, thresholds: [2]float64{0.0, 0.0}}
	newItem = cache.updateHotPeerStat(newItem, nil, 0, 0, 0)
	c.Check(newItem, IsNil)

	// new peer, interval is larger than report interval, but no hot
	newItem = &HotPeerStat{needDelete: false, thresholds: [2]float64{1.0, 1.0}}
	newItem = cache.updateHotPeerStat(newItem, nil, 0, 0, 60*time.Second)
	c.Check(newItem, IsNil)

	// new peer, interval is less than report interval
	newItem = &HotPeerStat{needDelete: false, thresholds: [2]float64{0.0, 0.0}}
	newItem = cache.updateHotPeerStat(newItem, nil, 60, 60, 30*time.Second)
	c.Check(newItem, NotNil)
	c.Check(newItem.HotDegree, Equals, 0)
	c.Check(newItem.AntiCount, Equals, 0)
	// sum of interval is less than report interval
	oldItem := newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 10*time.Second)
	c.Check(newItem.HotDegree, Equals, 0)
	c.Check(newItem.AntiCount, Equals, 0)
	// sum of interval is larger than report interval, and hot
	oldItem = newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 30*time.Second)
	c.Check(newItem.HotDegree, Equals, 1)
	c.Check(newItem.AntiCount, Equals, 2)
	// sum of interval is less than report interval
	oldItem = newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 10*time.Second)
	c.Check(newItem.HotDegree, Equals, 1)
	c.Check(newItem.AntiCount, Equals, 2)
	// sum of interval is larger than report interval, and hot
	oldItem = newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 50*time.Second)
	c.Check(newItem.HotDegree, Equals, 2)
	c.Check(newItem.AntiCount, Equals, 2)
	// sum of interval is larger than report interval, and cold
	oldItem = newItem
	newItem.thresholds = [2]float64{10.0, 10.0}
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 60*time.Second)
	c.Check(newItem.HotDegree, Equals, 1)
	c.Check(newItem.AntiCount, Equals, 1)
	// sum of interval is larger than report interval, and cold
	oldItem = newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, 60, 60, 60*time.Second)
	c.Check(newItem.HotDegree, Equals, 0)
	c.Check(newItem.AntiCount, Equals, 0)
	c.Check(newItem.needDelete, Equals, true)
}

func (t *testHotPeerCache) TestThresholdWithUpdateHotPeerStat(c *C) {
	byteRate := minHotThresholds[ReadFlow][byteDim] * 2
	expectThreshold := byteRate * HotThresholdRatio
	t.testMetrics(c, 120., byteRate, expectThreshold)
	t.testMetrics(c, 60., byteRate, expectThreshold)
	t.testMetrics(c, 30., byteRate, expectThreshold)
	t.testMetrics(c, 17., byteRate, expectThreshold)
	t.testMetrics(c, 1., byteRate, expectThreshold)
}
func (t *testHotPeerCache) testMetrics(c *C, interval, byteRate, expectThreshold float64) {
	cache := NewHotStoresStats(ReadFlow)
	minThresholds := minHotThresholds[cache.kind]
	storeID := uint64(1)
	c.Assert(byteRate, GreaterEqual, minThresholds[byteDim])
	for i := uint64(1); i < TopNN+10; i++ {
		var oldItem *HotPeerStat
		for {
			thresholds := cache.calcHotThresholds(storeID)
			newItem := &HotPeerStat{
				StoreID:    storeID,
				RegionID:   i,
				needDelete: false,
				thresholds: thresholds,
				ByteRate:   byteRate,
				KeyRate:    0,
			}
			oldItem = cache.getOldHotPeerStat(i, storeID)
			if oldItem != nil && oldItem.rollingByteRate.isHot(thresholds) == true {
				break
			}
			item := cache.updateHotPeerStat(newItem, oldItem, byteRate*interval, 0, time.Duration(interval)*time.Second)
			cache.Update(item)
		}
		thresholds := cache.calcHotThresholds(storeID)
		if i < TopNN {
			c.Assert(thresholds[byteDim], Equals, minThresholds[byteDim])
		} else {
			c.Assert(thresholds[byteDim], Equals, expectThreshold)
		}
	}
}

func (t *testHotPeerCache) TestRemoveFromCache(c *C) {
	peerCount := 3
	cache := NewHotStoresStats(WriteFlow)
	region := buildRegion(WriteFlow, peerCount, 5)
	// prepare
	for i := 1; i <= 200; i++ {
		checkAndUpdate(c, cache, region)
	}
	// make the interval sum of peers are different
	checkAndUpdateSkipOne(c, cache, region)
	checkIntervalSum(c, cache, region, peerCount)
	// check whether cold cache is cleared
	var isClear bool
	region = region.Clone(core.SetWrittenBytes(0), core.SetWrittenKeys(0))
	for i := 1; i <= 200; i++ {
		checkAndUpdate(c, cache, region)
		if len(cache.storesOfRegion[region.GetID()]) == 0 {
			isClear = true
			break
		}
	}
	c.Assert(isClear, IsTrue)
}

func (t *testHotPeerCache) TestRemoveFromCacheRandom(c *C) {
	peerCounts := []int{3, 5}
	intervals := []uint64{120, 60, 10, 5}

	for _, peerCount := range peerCounts {
		for _, interval := range intervals {
			cache := NewHotStoresStats(WriteFlow)
			region := buildRegion(WriteFlow, peerCount, interval)

			target := uint64(10)
			movePeer := func() {
				tmp := uint64(0)
				tmp, region = schedule(c, removeReplica, region)
				_, region = schedule(c, addReplica, region, target)
				target = tmp
			}
			// prepare with random move peer to make the interval sum of peers are different
			for i := 1; i <= 200; i++ {
				if i%5 == 0 {
					movePeer()
				}
				checkAndUpdate(c, cache, region)
			}
			if interval < RegionHeartBeatReportInterval {
				checkIntervalSum(c, cache, region, peerCount)
			}
			c.Assert(cache.storesOfRegion[region.GetID()], HasLen, peerCount)
			// check whether cold cache is cleared
			var isClear bool
			region = region.Clone(core.SetWrittenBytes(0), core.SetWrittenKeys(0))
			for i := 1; i <= 200; i++ {
				if i%5 == 0 {
					movePeer()
				}
				checkAndUpdate(c, cache, region)
				if len(cache.storesOfRegion[region.GetID()]) == 0 {
					isClear = true
					break
				}
			}
			c.Assert(isClear, IsTrue)
		}
	}
}

func BenchmarkCheckRegionFlow(b *testing.B) {
	cache := NewHotStoresStats(ReadFlow)
	region := core.NewRegionInfo(&metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			{Id: 101, StoreId: 1},
			{Id: 102, StoreId: 2},
			{Id: 103, StoreId: 3},
		},
	},
		&metapb.Peer{Id: 101, StoreId: 1},
	)
	newRegion := region.Clone(
		core.WithInterval(&pdpb.TimeInterval{StartTimestamp: 0, EndTimestamp: 10}),
		core.SetReadBytes(30000*10),
		core.SetReadKeys(300000*10))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rets := cache.CheckRegionFlow(newRegion)
		for _, ret := range rets {
			cache.Update(ret)
		}
	}
}
