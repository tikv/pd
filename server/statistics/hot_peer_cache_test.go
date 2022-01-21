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
	"github.com/tikv/pd/server/core"
)

var _ = Suite(&testHotPeerCache{})

type testHotPeerCache struct{}

func (t *testHotPeerCache) TestStoreTimeUnsync(c *C) {
	cache := NewHotPeerCache(WriteFlow)
	intervals := []uint64{120, 60}
	for _, interval := range intervals {
		region := buildRegion(WriteFlow, 3, interval)
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
	kind       FlowKind
	operator   operator
	expect     int
	needDelete bool
}

func (t *testHotPeerCache) TestCache(c *C) {
	tests := []*testCacheCase{
		{ReadFlow, transferLeader, 3, false},
		{ReadFlow, movePeer, 4, true},
		{ReadFlow, addReplica, 4, false},
		{WriteFlow, transferLeader, 3, true},
		{WriteFlow, movePeer, 4, true},
		{WriteFlow, addReplica, 4, true},
	}
	for _, t := range tests {
		testCache(c, t)
	}
}

func testCache(c *C, t *testCacheCase) {
	defaultSize := map[FlowKind]int{
		ReadFlow:  3, // all peers
		WriteFlow: 3, // all peers
	}
	cache := NewHotPeerCache(t.kind)
	region := buildRegion(t.kind, 3, 60)
	checkAndUpdate(c, cache, region, defaultSize[t.kind])
	checkHit(c, cache, region, t.kind, false) // all peers are new

	srcStore, region := schedule(c, t.operator, region, 10)
	res := checkAndUpdate(c, cache, region, t.expect)
	checkHit(c, cache, region, t.kind, true) // hit cache
	if t.expect != defaultSize[t.kind] {
		checkNeedDelete(c, res, srcStore, t.needDelete)
	}
}

func orderingPeers(cache *hotPeerCache, region *core.RegionInfo) []*metapb.Peer {
	var peers []*metapb.Peer
	for _, peer := range region.GetPeers() {
		if cache.getOldHotPeerStat(region.GetID(), peer.StoreId) != nil {
			peers = append([]*metapb.Peer{peer}, peers...)
		} else {
			peers = append(peers, peer)
		}
	}
	return peers
}

func checkFlow(cache *hotPeerCache, region *core.RegionInfo, peers []*metapb.Peer) (res []*HotPeerStat) {
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	res = append(res, cache.CollectExpiredItems(region)...)
	for _, peer := range peers {
		peerInfo := core.NewPeerInfo(peer, region.GetLoads(), interval)
		item := cache.CheckPeerFlow(peerInfo, region)
		if item != nil {
			res = append(res, item)
		}
	}
	return res
}

func updateFlow(cache *hotPeerCache, res []*HotPeerStat) []*HotPeerStat {
	for _, p := range res {
		cache.Update(p)
	}
	return res
}

type check func(c *C, cache *hotPeerCache, region *core.RegionInfo, expect ...int) (res []*HotPeerStat)

func checkAndUpdate(c *C, cache *hotPeerCache, region *core.RegionInfo, expect ...int) (res []*HotPeerStat) {
	res = checkFlow(cache, region, region.GetPeers())
	if len(expect) != 0 {
		c.Assert(res, HasLen, expect[0])
	}
	return updateFlow(cache, res)
}

// Check and update peers in the specified order that old item that he items that have not expired come first, and the items that have expired come second.
// This order is also similar to the previous version. By the way the order in now version is random.
func checkAndUpdateWithOrdering(c *C, cache *hotPeerCache, region *core.RegionInfo, expect ...int) (res []*HotPeerStat) {
	res = checkFlow(cache, region, orderingPeers(cache, region))
	if len(expect) != 0 {
		c.Assert(res, HasLen, expect[0])
	}
	return updateFlow(cache, res)
}

func checkAndUpdateSkipOne(c *C, cache *hotPeerCache, region *core.RegionInfo, expect ...int) (res []*HotPeerStat) {
	res = checkFlow(cache, region, region.GetPeers()[1:])
	if len(expect) != 0 {
		c.Assert(res, HasLen, expect[0])
	}
	return updateFlow(cache, res)
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

func checkNeedDelete(c *C, ret []*HotPeerStat, storeID uint64, needDelete bool) {
	for _, item := range ret {
		if item.StoreID == storeID {
			c.Assert(item.needDelete, Equals, needDelete)
			return
		}
	}
}

// checkIntervalSum checks whether the interval sum of the peers are different.
func checkIntervalSum(cache *hotPeerCache, region *core.RegionInfo) bool {
	var intervalSums []int
	for _, peer := range region.GetPeers() {
		oldItem := cache.getOldHotPeerStat(region.GetID(), peer.StoreId)
		if oldItem != nil {
			intervalSums = append(intervalSums, int(oldItem.getIntervalSum()))
		}
	}
	sort.Ints(intervalSums)
	return intervalSums[0] != intervalSums[len(intervalSums)-1]
}

// checkIntervalSumContinuous checks whether the interval sum of the peer is continuous.
func checkIntervalSumContinuous(c *C, intervalSums map[uint64]int, rets []*HotPeerStat, interval uint64) {
	for _, ret := range rets {
		if ret.needDelete == true {
			delete(intervalSums, ret.StoreID)
			continue
		}
		new := int(ret.getIntervalSum() / 1000000000)
		if ret.source == direct {
			if old, ok := intervalSums[ret.StoreID]; ok {
				c.Assert(new, Equals, (old+int(interval))%RegionHeartBeatReportInterval)
			}
		}
		intervalSums[ret.StoreID] = new
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
			core.SetReadQuery(1024*interval),
		)
	case WriteFlow:
		return core.NewRegionInfo(
			meta,
			leader,
			core.SetReportInterval(interval),
			core.SetWrittenBytes(10*1024*1024*interval),
			core.SetWrittenKeys(10*1024*1024*interval),
			core.SetWrittenQuery(1024*interval),
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
	cache := NewHotPeerCache(ReadFlow)
	// we statistic read peer info from store heartbeat rather than region heartbeat
	m := RegionHeartBeatReportInterval / StoreHeartBeatReportInterval

	// skip interval=0
	newItem := &HotPeerStat{needDelete: false, thresholds: []float64{0.0, 0.0, 0.0}, Kind: ReadFlow}
	newItem = cache.updateHotPeerStat(newItem, nil, []float64{0.0, 0.0, 0.0}, 0)
	c.Check(newItem, IsNil)

	// new peer, interval is larger than report interval, but no hot
	newItem = &HotPeerStat{needDelete: false, thresholds: []float64{1.0, 1.0, 1.0}, Kind: ReadFlow}
	newItem = cache.updateHotPeerStat(newItem, nil, []float64{0.0, 0.0, 0.0}, 10*time.Second)
	c.Check(newItem, IsNil)

	// new peer, interval is less than report interval
	newItem = &HotPeerStat{needDelete: false, thresholds: []float64{0.0, 0.0, 0.0}, Kind: ReadFlow}
	newItem = cache.updateHotPeerStat(newItem, nil, []float64{60.0, 60.0, 60.0}, 4*time.Second)
	c.Check(newItem, NotNil)
	c.Check(newItem.HotDegree, Equals, 0)
	c.Check(newItem.AntiCount, Equals, 0)
	// sum of interval is less than report interval
	oldItem := newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, []float64{60.0, 60.0, 60.0}, 4*time.Second)
	c.Check(newItem.HotDegree, Equals, 0)
	c.Check(newItem.AntiCount, Equals, 0)
	// sum of interval is larger than report interval, and hot
	oldItem = newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, []float64{60.0, 60.0, 60.0}, 4*time.Second)
	c.Check(newItem.HotDegree, Equals, 1)
	c.Check(newItem.AntiCount, Equals, 2*m)
	// sum of interval is less than report interval
	oldItem = newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, []float64{60.0, 60.0, 60.0}, 4*time.Second)
	c.Check(newItem.HotDegree, Equals, 1)
	c.Check(newItem.AntiCount, Equals, 2*m)
	// sum of interval is larger than report interval, and hot
	oldItem = newItem
	newItem = cache.updateHotPeerStat(newItem, oldItem, []float64{60.0, 60.0, 60.0}, 10*time.Second)
	c.Check(newItem.HotDegree, Equals, 2)
	c.Check(newItem.AntiCount, Equals, 2*m)
	// sum of interval is larger than report interval, and cold
	oldItem = newItem
	newItem.thresholds = []float64{10.0, 10.0, 10.0}
	newItem = cache.updateHotPeerStat(newItem, oldItem, []float64{60.0, 60.0, 60.0}, 10*time.Second)
	c.Check(newItem.HotDegree, Equals, 1)
	c.Check(newItem.AntiCount, Equals, 2*m-1)
	// sum of interval is larger than report interval, and cold
	for i := 0; i < 2*m-1; i++ {
		oldItem = newItem
		newItem = cache.updateHotPeerStat(newItem, oldItem, []float64{60.0, 60.0, 60.0}, 10*time.Second)
	}
	c.Check(newItem.HotDegree, Less, 0)
	c.Check(newItem.AntiCount, Equals, 0)
	c.Check(newItem.needDelete, Equals, true)
}

func (t *testHotPeerCache) TestThresholdWithUpdateHotPeerStat(c *C) {
	byteRate := minHotThresholds[RegionReadBytes] * 2
	expectThreshold := byteRate * HotThresholdRatio
	t.testMetrics(c, 120., byteRate, expectThreshold)
	t.testMetrics(c, 60., byteRate, expectThreshold)
	t.testMetrics(c, 30., byteRate, expectThreshold)
	t.testMetrics(c, 17., byteRate, expectThreshold)
	t.testMetrics(c, 1., byteRate, expectThreshold)
}

func (t *testHotPeerCache) testMetrics(c *C, interval, byteRate, expectThreshold float64) {
	cache := NewHotPeerCache(ReadFlow)
	storeID := uint64(1)
	c.Assert(byteRate, GreaterEqual, minHotThresholds[RegionReadBytes])
	for i := uint64(1); i < TopNN+10; i++ {
		var oldItem *HotPeerStat
		for {
			thresholds := cache.calcHotThresholds(storeID)
			newItem := &HotPeerStat{
				Kind:       cache.kind,
				StoreID:    storeID,
				RegionID:   i,
				needDelete: false,
				thresholds: thresholds,
				Loads:      make([]float64, DimLen),
			}
			newItem.Loads[RegionReadBytes] = byteRate
			newItem.Loads[RegionReadKeys] = 0
			oldItem = cache.getOldHotPeerStat(i, storeID)
			if oldItem != nil && oldItem.rollingLoads[RegionReadBytes].isHot(thresholds[RegionReadBytes]) == true {
				break
			}
			item := cache.updateHotPeerStat(newItem, oldItem, []float64{byteRate * interval, 0.0, 0.0}, time.Duration(interval)*time.Second)
			cache.Update(item)
		}
		thresholds := cache.calcHotThresholds(storeID)
		if i < TopNN {
			c.Assert(thresholds[RegionReadBytes], Equals, minHotThresholds[RegionReadBytes])
		} else {
			c.Assert(thresholds[RegionReadBytes], Equals, expectThreshold)
		}
	}
}

func (t *testHotPeerCache) TestRemoveFromCache(c *C) {
	peerCount := 3
	interval := uint64(5)
	checkers := []check{checkAndUpdate, checkAndUpdateWithOrdering}
	for _, checker := range checkers {
		cache := NewHotPeerCache(WriteFlow)
		region := buildRegion(WriteFlow, peerCount, interval)
		// prepare
		intervalSums := make(map[uint64]int)
		for i := 1; i <= 200; i++ {
			rets := checker(c, cache, region)
			checkIntervalSumContinuous(c, intervalSums, rets, interval)
		}
		// make the interval sum of peers are different
		checkAndUpdateSkipOne(c, cache, region)
		checkIntervalSum(cache, region)
		// check whether cold cache is cleared
		var isClear bool
		intervalSums = make(map[uint64]int)
		region = region.Clone(core.SetWrittenBytes(0), core.SetWrittenKeys(0), core.SetWrittenQuery(0))
		for i := 1; i <= 200; i++ {
			rets := checker(c, cache, region)
			checkIntervalSumContinuous(c, intervalSums, rets, interval)
			if len(cache.storesOfRegion[region.GetID()]) == 0 {
				isClear = true
				break
			}
		}
		c.Assert(isClear, IsTrue)
	}
}

func (t *testHotPeerCache) TestRemoveFromCacheRandom(c *C) {
	peerCounts := []int{3, 5}
	intervals := []uint64{120, 60, 10, 5}
	checkers := []check{checkAndUpdate, checkAndUpdateWithOrdering}
	for _, peerCount := range peerCounts {
		for _, interval := range intervals {
			for _, checker := range checkers {
				cache := NewHotPeerCache(WriteFlow)
				region := buildRegion(WriteFlow, peerCount, interval)

				target := uint64(10)
				intervalSums := make(map[uint64]int)
				step := func(i int) {
					tmp := uint64(0)
					if i%5 == 0 {
						tmp, region = schedule(c, removeReplica, region)
					}
					rets := checker(c, cache, region)
					checkIntervalSumContinuous(c, intervalSums, rets, interval)
					if i%5 == 0 {
						_, region = schedule(c, addReplica, region, target)
						target = tmp
					}
				}

				// prepare with random move peer to make the interval sum of peers are different
				for i := 1; i < 150; i++ {
					step(i)
					if i > 150 && checkIntervalSum(cache, region) {
						break
					}
				}
				if interval < RegionHeartBeatReportInterval {
					c.Assert(checkIntervalSum(cache, region), IsTrue)
				}
				c.Assert(cache.storesOfRegion[region.GetID()], HasLen, peerCount)

				// check whether cold cache is cleared
				var isClear bool
				intervalSums = make(map[uint64]int)
				region = region.Clone(core.SetWrittenBytes(0), core.SetWrittenKeys(0), core.SetWrittenQuery(0))
				for i := 1; i < 200; i++ {
					step(i)
					if len(cache.storesOfRegion[region.GetID()]) == 0 {
						isClear = true
						break
					}
				}
				c.Assert(isClear, IsTrue)
			}
		}
	}
}

func checkCoolDown(c *C, cache *hotPeerCache, region *core.RegionInfo, expect bool) {
	item := cache.getOldHotPeerStat(region.GetID(), region.GetLeader().GetStoreId())
	c.Assert(item.IsNeedCoolDownTransferLeader(3), Equals, expect)
}

func (t *testHotPeerCache) TestCoolDownTransferLeader(c *C) {
	cache := NewHotPeerCache(ReadFlow)
	region := buildRegion(ReadFlow, 3, 60)

	moveLeader := func() {
		_, region = schedule(c, movePeer, region, 10)
		checkAndUpdate(c, cache, region)
		checkCoolDown(c, cache, region, false)
		_, region = schedule(c, transferLeader, region, 10)
		checkAndUpdate(c, cache, region)
		checkCoolDown(c, cache, region, true)
	}
	transferLeader := func() {
		_, region = schedule(c, transferLeader, region)
		checkAndUpdate(c, cache, region)
		checkCoolDown(c, cache, region, true)
	}
	movePeer := func() {
		_, region = schedule(c, movePeer, region, 10)
		checkAndUpdate(c, cache, region)
		checkCoolDown(c, cache, region, false)
	}
	addReplica := func() {
		_, region = schedule(c, addReplica, region, 10)
		checkAndUpdate(c, cache, region)
		checkCoolDown(c, cache, region, false)
	}
	removeReplica := func() {
		_, region = schedule(c, removeReplica, region, 10)
		checkAndUpdate(c, cache, region)
		checkCoolDown(c, cache, region, false)
	}
	cases := []func(){moveLeader, transferLeader, movePeer, addReplica, removeReplica}
	for _, runCase := range cases {
		cache = NewHotPeerCache(ReadFlow)
		region = buildRegion(ReadFlow, 3, 60)
		for i := 1; i <= 200; i++ {
			checkAndUpdate(c, cache, region)
		}
		checkCoolDown(c, cache, region, false)
		runCase()
	}
}

// See issue #4510
func (t *testHotPeerCache) TestCacheInherit(c *C) {
	cache := NewHotPeerCache(ReadFlow)
	region := buildRegion(ReadFlow, 3, 10)
	// prepare
	for i := 1; i <= 200; i++ {
		checkAndUpdate(c, cache, region)
	}
	// move peer
	newStoreID := uint64(10)
	_, region = schedule(c, addReplica, region, newStoreID)
	checkAndUpdate(c, cache, region)
	newStoreID, region = schedule(c, removeReplica, region)
	rets := checkAndUpdate(c, cache, region)
	for _, ret := range rets {
		if ret.needDelete == false {
			flow := ret.GetLoads()[RegionReadBytes]
			c.Assert(flow, Equals, float64(region.GetBytesRead()/ReadReportInterval))
		}
	}
	// new flow
	newFlow := region.GetBytesRead() * 10
	region = region.Clone(core.SetReadBytes(newFlow))
	for i := 1; i <= 200; i++ {
		checkAndUpdate(c, cache, region)
	}
	// move peer
	_, region = schedule(c, addReplica, region, newStoreID)
	checkAndUpdate(c, cache, region)
	_, region = schedule(c, removeReplica, region)
	rets = checkAndUpdate(c, cache, region)
	for _, ret := range rets {
		if ret.needDelete == false {
			flow := ret.GetLoads()[RegionReadBytes]
			c.Assert(flow, Equals, float64(newFlow/ReadReportInterval))
		}
	}
}

func BenchmarkCheckRegionFlow(b *testing.B) {
	cache := NewHotPeerCache(ReadFlow)
	region := buildRegion(ReadFlow, 3, 10)
	peerInfos := make([]*core.PeerInfo, 0)
	for _, peer := range region.GetPeers() {
		peerInfo := core.NewPeerInfo(peer, region.GetLoads(), 10)
		peerInfos = append(peerInfos, peerInfo)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items := make([]*HotPeerStat, 0)
		for _, peerInfo := range peerInfos {
			item := cache.CheckPeerFlow(peerInfo, region)
			if item != nil {
				items = append(items, item)
			}
		}
		for _, ret := range items {
			cache.Update(ret)
		}
	}
}
