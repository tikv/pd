// Copyright 2016 TiKV Project Authors.
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

package core

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/id"
)

func TestCore(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRegionInfoSuite{})

type testRegionInfoSuite struct{}

func (s *testRegionInfoSuite) TestSortedEqual(c *C) {
	testcases := []struct {
		idsA    []int
		idsB    []int
		isEqual bool
	}{
		{
			[]int{},
			[]int{},
			true,
		},
		{
			[]int{},
			[]int{1, 2},
			false,
		},
		{
			[]int{1, 2},
			[]int{1, 2},
			true,
		},
		{
			[]int{1, 2},
			[]int{2, 1},
			true,
		},
		{
			[]int{1, 2},
			[]int{1, 2, 3},
			false,
		},
		{
			[]int{1, 2, 3},
			[]int{2, 3, 1},
			true,
		},
		{
			[]int{1, 3},
			[]int{1, 2},
			false,
		},
	}
	meta := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{
				Id:      1,
				StoreId: 10,
			},
			{
				Id:      3,
				StoreId: 30,
			},
			{
				Id:      2,
				StoreId: 20,
				Role:    metapb.PeerRole_Learner,
			},
			{
				Id:      4,
				StoreId: 40,
				Role:    metapb.PeerRole_IncomingVoter,
			},
		},
	}
	pickPeers := func(ids []int) []*metapb.Peer {
		peers := make([]*metapb.Peer, 0, len(ids))
		for _, i := range ids {
			peers = append(peers, meta.Peers[i])
		}
		return peers
	}
	pickPeerStats := func(ids []int) []*pdpb.PeerStats {
		peers := make([]*pdpb.PeerStats, 0, len(ids))
		for _, i := range ids {
			peers = append(peers, &pdpb.PeerStats{Peer: meta.Peers[i]})
		}
		return peers
	}
	// test NewRegionInfo
	for _, t := range testcases {
		regionA := NewRegionInfo(&metapb.Region{Id: 100, Peers: pickPeers(t.idsA)}, nil)
		regionB := NewRegionInfo(&metapb.Region{Id: 100, Peers: pickPeers(t.idsB)}, nil)
		c.Assert(SortedPeersEqual(regionA.GetVoters(), regionB.GetVoters()), Equals, t.isEqual)
		c.Assert(SortedPeersEqual(regionA.GetVoters(), regionB.GetVoters()), Equals, t.isEqual)
	}

	// test RegionFromHeartbeat
	for _, t := range testcases {
		regionA := RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
			Region:       &metapb.Region{Id: 100, Peers: pickPeers(t.idsA)},
			DownPeers:    pickPeerStats(t.idsA),
			PendingPeers: pickPeers(t.idsA),
		})
		regionB := RegionFromHeartbeat(&pdpb.RegionHeartbeatRequest{
			Region:       &metapb.Region{Id: 100, Peers: pickPeers(t.idsB)},
			DownPeers:    pickPeerStats(t.idsB),
			PendingPeers: pickPeers(t.idsB),
		})
		c.Assert(SortedPeersEqual(regionA.GetVoters(), regionB.GetVoters()), Equals, t.isEqual)
		c.Assert(SortedPeersEqual(regionA.GetVoters(), regionB.GetVoters()), Equals, t.isEqual)
		c.Assert(SortedPeersEqual(regionA.GetPendingPeers(), regionB.GetPendingPeers()), Equals, t.isEqual)
		c.Assert(SortedPeersStatsEqual(regionA.GetDownPeers(), regionB.GetDownPeers()), Equals, t.isEqual)
	}

	// test Clone
	region := NewRegionInfo(meta, meta.Peers[0])
	for _, t := range testcases {
		downPeersA := pickPeerStats(t.idsA)
		downPeersB := pickPeerStats(t.idsB)
		pendingPeersA := pickPeers(t.idsA)
		pendingPeersB := pickPeers(t.idsB)

		regionA := region.Clone(WithDownPeers(downPeersA), WithPendingPeers(pendingPeersA))
		regionB := region.Clone(WithDownPeers(downPeersB), WithPendingPeers(pendingPeersB))
		c.Assert(SortedPeersStatsEqual(regionA.GetDownPeers(), regionB.GetDownPeers()), Equals, t.isEqual)
		c.Assert(SortedPeersEqual(regionA.GetPendingPeers(), regionB.GetPendingPeers()), Equals, t.isEqual)
	}
}

func (s *testRegionInfoSuite) TestRegionRoundingFlow(c *C) {
	testcases := []struct {
		flow   uint64
		digit  int
		expect uint64
	}{
		{10, 0, 10},
		{13, 1, 10},
		{11807, 3, 12000},
		{252623, 4, 250000},
		{258623, 4, 260000},
		{258623, 64, 0},
		{252623, math.MaxInt64, 0},
		{252623, math.MinInt64, 252623},
	}
	for _, t := range testcases {
		r := NewRegionInfo(&metapb.Region{Id: 100}, nil, WithFlowRoundByDigit(t.digit))
		r.readBytes = t.flow
		r.writtenBytes = t.flow
		c.Assert(r.GetRoundBytesRead(), Equals, t.expect)
	}
}

func (s *testRegionInfoSuite) TestRegionWriteRate(c *C) {
	testcases := []struct {
		bytes           uint64
		keys            uint64
		interval        uint64
		expectBytesRate float64
		expectKeysRate  float64
	}{
		{0, 0, 0, 0, 0},
		{10, 3, 0, 0, 0},
		{0, 0, 1, 0, 0},
		{10, 3, 1, 0, 0},
		{0, 0, 5, 0, 0},
		{10, 3, 5, 2, 0.6},
		{0, 0, 500, 0, 0},
		{10, 3, 500, 0, 0},
	}
	for _, t := range testcases {
		r := NewRegionInfo(&metapb.Region{Id: 100}, nil, SetWrittenBytes(t.bytes), SetWrittenKeys(t.keys), SetReportInterval(t.interval))
		bytesRate, keysRate := r.GetWriteRate()
		c.Assert(bytesRate, Equals, t.expectBytesRate)
		c.Assert(keysRate, Equals, t.expectKeysRate)
	}
}

var _ = Suite(&testRegionMapSuite{})

type testRegionMapSuite struct{}

func (s *testRegionMapSuite) TestRegionMap(c *C) {
	rm := newRegionMap()
	s.check(c, rm)
	rm.AddNew(s.regionInfo(1))
	s.check(c, rm, 1)

	rm.AddNew(s.regionInfo(2))
	rm.AddNew(s.regionInfo(3))
	s.check(c, rm, 1, 2, 3)

	rm.AddNew(s.regionInfo(3))
	rm.Delete(4)
	s.check(c, rm, 1, 2, 3)

	rm.Delete(3)
	rm.Delete(1)
	s.check(c, rm, 2)

	rm.AddNew(s.regionInfo(3))
	s.check(c, rm, 2, 3)
}

func (s *testRegionMapSuite) regionInfo(id uint64) *RegionInfo {
	return &RegionInfo{
		meta: &metapb.Region{
			Id: id,
		},
		approximateSize: int64(id),
		approximateKeys: int64(id),
	}
}

func (s *testRegionMapSuite) check(c *C, rm regionMap, ids ...uint64) {
	// Check Get.
	for _, id := range ids {
		c.Assert(rm.Get(id).region.GetID(), Equals, id)
	}
	// Check Len.
	c.Assert(rm.Len(), Equals, len(ids))
	// Check id set.
	expect := make(map[uint64]struct{})
	for _, id := range ids {
		expect[id] = struct{}{}
	}
	set1 := make(map[uint64]struct{})
	for _, r := range rm {
		set1[r.region.GetID()] = struct{}{}
	}
	c.Assert(set1, DeepEquals, expect)
}

var _ = Suite(&testRegionKey{})

type testRegionKey struct{}

func (*testRegionKey) TestRegionKey(c *C) {
	testCase := []struct {
		key    string
		expect string
	}{
		{`"t\x80\x00\x00\x00\x00\x00\x00\xff!_r\x80\x00\x00\x00\x00\xff\x02\u007fY\x00\x00\x00\x00\x00\xfa"`,
			`7480000000000000FF215F728000000000FF027F590000000000FA`},
		{"\"\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\xff\\x05\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\xf8\"",
			`80000000000000FF0500000000000000F8`},
	}
	for _, t := range testCase {
		got, err := strconv.Unquote(t.key)
		c.Assert(err, IsNil)
		s := fmt.Sprintln(RegionToHexMeta(&metapb.Region{StartKey: []byte(got)}))
		c.Assert(strings.Contains(s, t.expect), IsTrue)

		// start key changed
		origin := NewRegionInfo(&metapb.Region{EndKey: []byte(got)}, nil)
		region := NewRegionInfo(&metapb.Region{StartKey: []byte(got), EndKey: []byte(got)}, nil)
		s = DiffRegionKeyInfo(origin, region)
		c.Assert(s, Matches, ".*StartKey Changed.*")
		c.Assert(strings.Contains(s, t.expect), IsTrue)

		// end key changed
		origin = NewRegionInfo(&metapb.Region{StartKey: []byte(got)}, nil)
		region = NewRegionInfo(&metapb.Region{StartKey: []byte(got), EndKey: []byte(got)}, nil)
		s = DiffRegionKeyInfo(origin, region)
		c.Assert(s, Matches, ".*EndKey Changed.*")
		c.Assert(strings.Contains(s, t.expect), IsTrue)
	}
}

func (*testRegionKey) TestSetRegion(c *C) {
	regions := NewRegionsInfo()
	for i := 0; i < 100; i++ {
		peer1 := &metapb.Peer{StoreId: uint64(i%5 + 1), Id: uint64(i*5 + 1)}
		peer2 := &metapb.Peer{StoreId: uint64((i+1)%5 + 1), Id: uint64(i*5 + 2)}
		peer3 := &metapb.Peer{StoreId: uint64((i+2)%5 + 1), Id: uint64(i*5 + 3)}
		region := NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			Peers:    []*metapb.Peer{peer1, peer2, peer3},
			StartKey: []byte(fmt.Sprintf("%20d", i*10)),
			EndKey:   []byte(fmt.Sprintf("%20d", (i+1)*10)),
		}, peer1)
		regions.SetRegion(region)
	}

	peer1 := &metapb.Peer{StoreId: uint64(4), Id: uint64(101)}
	peer2 := &metapb.Peer{StoreId: uint64(5), Id: uint64(102)}
	peer3 := &metapb.Peer{StoreId: uint64(1), Id: uint64(103)}
	region := NewRegionInfo(&metapb.Region{
		Id:       uint64(21),
		Peers:    []*metapb.Peer{peer1, peer2, peer3},
		StartKey: []byte(fmt.Sprintf("%20d", 184)),
		EndKey:   []byte(fmt.Sprintf("%20d", 211)),
	}, peer1)
	region.learners = append(region.learners, peer2)
	region.pendingPeers = append(region.pendingPeers, peer3)
	regions.SetRegion(region)
	checkRegions(c, regions)
	c.Assert(regions.tree.length(), Equals, 97)
	c.Assert(len(regions.GetRegions()), Equals, 97)

	regions.SetRegion(region)
	peer1 = &metapb.Peer{StoreId: uint64(2), Id: uint64(101)}
	peer2 = &metapb.Peer{StoreId: uint64(3), Id: uint64(102)}
	peer3 = &metapb.Peer{StoreId: uint64(1), Id: uint64(103)}
	region = NewRegionInfo(&metapb.Region{
		Id:       uint64(21),
		Peers:    []*metapb.Peer{peer1, peer2, peer3},
		StartKey: []byte(fmt.Sprintf("%20d", 184)),
		EndKey:   []byte(fmt.Sprintf("%20d", 212)),
	}, peer1)
	region.learners = append(region.learners, peer2)
	region.pendingPeers = append(region.pendingPeers, peer3)
	regions.SetRegion(region)
	checkRegions(c, regions)
	c.Assert(regions.tree.length(), Equals, 97)
	c.Assert(len(regions.GetRegions()), Equals, 97)

	// Test remove overlaps.
	region = region.Clone(WithStartKey([]byte(fmt.Sprintf("%20d", 175))), WithNewRegionID(201))
	c.Assert(regions.GetRegion(21), NotNil)
	c.Assert(regions.GetRegion(18), NotNil)
	regions.SetRegion(region)
	checkRegions(c, regions)
	c.Assert(regions.tree.length(), Equals, 96)
	c.Assert(len(regions.GetRegions()), Equals, 96)
	c.Assert(regions.GetRegion(201), NotNil)
	c.Assert(regions.GetRegion(21), IsNil)
	c.Assert(regions.GetRegion(18), IsNil)

	// Test update keys and size of region.
	region = region.Clone(
		SetApproximateKeys(20),
		SetApproximateSize(30),
		SetWrittenBytes(40),
		SetWrittenKeys(10),
		SetReportInterval(5))
	regions.SetRegion(region)
	checkRegions(c, regions)
	c.Assert(regions.tree.length(), Equals, 96)
	c.Assert(len(regions.GetRegions()), Equals, 96)
	c.Assert(regions.GetRegion(201), NotNil)
	c.Assert(regions.tree.TotalSize(), Equals, int64(30))
	bytesRate, keysRate := regions.tree.TotalWriteRate()
	c.Assert(bytesRate, Equals, float64(8))
	c.Assert(keysRate, Equals, float64(2))
}

func (*testRegionKey) TestShouldRemoveFromSubTree(c *C) {
	regions := NewRegionsInfo()
	peer1 := &metapb.Peer{StoreId: uint64(1), Id: uint64(1)}
	peer2 := &metapb.Peer{StoreId: uint64(2), Id: uint64(2)}
	peer3 := &metapb.Peer{StoreId: uint64(3), Id: uint64(3)}
	peer4 := &metapb.Peer{StoreId: uint64(3), Id: uint64(3)}
	region := NewRegionInfo(&metapb.Region{
		Id:       uint64(1),
		Peers:    []*metapb.Peer{peer1, peer2, peer4},
		StartKey: []byte(fmt.Sprintf("%20d", 10)),
		EndKey:   []byte(fmt.Sprintf("%20d", 20)),
	}, peer1)

	origin := NewRegionInfo(&metapb.Region{
		Id:       uint64(1),
		Peers:    []*metapb.Peer{peer1, peer2, peer3},
		StartKey: []byte(fmt.Sprintf("%20d", 10)),
		EndKey:   []byte(fmt.Sprintf("%20d", 20)),
	}, peer1)
	c.Assert(regions.shouldRemoveFromSubTree(region, origin), Equals, false)

	region.leader = peer2
	c.Assert(regions.shouldRemoveFromSubTree(region, origin), Equals, true)

	region.leader = peer1
	region.pendingPeers = append(region.pendingPeers, peer4)
	c.Assert(regions.shouldRemoveFromSubTree(region, origin), Equals, true)

	region.pendingPeers = nil
	region.learners = append(region.learners, peer2)
	c.Assert(regions.shouldRemoveFromSubTree(region, origin), Equals, true)

	origin.learners = append(origin.learners, peer2, peer3)
	region.learners = append(region.learners, peer4)
	c.Assert(regions.shouldRemoveFromSubTree(region, origin), Equals, false)

	region.voters[2].StoreId = 4
	c.Assert(regions.shouldRemoveFromSubTree(region, origin), Equals, true)
}

func checkRegions(c *C, regions *RegionsInfo) {
	leaderMap := make(map[uint64]uint64)
	followerMap := make(map[uint64]uint64)
	learnerMap := make(map[uint64]uint64)
	pendingPeerMap := make(map[uint64]uint64)
	for _, item := range regions.GetRegions() {
		if leaderCount, ok := leaderMap[item.leader.StoreId]; ok {
			leaderMap[item.leader.StoreId] = leaderCount + 1
		} else {
			leaderMap[item.leader.StoreId] = 1
		}
		for _, follower := range item.GetFollowers() {
			if followerCount, ok := followerMap[follower.StoreId]; ok {
				followerMap[follower.StoreId] = followerCount + 1
			} else {
				followerMap[follower.StoreId] = 1
			}
		}
		for _, learner := range item.GetLearners() {
			if learnerCount, ok := learnerMap[learner.StoreId]; ok {
				learnerMap[learner.StoreId] = learnerCount + 1
			} else {
				learnerMap[learner.StoreId] = 1
			}
		}
		for _, pendingPeer := range item.GetPendingPeers() {
			if pendingPeerCount, ok := pendingPeerMap[pendingPeer.StoreId]; ok {
				pendingPeerMap[pendingPeer.StoreId] = pendingPeerCount + 1
			} else {
				pendingPeerMap[pendingPeer.StoreId] = 1
			}
		}
	}
	for key, value := range regions.leaders {
		c.Assert(value.length(), Equals, int(leaderMap[key]))
	}
	for key, value := range regions.followers {
		c.Assert(value.length(), Equals, int(followerMap[key]))
	}
	for key, value := range regions.learners {
		c.Assert(value.length(), Equals, int(learnerMap[key]))
	}
	for key, value := range regions.pendingPeers {
		c.Assert(value.length(), Equals, int(pendingPeerMap[key]))
	}
}

func BenchmarkRandomRegion(b *testing.B) {
	regions := NewRegionsInfo()
	for i := 0; i < 5000000; i++ {
		peer := &metapb.Peer{StoreId: 1, Id: uint64(i + 1)}
		region := NewRegionInfo(&metapb.Region{
			Id:       uint64(i + 1),
			Peers:    []*metapb.Peer{peer},
			StartKey: []byte(fmt.Sprintf("%20d", i)),
			EndKey:   []byte(fmt.Sprintf("%20d", i+1)),
		}, peer)
		regions.SetRegion(region)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		regions.RandLeaderRegion(1, nil)
	}
}

const keyLength = 100

func randomBytes(n int) []byte {
	bytes := make([]byte, n)
	_, err := rand.Read(bytes)
	if err != nil {
		panic(err)
	}
	return bytes
}

func newRegionInfoID(idAllocator id.Allocator) *RegionInfo {
	var (
		peers  []*metapb.Peer
		leader *metapb.Peer
	)
	for i := 0; i < 3; i++ {
		id, _ := idAllocator.Alloc()
		p := &metapb.Peer{Id: id, StoreId: id}
		if i == 0 {
			leader = p
		}
		peers = append(peers, p)
	}
	regionID, _ := idAllocator.Alloc()
	return NewRegionInfo(
		&metapb.Region{
			Id:       regionID,
			StartKey: randomBytes(keyLength),
			EndKey:   randomBytes(keyLength),
			Peers:    peers,
		},
		leader,
	)
}

func BenchmarkAddRegion(b *testing.B) {
	regions := NewRegionsInfo()
	idAllocator := mockid.NewIDAllocator()
	var items []*RegionInfo
	for i := 0; i < 10000000; i++ {
		items = append(items, newRegionInfoID(idAllocator))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		regions.SetRegion(items[i])
	}
}
